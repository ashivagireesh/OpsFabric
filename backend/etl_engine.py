"""
ETL Engine - Core execution engine for running pipelines.
Supports: Databases, Files, APIs, Cloud Storage, Message Queues
"""
import asyncio
import ast
import json
import math
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus
from loguru import logger


class ETLEngine:
    def __init__(self):
        self.active_executions: Dict[str, dict] = {}
        # Hot-path caches for custom expression evaluation.
        self._expr_ast_cache: Dict[str, ast.AST] = {}
        self._json_template_cache: Dict[str, Any] = {}
        self._path_variants_cache: Dict[str, List[str]] = {}

    def _uploads_dir(self) -> str:
        return os.path.join(os.path.dirname(__file__), "uploads")

    def _safe_filename(self, value: str) -> str:
        return "".join(c if c.isalnum() or c in "._-" else "_" for c in (value or ""))

    def _find_uploaded_file(self, file_name: str) -> Optional[str]:
        """
        Find the most recent uploaded file matching the given file name.
        Uploads are stored as: <8hex>_<safe_filename>.
        """
        base_name = os.path.basename(str(file_name or "").strip())
        if not base_name:
            return None

        uploads_dir = self._uploads_dir()
        if not os.path.isdir(uploads_dir):
            return None

        safe_name = self._safe_filename(base_name)
        candidates = []
        try:
            for fname in os.listdir(uploads_dir):
                if fname == safe_name or fname.endswith(f"_{safe_name}"):
                    fpath = os.path.join(uploads_dir, fname)
                    if os.path.isfile(fpath):
                        candidates.append(fpath)
        except Exception:
            return None

        if not candidates:
            return None
        candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return candidates[0]

    def _resolve_input_file_path(self, raw_path: str) -> str:
        """
        Resolve source file path for ETL sources.

        Supports:
        - absolute server paths
        - fallback lookup in uploads/ for plain file names
        - recovery from local://<filename> by locating a matching uploaded file
        """
        file_path = str(raw_path or "").strip()
        if not file_path:
            raise RuntimeError("File path is empty. Select a source file.")

        # Browser-only placeholder path. Try to recover from uploads cache.
        if file_path.startswith("local://"):
            original_name = file_path.replace("local://", "", 1).strip().strip("/")
            recovered = self._find_uploaded_file(original_name)
            if recovered:
                logger.info(f"Recovered local file reference '{original_name}' to uploaded path: {recovered}")
                return recovered
            raise RuntimeError(
                f"File '{original_name}' was not uploaded to the server. "
                "Please re-open the source node and re-select your file while the backend is running."
            )

        # Normal absolute/relative path.
        if os.path.isfile(file_path):
            return file_path

        # Allow bare filename recovery from uploads dir.
        recovered = self._find_uploaded_file(file_path)
        if recovered:
            logger.info(f"Recovered file path '{file_path}' to uploaded path: {recovered}")
            return recovered

        return file_path

    def _runtime_state_path(self) -> str:
        state_dir = os.path.join(os.path.dirname(__file__), "state")
        os.makedirs(state_dir, exist_ok=True)
        return os.path.join(state_dir, "pipeline_runtime_state.json")

    def _load_runtime_state(self) -> Dict[str, Any]:
        path = self._runtime_state_path()
        if not os.path.isfile(path):
            return {"pipelines": {}}
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                data.setdefault("pipelines", {})
                return data
        except Exception:
            pass
        return {"pipelines": {}}

    def _save_runtime_state(self, state: Dict[str, Any]) -> None:
        path = self._runtime_state_path()
        try:
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(state, fh, ensure_ascii=False, indent=2)
        except Exception as exc:
            logger.warning(f"Failed to persist runtime state: {exc}")

    def _normalize_runtime_config(self, runtime_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        cfg = runtime_config if isinstance(runtime_config, dict) else {}
        mode = str(cfg.get("execution_mode") or cfg.get("mode") or "batch").strip().lower()
        if mode not in {"batch", "incremental", "streaming"}:
            mode = "batch"

        def _bounded_int(value: Any, default: int, min_v: int, max_v: int) -> int:
            try:
                num = int(value)
            except Exception:
                num = default
            return max(min_v, min(num, max_v))

        return {
            "mode": mode,
            "batch_size": _bounded_int(cfg.get("batch_size", 5000), 5000, 1, 1_000_000),
            "incremental_field": str(cfg.get("incremental_field") or "").strip(),
            "streaming_interval_seconds": _bounded_int(cfg.get("streaming_interval_seconds", 5), 5, 1, 3600),
            "streaming_max_batches": _bounded_int(cfg.get("streaming_max_batches", 10), 10, 1, 10_000),
        }

    def _is_source_node(self, node_type: str) -> bool:
        return node_type.endswith("_source")

    def _is_chunkable_transform(self, node_type: str, config: Dict[str, Any]) -> bool:
        if node_type in {"filter_transform", "rename_transform", "type_convert_transform", "flatten_transform"}:
            return True
        if node_type == "map_transform":
            # Grouped custom fields require full group scope.
            grouped = str(
                config.get("custom_primary_key_field")
                or config.get("custom_group_by_field")
                or ""
            ).strip()
            return not grouped
        return False

    def _chunk_rows(self, rows: List[Any], size: int) -> List[List[Any]]:
        if size <= 0 or len(rows) <= size:
            return [rows]
        return [rows[i:i + size] for i in range(0, len(rows), size)]

    def _comparison_token(self, value: Any) -> Tuple[str, Any]:
        if value is None:
            return ("none", None)
        if isinstance(value, bool):
            return ("num", int(value))
        if isinstance(value, (int, float)):
            return ("num", float(value))
        if isinstance(value, datetime):
            return ("dt", value.timestamp())
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return ("str", "")
            try:
                return ("num", float(text.replace(",", "")))
            except Exception:
                pass
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                return ("dt", dt.timestamp())
            except Exception:
                pass
            return ("str", text)
        return ("str", str(value))

    def _compare_values(self, left: Any, right: Any) -> int:
        lt, lv = self._comparison_token(left)
        rt, rv = self._comparison_token(right)
        if lv is None and rv is None:
            return 0
        if lv is None:
            return -1
        if rv is None:
            return 1
        if lt == rt:
            try:
                if lv < rv:
                    return -1
                if lv > rv:
                    return 1
                return 0
            except Exception:
                pass
        ls = str(left)
        rs = str(right)
        if ls < rs:
            return -1
        if ls > rs:
            return 1
        return 0

    def _max_value(self, a: Any, b: Any) -> Any:
        if a is None:
            return b
        if b is None:
            return a
        return b if self._compare_values(b, a) > 0 else a

    def _apply_incremental_filter(
        self,
        rows: List[Any],
        field_path: str,
        last_checkpoint: Any,
    ) -> Tuple[List[Any], Any, int]:
        if not isinstance(rows, list) or not rows or not field_path:
            return rows, last_checkpoint, 0

        filtered: List[Any] = []
        scanned = 0
        max_seen = last_checkpoint
        for row in rows:
            if not isinstance(row, dict):
                filtered.append(row)
                continue
            value, found = self._extract_row_value_by_path(row, field_path)
            if not found or value is None:
                filtered.append(row)
                continue
            scanned += 1
            max_seen = self._max_value(max_seen, value)
            if last_checkpoint is None or self._compare_values(value, last_checkpoint) > 0:
                filtered.append(row)

        return filtered, max_seen, scanned

    def _extract_series_from_rows(self, rows: Any, field_path: str) -> List[Any]:
        if not isinstance(rows, list):
            return []
        path = str(field_path or "").strip()
        if not path:
            return []
        series: List[Any] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            value, found = self._extract_row_value_by_path(row, path)
            if found:
                series.append(value)
        return series

    def _normalize_json_path_expr(self, path: Optional[str]) -> str:
        text = str(path or "").strip()
        if not text or text == "$":
            return ""
        if text.startswith("$."):
            text = text[2:]
        elif text.startswith("$"):
            text = text[1:]
        text = text.strip(".")
        text = text.replace("[*]", "[]")
        return text

    def _extract_json_path_value(self, payload: Any, path: Optional[str]) -> Any:
        import re

        expr = self._normalize_json_path_expr(path)
        if not expr:
            return payload

        tokenized = re.sub(r"\[(\d+)\]", lambda m: f".#{m.group(1)}", expr)
        tokenized = tokenized.replace("[]", ".#all")
        tokens = [t for t in tokenized.split(".") if t]

        current: Any = payload
        for tok in tokens:
            if tok.startswith("#"):
                if tok == "#all":
                    if isinstance(current, list):
                        continue
                    if current is None:
                        return []
                    current = [current]
                    continue
                try:
                    idx = int(tok[1:])
                except Exception:
                    return None
                if not isinstance(current, list):
                    return None
                if idx < 0 or idx >= len(current):
                    return None
                current = current[idx]
                continue

            if isinstance(current, dict):
                current = current.get(tok)
                continue
            if isinstance(current, list):
                projected = []
                for item in current:
                    if isinstance(item, dict) and tok in item:
                        projected.append(item.get(tok))
                current = projected
                continue
            return None
        return current

    def _rows_from_json_value(self, value: Any) -> list:
        if value is None:
            return []
        if isinstance(value, dict):
            # Convert object-of-arrays payloads into row-wise records.
            array_fields = {k: v for k, v in value.items() if isinstance(v, list)}
            if array_fields:
                lengths = {len(v) for v in array_fields.values()}
                if len(lengths) == 1:
                    row_count = next(iter(lengths))
                    if row_count > 0:
                        scalar_fields = {k: v for k, v in value.items() if not isinstance(v, list)}
                        rows = []
                        for idx in range(row_count):
                            row = {k: col[idx] for k, col in array_fields.items()}
                            if scalar_fields:
                                row.update(scalar_fields)
                            rows.append(row)
                        return rows
            nested = value.get("data")
            if isinstance(nested, list) and all(isinstance(row, dict) for row in nested):
                return nested
            return [value]
        if isinstance(value, list):
            if not value:
                return []
            if all(isinstance(row, dict) for row in value):
                return value
            flattened = []
            for item in value:
                if isinstance(item, list):
                    for child in item:
                        if isinstance(child, dict):
                            flattened.append(child)
                elif isinstance(item, dict):
                    flattened.append(item)
            if flattened:
                return flattened
            return [{"value": item} for item in value]
        return [{"value": value}]

    def _is_single_value_rows(self, rows: Any) -> bool:
        if not isinstance(rows, list) or not rows:
            return False
        sample = rows[: min(len(rows), 20)]
        for row in sample:
            if not isinstance(row, dict):
                return False
            if set(row.keys()) != {"value"}:
                return False
        return True

    def _parent_json_path(self, path: Optional[str]) -> str:
        import re

        expr = self._normalize_json_path_expr(path)
        if not expr:
            return ""
        # Remove terminal index (e.g. items[0] -> items, items[] -> items)
        without_index = re.sub(r"\[(?:\d+|\*|)\]$", "", expr)
        if without_index != expr:
            return without_index
        if "." in expr:
            return expr.rsplit(".", 1)[0]
        return ""

    def _parse_request_object(self, value: Any, allow_query_string: bool = False) -> dict:
        if value is None or value == "":
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return {}
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    return parsed
                return {}
            except Exception:
                if allow_query_string:
                    from urllib.parse import parse_qsl
                    return dict(parse_qsl(text, keep_blank_values=True))
                return {}
        return {}

    def _parse_request_payload(self, value: Any) -> Any:
        if value is None or value == "":
            return None
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                return json.loads(text)
            except Exception:
                return None
        return None

    async def execute_pipeline(
        self,
        pipeline: dict,
        execution_id: str,
        db,
        websocket_manager=None,
        on_node_done=None,
        runtime_config: Optional[Dict[str, Any]] = None,
        pipeline_id: Optional[str] = None,
    ) -> dict:
        """Execute a full ETL pipeline by topologically sorting nodes."""
        nodes = {n["id"]: n for n in pipeline.get("nodes", [])}
        edges = pipeline.get("edges", [])
        runtime = self._normalize_runtime_config(runtime_config)
        mode = runtime["mode"]

        # Build adjacency list
        adj: Dict[str, List[str]] = {nid: [] for nid in nodes}
        in_degree: Dict[str, int] = {nid: 0 for nid in nodes}
        for edge in edges:
            src, tgt = edge["source"], edge["target"]
            if src in adj:
                adj[src].append(tgt)
            if tgt in in_degree:
                in_degree[tgt] += 1

        # Kahn's topological sort
        queue = [nid for nid, deg in in_degree.items() if deg == 0]
        order = []
        while queue:
            nid = queue.pop(0)
            order.append(nid)
            for neighbor in adj.get(nid, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        runtime_state = self._load_runtime_state()
        pipelines_state = runtime_state.setdefault("pipelines", {})
        pipeline_state: Dict[str, Any] = {}
        if pipeline_id:
            pipeline_state = pipelines_state.setdefault(pipeline_id, {})
        incremental_checkpoints = pipeline_state.setdefault("incremental_checkpoints", {})

        results: Dict[str, Any] = {}
        logs: List[dict] = []
        total_rows = 0
        stream_iterations = runtime["streaming_max_batches"] if mode == "streaming" else 1

        for stream_idx in range(stream_iterations):
            if mode == "streaming":
                batch_log = {
                    "nodeId": "__stream__",
                    "nodeLabel": "Streaming Engine",
                    "timestamp": datetime.utcnow().isoformat(),
                    "status": "running",
                    "message": f"⟳ Streaming batch {stream_idx + 1}/{stream_iterations}…",
                    "rows": 0,
                }
                logs.append(batch_log)
                if on_node_done:
                    on_node_done(logs)
                if websocket_manager:
                    await websocket_manager.broadcast(execution_id, {
                        "type": "log",
                        "log_entry": dict(batch_log),
                    })

            pass_results: Dict[str, Any] = {}
            pass_rows = 0
            for nid in order:
                node = nodes[nid]
                node_type = node.get("data", {}).get("nodeType", "")
                config = node.get("data", {}).get("config", {})
                label = node.get("data", {}).get("label", node_type)

                log_entry = {
                    "nodeId": nid,
                    "nodeLabel": label,
                    "timestamp": datetime.utcnow().isoformat(),
                    "status": "running",
                    "message": f"⟳ Running {label}…",
                    "rows": 0
                }
                logs.append(log_entry)

                if websocket_manager:
                    await websocket_manager.broadcast(execution_id, {
                        "type": "node_start",
                        "nodeId": nid,
                        "label": label,
                        "log_entry": dict(log_entry),
                    })

                try:
                    upstream_data = []
                    incoming_by_source: Dict[str, list] = {}
                    incoming_order: List[str] = []
                    for edge in edges:
                        if edge["target"] == nid and edge["source"] in pass_results:
                            source_id = edge["source"]
                            source_rows = pass_results[source_id] or []
                            incoming_by_source[source_id] = source_rows
                            incoming_order.append(source_id)
                            upstream_data.extend(source_rows)

                    chunk_batches = 1
                    if (
                        mode in {"batch", "streaming"}
                        and isinstance(upstream_data, list)
                        and len(upstream_data) > runtime["batch_size"]
                        and self._is_chunkable_transform(node_type, config)
                    ):
                        chunks = self._chunk_rows(upstream_data, runtime["batch_size"])
                        chunk_batches = len(chunks)
                        output: List[Any] = []
                        for chunk in chunks:
                            chunk_output = await self._execute_node(
                                node_type,
                                config,
                                chunk,
                                incoming_by_source={},
                                incoming_order=[],
                            )
                            if isinstance(chunk_output, list):
                                output.extend(chunk_output)
                            elif chunk_output is not None:
                                output.append(chunk_output)
                    else:
                        output = await self._execute_node(
                            node_type,
                            config,
                            upstream_data,
                            incoming_by_source=incoming_by_source,
                            incoming_order=incoming_order,
                        )

                    incremental_note = ""
                    if (
                        mode in {"incremental", "streaming"}
                        and self._is_source_node(node_type)
                        and runtime["incremental_field"]
                        and isinstance(output, list)
                    ):
                        checkpoint_key = f"{nid}:{runtime['incremental_field']}"
                        previous_checkpoint = incremental_checkpoints.get(checkpoint_key)
                        filtered, next_checkpoint, scanned = self._apply_incremental_filter(
                            output,
                            runtime["incremental_field"],
                            previous_checkpoint,
                        )
                        dropped = max(0, len(output) - len(filtered))
                        output = filtered
                        if next_checkpoint is not None:
                            incremental_checkpoints[checkpoint_key] = next_checkpoint
                        if scanned > 0 or dropped > 0:
                            incremental_note = f" | incremental field={runtime['incremental_field']} kept={len(filtered):,} dropped={dropped:,}"

                    pass_results[nid] = output
                    row_count = len(output) if isinstance(output, list) else 0
                    if (
                        isinstance(output, list)
                        and output
                        and isinstance(output[0], dict)
                        and isinstance(output[0].get("rows"), (int, float))
                    ):
                        row_count = int(output[0].get("rows") or 0)
                    total_rows += row_count
                    pass_rows += row_count
                    log_entry["status"] = "success"
                    log_entry["rows"] = row_count

                    batch_note = f" | batches={chunk_batches}" if chunk_batches > 1 else ""
                    if (
                        isinstance(output, list)
                        and output
                        and isinstance(output[0], dict)
                        and "path" in output[0]
                    ):
                        file_path = output[0]["path"]
                        log_entry["message"] = f"✓ {label} — written: {file_path} ({row_count:,} rows){batch_note}{incremental_note}"
                        log_entry["output_path"] = file_path
                    else:
                        log_entry["message"] = f"✓ {label} — {row_count:,} rows{batch_note}{incremental_note}"

                    if websocket_manager:
                        await websocket_manager.broadcast(execution_id, {
                            "type": "node_success",
                            "nodeId": nid,
                            "rows": row_count,
                            "log_entry": dict(log_entry),
                        })
                    if on_node_done:
                        on_node_done(logs)

                except Exception as e:
                    log_entry["status"] = "error"
                    log_entry["message"] = f"✗ {label}: {str(e)}"
                    logger.error(f"Node {nid} ({label}) failed: {e}")

                    if websocket_manager:
                        await websocket_manager.broadcast(execution_id, {
                            "type": "node_error",
                            "nodeId": nid,
                            "error": str(e),
                            "log_entry": dict(log_entry),
                        })
                    if on_node_done:
                        on_node_done(logs)
                    raise

            results = pass_results
            if mode == "streaming":
                if pass_rows <= 0 and stream_idx > 0:
                    break
                if stream_idx < stream_iterations - 1:
                    await asyncio.sleep(runtime["streaming_interval_seconds"])

        if pipeline_id:
            self._save_runtime_state(runtime_state)

        return {"node_results": results, "logs": logs, "rows_processed": total_rows}

    async def _execute_node(
        self,
        node_type: str,
        config: dict,
        upstream: list,
        incoming_by_source: Optional[Dict[str, list]] = None,
        incoming_order: Optional[List[str]] = None,
    ) -> list:
        """Dispatch to the correct connector/transform handler."""
        await asyncio.sleep(0.3)  # Simulate async I/O

        # ─── TRIGGERS ──────────────────────────────────────────
        if node_type in ("manual_trigger", "schedule_trigger", "webhook_trigger"):
            return []

        # ─── SOURCES ───────────────────────────────────────────
        elif node_type == "postgres_source":
            return await self._execute_postgres(config)
        elif node_type == "mysql_source":
            return await self._execute_mysql(config)
        elif node_type == "oracle_source":
            return await self._execute_oracle(config)
        elif node_type == "mongodb_source":
            return await self._execute_mongodb(config)
        elif node_type == "redis_source":
            return await self._execute_redis(config)
        elif node_type == "elasticsearch_source":
            return await self._execute_elasticsearch(config)
        elif node_type == "csv_source":
            return await self._execute_csv(config)
        elif node_type == "json_source":
            return await self._execute_json(config)
        elif node_type == "excel_source":
            return await self._execute_excel(config)
        elif node_type == "xml_source":
            return await self._execute_xml(config)
        elif node_type == "parquet_source":
            return await self._execute_parquet(config)
        elif node_type == "rest_api_source":
            return await self._execute_rest_api(config)
        elif node_type == "graphql_source":
            return await self._execute_graphql(config)
        elif node_type == "s3_source":
            return await self._execute_s3(config)
        elif node_type == "kafka_source":
            return await self._execute_kafka(config)

        # ─── TRANSFORMS ────────────────────────────────────────
        elif node_type == "filter_transform":
            return self._transform_filter(upstream, config)
        elif node_type == "map_transform":
            return self._transform_map(upstream, config)
        elif node_type == "rename_transform":
            return self._transform_rename(upstream, config)
        elif node_type == "aggregate_transform":
            return self._transform_aggregate(upstream, config)
        elif node_type == "join_transform":
            return self._transform_join(
                upstream,
                config,
                incoming_by_source=incoming_by_source or {},
                incoming_order=incoming_order or [],
            )
        elif node_type == "sort_transform":
            return self._transform_sort(upstream, config)
        elif node_type == "deduplicate_transform":
            return self._transform_deduplicate(upstream, config)
        elif node_type == "python_script_transform":
            return await self._transform_python(upstream, config)
        elif node_type == "sql_transform":
            return await self._transform_sql(upstream, config)
        elif node_type == "type_convert_transform":
            return self._transform_type_convert(upstream, config)
        elif node_type == "limit_transform":
            n = int(config.get("limit", 100))
            return upstream[:n]
        elif node_type == "flatten_transform":
            return self._transform_flatten(upstream, config)

        # ─── DESTINATIONS ──────────────────────────────────────
        elif node_type in (
            "postgres_destination", "mysql_destination", "oracle_destination", "mongodb_destination",
            "s3_destination", "csv_destination", "json_destination",
            "excel_destination", "elasticsearch_destination", "redis_destination",
            "rest_api_destination"
        ):
            return await self._execute_destination(node_type, config, upstream)

        # ─── FLOW CONTROL ──────────────────────────────────────
        elif node_type == "condition_node":
            return self._flow_condition(upstream, config)
        elif node_type == "merge_node":
            return upstream

        return upstream

    # ─── SOURCE IMPLEMENTATIONS ────────────────────────────────────────────────

    async def _execute_postgres(self, config: dict) -> list:
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=config.get("host", "localhost"),
                port=int(config.get("port", 5432)),
                database=config.get("database", ""),
                user=config.get("user", ""),
                password=config.get("password", "")
            )
            cursor = conn.cursor()
            query = config.get("query", "SELECT 1")
            cursor.execute(query)
            cols = [desc[0] for desc in cursor.description]
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return rows
        except Exception as e:
            logger.warning(f"PostgreSQL simulation mode: {e}")
            return self._mock_data(config, "postgres")

    async def _execute_mysql(self, config: dict) -> list:
        try:
            import pymysql
            conn = pymysql.connect(
                host=config.get("host", "localhost"),
                port=int(config.get("port", 3306)),
                database=config.get("database", ""),
                user=config.get("user", ""),
                password=config.get("password", ""),
                cursorclass=pymysql.cursors.DictCursor
            )
            with conn.cursor() as cursor:
                cursor.execute(config.get("query", "SELECT 1"))
                rows = cursor.fetchall()
            conn.close()
            return list(rows)
        except Exception as e:
            return self._mock_data(config, "mysql")

    async def _execute_oracle(self, config: dict) -> list:
        try:
            import oracledb

            dsn = self._build_oracle_dsn(config)
            conn = oracledb.connect(
                user=config.get("user", ""),
                password=config.get("password", ""),
                dsn=dsn,
            )
            cursor = conn.cursor()
            query = str(config.get("query", "") or "").strip()
            if not query:
                table = str(config.get("table", "") or "").strip()
                limit_raw = config.get("limit", 1000)
                try:
                    limit = max(1, min(int(limit_raw), 50000))
                except Exception:
                    limit = 1000
                if table:
                    query = f"SELECT * FROM {table} FETCH FIRST {limit} ROWS ONLY"
                else:
                    raise RuntimeError("Oracle source requires SQL query (or table name).")
            query = query.rstrip(";").strip()
            cursor.execute(query)
            cols = [desc[0] for desc in (cursor.description or [])]
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return rows
        except Exception as e:
            err = str(e)
            hint = "Check host/port/service_name(or SID), username/password, and SQL query."
            if any(code in err for code in ("ORA-12154", "ORA-12514", "ORA-12541", "DPY-6005")):
                hint = "Oracle listener/service mismatch. Verify host, port, service_name (or sid), and listener status."
            raise RuntimeError(f"Oracle connection/query failed: {err}. {hint}")

    async def _execute_mongodb(self, config: dict) -> list:
        try:
            from pymongo import MongoClient
            client = MongoClient(config.get("connection_string", "mongodb://localhost:27017"))
            db = client[config.get("database", "test")]
            collection = db[config.get("collection", "data")]
            filter_query = json.loads(config.get("filter", "{}"))
            docs = list(collection.find(filter_query).limit(int(config.get("limit", 1000))))
            for doc in docs:
                doc["_id"] = str(doc["_id"])
            return docs
        except Exception as e:
            return self._mock_data(config, "mongodb")

    async def _execute_redis(self, config: dict) -> list:
        try:
            import redis as redis_lib
            r = redis_lib.Redis(
                host=config.get("host", "localhost"),
                port=int(config.get("port", 6379)),
                password=config.get("password"),
                decode_responses=True
            )
            key_pattern = config.get("key_pattern", "*")
            keys = r.keys(key_pattern)[:100]
            return [{"key": k, "value": r.get(k)} for k in keys]
        except Exception as e:
            return self._mock_data(config, "redis")

    async def _execute_elasticsearch(self, config: dict) -> list:
        try:
            from elasticsearch import Elasticsearch
            es = Elasticsearch(config.get("hosts", ["http://localhost:9200"]))
            result = es.search(
                index=config.get("index", "*"),
                body=json.loads(config.get("query", '{"query": {"match_all": {}}}')),
                size=int(config.get("size", 100))
            )
            return [hit["_source"] for hit in result["hits"]["hits"]]
        except Exception as e:
            return self._mock_data(config, "elasticsearch")

    async def _execute_csv(self, config: dict) -> list:
        file_path = self._resolve_input_file_path(config.get("file_path", ""))
        try:
            import pandas as pd
            delimiter = config.get("delimiter", ",") or ","
            has_header = config.get("has_header", True)
            header = 0 if has_header else None
            requested_encoding = str(config.get("encoding", "utf-8") or "utf-8").strip() or "utf-8"

            candidates = [requested_encoding]
            for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1", "iso-8859-1"):
                if enc.lower() not in {c.lower() for c in candidates}:
                    candidates.append(enc)

            last_decode_err: Optional[Exception] = None
            df = None
            for enc in candidates:
                try:
                    df = pd.read_csv(
                        file_path,
                        delimiter=delimiter,
                        header=header,
                        encoding=enc,
                        low_memory=True,
                    )
                    if enc.lower() != requested_encoding.lower():
                        logger.warning(
                            f"CSV source encoding fallback used: requested='{requested_encoding}', applied='{enc}', file='{file_path}'"
                        )
                    break
                except UnicodeDecodeError as exc:
                    last_decode_err = exc
                    continue
                except Exception as exc:
                    msg = str(exc).lower()
                    if "codec can't decode" in msg or "unicode" in msg:
                        last_decode_err = exc
                        continue
                    raise

            if df is None:
                # Final lossy fallback so pipeline can continue with difficult files.
                try:
                    df = pd.read_csv(
                        file_path,
                        delimiter=delimiter,
                        header=header,
                        encoding=requested_encoding,
                        encoding_errors="replace",
                        low_memory=True,
                    )
                    logger.warning(
                        f"CSV source encoding fallback used lossy decode: requested='{requested_encoding}', file='{file_path}'"
                    )
                except TypeError:
                    # Older pandas without encoding_errors support.
                    pass
                except Exception as exc:
                    last_decode_err = exc

            if df is None and last_decode_err is not None:
                raise RuntimeError(
                    "Failed to decode CSV with configured encoding. "
                    "Try setting CSV source encoding to cp1252 or latin1."
                ) from last_decode_err
            if df is None:
                raise RuntimeError("Failed to read CSV for an unknown reason.")
            return df.to_dict(orient="records")
        except FileNotFoundError:
            raise RuntimeError(f"CSV file not found at path: {file_path!r}. Upload the file again.")
        except Exception as e:
            raise RuntimeError(f"Failed to read CSV: {e}")

    async def _execute_json(self, config: dict) -> list:
        file_path = self._resolve_input_file_path(config.get("file_path", ""))
        try:
            with open(file_path, "r", encoding="utf-8") as jf:
                payload = json.load(jf)
            json_path = str(config.get("json_path", "") or "").strip()
            selected = self._extract_json_path_value(payload, json_path)
            rows = self._rows_from_json_value(selected)
            if rows:
                # If a leaf array path was picked (e.g. daily.temperature_2m_max),
                # promote to parent object when it contains richer tabular fields.
                if json_path and self._is_single_value_rows(rows):
                    parent_path = self._parent_json_path(json_path)
                    if parent_path:
                        parent_selected = self._extract_json_path_value(payload, parent_path)
                        parent_rows = self._rows_from_json_value(parent_selected)
                        if parent_rows and not self._is_single_value_rows(parent_rows):
                            return parent_rows
                return rows
            # Fallback to root payload when path is invalid or empty-output
            rows = self._rows_from_json_value(payload)
            if rows:
                return rows
            import pandas as pd
            df = pd.read_json(file_path)
            return df.to_dict(orient="records")
        except FileNotFoundError:
            raise RuntimeError(f"JSON file not found: {file_path!r}")
        except Exception as e:
            raise RuntimeError(f"Failed to read JSON: {e}")

    async def _execute_excel(self, config: dict) -> list:
        file_path = self._resolve_input_file_path(config.get("file_path", ""))
        try:
            import pandas as pd
            sheet = config.get("sheet", 0)
            try:
                sheet = int(sheet)
            except (ValueError, TypeError):
                pass
            df = pd.read_excel(file_path, sheet_name=sheet)
            return df.to_dict(orient="records")
        except FileNotFoundError:
            raise RuntimeError(f"Excel file not found: {file_path!r}")
        except Exception as e:
            raise RuntimeError(f"Failed to read Excel: {e}")

    async def _execute_xml(self, config: dict) -> list:
        file_path = self._resolve_input_file_path(config.get("file_path", ""))
        try:
            import pandas as pd
            df = pd.read_xml(file_path)
            return df.to_dict(orient="records")
        except FileNotFoundError:
            raise RuntimeError(f"XML file not found: {file_path!r}")
        except Exception as e:
            raise RuntimeError(f"Failed to read XML: {e}")

    async def _execute_parquet(self, config: dict) -> list:
        file_path = self._resolve_input_file_path(config.get("file_path", ""))
        try:
            import pandas as pd
            df = pd.read_parquet(file_path)
            return df.to_dict(orient="records")
        except FileNotFoundError:
            raise RuntimeError(f"Parquet file not found: {file_path!r}")
        except Exception as e:
            raise RuntimeError(f"Failed to read Parquet: {e}")

    async def _execute_rest_api(self, config: dict) -> list:
        try:
            import httpx
            method = config.get("method", "GET").upper()
            url = config.get("url", "")
            headers = self._parse_request_object(config.get("headers", {}))
            params = self._parse_request_object(config.get("params", {}), allow_query_string=True)
            body_payload = self._parse_request_payload(config.get("body"))

            request_kwargs = {
                "method": method,
                "url": url,
                "timeout": 30,
            }
            if headers:
                request_kwargs["headers"] = headers
            if params:
                request_kwargs["params"] = params
            if body_payload is not None and method not in {"GET", "HEAD", "OPTIONS"}:
                request_kwargs["json"] = body_payload

            async with httpx.AsyncClient() as client:
                resp = await client.request(**request_kwargs)
                data = resp.json()
                json_path = str(config.get("json_path", "") or "").strip()
                selected = self._extract_json_path_value(data, json_path)
                rows = self._rows_from_json_value(selected)
                if rows:
                    # If a leaf array path was picked (e.g. daily.temperature_2m_max),
                    # promote to parent object when it contains richer tabular fields.
                    if json_path and self._is_single_value_rows(rows):
                        parent_path = self._parent_json_path(json_path)
                        if parent_path:
                            parent_selected = self._extract_json_path_value(data, parent_path)
                            parent_rows = self._rows_from_json_value(parent_selected)
                            if parent_rows and not self._is_single_value_rows(parent_rows):
                                return parent_rows
                    return rows
                rows = self._rows_from_json_value(data)
                if rows:
                    return rows
                return []
        except Exception as e:
            return self._mock_data(config, "rest_api")

    async def _execute_graphql(self, config: dict) -> list:
        try:
            import httpx
            headers = self._parse_request_object(config.get("headers", {}))
            variables = self._parse_request_object(config.get("variables", {}))
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    config.get("endpoint", ""),
                    json={"query": config.get("query", "{ __typename }"),
                          "variables": variables},
                    headers=headers
                )
                data = resp.json().get("data", {})
                for v in data.values():
                    if isinstance(v, list):
                        return v
                return [data]
        except Exception:
            return self._mock_data(config, "graphql")

    async def _execute_s3(self, config: dict) -> list:
        try:
            import boto3, io, pandas as pd
            s3 = boto3.client(
                "s3",
                aws_access_key_id=config.get("access_key"),
                aws_secret_access_key=config.get("secret_key"),
                region_name=config.get("region", "us-east-1")
            )
            obj = s3.get_object(Bucket=config.get("bucket"), Key=config.get("key"))
            content = obj["Body"].read()
            ext = config.get("key", "").split(".")[-1].lower()
            if ext == "csv":
                df = pd.read_csv(io.BytesIO(content))
            elif ext == "json":
                df = pd.read_json(io.BytesIO(content))
            elif ext == "parquet":
                df = pd.read_parquet(io.BytesIO(content))
            else:
                df = pd.read_csv(io.BytesIO(content))
            return df.to_dict(orient="records")
        except Exception:
            return self._mock_data(config, "s3")

    async def _execute_kafka(self, config: dict) -> list:
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                config.get("topic"),
                bootstrap_servers=config.get("bootstrap_servers", "localhost:9092"),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=5000,
                value_deserializer=lambda x: json.loads(x.decode("utf-8"))
            )
            messages = []
            for msg in consumer:
                messages.append(msg.value)
                if len(messages) >= int(config.get("max_records", 100)):
                    break
            consumer.close()
            return messages
        except Exception:
            return self._mock_data(config, "kafka")

    # ─── TRANSFORM IMPLEMENTATIONS ─────────────────────────────────────────────

    def _transform_filter(self, data: list, config: dict) -> list:
        field = config.get("field", "")
        operator = config.get("operator", "equals")
        value = config.get("value", "")
        if not field:
            return data
        result = []
        for row in data:
            v = row.get(field)
            try:
                if operator == "equals" and str(v) == str(value):
                    result.append(row)
                elif operator == "not_equals" and str(v) != str(value):
                    result.append(row)
                elif operator == "contains" and str(value).lower() in str(v).lower():
                    result.append(row)
                elif operator == "greater_than" and float(v) > float(value):
                    result.append(row)
                elif operator == "less_than" and float(v) < float(value):
                    result.append(row)
                elif operator == "is_null" and v is None:
                    result.append(row)
                elif operator == "is_not_null" and v is not None:
                    result.append(row)
            except (TypeError, ValueError):
                    pass
        return result

    def _parse_selected_fields(self, value: Any) -> List[str]:
        if isinstance(value, list):
            parts = [str(v).strip() for v in value]
        else:
            import re
            parts = [p.strip() for p in re.split(r"[,\n]", str(value or ""))]
        out: List[str] = []
        seen = set()
        for part in parts:
            if not part:
                continue
            key = part.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(part)
        return out

    def _path_variants(self, field_path: str) -> List[str]:
        import re

        raw = str(field_path or "").strip()
        if not raw:
            return []
        cached = self._path_variants_cache.get(raw)
        if cached is not None:
            return list(cached)
        normalized = self._normalize_json_path_expr(raw)
        if not normalized:
            return []

        variants: List[str] = [normalized]
        parts = [p for p in normalized.split(".") if p]
        for i in range(1, len(parts)):
            variants.append(".".join(parts[i:]))

        stripped = [re.sub(r"\[(?:\d+|\*|)\]", "", v).replace("[]", "") for v in variants]
        variants.extend(stripped)

        deduped: List[str] = []
        seen = set()
        for v in variants:
            text = str(v or "").strip(". ")
            if not text:
                continue
            if text in seen:
                continue
            seen.add(text)
            deduped.append(text)
        if len(self._path_variants_cache) > 4096:
            self._path_variants_cache.clear()
        self._path_variants_cache[raw] = list(deduped)
        return deduped

    def _extract_row_value_by_path(self, row: Any, field_path: str) -> tuple[Any, bool]:
        path = str(field_path or "").strip()
        if not path:
            return None, False
        variants = self._path_variants(path)
        if not variants:
            return None, False

        if isinstance(row, list):
            for variant in variants:
                value = self._extract_json_path_value(row, variant)
                if value is not None:
                    return value, True
            return None, False

        if not isinstance(row, dict):
            return None, False

        # Case/space-insensitive lookup map for robust field resolution
        row_key_lookup: Dict[str, Any] = {}
        for rk in row.keys():
            key_norm = str(rk or "").strip().lower()
            if key_norm and key_norm not in row_key_lookup:
                row_key_lookup[key_norm] = rk

        # Direct key hit first.
        if path in row:
            return row.get(path), True
        direct_norm = path.strip().lower()
        if direct_norm in row_key_lookup:
            return row.get(row_key_lookup[direct_norm]), True

        for variant in variants:
            if variant in row:
                return row.get(variant), True
            variant_norm = str(variant or "").strip().lower()
            if variant_norm in row_key_lookup:
                return row.get(row_key_lookup[variant_norm]), True

            # Common flattened-key variant: "a.b.c" => "a_b_c"
            underscored = variant_norm.replace(".", "_")
            if underscored in row_key_lookup:
                return row.get(row_key_lookup[underscored]), True
            value = self._extract_json_path_value(row, variant)
            if value is not None:
                return value, True

        # Common case: user selects root-qualified path (e.g. daily.time)
        # while rows already represent that nested object (key is time).
        import re
        normalized = variants[0] if variants else self._normalize_json_path_expr(path)
        if normalized and "." in normalized:
            leaf = normalized.split(".")[-1]
            leaf = re.sub(r"\[(?:\d+|\*|)\]", "", leaf).replace("[]", "").strip()
            if leaf in row:
                return row.get(leaf), True
            leaf_norm = leaf.lower()
            if leaf_norm in row_key_lookup:
                return row.get(row_key_lookup[leaf_norm]), True

        # If user picked an array container (e.g. items[]) and upstream already
        # flattened to row-level records, keep the current row instead of empty output.
        if normalized and re.search(r"\[(?:\d+|\*|)\]$", normalized):
            return row, True

        return None, False

    def _parse_custom_fields_config(self, value: Any) -> List[dict]:
        raw = value
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                return []
            try:
                raw = json.loads(text)
            except Exception:
                return []
        if not isinstance(raw, list):
            return []

        parsed: List[dict] = []
        seen = set()
        for item in raw:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or item.get("field") or "").strip()
            if not name:
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)

            mode = str(item.get("kind") or item.get("mode") or item.get("type") or "value").strip().lower()
            if mode not in {"value", "json"}:
                mode = "value"

            expression = str(item.get("expression") or item.get("expr") or "").strip()
            json_template = item.get("json_template", item.get("template"))
            enabled = bool(item.get("enabled", True))
            if not enabled:
                continue

            parsed.append(
                {
                    "name": name,
                    "mode": mode,
                    "expression": expression,
                    "json_template": json_template,
                }
            )
        return parsed

    def _to_number(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        try:
            return float(text.replace(",", ""))
        except Exception:
            return None

    def _build_expression_context(
        self,
        row: Any,
        custom_values: Dict[str, Any],
        dataset_rows: Optional[List[Any]] = None,
        row_index: Optional[int] = None,
    ) -> Dict[str, Any]:
        if isinstance(row, (dict, list)):
            row_scope: Any = row
        else:
            row_scope = {"value": row}
        row_obj = row_scope if isinstance(row_scope, dict) else {}
        group_size = len(row_scope) if isinstance(row_scope, list) else 1

        def _flatten_sequence(values: Any) -> List[Any]:
            if values is None:
                return []
            out: List[Any] = []
            stack = [values]
            while stack:
                current = stack.pop()
                if isinstance(current, (list, tuple, set)):
                    stack.extend(reversed(list(current)))
                    continue
                out.append(current)
            return out

        def _numeric_sequence(values: Any) -> List[float]:
            nums: List[float] = []
            for value in _flatten_sequence(values):
                num = self._to_number(value)
                if num is not None:
                    nums.append(num)
            return nums

        _missing = object()
        scalar_cache: Dict[str, Tuple[bool, Any]] = {}
        series_cache: Dict[str, List[Any]] = {}

        def _series_for_key(key: str) -> List[Any]:
            cached = series_cache.get(key)
            if cached is not None:
                return cached
            values: List[Any] = []
            if key and isinstance(row_scope, list):
                values = self._extract_series_from_rows(row_scope, key)
            elif key and isinstance(dataset_rows, list) and dataset_rows and not isinstance(row_scope, list):
                values = self._extract_series_from_rows(dataset_rows, key)
            series_cache[key] = values
            return values

        def _field(path: Any, default: Any = None) -> Any:
            key = str(path or "").strip()
            if not key:
                return default
            if key in custom_values:
                return custom_values.get(key)
            cached = scalar_cache.get(key, _missing)
            if cached is not _missing:
                found_cached, value_cached = cached
                if found_cached:
                    return value_cached
                return default
            # In grouped scope, field() is scalar accessor (first value),
            # while values() returns full series.
            if isinstance(row_scope, list):
                series = _series_for_key(key)
                if series:
                    scalar_cache[key] = (True, series[0])
                    return series[0]
                scalar_cache[key] = (False, None)
                return default
            value, found = self._extract_row_value_by_path(row_scope, key)
            if found:
                scalar_cache[key] = (True, value)
                return value
            scalar_cache[key] = (False, None)
            return default

        def _values(path: Any, default: Optional[List[Any]] = None) -> List[Any]:
            fallback: List[Any] = default if isinstance(default, list) else []
            key = str(path or "").strip()
            if key:
                series_values = _series_for_key(key)
                if series_values:
                    return series_values
            value = _field(path, fallback)
            if value is None:
                return fallback
            if isinstance(value, list):
                return value
            return [value]

        def _coalesce(*args: Any) -> Any:
            for arg in args:
                if arg is not None and arg != "":
                    return arg
            return None

        def _to_int(value: Any, default: int = 0) -> int:
            num = self._to_number(value)
            return int(num) if num is not None else int(default)

        def _to_float(value: Any, default: float = 0.0) -> float:
            num = self._to_number(value)
            return float(num) if num is not None else float(default)

        def _mean(values: Any) -> Optional[float]:
            nums = _numeric_sequence(values)
            if not nums:
                return None
            return sum(nums) / len(nums)

        def _sum(values: Any) -> float:
            nums = _numeric_sequence(values)
            return float(sum(nums)) if nums else 0.0

        def _min_value(*args: Any) -> Any:
            seq = args[0] if len(args) == 1 else list(args)
            flat = [v for v in _flatten_sequence(seq) if v is not None]
            if not flat:
                return None
            nums = _numeric_sequence(flat)
            if len(nums) == len(flat):
                return min(nums)
            try:
                return min(flat)
            except Exception:
                return min([str(v) for v in flat])

        def _max_value(*args: Any) -> Any:
            seq = args[0] if len(args) == 1 else list(args)
            flat = [v for v in _flatten_sequence(seq) if v is not None]
            if not flat:
                return None
            nums = _numeric_sequence(flat)
            if len(nums) == len(flat):
                return max(nums)
            try:
                return max(flat)
            except Exception:
                return max([str(v) for v in flat])

        def _count(values: Any = None) -> int:
            if values is None:
                return int(group_size)
            return len(_flatten_sequence(values))

        def _distinct(values: Any) -> List[Any]:
            seq = _flatten_sequence(values)
            out: List[Any] = []
            seen = set()
            for value in seq:
                token = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
                if token in seen:
                    continue
                seen.add(token)
                out.append(value)
            return out

        def _count_if(
            value_path: Any,
            condition_path: Any,
            expected_value: Any,
            op: Any = "eq",
            include_null_values: Any = False,
            case_sensitive: Any = False,
        ) -> int:
            import re as _re

            value_field = str(value_path or "").strip()
            condition_field = str(condition_path or "").strip()
            if not value_field or not condition_field:
                return 0

            if isinstance(row_scope, list):
                source_rows: List[Any] = row_scope
            elif isinstance(dataset_rows, list) and dataset_rows:
                source_rows = dataset_rows
            else:
                source_rows = [row_scope]

            # Backward compatibility:
            # old signature was count_if(value_path, condition_path, expected_value, include_null_values=False, case_sensitive=False)
            if isinstance(op, bool):
                sensitive = bool(include_null_values)
                keep_nulls = bool(op)
                op_name = "eq"
            else:
                sensitive = bool(case_sensitive)
                keep_nulls = bool(include_null_values)
                op_name = str(op or "eq").strip().lower()

            def _normalize_cmp(value: Any) -> Any:
                if value is None:
                    return None
                if isinstance(value, (int, float, bool)):
                    return value
                text = str(value).strip()
                return text if sensitive else text.lower()

            def _to_text(value: Any) -> str:
                if value is None:
                    return ""
                text = str(value).strip()
                return text if sensitive else text.lower()

            if isinstance(expected_value, (list, tuple, set)):
                expected_list = list(expected_value)
            else:
                expected_list = [expected_value]

            expected_tokens = {_normalize_cmp(v) for v in expected_list}
            expected_texts = [_to_text(v) for v in expected_list if v is not None]

            negate = False
            if op_name in {"not", "ne", "!=", "<>", "not_eq", "neq", "is_not"}:
                negate = True
                base_op = "eq"
            elif op_name in {"not_contains", "ncontains", "!contains", "does_not_contain"}:
                negate = True
                base_op = "contains"
            elif op_name in {"not_like", "nlike", "!like"}:
                negate = True
                base_op = "like"
            elif op_name in {"not_in", "nin"}:
                negate = True
                base_op = "in"
            elif op_name in {"not_startswith", "not_starts_with"}:
                negate = True
                base_op = "startswith"
            elif op_name in {"not_endswith", "not_ends_with"}:
                negate = True
                base_op = "endswith"
            elif op_name in {"not_regex", "not_matches"}:
                negate = True
                base_op = "regex"
            else:
                base_op = op_name

            if base_op in {"", "eq", "=", "==", "is"}:
                base_op = "eq"
            elif base_op in {"contains", "has"}:
                base_op = "contains"
            elif base_op in {"like", "sql_like"}:
                base_op = "like"
            elif base_op in {"in", "one_of"}:
                base_op = "in"
            elif base_op in {"startswith", "starts_with", "prefix"}:
                base_op = "startswith"
            elif base_op in {"endswith", "ends_with", "suffix"}:
                base_op = "endswith"
            elif base_op in {"regex", "re", "matches"}:
                base_op = "regex"
            else:
                base_op = "eq"

            regex_cache: Dict[str, Any] = {}

            def _sql_like_to_regex(pattern: str) -> Any:
                cached = regex_cache.get(pattern)
                if cached is not None:
                    return cached
                escaped = _re.escape(pattern)
                rx = "^" + escaped.replace("%", ".*").replace("_", ".") + "$"
                flags = 0 if sensitive else _re.IGNORECASE
                compiled = _re.compile(rx, flags)
                regex_cache[pattern] = compiled
                return compiled

            def _match_single(condition_value: Any) -> bool:
                cond_token = _normalize_cmp(condition_value)
                cond_text = _to_text(condition_value)

                if base_op == "eq":
                    return cond_token in expected_tokens
                if base_op == "in":
                    return cond_token in expected_tokens
                if base_op == "contains":
                    if not expected_texts:
                        return False
                    return any(exp in cond_text for exp in expected_texts)
                if base_op == "startswith":
                    if not expected_texts:
                        return False
                    return any(cond_text.startswith(exp) for exp in expected_texts)
                if base_op == "endswith":
                    if not expected_texts:
                        return False
                    return any(cond_text.endswith(exp) for exp in expected_texts)
                if base_op == "like":
                    if not expected_texts:
                        return False
                    return any(bool(_sql_like_to_regex(pat).match(cond_text)) for pat in expected_texts)
                if base_op == "regex":
                    if not expected_texts:
                        return False
                    flags = 0 if sensitive else _re.IGNORECASE
                    for pat in expected_texts:
                        try:
                            if _re.search(pat, cond_text, flags):
                                return True
                        except Exception:
                            continue
                    return False
                return cond_token in expected_tokens

            count = 0
            for src in source_rows:
                if not isinstance(src, dict):
                    continue
                condition_raw, condition_found = self._extract_row_value_by_path(src, condition_field)
                if not condition_found:
                    continue

                condition_values = _flatten_sequence(condition_raw)
                if not condition_values:
                    condition_values = [condition_raw]

                matched = any(_match_single(v) for v in condition_values)
                if negate:
                    matched = not matched
                if not matched:
                    continue

                value_raw, value_found = self._extract_row_value_by_path(src, value_field)
                if not value_found:
                    continue

                values_flat = _flatten_sequence(value_raw)
                if not values_flat:
                    if value_raw is None and keep_nulls:
                        count += 1
                    continue
                for item in values_flat:
                    if item is None and not keep_nulls:
                        continue
                    if isinstance(item, str) and item.strip() == "" and not keep_nulls:
                        continue
                    count += 1
            return count

        def _agg(path_or_values: Any, func: Any = "sum") -> Any:
            func_name = str(func or "sum").strip().lower()
            values = _values(path_or_values, []) if isinstance(path_or_values, str) else path_or_values
            if func_name in {"sum", "total"}:
                return _sum(values)
            if func_name in {"avg", "average", "mean"}:
                return _mean(values)
            if func_name in {"min", "minimum"}:
                return _min_value(values)
            if func_name in {"max", "maximum"}:
                return _max_value(values)
            if func_name in {"count", "size"}:
                return _count(values)
            if func_name in {"distinct", "unique"}:
                return _distinct(values)
            if func_name in {"first"}:
                flat = _flatten_sequence(values)
                return flat[0] if flat else None
            if func_name in {"last"}:
                flat = _flatten_sequence(values)
                return flat[-1] if flat else None
            return None

        def _std(values: Any) -> Optional[float]:
            nums = _numeric_sequence(values)
            n = len(nums)
            if n <= 1:
                return 0.0 if n == 1 else None
            mean_val = sum(nums) / n
            variance = sum((x - mean_val) ** 2 for x in nums) / n
            return math.sqrt(variance)

        def _running(values: Any, func: Any = "sum", return_series: Any = False) -> Any:
            func_name = str(func or "sum").strip().lower()
            flat = _flatten_sequence(values)
            out: List[Any] = []
            window: List[Any] = []
            for value in flat:
                window.append(value)
                if func_name in {"sum", "total"}:
                    out.append(_sum(window))
                elif func_name in {"avg", "average", "mean"}:
                    out.append(_mean(window))
                elif func_name in {"min", "minimum"}:
                    out.append(_min_value(window))
                elif func_name in {"max", "maximum"}:
                    out.append(_max_value(window))
                elif func_name in {"count", "size"}:
                    out.append(_count(window))
                elif func_name in {"std", "stdev", "stddev"}:
                    out.append(_std(window))
                else:
                    out.append(_agg(window, func_name))
            if bool(return_series):
                return out
            if row_index is not None:
                if not out:
                    return None
                idx = max(0, min(int(row_index), len(out) - 1))
                return out[idx]
            return out

        def _rolling(
            values: Any,
            window: Any = 3,
            func: Any = "mean",
            min_periods: Any = 1,
            return_series: Any = False,
        ) -> Any:
            try:
                win = int(window)
            except Exception:
                win = 3
            try:
                minp = int(min_periods)
            except Exception:
                minp = 1
            win = max(1, win)
            minp = max(1, min(minp, win))
            func_name = str(func or "mean").strip().lower()
            flat = _flatten_sequence(values)
            out: List[Any] = []
            for idx in range(len(flat)):
                start = max(0, idx - win + 1)
                chunk = flat[start: idx + 1]
                if len(chunk) < minp:
                    out.append(None)
                    continue
                if func_name in {"sum", "total"}:
                    out.append(_sum(chunk))
                elif func_name in {"avg", "average", "mean"}:
                    out.append(_mean(chunk))
                elif func_name in {"min", "minimum"}:
                    out.append(_min_value(chunk))
                elif func_name in {"max", "maximum"}:
                    out.append(_max_value(chunk))
                elif func_name in {"count", "size"}:
                    out.append(_count(chunk))
                elif func_name in {"std", "stdev", "stddev"}:
                    out.append(_std(chunk))
                else:
                    out.append(_agg(chunk, func_name))
            if bool(return_series):
                return out
            if row_index is not None:
                if not out:
                    return None
                idx = max(0, min(int(row_index), len(out) - 1))
                return out[idx]
            return out

        def _last(values: Any, default: Any = None) -> Any:
            flat = _flatten_sequence(values)
            if not flat:
                return default
            return flat[-1]

        def _group_profile(
            key_path: Any,
            value_path: Any,
            agg_func: Any = "max",
            key_name: Any = "key",
            value_name: Any = "value",
            include_null_key: Any = False,
            count_name: Any = "",
        ) -> List[Dict[str, Any]]:
            key_field = str(key_path or "").strip()
            value_field = str(value_path or "").strip()
            if not key_field or not value_field:
                return []

            if isinstance(row_scope, list):
                source_rows: List[Any] = row_scope
            elif isinstance(dataset_rows, list) and dataset_rows:
                source_rows = dataset_rows
            else:
                source_rows = [row_scope]

            buckets: Dict[str, Dict[str, Any]] = {}
            order: List[str] = []
            keep_null_key = bool(include_null_key)

            for src in source_rows:
                if not isinstance(src, dict):
                    continue
                key_value, key_found = self._extract_row_value_by_path(src, key_field)
                if not key_found:
                    continue
                if key_value is None and not keep_null_key:
                    continue
                try:
                    token = json.dumps(key_value, ensure_ascii=False, sort_keys=True, default=str)
                except Exception:
                    token = str(key_value)
                if token not in buckets:
                    buckets[token] = {"key": key_value, "values": [], "row_count": 0}
                    order.append(token)
                buckets[token]["row_count"] = int(buckets[token].get("row_count", 0)) + 1
                value, value_found = self._extract_row_value_by_path(src, value_field)
                if value_found:
                    buckets[token]["values"].append(value)

            out: List[Dict[str, Any]] = []
            group_key_name = str(key_name or "key").strip() or "key"
            group_value_name = str(value_name or "value").strip() or "value"
            txn_count_name = str(count_name or "").strip()
            func_name = str(agg_func or "max").strip().lower()
            for token in order:
                bucket = buckets[token]
                agg_value = _agg(bucket.get("values", []), func_name)
                row_obj: Dict[str, Any] = {
                    group_key_name: bucket.get("key"),
                    group_value_name: agg_value,
                }
                if txn_count_name:
                    row_obj[txn_count_name] = int(bucket.get("row_count", 0))
                out.append(row_obj)
            return out

        def _group_profile_stats(
            key_path: Any,
            value_path: Any,
            key_name: Any = "customer",
            min_name: Any = "mintime",
            max_name: Any = "maxtime",
            count_name: Any = "transaction_count",
            count_path: Any = None,
            include_null_key: Any = False,
        ) -> List[Dict[str, Any]]:
            key_field = str(key_path or "").strip()
            value_field = str(value_path or "").strip()
            if not key_field or not value_field:
                return []

            if isinstance(row_scope, list):
                source_rows: List[Any] = row_scope
            elif isinstance(dataset_rows, list) and dataset_rows:
                source_rows = dataset_rows
            else:
                source_rows = [row_scope]

            count_field = str(count_path or "").strip()
            buckets: Dict[str, Dict[str, Any]] = {}
            order: List[str] = []
            keep_null_key = bool(include_null_key)

            for src in source_rows:
                if not isinstance(src, dict):
                    continue
                key_value, key_found = self._extract_row_value_by_path(src, key_field)
                if not key_found:
                    continue
                if key_value is None and not keep_null_key:
                    continue
                try:
                    token = json.dumps(key_value, ensure_ascii=False, sort_keys=True, default=str)
                except Exception:
                    token = str(key_value)
                if token not in buckets:
                    buckets[token] = {"key": key_value, "values": [], "row_count": 0}
                    order.append(token)

                should_count = True
                if count_field:
                    count_value, count_found = self._extract_row_value_by_path(src, count_field)
                    should_count = count_found and count_value is not None
                if should_count:
                    buckets[token]["row_count"] = int(buckets[token].get("row_count", 0)) + 1

                value, value_found = self._extract_row_value_by_path(src, value_field)
                if value_found:
                    buckets[token]["values"].append(value)

            out: List[Dict[str, Any]] = []
            group_key_name = str(key_name or "customer").strip() or "customer"
            min_field_name = str(min_name or "mintime").strip() or "mintime"
            max_field_name = str(max_name or "maxtime").strip() or "maxtime"
            txn_count_name = str(count_name or "transaction_count").strip() or "transaction_count"
            for token in order:
                bucket = buckets[token]
                values = bucket.get("values", [])
                row_obj: Dict[str, Any] = {
                    group_key_name: bucket.get("key"),
                    min_field_name: _agg(values, "min"),
                    max_field_name: _agg(values, "max"),
                }
                if txn_count_name:
                    row_obj[txn_count_name] = int(bucket.get("row_count", 0))
                out.append(row_obj)
            return out

        def _group_aggregate(
            key_path: Any,
            metrics: Any,
            key_name: Any = "key",
            include_null_key: Any = False,
        ) -> List[Dict[str, Any]]:
            key_field = str(key_path or "").strip()
            if not key_field:
                return []

            if isinstance(row_scope, list):
                source_rows: List[Any] = row_scope
            elif isinstance(dataset_rows, list) and dataset_rows:
                source_rows = dataset_rows
            else:
                source_rows = [row_scope]

            def _parse_metrics_config(raw_metrics: Any) -> List[Dict[str, Any]]:
                raw = raw_metrics
                if isinstance(raw, str):
                    text = raw.strip()
                    if not text:
                        return []
                    try:
                        raw = json.loads(text)
                    except Exception:
                        raw = [part.strip() for part in text.split(",") if part.strip()]

                specs: List[Dict[str, Any]] = []
                seen_names = set()

                def _add_spec(name: str, path: str, agg: str, default_value: Any = None, has_default: bool = False):
                    out_name = str(name or "").strip()
                    if not out_name:
                        return
                    key = out_name.lower()
                    if key in seen_names:
                        return
                    seen_names.add(key)
                    spec: Dict[str, Any] = {
                        "name": out_name,
                        "path": str(path or "").strip(),
                        "agg": str(agg or "count").strip().lower(),
                    }
                    if has_default:
                        spec["default"] = default_value
                    specs.append(spec)

                if isinstance(raw, dict):
                    for out_name, conf in raw.items():
                        metric_name = str(out_name or "").strip()
                        if not metric_name:
                            continue
                        if isinstance(conf, dict):
                            metric_path = str(
                                conf.get("path")
                                or conf.get("field")
                                or conf.get("column")
                                or ""
                            ).strip()
                            metric_agg = str(
                                conf.get("agg")
                                or conf.get("func")
                                or conf.get("op")
                                or "count"
                            ).strip().lower()
                            has_default = "default" in conf
                            _add_spec(metric_name, metric_path, metric_agg, conf.get("default"), has_default)
                        elif isinstance(conf, list):
                            metric_path = str(conf[0] if len(conf) > 0 else "").strip()
                            metric_agg = str(conf[1] if len(conf) > 1 else "count").strip().lower()
                            _add_spec(metric_name, metric_path, metric_agg)
                        elif isinstance(conf, str):
                            text = conf.strip()
                            if ":" in text:
                                metric_path, metric_agg = [p.strip() for p in text.split(":", 1)]
                            else:
                                metric_path, metric_agg = "", text
                            _add_spec(metric_name, metric_path, metric_agg)
                elif isinstance(raw, list):
                    for idx, item in enumerate(raw):
                        if isinstance(item, dict):
                            metric_name = str(
                                item.get("name")
                                or item.get("alias")
                                or item.get("output")
                                or ""
                            ).strip()
                            metric_path = str(
                                item.get("path")
                                or item.get("field")
                                or item.get("column")
                                or ""
                            ).strip()
                            metric_agg = str(
                                item.get("agg")
                                or item.get("func")
                                or item.get("op")
                                or "count"
                            ).strip().lower()
                            if not metric_name:
                                if metric_path:
                                    metric_name = f"{metric_path.split('.')[-1]}_{metric_agg}"
                                else:
                                    metric_name = f"metric_{idx + 1}"
                            has_default = "default" in item
                            _add_spec(metric_name, metric_path, metric_agg, item.get("default"), has_default)
                        elif isinstance(item, str):
                            text = item.strip()
                            if not text:
                                continue
                            metric_name = ""
                            metric_path = ""
                            metric_agg = "count"
                            if "=" in text:
                                metric_name, text = [p.strip() for p in text.split("=", 1)]
                            if ":" in text:
                                metric_path, metric_agg = [p.strip() for p in text.split(":", 1)]
                            else:
                                metric_agg = text
                            if not metric_name:
                                metric_name = (
                                    f"{metric_path.split('.')[-1]}_{metric_agg}"
                                    if metric_path else
                                    f"metric_{idx + 1}"
                                )
                            _add_spec(metric_name, metric_path, metric_agg)
                return specs

            metric_specs = _parse_metrics_config(metrics)
            if not metric_specs:
                return []

            runtime_specs: List[Dict[str, Any]] = []
            for spec in metric_specs:
                runtime_specs.append(
                    {
                        "name": str(spec.get("name") or "").strip(),
                        "path": str(spec.get("path") or "").strip(),
                        "agg": str(spec.get("agg") or "count").strip().lower(),
                        "has_default": "default" in spec,
                        "default": spec.get("default"),
                    }
                )

            def _init_metric_state(agg_name: str) -> Dict[str, Any]:
                op = str(agg_name or "count").strip().lower()
                if op in {"row_count", "rows", "size"}:
                    return {"kind": "row_count", "count": 0}
                if op in {"count_non_null", "non_null_count"}:
                    return {"kind": "count_non_null", "count": 0}
                if op in {"distinct_count", "nunique", "unique_count"}:
                    return {"kind": "distinct_count", "seen": set()}
                if op in {"distinct", "unique"}:
                    return {"kind": "distinct", "seen": set(), "values": []}
                if op in {"value_counts", "count_by_value", "frequency", "freq"}:
                    return {"kind": "value_counts", "items": {}, "order": []}
                if op in {"count"}:
                    return {"kind": "count", "count": 0}
                if op in {"sum", "total"}:
                    return {"kind": "sum", "value": 0.0}
                if op in {"avg", "average", "mean"}:
                    return {"kind": "mean", "sum": 0.0, "count": 0}
                if op in {"min", "minimum"}:
                    return {"kind": "min", "has_value": False, "value": None}
                if op in {"max", "maximum"}:
                    return {"kind": "max", "has_value": False, "value": None}
                if op in {"first"}:
                    return {"kind": "first", "has_value": False, "value": None}
                if op in {"last"}:
                    return {"kind": "last", "has_value": False, "value": None}
                return {"kind": "unsupported"}

            def _pick_min(current: Any, candidate: Any) -> Any:
                if current is None:
                    return candidate
                if candidate is None:
                    return current
                current_num = self._to_number(current)
                candidate_num = self._to_number(candidate)
                if current_num is not None and candidate_num is not None:
                    return candidate if candidate_num < current_num else current
                try:
                    return candidate if candidate < current else current
                except Exception:
                    return candidate if str(candidate) < str(current) else current

            def _pick_max(current: Any, candidate: Any) -> Any:
                if current is None:
                    return candidate
                if candidate is None:
                    return current
                current_num = self._to_number(current)
                candidate_num = self._to_number(candidate)
                if current_num is not None and candidate_num is not None:
                    return candidate if candidate_num > current_num else current
                try:
                    return candidate if candidate > current else current
                except Exception:
                    return candidate if str(candidate) > str(current) else current

            buckets: Dict[str, Dict[str, Any]] = {}
            order: List[str] = []
            keep_null_key = bool(include_null_key)
            for src in source_rows:
                if not isinstance(src, dict):
                    continue
                key_value, key_found = self._extract_row_value_by_path(src, key_field)
                if not key_found:
                    continue
                if key_value is None and not keep_null_key:
                    continue
                try:
                    token = json.dumps(key_value, ensure_ascii=False, sort_keys=True, default=str)
                except Exception:
                    token = str(key_value)
                if token not in buckets:
                    buckets[token] = {
                        "key": key_value,
                        "states": [_init_metric_state(spec.get("agg", "count")) for spec in runtime_specs],
                    }
                    order.append(token)

                bucket = buckets[token]
                metric_states = bucket.get("states", [])
                row_path_cache: Dict[str, List[Any]] = {}
                for idx, spec in enumerate(runtime_specs):
                    if idx >= len(metric_states):
                        continue
                    state = metric_states[idx]
                    kind = state.get("kind")
                    if kind == "row_count":
                        state["count"] = int(state.get("count", 0)) + 1
                        continue

                    path = str(spec.get("path") or "").strip()
                    if not path:
                        continue

                    if path in row_path_cache:
                        flat_values = row_path_cache[path]
                    else:
                        raw_value, found = self._extract_row_value_by_path(src, path)
                        if not found:
                            flat_values = []
                        elif isinstance(raw_value, list):
                            flat_values = _flatten_sequence(raw_value)
                        else:
                            flat_values = _flatten_sequence([raw_value])
                        row_path_cache[path] = flat_values

                    if not flat_values:
                        continue

                    if kind == "count":
                        state["count"] = int(state.get("count", 0)) + len(flat_values)
                    elif kind == "count_non_null":
                        current = int(state.get("count", 0))
                        current += len([v for v in flat_values if v is not None and str(v).strip() != ""])
                        state["count"] = current
                    elif kind == "distinct_count":
                        seen = state.get("seen")
                        if not isinstance(seen, set):
                            seen = set()
                            state["seen"] = seen
                        for value in flat_values:
                            token_value = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
                            seen.add(token_value)
                    elif kind == "distinct":
                        seen = state.get("seen")
                        values_out = state.get("values")
                        if not isinstance(seen, set):
                            seen = set()
                            state["seen"] = seen
                        if not isinstance(values_out, list):
                            values_out = []
                            state["values"] = values_out
                        for value in flat_values:
                            token_value = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
                            if token_value in seen:
                                continue
                            seen.add(token_value)
                            values_out.append(value)
                    elif kind == "value_counts":
                        items = state.get("items")
                        order_tokens = state.get("order")
                        if not isinstance(items, dict):
                            items = {}
                            state["items"] = items
                        if not isinstance(order_tokens, list):
                            order_tokens = []
                            state["order"] = order_tokens
                        for value in flat_values:
                            if value is None:
                                continue
                            token_value = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
                            current = items.get(token_value)
                            if not isinstance(current, dict):
                                items[token_value] = {"value": value, "count": 1}
                                order_tokens.append(token_value)
                            else:
                                current["count"] = int(current.get("count", 0)) + 1
                    elif kind == "sum":
                        total = float(state.get("value", 0.0))
                        for value in flat_values:
                            num = self._to_number(value)
                            if num is not None:
                                total += float(num)
                        state["value"] = total
                    elif kind == "mean":
                        total = float(state.get("sum", 0.0))
                        count = int(state.get("count", 0))
                        for value in flat_values:
                            num = self._to_number(value)
                            if num is None:
                                continue
                            total += float(num)
                            count += 1
                        state["sum"] = total
                        state["count"] = count
                    elif kind == "min":
                        current_value = state.get("value")
                        has_value = bool(state.get("has_value", False))
                        for value in flat_values:
                            if value is None:
                                continue
                            current_value = value if not has_value else _pick_min(current_value, value)
                            has_value = True
                        state["value"] = current_value
                        state["has_value"] = has_value
                    elif kind == "max":
                        current_value = state.get("value")
                        has_value = bool(state.get("has_value", False))
                        for value in flat_values:
                            if value is None:
                                continue
                            current_value = value if not has_value else _pick_max(current_value, value)
                            has_value = True
                        state["value"] = current_value
                        state["has_value"] = has_value
                    elif kind == "first":
                        if not bool(state.get("has_value", False)):
                            state["value"] = flat_values[0]
                            state["has_value"] = True
                    elif kind == "last":
                        state["value"] = flat_values[-1]
                        state["has_value"] = True

            out: List[Dict[str, Any]] = []
            group_key_name = str(key_name or "key").strip() or "key"
            for token in order:
                bucket = buckets[token]
                metric_states = bucket.get("states", [])
                row_obj: Dict[str, Any] = {group_key_name: bucket.get("key")}
                for idx, spec in enumerate(runtime_specs):
                    if idx >= len(metric_states):
                        continue
                    state = metric_states[idx]
                    kind = state.get("kind")
                    metric_value: Any = None
                    if kind in {"row_count", "count", "count_non_null"}:
                        metric_value = int(state.get("count", 0))
                    elif kind == "distinct_count":
                        seen = state.get("seen")
                        metric_value = len(seen) if isinstance(seen, set) else 0
                    elif kind == "distinct":
                        values_out = state.get("values")
                        metric_value = values_out if isinstance(values_out, list) else []
                    elif kind == "value_counts":
                        items = state.get("items")
                        order_tokens = state.get("order")
                        if isinstance(items, dict) and isinstance(order_tokens, list):
                            metric_value = [
                                {
                                    "value": items[token].get("value"),
                                    "count": int(items[token].get("count", 0)),
                                }
                                for token in order_tokens
                                if token in items
                            ]
                        else:
                            metric_value = []
                    elif kind == "sum":
                        metric_value = float(state.get("value", 0.0))
                    elif kind == "mean":
                        count = int(state.get("count", 0))
                        metric_value = (float(state.get("sum", 0.0)) / count) if count > 0 else None
                    elif kind in {"min", "max", "first", "last"}:
                        metric_value = state.get("value") if bool(state.get("has_value", False)) else None

                    if metric_value is None and bool(spec.get("has_default")):
                        metric_value = spec.get("default")
                    row_obj[str(spec.get("name"))] = metric_value
                out.append(row_obj)
            return out
        def _running_sum(values: Any) -> List[Any]:
            return _running(values, "sum")

        def _running_mean(values: Any) -> List[Any]:
            return _running(values, "mean")

        def _running_min(values: Any) -> List[Any]:
            return _running(values, "min")

        def _running_max(values: Any) -> List[Any]:
            return _running(values, "max")

        def _running_count(values: Any) -> List[Any]:
            return _running(values, "count")

        def _running_std(values: Any) -> List[Any]:
            return _running(values, "std")

        def _rolling_sum(values: Any, window: Any = 3, min_periods: Any = 1) -> List[Any]:
            return _rolling(values, window, "sum", min_periods)

        def _rolling_mean(values: Any, window: Any = 3, min_periods: Any = 1) -> List[Any]:
            return _rolling(values, window, "mean", min_periods)

        def _rolling_min(values: Any, window: Any = 3, min_periods: Any = 1) -> List[Any]:
            return _rolling(values, window, "min", min_periods)

        def _rolling_max(values: Any, window: Any = 3, min_periods: Any = 1) -> List[Any]:
            return _rolling(values, window, "max", min_periods)

        def _rolling_count(values: Any, window: Any = 3, min_periods: Any = 1) -> List[Any]:
            return _rolling(values, window, "count", min_periods)

        def _rolling_std(values: Any, window: Any = 3, min_periods: Any = 1) -> List[Any]:
            return _rolling(values, window, "std", min_periods)

        def _running_all(values: Any, func: Any = "sum") -> List[Any]:
            series = _running(values, func, True)
            return series if isinstance(series, list) else ([series] if series is not None else [])

        def _rolling_all(values: Any, window: Any = 3, func: Any = "mean", min_periods: Any = 1) -> List[Any]:
            series = _rolling(values, window, func, min_periods, True)
            return series if isinstance(series, list) else ([series] if series is not None else [])

        def _if(cond: Any, yes: Any, no: Any) -> Any:
            return yes if bool(cond) else no

        def _contains(haystack: Any, needle: Any) -> bool:
            if haystack is None:
                return False
            return str(needle or "") in str(haystack)

        context: Dict[str, Any] = {
            "field": _field,
            "get": _field,
            "values": _values,
            "coalesce": _coalesce,
            "if_": _if,
            "iff": _if,
            "contains": _contains,
            "upper": lambda v: None if v is None else str(v).upper(),
            "lower": lambda v: None if v is None else str(v).lower(),
            "title": lambda v: None if v is None else str(v).title(),
            "trim": lambda v: None if v is None else str(v).strip(),
            "length": lambda v: len(v) if v is not None else 0,
            "int": _to_int,
            "float": _to_float,
            "str": lambda v: "" if v is None else str(v),
            "bool": lambda v: bool(v),
            "round": round,
            "abs": abs,
            "min": _min_value,
            "max": _max_value,
            "sum": _sum,
            "mean": _mean,
            "count": _count,
            "count_if": _count_if,
            "distinct": _distinct,
            "agg": _agg,
            "std": _std,
            "running": _running,
            "rolling": _rolling,
            "running_all": _running_all,
            "rolling_all": _rolling_all,
            "last": _last,
            "group_profile": _group_profile,
            "group_objects": _group_profile,
            "group_profile_stats": _group_profile_stats,
            "group_profile_minmax": _group_profile_stats,
            "group_aggregate": _group_aggregate,
            "group_metrics": _group_aggregate,
            "running_sum": _running_sum,
            "running_mean": _running_mean,
            "running_min": _running_min,
            "running_max": _running_max,
            "running_count": _running_count,
            "running_std": _running_std,
            "rolling_sum": _rolling_sum,
            "rolling_mean": _rolling_mean,
            "rolling_min": _rolling_min,
            "rolling_max": _rolling_max,
            "rolling_count": _rolling_count,
            "rolling_std": _rolling_std,
            "cumulative_sum": _running_sum,
            "cumulative_mean": _running_mean,
            "pow": pow,
            "sqrt": lambda v: math.sqrt(self._to_float(v, 0.0)),
            "log": lambda v, base=math.e: math.log(max(self._to_float(v, 0.0), 1e-12), base),
            "exp": lambda v: math.exp(self._to_float(v, 0.0)),
            "ceil": lambda v: math.ceil(self._to_float(v, 0.0)),
            "floor": lambda v: math.floor(self._to_float(v, 0.0)),
            "sin": lambda v: math.sin(self._to_float(v, 0.0)),
            "cos": lambda v: math.cos(self._to_float(v, 0.0)),
            "tan": lambda v: math.tan(self._to_float(v, 0.0)),
            "pi": math.pi,
            "e": math.e,
            "array": lambda *args: list(args),
            "obj": lambda **kwargs: kwargs,
            "json_parse": lambda text: json.loads(str(text or "")),
            "json_dump": lambda value: json.dumps(value, ensure_ascii=False),
            "now": lambda: datetime.utcnow().isoformat(),
            "null": None,
            "None": None,
            "true": True,
            "false": False,
            "True": True,
            "False": False,
            "group_size": group_size,
        }

        # Direct field references for identifier-safe names.
        for k, v in row_obj.items():
            if isinstance(k, str) and k.isidentifier():
                context[k] = v
        if isinstance(row_scope, list) and row_scope:
            first = row_scope[0]
            if isinstance(first, dict):
                for k, v in first.items():
                    if isinstance(k, str) and k.isidentifier() and k not in context:
                        context[k] = v
        for k, v in custom_values.items():
            if isinstance(k, str) and k.isidentifier():
                context[k] = v
            context[k] = v
        return context

    def _eval_expression_ast(self, node: ast.AST, context: Dict[str, Any], depth: int = 0) -> Any:
        if depth > 80:
            raise RuntimeError("Expression too complex")

        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            return context.get(node.id)
        if isinstance(node, ast.List):
            return [self._eval_expression_ast(el, context, depth + 1) for el in node.elts]
        if isinstance(node, ast.Tuple):
            return tuple(self._eval_expression_ast(el, context, depth + 1) for el in node.elts)
        if isinstance(node, ast.Dict):
            return {
                self._eval_expression_ast(k, context, depth + 1): self._eval_expression_ast(v, context, depth + 1)
                for k, v in zip(node.keys, node.values)
            }
        if isinstance(node, ast.Subscript):
            target = self._eval_expression_ast(node.value, context, depth + 1)
            if isinstance(node.slice, ast.Slice):
                lower = self._eval_expression_ast(node.slice.lower, context, depth + 1) if node.slice.lower else None
                upper = self._eval_expression_ast(node.slice.upper, context, depth + 1) if node.slice.upper else None
                step = self._eval_expression_ast(node.slice.step, context, depth + 1) if node.slice.step else None
                return target[slice(lower, upper, step)]
            key = self._eval_expression_ast(node.slice, context, depth + 1)
            return target[key]
        if isinstance(node, ast.UnaryOp):
            value = self._eval_expression_ast(node.operand, context, depth + 1)
            if isinstance(node.op, ast.Not):
                return not bool(value)
            if isinstance(node.op, ast.USub):
                return -(value or 0)
            if isinstance(node.op, ast.UAdd):
                return +(value or 0)
            raise RuntimeError("Unsupported unary operator")
        if isinstance(node, ast.BinOp):
            left = self._eval_expression_ast(node.left, context, depth + 1)
            right = self._eval_expression_ast(node.right, context, depth + 1)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left / right
            if isinstance(node.op, ast.FloorDiv):
                return left // right
            if isinstance(node.op, ast.Mod):
                return left % right
            if isinstance(node.op, ast.Pow):
                return left ** right
            raise RuntimeError("Unsupported binary operator")
        if isinstance(node, ast.BoolOp):
            if isinstance(node.op, ast.And):
                out = True
                for v in node.values:
                    out = self._eval_expression_ast(v, context, depth + 1)
                    if not out:
                        return out
                return out
            if isinstance(node.op, ast.Or):
                for v in node.values:
                    out = self._eval_expression_ast(v, context, depth + 1)
                    if out:
                        return out
                return out
            raise RuntimeError("Unsupported boolean operator")
        if isinstance(node, ast.Compare):
            left = self._eval_expression_ast(node.left, context, depth + 1)
            for op, comparator in zip(node.ops, node.comparators):
                right = self._eval_expression_ast(comparator, context, depth + 1)
                if isinstance(op, ast.Eq):
                    ok = left == right
                elif isinstance(op, ast.NotEq):
                    ok = left != right
                elif isinstance(op, ast.Gt):
                    ok = left > right
                elif isinstance(op, ast.GtE):
                    ok = left >= right
                elif isinstance(op, ast.Lt):
                    ok = left < right
                elif isinstance(op, ast.LtE):
                    ok = left <= right
                elif isinstance(op, ast.In):
                    ok = left in right
                elif isinstance(op, ast.NotIn):
                    ok = left not in right
                else:
                    raise RuntimeError("Unsupported comparison operator")
                if not ok:
                    return False
                left = right
            return True
        if isinstance(node, ast.IfExp):
            cond = self._eval_expression_ast(node.test, context, depth + 1)
            branch = node.body if cond else node.orelse
            return self._eval_expression_ast(branch, context, depth + 1)
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                fn_name = node.func.id
                fn = context.get(fn_name)
                if fn is None:
                    fn = context.get(fn_name.lower())
            else:
                raise RuntimeError("Only named function calls are allowed")
            if not callable(fn):
                raise RuntimeError("Function not allowed")
            args = [self._eval_expression_ast(arg, context, depth + 1) for arg in node.args]
            kwargs = {
                kw.arg: self._eval_expression_ast(kw.value, context, depth + 1)
                for kw in node.keywords
                if kw.arg
            }
            return fn(*args, **kwargs)

        raise RuntimeError(f"Unsupported expression element: {type(node).__name__}")

    def _rewrite_series_expression(self, parsed: ast.AST) -> ast.AST:
        series_fns = {
            "running",
            "rolling",
            "running_sum",
            "running_mean",
            "running_min",
            "running_max",
            "running_count",
            "running_std",
            "rolling_sum",
            "rolling_mean",
            "rolling_min",
            "rolling_max",
            "rolling_count",
            "rolling_std",
            "cumulative_sum",
            "cumulative_mean",
        }
        aggregate_fns = {
            "sum",
            "mean",
            "count",
            "distinct",
            "std",
            "stdev",
            "stddev",
            "agg",
            "min",
            "max",
        }

        class _SeriesArgRewrite(ast.NodeTransformer):
            def visit_Call(self, node: ast.Call) -> Any:
                node = self.generic_visit(node)
                if isinstance(node.func, ast.Name):
                    fn_name = str(node.func.id or "").strip().lower()
                    if fn_name in series_fns and node.args:
                        first = node.args[0]
                        if (
                            isinstance(first, ast.Call)
                            and isinstance(first.func, ast.Name)
                            and str(first.func.id or "").strip().lower() in {"field", "get"}
                        ):
                            first.func.id = "values"
                    elif fn_name in aggregate_fns and node.args:
                        # Keep scalar behavior for min/max(a,b), rewrite only single-arg calls.
                        if fn_name in {"min", "max"} and len(node.args) != 1:
                            return node
                        first = node.args[0]
                        if (
                            isinstance(first, ast.Call)
                            and isinstance(first.func, ast.Name)
                            and str(first.func.id or "").strip().lower() in {"field", "get"}
                        ):
                            first.func.id = "values"
                return node

        transformed = _SeriesArgRewrite().visit(parsed)
        ast.fix_missing_locations(transformed)
        return transformed

    def _evaluate_custom_expression(
        self,
        expression: str,
        row: Any,
        custom_values: Dict[str, Any],
        dataset_rows: Optional[List[Any]] = None,
        row_index: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Any:
        expr = str(expression or "").strip()
        if expr.startswith("="):
            expr = expr[1:].strip()
        if not expr:
            return None

        eval_context = context or self._build_expression_context(
            row,
            custom_values,
            dataset_rows=dataset_rows,
            row_index=row_index,
        )
        cached_node = self._expr_ast_cache.get(expr)
        if cached_node is None:
            parsed = ast.parse(expr, mode="eval")
            rewritten = self._rewrite_series_expression(parsed)
            cached_node = rewritten.body
            if len(self._expr_ast_cache) > 8192:
                self._expr_ast_cache.clear()
            self._expr_ast_cache[expr] = cached_node
        return self._eval_expression_ast(cached_node, eval_context)

    def _evaluate_json_template(
        self,
        template_value: Any,
        row: Any,
        custom_values: Dict[str, Any],
        dataset_rows: Optional[List[Any]] = None,
        row_index: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Any:
        if template_value is None:
            return None

        obj = template_value
        if isinstance(template_value, str):
            text = template_value.strip()
            if not text:
                return None
            cached_template = self._json_template_cache.get(text)
            if cached_template is None:
                cached_template = json.loads(text)
                if len(self._json_template_cache) > 2048:
                    self._json_template_cache.clear()
                self._json_template_cache[text] = cached_template
            obj = cached_template

        eval_context = context or self._build_expression_context(
            row,
            custom_values,
            dataset_rows=dataset_rows,
            row_index=row_index,
        )

        def walk(value: Any) -> Any:
            if isinstance(value, str):
                text = value.strip()
                if text.startswith("="):
                    return self._evaluate_custom_expression(
                        text[1:],
                        row,
                        custom_values,
                        dataset_rows=dataset_rows,
                        row_index=row_index,
                        context=eval_context,
                    )
                return value
            if isinstance(value, list):
                return [walk(v) for v in value]
            if isinstance(value, dict):
                return {k: walk(v) for k, v in value.items()}
            return value

        return walk(obj)

    def _transform_custom_fields(self, data: list, config: dict, custom_specs: List[dict]) -> list:
        include_source = bool(config.get("custom_include_source_fields", True))
        primary_key_field = str(
            config.get("custom_primary_key_field")
            or config.get("custom_group_by_field")
            or ""
        ).strip()
        result: list = []
        warning_count = 0
        base_rows: List[Dict[str, Any]] = [
            row if isinstance(row, dict) else {"value": row}
            for row in data
        ]
        grouped_rows: List[Dict[str, Any]] = []
        if primary_key_field:
            buckets: Dict[str, Dict[str, Any]] = {}
            bucket_order: List[str] = []
            for base_row in base_rows:
                pk_value, pk_found = self._extract_row_value_by_path(base_row, primary_key_field)
                group_value = pk_value if pk_found else None
                try:
                    bucket_key = json.dumps(group_value, ensure_ascii=False, sort_keys=True, default=str)
                except Exception:
                    bucket_key = str(group_value)
                if bucket_key not in buckets:
                    buckets[bucket_key] = {"group_value": group_value, "rows": []}
                    bucket_order.append(bucket_key)
                buckets[bucket_key]["rows"].append(base_row)
            grouped_rows = [buckets[key] for key in bucket_order]
        else:
            grouped_rows = [
                {"group_value": None, "rows": [row], "row_index": idx}
                for idx, row in enumerate(base_rows)
            ]

        for row_idx, group in enumerate(grouped_rows):
            rows = group.get("rows") if isinstance(group, dict) else []
            if not isinstance(rows, list) or not rows:
                continue
            eval_scope: Any = rows if primary_key_field else rows[0]
            eval_row_index = None if primary_key_field else group.get("row_index")
            eval_dataset_rows = rows if primary_key_field else base_rows
            custom_values: Dict[str, Any] = {}

            # Multi-pass evaluation so custom fields can reference other custom fields
            # regardless of UI order (e.g., JSON template referencing a later expression field).
            max_passes = 3 if len(custom_specs) > 1 else 1
            for _pass_idx in range(max_passes):
                changed = False
                eval_context = self._build_expression_context(
                    eval_scope,
                    custom_values,
                    dataset_rows=eval_dataset_rows,
                    row_index=eval_row_index,
                )
                for spec in custom_specs:
                    name = str(spec.get("name") or "").strip()
                    if not name:
                        continue
                    mode = str(spec.get("mode") or "value").lower()
                    expression = str(spec.get("expression") or "")
                    template = spec.get("json_template")

                    try:
                        if mode == "json":
                            value = self._evaluate_json_template(
                                template,
                                eval_scope,
                                custom_values,
                                dataset_rows=eval_dataset_rows,
                                row_index=eval_row_index,
                                context=eval_context,
                            )
                        else:
                            value = self._evaluate_custom_expression(
                                expression,
                                eval_scope,
                                custom_values,
                                dataset_rows=eval_dataset_rows,
                                row_index=eval_row_index,
                                context=eval_context,
                            )
                    except Exception as exc:
                        value = None
                        if warning_count < 5:
                            logger.warning(
                                f"Custom field '{name}' evaluation failed at row {row_idx + 1}: {exc}"
                            )
                        warning_count += 1

                    prev_value = custom_values.get(name, None)
                    if prev_value != value:
                        changed = True
                    custom_values[name] = value
                    eval_context[name] = value
                    if isinstance(name, str) and name.isidentifier():
                        eval_context[name] = value
                if not changed:
                    break

            representative_row = rows[0] if isinstance(rows[0], dict) else {"value": rows[0]}
            out_row = dict(representative_row) if include_source else {}
            if primary_key_field and primary_key_field not in custom_values:
                out_row[primary_key_field] = group.get("group_value") if isinstance(group, dict) else None
            out_row.update(custom_values)
            result.append(out_row)

        return result

    def _transform_map(self, data: list, config: dict) -> list:
        custom_specs = self._parse_custom_fields_config(config.get("custom_fields"))
        if custom_specs:
            return self._transform_custom_fields(data, config, custom_specs)

        fields = self._parse_selected_fields(config.get("fields", ""))
        if not fields:
            return data

        result = []
        matched_count = 0
        for row in data:
            out_row = {}
            for field in fields:
                value, found = self._extract_row_value_by_path(row, field)
                if found:
                    out_row[field] = value
                    matched_count += 1
            result.append(out_row)
        if matched_count == 0 and data:
            logger.warning("Select Fields matched zero fields; returning input rows unchanged")
            return data
        return result

    def _transform_rename(self, data: list, config: dict) -> list:
        mappings = {}
        for line in config.get("mappings", "").splitlines():
            if ":" in line:
                old, new = line.split(":", 1)
                mappings[old.strip()] = new.strip()
        if not mappings:
            return data
        result = []
        for row in data:
            new_row = {}
            for k, v in row.items():
                new_row[mappings.get(k, k)] = v
            result.append(new_row)
        return result

    def _transform_aggregate(self, data: list, config: dict) -> list:
        import pandas as pd
        if not data:
            return []
        df = pd.DataFrame(data)
        group_by = [f.strip() for f in config.get("group_by", "").split(",") if f.strip()]
        agg_field = config.get("agg_field", "")
        agg_func = config.get("agg_func", "sum")
        if group_by and agg_field and agg_field in df.columns:
            valid_groups = [g for g in group_by if g in df.columns]
            if valid_groups:
                result = df.groupby(valid_groups)[agg_field].agg(agg_func).reset_index()
                return result.to_dict(orient="records")
        return data

    def _transform_join(
        self,
        data: list,
        config: dict,
        incoming_by_source: Optional[Dict[str, list]] = None,
        incoming_order: Optional[List[str]] = None,
    ) -> list:
        import pandas as pd

        by_source = incoming_by_source or {}
        order = [sid for sid in (incoming_order or []) if sid in by_source]

        def _dict_rows(rows: Any) -> list:
            if not isinstance(rows, list):
                return []
            return [row for row in rows if isinstance(row, dict)]

        # Resolve left/right sources from config when provided, else pick first two incoming branches.
        left_source = str(config.get("left_source_node") or "").strip()
        right_source = str(config.get("right_source_node") or "").strip()

        available_sources = order or list(by_source.keys())
        if not left_source or left_source not in by_source:
            left_source = available_sources[0] if available_sources else ""
        if not right_source or right_source not in by_source or right_source == left_source:
            right_source = next((sid for sid in available_sources if sid != left_source), "")

        left_rows = _dict_rows(by_source.get(left_source, []))
        right_rows = _dict_rows(by_source.get(right_source, []))

        # Fallback for legacy/single-input join configs.
        if not left_rows and not right_rows:
            return data

        if not left_rows or not right_rows:
            raise RuntimeError("Join transform requires two connected source branches with tabular rows.")

        left_key = str(config.get("left_key") or "").strip()
        right_key = str(config.get("right_key") or "").strip()
        if not left_key or not right_key:
            raise RuntimeError("Join transform requires both 'left_key' and 'right_key'.")

        join_type_raw = str(config.get("join_type") or "inner").strip().lower()
        join_map = {
            "inner": "inner",
            "left": "left",
            "right": "right",
            "full": "outer",
            "outer": "outer",
        }
        join_type = join_map.get(join_type_raw, "inner")

        left_suffix = str(config.get("left_suffix") or "_left")
        right_suffix = str(config.get("right_suffix") or "_right")

        left_df = pd.DataFrame(left_rows)
        right_df = pd.DataFrame(right_rows)

        if left_key not in left_df.columns:
            raise RuntimeError(f"Left key '{left_key}' not found in left source fields.")
        if right_key not in right_df.columns:
            raise RuntimeError(f"Right key '{right_key}' not found in right source fields.")

        joined = left_df.merge(
            right_df,
            how=join_type,
            left_on=left_key,
            right_on=right_key,
            suffixes=(left_suffix, right_suffix),
            sort=False,
        )
        joined = joined.astype(object).where(pd.notna(joined), None)
        return joined.to_dict(orient="records")

    def _transform_sort(self, data: list, config: dict) -> list:
        field = config.get("field", "")
        order = config.get("order", "asc")
        if not field:
            return data
        return sorted(data, key=lambda x: x.get(field, ""), reverse=(order == "desc"))

    def _transform_deduplicate(self, data: list, config: dict) -> list:
        key_field = config.get("key_field", "")
        seen = set()
        result = []
        for row in data:
            key = row.get(key_field) if key_field else json.dumps(row, sort_keys=True)
            if key not in seen:
                seen.add(key)
                result.append(row)
        return result

    async def _transform_python(self, data: list, config: dict) -> list:
        script = config.get("script", "output = input_data")
        local_vars = {"input_data": data, "output": data}
        exec(script, {"json": json, "__builtins__": __builtins__}, local_vars)
        return local_vars.get("output", data)

    async def _transform_sql(self, data: list, config: dict) -> list:
        try:
            import pandas as pd
            from pandasql import sqldf
            df = pd.DataFrame(data)
            query = config.get("query", "SELECT * FROM df")
            result = sqldf(query, {"df": df})
            return result.to_dict(orient="records")
        except Exception:
            return data

    def _transform_type_convert(self, data: list, config: dict) -> list:
        field = config.get("field", "")
        target_type = config.get("target_type", "string")
        converters = {"string": str, "integer": int, "float": float, "boolean": bool}
        conv = converters.get(target_type, str)
        result = []
        for row in data:
            new_row = dict(row)
            if field in new_row:
                try:
                    new_row[field] = conv(new_row[field])
                except (ValueError, TypeError):
                    pass
            result.append(new_row)
        return result

    def _transform_flatten(self, data: list, config: dict) -> list:
        def flatten(d, parent_key="", sep="_"):
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(flatten(v, new_key, sep).items())
                else:
                    items.append((new_key, v))
            return dict(items)
        return [flatten(row) for row in data]

    def _flow_condition(self, data: list, config: dict) -> list:
        return self._transform_filter(data, config)

    # ─── PATH RESOLVER ─────────────────────────────────────────────────────────

    def _resolve_output_path(self, raw_path: str, default_ext: str) -> str:
        """
        Resolve the output path for a destination node.

        Rules:
        - Empty / None        → write to backend/outputs/<timestamp><ext>
        - __local__://…       → browser picked a local folder; write to backend/outputs/<filename>
                                 (the frontend will then copy it to the local folder via File System Access API)
        - Absolute path (/…)  → use as-is (server must have write permission)
        - Relative path       → treat basename only, write to backend/outputs/<basename>
        """
        import os
        from datetime import datetime

        OUTPUTS_DIR = os.path.join(os.path.dirname(__file__), "outputs")
        os.makedirs(OUTPUTS_DIR, exist_ok=True)

        if not raw_path:
            fname = f"output_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}{default_ext}"
            return os.path.join(OUTPUTS_DIR, fname)

        if raw_path.startswith("__local__://"):
            # Extract just the filename the browser chose
            fname = raw_path.replace("__local__://", "").rsplit("/", 1)[-1]
            if not fname:
                fname = f"output_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}{default_ext}"
            return os.path.join(OUTPUTS_DIR, fname)

        if os.path.isabs(raw_path):
            # Absolute server path — use directly
            os.makedirs(os.path.dirname(raw_path) or OUTPUTS_DIR, exist_ok=True)
            return raw_path

        # Relative path — use just the basename inside outputs/
        fname = os.path.basename(raw_path) or f"output_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}{default_ext}"
        return os.path.join(OUTPUTS_DIR, fname)

    # ─── DESTINATION IMPLEMENTATIONS ──────────────────────────────────────────

    async def _execute_destination(self, node_type: str, config: dict, data: list) -> list:
        """Load data into destination systems."""
        import pandas as pd, os
        logger.info(f"Loading {len(data)} rows to {node_type}")

        if not data:
            return [{"status": "loaded", "rows": 0, "destination": node_type, "note": "No data to write"}]

        df = pd.DataFrame(data)

        # ── File destinations ───────────────────────────────────────────────
        if node_type == "csv_destination":
            out_path = self._resolve_output_path(config.get("file_path", ""), ".csv")
            delimiter = config.get("delimiter", ",") or ","
            df.to_csv(out_path, index=False, sep=delimiter)
            logger.info(f"✅ CSV written: {out_path} ({len(df)} rows)")
            return [{"status": "written", "rows": len(df), "path": out_path}]

        elif node_type == "json_destination":
            out_path = self._resolve_output_path(config.get("file_path", ""), ".json")
            orient = config.get("orient", "records") or "records"
            df.to_json(out_path, orient=orient, indent=2, force_ascii=False)
            logger.info(f"✅ JSON written: {out_path} ({len(df)} rows)")
            return [{"status": "written", "rows": len(df), "path": out_path}]

        elif node_type == "excel_destination":
            raw = config.get("file_path", "")
            if raw and not raw.startswith("__local__://") and not raw.endswith((".xlsx", ".xls")):
                raw += ".xlsx"
            out_path = self._resolve_output_path(raw, ".xlsx")
            sheet = config.get("sheet", "Sheet1") or "Sheet1"
            df.to_excel(out_path, index=False, sheet_name=str(sheet))
            logger.info(f"✅ Excel written: {out_path} ({len(df)} rows)")
            return [{"status": "written", "rows": len(df), "path": out_path}]

        # ── Database destinations ───────────────────────────────────────────
        elif node_type == "postgres_destination":
            return await self._dest_postgres(config, df)

        elif node_type == "mysql_destination":
            return await self._dest_mysql(config, df)

        elif node_type == "oracle_destination":
            return await self._dest_oracle(config, df)

        elif node_type == "mongodb_destination":
            return await self._dest_mongodb(config, data)

        elif node_type == "elasticsearch_destination":
            return await self._dest_elasticsearch(config, data)

        elif node_type == "redis_destination":
            return await self._dest_redis(config, data)

        elif node_type == "s3_destination":
            return await self._dest_s3(config, df)

        elif node_type == "rest_api_destination":
            return await self._dest_rest_api(config, data)

        # Fallback
        logger.warning(f"No handler for destination {node_type}, skipping write")
        return [{"status": "skipped", "rows": len(data), "destination": node_type}]

    async def _dest_postgres(self, config: dict, df) -> list:
        try:
            from sqlalchemy import create_engine
            url = (f"postgresql+psycopg2://{config.get('user','')}:{config.get('password','')}@"
                   f"{config.get('host','localhost')}:{config.get('port', 5432)}/{config.get('database','')}")
            engine = create_engine(url)
            table = config.get("table", "etl_output")
            mode = "replace" if config.get("if_exists", "append") == "replace" else "append"
            df.to_sql(table, engine, if_exists=mode, index=False)
            return [{"status": "loaded", "rows": len(df), "table": table}]
        except Exception as e:
            raise RuntimeError(f"PostgreSQL write failed: {e}")

    async def _dest_mysql(self, config: dict, df) -> list:
        try:
            from sqlalchemy import create_engine
            url = (f"mysql+pymysql://{config.get('user','')}:{config.get('password','')}@"
                   f"{config.get('host','localhost')}:{config.get('port', 3306)}/{config.get('database','')}")
            engine = create_engine(url)
            table = config.get("table", "etl_output")
            mode = "replace" if config.get("if_exists", "append") == "replace" else "append"
            df.to_sql(table, engine, if_exists=mode, index=False)
            return [{"status": "loaded", "rows": len(df), "table": table}]
        except Exception as e:
            raise RuntimeError(f"MySQL write failed: {e}")

    def _build_oracle_dsn(self, config: dict) -> str:
        dsn = str(config.get("dsn", "")).strip()
        if dsn:
            return dsn

        host = str(config.get("host", "localhost")).strip() or "localhost"
        port = int(config.get("port", 1521))
        service_name = str(config.get("service_name", "") or config.get("database", "")).strip()
        sid = str(config.get("sid", "")).strip()

        if service_name:
            return f"{host}:{port}/{service_name}"
        if sid:
            return f"{host}:{port}:{sid}"
        return f"{host}:{port}/ORCLCDB"

    def _build_sqlalchemy_oracle_url(self, config: dict) -> str:
        user = quote_plus(str(config.get("user", "")))
        password = quote_plus(str(config.get("password", "")))

        dsn = str(config.get("dsn", "")).strip()
        if dsn:
            return f"oracle+oracledb://{user}:{password}@{dsn}"

        host = str(config.get("host", "localhost")).strip() or "localhost"
        port = int(config.get("port", 1521))
        service_name = str(config.get("service_name", "") or config.get("database", "")).strip()
        sid = str(config.get("sid", "")).strip()

        if service_name:
            return (
                f"oracle+oracledb://{user}:{password}@{host}:{port}/"
                f"?service_name={quote_plus(service_name)}"
            )
        if sid:
            return (
                f"oracle+oracledb://{user}:{password}@{host}:{port}/"
                f"?sid={quote_plus(sid)}"
            )
        return f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name=ORCLCDB"

    async def _dest_oracle(self, config: dict, df) -> list:
        try:
            from sqlalchemy import create_engine
            from sqlalchemy import types as sql_types
            from sqlalchemy.dialects import oracle as oracle_types
            import pandas as pd

            def _parse_bool_like(value: Any, default: bool = False) -> bool:
                if isinstance(value, bool):
                    return value
                if isinstance(value, (int, float)):
                    return value != 0
                if isinstance(value, str):
                    norm = value.strip().lower()
                    if norm in {"1", "true", "yes", "y", "on"}:
                        return True
                    if norm in {"0", "false", "no", "n", "off"}:
                        return False
                return default

            def _parse_oracle_mappings(value: Any) -> List[Dict[str, Any]]:
                raw = value
                if isinstance(raw, str):
                    text = raw.strip()
                    if not text:
                        return []
                    try:
                        raw = json.loads(text)
                    except Exception:
                        return []
                if not isinstance(raw, list):
                    return []
                out: List[Dict[str, Any]] = []
                for item in raw:
                    if not isinstance(item, dict):
                        continue
                    source = str(item.get("source", item.get("from", ""))).strip()
                    destination = str(item.get("destination", item.get("to", ""))).strip()
                    enabled = _parse_bool_like(item.get("enabled", True), True)
                    if not source and not destination:
                        continue
                    out.append({
                        "source": source,
                        "destination": destination,
                        "enabled": enabled,
                    })
                return out

            def _split_sql_statements(sql_script: str) -> List[str]:
                text = str(sql_script or "").strip()
                if not text:
                    return []
                statements: List[str] = []
                buf: List[str] = []
                quote: Optional[str] = None
                prev_ch = ""
                for ch in text:
                    if ch in {"'", '"'} and prev_ch != "\\":
                        if quote is None:
                            quote = ch
                        elif quote == ch:
                            quote = None
                    if ch == ";" and quote is None:
                        stmt = "".join(buf).strip()
                        if stmt:
                            statements.append(stmt)
                        buf = []
                    else:
                        buf.append(ch)
                    prev_ch = ch
                tail = "".join(buf).strip()
                if tail:
                    statements.append(tail)
                return statements

            def _parse_key_columns(value: Any) -> List[str]:
                if isinstance(value, list):
                    parts = [str(v).strip() for v in value]
                elif isinstance(value, str):
                    text = value.strip()
                    if not text:
                        parts = []
                    else:
                        try:
                            parsed = json.loads(text)
                            if isinstance(parsed, list):
                                parts = [str(v).strip() for v in parsed]
                            else:
                                parts = [p.strip() for p in text.replace("\n", ",").split(",")]
                        except Exception:
                            parts = [p.strip() for p in text.replace("\n", ",").split(",")]
                else:
                    parts = []

                out: List[str] = []
                seen: set = set()
                for item in parts:
                    if not item:
                        continue
                    key = item.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append(item)
                return out

            def _quote_ident(name: str) -> str:
                return '"' + str(name or "").replace('"', '""') + '"'

            def _normalize_value(value: Any) -> Any:
                if value is None:
                    return None
                if isinstance(value, float) and math.isnan(value):
                    return None
                if isinstance(value, dict):
                    return json.dumps(value, ensure_ascii=False)
                if isinstance(value, (list, tuple)):
                    return json.dumps(list(value), ensure_ascii=False)
                if isinstance(value, set):
                    return json.dumps(list(value), ensure_ascii=False)
                try:
                    import pandas as _pd
                    if _pd.isna(value):
                        return None
                    if isinstance(value, _pd.Timestamp):
                        return value.to_pydatetime()
                except Exception:
                    pass
                return value

            url = self._build_sqlalchemy_oracle_url(config)
            engine = create_engine(url)
            table = config.get("table", "ETL_OUTPUT")
            schema = config.get("schema") or None
            if_exists = (config.get("if_exists", "append") or "append").lower()
            if if_exists not in {"append", "replace", "fail"}:
                if_exists = "append"
            operation = str(config.get("oracle_operation", "insert") or "insert").strip().lower()
            if operation not in {"insert", "update", "upsert"}:
                operation = "insert"

            df_to_write = df.copy()
            mappings = _parse_oracle_mappings(config.get("oracle_column_mappings"))
            mapped_only = _parse_bool_like(config.get("oracle_only_mapped_columns"), False)
            pre_sql = str(config.get("oracle_pre_sql") or "").strip()
            post_sql = str(config.get("oracle_post_sql") or "").strip()

            enabled_mappings = [m for m in mappings if m.get("enabled", True)]
            duplicate_dest = sorted({
                m["destination"]
                for m in enabled_mappings
                if m.get("destination")
                and sum(1 for x in enabled_mappings if x.get("destination") == m.get("destination")) > 1
            })
            if duplicate_dest:
                raise RuntimeError(
                    "Oracle mapping contains duplicate destination columns: "
                    + ", ".join(duplicate_dest[:10])
                )

            rename_map: Dict[str, str] = {}
            mapped_sources: List[str] = []
            for row in enabled_mappings:
                src = str(row.get("source") or "").strip()
                dst = str(row.get("destination") or "").strip()
                if not src or not dst or src not in df_to_write.columns:
                    continue
                rename_map[src] = dst
                mapped_sources.append(src)

            mapped_sources = list(dict.fromkeys(mapped_sources))
            has_mapping_config = len(mappings) > 0
            if has_mapping_config:
                if not mapped_sources:
                    raise RuntimeError(
                        "Oracle mapping is configured, but no enabled source->destination mapping matched input columns."
                    )
                # When mapping is configured, treat it as explicit selection:
                # only mapped columns are inserted to avoid accidental writes.
                df_to_write = df_to_write[mapped_sources].rename(columns=rename_map)
                mapped_only = True
            elif mapped_only:
                raise RuntimeError(
                    "Mapped-only mode is enabled, but no Oracle column mappings are configured."
                )

            key_columns = _parse_key_columns(config.get("oracle_key_columns"))
            if rename_map and key_columns:
                # Allow key columns configured with original source names.
                key_columns = [rename_map.get(col, col) for col in key_columns]
                key_columns = _parse_key_columns(key_columns)

            dtype_map = {}
            for col, dtype in df_to_write.dtypes.items():
                if pd.api.types.is_integer_dtype(dtype):
                    dtype_map[col] = sql_types.BigInteger()
                elif pd.api.types.is_float_dtype(dtype):
                    dtype_map[col] = oracle_types.FLOAT(binary_precision=126)
                elif pd.api.types.is_bool_dtype(dtype):
                    dtype_map[col] = sql_types.Integer()
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    dtype_map[col] = sql_types.DateTime()
                else:
                    dtype_map[col] = sql_types.Text()

            pre_statements = _split_sql_statements(pre_sql)
            post_statements = _split_sql_statements(post_sql)
            rows_written = 0
            updated_rows = 0
            inserted_rows = 0

            with engine.begin() as conn:
                for stmt in pre_statements:
                    conn.exec_driver_sql(stmt)

                if operation == "insert":
                    df_to_write.to_sql(
                        table,
                        conn,
                        if_exists=if_exists,
                        index=False,
                        schema=schema,
                        dtype=dtype_map,
                    )
                    rows_written = len(df_to_write)
                    inserted_rows = rows_written
                else:
                    if not key_columns:
                        raise RuntimeError(
                            f"Oracle operation '{operation}' requires key columns (oracle_key_columns)."
                        )
                    missing_keys = [col for col in key_columns if col not in df_to_write.columns]
                    if missing_keys:
                        raise RuntimeError(
                            "Key columns not found in mapped output: " + ", ".join(missing_keys[:20])
                        )
                    set_columns = [col for col in df_to_write.columns if col not in key_columns]
                    if not set_columns:
                        raise RuntimeError(
                            f"Oracle operation '{operation}' requires at least one non-key column to update."
                        )

                    table_expr = _quote_ident(table)
                    if schema:
                        table_expr = f"{_quote_ident(schema)}.{table_expr}"

                    set_pairs = [f"{_quote_ident(col)} = :s_{idx}" for idx, col in enumerate(set_columns)]
                    where_pairs = [f"{_quote_ident(col)} = :k_{idx}" for idx, col in enumerate(key_columns)]
                    update_sql = (
                        f"UPDATE {table_expr} "
                        f"SET {', '.join(set_pairs)} "
                        f"WHERE {' AND '.join(where_pairs)}"
                    )

                    insert_cols = list(df_to_write.columns)
                    insert_col_expr = ", ".join(_quote_ident(col) for col in insert_cols)
                    insert_bind_expr = ", ".join(f":i_{idx}" for idx, _ in enumerate(insert_cols))
                    insert_sql = f"INSERT INTO {table_expr} ({insert_col_expr}) VALUES ({insert_bind_expr})"

                    normalized_rows = (
                        df_to_write
                        .where(pd.notnull(df_to_write), None)
                        .to_dict(orient="records")
                    )
                    for row_idx, rec in enumerate(normalized_rows):
                        null_keys = [k for k in key_columns if _normalize_value(rec.get(k)) is None]
                        if null_keys:
                            raise RuntimeError(
                                f"Row {row_idx + 1} has null key values for {', '.join(null_keys)}; cannot {operation}."
                            )

                        update_params: Dict[str, Any] = {}
                        for idx, col in enumerate(set_columns):
                            update_params[f"s_{idx}"] = _normalize_value(rec.get(col))
                        for idx, col in enumerate(key_columns):
                            update_params[f"k_{idx}"] = _normalize_value(rec.get(col))

                        result = conn.exec_driver_sql(update_sql, update_params)
                        affected = int(result.rowcount or 0)
                        if affected > 0:
                            updated_rows += affected
                            rows_written += affected
                        elif operation == "upsert":
                            insert_params = {
                                f"i_{idx}": _normalize_value(rec.get(col))
                                for idx, col in enumerate(insert_cols)
                            }
                            conn.exec_driver_sql(insert_sql, insert_params)
                            inserted_rows += 1
                            rows_written += 1

                for stmt in post_statements:
                    conn.exec_driver_sql(stmt)

            engine.dispose()
            return [{
                "status": "loaded",
                "rows": rows_written,
                "table": table,
                "schema": schema,
                "operation": operation,
                "if_exists": if_exists if operation == "insert" else None,
                "key_columns": key_columns if operation in {"update", "upsert"} else [],
                "mapped_columns": len(rename_map),
                "mapped_only": mapped_only,
                "updated_rows": updated_rows,
                "inserted_rows": inserted_rows,
                "pre_sql_statements": len(pre_statements),
                "post_sql_statements": len(post_statements),
            }]
        except Exception as e:
            raise RuntimeError(f"Oracle write failed: {e}")

    async def _dest_mongodb(self, config: dict, data: list) -> list:
        try:
            from pymongo import MongoClient
            client = MongoClient(config.get("connection_string", "mongodb://localhost:27017"))
            db = client[config.get("database", "etl")]
            collection = db[config.get("collection", "output")]
            if config.get("drop_first", False):
                collection.drop()
            collection.insert_many(data)
            return [{"status": "loaded", "rows": len(data), "collection": config.get("collection")}]
        except Exception as e:
            raise RuntimeError(f"MongoDB write failed: {e}")

    async def _dest_elasticsearch(self, config: dict, data: list) -> list:
        try:
            from elasticsearch import Elasticsearch
            from elasticsearch.helpers import bulk
            es = Elasticsearch(config.get("hosts", ["http://localhost:9200"]))
            index = config.get("index", "etl_output")
            actions = [{"_index": index, "_source": doc} for doc in data]
            bulk(es, actions)
            return [{"status": "loaded", "rows": len(data), "index": index}]
        except Exception as e:
            raise RuntimeError(f"Elasticsearch write failed: {e}")

    async def _dest_redis(self, config: dict, data: list) -> list:
        try:
            import redis as redis_lib
            r = redis_lib.Redis(
                host=config.get("host", "localhost"),
                port=int(config.get("port", 6379)),
                password=config.get("password"),
                decode_responses=True
            )
            key_prefix = config.get("key_prefix", "etl:")
            for i, doc in enumerate(data):
                r.set(f"{key_prefix}{i}", json.dumps(doc))
            return [{"status": "loaded", "rows": len(data)}]
        except Exception as e:
            raise RuntimeError(f"Redis write failed: {e}")

    async def _dest_s3(self, config: dict, df) -> list:
        try:
            import boto3, io
            s3 = boto3.client(
                "s3",
                aws_access_key_id=config.get("access_key"),
                aws_secret_access_key=config.get("secret_key"),
                region_name=config.get("region", "us-east-1")
            )
            key = config.get("key", "etl_output.csv")
            buf = io.BytesIO()
            if key.endswith(".parquet"):
                df.to_parquet(buf, index=False)
            elif key.endswith(".json"):
                df.to_json(buf, orient="records", indent=2)
            else:
                df.to_csv(buf, index=False)
            buf.seek(0)
            s3.upload_fileobj(buf, config.get("bucket", ""), key)
            return [{"status": "uploaded", "rows": len(df), "key": key}]
        except Exception as e:
            raise RuntimeError(f"S3 write failed: {e}")

    async def _dest_rest_api(self, config: dict, data: list) -> list:
        try:
            import httpx
            url = config.get("url", "")
            headers = json.loads(config.get("headers", "{}"))
            async with httpx.AsyncClient() as client:
                resp = await client.post(url, json=data, headers=headers, timeout=30)
                resp.raise_for_status()
            return [{"status": "posted", "rows": len(data), "http_status": resp.status_code}]
        except Exception as e:
            raise RuntimeError(f"REST API write failed: {e}")

    # ─── MOCK DATA ────────────────────────────────────────────────────────────

    def _mock_data(self, config: dict, source_type: str) -> list:
        """Generate realistic mock data for demo/testing purposes."""
        import random
        size = int(config.get("limit", config.get("max_records", 25)))
        size = min(size, 50)

        base = {
            "postgres": lambda i: {"id": i, "name": f"User {i}", "email": f"user{i}@example.com",
                                    "created_at": f"2024-0{(i%9)+1}-{(i%28)+1:02d}", "revenue": round(random.uniform(100, 9999), 2)},
            "mysql": lambda i: {"id": i, "product": f"Product {i}", "category": random.choice(["A","B","C"]),
                                 "price": round(random.uniform(5, 500), 2), "stock": random.randint(0, 1000)},
            "oracle": lambda i: {"ID": i, "CUSTOMER_NAME": f"Customer {i}", "ACCOUNT_TYPE": random.choice(["SAVINGS", "CURRENT"]),
                                 "BALANCE": round(random.uniform(1000, 100000), 2), "BRANCH_CODE": f"BR{i % 100:03d}"},
            "mongodb": lambda i: {"_id": str(uuid.uuid4()), "user": f"user_{i}", "action": random.choice(["click","view","purchase"]),
                                   "timestamp": f"2024-03-{(i%28)+1:02d}T12:00:00Z"},
            "redis": lambda i: {"key": f"session:{uuid.uuid4().hex[:8]}", "value": f"token_{i}"},
            "elasticsearch": lambda i: {"doc_id": str(uuid.uuid4()), "title": f"Document {i}",
                                         "content": f"Sample content for document {i}", "score": round(random.random(), 4)},
            "csv": lambda i: {"row_id": i, "col_a": f"value_{i}", "col_b": random.randint(1, 100),
                               "col_c": round(random.uniform(0, 1), 4)},
            "json": lambda i: {"id": i, "data": {"nested_key": f"val_{i}", "count": i * 10}},
            "excel": lambda i: {"sheet_row": i, "name": f"Item {i}", "amount": random.randint(100, 9999),
                                  "status": random.choice(["active","inactive","pending"])},
            "xml": lambda i: {"element": f"elem_{i}", "attribute": f"attr_val_{i}"},
            "parquet": lambda i: {"partition": i // 10, "record_id": i, "metric": round(random.gauss(50, 15), 2)},
            "rest_api": lambda i: {"id": i, "title": f"Record {i}", "body": f"Content {i}",
                                    "userId": (i % 10) + 1},
            "graphql": lambda i: {"node_id": str(uuid.uuid4()), "field1": f"value_{i}", "count": i},
            "s3": lambda i: {"s3_key": f"data/part-{i:05d}.parquet", "size_bytes": random.randint(1024, 1048576),
                             "last_modified": f"2024-03-{(i%28)+1:02d}"},
            "kafka": lambda i: {"offset": i, "partition": i % 4, "topic": config.get("topic","events"),
                                 "payload": {"event": random.choice(["created","updated","deleted"]), "id": str(uuid.uuid4())}},
        }
        gen = base.get(source_type, lambda i: {"id": i, "value": f"item_{i}"})
        return [gen(i) for i in range(1, size + 1)]
