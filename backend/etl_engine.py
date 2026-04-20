"""
ETL Engine - Core execution engine for running pipelines.
Supports: Databases, Files, APIs, Cloud Storage, Message Queues
"""
import asyncio
import ast
import base64
import copy
import json
import math
import os
import re
import tempfile
import time as pytime
import uuid
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote_plus
from loguru import logger
try:
    import lmdb  # type: ignore
except Exception:  # pragma: no cover - optional dependency fallback
    lmdb = None


class ExecutionAbortedError(Exception):
    """Raised when an execution is aborted by user request."""


class ETLEngine:
    def __init__(self):
        self.active_executions: Dict[str, dict] = {}
        # Hot-path caches for custom expression evaluation.
        self._expr_ast_cache: Dict[str, ast.AST] = {}
        self._json_template_cache: Dict[str, Any] = {}
        self._path_variants_cache: Dict[str, List[str]] = {}
        self._profile_event_time_cache: Dict[Any, Optional[datetime]] = {}
        self._profile_fractional_seconds_re = re.compile(r"\.(\d{1,9})")
        self._profile_lmdb_env = None
        self._profile_lmdb_enabled = lmdb is not None

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

    def _resolve_lmdb_env_path(self, raw_path: str) -> str:
        """
        Resolve LMDB environment path.

        Supports:
        - absolute/relative directory path
        - absolute/relative data.mdb file path (auto-resolves to parent directory)
        - environment variables and ~ expansion
        """
        path_text = str(raw_path or "").strip()
        if not path_text:
            raise RuntimeError("LMDB environment path is empty. Select LMDB folder or data.mdb file.")

        expanded = os.path.expandvars(os.path.expanduser(path_text))
        if expanded.startswith("local://"):
            original_name = expanded.replace("local://", "", 1).strip().strip("/")
            recovered = self._find_uploaded_file(original_name)
            if recovered:
                expanded = recovered
            else:
                raise RuntimeError(
                    f"LMDB file '{original_name}' was not uploaded to the server. "
                    "Re-open LMDB source node and browse the LMDB folder again."
                )

        if os.path.isfile(expanded):
            base = os.path.basename(expanded).lower()
            if base == "data.mdb":
                expanded = os.path.dirname(expanded)
            else:
                raise RuntimeError(
                    "LMDB path must point to an environment directory or a data.mdb file."
                )

        if not os.path.isdir(expanded):
            raise RuntimeError(f"LMDB environment directory not found: {expanded}")

        return expanded

    def _decode_lmdb_value(self, value: bytes, value_format: str) -> Tuple[Any, str]:
        fmt = str(value_format or "auto").strip().lower() or "auto"
        if fmt == "base64":
            return base64.b64encode(value).decode("ascii"), "base64"

        decoded_text = None
        try:
            decoded_text = value.decode("utf-8")
        except Exception:
            if fmt in {"text", "string"}:
                decoded_text = value.decode("utf-8", errors="replace")
            elif fmt in {"json"}:
                decoded_text = value.decode("utf-8", errors="replace")

        if fmt in {"json", "auto"} and decoded_text is not None:
            try:
                return json.loads(decoded_text), "json"
            except Exception:
                if fmt == "json":
                    return decoded_text, "text"

        if fmt in {"text", "string"}:
            if decoded_text is None:
                decoded_text = value.decode("utf-8", errors="replace")
            return decoded_text, "text"

        if fmt == "auto":
            if decoded_text is not None:
                return decoded_text, "text"
            return base64.b64encode(value).decode("ascii"), "base64"

        if decoded_text is not None:
            return decoded_text, "text"
        return base64.b64encode(value).decode("ascii"), "base64"

    def _expand_lmdb_profile_documents(
        self,
        lmdb_key: str,
        decoded_value: Any,
        include_value_kind: bool,
        value_kind: str,
    ) -> List[Dict[str, Any]]:
        """
        Expand profile-state style payloads stored as:
          {"documents": {entity_key: profile_obj, ...}, "meta": {...}}
        into one output row per entity.
        """
        if not isinstance(decoded_value, dict):
            return []

        documents = decoded_value.get("documents")
        if not isinstance(documents, dict) or not documents:
            return []

        meta = decoded_value.get("meta")
        node_stats = decoded_value.get("stats")
        out_rows: List[Dict[str, Any]] = []

        for entity_key, profile_value in documents.items():
            row: Dict[str, Any] = {}
            if isinstance(profile_value, dict):
                row.update(profile_value)
            else:
                row["profile"] = profile_value

            row["lmdb_key"] = str(lmdb_key)
            row["lmdb_entity_key"] = str(entity_key)
            row["_lmdb_profile_source"] = "documents"

            if isinstance(meta, dict):
                entity_meta = meta.get(entity_key)
                if entity_meta is None:
                    entity_meta = meta.get(str(entity_key))
                if entity_meta is not None:
                    row["_lmdb_entity_meta"] = entity_meta
            if isinstance(node_stats, dict):
                row["_lmdb_node_stats"] = node_stats

            if include_value_kind:
                row["_lmdb_value_kind"] = value_kind

            out_rows.append(self._json_safe_value(row))

        return out_rows

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
        except Exception as exc:
            # Corrupted state file should not block execution.
            # Move it aside once and continue with a clean state.
            try:
                stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                backup_path = f"{path}.corrupt_{stamp}"
                os.replace(path, backup_path)
                logger.warning(
                    f"Runtime state file was corrupted and has been moved to {backup_path}: {exc}"
                )
            except Exception:
                logger.warning(f"Failed to load runtime state: {exc}")
        return {"pipelines": {}}

    def _save_runtime_state(self, state: Dict[str, Any]) -> None:
        path = self._runtime_state_path()
        tmp_path = ""
        try:
            safe_state = self._json_safe_value(state)
            fd, tmp_path = tempfile.mkstemp(
                prefix="pipeline_runtime_state_",
                suffix=".tmp",
                dir=os.path.dirname(path),
                text=True,
            )
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(safe_state, fh, ensure_ascii=False, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, path)
        except Exception as exc:
            logger.warning(f"Failed to persist runtime state: {exc}")
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    def _profile_lmdb_dir(self) -> str:
        state_dir = os.path.join(os.path.dirname(__file__), "state")
        os.makedirs(state_dir, exist_ok=True)
        return os.path.join(state_dir, "profile_store.lmdb")

    def _get_profile_lmdb_env(self):
        if not self._profile_lmdb_enabled or lmdb is None:
            return None
        if self._profile_lmdb_env is not None:
            return self._profile_lmdb_env
        lmdb_dir = self._profile_lmdb_dir()
        os.makedirs(lmdb_dir, exist_ok=True)
        try:
            self._profile_lmdb_env = lmdb.open(
                lmdb_dir,
                map_size=4 * 1024 * 1024 * 1024,  # 4GB
                max_dbs=1,
                subdir=True,
                create=True,
                lock=True,
                sync=False,
                metasync=False,
                readahead=True,
            )
        except Exception as exc:
            logger.warning(f"LMDB profile store init failed; falling back to JSON runtime state: {exc}")
            self._profile_lmdb_enabled = False
            self._profile_lmdb_env = None
            return None
        return self._profile_lmdb_env

    def _profile_lmdb_prefix(self, pipeline_id: str) -> bytes:
        return f"{str(pipeline_id)}::".encode("utf-8")

    def _profile_lmdb_key(self, pipeline_id: str, node_id: str) -> bytes:
        return f"{str(pipeline_id)}::{str(node_id)}".encode("utf-8")

    def _node_profile_counts(self, node_state: Any) -> Tuple[int, int]:
        if not isinstance(node_state, dict):
            return 0, 0
        docs = node_state.get("documents")
        meta = node_state.get("meta")
        return (
            len(docs) if isinstance(docs, dict) else 0,
            len(meta) if isinstance(meta, dict) else 0,
        )

    def _load_runtime_profile_state_by_node(self, pipeline_id: str) -> Dict[str, Any]:
        runtime_state = self._load_runtime_state()
        pipelines_state = runtime_state.get("pipelines") if isinstance(runtime_state, dict) else {}
        if not isinstance(pipelines_state, dict):
            return {}
        pipeline_state = pipelines_state.get(str(pipeline_id))
        if not isinstance(pipeline_state, dict):
            return {}
        profile_documents = pipeline_state.get("profile_documents")
        if not isinstance(profile_documents, dict):
            return {}
        out: Dict[str, Any] = {}
        for nid, node_state in profile_documents.items():
            if not isinstance(node_state, dict):
                continue
            docs = node_state.get("documents")
            meta = node_state.get("meta")
            stats = node_state.get("stats")
            out[str(nid)] = {
                "documents": docs if isinstance(docs, dict) else {},
                "meta": meta if isinstance(meta, dict) else {},
                "stats": stats if isinstance(stats, dict) else {},
            }
        return out

    def _load_lmdb_profile_state_by_node(self, pipeline_id: str) -> Dict[str, Any]:
        env = self._get_profile_lmdb_env()
        if env is None:
            return {}
        prefix = self._profile_lmdb_prefix(str(pipeline_id))
        out: Dict[str, Any] = {}
        try:
            with env.begin(write=False) as txn:
                cursor = txn.cursor()
                if cursor.set_range(prefix):
                    for key_bytes, value_bytes in cursor:
                        if not key_bytes.startswith(prefix):
                            break
                        node_id = key_bytes[len(prefix):].decode("utf-8", errors="ignore")
                        if not node_id:
                            continue
                        try:
                            payload = json.loads(value_bytes.decode("utf-8"))
                        except Exception:
                            continue
                        if not isinstance(payload, dict):
                            continue
                        docs = payload.get("documents")
                        meta = payload.get("meta")
                        stats = payload.get("stats")
                        out[str(node_id)] = {
                            "documents": docs if isinstance(docs, dict) else {},
                            "meta": meta if isinstance(meta, dict) else {},
                            "stats": stats if isinstance(stats, dict) else {},
                        }
        except Exception as exc:
            logger.warning(f"Failed to read LMDB profile state for pipeline {pipeline_id}: {exc}")
            return {}
        return out

    def _save_profile_state_by_node(self, pipeline_id: str, profile_state_by_node: Dict[str, Any]) -> bool:
        env = self._get_profile_lmdb_env()
        if env is None:
            return False
        if not isinstance(profile_state_by_node, dict):
            profile_state_by_node = {}
        safe_state = self._json_safe_value(profile_state_by_node)
        prefix = self._profile_lmdb_prefix(str(pipeline_id))
        try:
            with env.begin(write=True) as txn:
                existing_keys: List[bytes] = []
                cursor = txn.cursor()
                if cursor.set_range(prefix):
                    for key_bytes, _ in cursor:
                        if not key_bytes.startswith(prefix):
                            break
                        existing_keys.append(bytes(key_bytes))

                keep_keys = set()
                for nid, node_state in safe_state.items():
                    node_id = str(nid or "").strip()
                    if not node_id:
                        continue
                    node_dict = node_state if isinstance(node_state, dict) else {}
                    docs = node_dict.get("documents")
                    meta = node_dict.get("meta")
                    stats = node_dict.get("stats")
                    payload = {
                        "documents": docs if isinstance(docs, dict) else {},
                        "meta": meta if isinstance(meta, dict) else {},
                        "stats": stats if isinstance(stats, dict) else {},
                    }
                    key_bytes = self._profile_lmdb_key(str(pipeline_id), node_id)
                    keep_keys.add(key_bytes)
                    txn.put(
                        key_bytes,
                        json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                    )

                for key_bytes in existing_keys:
                    if key_bytes not in keep_keys:
                        txn.delete(key_bytes)
            return True
        except Exception as exc:
            logger.warning(f"Failed to persist LMDB profile state for pipeline {pipeline_id}: {exc}")
            return False

    def _save_profile_state_single_node(self, pipeline_id: str, node_id: str, node_state: Dict[str, Any]) -> bool:
        env = self._get_profile_lmdb_env()
        if env is None:
            return False
        safe_node_state = self._json_safe_value(node_state if isinstance(node_state, dict) else {})
        docs = safe_node_state.get("documents") if isinstance(safe_node_state, dict) else {}
        meta = safe_node_state.get("meta") if isinstance(safe_node_state, dict) else {}
        stats = safe_node_state.get("stats") if isinstance(safe_node_state, dict) else {}
        payload = {
            "documents": docs if isinstance(docs, dict) else {},
            "meta": meta if isinstance(meta, dict) else {},
            "stats": stats if isinstance(stats, dict) else {},
        }
        key_bytes = self._profile_lmdb_key(str(pipeline_id), str(node_id))
        try:
            with env.begin(write=True) as txn:
                txn.put(
                    key_bytes,
                    json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                )
            return True
        except Exception as exc:
            logger.warning(
                f"Failed to persist incremental LMDB profile state for pipeline {pipeline_id}, node {node_id}: {exc}"
            )
            return False

    def _clear_runtime_profile_state_by_node(self, pipeline_id: str, node_id: Optional[str] = None) -> None:
        runtime_state = self._load_runtime_state()
        pipelines_state = runtime_state.get("pipelines") if isinstance(runtime_state, dict) else {}
        if not isinstance(pipelines_state, dict):
            return
        pipeline_state = pipelines_state.get(str(pipeline_id))
        if not isinstance(pipeline_state, dict):
            return
        profile_documents = pipeline_state.get("profile_documents")
        if not isinstance(profile_documents, dict):
            return
        if node_id:
            profile_documents.pop(str(node_id), None)
        else:
            pipeline_state.pop("profile_documents", None)
        self._save_runtime_state(runtime_state)

    def _clear_lmdb_profile_state_by_node(
        self,
        pipeline_id: str,
        node_id: Optional[str] = None,
    ) -> Tuple[int, int, int]:
        env = self._get_profile_lmdb_env()
        if env is None:
            return 0, 0, 0

        removed_nodes = 0
        removed_entities = 0
        removed_meta_entries = 0
        prefix = self._profile_lmdb_prefix(str(pipeline_id))
        try:
            with env.begin(write=True) as txn:
                keys_to_delete: List[bytes] = []
                if node_id:
                    key_bytes = self._profile_lmdb_key(str(pipeline_id), str(node_id))
                    raw_value = txn.get(key_bytes)
                    if raw_value is not None:
                        try:
                            payload = json.loads(raw_value.decode("utf-8"))
                        except Exception:
                            payload = {}
                        docs_count, meta_count = self._node_profile_counts(payload)
                        removed_nodes = 1
                        removed_entities = docs_count
                        removed_meta_entries = meta_count
                        keys_to_delete.append(key_bytes)
                else:
                    cursor = txn.cursor()
                    if cursor.set_range(prefix):
                        for key_bytes, value_bytes in cursor:
                            if not key_bytes.startswith(prefix):
                                break
                            try:
                                payload = json.loads(value_bytes.decode("utf-8"))
                            except Exception:
                                payload = {}
                            docs_count, meta_count = self._node_profile_counts(payload)
                            removed_nodes += 1
                            removed_entities += docs_count
                            removed_meta_entries += meta_count
                            keys_to_delete.append(bytes(key_bytes))

                for key_bytes in keys_to_delete:
                    txn.delete(key_bytes)
        except Exception as exc:
            logger.warning(f"Failed to clear LMDB profile state for pipeline {pipeline_id}: {exc}")
            return 0, 0, 0
        return removed_nodes, removed_entities, removed_meta_entries

    def _load_profile_state_by_node(self, pipeline_id: str) -> Dict[str, Any]:
        pipeline_key = str(pipeline_id)
        lmdb_state = self._load_lmdb_profile_state_by_node(pipeline_key)
        if lmdb_state:
            return lmdb_state

        runtime_state = self._load_runtime_profile_state_by_node(pipeline_key)
        # One-time migration of legacy JSON runtime profile state into LMDB.
        # Strict mode for profiling: do not continue using runtime JSON as active store.
        if runtime_state:
            migrated = self._save_profile_state_by_node(pipeline_key, runtime_state)
            if migrated:
                self._clear_runtime_profile_state_by_node(pipeline_key)
                reloaded = self._load_lmdb_profile_state_by_node(pipeline_key)
                if reloaded:
                    return reloaded
            logger.warning(
                "Profile state found in legacy runtime JSON but LMDB is unavailable. "
                "Profile state will be treated as empty until LMDB is available."
            )
        return {}

    def get_profile_state_summary(
        self,
        pipeline_id: str,
        node_id: Optional[str] = None,
        limit: int = 10,
        preferred_primary_key_field: Optional[str] = None,
    ) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit or 10), 100))
        preferred_key_field = str(preferred_primary_key_field or "").strip()
        profile_documents = self._load_profile_state_by_node(str(pipeline_id))

        nodes: List[Dict[str, Any]] = []
        total_entities = 0
        total_meta_entries = 0
        available_node_ids: List[str] = []

        for nid, node_state in profile_documents.items():
            node_name = str(nid)
            if node_id and node_name != str(node_id):
                continue
            if not isinstance(node_state, dict):
                continue
            available_node_ids.append(node_name)
            documents = node_state.get("documents")
            meta = node_state.get("meta")
            if not isinstance(documents, dict):
                documents = {}
            if not isinstance(meta, dict):
                meta = {}

            entity_keys = list(documents.keys())
            entity_count = len(entity_keys)
            meta_count = len(meta)
            total_entities += entity_count
            total_meta_entries += meta_count

            if preferred_key_field:
                matching_entity_keys: List[Any] = []
                fallback_entity_keys: List[Any] = []
                # Bound scan effort for very large profile stores while still
                # prioritizing samples aligned to the selected primary key field.
                scan_cap = min(len(entity_keys), max(safe_limit * 300, 5000))
                for idx, entity_key in enumerate(entity_keys):
                    if idx >= scan_cap:
                        break
                    profile_doc = documents.get(entity_key)
                    if isinstance(profile_doc, dict):
                        pk_value, pk_found = self._extract_row_value_by_path(profile_doc, preferred_key_field)
                        if pk_found and pk_value is not None and str(pk_value).strip() != "":
                            matching_entity_keys.append(entity_key)
                            if len(matching_entity_keys) >= safe_limit:
                                continue
                            continue
                    fallback_entity_keys.append(entity_key)
                    if len(matching_entity_keys) >= safe_limit and len(fallback_entity_keys) >= safe_limit:
                        break
                sample_entity_keys = (matching_entity_keys + fallback_entity_keys)[:safe_limit]
                if not sample_entity_keys:
                    sample_entity_keys = entity_keys[:safe_limit]
            else:
                sample_entity_keys = entity_keys[:safe_limit]
            sample_documents: List[Dict[str, Any]] = []
            for entity_key in sample_entity_keys:
                sample_documents.append(
                    {
                        "entity_key": str(entity_key),
                        "profile": self._json_safe_value(documents.get(entity_key)),
                    }
                )

            nodes.append(
                {
                    "node_id": node_name,
                    "entity_count": entity_count,
                    "meta_count": meta_count,
                    "sample_entity_keys": sample_entity_keys,
                    "sample_documents": sample_documents,
                }
            )

        return {
            "pipeline_id": str(pipeline_id),
            "node_id": node_id or None,
            "limit": safe_limit,
            "storage": "lmdb",
            "total_nodes": len(nodes),
            "total_entities": total_entities,
            "total_meta_entries": total_meta_entries,
            "available_node_ids": available_node_ids,
            "nodes": nodes,
            "generated_at": datetime.utcnow().isoformat(),
        }

    def clear_profile_state(
        self,
        pipeline_id: str,
        node_id: Optional[str] = None,
    ) -> Dict[str, int]:
        pipeline_key = str(pipeline_id)
        current_state = self._load_profile_state_by_node(pipeline_key)

        removed_nodes = 0
        removed_entities = 0
        removed_meta_entries = 0
        for nid, node_state in current_state.items():
            if node_id and str(nid) != str(node_id):
                continue
            docs_count, meta_count = self._node_profile_counts(node_state)
            if docs_count > 0 or meta_count > 0:
                removed_nodes += 1
            removed_entities += docs_count
            removed_meta_entries += meta_count

        # Clear both LMDB and legacy JSON runtime storage.
        self._clear_lmdb_profile_state_by_node(pipeline_key, node_id=node_id)
        self._clear_runtime_profile_state_by_node(pipeline_key, node_id=node_id)

        return {
            "removed_nodes": removed_nodes,
            "removed_entities": removed_entities,
            "removed_meta_entries": removed_meta_entries,
        }

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
        on_progress: Optional[Callable[[Dict[str, Any]], None]] = None,
        progress_every: int = 5000,
        should_abort: Optional[Callable[[], bool]] = None,
    ) -> Tuple[List[Any], Any, int]:
        if not isinstance(rows, list) or not rows or not field_path:
            return rows, last_checkpoint, 0

        filtered: List[Any] = []
        scanned = 0
        dropped = 0
        max_seen = last_checkpoint
        total_rows = len(rows)
        safe_progress_every = max(100, min(int(progress_every or 5000), 100000))
        for idx, row in enumerate(rows, start=1):
            if callable(should_abort):
                try:
                    if bool(should_abort()):
                        raise ExecutionAbortedError("Execution aborted during incremental filtering.")
                except ExecutionAbortedError:
                    raise
                except Exception:
                    pass
            if not isinstance(row, dict):
                filtered.append(row)
                if on_progress and (idx % safe_progress_every == 0 or idx == total_rows):
                    try:
                        on_progress({
                            "processed_rows": idx,
                            "total_rows": total_rows,
                            "candidate_rows": scanned,
                            "kept_rows": len(filtered),
                            "dropped_rows": dropped,
                        })
                    except Exception:
                        pass
                continue
            value, found = self._extract_row_value_by_path(row, field_path)
            if not found or value is None:
                filtered.append(row)
                if on_progress and (idx % safe_progress_every == 0 or idx == total_rows):
                    try:
                        on_progress({
                            "processed_rows": idx,
                            "total_rows": total_rows,
                            "candidate_rows": scanned,
                            "kept_rows": len(filtered),
                            "dropped_rows": dropped,
                        })
                    except Exception:
                        pass
                continue
            scanned += 1
            max_seen = self._max_value(max_seen, value)
            if last_checkpoint is None or self._compare_values(value, last_checkpoint) > 0:
                filtered.append(row)
            else:
                dropped += 1

            if on_progress and (idx % safe_progress_every == 0 or idx == total_rows):
                try:
                    on_progress({
                        "processed_rows": idx,
                        "total_rows": total_rows,
                        "candidate_rows": scanned,
                        "kept_rows": len(filtered),
                        "dropped_rows": dropped,
                    })
                except Exception:
                    pass

        return filtered, max_seen, scanned

    def _get_incremental_source_context(
        self,
        execution_context: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(execution_context, dict):
            return None
        mode = str(execution_context.get("mode") or "").strip().lower()
        if mode not in {"incremental", "streaming"}:
            return None
        runtime = execution_context.get("runtime")
        if not isinstance(runtime, dict):
            return None
        field_path = str(runtime.get("incremental_field") or "").strip()
        if not field_path:
            return None
        node_id = str(execution_context.get("node_id") or "").strip()
        if not node_id:
            return None
        pipeline_state = execution_context.get("pipeline_state")
        if not isinstance(pipeline_state, dict):
            return None
        checkpoints = pipeline_state.get("incremental_checkpoints")
        if not isinstance(checkpoints, dict):
            return None
        checkpoint_key = f"{node_id}:{field_path}"
        return {
            "field_path": field_path,
            "checkpoint_key": checkpoint_key,
            "last_checkpoint": checkpoints.get(checkpoint_key),
        }

    def _can_pushdown_incremental_field(self, field_path: str) -> bool:
        path = str(field_path or "").strip()
        if not path:
            return False
        # SQL source pushdown supports only direct column identifiers.
        # Nested/json-style paths (for example "daily.time") are filtered post-fetch.
        if "." in path or "[" in path or "]" in path:
            return False
        ident_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")
        return bool(ident_re.match(path))

    def _build_incremental_pushdown_query(
        self,
        base_query: str,
        field_path: str,
        dialect: str,
    ) -> Optional[str]:
        query = str(base_query or "").strip().rstrip(";")
        field = str(field_path or "").strip()
        if not query or not self._can_pushdown_incremental_field(field):
            return None
        if dialect == "oracle":
            return (
                f"SELECT * FROM ({query}) src_q "
                f"WHERE src_q.{field} > :checkpoint "
                f"ORDER BY src_q.{field} ASC"
            )
        return (
            f"SELECT * FROM ({query}) src_q "
            f"WHERE src_q.{field} > %s "
            f"ORDER BY src_q.{field} ASC"
        )

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

    def _parse_bool_like(self, value: Any, default: bool = True) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            norm = value.strip().lower()
            if norm in {"0", "false", "no", "off", "disabled", "disable"}:
                return False
            if norm in {"1", "true", "yes", "on", "enabled", "enable"}:
                return True
        return default

    def _is_node_enabled(self, node: Dict[str, Any]) -> bool:
        if not isinstance(node, dict):
            return True
        node_data = node.get("data")
        if not isinstance(node_data, dict):
            return True
        cfg = node_data.get("config")
        if not isinstance(cfg, dict):
            return True
        return self._parse_bool_like(cfg.get("node_enabled", True), True)

    async def execute_pipeline(
        self,
        pipeline: dict,
        execution_id: str,
        db,
        websocket_manager=None,
        on_node_done=None,
        runtime_config: Optional[Dict[str, Any]] = None,
        pipeline_id: Optional[str] = None,
        should_abort: Optional[Callable[[], bool]] = None,
    ) -> dict:
        """Execute a full ETL pipeline by topologically sorting nodes."""
        def _raise_if_aborted() -> None:
            if not callable(should_abort):
                return
            try:
                if bool(should_abort()):
                    raise ExecutionAbortedError("Execution aborted by user.")
            except ExecutionAbortedError:
                raise
            except Exception:
                return

        nodes = {n["id"]: n for n in pipeline.get("nodes", [])}
        edges = pipeline.get("edges", [])
        runtime = self._normalize_runtime_config(runtime_config)
        mode = runtime["mode"]
        profile_mode_requested = False
        for node in pipeline.get("nodes", []):
            node_data = node.get("data") if isinstance(node, dict) else {}
            node_config = node_data.get("config") if isinstance(node_data, dict) else {}
            if not isinstance(node_config, dict):
                continue
            if bool(node_config.get("custom_profile_enabled", False)):
                profile_mode_requested = True
                break
        if profile_mode_requested and self._get_profile_lmdb_env() is None:
            raise RuntimeError(
                "LMDB profile store is required for custom profile mode. "
                "Install/enable LMDB and restart backend."
            )

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
        profile_state_by_node: Dict[str, Any] = {}
        if pipeline_id:
            pipeline_state = pipelines_state.setdefault(pipeline_id, {})
            loaded_profile_state = self._load_profile_state_by_node(pipeline_id)
            if isinstance(loaded_profile_state, dict):
                profile_state_by_node = loaded_profile_state
        incremental_checkpoints = pipeline_state.setdefault("incremental_checkpoints", {})

        results: Dict[str, Any] = {}
        logs: List[dict] = []
        total_rows = 0
        stream_iterations = runtime["streaming_max_batches"] if mode == "streaming" else 1
        if mode == "streaming":
            stream_capable_sources = {"kafka_source", "webhook_trigger"}
            has_stream_capable_source = any(
                str((node.get("data") or {}).get("nodeType") or node.get("type") or "")
                in stream_capable_sources
                for node in nodes.values()
            )
            # Guardrail: static batch-style pipelines should not loop in streaming mode
            # unless an incremental field or streaming source is configured.
            if not runtime["incremental_field"] and not has_stream_capable_source:
                stream_iterations = 1

        for stream_idx in range(stream_iterations):
            _raise_if_aborted()
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
                _raise_if_aborted()
                node = nodes[nid]
                node_type = node.get("data", {}).get("nodeType", "")
                config = node.get("data", {}).get("config", {})
                label = node.get("data", {}).get("label", node_type)

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

                if not self._is_node_enabled(node):
                    output = upstream_data
                    pass_results[nid] = output
                    row_count = len(output) if isinstance(output, list) else 0
                    total_rows += row_count
                    pass_rows += row_count
                    skip_entry = {
                        "nodeId": nid,
                        "nodeLabel": label,
                        "timestamp": datetime.utcnow().isoformat(),
                        "status": "success",
                        "message": f"⏭ {label} — node disabled, skipped ({row_count:,} rows pass-through)",
                        "rows": row_count,
                        "skipped": True,
                    }
                    logs.append(skip_entry)
                    if websocket_manager:
                        await websocket_manager.broadcast(execution_id, {
                            "type": "node_success",
                            "nodeId": nid,
                            "rows": row_count,
                            "log_entry": dict(skip_entry),
                        })
                    if on_node_done:
                        on_node_done(logs)
                    continue

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
                if on_node_done:
                    on_node_done(logs)

                progress_every = max(
                    500,
                    min(int(runtime.get("batch_size") or 5000), 20000),
                )

                def _emit_live_node_progress(progress_payload: Dict[str, Any]) -> None:
                    processed_rows = int(progress_payload.get("processed_rows") or 0)
                    validated_rows = int(progress_payload.get("validated_rows") or 0)
                    output_rows = int(progress_payload.get("output_rows") or 0)
                    if output_rows > 0:
                        log_entry["rows"] = output_rows
                    elif processed_rows > 0:
                        log_entry["rows"] = processed_rows
                    log_entry["processed_rows"] = processed_rows
                    log_entry["validated_rows"] = validated_rows
                    custom_message = str(progress_payload.get("message") or "").strip()
                    if custom_message:
                        log_entry["message"] = custom_message
                    elif processed_rows > 0 or validated_rows > 0:
                        log_entry["message"] = (
                            f"⟳ Running {label}… processed={processed_rows:,} "
                            f"validated={validated_rows:,}"
                        )
                    if websocket_manager:
                        try:
                            asyncio.create_task(websocket_manager.broadcast(execution_id, {
                                "type": "node_progress",
                                "nodeId": nid,
                                "rows": int(log_entry.get("rows") or 0),
                                "processed_rows": processed_rows,
                                "validated_rows": validated_rows,
                                "log_entry": dict(log_entry),
                            }))
                        except Exception:
                            pass
                    if on_node_done:
                        try:
                            on_node_done(logs)
                        except Exception:
                            pass

                try:
                    node_warnings: List[str] = []

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
                        live_processed_rows = 0
                        for chunk_idx, chunk in enumerate(chunks):
                            _raise_if_aborted()
                            chunk_output = await self._execute_node(
                                node_type,
                                config,
                                chunk,
                                incoming_by_source={},
                                incoming_order=[],
                                execution_context={
                                    "execution_id": execution_id,
                                    "pipeline_id": pipeline_id,
                                    "node_id": nid,
                                    "node_label": label,
                                    "mode": mode,
                                    "stream_iteration": stream_idx,
                                    "runtime": runtime,
                                    "pipeline_state": pipeline_state,
                                    "profile_state_by_node": profile_state_by_node,
                                    "node_warnings": node_warnings,
                                    "emit_node_progress": _emit_live_node_progress,
                                    "node_progress_every": progress_every,
                                    "should_abort": should_abort,
                                    "raise_if_aborted": _raise_if_aborted,
                                },
                            )
                            if isinstance(chunk_output, list):
                                output.extend(chunk_output)
                            elif chunk_output is not None:
                                output.append(chunk_output)

                            chunk_rows = len(chunk_output) if isinstance(chunk_output, list) else 0
                            if (
                                isinstance(chunk_output, list)
                                and chunk_output
                                and isinstance(chunk_output[0], dict)
                                and isinstance(chunk_output[0].get("rows"), (int, float))
                            ):
                                chunk_rows = int(chunk_output[0].get("rows") or 0)
                            live_processed_rows += max(0, int(chunk_rows))

                            if websocket_manager:
                                progress_entry = {
                                    "nodeId": nid,
                                    "nodeLabel": label,
                                    "timestamp": datetime.utcnow().isoformat(),
                                    "status": "running",
                                    "message": (
                                        f"⟳ Running {label}… "
                                        f"{live_processed_rows:,} rows "
                                        f"({chunk_idx + 1}/{chunk_batches} chunks)"
                                    ),
                                    "rows": live_processed_rows,
                                }
                                await websocket_manager.broadcast(execution_id, {
                                    "type": "node_progress",
                                    "nodeId": nid,
                                    "rows": live_processed_rows,
                                    "chunk_index": chunk_idx + 1,
                                    "chunk_total": chunk_batches,
                                    "log_entry": progress_entry,
                                })
                            _raise_if_aborted()
                    else:
                        _raise_if_aborted()
                        output = await self._execute_node(
                            node_type,
                            config,
                            upstream_data,
                            incoming_by_source=incoming_by_source,
                            incoming_order=incoming_order,
                            execution_context={
                                "execution_id": execution_id,
                                "pipeline_id": pipeline_id,
                                "node_id": nid,
                                "node_label": label,
                                "mode": mode,
                                "stream_iteration": stream_idx,
                                "runtime": runtime,
                                "pipeline_state": pipeline_state,
                                "profile_state_by_node": profile_state_by_node,
                                "node_warnings": node_warnings,
                                "emit_node_progress": _emit_live_node_progress,
                                "node_progress_every": progress_every,
                                "should_abort": should_abort,
                                "raise_if_aborted": _raise_if_aborted,
                            },
                        )
                        _raise_if_aborted()

                    incremental_note = ""
                    if (
                        mode in {"incremental", "streaming"}
                        and self._is_source_node(node_type)
                        and runtime["incremental_field"]
                        and isinstance(output, list)
                    ):
                        checkpoint_key = f"{nid}:{runtime['incremental_field']}"
                        previous_checkpoint = incremental_checkpoints.get(checkpoint_key)
                        progress_every = max(
                            500,
                            min(int(runtime.get("batch_size") or 5000), 20000),
                        )

                        def _emit_incremental_progress(progress: Dict[str, Any]) -> None:
                            processed_now = int(progress.get("processed_rows") or 0)
                            total_now = int(progress.get("total_rows") or 0)
                            candidate_now = int(progress.get("candidate_rows") or 0)
                            kept_now = int(progress.get("kept_rows") or 0)
                            dropped_now = int(progress.get("dropped_rows") or 0)
                            log_entry["rows"] = kept_now
                            log_entry["incremental_counter"] = {
                                "processed_rows": processed_now,
                                "total_rows": total_now,
                                "candidate_rows": candidate_now,
                                "kept_rows": kept_now,
                                "dropped_rows": dropped_now,
                            }
                            log_entry["message"] = (
                                f"⟳ Running {label}… incremental "
                                f"{processed_now:,}/{total_now:,} scanned "
                                f"| kept={kept_now:,} dropped={dropped_now:,}"
                            )

                            if websocket_manager:
                                try:
                                    asyncio.create_task(websocket_manager.broadcast(execution_id, {
                                        "type": "node_progress",
                                        "nodeId": nid,
                                        "rows": kept_now,
                                        "incremental_scanned": processed_now,
                                        "incremental_kept": kept_now,
                                        "incremental_dropped": dropped_now,
                                        "log_entry": dict(log_entry),
                                    }))
                                except Exception:
                                    pass
                            if on_node_done:
                                try:
                                    on_node_done(logs)
                                except Exception:
                                    pass

                        filtered, next_checkpoint, scanned = self._apply_incremental_filter(
                            output,
                            runtime["incremental_field"],
                            previous_checkpoint,
                            on_progress=_emit_incremental_progress,
                            progress_every=progress_every,
                            should_abort=should_abort,
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
                    warning_note = ""
                    if node_warnings:
                        deduped_warnings: List[str] = []
                        seen_warning_text = set()
                        for warn in node_warnings:
                            text = str(warn or "").strip()
                            if not text or text in seen_warning_text:
                                continue
                            seen_warning_text.add(text)
                            deduped_warnings.append(text)
                        if deduped_warnings:
                            first_warning = deduped_warnings[0].replace("\n", " ")
                            if len(first_warning) > 180:
                                first_warning = f"{first_warning[:177]}..."
                            warning_note = f" | warnings={len(deduped_warnings)} | {first_warning}"
                            log_entry["warning_count"] = len(deduped_warnings)
                            log_entry["warnings"] = deduped_warnings[:20]

                    if (
                        isinstance(output, list)
                        and output
                        and isinstance(output[0], dict)
                        and "path" in output[0]
                    ):
                        file_path = output[0]["path"]
                        log_entry["message"] = f"✓ {label} — written: {file_path} ({row_count:,} rows){batch_note}{incremental_note}{warning_note}"
                        log_entry["output_path"] = file_path
                    else:
                        log_entry["message"] = f"✓ {label} — {row_count:,} rows{batch_note}{incremental_note}{warning_note}"

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
                    _raise_if_aborted()
                    await asyncio.sleep(runtime["streaming_interval_seconds"])

        if pipeline_id:
            # Strict profile persistence: LMDB only.
            if profile_state_by_node:
                saved_to_lmdb = self._save_profile_state_by_node(pipeline_id, profile_state_by_node)
                if not saved_to_lmdb:
                    raise RuntimeError(
                        "Failed to persist profile state to LMDB. "
                        "Runtime JSON fallback is disabled for profiling."
                    )
            pipeline_state.pop("profile_documents", None)
            self._clear_runtime_profile_state_by_node(pipeline_id)
            self._save_runtime_state(runtime_state)

        return {"node_results": results, "logs": logs, "rows_processed": total_rows}

    async def _execute_node(
        self,
        node_type: str,
        config: dict,
        upstream: list,
        incoming_by_source: Optional[Dict[str, list]] = None,
        incoming_order: Optional[List[str]] = None,
        execution_context: Optional[Dict[str, Any]] = None,
    ) -> list:
        """Dispatch to the correct connector/transform handler."""
        await asyncio.sleep(0.3)  # Simulate async I/O

        # ─── TRIGGERS ──────────────────────────────────────────
        if node_type in ("manual_trigger", "schedule_trigger", "webhook_trigger"):
            return []

        # ─── SOURCES ───────────────────────────────────────────
        elif node_type == "postgres_source":
            return await self._execute_postgres(config, execution_context=execution_context)
        elif node_type == "mysql_source":
            return await self._execute_mysql(config, execution_context=execution_context)
        elif node_type == "oracle_source":
            return await self._execute_oracle(config, execution_context=execution_context)
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
        elif node_type == "lmdb_source":
            return await self._execute_lmdb(config, execution_context=execution_context)
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
            return self._transform_map(upstream, config, execution_context=execution_context)
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
            return await self._execute_destination(node_type, config, upstream, execution_context=execution_context)

        # ─── FLOW CONTROL ──────────────────────────────────────
        elif node_type == "condition_node":
            return self._flow_condition(upstream, config)
        elif node_type == "merge_node":
            return upstream

        return upstream

    # ─── SOURCE IMPLEMENTATIONS ────────────────────────────────────────────────

    async def _execute_postgres(
        self,
        config: dict,
        execution_context: Optional[Dict[str, Any]] = None,
    ) -> list:
        node_warnings = execution_context.get("node_warnings") if isinstance(execution_context, dict) else None

        def _warn(msg: str) -> None:
            logger.warning(msg)
            if isinstance(node_warnings, list) and msg not in node_warnings:
                node_warnings.append(msg)

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
            query = str(config.get("query", "SELECT 1") or "SELECT 1").strip()
            params = None

            inc_ctx = self._get_incremental_source_context(execution_context)
            pushdown_attempted = False
            if inc_ctx and inc_ctx.get("last_checkpoint") is not None:
                pushdown_query = self._build_incremental_pushdown_query(
                    query,
                    str(inc_ctx.get("field_path") or ""),
                    dialect="postgres",
                )
                if pushdown_query:
                    query = pushdown_query
                    params = (inc_ctx.get("last_checkpoint"),)
                    pushdown_attempted = True
                else:
                    _warn(
                        f"Incremental pushdown skipped for PostgreSQL: "
                        f"unsupported incremental field '{inc_ctx.get('field_path')}'. "
                        f"Using post-fetch filtering."
                    )

            try:
                if params is None:
                    cursor.execute(query)
                else:
                    cursor.execute(query, params)
            except Exception:
                if not pushdown_attempted:
                    raise
                # Fallback: if pushdown fails (type/cast/sql syntax), run base query.
                base_query = str(config.get("query", "SELECT 1") or "SELECT 1").strip().rstrip(";")
                _warn("Incremental pushdown failed for PostgreSQL query; fell back to full-source fetch + filter.")
                cursor.execute(base_query)

            cols = [desc[0] for desc in cursor.description]
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return rows
        except Exception as e:
            logger.warning(f"PostgreSQL simulation mode: {e}")
            return self._mock_data(config, "postgres")

    async def _execute_mysql(
        self,
        config: dict,
        execution_context: Optional[Dict[str, Any]] = None,
    ) -> list:
        node_warnings = execution_context.get("node_warnings") if isinstance(execution_context, dict) else None

        def _warn(msg: str) -> None:
            logger.warning(msg)
            if isinstance(node_warnings, list) and msg not in node_warnings:
                node_warnings.append(msg)

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
            query = str(config.get("query", "SELECT 1") or "SELECT 1").strip()
            params = None
            inc_ctx = self._get_incremental_source_context(execution_context)
            pushdown_attempted = False
            if inc_ctx and inc_ctx.get("last_checkpoint") is not None:
                pushdown_query = self._build_incremental_pushdown_query(
                    query,
                    str(inc_ctx.get("field_path") or ""),
                    dialect="mysql",
                )
                if pushdown_query:
                    query = pushdown_query
                    params = (inc_ctx.get("last_checkpoint"),)
                    pushdown_attempted = True
                else:
                    _warn(
                        f"Incremental pushdown skipped for MySQL: "
                        f"unsupported incremental field '{inc_ctx.get('field_path')}'. "
                        f"Using post-fetch filtering."
                    )
            with conn.cursor() as cursor:
                try:
                    if params is None:
                        cursor.execute(query)
                    else:
                        cursor.execute(query, params)
                except Exception:
                    if not pushdown_attempted:
                        raise
                    base_query = str(config.get("query", "SELECT 1") or "SELECT 1").strip().rstrip(";")
                    _warn("Incremental pushdown failed for MySQL query; fell back to full-source fetch + filter.")
                    cursor.execute(base_query)
                rows = cursor.fetchall()
            conn.close()
            return list(rows)
        except Exception as e:
            return self._mock_data(config, "mysql")

    async def _execute_oracle(
        self,
        config: dict,
        execution_context: Optional[Dict[str, Any]] = None,
    ) -> list:
        node_warnings = execution_context.get("node_warnings") if isinstance(execution_context, dict) else None

        def _warn(msg: str) -> None:
            logger.warning(msg)
            if isinstance(node_warnings, list) and msg not in node_warnings:
                node_warnings.append(msg)

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
            params = None
            inc_ctx = self._get_incremental_source_context(execution_context)
            pushdown_attempted = False
            if inc_ctx and inc_ctx.get("last_checkpoint") is not None:
                pushdown_query = self._build_incremental_pushdown_query(
                    query,
                    str(inc_ctx.get("field_path") or ""),
                    dialect="oracle",
                )
                if pushdown_query:
                    query = pushdown_query
                    params = {"checkpoint": inc_ctx.get("last_checkpoint")}
                    pushdown_attempted = True
                else:
                    _warn(
                        f"Incremental pushdown skipped for Oracle: "
                        f"unsupported incremental field '{inc_ctx.get('field_path')}'. "
                        f"Using post-fetch filtering."
                    )
            try:
                if params is None:
                    cursor.execute(query)
                else:
                    cursor.execute(query, params)
            except Exception:
                if not pushdown_attempted:
                    raise
                base_query = str(config.get("query", "") or "").strip().rstrip(";")
                _warn("Incremental pushdown failed for Oracle query; fell back to full-source fetch + filter.")
                if not base_query:
                    table = str(config.get("table", "") or "").strip()
                    if not table:
                        raise
                    limit_raw = config.get("limit", 1000)
                    try:
                        limit = max(1, min(int(limit_raw), 50000))
                    except Exception:
                        limit = 1000
                    base_query = f"SELECT * FROM {table} FETCH FIRST {limit} ROWS ONLY"
                cursor.execute(base_query)
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

    async def _execute_lmdb(
        self,
        config: dict,
        execution_context: Optional[Dict[str, Any]] = None,
    ) -> list:
        if lmdb is None:
            raise RuntimeError("LMDB dependency is not installed. Install with: pip install lmdb")

        should_abort_cb = None
        raise_if_aborted_cb = None
        if isinstance(execution_context, dict):
            if callable(execution_context.get("should_abort")):
                should_abort_cb = execution_context.get("should_abort")
            if callable(execution_context.get("raise_if_aborted")):
                raise_if_aborted_cb = execution_context.get("raise_if_aborted")

        def _raise_if_aborted() -> None:
            if callable(raise_if_aborted_cb):
                raise_if_aborted_cb()
                return
            if callable(should_abort_cb):
                try:
                    if bool(should_abort_cb()):
                        raise ExecutionAbortedError("Execution aborted during LMDB source read.")
                except ExecutionAbortedError:
                    raise
                except Exception:
                    pass

        raw_path = (
            config.get("env_path")
            or config.get("file_path")
            or config.get("path")
            or ""
        )
        env_path = self._resolve_lmdb_env_path(str(raw_path))

        db_name = str(config.get("db_name", "") or "").strip()
        value_format = str(config.get("value_format", "auto") or "auto").strip().lower() or "auto"
        flatten_json_values = bool(config.get("flatten_json_values", True))
        expand_profile_documents = bool(config.get("expand_profile_documents", True))
        include_value_kind = bool(config.get("include_value_kind", True))
        key_contains = str(config.get("key_contains", "") or "").strip()
        value_contains = str(config.get("value_contains", "") or "").strip().lower()
        key_prefix = str(config.get("key_prefix", "") or "").strip()
        start_key = str(config.get("start_key", "") or "").strip()
        end_key = str(config.get("end_key", "") or "").strip()
        global_filter_column = str(config.get("global_filter_column", "") or "").strip()
        raw_global_filter_values = config.get("global_filter_values", None)
        if raw_global_filter_values is None:
            fallback_value = str(config.get("global_filter_value", "") or "").strip()
            raw_global_filter_values = [fallback_value] if fallback_value else []
        if isinstance(raw_global_filter_values, str):
            try:
                parsed = json.loads(raw_global_filter_values)
                if isinstance(parsed, list):
                    raw_global_filter_values = parsed
                else:
                    raw_global_filter_values = [raw_global_filter_values]
            except Exception:
                raw_global_filter_values = [raw_global_filter_values]
        elif not isinstance(raw_global_filter_values, (list, tuple, set)):
            raw_global_filter_values = [raw_global_filter_values]
        global_filter_values: List[str] = []
        global_filter_seen = set()
        for item in list(raw_global_filter_values):
            text = str(item if item is not None else "").strip()
            if not text or text in global_filter_seen:
                continue
            global_filter_seen.add(text)
            global_filter_values.append(text)
        raw_column_filters = config.get("column_filters", {})
        if isinstance(raw_column_filters, str):
            try:
                raw_column_filters = json.loads(raw_column_filters)
            except Exception:
                raw_column_filters = {}
        if not isinstance(raw_column_filters, dict):
            raw_column_filters = {}
        column_filters: Dict[str, set] = {}
        for raw_name, raw_values in raw_column_filters.items():
            col_name = str(raw_name or "").strip()
            if not col_name:
                continue
            if isinstance(raw_values, (list, tuple, set)):
                values_iter = list(raw_values)
            else:
                values_iter = [raw_values]
            normalized_values = []
            for item in values_iter:
                text = str(item if item is not None else "").strip()
                if text:
                    normalized_values.append(text)
            if normalized_values:
                column_filters[col_name] = set(normalized_values)

        raw_limit = config.get("limit", 1000)
        try:
            if raw_limit is None or str(raw_limit).strip() == "":
                limit = 1000
            else:
                limit = int(raw_limit)
        except Exception:
            limit = 1000
        # limit <= 0 means no cap (read all matching records).
        if limit < 0:
            limit = 0
        try:
            preview_offset = int(config.get("preview_offset", 0) or 0)
        except Exception:
            preview_offset = 0
        preview_offset = max(0, preview_offset)

        try:
            max_dbs = int(config.get("max_dbs", 16) or 16)
        except Exception:
            max_dbs = 16
        max_dbs = max(1, min(max_dbs, 256))

        rows: List[Dict[str, Any]] = []
        matched_rows = 0
        env = None

        def _to_cell_text(value: Any) -> str:
            if value is None:
                return ""
            if isinstance(value, str):
                return value
            if isinstance(value, (int, float, bool)):
                return str(value)
            try:
                return json.dumps(value, ensure_ascii=False, default=str)
            except Exception:
                return str(value)

        def _row_matches_filters(row: Dict[str, Any]) -> bool:
            if global_filter_values:
                if global_filter_column and global_filter_column != "__ALL__":
                    targets = [global_filter_column]
                else:
                    targets = list(row.keys())
                matched = False
                for name in targets:
                    text = _to_cell_text(row.get(name)).strip()
                    text_lower = text.lower()
                    for query in global_filter_values:
                        if query == "__LMDB_EMPTY__":
                            if text == "":
                                matched = True
                                break
                        elif query.lower() in text_lower:
                            matched = True
                            break
                    if matched:
                        break
                if not matched:
                    return False

            if column_filters:
                for name, allowed_values in column_filters.items():
                    text = _to_cell_text(row.get(name)).strip()
                    if "__LMDB_EMPTY__" in allowed_values and text == "":
                        continue
                    if text in allowed_values:
                        continue
                    return False
            return True

        try:
            _raise_if_aborted()
            env = lmdb.open(
                env_path,
                readonly=True,
                lock=False,
                readahead=False,
                max_dbs=max_dbs,
                subdir=True,
            )

            dbi = None
            if db_name:
                dbi = env.open_db(db_name.encode("utf-8"), create=False)

            prefix_bytes = key_prefix.encode("utf-8") if key_prefix else None
            start_bytes = start_key.encode("utf-8") if start_key else None
            end_bytes = end_key.encode("utf-8") if end_key else None

            with env.begin(write=False, db=dbi) as txn:
                cursor = txn.cursor()
                if start_bytes is not None:
                    has_item = cursor.set_range(start_bytes)
                elif prefix_bytes is not None:
                    has_item = cursor.set_range(prefix_bytes)
                else:
                    has_item = cursor.first()

                while has_item and (limit == 0 or len(rows) < limit):
                    _raise_if_aborted()
                    key_bytes, value_bytes = cursor.item()

                    if prefix_bytes is not None and not key_bytes.startswith(prefix_bytes):
                        break
                    if end_bytes is not None and key_bytes > end_bytes:
                        break

                    key_text = key_bytes.decode("utf-8", errors="replace")
                    if key_contains and key_contains not in key_text:
                        has_item = cursor.next()
                        continue

                    decoded_value, value_kind = self._decode_lmdb_value(value_bytes, value_format)
                    if value_contains:
                        try:
                            if isinstance(decoded_value, str):
                                haystack = decoded_value
                            else:
                                haystack = json.dumps(decoded_value, ensure_ascii=False)
                        except Exception:
                            haystack = str(decoded_value)
                        if value_contains not in haystack.lower():
                            has_item = cursor.next()
                            continue

                    if flatten_json_values and expand_profile_documents:
                        expanded_rows = self._expand_lmdb_profile_documents(
                            key_text,
                            decoded_value,
                            include_value_kind=include_value_kind,
                            value_kind=value_kind,
                        )
                        if expanded_rows:
                            for expanded_row in expanded_rows:
                                _raise_if_aborted()
                                if not _row_matches_filters(expanded_row):
                                    continue
                                if matched_rows < preview_offset:
                                    matched_rows += 1
                                    continue
                                if limit > 0 and len(rows) >= limit:
                                    break
                                rows.append(expanded_row)
                                matched_rows += 1
                            if limit > 0 and len(rows) >= limit:
                                break
                            has_item = cursor.next()
                            continue

                    if flatten_json_values and isinstance(decoded_value, dict):
                        row: Dict[str, Any] = {"lmdb_key": key_text, **decoded_value}
                        if include_value_kind:
                            row["_lmdb_value_kind"] = value_kind
                    else:
                        row = {"lmdb_key": key_text, "lmdb_value": decoded_value}
                        if include_value_kind:
                            row["_lmdb_value_kind"] = value_kind

                    if not _row_matches_filters(row):
                        has_item = cursor.next()
                        continue

                    if matched_rows < preview_offset:
                        matched_rows += 1
                        has_item = cursor.next()
                        continue
                    rows.append(row)
                    matched_rows += 1
                    has_item = cursor.next()

        except ExecutionAbortedError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Failed to read LMDB source: {exc}")
        finally:
            if env is not None:
                try:
                    env.close()
                except Exception:
                    pass

        return rows

    async def _summarize_lmdb(self, config: dict) -> Dict[str, Any]:
        if lmdb is None:
            raise RuntimeError("LMDB dependency is not installed. Install with: pip install lmdb")

        raw_path = (
            config.get("env_path")
            or config.get("file_path")
            or config.get("path")
            or ""
        )
        env_path = self._resolve_lmdb_env_path(str(raw_path))

        db_name = str(config.get("db_name", "") or "").strip()
        value_format = str(config.get("value_format", "auto") or "auto").strip().lower() or "auto"
        flatten_json_values = bool(config.get("flatten_json_values", True))
        expand_profile_documents = bool(config.get("expand_profile_documents", True))
        include_value_kind = bool(config.get("include_value_kind", True))
        key_contains = str(config.get("key_contains", "") or "").strip()
        value_contains = str(config.get("value_contains", "") or "").strip().lower()
        key_prefix = str(config.get("key_prefix", "") or "").strip()
        start_key = str(config.get("start_key", "") or "").strip()
        end_key = str(config.get("end_key", "") or "").strip()
        global_filter_column = str(config.get("global_filter_column", "") or "").strip()
        raw_global_filter_values = config.get("global_filter_values", None)
        if raw_global_filter_values is None:
            fallback_value = str(config.get("global_filter_value", "") or "").strip()
            raw_global_filter_values = [fallback_value] if fallback_value else []
        if isinstance(raw_global_filter_values, str):
            try:
                parsed = json.loads(raw_global_filter_values)
                if isinstance(parsed, list):
                    raw_global_filter_values = parsed
                else:
                    raw_global_filter_values = [raw_global_filter_values]
            except Exception:
                raw_global_filter_values = [raw_global_filter_values]
        elif not isinstance(raw_global_filter_values, (list, tuple, set)):
            raw_global_filter_values = [raw_global_filter_values]
        global_filter_values: List[str] = []
        global_filter_seen = set()
        for item in list(raw_global_filter_values):
            text = str(item if item is not None else "").strip()
            if not text or text in global_filter_seen:
                continue
            global_filter_seen.add(text)
            global_filter_values.append(text)

        raw_column_filters = config.get("column_filters", {})
        if isinstance(raw_column_filters, str):
            try:
                raw_column_filters = json.loads(raw_column_filters)
            except Exception:
                raw_column_filters = {}
        if not isinstance(raw_column_filters, dict):
            raw_column_filters = {}
        column_filters: Dict[str, set] = {}
        for raw_name, raw_values in raw_column_filters.items():
            col_name = str(raw_name or "").strip()
            if not col_name:
                continue
            if isinstance(raw_values, (list, tuple, set)):
                values_iter = list(raw_values)
            else:
                values_iter = [raw_values]
            normalized_values = []
            for item in values_iter:
                text = str(item if item is not None else "").strip()
                if text:
                    normalized_values.append(text)
            if normalized_values:
                column_filters[col_name] = set(normalized_values)

        try:
            summary_scan_limit = int(config.get("summary_scan_limit", 0) or 0)
        except Exception:
            summary_scan_limit = 0
        summary_scan_limit = max(0, summary_scan_limit)

        try:
            max_dbs = int(config.get("max_dbs", 16) or 16)
        except Exception:
            max_dbs = 16
        max_dbs = max(1, min(max_dbs, 256))

        env = None
        scan_started_at = datetime.utcnow()
        scan_capped = False
        scanned_entries = 0
        matched_rows = 0
        profile_rows = 0
        non_profile_rows = 0
        unique_keys: set = set()
        unique_entities: set = set()
        profile_like_keys: set = set()
        txn_latency_samples = 0
        txn_latency_sum_ms = 0.0
        txn_latency_min_ms: Optional[float] = None
        txn_latency_max_ms: Optional[float] = None
        node_incremental_stats_by_key: Dict[str, Dict[str, int]] = {}

        def _to_cell_text(value: Any) -> str:
            if value is None:
                return ""
            if isinstance(value, str):
                return value
            if isinstance(value, (int, float, bool)):
                return str(value)
            try:
                return json.dumps(value, ensure_ascii=False, default=str)
            except Exception:
                return str(value)

        def _row_matches_filters(row: Dict[str, Any]) -> bool:
            if global_filter_values:
                if global_filter_column and global_filter_column != "__ALL__":
                    targets = [global_filter_column]
                else:
                    targets = list(row.keys())
                matched = False
                for name in targets:
                    text = _to_cell_text(row.get(name)).strip()
                    text_lower = text.lower()
                    for query in global_filter_values:
                        if query == "__LMDB_EMPTY__":
                            if text == "":
                                matched = True
                                break
                        elif query.lower() in text_lower:
                            matched = True
                            break
                    if matched:
                        break
                if not matched:
                    return False

            if column_filters:
                for name, allowed_values in column_filters.items():
                    text = _to_cell_text(row.get(name)).strip()
                    if "__LMDB_EMPTY__" in allowed_values and text == "":
                        continue
                    if text in allowed_values:
                        continue
                    return False
            return True

        latency_ms_fields = (
            "latency_ms",
            "processing_latency_ms",
            "transaction_latency_ms",
            "txn_latency_ms",
            "response_time_ms",
            "duration_ms",
            "elapsed_ms",
            "processing_time_ms",
            "latency",
            "duration",
            "response_time",
            "processing_time",
        )
        latency_seconds_fields = (
            "latency_seconds",
            "duration_seconds",
            "elapsed_seconds",
            "response_time_seconds",
            "processing_time_seconds",
        )
        latency_time_pairs = (
            ("request_time", "response_time"),
            ("start_time", "end_time"),
            ("start_ts", "end_ts"),
            ("txn_start_time", "txn_end_time"),
            ("transaction_start_time", "transaction_end_time"),
            ("mintime", "maxtime"),
            ("min_txndate", "max_txndate"),
        )

        def _to_float(value: Any) -> Optional[float]:
            if value is None or isinstance(value, bool):
                return None
            if isinstance(value, (int, float)):
                if math.isfinite(float(value)):
                    return float(value)
                return None
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return None
                text = text.replace(",", "")
                try:
                    parsed = float(text)
                    if math.isfinite(parsed):
                        return parsed
                except Exception:
                    return None
            return None

        def _to_non_negative_int(value: Any) -> Optional[int]:
            parsed = _to_float(value)
            if parsed is None:
                return None
            try:
                as_int = int(parsed)
            except Exception:
                return None
            if as_int < 0:
                return None
            return as_int

        def _extract_transaction_latency_ms(row: Dict[str, Any]) -> Optional[float]:
            normalized: Dict[str, Any] = {}
            for raw_key, raw_value in row.items():
                key = str(raw_key or "").strip().lower()
                if not key:
                    continue
                normalized[key] = raw_value

            def _collect_nested_named_values(
                value: Any,
                wanted_keys: set,
                out: Dict[str, List[Any]],
                depth: int,
                budget: List[int],
            ) -> None:
                if budget[0] <= 0 or depth > 6:
                    return
                if isinstance(value, dict):
                    for raw_name, raw_child in value.items():
                        if budget[0] <= 0:
                            break
                        budget[0] -= 1
                        name = str(raw_name or "").strip().lower()
                        if name in wanted_keys and not isinstance(raw_child, (dict, list)):
                            bucket = out.get(name)
                            if bucket is None:
                                out[name] = [raw_child]
                            elif len(bucket) < 32:
                                bucket.append(raw_child)
                        if isinstance(raw_child, (dict, list)):
                            _collect_nested_named_values(raw_child, wanted_keys, out, depth + 1, budget)
                    return
                if isinstance(value, list):
                    for item in value:
                        if budget[0] <= 0:
                            break
                        budget[0] -= 1
                        if isinstance(item, (dict, list)):
                            _collect_nested_named_values(item, wanted_keys, out, depth + 1, budget)

            def _get_value_candidates(name: str, nested_map: Dict[str, List[Any]]) -> List[Any]:
                candidates: List[Any] = []
                if name in normalized:
                    candidates.append(normalized.get(name))
                nested_values = nested_map.get(name) or []
                if nested_values:
                    candidates.extend(nested_values)
                return candidates

            wanted_keys = set(latency_ms_fields) | set(latency_seconds_fields)
            for start_field, end_field in latency_time_pairs:
                wanted_keys.add(start_field)
                wanted_keys.add(end_field)
            nested_candidates: Dict[str, List[Any]] = {}

            def _ensure_nested_candidates() -> Dict[str, List[Any]]:
                nonlocal nested_candidates
                if nested_candidates:
                    return nested_candidates
                budget = [800]
                _collect_nested_named_values(row, wanted_keys, nested_candidates, 0, budget)
                return nested_candidates

            for field_name in latency_ms_fields:
                candidates = [normalized.get(field_name)] if field_name in normalized else []
                if not candidates:
                    candidates = _ensure_nested_candidates().get(field_name, [])
                for candidate in candidates:
                    parsed = _to_float(candidate)
                    if parsed is not None and parsed >= 0:
                        return parsed

            for field_name in latency_seconds_fields:
                candidates = [normalized.get(field_name)] if field_name in normalized else []
                if not candidates:
                    candidates = _ensure_nested_candidates().get(field_name, [])
                for candidate in candidates:
                    parsed = _to_float(candidate)
                    if parsed is not None and parsed >= 0:
                        return parsed * 1000.0

            nested_map = _ensure_nested_candidates()
            for start_field, end_field in latency_time_pairs:
                start_candidates = _get_value_candidates(start_field, nested_map)
                end_candidates = _get_value_candidates(end_field, nested_map)
                if not start_candidates or not end_candidates:
                    continue

                # Prefer index-wise pairing when both fields come from repeated structures.
                pair_len = min(len(start_candidates), len(end_candidates), 16)
                for idx in range(pair_len):
                    start_dt = self._parse_profile_event_time(start_candidates[idx])
                    end_dt = self._parse_profile_event_time(end_candidates[idx])
                    if start_dt is None or end_dt is None:
                        continue
                    delta_ms = (end_dt - start_dt).total_seconds() * 1000.0
                    if math.isfinite(delta_ms) and delta_ms >= 0:
                        return delta_ms

                # Fallback to first parsable pair.
                for start_value in start_candidates[:8]:
                    start_dt = self._parse_profile_event_time(start_value)
                    if start_dt is None:
                        continue
                    for end_value in end_candidates[:8]:
                        end_dt = self._parse_profile_event_time(end_value)
                        if end_dt is None:
                            continue
                        delta_ms = (end_dt - start_dt).total_seconds() * 1000.0
                        if math.isfinite(delta_ms) and delta_ms >= 0:
                            return delta_ms
            return None

        def _track_row(row: Dict[str, Any]) -> None:
            nonlocal matched_rows, profile_rows, non_profile_rows
            nonlocal txn_latency_samples, txn_latency_sum_ms, txn_latency_min_ms, txn_latency_max_ms
            matched_rows += 1

            key_text = str(row.get("lmdb_key") or "").strip()
            if key_text:
                unique_keys.add(key_text)
                if "profile" in key_text.lower():
                    profile_like_keys.add(key_text)

            entity_key = str(row.get("lmdb_entity_key") or "").strip()
            profile_source = str(row.get("_lmdb_profile_source") or "").strip().lower()
            is_profile_row = bool(entity_key) or profile_source == "documents" or profile_source == "profile"
            if is_profile_row:
                profile_rows += 1
            else:
                non_profile_rows += 1
            if entity_key:
                unique_entities.add(entity_key)

            latency_ms = _extract_transaction_latency_ms(row)
            if latency_ms is not None:
                txn_latency_samples += 1
                txn_latency_sum_ms += latency_ms
                if txn_latency_min_ms is None or latency_ms < txn_latency_min_ms:
                    txn_latency_min_ms = latency_ms
                if txn_latency_max_ms is None or latency_ms > txn_latency_max_ms:
                    txn_latency_max_ms = latency_ms

            node_stats = row.get("_lmdb_node_stats")
            if isinstance(node_stats, dict):
                processed = _to_non_negative_int(node_stats.get("custom_fields_incremental_processed_rows"))
                validated = _to_non_negative_int(node_stats.get("custom_fields_incremental_validated_rows"))
                output_rows = _to_non_negative_int(node_stats.get("custom_fields_incremental_output_rows"))
                if processed is not None or validated is not None or output_rows is not None:
                    stats_key = key_text or str(row.get("lmdb_entity_key") or "") or "__lmdb_default__"
                    existing = node_incremental_stats_by_key.get(stats_key)
                    if not isinstance(existing, dict):
                        existing = {"processed": 0, "validated": 0, "output_rows": 0}
                    if processed is not None:
                        existing["processed"] = max(int(existing.get("processed") or 0), int(processed))
                    if validated is not None:
                        existing["validated"] = max(int(existing.get("validated") or 0), int(validated))
                    if output_rows is not None:
                        existing["output_rows"] = max(int(existing.get("output_rows") or 0), int(output_rows))
                    node_incremental_stats_by_key[stats_key] = existing

        try:
            env = lmdb.open(
                env_path,
                readonly=True,
                lock=False,
                readahead=False,
                max_dbs=max_dbs,
                subdir=True,
            )

            dbi = None
            if db_name:
                dbi = env.open_db(db_name.encode("utf-8"), create=False)

            prefix_bytes = key_prefix.encode("utf-8") if key_prefix else None
            start_bytes = start_key.encode("utf-8") if start_key else None
            end_bytes = end_key.encode("utf-8") if end_key else None

            with env.begin(write=False, db=dbi) as txn:
                cursor = txn.cursor()
                if start_bytes is not None:
                    has_item = cursor.set_range(start_bytes)
                elif prefix_bytes is not None:
                    has_item = cursor.set_range(prefix_bytes)
                else:
                    has_item = cursor.first()

                while has_item:
                    scanned_entries += 1
                    key_bytes, value_bytes = cursor.item()

                    if prefix_bytes is not None and not key_bytes.startswith(prefix_bytes):
                        break
                    if end_bytes is not None and key_bytes > end_bytes:
                        break

                    key_text = key_bytes.decode("utf-8", errors="replace")
                    if key_contains and key_contains not in key_text:
                        has_item = cursor.next()
                        continue

                    decoded_value, value_kind = self._decode_lmdb_value(value_bytes, value_format)
                    if value_contains:
                        try:
                            if isinstance(decoded_value, str):
                                haystack = decoded_value
                            else:
                                haystack = json.dumps(decoded_value, ensure_ascii=False)
                        except Exception:
                            haystack = str(decoded_value)
                        if value_contains not in haystack.lower():
                            has_item = cursor.next()
                            continue

                    if flatten_json_values and expand_profile_documents:
                        expanded_rows = self._expand_lmdb_profile_documents(
                            key_text,
                            decoded_value,
                            include_value_kind=include_value_kind,
                            value_kind=value_kind,
                        )
                        if expanded_rows:
                            for expanded_row in expanded_rows:
                                if not _row_matches_filters(expanded_row):
                                    continue
                                _track_row(expanded_row)
                                if summary_scan_limit > 0 and matched_rows >= summary_scan_limit:
                                    scan_capped = True
                                    break
                            if scan_capped:
                                break
                            has_item = cursor.next()
                            continue

                    if flatten_json_values and isinstance(decoded_value, dict):
                        row: Dict[str, Any] = {"lmdb_key": key_text, **decoded_value}
                        if include_value_kind:
                            row["_lmdb_value_kind"] = value_kind
                    else:
                        row = {"lmdb_key": key_text, "lmdb_value": decoded_value}
                        if include_value_kind:
                            row["_lmdb_value_kind"] = value_kind

                    if not _row_matches_filters(row):
                        has_item = cursor.next()
                        continue

                    _track_row(row)
                    if summary_scan_limit > 0 and matched_rows >= summary_scan_limit:
                        scan_capped = True
                        break
                    has_item = cursor.next()

        except Exception as exc:
            raise RuntimeError(f"Failed to summarize LMDB source: {exc}")
        finally:
            if env is not None:
                try:
                    env.close()
                except Exception:
                    pass

        scan_elapsed_seconds = max(0.0, (datetime.utcnow() - scan_started_at).total_seconds())
        processing_latency_rps = (
            (matched_rows / scan_elapsed_seconds)
            if scan_elapsed_seconds > 0 and matched_rows >= 0
            else None
        )
        custom_fields_incremental_processed_rows = sum(
            int((stats or {}).get("processed") or 0)
            for stats in node_incremental_stats_by_key.values()
            if isinstance(stats, dict)
        )
        custom_fields_incremental_validated_rows = sum(
            int((stats or {}).get("validated") or 0)
            for stats in node_incremental_stats_by_key.values()
            if isinstance(stats, dict)
        )
        custom_fields_incremental_output_rows = sum(
            int((stats or {}).get("output_rows") or 0)
            for stats in node_incremental_stats_by_key.values()
            if isinstance(stats, dict)
        )

        return {
            "total_rows": matched_rows,
            "profile_rows": profile_rows,
            "non_profile_rows": non_profile_rows,
            "unique_keys": len(unique_keys),
            "unique_entities": len(unique_entities),
            "profile_like_keys": len(profile_like_keys),
            "txn_latency_samples": txn_latency_samples,
            "txn_latency_avg_ms": (txn_latency_sum_ms / txn_latency_samples) if txn_latency_samples > 0 else None,
            "txn_latency_min_ms": txn_latency_min_ms,
            "txn_latency_max_ms": txn_latency_max_ms,
            "processing_latency_rps": processing_latency_rps,
            "records_processed_per_second": processing_latency_rps,
            "scan_elapsed_seconds": scan_elapsed_seconds,
            "scan_capped": scan_capped,
            "scan_limit": summary_scan_limit,
            "scanned_entries": scanned_entries,
            "custom_fields_incremental_processed_rows": custom_fields_incremental_processed_rows,
            "custom_fields_incremental_validated_rows": custom_fields_incremental_validated_rows,
            "custom_fields_incremental_output_rows": custom_fields_incremental_output_rows,
            "generated_at": datetime.utcnow().isoformat(),
        }

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

    def _normalize_single_value_output(self, value: Any) -> str:
        mode = str(value or "").strip().lower()
        if mode in {"plain_text", "text", "plain", "string", "str"}:
            return "plain_text"
        return "json"

    def _apply_single_value_output_mode(self, value: Any, mode: Any) -> Any:
        output_mode = self._normalize_single_value_output(mode)
        safe_value = self._json_safe_value(value)
        if output_mode == "plain_text":
            if safe_value is None:
                return ""
            if isinstance(safe_value, (dict, list)):
                try:
                    return json.dumps(safe_value, ensure_ascii=False)
                except Exception:
                    return str(safe_value)
            return str(safe_value)
        return safe_value

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
            single_value_output = self._normalize_single_value_output(
                item.get("single_value_output", item.get("value_output", item.get("output_format")))
            )
            enabled_raw = item.get("enabled", True)
            if isinstance(enabled_raw, bool):
                enabled = enabled_raw
            elif isinstance(enabled_raw, (int, float)):
                enabled = enabled_raw != 0
            elif isinstance(enabled_raw, str):
                norm = enabled_raw.strip().lower()
                if norm in {"0", "false", "no", "off", "n"}:
                    enabled = False
                elif norm in {"1", "true", "yes", "on", "y"}:
                    enabled = True
                else:
                    enabled = True
            else:
                enabled = True
            if not enabled:
                continue

            parsed.append(
                {
                    "name": name,
                    "mode": mode,
                    "single_value_output": single_value_output,
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

    def _stable_json_token(self, value: Any) -> str:
        try:
            return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        except Exception:
            return str(value)

    def _json_safe_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        if isinstance(value, (str, int, float, bool)):
            return value

        to_py = getattr(value, "to_pydatetime", None)
        if callable(to_py):
            try:
                value = to_py()
            except Exception:
                pass

        if isinstance(value, datetime):
            if (
                value.hour == 0
                and value.minute == 0
                and value.second == 0
                and value.microsecond == 0
            ):
                return value.date().isoformat()
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, time):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(k): self._json_safe_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._json_safe_value(v) for v in value]
        return str(value)

    def _split_profile_path(self, path: Any) -> List[Any]:
        import re

        text = str(path or "").strip()
        if not text:
            return []
        out: List[Any] = []
        for part in text.split("."):
            token = part.strip()
            if not token:
                continue
            pieces = re.findall(r"([^\[\]]+)|\[(\d+)\]", token)
            if not pieces:
                out.append(token)
                continue
            for name, idx in pieces:
                if name:
                    out.append(name)
                elif idx:
                    try:
                        out.append(int(idx))
                    except Exception:
                        out.append(idx)
        return out

    def _get_profile_path_value(self, data: Any, path: Any, default: Any = None) -> Any:
        tokens = self._split_profile_path(path)
        if not tokens:
            return data if data is not None else default

        current = data
        for token in tokens:
            if isinstance(token, int):
                if isinstance(current, list) and 0 <= token < len(current):
                    current = current[token]
                    continue
                return default

            if isinstance(current, dict):
                if token in current:
                    current = current[token]
                    continue
                token_norm = str(token).strip().lower()
                matched_key = None
                for k in current.keys():
                    if str(k).strip().lower() == token_norm:
                        matched_key = k
                        break
                if matched_key is None:
                    return default
                current = current.get(matched_key)
                continue

            return default

        return current if current is not None else default

    def _set_profile_path_value(self, data: Dict[str, Any], path: Any, value: Any) -> None:
        tokens = self._split_profile_path(path)
        if not tokens:
            return

        current: Any = data
        for idx, token in enumerate(tokens):
            is_last = idx == len(tokens) - 1
            next_token = tokens[idx + 1] if not is_last else None

            if isinstance(token, int):
                if not isinstance(current, list):
                    return
                while len(current) <= token:
                    current.append({} if not isinstance(next_token, int) else [])
                if is_last:
                    current[token] = value
                else:
                    nxt = current[token]
                    if isinstance(next_token, int) and not isinstance(nxt, list):
                        nxt = []
                        current[token] = nxt
                    elif not isinstance(next_token, int) and not isinstance(nxt, dict):
                        nxt = {}
                        current[token] = nxt
                    current = nxt
                continue

            key = str(token)
            if not isinstance(current, dict):
                return
            if is_last:
                current[key] = value
                return

            nxt = current.get(key)
            if isinstance(next_token, int):
                if not isinstance(nxt, list):
                    nxt = []
                    current[key] = nxt
            else:
                if not isinstance(nxt, dict):
                    nxt = {}
                    current[key] = nxt
            current = nxt

    def _parse_profile_event_time(self, value: Any) -> Optional[datetime]:
        if value is None:
            return None

        cache = self._profile_event_time_cache
        _missing = object()

        def _cache_get(key: Any) -> Any:
            if key is None:
                return _missing
            return cache.get(key, _missing)

        def _cache_put(key: Any, parsed: Optional[datetime]) -> Optional[datetime]:
            if key is not None:
                if len(cache) > 200000:
                    cache.clear()
                cache[key] = parsed
            return parsed

        to_py = getattr(value, "to_pydatetime", None)
        if callable(to_py):
            try:
                value = to_py()
            except Exception:
                pass

        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            key = ("d", value.toordinal())
            cached = _cache_get(key)
            if cached is not _missing:
                return cached
            return _cache_put(key, datetime.combine(value, time.min))
        if isinstance(value, (int, float)):
            key = ("n", float(value))
            cached = _cache_get(key)
            if cached is not _missing:
                return cached
            try:
                ts = float(value)
                # Support epoch-millis as well.
                if ts > 1_000_000_000_000:
                    ts = ts / 1000.0
                return _cache_put(key, datetime.utcfromtimestamp(ts))
            except Exception:
                return _cache_put(key, None)
        if isinstance(value, str):
            text = " ".join(value.strip().split())
            if not text:
                return None
            key = ("s", text)
            cached = _cache_get(key)
            if cached is not _missing:
                return cached
            normalized = self._profile_fractional_seconds_re.sub(
                lambda m: "." + m.group(1)[:6].ljust(6, "0"),
                text,
                count=1,
            )
            candidates = [normalized]
            if normalized.endswith("Z"):
                candidates.append(normalized[:-1] + "+00:00")

            formats = (
                "%Y-%m-%d",
                "%Y%m%d",
                "%d-%m-%Y",
                "%d-%m-%y",
                "%d/%m/%Y",
                "%d/%m/%y",
                "%m/%d/%Y",
                "%m/%d/%y",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%d-%m %H:%M:%S",
                "%Y-%d-%m %H:%M:%S.%f",
                "%d-%m-%Y %H:%M:%S",
                "%d-%m-%Y %H:%M:%S.%f",
                "%d-%m-%y %H:%M:%S",
                "%d-%m-%y %H:%M:%S.%f",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M:%S.%f",
                "%d/%m/%y %H:%M:%S",
                "%d/%m/%y %H:%M:%S.%f",
                "%d-%m-%Y %I:%M:%S %p",
                "%d-%m-%Y %I:%M:%S.%f %p",
                "%d-%m-%y %I:%M:%S %p",
                "%d-%m-%y %I:%M:%S.%f %p",
                "%d/%m/%Y %I:%M:%S %p",
                "%d/%m/%Y %I:%M:%S.%f %p",
                "%d/%m/%y %I:%M:%S %p",
                "%d/%m/%y %I:%M:%S.%f %p",
            )
            for candidate in candidates:
                # Fast path for ISO-like strings.
                try:
                    parsed_iso = datetime.fromisoformat(candidate)
                    return _cache_put(key, parsed_iso)
                except Exception:
                    pass
                for fmt in formats:
                    try:
                        return _cache_put(key, datetime.strptime(candidate, fmt))
                    except Exception:
                        continue
        return _cache_put(("s", str(value)), None)

    def _parse_profile_windows(self, value: Any, default_windows: Optional[List[int]] = None) -> List[int]:
        fallback = default_windows if isinstance(default_windows, list) and default_windows else [1, 7, 30]
        raw_parts: List[Any] = []
        if isinstance(value, list):
            raw_parts = list(value)
        elif isinstance(value, tuple):
            raw_parts = list(value)
        elif isinstance(value, (int, float)):
            raw_parts = [value]
        elif isinstance(value, str):
            text = value.strip()
            if text:
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, list):
                        raw_parts = list(parsed)
                    else:
                        raw_parts = [p.strip() for p in text.replace("\n", ",").split(",")]
                except Exception:
                    raw_parts = [p.strip() for p in text.replace("\n", ",").split(",")]

        out: List[int] = []
        seen = set()
        for item in raw_parts:
            try:
                day = int(float(item))
            except Exception:
                continue
            if day <= 0:
                continue
            if day in seen:
                continue
            seen.add(day)
            out.append(day)
        if not out:
            out = [int(v) for v in fallback if int(v) > 0]
        out.sort()
        return out

    def _build_expression_context(
        self,
        row: Any,
        custom_values: Dict[str, Any],
        dataset_rows: Optional[List[Any]] = None,
        row_index: Optional[int] = None,
        extra_context: Optional[Dict[str, Any]] = None,
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

        def _looks_date_only_text(text: str) -> bool:
            import re as _re
            t = str(text or "").strip()
            if not t:
                return False
            if _re.fullmatch(r"\d{8}", t):
                return True
            if _re.fullmatch(r"\d{4}-\d{2}-\d{2}", t):
                return True
            if _re.fullmatch(r"\d{2}-\d{2}-\d{2,4}", t):
                return True
            if _re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2,4}", t):
                return True
            return False

        def _parse_temporal_value(value: Any) -> Optional[datetime]:
            import re as _re

            if value is None:
                return None
            to_py = getattr(value, "to_pydatetime", None)
            if callable(to_py):
                try:
                    value = to_py()
                except Exception:
                    pass
            if isinstance(value, datetime):
                return value
            if isinstance(value, date):
                return datetime.combine(value, time.min)
            if isinstance(value, str):
                text = " ".join(value.strip().split())
                if not text:
                    return None
                # Normalize fractional seconds: keep up to microseconds for strptime/fromisoformat.
                normalized = _re.sub(
                    r"\.(\d{1,9})",
                    lambda m: "." + m.group(1)[:6].ljust(6, "0"),
                    text,
                    count=1,
                )

                candidates = [normalized]
                if normalized.endswith("Z"):
                    candidates.append(normalized[:-1] + "+00:00")

                parse_formats = (
                    # Date only
                    "%Y-%m-%d",
                    "%Y%m%d",
                    "%d-%m-%Y",
                    "%d-%m-%y",
                    "%d/%m/%Y",
                    "%d/%m/%y",
                    "%m/%d/%Y",
                    "%m/%d/%y",
                    # 24h datetime
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%d-%m %H:%M:%S",
                    "%Y-%d-%m %H:%M:%S.%f",
                    "%d-%m-%Y %H:%M:%S",
                    "%d-%m-%Y %H:%M:%S.%f",
                    "%d-%m-%y %H:%M:%S",
                    "%d-%m-%y %H:%M:%S.%f",
                    "%d/%m/%Y %H:%M:%S",
                    "%d/%m/%Y %H:%M:%S.%f",
                    "%d/%m/%y %H:%M:%S",
                    "%d/%m/%y %H:%M:%S.%f",
                    # 12h datetime with AM/PM
                    "%d-%m-%Y %I:%M:%S %p",
                    "%d-%m-%Y %I:%M:%S.%f %p",
                    "%d-%m-%y %I:%M:%S %p",
                    "%d-%m-%y %I:%M:%S.%f %p",
                    "%d/%m/%Y %I:%M:%S %p",
                    "%d/%m/%Y %I:%M:%S.%f %p",
                    "%d/%m/%y %I:%M:%S %p",
                    "%d/%m/%y %I:%M:%S.%f %p",
                )

                for candidate_text in candidates:
                    for fmt in parse_formats:
                        try:
                            return datetime.strptime(candidate_text, fmt)
                        except Exception:
                            continue
                    try:
                        return datetime.fromisoformat(candidate_text)
                    except Exception:
                        continue
                return None
            return None

        def _format_temporal_output(value: Any, date_only_hint: bool = False) -> Any:
            to_py = getattr(value, "to_pydatetime", None)
            if callable(to_py):
                try:
                    value = to_py()
                except Exception:
                    pass
            if isinstance(value, datetime):
                is_midnight = (
                    value.hour == 0
                    and value.minute == 0
                    and value.second == 0
                    and value.microsecond == 0
                )
                if date_only_hint or is_midnight:
                    return value.date().isoformat()
                return value.strftime("%Y-%d-%m %H:%M:%S")
            if isinstance(value, date):
                return value.isoformat()
            if isinstance(value, time):
                return value.isoformat()
            return value

        def _temporal_sequence(values: Any) -> Tuple[List[datetime], bool]:
            import re as _re
            parsed: List[datetime] = []
            all_date_only = True
            flat = _flatten_sequence(values)
            for raw in flat:
                if raw is None:
                    continue
                dt_value = _parse_temporal_value(raw)
                if dt_value is None:
                    return [], False
                parsed.append(dt_value)

                explicit_time = False
                if isinstance(raw, str):
                    text = raw.strip()
                    if _looks_date_only_text(text):
                        explicit_time = False
                    else:
                        explicit_time = bool(
                            _re.search(r"([Tt ]\d{1,2}:)|\bAM\b|\bPM\b", text, flags=_re.IGNORECASE)
                        )
                if explicit_time or not (
                    dt_value.hour == 0
                    and dt_value.minute == 0
                    and dt_value.second == 0
                    and dt_value.microsecond == 0
                ):
                    all_date_only = False
            return parsed, all_date_only

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
            temporal_values, temporal_date_only = _temporal_sequence(flat)
            if len(temporal_values) == len(flat) and temporal_values:
                return _format_temporal_output(min(temporal_values), temporal_date_only)
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
            temporal_values, temporal_date_only = _temporal_sequence(flat)
            if len(temporal_values) == len(flat) and temporal_values:
                return _format_temporal_output(max(temporal_values), temporal_date_only)
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

        def _conditional_values(
            value_path: Any,
            condition_path: Any,
            expected_value: Any,
            op: Any = "eq",
            include_null_values: Any = False,
            case_sensitive: Any = False,
        ) -> List[Any]:
            import re as _re

            value_field = str(value_path or "").strip()
            condition_field = str(condition_path or "").strip()
            if not value_field or not condition_field:
                return []

            if isinstance(row_scope, list):
                source_rows: List[Any] = row_scope
            elif isinstance(dataset_rows, list) and dataset_rows:
                source_rows = dataset_rows
            else:
                source_rows = [row_scope]

            # Backward compatibility for old count_if signature:
            # count_if(value_path, condition_path, expected_value, include_null_values=False, case_sensitive=False)
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

            out: List[Any] = []
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
                        out.append(None)
                    continue
                for item in values_flat:
                    if item is None and not keep_nulls:
                        continue
                    if isinstance(item, str) and item.strip() == "" and not keep_nulls:
                        continue
                    out.append(item)
            return out

        def _agg_if(
            value_path: Any,
            condition_path: Any,
            expected_value: Any,
            agg: Any = "sum",
            op: Any = "eq",
            include_null_values: Any = False,
            case_sensitive: Any = False,
        ) -> Any:
            values = _conditional_values(
                value_path,
                condition_path,
                expected_value,
                op=op,
                include_null_values=include_null_values,
                case_sensitive=case_sensitive,
            )
            agg_name = str(agg or "sum").strip().lower()

            if agg_name in {"sum", "total"}:
                return _sum(values)
            if agg_name in {"avg", "average", "mean"}:
                return _mean(values)
            if agg_name in {"min", "minimum"}:
                return _min_value(values)
            if agg_name in {"max", "maximum"}:
                return _max_value(values)
            if agg_name in {"count", "size"}:
                return len(_flatten_sequence(values))
            if agg_name in {"count_non_null", "non_null_count"}:
                flat = _flatten_sequence(values)
                return len([v for v in flat if v is not None and str(v).strip() != ""])
            if agg_name in {"distinct", "unique"}:
                return _distinct(values)
            if agg_name in {"distinct_count", "nunique", "unique_count"}:
                return len(_distinct(values))
            if agg_name in {"first"}:
                flat = _flatten_sequence(values)
                return flat[0] if flat else None
            if agg_name in {"last"}:
                flat = _flatten_sequence(values)
                return flat[-1] if flat else None
            if agg_name in {"std", "stdev", "stddev"}:
                return _std(values)
            return _agg(values, agg_name)

        def _count_if(
            value_path: Any,
            condition_path: Any,
            expected_value: Any,
            op: Any = "eq",
            include_null_values: Any = False,
            case_sensitive: Any = False,
        ) -> int:
            return int(
                _agg_if(
                    value_path,
                    condition_path,
                    expected_value,
                    agg="count",
                    op=op,
                    include_null_values=include_null_values,
                    case_sensitive=case_sensitive,
                ) or 0
            )

        def _sum_if(
            value_path: Any,
            condition_path: Any,
            expected_value: Any,
            op: Any = "eq",
            include_null_values: Any = False,
            case_sensitive: Any = False,
        ) -> float:
            return float(
                _agg_if(
                    value_path,
                    condition_path,
                    expected_value,
                    agg="sum",
                    op=op,
                    include_null_values=include_null_values,
                    case_sensitive=case_sensitive,
                ) or 0.0
            )

        def _mean_if(
            value_path: Any,
            condition_path: Any,
            expected_value: Any,
            op: Any = "eq",
            include_null_values: Any = False,
            case_sensitive: Any = False,
        ) -> Any:
            return _agg_if(
                value_path,
                condition_path,
                expected_value,
                agg="mean",
                op=op,
                include_null_values=include_null_values,
                case_sensitive=case_sensitive,
            )

        def _min_if(
            value_path: Any,
            condition_path: Any,
            expected_value: Any,
            op: Any = "eq",
            include_null_values: Any = False,
            case_sensitive: Any = False,
        ) -> Any:
            return _agg_if(
                value_path,
                condition_path,
                expected_value,
                agg="min",
                op=op,
                include_null_values=include_null_values,
                case_sensitive=case_sensitive,
            )

        def _max_if(
            value_path: Any,
            condition_path: Any,
            expected_value: Any,
            op: Any = "eq",
            include_null_values: Any = False,
            case_sensitive: Any = False,
        ) -> Any:
            return _agg_if(
                value_path,
                condition_path,
                expected_value,
                agg="max",
                op=op,
                include_null_values=include_null_values,
                case_sensitive=case_sensitive,
            )

        def _distinct_if(
            value_path: Any,
            condition_path: Any,
            expected_value: Any,
            op: Any = "eq",
            include_null_values: Any = False,
            case_sensitive: Any = False,
        ) -> List[Any]:
            result = _agg_if(
                value_path,
                condition_path,
                expected_value,
                agg="distinct",
                op=op,
                include_null_values=include_null_values,
                case_sensitive=case_sensitive,
            )
            return result if isinstance(result, list) else []

        def _distinct_count_if(
            value_path: Any,
            condition_path: Any,
            expected_value: Any,
            op: Any = "eq",
            include_null_values: Any = False,
            case_sensitive: Any = False,
        ) -> int:
            return int(
                _agg_if(
                    value_path,
                    condition_path,
                    expected_value,
                    agg="distinct_count",
                    op=op,
                    include_null_values=include_null_values,
                    case_sensitive=case_sensitive,
                ) or 0
            )

        def _count_non_null_if(
            value_path: Any,
            condition_path: Any,
            expected_value: Any,
            op: Any = "eq",
            include_null_values: Any = False,
            case_sensitive: Any = False,
        ) -> int:
            return int(
                _agg_if(
                    value_path,
                    condition_path,
                    expected_value,
                    agg="count_non_null",
                    op=op,
                    include_null_values=include_null_values,
                    case_sensitive=case_sensitive,
                ) or 0
            )

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
                current_dt = _parse_temporal_value(current)
                candidate_dt = _parse_temporal_value(candidate)
                if current_dt is not None and candidate_dt is not None:
                    return candidate if candidate_dt < current_dt else current
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
                current_dt = _parse_temporal_value(current)
                candidate_dt = _parse_temporal_value(candidate)
                if current_dt is not None and candidate_dt is not None:
                    return candidate if candidate_dt > current_dt else current
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

                normalized_key_value = key_value
                parsed_key_dt = _parse_temporal_value(key_value)
                if parsed_key_dt is not None:
                    key_date_only_hint = False
                    if isinstance(key_value, str):
                        key_text = key_value.strip()
                        if _looks_date_only_text(key_text):
                            key_date_only_hint = True
                    elif isinstance(key_value, datetime):
                        key_date_only_hint = (
                            key_value.hour == 0
                            and key_value.minute == 0
                            and key_value.second == 0
                            and key_value.microsecond == 0
                        )
                    elif isinstance(key_value, date):
                        key_date_only_hint = True
                    normalized_key_value = _format_temporal_output(parsed_key_dt, key_date_only_hint)

                try:
                    token = json.dumps(normalized_key_value, ensure_ascii=False, sort_keys=True, default=str)
                except Exception:
                    token = str(normalized_key_value)
                if token not in buckets:
                    buckets[token] = {
                        "key": normalized_key_value,
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
                        raw_metric = state.get("value") if bool(state.get("has_value", False)) else None
                        if raw_metric is None:
                            metric_value = None
                        else:
                            parsed_metric_dt = _parse_temporal_value(raw_metric)
                            if parsed_metric_dt is not None:
                                date_only_hint = False
                                if isinstance(raw_metric, str):
                                    raw_text = raw_metric.strip()
                                    if _looks_date_only_text(raw_text):
                                        date_only_hint = True
                                elif isinstance(raw_metric, datetime):
                                    date_only_hint = (
                                        raw_metric.hour == 0
                                        and raw_metric.minute == 0
                                        and raw_metric.second == 0
                                        and raw_metric.microsecond == 0
                                    )
                                elif isinstance(raw_metric, date):
                                    date_only_hint = True
                                metric_value = _format_temporal_output(parsed_metric_dt, date_only_hint)
                            else:
                                metric_value = raw_metric

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
            "agg_if": _agg_if,
            "count_if": _count_if,
            "sum_if": _sum_if,
            "mean_if": _mean_if,
            "min_if": _min_if,
            "max_if": _max_if,
            "distinct_if": _distinct_if,
            "distinct_count_if": _distinct_count_if,
            "count_non_null_if": _count_non_null_if,
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
        if isinstance(extra_context, dict):
            context.update(extra_context)
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
                    ok = self._compare_values(left, right) > 0
                elif isinstance(op, ast.GtE):
                    ok = self._compare_values(left, right) >= 0
                elif isinstance(op, ast.Lt):
                    ok = self._compare_values(left, right) < 0
                elif isinstance(op, ast.LtE):
                    ok = self._compare_values(left, right) <= 0
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

    def _transform_custom_fields(
        self,
        data: list,
        config: dict,
        custom_specs: List[dict],
        execution_context: Optional[Dict[str, Any]] = None,
    ) -> list:
        include_source = bool(config.get("custom_include_source_fields", True))
        primary_key_field = str(
            config.get("custom_primary_key_field")
            or config.get("custom_group_by_field")
            or ""
        ).strip()
        profile_enabled = bool(config.get("custom_profile_enabled", False))
        profile_processing_mode = str(config.get("custom_profile_processing_mode") or "batch").strip().lower()
        if profile_processing_mode not in {"batch", "incremental"}:
            profile_processing_mode = "batch"
        profile_emit_mode = str(config.get("custom_profile_emit_mode") or "changed_only").strip().lower()
        if profile_emit_mode not in {"changed_only", "all_entities"}:
            profile_emit_mode = "changed_only"
        profile_required_fields = self._parse_selected_fields(config.get("custom_profile_required_fields", ""))
        profile_event_time_field = str(config.get("custom_profile_event_time_field") or "").strip()
        profile_default_windows = self._parse_profile_windows(config.get("custom_profile_window_days"), [1, 7, 30])
        try:
            profile_retention_days = int(config.get("custom_profile_retention_days", 45))
        except Exception:
            profile_retention_days = 45
        if profile_retention_days <= 0:
            profile_retention_days = 45
        profile_include_change_fields = bool(config.get("custom_profile_include_change_fields", False))

        result: list = []
        warning_count = 0
        node_warnings: Optional[List[str]] = None
        if isinstance(execution_context, dict):
            existing_warnings = execution_context.get("node_warnings")
            if isinstance(existing_warnings, list):
                node_warnings = existing_warnings
        emit_node_progress = (
            execution_context.get("emit_node_progress")
            if isinstance(execution_context, dict) and callable(execution_context.get("emit_node_progress"))
            else None
        )
        should_abort_cb = (
            execution_context.get("should_abort")
            if isinstance(execution_context, dict) and callable(execution_context.get("should_abort"))
            else None
        )
        raise_if_aborted_cb = (
            execution_context.get("raise_if_aborted")
            if isinstance(execution_context, dict) and callable(execution_context.get("raise_if_aborted"))
            else None
        )
        try:
            node_progress_every = int(
                (execution_context or {}).get("node_progress_every", 2000)
            ) if isinstance(execution_context, dict) else 2000
        except Exception:
            node_progress_every = 2000
        node_progress_every = max(1, min(node_progress_every, 50000))
        if profile_processing_mode == "incremental":
            # Keep live progress visibly ticking in UI for incremental mode.
            node_progress_every = min(node_progress_every, 25)

        def _raise_if_aborted() -> None:
            if callable(raise_if_aborted_cb):
                raise_if_aborted_cb()
                return
            if callable(should_abort_cb):
                try:
                    if bool(should_abort_cb()):
                        raise ExecutionAbortedError("Execution aborted by user.")
                except ExecutionAbortedError:
                    raise
                except Exception:
                    pass

        def _record_custom_field_warning(message: str) -> None:
            nonlocal warning_count
            text = str(message or "").strip()
            if not text:
                return
            if isinstance(node_warnings, list) and len(node_warnings) < 200:
                node_warnings.append(text)
            if warning_count < 5:
                logger.warning(text)
            warning_count += 1

        base_rows: List[Dict[str, Any]] = [
            row if isinstance(row, dict) else {"value": row}
            for row in data
        ]
        _raise_if_aborted()

        if profile_enabled and not primary_key_field:
            _record_custom_field_warning(
                "Custom profile/document mode requires Primary Key / Group By field. Falling back to regular custom fields mode."
            )
            profile_enabled = False

        if profile_enabled:
            node_id = str((execution_context or {}).get("node_id") or "map_transform")
            pipeline_id_for_profile = str((execution_context or {}).get("pipeline_id") or "").strip()
            profile_state_by_node = (
                (execution_context or {}).get("profile_state_by_node")
                if isinstance((execution_context or {}).get("profile_state_by_node"), dict)
                else None
            )
            node_profile_store: Dict[str, Any]
            if isinstance(profile_state_by_node, dict):
                existing_node_state = profile_state_by_node.get(node_id)
                if not isinstance(existing_node_state, dict):
                    existing_node_state = {}
                    profile_state_by_node[node_id] = existing_node_state
                node_profile_store = existing_node_state
            else:
                node_profile_store = {}

            documents_store = node_profile_store.get("documents")
            if not isinstance(documents_store, dict):
                documents_store = {}
                node_profile_store["documents"] = documents_store

            meta_store = node_profile_store.get("meta")
            if not isinstance(meta_store, dict):
                meta_store = {}
                node_profile_store["meta"] = meta_store

            changed_tokens: List[str] = []
            changed_token_set: set = set()
            changed_fields_by_token: Dict[str, List[str]] = {}
            seen_entity_tokens: List[str] = []
            seen_entity_set: set = set()
            entity_value_by_token: Dict[str, Any] = {}
            last_source_by_token: Dict[str, Dict[str, Any]] = {}
            live_persist_warning_emitted = False
            stats_store = node_profile_store.get("stats")
            if not isinstance(stats_store, dict):
                stats_store = {}
                node_profile_store["stats"] = stats_store
            total_input_rows = len(base_rows)
            incremental_processed_rows = 0
            incremental_validated_rows = 0
            last_progress_emit_at = 0.0

            def _emit_profile_progress(force: bool = False) -> None:
                nonlocal incremental_processed_rows, incremental_validated_rows, last_progress_emit_at
                _raise_if_aborted()
                now = pytime.monotonic()
                should_emit_by_rows = (incremental_processed_rows % node_progress_every == 0)
                should_emit_by_time = (now - last_progress_emit_at) >= 0.5
                if not force and not should_emit_by_rows and not should_emit_by_time:
                    return
                last_progress_emit_at = now
                stats_store["custom_fields_incremental_processed_rows"] = int(incremental_processed_rows)
                stats_store["custom_fields_incremental_validated_rows"] = int(incremental_validated_rows)
                stats_store["custom_fields_incremental_output_rows"] = int(len(result))
                stats_store["custom_fields_incremental_last_updated_at"] = datetime.utcnow().isoformat()
                if emit_node_progress and profile_processing_mode == "incremental":
                    progress_pct = (
                        100.0
                        if total_input_rows <= 0
                        else min(100.0, (float(incremental_processed_rows) / float(total_input_rows)) * 100.0)
                    )
                    try:
                        emit_node_progress({
                            "processed_rows": int(incremental_processed_rows),
                            "validated_rows": int(incremental_validated_rows),
                            "output_rows": int(len(result)),
                            "message": (
                                f"⟳ Running {str((execution_context or {}).get('node_label') or 'Custom Fields')}… "
                                f"incremental {incremental_processed_rows:,}/{total_input_rows:,} processed "
                                f"({progress_pct:.1f}%) "
                                f"| validated={incremental_validated_rows:,} | output={len(result):,}"
                            ),
                        })
                    except Exception:
                        pass

            for row_idx, base_row in enumerate(base_rows):
                if row_idx == 0 or row_idx % 25 == 0:
                    _raise_if_aborted()
                incremental_processed_rows += 1
                row_obj = base_row if isinstance(base_row, dict) else {"value": base_row}
                pk_value, pk_found = self._extract_row_value_by_path(row_obj, primary_key_field)
                if not pk_found or pk_value is None or str(pk_value).strip() == "":
                    _record_custom_field_warning(
                        f"Custom profile row {row_idx + 1} skipped: missing entity key field '{primary_key_field}'."
                    )
                    _emit_profile_progress()
                    continue

                missing_required: List[str] = []
                for required_field in profile_required_fields:
                    req_value, req_found = self._extract_row_value_by_path(row_obj, required_field)
                    if not req_found or req_value is None or str(req_value).strip() == "":
                        missing_required.append(required_field)
                if missing_required:
                    _record_custom_field_warning(
                        f"Custom profile row {row_idx + 1} skipped: missing required fields {', '.join(missing_required)}."
                    )
                    _emit_profile_progress()
                    continue
                incremental_validated_rows += 1

                token = self._stable_json_token(pk_value)
                if token not in seen_entity_set:
                    seen_entity_set.add(token)
                    seen_entity_tokens.append(token)
                entity_value_by_token[token] = pk_value
                last_source_by_token[token] = row_obj

                previous_doc = documents_store.get(token)
                is_new = not isinstance(previous_doc, dict)
                if not isinstance(previous_doc, dict):
                    previous_doc = {}
                previous_meta = meta_store.get(token)
                if not isinstance(previous_meta, dict):
                    previous_meta = {}

                # Performance-critical path:
                # keep profile/doc updates incremental and in-place per entity
                # instead of deep-copying full documents on every incoming row.
                working_doc: Dict[str, Any] = previous_doc
                working_meta: Dict[str, Any] = previous_meta
                custom_values: Dict[str, Any] = {}
                changed_fields: List[str] = []
                profile_meta_touched = False
                append_unique_cache: Dict[str, Dict[str, Any]] = {}

                event_time_value = None
                if profile_event_time_field:
                    event_time_value, _ = self._extract_row_value_by_path(row_obj, profile_event_time_field)
                if event_time_value is None:
                    for fallback_time_field in (
                        "txn_time",
                        "transaction_time",
                        "timestamp",
                        "created_at",
                        "updated_at",
                        "SERVERTIME",
                        "TXNDATE",
                    ):
                        fallback_value, found = self._extract_row_value_by_path(row_obj, fallback_time_field)
                        if found and fallback_value is not None and str(fallback_value).strip() != "":
                            event_time_value = fallback_value
                            break
                current_event_dt = self._parse_profile_event_time(event_time_value) or datetime.utcnow()
                current_event_time = current_event_dt.strftime("%Y-%m-%d %H:%M:%S")
                active_profile_field: Dict[str, str] = {"name": "", "mode": "value"}
                _missing = object()

                def _resolve_profile_path(path: Any) -> str:
                    path_text = str(path or "").strip()
                    if not path_text:
                        return path_text

                    # In JSON profile templates, allow shorthand paths:
                    # append_unique('ids', x) will map to "<field_name>.ids".
                    mode = str(active_profile_field.get("mode") or "value").strip().lower()
                    field_name = str(active_profile_field.get("name") or "").strip()
                    if mode != "json" or not field_name:
                        return path_text
                    if path_text == field_name or path_text.startswith(f"{field_name}."):
                        return path_text
                    if path_text in custom_values:
                        return path_text
                    existing_root = self._get_profile_path_value(working_doc, path_text, _missing)
                    if existing_root is not _missing:
                        return path_text
                    scoped = f"{field_name}.{path_text}"
                    existing_scoped = self._get_profile_path_value(working_doc, scoped, _missing)
                    if existing_scoped is not _missing:
                        return scoped
                    return scoped

                def _profile_prev(path: Any = None, default: Any = None) -> Any:
                    if path is None or str(path).strip() == "":
                        return copy.deepcopy(working_doc)
                    path_text = _resolve_profile_path(path)
                    if path_text in custom_values:
                        value = custom_values.get(path_text)
                    else:
                        value = self._get_profile_path_value(working_doc, path_text, None)
                    if value is None:
                        return default
                    if isinstance(value, (dict, list)):
                        return copy.deepcopy(_resolve_profile_dynamic_value(value, working_meta))
                    return value

                def _num(value: Any, default: Any = 0) -> float:
                    n = self._to_number(value)
                    if n is not None:
                        return float(n)
                    d = self._to_number(default)
                    return float(d) if d is not None else 0.0

                def _safe_div(numerator: Any, denominator: Any, default: Any = None) -> Any:
                    den = self._to_number(denominator)
                    if den in (None, 0):
                        return default
                    num = self._to_number(numerator)
                    if num is None:
                        return default
                    return float(num) / float(den)

                def _inc(path_or_value: Any, amount: Any = 1, default: Any = 0) -> Any:
                    if isinstance(path_or_value, str):
                        base_value = _profile_prev(_resolve_profile_path(path_or_value), default)
                    else:
                        base_value = path_or_value
                    total = _num(base_value, default) + _num(amount, 0)
                    return int(total) if float(total).is_integer() else float(total)

                def _map_inc(path_or_map: Any, key: Any, amount: Any = 1, default: Any = 0) -> Dict[str, Any]:
                    if isinstance(path_or_map, str):
                        base_map = _profile_prev(_resolve_profile_path(path_or_map), {})
                    else:
                        base_map = path_or_map
                    current_map = dict(base_map) if isinstance(base_map, dict) else {}
                    map_key = str(key) if key is not None else "null"
                    current_value = current_map.get(map_key, default)
                    next_value = _num(current_value, default) + _num(amount, 0)
                    current_map[map_key] = int(next_value) if float(next_value).is_integer() else float(next_value)
                    return current_map

                def _append_unique(
                    path_or_values: Any,
                    value: Any = None,
                    max_items: Any = None,
                    normalize: Any = True,
                ) -> List[Any]:
                    normalize_items = True
                    if isinstance(normalize, bool):
                        normalize_items = normalize
                    elif isinstance(normalize, (int, float)):
                        normalize_items = normalize != 0
                    elif isinstance(normalize, str):
                        norm_flag = normalize.strip().lower()
                        if norm_flag in {"0", "false", "no", "off", "n"}:
                            normalize_items = False

                    def _candidate_value(raw: Any) -> Any:
                        candidate = self._json_safe_value(raw)
                        if normalize_items and isinstance(candidate, str):
                            candidate = candidate.strip()
                        return candidate

                    def _candidate_token(raw: Any) -> str:
                        candidate = _candidate_value(raw)
                        return self._stable_json_token(candidate)

                    if isinstance(path_or_values, str):
                        resolved_path = _resolve_profile_path(path_or_values)
                        cached_items_state = append_unique_cache.get(resolved_path)
                        if isinstance(cached_items_state, dict):
                            current_cached = cached_items_state.get("current")
                            seen_cached = cached_items_state.get("seen")
                            if isinstance(current_cached, list) and isinstance(seen_cached, set):
                                current = current_cached
                                seen = seen_cached
                            else:
                                cached_items_state = None
                        if not isinstance(cached_items_state, dict):
                            base_items = self._get_profile_path_value(working_doc, resolved_path, _missing)
                        # Compatibility fallback:
                        # If expression path points to a parent JSON object path but the current
                        # custom field is a value field (for example name "_customer_ids" with path
                        # "CUSTOMER_INFO._customer_ids"), continue from current field state.
                            if base_items is _missing:
                                current_field_name = str(active_profile_field.get("name") or "").strip()
                                if current_field_name and current_field_name != resolved_path:
                                    fallback_value = self._get_profile_path_value(working_doc, current_field_name, _missing)
                                    if fallback_value is not _missing:
                                        base_items = fallback_value
                            if base_items is _missing:
                                base_items = []
                            current = list(base_items) if isinstance(base_items, list) else []
                            seen = {_candidate_token(v) for v in current}
                            append_unique_cache[resolved_path] = {"current": current, "seen": seen}
                        incoming_values = [value]
                    else:
                        # One-arg form append_unique(values(...)) should return a de-duplicated
                        # flattened list from the provided sequence.
                        if value is None:
                            base_items = []
                            incoming_values = [path_or_values]
                        else:
                            base_items = path_or_values
                            incoming_values = [value]
                        current = list(base_items) if isinstance(base_items, list) else []
                        seen = {_candidate_token(v) for v in current}
                    flattened_incoming = _flatten_profile_values(incoming_values)
                    for incoming in flattened_incoming:
                        if incoming is None:
                            continue
                        token_value = _candidate_token(incoming)
                        if token_value in seen:
                            continue
                        seen.add(token_value)
                        current.append(_candidate_value(incoming))
                    try:
                        cap = int(max_items) if max_items is not None else 0
                    except Exception:
                        cap = 0
                    if cap > 0 and len(current) > cap:
                        current = current[-cap:]
                    return current

                def _rolling_update(
                    name: Any,
                    value: Any,
                    txn_time: Any = None,
                    windows: Any = None,
                    retention_days: Any = None,
                ) -> Dict[str, Any]:
                    nonlocal profile_meta_touched
                    profile_meta_touched = True
                    metric_name = str(name or "").strip() or "value"
                    window_days = self._parse_profile_windows(windows, profile_default_windows)
                    try:
                        retention = int(retention_days) if retention_days is not None else profile_retention_days
                    except Exception:
                        retention = profile_retention_days
                    if retention <= 0:
                        retention = profile_retention_days
                    max_window = max(window_days) if window_days else 30
                    retention = max(retention, max_window)

                    rolling_root = working_meta.setdefault("rolling", {})
                    if not isinstance(rolling_root, dict):
                        rolling_root = {}
                        working_meta["rolling"] = rolling_root
                    raw_stream = rolling_root.get(metric_name)
                    stream: List[Dict[str, Any]] = []
                    if isinstance(raw_stream, list):
                        for item in raw_stream:
                            if not isinstance(item, dict):
                                continue
                            ts = self._parse_profile_event_time(item.get("ts"))
                            if ts is None:
                                continue
                            stream.append({"ts": ts, "value": self._to_number(item.get("value"))})

                    event_dt = self._parse_profile_event_time(txn_time) or current_event_dt
                    stream.append({"ts": event_dt, "value": self._to_number(value)})
                    cutoff = event_dt - timedelta(days=retention)
                    stream = [item for item in stream if isinstance(item.get("ts"), datetime) and item["ts"] >= cutoff]
                    stream.sort(key=lambda item: item["ts"])

                    rolling_root[metric_name] = [
                        {
                            "ts": item["ts"].strftime("%Y-%m-%d %H:%M:%S"),
                            "value": item.get("value"),
                        }
                        for item in stream
                    ]

                    metrics: Dict[str, Any] = {}
                    for day in window_days:
                        threshold = event_dt - timedelta(days=int(day))
                        window_items = [item for item in stream if item["ts"] >= threshold]
                        nums = [float(item["value"]) for item in window_items if item.get("value") is not None]
                        total = float(sum(nums)) if nums else 0.0
                        metrics[f"d{int(day)}"] = {
                            "count": len(window_items),
                            "sum": total,
                            "avg": (total / len(nums)) if nums else None,
                            "min": min(nums) if nums else None,
                            "max": max(nums) if nums else None,
                            "last": nums[-1] if nums else None,
                            "start_time": threshold.strftime("%Y-%m-%d %H:%M:%S"),
                            "end_time": event_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    return metrics

                def _flatten_profile_values(value: Any) -> List[Any]:
                    out: List[Any] = []

                    def walk(item: Any) -> None:
                        if isinstance(item, (list, tuple, set)):
                            for sub in item:
                                walk(sub)
                            return
                        out.append(item)

                    walk(value)
                    return out

                def _profile_pick_min(current: Any, incoming: Any) -> Any:
                    if current is None:
                        return incoming
                    if incoming is None:
                        return current
                    cur_dt = self._parse_profile_event_time(current)
                    in_dt = self._parse_profile_event_time(incoming)
                    if cur_dt is not None and in_dt is not None:
                        return incoming if in_dt < cur_dt else current
                    cur_num = self._to_number(current)
                    in_num = self._to_number(incoming)
                    if cur_num is not None and in_num is not None:
                        return incoming if in_num < cur_num else current
                    try:
                        return incoming if str(incoming) < str(current) else current
                    except Exception:
                        return current

                def _profile_pick_max(current: Any, incoming: Any) -> Any:
                    if current is None:
                        return incoming
                    if incoming is None:
                        return current
                    cur_dt = self._parse_profile_event_time(current)
                    in_dt = self._parse_profile_event_time(incoming)
                    if cur_dt is not None and in_dt is not None:
                        return incoming if in_dt > cur_dt else current
                    cur_num = self._to_number(current)
                    in_num = self._to_number(incoming)
                    if cur_num is not None and in_num is not None:
                        return incoming if in_num > cur_num else current
                    try:
                        return incoming if str(incoming) > str(current) else current
                    except Exception:
                        return current

                def _profile_metric_kind(agg_name: Any) -> str:
                    op = str(agg_name or "count").strip().lower()
                    if op in {"rows", "row_count", "size"}:
                        return "row_count"
                    if op in {"count_non_null", "non_null_count"}:
                        return "count_non_null"
                    if op in {"distinct_count", "nunique", "unique_count"}:
                        return "distinct_count"
                    if op in {"distinct", "unique"}:
                        return "distinct"
                    if op in {"value_counts", "count_by_value", "frequency", "freq"}:
                        return "value_counts"
                    if op in {"sum", "total"}:
                        return "sum"
                    if op in {"avg", "average", "mean"}:
                        return "mean"
                    if op in {"min", "minimum"}:
                        return "min"
                    if op in {"max", "maximum"}:
                        return "max"
                    if op == "first":
                        return "first"
                    if op == "last":
                        return "last"
                    return "count"

                def _profile_metric_init(kind: str) -> Dict[str, Any]:
                    if kind in {"count", "count_non_null", "row_count"}:
                        return {"kind": kind, "count": 0}
                    if kind == "sum":
                        return {"kind": kind, "value": 0.0}
                    if kind == "mean":
                        return {"kind": kind, "sum": 0.0, "count": 0}
                    if kind in {"min", "max", "first", "last"}:
                        return {"kind": kind, "has_value": False, "value": None}
                    if kind == "distinct_count":
                        return {"kind": kind, "seen": {}}
                    if kind == "distinct":
                        return {"kind": kind, "seen": {}, "values": []}
                    if kind == "value_counts":
                        return {"kind": kind, "items": {}, "order": []}
                    return {"kind": "count", "count": 0}

                def _profile_metric_update(state: Any, kind: str, raw_value: Any) -> Dict[str, Any]:
                    current_state = state if isinstance(state, dict) else _profile_metric_init(kind)
                    if str(current_state.get("kind") or "") != kind:
                        current_state = _profile_metric_init(kind)

                    if kind == "row_count":
                        current_state["count"] = int(current_state.get("count", 0)) + 1
                        return current_state

                    values = _flatten_profile_values(raw_value)
                    if kind == "count":
                        current_state["count"] = int(current_state.get("count", 0)) + len(values)
                        return current_state

                    if kind == "count_non_null":
                        non_null = len(
                            [v for v in values if v is not None and str(v).strip() != ""]
                        )
                        current_state["count"] = int(current_state.get("count", 0)) + non_null
                        return current_state

                    if kind == "sum":
                        total = float(current_state.get("value", 0.0))
                        for value in values:
                            num = self._to_number(value)
                            if num is not None:
                                total += float(num)
                        current_state["value"] = total
                        return current_state

                    if kind == "mean":
                        total = float(current_state.get("sum", 0.0))
                        count = int(current_state.get("count", 0))
                        for value in values:
                            num = self._to_number(value)
                            if num is None:
                                continue
                            total += float(num)
                            count += 1
                        current_state["sum"] = total
                        current_state["count"] = count
                        return current_state

                    if kind == "min":
                        cur_val = current_state.get("value")
                        has_val = bool(current_state.get("has_value", False))
                        for value in values:
                            if value is None:
                                continue
                            cur_val = value if not has_val else _profile_pick_min(cur_val, value)
                            has_val = True
                        current_state["value"] = cur_val
                        current_state["has_value"] = has_val
                        return current_state

                    if kind == "max":
                        cur_val = current_state.get("value")
                        has_val = bool(current_state.get("has_value", False))
                        for value in values:
                            if value is None:
                                continue
                            cur_val = value if not has_val else _profile_pick_max(cur_val, value)
                            has_val = True
                        current_state["value"] = cur_val
                        current_state["has_value"] = has_val
                        return current_state

                    if kind == "first":
                        if bool(current_state.get("has_value", False)):
                            return current_state
                        for value in values:
                            if value is None:
                                continue
                            current_state["value"] = value
                            current_state["has_value"] = True
                            break
                        return current_state

                    if kind == "last":
                        for value in values:
                            if value is None:
                                continue
                            current_state["value"] = value
                            current_state["has_value"] = True
                        return current_state

                    if kind == "distinct_count":
                        seen = current_state.get("seen")
                        if not isinstance(seen, dict):
                            seen = {}
                            current_state["seen"] = seen
                        for value in values:
                            if value is None:
                                continue
                            seen[self._stable_json_token(value)] = 1
                        return current_state

                    if kind == "distinct":
                        seen = current_state.get("seen")
                        if not isinstance(seen, dict):
                            seen = {}
                            current_state["seen"] = seen
                        out_values = current_state.get("values")
                        if not isinstance(out_values, list):
                            out_values = []
                            current_state["values"] = out_values
                        for value in values:
                            if value is None:
                                continue
                            token_value = self._stable_json_token(value)
                            if token_value in seen:
                                continue
                            seen[token_value] = 1
                            out_values.append(value)
                        return current_state

                    if kind == "value_counts":
                        items = current_state.get("items")
                        if not isinstance(items, dict):
                            items = {}
                            current_state["items"] = items
                        order = current_state.get("order")
                        if not isinstance(order, list):
                            order = []
                            current_state["order"] = order
                        for value in values:
                            if value is None:
                                continue
                            token_value = self._stable_json_token(value)
                            bucket = items.get(token_value)
                            if not isinstance(bucket, dict):
                                items[token_value] = {"value": value, "count": 1}
                                order.append(token_value)
                            else:
                                bucket["count"] = int(bucket.get("count", 0)) + 1
                        return current_state

                    current_state["count"] = int(current_state.get("count", 0)) + len(values)
                    return current_state

                def _profile_metric_finalize(state: Any, kind: str) -> Any:
                    item = state if isinstance(state, dict) else _profile_metric_init(kind)
                    if kind in {"count", "count_non_null", "row_count"}:
                        return int(item.get("count", 0))
                    if kind == "sum":
                        total = float(item.get("value", 0.0))
                        return int(total) if float(total).is_integer() else total
                    if kind == "mean":
                        count = int(item.get("count", 0))
                        if count <= 0:
                            return None
                        return float(item.get("sum", 0.0)) / count
                    if kind in {"min", "max", "first", "last"}:
                        if not bool(item.get("has_value", False)):
                            return None
                        return item.get("value")
                    if kind == "distinct_count":
                        seen = item.get("seen")
                        return len(seen) if isinstance(seen, dict) else 0
                    if kind == "distinct":
                        values_out = item.get("values")
                        return values_out if isinstance(values_out, list) else []
                    if kind == "value_counts":
                        items = item.get("items")
                        order = item.get("order")
                        if not isinstance(items, dict) or not isinstance(order, list):
                            return []
                        return [
                            {
                                "value": items[token].get("value"),
                                "count": int(items[token].get("count", 0)),
                            }
                            for token in order
                            if token in items
                        ]
                    return int(item.get("count", 0))

                def _profile_compile_group_metrics(raw_metrics: Any) -> List[Dict[str, Any]]:
                    raw = raw_metrics
                    if isinstance(raw, str):
                        text = raw.strip()
                        if not text:
                            return []
                        try:
                            raw = json.loads(text)
                        except Exception:
                            raw = {}

                    specs: List[Dict[str, Any]] = []
                    seen_names = set()

                    def add_spec(name: Any, conf: Any) -> None:
                        metric_name = str(name or "").strip()
                        if not metric_name:
                            return
                        metric_key = metric_name.lower()
                        if metric_key in seen_names:
                            return
                        seen_names.add(metric_key)

                        agg = "count"
                        metric_mode = "const"
                        metric_path = ""
                        metric_const = None
                        if isinstance(conf, dict):
                            agg = str(
                                conf.get("agg")
                                or conf.get("op")
                                or conf.get("func")
                                or conf.get("operation")
                                or "count"
                            ).strip().lower()
                            if "value" in conf:
                                metric_const = conf.get("value")
                            else:
                                metric_path = str(
                                    conf.get("path")
                                    or conf.get("field")
                                    or conf.get("column")
                                    or ""
                                ).strip()
                                if metric_path:
                                    metric_mode = "path"
                        elif isinstance(conf, list):
                            metric_const = conf[0] if len(conf) > 0 else None
                            agg = str(conf[1] if len(conf) > 1 else "count").strip().lower()
                        elif isinstance(conf, str):
                            text = conf.strip()
                            if ":" in text:
                                maybe_path, maybe_agg = [part.strip() for part in text.split(":", 1)]
                                agg = maybe_agg or "count"
                                if maybe_path:
                                    metric_mode = "path"
                                    metric_path = maybe_path
                            else:
                                agg = text or "count"
                        else:
                            metric_const = conf

                        specs.append(
                            {
                                "name": metric_name,
                                "kind": _profile_metric_kind(agg),
                                "mode": metric_mode,
                                "path": metric_path,
                                "const": metric_const,
                            }
                        )

                    if isinstance(raw, dict):
                        for metric_name, conf in raw.items():
                            add_spec(metric_name, conf)
                    elif isinstance(raw, list):
                        for idx, conf in enumerate(raw):
                            add_spec(f"metric_{idx + 1}", conf)
                    return specs

                def _profile_bind_group_metrics(metric_defs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
                    out_specs: List[Dict[str, Any]] = []
                    for metric_def in metric_defs:
                        metric_name = str(metric_def.get("name") or "").strip()
                        metric_kind = str(metric_def.get("kind") or "count").strip().lower()
                        if not metric_name:
                            continue
                        mode = str(metric_def.get("mode") or "const").strip().lower()
                        metric_value = metric_def.get("const")
                        if mode == "path":
                            value_path = str(metric_def.get("path") or "").strip()
                            if value_path:
                                metric_value, _ = self._extract_row_value_by_path(row_obj, value_path)
                            else:
                                metric_value = None
                        out_specs.append(
                            {
                                "name": metric_name,
                                "kind": metric_kind,
                                "value": metric_value,
                            }
                        )
                    return out_specs

                def _profile_group_aggregate(
                    key_path: Any,
                    metrics: Any,
                    key_name: Any = "key",
                    include_null_key: Any = False,
                ) -> List[Dict[str, Any]]:
                    nonlocal profile_meta_touched
                    key_field = str(key_path or "").strip()
                    if not key_field:
                        return []

                    profile_field_name = str(active_profile_field.get("name") or "").strip()
                    output_key_name = str(key_name or "key").strip() or "key"
                    # Keep independent state per aggregate call signature so multiple
                    # group_aggregate(...) expressions inside one JSON profile field do
                    # not overwrite each other's buckets/keys.
                    store_key = "||".join(
                        [
                            profile_field_name or "__profile__",
                            key_field,
                            output_key_name,
                        ]
                    )
                    aggregate_root = working_meta.get("group_aggregate")
                    if not isinstance(aggregate_root, dict):
                        aggregate_root = {}
                        working_meta["group_aggregate"] = aggregate_root

                    state = aggregate_root.get(store_key)
                    if not isinstance(state, dict):
                        state = {
                            "groups": {},
                            "order": [],
                            "metric_specs": [],
                            "output_key_name": output_key_name,
                            "dirty": True,
                            "cached_rows": [],
                        }
                        aggregate_root[store_key] = state
                    else:
                        state["output_key_name"] = output_key_name

                    metric_defs = state.get("metric_defs")
                    if not isinstance(metric_defs, list) or not metric_defs:
                        metric_defs = _profile_compile_group_metrics(metrics)
                        state["metric_defs"] = metric_defs
                    metric_specs = _profile_bind_group_metrics(metric_defs if isinstance(metric_defs, list) else [])
                    if metric_defs:
                        state["metric_specs"] = [
                            {
                                "name": str(item.get("name") or "").strip(),
                                "kind": str(item.get("kind") or "count").strip().lower(),
                            }
                            for item in metric_defs
                            if isinstance(item, dict)
                        ]
                        profile_meta_touched = True
                    else:
                        metric_specs = state.get("metric_specs") if isinstance(state.get("metric_specs"), list) else []

                    key_value, key_found = self._extract_row_value_by_path(row_obj, key_field)
                    keep_null_key = bool(include_null_key)
                    if key_found and (key_value is not None or keep_null_key):
                        groups = state.get("groups")
                        if not isinstance(groups, dict):
                            groups = {}
                            state["groups"] = groups
                        order = state.get("order")
                        if not isinstance(order, list):
                            order = []
                            state["order"] = order

                        token_value = self._stable_json_token(key_value)
                        bucket = groups.get(token_value)
                        if not isinstance(bucket, dict):
                            bucket = {"key": key_value, "metrics": {}}
                            groups[token_value] = bucket
                            order.append(token_value)
                        else:
                            bucket["key"] = key_value

                        bucket_metrics = bucket.get("metrics")
                        if not isinstance(bucket_metrics, dict):
                            bucket_metrics = {}
                            bucket["metrics"] = bucket_metrics

                        for spec in metric_specs:
                            metric_name = str(spec.get("name") or "").strip()
                            metric_kind = str(spec.get("kind") or "count").strip().lower()
                            if not metric_name:
                                continue
                            previous_state = bucket_metrics.get(metric_name)
                            next_state = _profile_metric_update(previous_state, metric_kind, spec.get("value"))
                            bucket_metrics[metric_name] = next_state
                        state["dirty"] = True
                        profile_meta_touched = True
                    # Return a lazy placeholder and materialize aggregate arrays only
                    # when emitting/persisting profile docs (not for every row update).
                    return {"__profile_group_aggregate_ref__": store_key}

                def _profile_materialize_group_aggregate(profile_meta: Dict[str, Any], store_key: str) -> List[Dict[str, Any]]:
                    aggregate_root = profile_meta.get("group_aggregate")
                    if not isinstance(aggregate_root, dict):
                        return []
                    state = aggregate_root.get(store_key)
                    if not isinstance(state, dict):
                        return []

                    cached_rows = state.get("cached_rows")
                    if not bool(state.get("dirty", True)) and isinstance(cached_rows, list):
                        return cached_rows

                    groups = state.get("groups")
                    order = state.get("order")
                    if not isinstance(groups, dict) or not isinstance(order, list):
                        state["cached_rows"] = []
                        state["dirty"] = False
                        return []

                    metric_specs_final = state.get("metric_specs")
                    if not isinstance(metric_specs_final, list):
                        metric_specs_final = []
                    output_key = str(state.get("output_key_name") or "key").strip() or "key"

                    out: List[Dict[str, Any]] = []
                    for token in order:
                        bucket = groups.get(token)
                        if not isinstance(bucket, dict):
                            continue
                        bucket_metrics = bucket.get("metrics")
                        if not isinstance(bucket_metrics, dict):
                            bucket_metrics = {}
                        row_out: Dict[str, Any] = {output_key: bucket.get("key")}
                        for spec in metric_specs_final:
                            metric_name = str(spec.get("name") or "").strip()
                            metric_kind = str(spec.get("kind") or "count").strip().lower()
                            if not metric_name:
                                continue
                            metric_state = bucket_metrics.get(metric_name)
                            row_out[metric_name] = _profile_metric_finalize(metric_state, metric_kind)
                        out.append(row_out)

                    state["cached_rows"] = out
                    state["dirty"] = False
                    return out

                def _resolve_profile_dynamic_value(value: Any, profile_meta: Dict[str, Any]) -> Any:
                    if isinstance(value, dict):
                        if (
                            len(value) == 1
                            and "__profile_group_aggregate_ref__" in value
                            and isinstance(value.get("__profile_group_aggregate_ref__"), str)
                        ):
                            store_key = str(value.get("__profile_group_aggregate_ref__") or "").strip()
                            if not store_key:
                                return []
                            return _profile_materialize_group_aggregate(profile_meta, store_key)
                        return {
                            k: _resolve_profile_dynamic_value(v, profile_meta)
                            for k, v in value.items()
                        }
                    if isinstance(value, list):
                        return [_resolve_profile_dynamic_value(v, profile_meta) for v in value]
                    return value

                extra_context = {
                    "prev": _profile_prev,
                    "profile_prev": _profile_prev,
                    "profile_get": _profile_prev,
                    "doc": _profile_prev,
                    "num": _num,
                    "safe_div": _safe_div,
                    "inc": _inc,
                    "map_inc": _map_inc,
                    "append_unique": _append_unique,
                    "appendunique": _append_unique,
                    "appen_unique": _append_unique,
                    "rolling_update": _rolling_update,
                    "rolling_window_update": _rolling_update,
                    "profile_group_aggregate": _profile_group_aggregate,
                    "group_aggregate_profile": _profile_group_aggregate,
                    "group_aggregate": _profile_group_aggregate,
                    "group_metrics": _profile_group_aggregate,
                    "entity_key": pk_value,
                    "entity_id": pk_value,
                    "event_time": current_event_time,
                }
                eval_context = self._build_expression_context(
                    row_obj,
                    custom_values,
                    dataset_rows=[row_obj],
                    row_index=row_idx,
                    extra_context=extra_context,
                )

                # Keep single-pass in profile mode to avoid side-effect duplication
                # for helpers like rolling_update().
                for spec in custom_specs:
                    name = str(spec.get("name") or "").strip()
                    if not name:
                        continue
                    active_profile_field["name"] = name
                    mode = str(spec.get("mode") or "value").lower()
                    active_profile_field["mode"] = mode
                    expression = str(spec.get("expression") or "")
                    template = spec.get("json_template")
                    try:
                        if mode == "json":
                            value = self._evaluate_json_template(
                                template,
                                row_obj,
                                custom_values,
                                dataset_rows=[row_obj],
                                row_index=row_idx,
                                context=eval_context,
                            )
                        else:
                            value = self._evaluate_custom_expression(
                                expression,
                                row_obj,
                                custom_values,
                                dataset_rows=[row_obj],
                                row_index=row_idx,
                                context=eval_context,
                            )
                    except Exception as exc:
                        value = None
                        _record_custom_field_warning(
                            f"Custom profile field '{name}' evaluation failed at row {row_idx + 1}: {exc}"
                        )
                    finally:
                        active_profile_field["name"] = ""
                        active_profile_field["mode"] = "value"

                    if mode != "json":
                        value = self._apply_single_value_output_mode(
                            value,
                            spec.get("single_value_output"),
                        )

                    # Keep per-row profile values raw in memory for speed; convert to
                    # JSON-safe once when emitting/persisting final state.
                    previous_value = self._get_profile_path_value(working_doc, name, _missing)
                    value_to_set = value
                    values_equal = False
                    if previous_value is _missing:
                        values_equal = False
                    elif previous_value is value_to_set:
                        values_equal = True
                    elif isinstance(previous_value, (dict, list)) or isinstance(value_to_set, (dict, list)):
                        # Avoid deep equality on large nested profile docs/lists.
                        values_equal = False
                    else:
                        try:
                            values_equal = previous_value == value_to_set
                        except Exception:
                            values_equal = False
                    if not values_equal:
                        changed_fields.append(name)
                    self._set_profile_path_value(working_doc, name, value_to_set)
                    custom_values[name] = value_to_set
                    eval_context[name] = value_to_set
                    if isinstance(name, str) and name.isidentifier():
                        eval_context[name] = value_to_set

                if self._get_profile_path_value(working_doc, primary_key_field, None) is None:
                    self._set_profile_path_value(working_doc, primary_key_field, self._json_safe_value(pk_value))
                    if primary_key_field not in changed_fields:
                        changed_fields.append(primary_key_field)

                doc_changed = bool(changed_fields)
                meta_changed = bool(profile_meta_touched)
                if doc_changed or meta_changed or is_new:
                    # Keep mutable profile state in-memory during the run to avoid
                    # expensive full JSON-safe conversion on every input row.
                    # Serialization safety is enforced once at execution persist time
                    # (LMDB/runtime save) and when emitting node results.
                    documents_store[token] = working_doc
                    meta_store[token] = working_meta
                    changed_fields_by_token[token] = list(changed_fields)
                    if token not in changed_token_set:
                        changed_token_set.add(token)
                        changed_tokens.append(token)

                if profile_processing_mode == "incremental" and (doc_changed or meta_changed or is_new):
                    profile_doc_for_emit = documents_store.get(token)
                    if not isinstance(profile_doc_for_emit, dict):
                        profile_doc_for_emit = working_doc
                    profile_meta_for_emit = meta_store.get(token)
                    if not isinstance(profile_meta_for_emit, dict):
                        profile_meta_for_emit = working_meta
                    resolved_profile_doc = _resolve_profile_dynamic_value(
                        profile_doc_for_emit,
                        profile_meta_for_emit,
                    )
                    if isinstance(resolved_profile_doc, dict):
                        # Keep materialized profile in state for persisted profile docs.
                        documents_store[token] = resolved_profile_doc
                        out_row: Dict[str, Any] = {}
                        if include_source and isinstance(row_obj, dict):
                            out_row.update(row_obj)
                        out_row.update(resolved_profile_doc)
                        if primary_key_field and self._get_profile_path_value(out_row, primary_key_field, None) is None:
                            self._set_profile_path_value(
                                out_row,
                                primary_key_field,
                                self._json_safe_value(pk_value),
                            )
                        if profile_include_change_fields:
                            out_row["_profile_changed_fields"] = list(changed_fields)
                        result.append(self._json_safe_value(out_row))
                        if (
                            pipeline_id_for_profile
                            and isinstance(profile_state_by_node, dict)
                        ):
                            _raise_if_aborted()
                            node_state_for_flush = profile_state_by_node.get(node_id)
                            if isinstance(node_state_for_flush, dict):
                                flushed = self._save_profile_state_single_node(
                                    pipeline_id_for_profile,
                                    node_id,
                                    node_state_for_flush,
                                )
                                if not flushed and not live_persist_warning_emitted:
                                    live_persist_warning_emitted = True
                                    _record_custom_field_warning(
                                        "Incremental profile live persist failed; final LMDB save will be attempted at pipeline end."
                                    )
                _emit_profile_progress()

            if profile_processing_mode == "incremental":
                _emit_profile_progress(force=True)
                return result

            emit_tokens: List[str]
            if profile_emit_mode == "all_entities":
                emit_tokens = []
                seen_emit = set()
                for token in seen_entity_tokens + list(documents_store.keys()):
                    if token in seen_emit:
                        continue
                    seen_emit.add(token)
                    emit_tokens.append(token)
            else:
                emit_tokens = list(changed_tokens)

            for token in emit_tokens:
                _raise_if_aborted()
                profile_doc = documents_store.get(token)
                if not isinstance(profile_doc, dict):
                    continue
                profile_meta = meta_store.get(token)
                if not isinstance(profile_meta, dict):
                    profile_meta = {}
                resolved_profile_doc = _resolve_profile_dynamic_value(profile_doc, profile_meta)
                if not isinstance(resolved_profile_doc, dict):
                    continue
                # Keep materialized profile in state so persisted LMDB documents are
                # directly consumable without placeholder indirection.
                documents_store[token] = resolved_profile_doc
                out_row: Dict[str, Any] = {}
                if include_source:
                    source_row = last_source_by_token.get(token)
                    if isinstance(source_row, dict):
                        out_row.update(source_row)
                out_row.update(resolved_profile_doc)
                if primary_key_field and self._get_profile_path_value(out_row, primary_key_field, None) is None:
                    self._set_profile_path_value(
                        out_row,
                        primary_key_field,
                        self._json_safe_value(entity_value_by_token.get(token)),
                    )
                if profile_include_change_fields:
                    out_row["_profile_changed_fields"] = changed_fields_by_token.get(token, [])
                result.append(self._json_safe_value(out_row))
            return result

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
            if row_idx == 0 or row_idx % 25 == 0:
                _raise_if_aborted()
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
                        _record_custom_field_warning(
                            f"Custom field '{name}' evaluation failed at row {row_idx + 1}: {exc}"
                        )

                    if mode != "json":
                        value = self._apply_single_value_output_mode(
                            value,
                            spec.get("single_value_output"),
                        )

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

    def _transform_map(
        self,
        data: list,
        config: dict,
        execution_context: Optional[Dict[str, Any]] = None,
    ) -> list:
        custom_specs = self._parse_custom_fields_config(config.get("custom_fields"))
        if not custom_specs and bool(config.get("custom_profile_enabled", False)):
            warn_msg = (
                "Custom profile mode is enabled, but no enabled custom fields were found. "
                "Enable at least one custom field in Custom Fields Studio."
            )
            node_warnings = execution_context.get("node_warnings") if isinstance(execution_context, dict) else None
            if isinstance(node_warnings, list) and warn_msg not in node_warnings:
                node_warnings.append(warn_msg)
            logger.warning(warn_msg)
        if custom_specs:
            return self._transform_custom_fields(
                data,
                config,
                custom_specs,
                execution_context=execution_context,
            )

        fields = self._parse_selected_fields(config.get("fields", ""))
        if not fields:
            return data

        should_abort_cb = (
            execution_context.get("should_abort")
            if isinstance(execution_context, dict) and callable(execution_context.get("should_abort"))
            else None
        )
        raise_if_aborted_cb = (
            execution_context.get("raise_if_aborted")
            if isinstance(execution_context, dict) and callable(execution_context.get("raise_if_aborted"))
            else None
        )

        def _raise_if_aborted() -> None:
            if callable(raise_if_aborted_cb):
                raise_if_aborted_cb()
                return
            if callable(should_abort_cb):
                try:
                    if bool(should_abort_cb()):
                        raise ExecutionAbortedError("Execution aborted by user.")
                except ExecutionAbortedError:
                    raise
                except Exception:
                    pass

        result = []
        matched_count = 0
        for idx, row in enumerate(data):
            if idx == 0 or idx % 500 == 0:
                _raise_if_aborted()
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
        - Relative path:
          - plain filename     → write to backend/outputs/<filename>
          - path with folders  → resolve relative to current working directory and use it directly
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

        expanded = os.path.expanduser(str(raw_path).strip())

        if os.path.isabs(expanded):
            # Absolute server path — use directly
            os.makedirs(os.path.dirname(expanded) or OUTPUTS_DIR, exist_ok=True)
            return expanded

        # Relative path:
        # - "result.csv" -> backend/outputs/result.csv
        # - "exports/result.csv" or "./exports/result.csv" -> <cwd>/exports/result.csv
        normalized = expanded.replace("\\", "/")
        has_subdir = "/" in normalized.strip("./")
        if has_subdir or expanded.startswith("."):
            rel_target = os.path.abspath(expanded)
            root, ext = os.path.splitext(rel_target)
            if not ext:
                rel_target = f"{root}{default_ext}"
            os.makedirs(os.path.dirname(rel_target) or OUTPUTS_DIR, exist_ok=True)
            return rel_target

        # Plain relative filename — keep writing inside backend outputs/
        fname = os.path.basename(expanded) or f"output_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}{default_ext}"
        return os.path.join(OUTPUTS_DIR, fname)

    # ─── DESTINATION IMPLEMENTATIONS ──────────────────────────────────────────

    async def _execute_destination(
        self,
        node_type: str,
        config: dict,
        data: list,
        execution_context: Optional[Dict[str, Any]] = None,
    ) -> list:
        """Load data into destination systems."""
        import pandas as pd, os
        logger.info(f"Loading {len(data)} rows to {node_type}")
        should_abort_cb = (
            execution_context.get("should_abort")
            if isinstance(execution_context, dict) and callable(execution_context.get("should_abort"))
            else None
        )
        raise_if_aborted_cb = (
            execution_context.get("raise_if_aborted")
            if isinstance(execution_context, dict) and callable(execution_context.get("raise_if_aborted"))
            else None
        )

        def _raise_if_aborted() -> None:
            if callable(raise_if_aborted_cb):
                raise_if_aborted_cb()
                return
            if callable(should_abort_cb):
                try:
                    if bool(should_abort_cb()):
                        raise ExecutionAbortedError("Execution aborted by user.")
                except ExecutionAbortedError:
                    raise
                except Exception:
                    pass

        _raise_if_aborted()

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
            try:
                indent = int(config.get("indent", 2))
            except Exception:
                indent = 2
            indent = max(0, min(indent, 8))

            date_format = str(config.get("date_format", "iso") or "iso").strip().lower()
            if date_format not in {"iso", "epoch"}:
                date_format = "iso"

            date_unit = str(config.get("date_unit", "ms") or "ms").strip().lower()
            if date_unit not in {"s", "ms", "us", "ns"}:
                date_unit = "ms"
            datetime_output_pattern = str(
                config.get("datetime_output_pattern", "%Y-%d-%m %H:%M:%S") or "%Y-%d-%m %H:%M:%S"
            ).strip() or "%Y-%d-%m %H:%M:%S"

            date_only_midnight = bool(config.get("date_only_midnight", True))
            df_json = df.copy()
            if date_format == "iso" and date_only_midnight and not df_json.empty:
                for col in list(df_json.columns):
                    series = df_json[col]
                    if pd.api.types.is_datetime64_any_dtype(series):
                        dt = pd.to_datetime(series, errors="coerce")
                        valid = dt.dropna()
                        if valid.empty:
                            continue
                        is_midnight = (
                            (valid.dt.hour == 0)
                            & (valid.dt.minute == 0)
                            & (valid.dt.second == 0)
                            & (valid.dt.microsecond == 0)
                        )
                        if bool(is_midnight.all()):
                            df_json[col] = dt.dt.strftime("%Y-%m-%d").where(dt.notna(), None)
                        else:
                            df_json[col] = dt.dt.strftime(datetime_output_pattern).where(dt.notna(), None)
                        continue
                    if pd.api.types.is_object_dtype(series):
                        non_null = series.dropna()
                        if non_null.empty:
                            continue
                        temporal_sample = non_null.head(100)
                        has_temporal = any(
                            isinstance(v, (datetime, date, time, pd.Timestamp))
                            for v in temporal_sample
                        )
                        if not has_temporal:
                            continue

                        def _normalize_temporal_obj(value: Any) -> Any:
                            if value is None:
                                return None
                            if isinstance(value, float) and math.isnan(value):
                                return None
                            if isinstance(value, pd.Timestamp):
                                try:
                                    value = value.to_pydatetime()
                                except Exception:
                                    return str(value)
                            if isinstance(value, datetime):
                                if (
                                    value.hour == 0
                                    and value.minute == 0
                                    and value.second == 0
                                    and value.microsecond == 0
                                ):
                                    return value.date().isoformat()
                                return value.strftime(datetime_output_pattern)
                            if isinstance(value, date):
                                return value.isoformat()
                            if isinstance(value, time):
                                return value.isoformat()
                            return value

                        df_json[col] = series.map(_normalize_temporal_obj)

            df_json.to_json(
                out_path,
                orient=orient,
                indent=indent,
                force_ascii=False,
                date_format=date_format,
                date_unit=date_unit,
                default_handler=str,
            )
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
            return await self._dest_oracle(config, df, execution_context=execution_context)

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

    async def _dest_oracle(
        self,
        config: dict,
        df,
        execution_context: Optional[Dict[str, Any]] = None,
    ) -> list:
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

            should_abort_cb = (
                execution_context.get("should_abort")
                if isinstance(execution_context, dict) and callable(execution_context.get("should_abort"))
                else None
            )
            raise_if_aborted_cb = (
                execution_context.get("raise_if_aborted")
                if isinstance(execution_context, dict) and callable(execution_context.get("raise_if_aborted"))
                else None
            )

            def _raise_if_aborted() -> None:
                if callable(raise_if_aborted_cb):
                    raise_if_aborted_cb()
                    return
                if callable(should_abort_cb):
                    try:
                        if bool(should_abort_cb()):
                            raise ExecutionAbortedError("Execution aborted by user.")
                    except ExecutionAbortedError:
                        raise
                    except Exception:
                        pass

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

            def _json_safe(value: Any) -> Any:
                if value is None:
                    return None
                if isinstance(value, float) and math.isnan(value):
                    return None
                if isinstance(value, (str, int, float, bool)):
                    return value
                if isinstance(value, datetime):
                    if (
                        value.hour == 0
                        and value.minute == 0
                        and value.second == 0
                        and value.microsecond == 0
                    ):
                        return value.date().isoformat()
                    return value.strftime("%Y-%d-%m %H:%M:%S")
                if isinstance(value, date):
                    return value.isoformat()
                if isinstance(value, time):
                    return value.isoformat()
                if isinstance(value, pd.Timestamp):
                    try:
                        py_dt = value.to_pydatetime()
                        if (
                            py_dt.hour == 0
                            and py_dt.minute == 0
                            and py_dt.second == 0
                            and py_dt.microsecond == 0
                        ):
                            return py_dt.date().isoformat()
                        return py_dt.strftime("%Y-%d-%m %H:%M:%S")
                    except Exception:
                        return str(value)
                if isinstance(value, dict):
                    return {str(k): _json_safe(v) for k, v in value.items()}
                if isinstance(value, (list, tuple, set)):
                    return [_json_safe(v) for v in value]
                try:
                    if pd.isna(value):
                        return None
                except Exception:
                    pass
                return str(value)

            def _normalize_value(value: Any) -> Any:
                if value is None:
                    return None
                if isinstance(value, float) and math.isnan(value):
                    return None
                if isinstance(value, dict):
                    return json.dumps(_json_safe(value), ensure_ascii=False)
                if isinstance(value, (list, tuple)):
                    return json.dumps(_json_safe(list(value)), ensure_ascii=False)
                if isinstance(value, set):
                    return json.dumps(_json_safe(list(value)), ensure_ascii=False)
                if isinstance(value, pd.Timestamp):
                    return value.to_pydatetime()
                if isinstance(value, (datetime, date, time)):
                    return value
                try:
                    if pd.isna(value):
                        return None
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
                _raise_if_aborted()
                for stmt in pre_statements:
                    _raise_if_aborted()
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
                        if row_idx == 0 or row_idx % 50 == 0:
                            _raise_if_aborted()
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
                    _raise_if_aborted()
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
        except ExecutionAbortedError:
            raise
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
