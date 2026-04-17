"""
ETL Flow Platform — FastAPI Backend
Full ETL operation platform with support for databases, files, APIs, cloud storage, message queues
"""
import uuid
import json
import asyncio
import os as _os
import html as _html
import re
import math
import base64
import smtplib
import ssl
from datetime import datetime, timedelta, date, time
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from contextlib import asynccontextmanager
from email.message import EmailMessage
from email.utils import make_msgid

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy.orm import Session, load_only
from loguru import logger

# Load environment from backend/.env and project-root/.env if present.
_BACKEND_DIR = Path(__file__).resolve().parent
load_dotenv(_BACKEND_DIR / ".env")
load_dotenv(_BACKEND_DIR.parent / ".env")

from database import engine, get_db, Base, SessionLocal
import models
from etl_engine import ETLEngine


# ─── STARTUP ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    recovered = _recover_stale_running_executions()
    if recovered:
        logger.warning(f"Recovered {recovered} stale running execution(s) on startup")
    if not scheduler.running:
        scheduler.start()
    _reload_all_pipeline_schedule_jobs()
    logger.info("✅ ETL Flow Platform started")
    yield
    if scheduler.running:
        scheduler.shutdown(wait=False)
    logger.info("ETL Flow Platform shutting down")

app = FastAPI(
    title="ETL Flow Platform",
    description="World-class ETL pipeline orchestration platform",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

etl_engine = ETLEngine()
scheduler = AsyncIOScheduler()
H2O_MODELS_DIR = _BACKEND_DIR / "outputs" / "mlops_h2o_models"
H2O_MOJO_DIR = _BACKEND_DIR / "outputs" / "mlops_h2o_mojo"
H2O_REQUEST_SOURCE_TYPES = {"pipeline", "file", "sample", "workflow", "rows", "etl"}
_H2O_MODEL_CACHE: Dict[str, Any] = {}
_H2O_ALGO_CANONICAL = {
    "gbm": "GBM",
    "drf": "DRF",
    "xrt": "XRT",
    "xgboost": "XGBoost",
    "glm": "GLM",
    "deeplearning": "DeepLearning",
    "stackedensemble": "StackedEnsemble",
}
_H2O_ALLOWED_ALGOS = sorted(set(_H2O_ALGO_CANONICAL.values()))


def _recover_stale_running_executions() -> int:
    """
    Mark executions that were left in 'running' (typically after backend restart/crash)
    as failed so UI polling/history do not stay stuck forever.
    """
    db = SessionLocal()
    try:
        stale_runs = db.query(models.Execution).filter(
            models.Execution.status == "running"
        ).all()
        if not stale_runs:
            return 0
        now = datetime.utcnow()
        for run in stale_runs:
            logs = run.logs if isinstance(run.logs, list) else []
            has_running_log = any(
                isinstance(entry, dict)
                and str(entry.get("status") or "").strip().lower() == "running"
                for entry in logs
            )
            has_error_log = any(
                isinstance(entry, dict)
                and str(entry.get("status") or "").strip().lower() == "error"
                for entry in logs
            )
            if logs and not has_running_log:
                run.status = "failed" if has_error_log else "success"
                if run.status == "failed" and not run.error_message:
                    err_msgs = [
                        str(entry.get("message") or "").strip()
                        for entry in logs
                        if isinstance(entry, dict)
                        and str(entry.get("status") or "").strip().lower() == "error"
                    ]
                    if err_msgs:
                        run.error_message = err_msgs[-1]
                if run.status == "success" and (run.rows_processed or 0) <= 0:
                    inferred_rows = 0
                    for entry in logs:
                        if not isinstance(entry, dict):
                            continue
                        try:
                            inferred_rows += int(entry.get("rows") or 0)
                        except Exception:
                            continue
                    if inferred_rows > 0:
                        run.rows_processed = inferred_rows
            else:
                run.status = "failed"
                if not run.error_message:
                    run.error_message = "Execution interrupted by backend restart."
            run.finished_at = now
        db.commit()
        return len(stale_runs)
    except Exception as exc:
        db.rollback()
        logger.error(f"Failed to recover stale executions: {exc}")
        return 0
    finally:
        db.close()


# ─── WEBSOCKET MANAGER ────────────────────────────────────────────────────────

class WebSocketManager:
    def __init__(self):
        self.connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, execution_id: str, ws: WebSocket):
        await ws.accept()
        self.connections.setdefault(execution_id, []).append(ws)

    def disconnect(self, execution_id: str, ws: WebSocket):
        if execution_id in self.connections:
            try:
                self.connections[execution_id].remove(ws)
            except ValueError:
                pass

    async def broadcast(self, execution_id: str, data: dict):
        for ws in self.connections.get(execution_id, []):
            try:
                await ws.send_json(data)
            except Exception:
                pass

ws_manager = WebSocketManager()


# ─── SCHEDULER ────────────────────────────────────────────────────────────────

SCHEDULE_JOB_PREFIX = "pipeline_schedule:"
SCHEDULE_PRESET_TO_CRON = {
    "every_1_min": "* * * * *",
    "every_5_min": "*/5 * * * *",
    "every_15_min": "*/15 * * * *",
    "hourly": "0 * * * *",
    "daily_9am": "0 9 * * *",
    "weekly_mon_9am": "0 9 * * 1",
}
EXECUTION_MODE_OPTIONS = {"batch", "incremental", "streaming"}
DEFAULT_EXECUTION_MODE = "batch"


def _schedule_job_id(pipeline_id: str) -> str:
    return f"{SCHEDULE_JOB_PREFIX}{pipeline_id}"


def _normalize_cron_expr(cron_expr: Optional[str]) -> str:
    normalized = " ".join(str(cron_expr or "").strip().split())
    if not normalized:
        raise ValueError("Cron expression is required")
    parts = normalized.split(" ")
    if len(parts) != 5:
        raise ValueError("Cron expression must have exactly 5 fields")
    return normalized


def _ensure_valid_cron_expr(cron_expr: Optional[str]) -> str:
    normalized = _normalize_cron_expr(cron_expr)
    try:
        CronTrigger.from_crontab(normalized)
    except Exception as exc:
        raise ValueError(f"Invalid cron expression: {normalized}") from exc
    return normalized


def _pipeline_next_scheduled_at(pipeline_id: str) -> Optional[str]:
    job = scheduler.get_job(_schedule_job_id(pipeline_id))
    if not job or not job.next_run_time:
        return None
    return job.next_run_time.isoformat()


def _extract_schedule_from_nodes(nodes: Any) -> Dict[str, Any]:
    """Derive pipeline schedule fields from schedule_trigger node config."""
    if not isinstance(nodes, list):
        return {}

    schedule_node = None
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_data = node.get("data") if isinstance(node.get("data"), dict) else {}
        node_type = str(node_data.get("nodeType") or node.get("type") or "")
        if node_type == "schedule_trigger":
            schedule_node = node
            break

    if schedule_node is None:
        # No schedule trigger in pipeline graph => disable backend schedule.
        return {"schedule_enabled": False}

    node_data = schedule_node.get("data") if isinstance(schedule_node.get("data"), dict) else {}
    config = node_data.get("config") if isinstance(node_data.get("config"), dict) else {}

    enabled = bool(config.get("enabled", True))
    if not enabled:
        return {"schedule_enabled": False}

    preset = str(config.get("schedule_preset") or "").strip()
    preset_cron = SCHEDULE_PRESET_TO_CRON.get(preset)
    raw_cron = str(config.get("cron") or "").strip()
    candidate = preset_cron or raw_cron
    if not candidate:
        return {"schedule_enabled": False}

    try:
        normalized = _ensure_valid_cron_expr(candidate)
    except ValueError:
        logger.warning(
            f"Schedule trigger has invalid cron '{candidate}'. Keeping existing pipeline schedule."
        )
        return {}

    return {
        "schedule_enabled": True,
        "schedule_cron": normalized,
    }


def _extract_execution_runtime_from_nodes(
    nodes: Any,
    triggered_by: Optional[str] = None,
) -> Dict[str, Any]:
    """Derive ETL execution runtime options from trigger node config."""
    runtime = {
        "execution_mode": DEFAULT_EXECUTION_MODE,
        "batch_size": 5000,
        "incremental_field": "",
        "streaming_interval_seconds": 5,
        "streaming_max_batches": 10,
    }
    if not isinstance(nodes, list):
        return runtime

    trigger_nodes: List[dict] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_data = node.get("data") if isinstance(node.get("data"), dict) else {}
        node_type = str(node_data.get("nodeType") or node.get("type") or "")
        if node_type in {"manual_trigger", "schedule_trigger", "webhook_trigger"}:
            trigger_nodes.append(node)

    if not trigger_nodes:
        return runtime

    trigger_kind_by_source = {
        "manual": "manual_trigger",
        "schedule": "schedule_trigger",
        "webhook": "webhook_trigger",
    }
    preferred_trigger_kind = trigger_kind_by_source.get(str(triggered_by or "").strip().lower())
    if preferred_trigger_kind:
        matching = []
        for node in trigger_nodes:
            node_data = node.get("data") if isinstance(node.get("data"), dict) else {}
            node_type = str(node_data.get("nodeType") or node.get("type") or "")
            if node_type == preferred_trigger_kind:
                matching.append(node)
        if matching:
            trigger_nodes = matching

    priority = {"schedule_trigger": 0, "manual_trigger": 1, "webhook_trigger": 2}

    def _trigger_score(node: dict) -> Tuple[int, int]:
        node_data = node.get("data") if isinstance(node.get("data"), dict) else {}
        node_type = str(node_data.get("nodeType") or node.get("type") or "")
        config = node_data.get("config") if isinstance(node_data.get("config"), dict) else {}
        explicit_mode = str(config.get("execution_mode") or "").strip().lower()
        has_explicit_mode = 0 if explicit_mode in EXECUTION_MODE_OPTIONS else 1
        return (has_explicit_mode, priority.get(node_type, 99))

    trigger_node = sorted(trigger_nodes, key=_trigger_score)[0]
    node_data = trigger_node.get("data") if isinstance(trigger_node.get("data"), dict) else {}
    config = node_data.get("config") if isinstance(node_data.get("config"), dict) else {}

    mode = str(config.get("execution_mode") or runtime["execution_mode"]).strip().lower()
    if mode not in EXECUTION_MODE_OPTIONS:
        mode = DEFAULT_EXECUTION_MODE

    def _bounded_int(value: Any, default: int, min_v: int, max_v: int) -> int:
        try:
            parsed = int(value)
        except Exception:
            parsed = default
        return max(min_v, min(parsed, max_v))

    return {
        "execution_mode": mode,
        "batch_size": _bounded_int(config.get("batch_size"), int(runtime["batch_size"]), 1, 1_000_000),
        "incremental_field": str(config.get("incremental_field") or "").strip(),
        "streaming_interval_seconds": _bounded_int(config.get("streaming_interval_seconds"), int(runtime["streaming_interval_seconds"]), 1, 3600),
        "streaming_max_batches": _bounded_int(config.get("streaming_max_batches"), int(runtime["streaming_max_batches"]), 1, 10_000),
    }


def _flush_logs(execution, logs: list, db):
    """Persist current logs snapshot to DB so the poll endpoint sees live progress."""
    try:
        execution.logs = _json_safe_value(list(logs))
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.warning(
            f"Execution {getattr(execution, 'id', '<unknown>')} log flush failed: {exc}"
        )


def _json_safe_value(value: Any) -> Any:
    """
    Convert nested values into JSON-safe primitives for DB JSON columns.
    This prevents commit failures when rows contain datetime/decimal/bytes, etc.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, (datetime, date, time)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)

    if isinstance(value, Decimal):
        try:
            return float(value)
        except Exception:
            return str(value)

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return base64.b64encode(value).decode("ascii")

    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k, v in value.items():
            out[str(k)] = _json_safe_value(v)
        return out

    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(item) for item in value]

    # Support numpy/pandas scalar-style objects when present.
    item_method = getattr(value, "item", None)
    if callable(item_method):
        try:
            return _json_safe_value(item_method())
        except Exception:
            pass

    return str(value)


def _apply_execution_terminal_payload(execution, payload: Dict[str, Any]) -> None:
    execution.status = payload.get("status", "failed")
    execution.finished_at = payload.get("finished_at") or datetime.utcnow()
    execution.error_message = payload.get("error_message")
    if payload.get("node_results") is not None:
        execution.node_results = _json_safe_value(payload["node_results"])
    if payload.get("logs") is not None:
        execution.logs = _json_safe_value(payload["logs"])
    if payload.get("rows_processed") is not None:
        execution.rows_processed = payload["rows_processed"]


def _persist_execution_terminal_payload(execution_id: str, payload: Dict[str, Any]) -> bool:
    recovery_db = SessionLocal()
    try:
        execution = recovery_db.query(models.Execution).filter(
            models.Execution.id == execution_id
        ).first()
        if execution is None:
            return False
        _apply_execution_terminal_payload(execution, payload)
        recovery_db.commit()
        return True
    except Exception as exc:
        recovery_db.rollback()
        logger.error(
            f"Failed to persist terminal state for execution {execution_id}: {exc}"
        )
        return False
    finally:
        recovery_db.close()


def _compact_node_result_for_history(value: Any, max_rows: int) -> Any:
    """
    Keep execution history lightweight by truncating very large in-memory node outputs.
    Destination metadata rows (with output `path`) are preserved as-is.
    """
    if max_rows <= 0:
        max_rows = 1

    if isinstance(value, list):
        if not value:
            return value

        if all(isinstance(item, dict) for item in value):
            first = value[0]
            try:
                # Keep destination/cache metadata rows untouched so file-backed results still resolve.
                if isinstance(first, dict) and _looks_like_pipeline_metadata_row(first):
                    return value
            except Exception:
                pass

            if len(value) <= max_rows:
                return value

            sample = [row for row in value[:max_rows] if isinstance(row, dict)]
            return {
                "kind": "truncated_rows",
                "rows": len(value),
                "sample_rows": len(sample),
                "data": sample,
                "note": f"Execution history truncated to first {len(sample)} rows for performance.",
            }

        if len(value) <= max_rows:
            return value

        sample_values = value[:max_rows]
        sample_rows = [
            item if isinstance(item, dict) else {"value": item}
            for item in sample_values
        ]
        return {
            "kind": "truncated_values",
            "rows": len(value),
            "sample_rows": max_rows,
            "data": sample_rows,
            "note": f"Execution history truncated to first {max_rows} values for performance.",
        }

    if isinstance(value, dict):
        nested = value.get("data")
        if isinstance(nested, list) and len(nested) > max_rows:
            compact = dict(value)
            clipped = nested[:max_rows]
            compact["data"] = [
                item if isinstance(item, dict) else {"value": item}
                for item in clipped
            ]
            compact["rows"] = len(nested)
            compact["sample_rows"] = max_rows
            compact["note"] = f"Execution history truncated to first {max_rows} rows for performance."
            return compact
    return value


def _compact_execution_node_results_for_history(node_results: Any) -> Any:
    if not isinstance(node_results, dict):
        return node_results

    max_rows = max(50, min(int(_os.getenv("EXECUTION_HISTORY_MAX_ROWS", "2000")), 50000))
    compact: Dict[str, Any] = {}
    for node_id, value in node_results.items():
        compact[node_id] = _compact_node_result_for_history(value, max_rows)
    return compact


async def _run_pipeline_execution_task(execution_id: str, pipeline_id: str):
    bg_db = SessionLocal()
    bg_exec = None
    terminal_payload: Dict[str, Any] = {
        "status": "failed",
        "error_message": "Execution ended unexpectedly",
        "finished_at": None,
        "node_results": None,
        "logs": None,
        "rows_processed": None,
    }
    try:
        bg_exec = bg_db.query(models.Execution).filter(
            models.Execution.id == execution_id
        ).first()
        pipeline = bg_db.query(models.Pipeline).filter(
            models.Pipeline.id == pipeline_id
        ).first()
        if not bg_exec:
            return
        if not pipeline:
            terminal_payload.update({
                "status": "failed",
                "error_message": "Pipeline not found",
            })
            return

        pipeline_data = {"nodes": pipeline.nodes or [], "edges": pipeline.edges or []}
        runtime_config = _extract_execution_runtime_from_nodes(
            pipeline.nodes or [],
            triggered_by=getattr(bg_exec, "triggered_by", None),
        )
        if (
            str(getattr(bg_exec, "triggered_by", "")).strip().lower() == "manual"
            and str(runtime_config.get("execution_mode", "")).strip().lower() == "streaming"
        ):
            runtime_config["streaming_max_batches"] = 1
        result = await etl_engine.execute_pipeline(
            pipeline_data,
            execution_id,
            bg_db,
            ws_manager,
            on_node_done=lambda logs: _flush_logs(bg_exec, logs, bg_db),
            runtime_config=runtime_config,
            pipeline_id=pipeline.id,
        )
        terminal_payload.update({
            "status": "success",
            "error_message": None,
            "node_results": _compact_execution_node_results_for_history(result["node_results"]),
            "logs": result["logs"],
            "rows_processed": result["rows_processed"],
        })
        logger.info(f"✅ Execution {execution_id} completed — {result['rows_processed']} rows")

    except Exception as exc:
        logger.error(f"❌ Execution {execution_id} failed: {exc}")
        terminal_payload.update({
            "status": "failed",
            "error_message": str(exc),
        })

    finally:
        terminal_payload["finished_at"] = datetime.utcnow()
        committed = False
        if bg_exec is not None:
            _apply_execution_terminal_payload(bg_exec, terminal_payload)
        try:
            bg_db.commit()
            committed = True
        except Exception as exc:
            bg_db.rollback()
            logger.error(
                f"Execution {execution_id} final commit failed in worker session: {exc}"
            )
        finally:
            bg_db.close()

        if not committed:
            _persist_execution_terminal_payload(execution_id, terminal_payload)


def _start_pipeline_execution(
    pipeline_id: str,
    triggered_by: str = "manual",
    skip_if_running: bool = False,
) -> Optional[str]:
    db = SessionLocal()
    execution_id: Optional[str] = None
    try:
        pipeline = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
        if not pipeline:
            logger.warning(f"Skipping execution: pipeline {pipeline_id} not found")
            return None

        if skip_if_running:
            running = db.query(models.Execution).filter(
                models.Execution.pipeline_id == pipeline_id,
                models.Execution.status == "running",
            ).first()
            if running:
                logger.info(
                    f"Skipping scheduled run for pipeline {pipeline_id} — execution {running.id} is still running"
                )
                return None

        execution_id = str(uuid.uuid4())
        execution = models.Execution(
            id=execution_id,
            pipeline_id=pipeline_id,
            status="running",
            triggered_by=triggered_by,
        )
        db.add(execution)
        db.commit()
    finally:
        db.close()

    if execution_id:
        asyncio.create_task(_run_pipeline_execution_task(execution_id, pipeline_id))
    return execution_id


async def _run_scheduled_pipeline(pipeline_id: str):
    execution_id = _start_pipeline_execution(
        pipeline_id=pipeline_id,
        triggered_by="schedule",
        skip_if_running=True,
    )
    if execution_id:
        logger.info(f"⏰ Scheduled pipeline {pipeline_id} started execution {execution_id}")


def _sync_pipeline_schedule_job(pipeline: models.Pipeline):
    job_id = _schedule_job_id(pipeline.id)
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    if not pipeline.schedule_enabled:
        return

    if not pipeline.schedule_cron:
        logger.warning(
            f"Schedule enabled for pipeline {pipeline.id} but cron is empty; schedule job not created"
        )
        return

    try:
        normalized_cron = _ensure_valid_cron_expr(pipeline.schedule_cron)
        trigger = CronTrigger.from_crontab(normalized_cron)
    except ValueError as exc:
        logger.error(
            f"Invalid schedule for pipeline {pipeline.id}: {pipeline.schedule_cron} ({exc})"
        )
        return

    scheduler.add_job(
        _run_scheduled_pipeline,
        trigger=trigger,
        args=[pipeline.id],
        id=job_id,
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    logger.info(f"Scheduled pipeline {pipeline.id} with cron '{normalized_cron}'")


def _reload_all_pipeline_schedule_jobs():
    if scheduler.running:
        for job in scheduler.get_jobs():
            if job.id.startswith(SCHEDULE_JOB_PREFIX):
                scheduler.remove_job(job.id)

    db = SessionLocal()
    try:
        pipelines = db.query(models.Pipeline).all()
        for pipeline in pipelines:
            _sync_pipeline_schedule_job(pipeline)
    finally:
        db.close()


# ─── PYDANTIC SCHEMAS ─────────────────────────────────────────────────────────

class PipelineCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    nodes: list = []
    edges: list = []
    tags: list = []
    status: str = "draft"

class PipelineUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    nodes: Optional[list] = None
    edges: Optional[list] = None
    tags: Optional[list] = None
    status: Optional[str] = None
    schedule_cron: Optional[str] = None
    schedule_enabled: Optional[bool] = None

class MLOpsWorkflowCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    nodes: list = []
    edges: list = []
    tags: list = []
    status: str = "draft"

class MLOpsWorkflowUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    nodes: Optional[list] = None
    edges: Optional[list] = None
    tags: Optional[list] = None
    status: Optional[str] = None
    schedule_cron: Optional[str] = None
    schedule_enabled: Optional[bool] = None

class BusinessWorkflowCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    nodes: list = []
    edges: list = []
    tags: list = []
    status: str = "draft"

class BusinessWorkflowUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    nodes: Optional[list] = None
    edges: Optional[list] = None
    tags: Optional[list] = None
    status: Optional[str] = None

class MLOpsEvaluateRequest(BaseModel):
    source_type: str = "pipeline"  # pipeline | file | sample
    pipeline_id: Optional[str] = None
    pipeline_node_id: Optional[str] = None
    file_path: Optional[str] = None
    dataset: Optional[str] = "sales"
    target_column: Optional[str] = None
    feature_columns: Optional[List[str]] = None
    test_size: float = 0.2
    task: str = "auto"  # auto | regression | classification

class MLOpsFeatureProfileRequest(BaseModel):
    node_id: Optional[str] = None
    sample_size: int = 2000

class H2OAutoMLTrainRequest(BaseModel):
    source_type: str = "pipeline"  # pipeline | file | sample | workflow | rows | etl
    pipeline_id: Optional[str] = None
    pipeline_node_id: Optional[str] = None
    file_path: Optional[str] = None
    dataset: Optional[str] = "sales"
    workflow_id: Optional[str] = None
    use_workflow_preprocessing: bool = True
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    target_column: Optional[str] = None
    feature_columns: Optional[List[str]] = None
    exclude_columns: List[str] = Field(default_factory=list)
    task: str = "auto"  # auto | regression | classification
    train_ratio: float = 0.8
    max_models: int = 20
    max_runtime_secs: int = 300
    seed: int = 42
    nfolds: int = 5
    balance_classes: bool = False
    include_algos: List[str] = Field(default_factory=list)
    exclude_algos: List[str] = Field(default_factory=list)
    sort_metric: Optional[str] = None
    stopping_metric: Optional[str] = None
    project_name: Optional[str] = None

class H2OModelPredictSingleRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    run_id: Optional[str] = None
    model_path: Optional[str] = None
    row: Dict[str, Any] = Field(default_factory=dict)

class H2OModelPredictBatchRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    run_id: Optional[str] = None
    model_path: Optional[str] = None
    source_type: str = "rows"  # rows | pipeline | file | sample | workflow | etl
    pipeline_id: Optional[str] = None
    pipeline_node_id: Optional[str] = None
    file_path: Optional[str] = None
    dataset: Optional[str] = "sales"
    workflow_id: Optional[str] = None
    use_workflow_preprocessing: bool = True
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    max_rows: int = 50000

class H2OModelEvaluateRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    run_id: Optional[str] = None
    model_path: Optional[str] = None
    source_type: str = "rows"  # rows | pipeline | file | sample | workflow | etl
    pipeline_id: Optional[str] = None
    pipeline_node_id: Optional[str] = None
    file_path: Optional[str] = None
    dataset: Optional[str] = "sales"
    workflow_id: Optional[str] = None
    use_workflow_preprocessing: bool = True
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    target_column: Optional[str] = None
    max_rows: int = 50000

class H2OSourceColumnsRequest(BaseModel):
    source_type: str = "rows"  # rows | pipeline | file | sample | workflow | etl
    pipeline_id: Optional[str] = None
    pipeline_node_id: Optional[str] = None
    file_path: Optional[str] = None
    dataset: Optional[str] = "sales"
    workflow_id: Optional[str] = None
    use_workflow_preprocessing: bool = True
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    max_profile_rows: int = 5000
    max_preview_rows: int = 25

class H2ORunManageRequest(BaseModel):
    label: Optional[str] = None

class JsonFieldOptionsRequest(BaseModel):
    node_type: str
    config: dict = {}
    max_paths: int = 200

class SourceFieldOptionsRequest(BaseModel):
    node_type: str
    config: dict = {}
    max_rows: int = 200


class CustomFieldValidationRequest(BaseModel):
    config: Dict[str, Any] = Field(default_factory=dict)
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    max_rows: int = 30

class CredentialCreate(BaseModel):
    name: str
    type: str
    data: dict = {}

class CredentialUpdate(BaseModel):
    name: Optional[str] = None
    data: Optional[dict] = None


def _build_pipeline_profile_state_summary(
    pipeline_id: str,
    node_id: Optional[str] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    safe_limit = max(1, min(int(limit or 10), 100))
    runtime_state = etl_engine._load_runtime_state()
    pipelines_state = runtime_state.get("pipelines") if isinstance(runtime_state, dict) else {}
    if not isinstance(pipelines_state, dict):
        pipelines_state = {}
    pipeline_state = pipelines_state.get(pipeline_id)
    if not isinstance(pipeline_state, dict):
        pipeline_state = {}
    profile_documents = pipeline_state.get("profile_documents")
    if not isinstance(profile_documents, dict):
        profile_documents = {}

    nodes: List[Dict[str, Any]] = []
    total_entities = 0
    total_meta_entries = 0
    available_node_ids: List[str] = []

    for nid, node_state in profile_documents.items():
        if node_id and str(nid) != str(node_id):
            continue
        if not isinstance(node_state, dict):
            continue
        node_name = str(nid)
        available_node_ids.append(node_name)
        documents = node_state.get("documents")
        if not isinstance(documents, dict):
            documents = {}
        meta = node_state.get("meta")
        if not isinstance(meta, dict):
            meta = {}

        entity_keys = list(documents.keys())
        entity_count = len(entity_keys)
        meta_count = len(meta)
        total_entities += entity_count
        total_meta_entries += meta_count

        sample_entity_keys = entity_keys[:safe_limit]
        sample_documents: List[Dict[str, Any]] = []
        for entity_key in sample_entity_keys:
            sample_documents.append(
                {
                    "entity_key": str(entity_key),
                    "profile": _json_safe_value(documents.get(entity_key)),
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
        "pipeline_id": pipeline_id,
        "node_id": node_id or None,
        "limit": safe_limit,
        "total_nodes": len(nodes),
        "total_entities": total_entities,
        "total_meta_entries": total_meta_entries,
        "available_node_ids": available_node_ids,
        "nodes": nodes,
        "generated_at": datetime.utcnow().isoformat(),
    }


def _clear_pipeline_profile_state(
    pipeline_id: str,
    node_id: Optional[str] = None,
) -> Dict[str, Any]:
    runtime_state = etl_engine._load_runtime_state()
    pipelines_state = runtime_state.get("pipelines") if isinstance(runtime_state, dict) else {}
    if not isinstance(pipelines_state, dict):
        pipelines_state = {}
        runtime_state["pipelines"] = pipelines_state

    pipeline_state = pipelines_state.get(pipeline_id)
    if not isinstance(pipeline_state, dict):
        return {
            "pipeline_id": pipeline_id,
            "node_id": node_id or None,
            "removed_nodes": 0,
            "removed_entities": 0,
            "removed_meta_entries": 0,
            "remaining_summary": _build_pipeline_profile_state_summary(pipeline_id, node_id=node_id),
            "cleared_at": datetime.utcnow().isoformat(),
        }

    profile_documents = pipeline_state.get("profile_documents")
    if not isinstance(profile_documents, dict):
        return {
            "pipeline_id": pipeline_id,
            "node_id": node_id or None,
            "removed_nodes": 0,
            "removed_entities": 0,
            "removed_meta_entries": 0,
            "remaining_summary": _build_pipeline_profile_state_summary(pipeline_id, node_id=node_id),
            "cleared_at": datetime.utcnow().isoformat(),
        }

    removed_nodes = 0
    removed_entities = 0
    removed_meta_entries = 0

    if node_id:
        node_key = str(node_id)
        node_state = profile_documents.pop(node_key, None)
        if isinstance(node_state, dict):
            removed_nodes = 1
            docs = node_state.get("documents")
            if isinstance(docs, dict):
                removed_entities += len(docs)
            meta = node_state.get("meta")
            if isinstance(meta, dict):
                removed_meta_entries += len(meta)
    else:
        for node_state in profile_documents.values():
            if not isinstance(node_state, dict):
                continue
            removed_nodes += 1
            docs = node_state.get("documents")
            if isinstance(docs, dict):
                removed_entities += len(docs)
            meta = node_state.get("meta")
            if isinstance(meta, dict):
                removed_meta_entries += len(meta)
        pipeline_state.pop("profile_documents", None)

    etl_engine._save_runtime_state(runtime_state)

    return {
        "pipeline_id": pipeline_id,
        "node_id": node_id or None,
        "removed_nodes": removed_nodes,
        "removed_entities": removed_entities,
        "removed_meta_entries": removed_meta_entries,
        "remaining_summary": _build_pipeline_profile_state_summary(pipeline_id, node_id=node_id),
        "cleared_at": datetime.utcnow().isoformat(),
    }


# ─── PIPELINES ────────────────────────────────────────────────────────────────

@app.get("/api/pipelines")
async def list_pipelines(db: Session = Depends(get_db)):
    pipelines = db.query(models.Pipeline).order_by(models.Pipeline.updated_at.desc()).all()
    pipeline_ids = [p.id for p in pipelines if getattr(p, "id", None)]
    last_exec_by_pipeline: Dict[str, Dict[str, Any]] = {}
    if pipeline_ids:
        exec_rows = db.query(
            models.Execution.id,
            models.Execution.pipeline_id,
            models.Execution.status,
            models.Execution.started_at,
            models.Execution.rows_processed,
        ).filter(
            models.Execution.pipeline_id.in_(pipeline_ids)
        ).order_by(
            models.Execution.pipeline_id.asc(),
            models.Execution.started_at.desc(),
        ).all()
        for ex_id, ex_pipeline_id, ex_status, ex_started_at, ex_rows in exec_rows:
            if ex_pipeline_id in last_exec_by_pipeline:
                continue
            last_exec_by_pipeline[ex_pipeline_id] = {
                "id": ex_id,
                "status": ex_status,
                "started_at": ex_started_at.isoformat() if ex_started_at else None,
                "rows_processed": ex_rows,
            }

    result = []
    for p in pipelines:
        last_exec = last_exec_by_pipeline.get(p.id)
        runtime = _extract_execution_runtime_from_nodes(p.nodes or [])
        result.append({
            "id": p.id, "name": p.name, "description": p.description,
            "status": p.status, "tags": p.tags or [],
            "nodeCount": len(p.nodes or []),
            "schedule_cron": p.schedule_cron,
            "schedule_enabled": p.schedule_enabled,
            "execution_mode": runtime["execution_mode"],
            "batch_size": runtime["batch_size"],
            "incremental_field": runtime["incremental_field"],
            "streaming_interval_seconds": runtime["streaming_interval_seconds"],
            "streaming_max_batches": runtime["streaming_max_batches"],
            "next_scheduled_at": _pipeline_next_scheduled_at(p.id),
            "created_at": p.created_at.isoformat() if p.created_at else None,
            "updated_at": p.updated_at.isoformat() if p.updated_at else None,
            "last_execution": last_exec if last_exec else None
        })
    return result

@app.post("/api/pipelines", status_code=201)
async def create_pipeline(body: PipelineCreate, db: Session = Depends(get_db)):
    schedule_fields = _extract_schedule_from_nodes(body.nodes)
    pipeline = models.Pipeline(
        id=str(uuid.uuid4()),
        name=body.name,
        description=body.description,
        nodes=body.nodes,
        edges=body.edges,
        tags=body.tags,
        status=body.status,
        schedule_cron=schedule_fields.get("schedule_cron"),
        schedule_enabled=bool(schedule_fields.get("schedule_enabled", False)),
    )
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    _sync_pipeline_schedule_job(pipeline)
    return {"id": pipeline.id, "name": pipeline.name, "status": pipeline.status}

@app.get("/api/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str, db: Session = Depends(get_db)):
    p = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    runtime = _extract_execution_runtime_from_nodes(p.nodes or [])
    return {
        "id": p.id, "name": p.name, "description": p.description,
        "nodes": p.nodes or [], "edges": p.edges or [],
        "status": p.status, "tags": p.tags or [],
        "schedule_cron": p.schedule_cron,
        "schedule_enabled": p.schedule_enabled,
        "execution_mode": runtime["execution_mode"],
        "batch_size": runtime["batch_size"],
        "incremental_field": runtime["incremental_field"],
        "streaming_interval_seconds": runtime["streaming_interval_seconds"],
        "streaming_max_batches": runtime["streaming_max_batches"],
        "next_scheduled_at": _pipeline_next_scheduled_at(p.id),
        "created_at": p.created_at.isoformat() if p.created_at else None,
        "updated_at": p.updated_at.isoformat() if p.updated_at else None,
    }

@app.put("/api/pipelines/{pipeline_id}")
async def update_pipeline(pipeline_id: str, body: PipelineUpdate, db: Session = Depends(get_db)):
    p = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    update_data = body.model_dump(exclude_none=True)
    if (
        "nodes" in update_data
        and "schedule_enabled" not in update_data
        and "schedule_cron" not in update_data
    ):
        derived = _extract_schedule_from_nodes(update_data.get("nodes"))
        update_data.update(derived)

    if "schedule_cron" in update_data:
        try:
            update_data["schedule_cron"] = _ensure_valid_cron_expr(update_data["schedule_cron"])
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    effective_enabled = update_data.get("schedule_enabled", p.schedule_enabled)
    effective_cron = update_data.get("schedule_cron", p.schedule_cron)
    if effective_enabled:
        try:
            update_data["schedule_cron"] = _ensure_valid_cron_expr(effective_cron)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Schedule is enabled but cron is invalid: {exc}",
            )

    for k, v in update_data.items():
        setattr(p, k, v)
    p.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(p)
    _sync_pipeline_schedule_job(p)
    return {"id": p.id, "name": p.name, "status": p.status}

@app.delete("/api/pipelines/{pipeline_id}", status_code=204)
async def delete_pipeline(pipeline_id: str, db: Session = Depends(get_db)):
    p = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    db.delete(p)
    db.commit()
    job_id = _schedule_job_id(pipeline_id)
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

@app.post("/api/pipelines/{pipeline_id}/duplicate")
async def duplicate_pipeline(pipeline_id: str, db: Session = Depends(get_db)):
    p = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    new_pipeline = models.Pipeline(
        id=str(uuid.uuid4()),
        name=f"{p.name} (Copy)",
        description=p.description,
        nodes=p.nodes,
        edges=p.edges,
        tags=p.tags,
        status="draft"
    )
    db.add(new_pipeline)
    db.commit()
    return {"id": new_pipeline.id, "name": new_pipeline.name}


@app.get("/api/pipelines/{pipeline_id}/profile-state")
async def get_pipeline_profile_state(
    pipeline_id: str,
    node_id: Optional[str] = None,
    limit: int = 10,
):
    return _build_pipeline_profile_state_summary(
        pipeline_id=pipeline_id,
        node_id=node_id,
        limit=limit,
    )


@app.delete("/api/pipelines/{pipeline_id}/profile-state")
async def clear_pipeline_profile_state(
    pipeline_id: str,
    node_id: Optional[str] = None,
):
    return _clear_pipeline_profile_state(
        pipeline_id=pipeline_id,
        node_id=node_id,
    )


# ─── MLOPS WORKFLOWS ─────────────────────────────────────────────────────────

def _mlops_topological_order(nodes: list, edges: list) -> list:
    node_ids = [str(n.get("id")) for n in nodes if n.get("id")]
    if not node_ids:
        return []
    adj: Dict[str, List[str]] = {nid: [] for nid in node_ids}
    in_deg: Dict[str, int] = {nid: 0 for nid in node_ids}
    for e in edges or []:
        src = str(e.get("source", ""))
        tgt = str(e.get("target", ""))
        if src in adj and tgt in in_deg:
            adj[src].append(tgt)
            in_deg[tgt] += 1
    queue = [nid for nid, deg in in_deg.items() if deg == 0]
    order: List[str] = []
    while queue:
        nid = queue.pop(0)
        order.append(nid)
        for nb in adj.get(nid, []):
            in_deg[nb] -= 1
            if in_deg[nb] == 0:
                queue.append(nb)
    for nid in node_ids:
        if nid not in order:
            order.append(nid)
    return order


def _mlops_node_label(node: dict) -> str:
    return str(
        node.get("data", {}).get("label")
        or node.get("label")
        or node.get("id")
        or "Step"
    )


def _mlops_node_type(node: dict) -> str:
    return str(node.get("data", {}).get("nodeType") or node.get("type") or "ml_step")


def _mlops_rows_for_node(node_type: str, upstream_rows: int) -> int:
    import random
    ntype = (node_type or "").lower()
    if "trigger" in ntype:
        return 0
    if "source" in ntype or "dataset" in ntype:
        return random.randint(20000, 250000)
    if "staging" in ntype or "quality" in ntype:
        return max(100, int((upstream_rows or random.randint(20000, 120000)) * random.uniform(0.82, 0.97)))
    if "feature" in ntype:
        return max(100, int((upstream_rows or random.randint(15000, 90000)) * random.uniform(0.7, 0.95)))
    if "split" in ntype:
        return max(100, int((upstream_rows or random.randint(15000, 90000)) * 0.8))
    if "training" in ntype or "train" in ntype:
        return max(100, int((upstream_rows or random.randint(15000, 90000)) * random.uniform(0.65, 0.9)))
    if "evaluation" in ntype or "forecast" in ntype:
        return max(100, int((upstream_rows or random.randint(8000, 60000)) * random.uniform(0.9, 1.05)))
    if "deploy" in ntype or "endpoint" in ntype or "registry" in ntype or "batch" in ntype:
        return max(50, int((upstream_rows or random.randint(8000, 60000)) * random.uniform(0.95, 1.05)))
    return max(100, int((upstream_rows or random.randint(8000, 60000)) * random.uniform(0.8, 1.05)))


def _mlops_normalize_sample_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    try:
        if hasattr(value, "isoformat"):
            return value.isoformat()
    except Exception:
        pass
    text = str(value)
    return text[:140] + "…" if len(text) > 140 else text


def _mlops_sanitize_sample_rows(rows: list, limit: Optional[int] = None) -> List[dict]:
    out: List[dict] = []
    selected_rows = rows if limit is None else rows[:max(int(limit), 0)]
    for raw in selected_rows:
        if not isinstance(raw, dict):
            continue
        clean: Dict[str, Any] = {}
        for k, v in raw.items():
            clean[str(k)] = _mlops_normalize_sample_value(v)
        if clean:
            out.append(clean)
    return out


def _mlops_feature_operation_catalog() -> List[dict]:
    return [
        {"value": "impute_mean", "label": "Impute Mean", "category": "missing", "requires_column": True},
        {"value": "impute_median", "label": "Impute Median", "category": "missing", "requires_column": True},
        {"value": "impute_mode", "label": "Impute Mode", "category": "missing", "requires_column": True},
        {"value": "impute_constant", "label": "Impute Constant", "category": "missing", "requires_column": True, "params": ["constant"]},
        {"value": "drop_null_rows", "label": "Drop Rows With Null", "category": "missing", "requires_column": True},
        {"value": "drop_column", "label": "Drop Column", "category": "column", "requires_column": True},
        {"value": "to_numeric", "label": "Cast To Numeric", "category": "cast", "requires_column": True},
        {"value": "to_datetime", "label": "Cast To DateTime", "category": "cast", "requires_column": True},
        {"value": "one_hot_encode", "label": "One-Hot Encode", "category": "encoding", "requires_column": True, "params": ["top_k", "drop_original"]},
        {"value": "label_encode", "label": "Label Encode", "category": "encoding", "requires_column": True},
        {"value": "frequency_encode", "label": "Frequency Encode", "category": "encoding", "requires_column": True},
        {"value": "scale_standard", "label": "Standard Scale", "category": "scaling", "requires_column": True},
        {"value": "scale_minmax", "label": "MinMax Scale", "category": "scaling", "requires_column": True},
        {"value": "scale_robust", "label": "Robust Scale (Median/IQR)", "category": "scaling", "requires_column": True},
        {"value": "log1p", "label": "Log1p Transform", "category": "numeric_transform", "requires_column": True},
        {"value": "sqrt", "label": "Square Root Transform", "category": "numeric_transform", "requires_column": True},
        {"value": "square", "label": "Square Transform", "category": "numeric_transform", "requires_column": True},
        {"value": "cube", "label": "Cube Transform", "category": "numeric_transform", "requires_column": True},
        {"value": "abs", "label": "Absolute Value", "category": "numeric_transform", "requires_column": True},
        {"value": "clip", "label": "Clip Values", "category": "numeric_transform", "requires_column": True, "params": ["min", "max"]},
        {"value": "winsorize", "label": "Winsorize", "category": "numeric_transform", "requires_column": True, "params": ["lower_pct", "upper_pct"]},
        {"value": "bin_equal_width", "label": "Equal Width Binning", "category": "binning", "requires_column": True, "params": ["bins"]},
        {"value": "bin_quantile", "label": "Quantile Binning", "category": "binning", "requires_column": True, "params": ["bins"]},
        {"value": "datetime_parts", "label": "Extract Date Parts", "category": "datetime", "requires_column": True, "params": ["parts"]},
        {"value": "cyclical_encode", "label": "Cyclical Encode Date/Time", "category": "datetime", "requires_column": True, "params": ["period"]},
        {"value": "text_length", "label": "Text Length", "category": "text", "requires_column": True},
        {"value": "text_word_count", "label": "Text Word Count", "category": "text", "requires_column": True},
        {"value": "text_lower", "label": "Text Lowercase", "category": "text", "requires_column": True},
        {"value": "interaction_multiply", "label": "Interaction Multiply", "category": "interaction", "requires_column": False, "params": ["left", "right", "output"]},
        {"value": "interaction_add", "label": "Interaction Add", "category": "interaction", "requires_column": False, "params": ["left", "right", "output"]},
        {"value": "interaction_subtract", "label": "Interaction Subtract", "category": "interaction", "requires_column": False, "params": ["left", "right", "output"]},
        {"value": "interaction_divide", "label": "Interaction Divide", "category": "interaction", "requires_column": False, "params": ["left", "right", "output"]},
    ]


def _mlops_infer_column_profile(rows: List[dict], sample_size: int = 2000) -> Dict[str, Any]:
    import math
    import pandas as pd

    tabular_rows = [r for r in rows if isinstance(r, dict)]
    if not tabular_rows:
        return {
            "row_count": 0,
            "sample_size": 0,
            "column_count": 0,
            "duplicate_rows": 0,
            "columns": [],
            "recommendations": [],
        }

    sample_rows = tabular_rows[:max(50, min(sample_size, len(tabular_rows)))]
    df = pd.DataFrame(sample_rows)
    cols: List[dict] = []
    recommendations: List[dict] = []
    duplicate_rows = int(df.duplicated().sum()) if not df.empty else 0

    def _num(v: Any) -> Optional[float]:
        try:
            if v is None:
                return None
            val = float(v)
            if math.isfinite(val):
                return val
        except Exception:
            return None
        return None

    for col in df.columns:
        series = df[col]
        non_null = series.dropna()
        total = max(int(len(series)), 1)
        missing_count = int(series.isna().sum())
        missing_pct = float(missing_count) / total * 100.0
        non_null_count = int(total - missing_count)
        unique_count = int(non_null.astype(str).nunique()) if len(non_null) else 0
        unique_ratio = float(unique_count / max(non_null_count, 1))

        num_series = pd.to_numeric(non_null, errors="coerce")
        numeric_ratio = float(num_series.notna().sum() / max(len(non_null), 1))

        dt_series = pd.to_datetime(non_null, errors="coerce", utc=True)
        datetime_ratio = float(dt_series.notna().sum() / max(len(non_null), 1))

        dtype = "text"
        if numeric_ratio >= 0.85:
            dtype = "numeric"
        elif datetime_ratio >= 0.85:
            dtype = "datetime"
        elif unique_count <= 40 or unique_ratio <= 0.35:
            dtype = "categorical"

        sample_values = [str(v)[:120] for v in non_null.head(4).tolist()]
        stats: Dict[str, Any] = {}
        quality_flags: List[str] = []
        suggested_ops: List[str] = []

        if missing_pct >= 35.0:
            quality_flags.append("mostly_missing")
        if unique_count <= 1 and non_null_count > 0:
            quality_flags.append("constant_value")
        if unique_ratio >= 0.98 and unique_count > 20 and dtype in {"categorical", "text"}:
            quality_flags.append("likely_identifier")
        if dtype in {"categorical", "text"} and unique_count > 100:
            quality_flags.append("high_cardinality")

        if dtype == "numeric":
            valid = pd.to_numeric(series, errors="coerce").dropna()
            if not valid.empty:
                p01 = float(valid.quantile(0.01))
                p05 = float(valid.quantile(0.05))
                p25 = float(valid.quantile(0.25))
                p50 = float(valid.quantile(0.50))
                p75 = float(valid.quantile(0.75))
                p95 = float(valid.quantile(0.95))
                p99 = float(valid.quantile(0.99))
                iqr = p75 - p25
                low = p25 - 1.5 * iqr
                high = p75 + 1.5 * iqr
                outlier_pct = float(((valid < low) | (valid > high)).mean()) * 100.0 if iqr > 0 else 0.0
                zero_pct = float((valid == 0).mean()) * 100.0
                negative_pct = float((valid < 0).mean()) * 100.0
                skew = float(valid.skew()) if len(valid) > 2 else 0.0
                kurtosis = float(valid.kurtosis()) if len(valid) > 3 else 0.0

                stats = {
                    "min": round(float(valid.min()), 6),
                    "max": round(float(valid.max()), 6),
                    "mean": round(float(valid.mean()), 6),
                    "median": round(p50, 6),
                    "std": round(float(valid.std(ddof=0)), 6),
                    "p01": round(p01, 6),
                    "p05": round(p05, 6),
                    "p25": round(p25, 6),
                    "p75": round(p75, 6),
                    "p95": round(p95, 6),
                    "p99": round(p99, 6),
                    "skew": round(skew, 6),
                    "kurtosis": round(kurtosis, 6),
                    "outlier_pct": round(outlier_pct, 4),
                    "zero_pct": round(zero_pct, 4),
                    "negative_pct": round(negative_pct, 4),
                }

                suggested_ops.extend(["impute_median", "scale_robust"])
                if abs(skew) >= 1.0:
                    suggested_ops.extend(["log1p"])
                if outlier_pct >= 3.0:
                    suggested_ops.extend(["winsorize"])
                if unique_count >= 30:
                    suggested_ops.extend(["bin_quantile"])
                if iqr == 0:
                    quality_flags.append("low_variance")
            else:
                quality_flags.append("non_numeric_values")

        elif dtype == "categorical":
            series_str = series.astype(str).fillna("")
            top_values = series_str.value_counts(dropna=False).head(5)
            stats = {
                "top_values": [
                    {"value": str(k), "count": int(v)}
                    for k, v in top_values.items()
                ],
                "mode": str(series_str.mode().iloc[0]) if not series_str.mode().empty else "",
            }
            suggested_ops.extend(["impute_mode"])
            if unique_count <= 20:
                suggested_ops.append("one_hot_encode")
            else:
                suggested_ops.append("frequency_encode")
            if unique_count > 200:
                suggested_ops.append("label_encode")

        elif dtype == "datetime":
            dt_full = pd.to_datetime(series, errors="coerce", utc=True)
            dt_valid = dt_full.dropna()
            if not dt_valid.empty:
                min_dt = dt_valid.min()
                max_dt = dt_valid.max()
                span_days = (max_dt - min_dt).days if max_dt is not None and min_dt is not None else 0
                stats = {
                    "min": min_dt.isoformat() if hasattr(min_dt, "isoformat") else str(min_dt),
                    "max": max_dt.isoformat() if hasattr(max_dt, "isoformat") else str(max_dt),
                    "span_days": int(span_days),
                }
                if span_days <= 1:
                    quality_flags.append("narrow_time_span")
            suggested_ops.extend(["datetime_parts", "cyclical_encode"])

        else:
            series_str = series.astype(str).fillna("")
            lengths = series_str.map(len)
            word_counts = series_str.map(lambda t: len([w for w in t.strip().split() if w]))
            stats = {
                "avg_length": round(float(lengths.mean()), 4) if len(lengths) else 0.0,
                "median_length": round(float(lengths.median()), 4) if len(lengths) else 0.0,
                "max_length": int(lengths.max()) if len(lengths) else 0,
                "avg_word_count": round(float(word_counts.mean()), 4) if len(word_counts) else 0.0,
            }
            suggested_ops.extend(["text_lower", "text_length", "text_word_count"])

        if missing_pct >= 5.0 and "impute_median" not in suggested_ops and "impute_mode" not in suggested_ops:
            suggested_ops.insert(0, "impute_mode" if dtype in {"categorical", "text"} else "impute_median")
        if "likely_identifier" in quality_flags and "drop_column" not in suggested_ops:
            suggested_ops.append("drop_column")

        # Stable uniqueness signal for downstream UI behavior.
        uniqueness_score = _num(unique_ratio * 100.0) or 0.0

        col_obj = {
            "name": str(col),
            "type": dtype,
            "missing_count": missing_count,
            "missing_pct": round(missing_pct, 4),
            "non_null_count": non_null_count,
            "unique_count": unique_count,
            "unique_ratio": round(unique_ratio, 4),
            "uniqueness_score": round(uniqueness_score, 4),
            "sample_values": sample_values,
            "stats": stats,
            "quality_flags": sorted(list(dict.fromkeys(quality_flags))),
            "suggested_operations": list(dict.fromkeys(suggested_ops))[:10],
        }
        cols.append(col_obj)
        recommendations.append({
            "column": str(col),
            "type": dtype,
            "operations": col_obj["suggested_operations"][:6],
            "quality_flags": col_obj["quality_flags"],
        })

    return {
        "row_count": len(tabular_rows),
        "sample_size": len(sample_rows),
        "column_count": len(df.columns),
        "duplicate_rows": duplicate_rows,
        "columns": cols,
        "recommendations": recommendations,
    }


def _mlops_parse_feature_operations(config: Dict[str, Any], rows: List[dict]) -> List[dict]:
    ops = config.get("feature_operations")
    if isinstance(ops, list):
        parsed = [op for op in ops if isinstance(op, dict) and str(op.get("operation") or "").strip()]
        if parsed:
            return parsed

    inferred_rows = [r for r in rows if isinstance(r, dict)]
    if not inferred_rows:
        return []
    first = inferred_rows[0]
    columns = list(first.keys())

    out: List[dict] = []
    categorical_strategy = str(config.get("categorical_strategy") or "").strip().lower()
    scaler = str(config.get("scaler") or "").strip().lower()

    if categorical_strategy in {"one_hot", "target", "label"}:
        for col in columns:
            values = [str((row or {}).get(col) or "") for row in inferred_rows[:300]]
            numeric_like = sum(_mlops_to_float(v) is not None for v in values) / max(len(values), 1) >= 0.85
            if numeric_like:
                continue
            mapped = {
                "one_hot": "one_hot_encode",
                "target": "frequency_encode",
                "label": "label_encode",
            }.get(categorical_strategy, "label_encode")
            out.append({"column": col, "operation": mapped, "params": {"top_k": 15}})

    if scaler in {"standard", "minmax"}:
        for col in columns:
            values = [(row or {}).get(col) for row in inferred_rows[:300]]
            numeric_like = sum(_mlops_to_float(v) is not None for v in values) / max(len(values), 1) >= 0.85
            if not numeric_like:
                continue
            mapped = "scale_standard" if scaler == "standard" else "scale_minmax"
            out.append({"column": col, "operation": mapped})

    return out


def _mlops_apply_staging_transform(rows: List[dict], config: Dict[str, Any]) -> Dict[str, Any]:
    tabular_rows = [dict(r) for r in rows if isinstance(r, dict)]
    if not tabular_rows:
        return {"rows": [], "summary": {"input_rows": 0, "output_rows": 0, "deduplicated": 0, "dropped_null_rows": 0}}

    deduplicate = bool(config.get("deduplicate", True))
    threshold_raw = config.get("drop_null_threshold", 30)
    try:
        null_threshold = max(0.0, min(float(threshold_raw), 100.0))
    except Exception:
        null_threshold = 30.0

    dedup_removed = 0
    if deduplicate:
        seen = set()
        deduped: List[dict] = []
        for row in tabular_rows:
            key = tuple(sorted((str(k), json.dumps(v, sort_keys=True, default=str)) for k, v in row.items()))
            if key in seen:
                dedup_removed += 1
                continue
            seen.add(key)
            deduped.append(row)
        tabular_rows = deduped

    dropped_null_rows = 0
    if null_threshold < 100.0:
        kept: List[dict] = []
        for row in tabular_rows:
            values = list(row.values())
            if not values:
                dropped_null_rows += 1
                continue
            nulls = sum(v is None or (isinstance(v, str) and not v.strip()) for v in values)
            ratio = (nulls / max(len(values), 1)) * 100.0
            if ratio > null_threshold:
                dropped_null_rows += 1
                continue
            kept.append(row)
        tabular_rows = kept

    return {
        "rows": tabular_rows,
        "summary": {
            "input_rows": len(rows),
            "output_rows": len(tabular_rows),
            "deduplicated": dedup_removed,
            "dropped_null_rows": dropped_null_rows,
        },
    }


def _mlops_apply_feature_engineering_rows(rows: List[dict], config: Dict[str, Any]) -> Dict[str, Any]:
    import math
    import numpy as np
    import pandas as pd

    tabular_rows = [dict(r) for r in rows if isinstance(r, dict)]
    if not tabular_rows:
        return {"rows": [], "summary": {"operations_applied": 0, "input_columns": 0, "output_columns": 0, "generated_features": [], "errors": []}}

    df = pd.DataFrame(tabular_rows)
    original_columns = list(df.columns)
    operations = _mlops_parse_feature_operations(config or {}, tabular_rows)
    generated_features: List[str] = []
    dropped_features: List[str] = []
    errors: List[str] = []
    applied_ops = 0

    def _safe_numeric(series: Any) -> Any:
        return pd.to_numeric(series, errors="coerce")

    for op in operations:
        op_name = str(op.get("operation") or "").strip().lower()
        col = str(op.get("column") or "").strip()
        params = op.get("params") if isinstance(op.get("params"), dict) else {}
        if not op_name:
            continue

        try:
            if op_name == "drop_column":
                if col and col in df.columns:
                    df = df.drop(columns=[col])
                    dropped_features.append(col)
                    applied_ops += 1
                continue

            if op_name.startswith("interaction_"):
                left = str(params.get("left") or "").strip()
                right = str(params.get("right") or "").strip()
                out = str(params.get("output") or f"{left}_{op_name.replace('interaction_', '')}_{right}").strip("_")
                if not left or not right or left not in df.columns or right not in df.columns:
                    errors.append(f"{op_name}: invalid left/right columns")
                    continue
                l = _safe_numeric(df[left]).fillna(0.0)
                r = _safe_numeric(df[right]).fillna(0.0)
                if op_name == "interaction_multiply":
                    df[out] = l * r
                elif op_name == "interaction_add":
                    df[out] = l + r
                elif op_name == "interaction_subtract":
                    df[out] = l - r
                elif op_name == "interaction_divide":
                    df[out] = l / r.replace({0.0: np.nan})
                generated_features.append(out)
                applied_ops += 1
                continue

            if col and col not in df.columns:
                errors.append(f"{op_name}: column '{col}' not found")
                continue
            if not col:
                errors.append(f"{op_name}: column is required")
                continue

            if op_name in {"to_numeric"}:
                df[col] = _safe_numeric(df[col])
                applied_ops += 1
                continue
            if op_name in {"to_datetime"}:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
                applied_ops += 1
                continue

            if op_name == "impute_mean":
                s = _safe_numeric(df[col])
                df[col] = s.fillna(s.mean())
                applied_ops += 1
                continue
            if op_name == "impute_median":
                s = _safe_numeric(df[col])
                df[col] = s.fillna(s.median())
                applied_ops += 1
                continue
            if op_name == "impute_mode":
                mode = df[col].mode(dropna=True)
                fill = mode.iloc[0] if len(mode) > 0 else ""
                df[col] = df[col].fillna(fill)
                applied_ops += 1
                continue
            if op_name == "impute_constant":
                fill = params.get("constant", 0)
                df[col] = df[col].fillna(fill)
                applied_ops += 1
                continue
            if op_name == "drop_null_rows":
                before = len(df)
                df = df[df[col].notna()].copy()
                if before != len(df):
                    applied_ops += 1
                continue

            if op_name == "one_hot_encode":
                top_k = int(params.get("top_k", 20) or 20)
                drop_original = bool(params.get("drop_original", False))
                series = df[col].astype(str).fillna("")
                top_values = list(series.value_counts(dropna=False).head(max(1, min(top_k, 200))).index)
                for v in top_values:
                    feature_name = f"{col}__{str(v).replace(' ', '_')[:40]}"
                    df[feature_name] = (series == v).astype(int)
                    generated_features.append(feature_name)
                if drop_original:
                    df = df.drop(columns=[col])
                    dropped_features.append(col)
                applied_ops += 1
                continue

            if op_name == "label_encode":
                series = df[col].astype(str).fillna("")
                uniques = sorted(series.unique().tolist())
                mapping = {tok: idx for idx, tok in enumerate(uniques)}
                feature_name = f"{col}__label"
                df[feature_name] = series.map(mapping).astype(float)
                generated_features.append(feature_name)
                applied_ops += 1
                continue

            if op_name == "frequency_encode":
                series = df[col].astype(str).fillna("")
                freq = series.value_counts(dropna=False)
                total = max(int(len(series)), 1)
                feature_name = f"{col}__freq"
                df[feature_name] = series.map(lambda x: float(freq.get(x, 0) / total))
                generated_features.append(feature_name)
                applied_ops += 1
                continue

            if op_name in {"scale_standard", "scale_minmax", "scale_robust"}:
                s = _safe_numeric(df[col])
                feature_name = f"{col}__scaled"
                if op_name == "scale_standard":
                    mean = float(s.mean()) if s.notna().any() else 0.0
                    std = float(s.std(ddof=0)) if s.notna().any() else 1.0
                    std = std if abs(std) > 1e-9 else 1.0
                    df[feature_name] = (s - mean) / std
                elif op_name == "scale_minmax":
                    min_v = float(s.min()) if s.notna().any() else 0.0
                    max_v = float(s.max()) if s.notna().any() else 1.0
                    denom = (max_v - min_v) if abs(max_v - min_v) > 1e-9 else 1.0
                    df[feature_name] = (s - min_v) / denom
                else:
                    median = float(s.median()) if s.notna().any() else 0.0
                    q1 = float(s.quantile(0.25)) if s.notna().any() else 0.0
                    q3 = float(s.quantile(0.75)) if s.notna().any() else 1.0
                    iqr = (q3 - q1) if abs(q3 - q1) > 1e-9 else 1.0
                    df[feature_name] = (s - median) / iqr
                generated_features.append(feature_name)
                applied_ops += 1
                continue

            if op_name in {"log1p", "sqrt", "square", "cube", "abs", "clip", "winsorize", "bin_equal_width", "bin_quantile"}:
                s = _safe_numeric(df[col])
                if op_name == "log1p":
                    feature_name = f"{col}__log1p"
                    df[feature_name] = np.log1p(np.clip(s, a_min=0.0, a_max=None))
                elif op_name == "sqrt":
                    feature_name = f"{col}__sqrt"
                    df[feature_name] = np.sqrt(np.clip(s, a_min=0.0, a_max=None))
                elif op_name == "square":
                    feature_name = f"{col}__square"
                    df[feature_name] = np.square(s)
                elif op_name == "cube":
                    feature_name = f"{col}__cube"
                    df[feature_name] = np.power(s, 3)
                elif op_name == "abs":
                    feature_name = f"{col}__abs"
                    df[feature_name] = np.abs(s)
                elif op_name == "clip":
                    min_v = _mlops_to_float(params.get("min"))
                    max_v = _mlops_to_float(params.get("max"))
                    feature_name = f"{col}__clip"
                    df[feature_name] = s.clip(lower=min_v, upper=max_v)
                elif op_name == "winsorize":
                    lp = float(params.get("lower_pct", 0.01) or 0.01)
                    up = float(params.get("upper_pct", 0.99) or 0.99)
                    lp = max(0.0, min(lp, 0.49))
                    up = max(0.51, min(up, 1.0))
                    low = s.quantile(lp)
                    high = s.quantile(up)
                    feature_name = f"{col}__winsor"
                    df[feature_name] = s.clip(lower=low, upper=high)
                elif op_name == "bin_equal_width":
                    bins = int(params.get("bins", 5) or 5)
                    bins = max(2, min(bins, 100))
                    feature_name = f"{col}__bin"
                    df[feature_name] = pd.cut(s, bins=bins, labels=False, duplicates="drop")
                else:
                    bins = int(params.get("bins", 5) or 5)
                    bins = max(2, min(bins, 100))
                    feature_name = f"{col}__qbin"
                    ranked = s.rank(method="first")
                    df[feature_name] = pd.qcut(ranked, q=min(bins, max(2, ranked.nunique())), labels=False, duplicates="drop")
                generated_features.append(feature_name)
                applied_ops += 1
                continue

            if op_name == "datetime_parts":
                parts = params.get("parts")
                if not isinstance(parts, list) or not parts:
                    parts = ["year", "month", "day", "dayofweek", "quarter", "is_weekend"]
                dt = pd.to_datetime(df[col], errors="coerce", utc=True)
                for part in parts:
                    part_name = str(part).strip().lower()
                    fname = f"{col}__{part_name}"
                    if part_name == "year":
                        df[fname] = dt.dt.year
                    elif part_name == "month":
                        df[fname] = dt.dt.month
                    elif part_name == "day":
                        df[fname] = dt.dt.day
                    elif part_name == "hour":
                        df[fname] = dt.dt.hour
                    elif part_name == "dayofweek":
                        df[fname] = dt.dt.dayofweek
                    elif part_name == "weekofyear":
                        df[fname] = dt.dt.isocalendar().week.astype(float)
                    elif part_name == "quarter":
                        df[fname] = dt.dt.quarter
                    elif part_name == "is_weekend":
                        df[fname] = dt.dt.dayofweek.isin([5, 6]).astype(float)
                    else:
                        continue
                    generated_features.append(fname)
                applied_ops += 1
                continue

            if op_name == "cyclical_encode":
                period = str(params.get("period") or "month").strip().lower()
                dt = pd.to_datetime(df[col], errors="coerce", utc=True)
                if period == "dayofweek":
                    value = dt.dt.dayofweek
                    max_period = 7.0
                elif period == "hour":
                    value = dt.dt.hour
                    max_period = 24.0
                else:
                    value = dt.dt.month
                    max_period = 12.0
                rad = 2.0 * math.pi * (value / max_period)
                sin_col = f"{col}__{period}_sin"
                cos_col = f"{col}__{period}_cos"
                df[sin_col] = np.sin(rad)
                df[cos_col] = np.cos(rad)
                generated_features.extend([sin_col, cos_col])
                applied_ops += 1
                continue

            if op_name in {"text_length", "text_word_count", "text_lower"}:
                series = df[col].astype(str).fillna("")
                if op_name == "text_length":
                    feature_name = f"{col}__len"
                    df[feature_name] = series.str.len()
                    generated_features.append(feature_name)
                elif op_name == "text_word_count":
                    feature_name = f"{col}__word_count"
                    df[feature_name] = series.map(lambda t: len([w for w in t.strip().split() if w]))
                    generated_features.append(feature_name)
                else:
                    feature_name = f"{col}__lower"
                    df[feature_name] = series.str.lower()
                    generated_features.append(feature_name)
                applied_ops += 1
                continue

            errors.append(f"{op_name}: unsupported operation")
        except Exception as exc:
            errors.append(f"{op_name}: {exc}")

    # Final NA handling for generated numeric features to keep downstream model stable.
    for col in df.columns:
        if col in original_columns:
            continue
        if str(df[col].dtype).lower().startswith(("float", "int")):
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    rows_out = df.replace({np.nan: None}).to_dict(orient="records")
    summary = {
        "operations_applied": applied_ops,
        "input_columns": len(original_columns),
        "output_columns": len(df.columns),
        "generated_features": sorted(list(dict.fromkeys(generated_features))),
        "dropped_features": sorted(list(dict.fromkeys(dropped_features))),
        "errors": errors[:20],
    }
    return {"rows": rows_out, "summary": summary}


def _mlops_source_rows_from_node_config(db: Session, node: dict) -> Dict[str, Any]:
    data = node.get("data", {}) if isinstance(node, dict) else {}
    config = data.get("config", {}) if isinstance(data, dict) else {}
    ntype = _mlops_node_type(node).lower()

    if "ml_pipeline_output_source" in ntype:
        return _mlops_source_rows(
            db=db,
            source_type="pipeline",
            pipeline_id=str(config.get("pipeline_id") or "").strip(),
            pipeline_node_id=str(config.get("pipeline_node_id") or config.get("node_id") or "").strip() or None,
        )

    source_type = str(config.get("source_type") or "sample").strip().lower()
    if source_type == "file":
        return _mlops_source_rows(
            db=db,
            source_type="file",
            file_path=str(config.get("file_path") or "").strip(),
            dataset=str(config.get("dataset") or "sales"),
        )
    if source_type == "pipeline":
        return _mlops_source_rows(
            db=db,
            source_type="pipeline",
            pipeline_id=str(config.get("pipeline_id") or "").strip(),
            pipeline_node_id=str(config.get("pipeline_node_id") or "").strip() or None,
            dataset=str(config.get("dataset") or "sales"),
        )
    return _mlops_source_rows(
        db=db,
        source_type="sample",
        dataset=str(config.get("dataset") or "sales"),
    )


def _mlops_preview_workflow_samples(
    workflow: models.MLOpsWorkflow,
    db: Session,
    sample_size: int = 3000,
) -> Dict[str, Dict[str, List[dict]]]:
    nodes = workflow.nodes or []
    edges = workflow.edges or []
    order = _mlops_topological_order(nodes, edges)
    nodes_by_id = {str(n.get("id")): n for n in nodes if isinstance(n, dict) and n.get("id")}
    edge_by_target: Dict[str, List[str]] = {}
    for e in edges:
        src = str(e.get("source") or "")
        tgt = str(e.get("target") or "")
        if src and tgt:
            edge_by_target.setdefault(tgt, []).append(src)

    in_rows: Dict[str, List[dict]] = {}
    out_rows: Dict[str, List[dict]] = {}

    for nid in order:
        node = nodes_by_id.get(nid)
        if not node:
            continue
        ntype = _mlops_node_type(node).lower()
        data = node.get("data", {}) if isinstance(node, dict) else {}
        config = data.get("config", {}) if isinstance(data, dict) else {}
        upstream = edge_by_target.get(nid, [])
        input_rows: List[dict] = []
        for uid in upstream:
            candidate = out_rows.get(uid) or []
            if candidate:
                input_rows = candidate
                break
        in_rows[nid] = input_rows[:sample_size]

        try:
            if "source" in ntype or "dataset" in ntype:
                src = _mlops_source_rows_from_node_config(db, node)
                output_rows = [r for r in src.get("rows", []) if isinstance(r, dict)]
            elif "staging" in ntype or "quality" in ntype:
                output_rows = _mlops_apply_staging_transform(input_rows, config).get("rows", [])
            elif "feature" in ntype:
                output_rows = _mlops_apply_feature_engineering_rows(input_rows, config).get("rows", [])
            elif "split" in ntype:
                output_rows = []
                test_size = config.get("test_size", 20)
                try:
                    ratio = float(test_size) / 100.0 if float(test_size) > 1 else float(test_size)
                except Exception:
                    ratio = 0.2
                ratio = max(0.05, min(ratio, 0.5))
                for idx, row in enumerate(input_rows):
                    next_row = dict(row)
                    next_row["split"] = "test" if (idx % max(int(round(1.0 / ratio)), 2) == 0) else "train"
                    output_rows.append(next_row)
            else:
                output_rows = list(input_rows)
        except Exception:
            output_rows = list(input_rows)

        out_rows[nid] = [r for r in output_rows[:sample_size] if isinstance(r, dict)]

    return {"input_rows_by_node": in_rows, "output_rows_by_node": out_rows}


def _mlops_apply_preprocessing_nodes(
    workflow: models.MLOpsWorkflow,
    rows: List[dict],
) -> List[dict]:
    nodes = workflow.nodes or []
    edges = workflow.edges or []
    order = _mlops_topological_order(nodes, edges)
    nodes_by_id = {str(n.get("id")): n for n in nodes if isinstance(n, dict) and n.get("id")}
    edge_by_target: Dict[str, List[str]] = {}
    for e in edges:
        src = str(e.get("source") or "")
        tgt = str(e.get("target") or "")
        if src and tgt:
            edge_by_target.setdefault(tgt, []).append(src)

    out_rows: Dict[str, List[dict]] = {}
    source_assigned = False
    for nid in order:
        node = nodes_by_id.get(nid)
        if not node:
            continue
        ntype = _mlops_node_type(node).lower()
        data = node.get("data", {}) if isinstance(node, dict) else {}
        config = data.get("config", {}) if isinstance(data, dict) else {}

        upstream = edge_by_target.get(nid, [])
        input_rows: List[dict] = []
        for uid in upstream:
            candidate = out_rows.get(uid) or []
            if candidate:
                input_rows = candidate
                break

        if ("source" in ntype or "dataset" in ntype) and not source_assigned:
            out_rows[nid] = [r for r in rows if isinstance(r, dict)]
            source_assigned = True
            continue

        if not input_rows:
            continue
        if "staging" in ntype or "quality" in ntype:
            out_rows[nid] = _mlops_apply_staging_transform(input_rows, config).get("rows", input_rows)
        elif "feature" in ntype:
            out_rows[nid] = _mlops_apply_feature_engineering_rows(input_rows, config).get("rows", input_rows)
        elif "split" in ntype:
            out_rows[nid] = list(input_rows)
        elif "training" in ntype or "train" in ntype or "evaluation" in ntype or "forecast" in ntype:
            out_rows[nid] = list(input_rows)

    # Prefer the latest preprocessing output if available.
    for nid in reversed(order):
        node = nodes_by_id.get(nid)
        if not node:
            continue
        ntype = _mlops_node_type(node).lower()
        if any(key in ntype for key in ["feature", "staging", "quality", "split"]):
            rows_out = out_rows.get(nid)
            if rows_out:
                return rows_out
    return [r for r in rows if isinstance(r, dict)]


def _mlops_synthetic_source_sample(rows: int) -> List[dict]:
    import random
    categories = ["A", "B", "C", "D"]
    base = max(100, rows // 300 if rows else 100)
    sample_size = max(30, min(rows if rows else 30, 1000))
    sample = []
    for i in range(sample_size):
        sample.append({
            "record_id": f"R-{1000 + i}",
            "event_date": f"2026-03-{((10 + i - 1) % 28) + 1:02d}",
            "region": ["North", "South", "East", "West"][i % 4],
            "segment": categories[i % len(categories)],
            "amount": round(random.uniform(25.0, 420.0), 2),
            "target": int((base + i) % 2),
        })
    return sample


def _mlops_pipeline_source_preview(
    db: Session,
    pipeline_id: str,
    pipeline_node_id: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    if not pipeline_id:
        return None

    exec_row = db.query(models.Execution).filter(
        models.Execution.pipeline_id == pipeline_id,
        models.Execution.status == "success"
    ).order_by(models.Execution.started_at.desc()).first()
    if not exec_row or not exec_row.node_results:
        return None

    rows = _extract_rows_from_pipeline_execution(exec_row.node_results, pipeline_node_id)
    if not rows:
        return None

    pipeline = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
    return {
        "row_count": len(rows),
        "sample_rows": _mlops_sanitize_sample_rows(rows),
        "pipeline_name": pipeline.name if pipeline else pipeline_id,
    }


def _mlops_build_output_sample(
    node_type: str,
    label: str,
    input_sample: List[dict],
    rows: int,
    metrics: dict,
    model_version: str,
    node_config: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    import random
    ntype = (node_type or "").lower()

    if "training" in ntype or "train" in ntype:
        return [{
            "model_name": label,
            "algorithm": "xgboost",
            "train_rows": rows,
            "accuracy": metrics.get("accuracy"),
            "precision": metrics.get("precision"),
            "recall": metrics.get("recall"),
            "model_version": model_version,
        }]

    if "evaluation" in ntype:
        return [{
            "rmse": metrics.get("rmse"),
            "mape": metrics.get("mape"),
            "f1_score": metrics.get("f1_score"),
            "evaluated_rows": rows,
        }]

    if "forecast" in ntype:
        horizon = int(metrics.get("forecast_horizon_days", 30))
        horizon = max(1, min(horizon, 3650))
        return [
            {
                "forecast_day": i + 1,
                "forecast_date": (date.today() + timedelta(days=i + 1)).isoformat(),
                "prediction": round(random.uniform(80, 600), 2),
                "lower_bound": round(random.uniform(50, 520), 2),
                "upper_bound": round(random.uniform(120, 690), 2),
            }
            for i in range(horizon)
        ]

    if "deploy" in ntype or "endpoint" in ntype or "registry" in ntype:
        return [{
            "model_version": model_version,
            "endpoint": metrics.get("deployment_endpoint"),
            "status": "deployed",
            "deployed_at": datetime.utcnow().isoformat(),
        }]

    if "feature" in ntype:
        base = input_sample if input_sample else _mlops_synthetic_source_sample(rows)
        transformed = _mlops_apply_feature_engineering_rows(base, node_config or {})
        summary = transformed.get("summary", {}) if isinstance(transformed, dict) else {}
        metrics["feature_count"] = int(summary.get("output_columns") or metrics.get("feature_count") or 0)
        metrics["feature_operations_applied"] = int(summary.get("operations_applied") or 0)
        errors = summary.get("errors") if isinstance(summary, dict) else None
        if isinstance(errors, list) and errors:
            metrics["feature_engineering_warnings"] = errors[:5]
        return _mlops_sanitize_sample_rows(transformed.get("rows", []))

    if "split" in ntype:
        base = input_sample if input_sample else _mlops_synthetic_source_sample(rows)
        return _mlops_sanitize_sample_rows([
            {**dict(row), "split": "train" if idx % 5 else "test"}
            for idx, row in enumerate(base)
        ])

    if "staging" in ntype or "quality" in ntype:
        base = input_sample if input_sample else _mlops_synthetic_source_sample(rows)
        staged = _mlops_apply_staging_transform(base, node_config or {})
        summary = staged.get("summary", {}) if isinstance(staged, dict) else {}
        metrics["staging_deduplicated"] = int(summary.get("deduplicated") or 0)
        metrics["staging_dropped_null_rows"] = int(summary.get("dropped_null_rows") or 0)
        return _mlops_sanitize_sample_rows(staged.get("rows", []))

    if input_sample:
        return _mlops_sanitize_sample_rows(input_sample)

    return _mlops_synthetic_source_sample(rows)


def _mlops_source_rows(
    db: Session,
    source_type: str,
    pipeline_id: Optional[str] = None,
    pipeline_node_id: Optional[str] = None,
    file_path: Optional[str] = None,
    dataset: Optional[str] = "sales",
) -> Dict[str, Any]:
    st = (source_type or "sample").strip().lower()

    if st == "pipeline":
        pid = (pipeline_id or "").strip()
        if not pid:
            raise HTTPException(400, "pipeline_id is required for source_type=pipeline")
        pipeline = db.query(models.Pipeline).filter(models.Pipeline.id == pid).first()
        if not pipeline:
            raise HTTPException(404, "Pipeline not found")
        exec_row = db.query(models.Execution).filter(
            models.Execution.pipeline_id == pid,
            models.Execution.status == "success"
        ).order_by(models.Execution.started_at.desc()).first()
        if not exec_row or not exec_row.node_results:
            raise HTTPException(400, "No successful execution with tabular output found")
        rows = _extract_rows_from_pipeline_execution(exec_row.node_results, pipeline_node_id)
        if not rows:
            raise HTTPException(400, "No tabular rows found in the selected ETL pipeline output")
        return {
            "rows": rows,
            "source": {
                "type": "pipeline",
                "pipeline_id": pid,
                "pipeline_name": pipeline.name,
                "pipeline_node_id": pipeline_node_id,
                "row_count": len(rows),
            },
        }

    if st == "file":
        path = (file_path or "").strip()
        if not path:
            raise HTTPException(400, "file_path is required for source_type=file")
        rows = _read_tabular_output_file(path)
        if not rows:
            raise HTTPException(400, f"No tabular rows could be read from file: {path}")
        return {
            "rows": rows,
            "source": {
                "type": "file",
                "file_path": path,
                "row_count": len(rows),
            },
        }

    ds = (dataset or "sales").strip() or "sales"
    rows = _get_sample_data(ds)
    if not rows:
        raise HTTPException(400, f"No sample rows available for dataset: {ds}")
    return {
        "rows": rows,
        "source": {
            "type": "sample",
            "dataset": ds,
            "row_count": len(rows),
        },
    }


def _mlops_to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text.replace(",", ""))
        except Exception:
            pass
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return float(dt.timestamp())
        except Exception:
            return None
    try:
        return float(value)
    except Exception:
        return None


def _mlops_infer_target_column(rows: list, preferred: Optional[str] = None) -> str:
    if not rows or not isinstance(rows[0], dict):
        raise HTTPException(400, "Rows are empty or not tabular")
    first = rows[0]
    columns = [str(c) for c in first.keys()]
    if preferred and preferred in columns:
        return preferred

    priority = ["target", "label", "y", "is_churn", "churn", "defaulted", "sales", "revenue", "amount"]
    for name in priority:
        if name in columns:
            return name

    numeric_cols = []
    for col in columns:
        valid = 0
        for row in rows[:300]:
            if _mlops_to_float((row or {}).get(col)) is not None:
                valid += 1
        if valid >= max(3, int(len(rows[:300]) * 0.7)):
            numeric_cols.append(col)
    if numeric_cols:
        return numeric_cols[-1]

    return columns[-1]


def _mlops_rows_to_matrix(rows: list, feature_columns: List[str]) -> Any:
    import numpy as np

    categorical_maps: Dict[str, Dict[str, float]] = {}
    matrix: List[List[float]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        vec: List[float] = []
        for col in feature_columns:
            value = row.get(col)
            as_num = _mlops_to_float(value)
            if as_num is None:
                token = "" if value is None else str(value).strip()
                cmap = categorical_maps.setdefault(col, {})
                if token not in cmap:
                    cmap[token] = float(len(cmap) + 1)
                as_num = cmap[token]
            vec.append(float(as_num))
        matrix.append(vec)
    if not matrix:
        raise HTTPException(400, "Could not construct numeric feature matrix")
    return np.array(matrix, dtype=float)


def _mlops_prepare_target(rows: list, target_column: str, task: str) -> Dict[str, Any]:
    import numpy as np

    labels = [((row or {}).get(target_column) if isinstance(row, dict) else None) for row in rows]
    numeric_values = [_mlops_to_float(v) for v in labels]
    numeric_ratio = sum(v is not None for v in numeric_values) / max(len(numeric_values), 1)

    normalized_task = (task or "auto").strip().lower()
    unique_raw = sorted({str(v) for v in labels})

    if normalized_task == "regression" or (normalized_task == "auto" and numeric_ratio >= 0.85 and len(set(v for v in numeric_values if v is not None)) > 8):
        y = np.array([float(v if v is not None else 0.0) for v in numeric_values], dtype=float)
        return {"task": "regression", "y": y, "target_info": {"target_column": target_column}}

    class_tokens = [str(v) for v in labels]
    classes = sorted(set(class_tokens))
    if len(classes) < 2:
        raise HTTPException(400, f"Target column '{target_column}' has fewer than 2 classes")

    if len(classes) > 2 and normalized_task == "classification":
        raise HTTPException(400, "Only binary classification is supported in current endpoint")

    if len(classes) > 2:
        # Auto mode fallback for multiclass: reduce to major class vs rest
        from collections import Counter
        major = Counter(class_tokens).most_common(1)[0][0]
        y = np.array([1.0 if token == major else 0.0 for token in class_tokens], dtype=float)
        return {
            "task": "classification",
            "y": y,
            "target_info": {
                "target_column": target_column,
                "class_mapping": {"negative": "not_" + major, "positive": major},
                "strategy": "one-vs-rest",
            },
        }

    mapping = {classes[0]: 0.0, classes[1]: 1.0}
    y = np.array([mapping[token] for token in class_tokens], dtype=float)
    return {
        "task": "classification",
        "y": y,
        "target_info": {"target_column": target_column, "class_mapping": mapping},
    }


def _mlops_regression_metrics(y_true: Any, y_pred: Any) -> Dict[str, float]:
    import numpy as np

    err = y_true - y_pred
    rmse = float(np.sqrt(np.mean(err ** 2)))
    mae = float(np.mean(np.abs(err)))
    with np.errstate(divide="ignore", invalid="ignore"):
        denom = np.where(np.abs(y_true) < 1e-9, 1.0, np.abs(y_true))
        mape = float(np.mean(np.abs(err) / denom) * 100.0)
    sst = float(np.sum((y_true - np.mean(y_true)) ** 2))
    sse = float(np.sum(err ** 2))
    r2 = 0.0 if sst <= 1e-12 else float(1.0 - (sse / sst))
    return {
        "rmse": round(rmse, 6),
        "mae": round(mae, 6),
        "mape": round(mape, 6),
        "r2": round(r2, 6),
    }


def _mlops_classification_metrics(y_true: Any, y_prob: Any) -> Dict[str, float]:
    import numpy as np

    y_pred = (y_prob >= 0.5).astype(float)
    tp = float(np.sum((y_pred == 1.0) & (y_true == 1.0)))
    tn = float(np.sum((y_pred == 0.0) & (y_true == 0.0)))
    fp = float(np.sum((y_pred == 1.0) & (y_true == 0.0)))
    fn = float(np.sum((y_pred == 0.0) & (y_true == 1.0)))

    accuracy = (tp + tn) / max((tp + tn + fp + fn), 1.0)
    precision = tp / max((tp + fp), 1.0)
    recall = tp / max((tp + fn), 1.0)
    f1 = 2.0 * precision * recall / max((precision + recall), 1e-12)

    return {
        "accuracy": round(float(accuracy), 6),
        "precision": round(float(precision), 6),
        "recall": round(float(recall), 6),
        "f1_score": round(float(f1), 6),
    }


def _mlops_run_real_evaluation(
    rows: list,
    target_column: Optional[str] = None,
    feature_columns: Optional[List[str]] = None,
    test_size: float = 0.2,
    task: str = "auto",
) -> Dict[str, Any]:
    import numpy as np

    tabular_rows = [r for r in rows if isinstance(r, dict)]
    if len(tabular_rows) < 12:
        raise HTTPException(400, "At least 12 tabular rows are required for real ML evaluation")

    target_col = _mlops_infer_target_column(tabular_rows, target_column)
    inferred_feature_cols = [c for c in tabular_rows[0].keys() if c != target_col]
    if feature_columns:
        allowed = set(tabular_rows[0].keys())
        inferred_feature_cols = [c for c in feature_columns if c in allowed and c != target_col]
    if not inferred_feature_cols:
        raise HTTPException(400, "No usable feature columns after preprocessing")

    X = _mlops_rows_to_matrix(tabular_rows, inferred_feature_cols)
    prep = _mlops_prepare_target(tabular_rows, target_col, task)
    y = prep["y"]
    resolved_task = prep["task"]

    n = X.shape[0]
    if n < 12:
        raise HTTPException(400, "Insufficient rows after preprocessing")
    ts = max(0.05, min(float(test_size), 0.45))
    n_test = max(2, int(round(n * ts)))
    n_test = min(n - 2, n_test)

    rng = np.random.default_rng(42)
    idx = np.arange(n)
    rng.shuffle(idx)
    test_idx = idx[:n_test]
    train_idx = idx[n_test:]

    X_train = X[train_idx]
    y_train = y[train_idx]
    X_test = X[test_idx]
    y_test = y[test_idx]

    if resolved_task == "regression":
        Xb_train = np.c_[np.ones((X_train.shape[0], 1)), X_train]
        coef = np.linalg.lstsq(Xb_train, y_train, rcond=None)[0]
        Xb_test = np.c_[np.ones((X_test.shape[0], 1)), X_test]
        pred_train = Xb_train @ coef
        pred_test = Xb_test @ coef
        train_metrics = _mlops_regression_metrics(y_train, pred_train)
        test_metrics = _mlops_regression_metrics(y_test, pred_test)
        weights = coef[1:]
    else:
        Xb_train = np.c_[np.ones((X_train.shape[0], 1)), X_train]
        w = np.zeros(Xb_train.shape[1], dtype=float)
        lr = 0.1
        for _ in range(700):
            z = np.clip(Xb_train @ w, -35, 35)
            pred = 1.0 / (1.0 + np.exp(-z))
            grad = (Xb_train.T @ (pred - y_train)) / max(len(y_train), 1)
            grad[1:] += 1e-4 * w[1:]
            w -= lr * grad

        Xb_test = np.c_[np.ones((X_test.shape[0], 1)), X_test]
        pred_train = 1.0 / (1.0 + np.exp(-np.clip(Xb_train @ w, -35, 35)))
        pred_test = 1.0 / (1.0 + np.exp(-np.clip(Xb_test @ w, -35, 35)))
        train_metrics = _mlops_classification_metrics(y_train, pred_train)
        test_metrics = _mlops_classification_metrics(y_test, pred_test)
        weights = w[1:]

    abs_w = np.abs(weights)
    total_w = float(np.sum(abs_w))
    if total_w <= 1e-12:
        feature_importance = [{"feature": col, "importance": 0.0} for col in inferred_feature_cols[:10]]
    else:
        feature_importance = sorted(
            [
                {"feature": col, "importance": round(float(val / total_w), 6)}
                for col, val in zip(inferred_feature_cols, abs_w)
            ],
            key=lambda x: x["importance"],
            reverse=True
        )[:10]

    sample_predictions = []
    preview_count = len(y_test)
    for i in range(preview_count):
        sample_predictions.append({
            "actual": _mlops_normalize_sample_value(float(y_test[i])),
            "predicted": _mlops_normalize_sample_value(float(pred_test[i])),
        })

    return {
        "task": resolved_task,
        "target": prep["target_info"],
        "rows": {
            "total": int(n),
            "train": int(len(train_idx)),
            "test": int(len(test_idx)),
            "test_size": round(float(len(test_idx) / max(n, 1)), 4),
        },
        "features": inferred_feature_cols,
        "train_metrics": train_metrics,
        "test_metrics": test_metrics,
        "feature_importance": feature_importance,
        "sample_predictions": sample_predictions,
        "output_preview": _mlops_sanitize_sample_rows(tabular_rows),
    }


def _mlops_extract_source_from_workflow(workflow: models.MLOpsWorkflow, db: Session) -> Dict[str, Any]:
    nodes = workflow.nodes or []
    edges = workflow.edges or []
    order = _mlops_topological_order(nodes, edges)
    nodes_by_id = {str(n.get("id")): n for n in nodes if isinstance(n, dict) and n.get("id")}

    source_node = None
    for nid in order:
        node = nodes_by_id.get(nid)
        if not node:
            continue
        ntype = _mlops_node_type(node).lower()
        if "source" in ntype or "dataset" in ntype:
            source_node = node
            break
    if not source_node:
        raise HTTPException(400, "Workflow has no source node")

    data = source_node.get("data", {}) if isinstance(source_node, dict) else {}
    config = data.get("config", {}) if isinstance(data, dict) else {}
    ntype = _mlops_node_type(source_node).lower()

    if "ml_pipeline_output_source" in ntype:
        return _mlops_source_rows(
            db=db,
            source_type="pipeline",
            pipeline_id=str(config.get("pipeline_id") or "").strip(),
            pipeline_node_id=str(config.get("pipeline_node_id") or config.get("node_id") or "").strip() or None,
        )

    if "dataset" in ntype:
        source_type = str(config.get("source_type") or "sample").strip().lower()
        if source_type == "file":
            return _mlops_source_rows(
                db=db,
                source_type="file",
                file_path=str(config.get("file_path") or "").strip(),
                dataset=str(config.get("dataset") or "sales"),
            )
        return _mlops_source_rows(
            db=db,
            source_type="sample",
            dataset=str(config.get("dataset") or "sales"),
        )

    return _mlops_source_rows(db=db, source_type="sample", dataset="sales")


def _mlops_eval_defaults_from_workflow(workflow: models.MLOpsWorkflow) -> Dict[str, Any]:
    nodes = workflow.nodes or []
    edges = workflow.edges or []
    order = _mlops_topological_order(nodes, edges)
    nodes_by_id = {str(n.get("id")): n for n in nodes if isinstance(n, dict) and n.get("id")}

    target_column = None
    feature_columns = None
    test_size = 0.2
    task = "auto"

    for nid in order:
        node = nodes_by_id.get(nid)
        if not node:
            continue
        ntype = _mlops_node_type(node).lower()
        config = (node.get("data", {}) or {}).get("config", {}) if isinstance(node.get("data", {}), dict) else {}

        if not target_column:
            maybe_target = str(config.get("target_column") or "").strip()
            if maybe_target:
                target_column = maybe_target

        if "split" in ntype:
            raw = config.get("test_size")
            if raw is not None:
                try:
                    val = float(raw)
                    test_size = val / 100.0 if val > 1 else val
                except Exception:
                    pass

        if "training" in ntype or "train" in ntype:
            algo = str(config.get("algorithm") or "").lower()
            if "class" in algo:
                task = "classification"
            elif "regress" in algo or "forecast" in algo:
                task = "regression"

            cols = config.get("feature_columns")
            if isinstance(cols, list):
                feature_columns = [str(c) for c in cols if str(c).strip()]

    return {
        "target_column": target_column,
        "feature_columns": feature_columns,
        "test_size": max(0.05, min(test_size, 0.45)),
        "task": task,
    }


def _mlops_message_for_node(label: str, node_type: str, rows: int, metrics: dict) -> str:
    ntype = (node_type or "").lower()
    if "feature" in ntype:
        feat_count = int(metrics.get("feature_count", 0))
        op_count = int(metrics.get("feature_operations_applied") or 0)
        if op_count > 0:
            return f"✓ {label} — engineered {feat_count} columns using {op_count} feature ops on {rows:,} rows"
        return f"✓ {label} — engineered {feat_count} columns from {rows:,} rows"
    if "training" in ntype or "train" in ntype:
        acc = metrics.get("accuracy")
        return f"✓ {label} — trained model on {rows:,} rows (accuracy {acc:.3f})"
    if "evaluation" in ntype:
        rmse = metrics.get("rmse")
        mape = metrics.get("mape")
        return f"✓ {label} — RMSE {rmse:.3f}, MAPE {mape:.2f}%"
    if "forecast" in ntype:
        horizon = int(metrics.get("forecast_horizon_days", 30))
        return f"✓ {label} — generated {rows:,} forecasts ({horizon}-day horizon)"
    if "deploy" in ntype or "endpoint" in ntype or "registry" in ntype:
        endpoint = metrics.get("deployment_endpoint")
        return f"✓ {label} — deployed model endpoint {endpoint}"
    return f"✓ {label} — processed {rows:,} rows"


async def _simulate_mlops_workflow_run(workflow: models.MLOpsWorkflow, run_id: str):
    import asyncio
    import random
    bg_db = SessionLocal()
    try:
        run_row = bg_db.query(models.MLOpsRun).filter(models.MLOpsRun.id == run_id).first()
        wf_row = bg_db.query(models.MLOpsWorkflow).filter(models.MLOpsWorkflow.id == workflow.id).first()
        if not run_row or not wf_row:
            return

        nodes = wf_row.nodes or []
        edges = wf_row.edges or []
        order = _mlops_topological_order(nodes, edges)
        if not order:
            run_row.status = "failed"
            run_row.error_message = "Workflow has no executable nodes"
            run_row.finished_at = datetime.utcnow()
            bg_db.commit()
            return

        nodes_by_id = {str(n.get("id")): n for n in nodes if n.get("id")}
        logs: List[dict] = []
        rows_by_node: Dict[str, int] = {}
        samples_by_node: Dict[str, List[dict]] = {}
        model_version = f"v{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        metrics: Dict[str, Any] = {
            "accuracy": None,
            "precision": None,
            "recall": None,
            "f1_score": None,
            "rmse": None,
            "mape": None,
            "feature_count": None,
            "feature_operations_applied": None,
            "forecast_horizon_days": 30,
            "deployment_endpoint": f"/mlops/inference/{workflow.id[:8]}",
            "model_version": model_version,
        }

        for nid in order:
            node = nodes_by_id.get(nid)
            if not node:
                continue
            label = _mlops_node_label(node)
            ntype = _mlops_node_type(node)

            running_log = {
                "nodeId": nid,
                "nodeLabel": label,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "running",
                "message": f"⟳ Running {label}…",
                "rows": 0,
            }
            logs.append(running_log)
            run_row.logs = list(logs)
            bg_db.commit()

            await asyncio.sleep(0.45 + random.random() * 0.35)

            node_data = node.get("data", {}) if isinstance(node, dict) else {}
            node_config = node_data.get("config", {}) if isinstance(node_data, dict) else {}
            upstream_node_ids = [
                str(e.get("source"))
                for e in edges
                if str(e.get("target")) == nid and e.get("source")
            ]
            input_sample = []
            for upstream_id in upstream_node_ids:
                upstream_sample = samples_by_node.get(upstream_id) or []
                if upstream_sample:
                    input_sample = _mlops_sanitize_sample_rows(upstream_sample)
                    break

            upstream_rows = sum(
                rows_by_node.get(str(e.get("source")), 0)
                for e in edges
                if str(e.get("target")) == nid
            )
            rows = _mlops_rows_for_node(ntype, upstream_rows)

            ntype_lower = ntype.lower()
            if "ml_pipeline_output_source" in ntype_lower:
                pipeline_id = str(node_config.get("pipeline_id") or "").strip()
                pipeline_node_id = node_config.get("pipeline_node_id") or node_config.get("node_id")
                preview = _mlops_pipeline_source_preview(bg_db, pipeline_id, str(pipeline_node_id).strip() if pipeline_node_id else None)
                if preview:
                    rows = int(preview["row_count"])
                    input_sample = []
                    output_sample = list(preview["sample_rows"])
                    metrics["source_pipeline"] = preview["pipeline_name"]
                else:
                    output_sample = _mlops_synthetic_source_sample(rows)
            elif "source" in ntype_lower or "dataset" in ntype_lower:
                output_sample = _mlops_synthetic_source_sample(rows)
            else:
                output_sample = _mlops_build_output_sample(
                    ntype,
                    label,
                    input_sample,
                    rows,
                    metrics,
                    model_version,
                    node_config,
                )

            rows_by_node[nid] = rows
            samples_by_node[nid] = _mlops_sanitize_sample_rows(output_sample)

            if "feature" in ntype_lower:
                if output_sample:
                    metrics["feature_count"] = max(
                        int(metrics.get("feature_count") or 0),
                        len(output_sample[0].keys())
                    )
            if "training" in ntype_lower or "train" in ntype_lower:
                metrics["accuracy"] = round(random.uniform(0.76, 0.97), 3)
                metrics["precision"] = round(max(0.6, metrics["accuracy"] - random.uniform(0.01, 0.06)), 3)
                metrics["recall"] = round(max(0.6, metrics["accuracy"] - random.uniform(0.01, 0.08)), 3)
                p = float(metrics["precision"])
                r = float(metrics["recall"])
                metrics["f1_score"] = round((2 * p * r) / max((p + r), 1e-9), 3)
            if "evaluation" in ntype_lower:
                metrics["rmse"] = round(random.uniform(1.2, 18.9), 3)
                metrics["mape"] = round(random.uniform(3.5, 22.5), 2)
            if "forecast" in ntype_lower:
                metrics["forecast_horizon_days"] = random.choice([7, 14, 30, 60, 90])

            success_log = {
                "nodeId": nid,
                "nodeLabel": label,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "message": _mlops_message_for_node(label, ntype, rows, metrics),
                "rows": rows,
                "input_sample": _mlops_sanitize_sample_rows(input_sample),
                "output_sample": _mlops_sanitize_sample_rows(output_sample),
            }
            logs[-1] = success_log
            run_row.logs = list(logs)
            run_row.metrics = dict(metrics)
            run_row.artifact_rows = rows
            run_row.model_version = model_version
            bg_db.commit()

        run_row.status = "success"
        run_row.finished_at = datetime.utcnow()
        run_row.logs = list(logs)
        run_row.metrics = dict(metrics)
        run_row.artifact_rows = max(rows_by_node.values()) if rows_by_node else 0
        run_row.model_version = model_version
        bg_db.commit()

    except Exception as e:
        logger.error(f"MLOps run {run_id} failed: {e}")
        run_row = bg_db.query(models.MLOpsRun).filter(models.MLOpsRun.id == run_id).first()
        if run_row:
            run_row.status = "failed"
            run_row.error_message = str(e)
            run_row.finished_at = datetime.utcnow()
            bg_db.commit()
    finally:
        bg_db.close()


@app.get("/api/mlops/workflows")
async def list_mlops_workflows(db: Session = Depends(get_db)):
    rows = db.query(models.MLOpsWorkflow).order_by(models.MLOpsWorkflow.updated_at.desc()).all()
    out = []
    for wf in rows:
        last_run = db.query(models.MLOpsRun)\
            .filter(models.MLOpsRun.workflow_id == wf.id)\
            .order_by(models.MLOpsRun.started_at.desc()).first()
        out.append({
            "id": wf.id,
            "name": wf.name,
            "description": wf.description,
            "status": wf.status,
            "tags": wf.tags or [],
            "nodeCount": len(wf.nodes or []),
            "schedule_cron": wf.schedule_cron,
            "schedule_enabled": wf.schedule_enabled,
            "created_at": wf.created_at.isoformat() if wf.created_at else None,
            "updated_at": wf.updated_at.isoformat() if wf.updated_at else None,
            "last_run": {
                "id": last_run.id,
                "status": last_run.status,
                "started_at": last_run.started_at.isoformat() if last_run.started_at else None,
                "artifact_rows": last_run.artifact_rows,
                "model_version": last_run.model_version,
                "metrics": last_run.metrics or {},
            } if last_run else None,
        })
    return out


@app.post("/api/mlops/workflows", status_code=201)
async def create_mlops_workflow(body: MLOpsWorkflowCreate, db: Session = Depends(get_db)):
    wf = models.MLOpsWorkflow(
        id=str(uuid.uuid4()),
        name=body.name,
        description=body.description,
        nodes=body.nodes,
        edges=body.edges,
        tags=body.tags,
        status=body.status,
    )
    db.add(wf)
    db.commit()
    db.refresh(wf)
    return {"id": wf.id, "name": wf.name, "status": wf.status}


@app.get("/api/mlops/workflows/{workflow_id}")
async def get_mlops_workflow(workflow_id: str, db: Session = Depends(get_db)):
    wf = db.query(models.MLOpsWorkflow).filter(models.MLOpsWorkflow.id == workflow_id).first()
    if not wf:
        raise HTTPException(status_code=404, detail="MLOps workflow not found")
    return {
        "id": wf.id,
        "name": wf.name,
        "description": wf.description,
        "nodes": wf.nodes or [],
        "edges": wf.edges or [],
        "status": wf.status,
        "tags": wf.tags or [],
        "schedule_cron": wf.schedule_cron,
        "schedule_enabled": wf.schedule_enabled,
        "created_at": wf.created_at.isoformat() if wf.created_at else None,
        "updated_at": wf.updated_at.isoformat() if wf.updated_at else None,
    }


@app.put("/api/mlops/workflows/{workflow_id}")
async def update_mlops_workflow(workflow_id: str, body: MLOpsWorkflowUpdate, db: Session = Depends(get_db)):
    wf = db.query(models.MLOpsWorkflow).filter(models.MLOpsWorkflow.id == workflow_id).first()
    if not wf:
        raise HTTPException(status_code=404, detail="MLOps workflow not found")
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(wf, k, v)
    wf.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(wf)
    return {"id": wf.id, "name": wf.name, "status": wf.status}


@app.delete("/api/mlops/workflows/{workflow_id}", status_code=204)
async def delete_mlops_workflow(workflow_id: str, db: Session = Depends(get_db)):
    wf = db.query(models.MLOpsWorkflow).filter(models.MLOpsWorkflow.id == workflow_id).first()
    if wf:
        db.delete(wf)
        db.commit()


@app.post("/api/mlops/workflows/{workflow_id}/duplicate")
async def duplicate_mlops_workflow(workflow_id: str, db: Session = Depends(get_db)):
    wf = db.query(models.MLOpsWorkflow).filter(models.MLOpsWorkflow.id == workflow_id).first()
    if not wf:
        raise HTTPException(status_code=404, detail="MLOps workflow not found")
    copy = models.MLOpsWorkflow(
        id=str(uuid.uuid4()),
        name=f"{wf.name} (Copy)",
        description=wf.description,
        nodes=wf.nodes,
        edges=wf.edges,
        tags=wf.tags,
        status="draft",
    )
    db.add(copy)
    db.commit()
    return {"id": copy.id, "name": copy.name}


@app.post("/api/mlops/workflows/{workflow_id}/execute")
async def execute_mlops_workflow(workflow_id: str, db: Session = Depends(get_db)):
    wf = db.query(models.MLOpsWorkflow).filter(models.MLOpsWorkflow.id == workflow_id).first()
    if not wf:
        raise HTTPException(status_code=404, detail="MLOps workflow not found")
    run_id = str(uuid.uuid4())
    run = models.MLOpsRun(
        id=run_id,
        workflow_id=workflow_id,
        status="running",
        triggered_by="manual",
    )
    db.add(run)
    db.commit()

    import asyncio
    asyncio.create_task(_simulate_mlops_workflow_run(wf, run_id))
    return {"run_id": run_id, "status": "running"}


@app.get("/api/mlops/runs")
async def list_mlops_runs(workflow_id: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(models.MLOpsRun).order_by(models.MLOpsRun.started_at.desc())
    if workflow_id:
        query = query.filter(models.MLOpsRun.workflow_id == workflow_id)
    rows = query.limit(300).all()
    out = []
    for r in rows:
        wf = db.query(models.MLOpsWorkflow).filter(models.MLOpsWorkflow.id == r.workflow_id).first()
        duration = None
        if r.started_at and r.finished_at:
            duration = (r.finished_at - r.started_at).total_seconds()
        out.append({
            "id": r.id,
            "workflow_id": r.workflow_id,
            "workflow_name": wf.name if wf else "Unknown",
            "status": r.status,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "finished_at": r.finished_at.isoformat() if r.finished_at else None,
            "duration": duration,
            "artifact_rows": r.artifact_rows,
            "model_version": r.model_version,
            "metrics": r.metrics or {},
            "logs": r.logs or [],
            "error_message": r.error_message,
            "triggered_by": r.triggered_by,
        })
    return out


@app.get("/api/mlops/runs/{run_id}")
async def get_mlops_run(run_id: str, db: Session = Depends(get_db)):
    r = db.query(models.MLOpsRun).filter(models.MLOpsRun.id == run_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="MLOps run not found")
    return {
        "id": r.id,
        "workflow_id": r.workflow_id,
        "status": r.status,
        "started_at": r.started_at.isoformat() if r.started_at else None,
        "finished_at": r.finished_at.isoformat() if r.finished_at else None,
        "artifact_rows": r.artifact_rows,
        "model_version": r.model_version,
        "metrics": r.metrics or {},
        "logs": r.logs or [],
        "error_message": r.error_message,
        "triggered_by": r.triggered_by,
    }


@app.get("/api/mlops/stats")
async def mlops_stats(db: Session = Depends(get_db)):
    total_workflows = db.query(models.MLOpsWorkflow).count()
    total_runs = db.query(models.MLOpsRun).count()
    successful = db.query(models.MLOpsRun).filter(models.MLOpsRun.status == "success").count()
    failed = db.query(models.MLOpsRun).filter(models.MLOpsRun.status == "failed").count()
    running = db.query(models.MLOpsRun).filter(models.MLOpsRun.status == "running").count()
    return {
        "total_workflows": total_workflows,
        "total_runs": total_runs,
        "successful_runs": successful,
        "failed_runs": failed,
        "running_runs": running,
        "success_rate": round(successful / max(total_runs, 1) * 100, 1),
    }


@app.post("/api/mlops/workflows/{workflow_id}/feature-profile")
async def mlops_feature_profile(
    workflow_id: str,
    body: MLOpsFeatureProfileRequest,
    db: Session = Depends(get_db),
):
    wf = db.query(models.MLOpsWorkflow).filter(models.MLOpsWorkflow.id == workflow_id).first()
    if not wf:
        raise HTTPException(status_code=404, detail="MLOps workflow not found")

    sample_size = max(50, min(int(body.sample_size or 2000), 10000))
    preview = _mlops_preview_workflow_samples(wf, db, sample_size=sample_size)
    input_rows_by_node = preview.get("input_rows_by_node", {})
    output_rows_by_node = preview.get("output_rows_by_node", {})

    node_id = str(body.node_id or "").strip()
    if not node_id:
        for n in wf.nodes or []:
            if not isinstance(n, dict):
                continue
            nid = str(n.get("id") or "")
            if not nid:
                continue
            ntype = _mlops_node_type(n).lower()
            if "feature" in ntype:
                node_id = nid
                break

    rows_for_profile: List[dict] = []
    node_label = ""
    node_type = ""

    if node_id:
        rows_for_profile = [r for r in input_rows_by_node.get(node_id, []) if isinstance(r, dict)]
        node = next((n for n in (wf.nodes or []) if isinstance(n, dict) and str(n.get("id")) == node_id), None)
        if node:
            node_label = _mlops_node_label(node)
            node_type = _mlops_node_type(node)
        if not rows_for_profile:
            rows_for_profile = [r for r in output_rows_by_node.get(node_id, []) if isinstance(r, dict)]

    if not rows_for_profile:
        src = _mlops_extract_source_from_workflow(wf, db)
        rows_for_profile = [r for r in src.get("rows", []) if isinstance(r, dict)]

    profile = _mlops_infer_column_profile(rows_for_profile, sample_size=sample_size)
    return {
        "workflow_id": wf.id,
        "workflow_name": wf.name,
        "node_id": node_id or None,
        "node_label": node_label or None,
        "node_type": node_type or None,
        "available_operations": _mlops_feature_operation_catalog(),
        "profile": profile,
        "sample_rows": _mlops_sanitize_sample_rows(rows_for_profile, limit=25),
    }


@app.post("/api/mlops/evaluate")
async def evaluate_mlops_model(body: MLOpsEvaluateRequest, db: Session = Depends(get_db)):
    src = _mlops_source_rows(
        db=db,
        source_type=body.source_type,
        pipeline_id=body.pipeline_id,
        pipeline_node_id=body.pipeline_node_id,
        file_path=body.file_path,
        dataset=body.dataset,
    )
    evaluation = _mlops_run_real_evaluation(
        rows=src["rows"],
        target_column=body.target_column,
        feature_columns=body.feature_columns,
        test_size=body.test_size,
        task=body.task,
    )
    return {
        "status": "success",
        "mode": "real",
        "source": src["source"],
        "evaluation": evaluation,
        "evaluated_at": datetime.utcnow().isoformat(),
    }


@app.post("/api/mlops/workflows/{workflow_id}/evaluate")
async def evaluate_mlops_workflow(workflow_id: str, db: Session = Depends(get_db)):
    wf = db.query(models.MLOpsWorkflow).filter(models.MLOpsWorkflow.id == workflow_id).first()
    if not wf:
        raise HTTPException(status_code=404, detail="MLOps workflow not found")

    src = _mlops_extract_source_from_workflow(wf, db)
    defaults = _mlops_eval_defaults_from_workflow(wf)
    transformed_rows = _mlops_apply_preprocessing_nodes(wf, [r for r in src["rows"] if isinstance(r, dict)])
    evaluation = _mlops_run_real_evaluation(
        rows=transformed_rows,
        target_column=defaults["target_column"],
        feature_columns=defaults["feature_columns"],
        test_size=defaults["test_size"],
        task=defaults["task"],
    )
    return {
        "status": "success",
        "mode": "real",
        "workflow_id": wf.id,
        "workflow_name": wf.name,
        "source": src["source"],
        "preprocessed_row_count": len(transformed_rows),
        "evaluation": evaluation,
        "evaluated_at": datetime.utcnow().isoformat(),
    }


# ─── MLOPS H2O AUTOML SERVICE ────────────────────────────────────────────────

def _h2o_import_modules():
    try:
        import h2o
        from h2o.automl import H2OAutoML
        return h2o, H2OAutoML
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="H2O dependency is not available. Install backend requirements to enable H2O AutoML."
        ) from exc


def _h2o_safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return float(int(value))
        as_float = float(value)
        if not math.isfinite(as_float):
            return None
        return as_float
    except Exception:
        return None


def _h2o_text_value(value: Any) -> str:
    if value is None:
        return ""
    try:
        if isinstance(value, float) and math.isnan(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _h2o_json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return float(value)
    if isinstance(value, str):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, list):
        return [_h2o_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _h2o_json_safe(v) for k, v in value.items()}
    try:
        if hasattr(value, "item"):
            return _h2o_json_safe(value.item())
    except Exception:
        pass
    return str(value)


def _h2o_init_cluster(h2o_mod: Any):
    H2O_MODELS_DIR.mkdir(parents=True, exist_ok=True)
    H2O_MOJO_DIR.mkdir(parents=True, exist_ok=True)

    try:
        cluster = h2o_mod.cluster()
        if cluster:
            return
    except Exception:
        pass

    h2o_url = str(_os.getenv("H2O_URL") or "").strip()
    if h2o_url:
        try:
            h2o_mod.connect(url=h2o_url, verbose=False)
            return
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Failed to connect H2O at {h2o_url}: {exc}") from exc

    try:
        h2o_mod.init(
            max_mem_size=str(_os.getenv("H2O_MAX_MEM") or "2G"),
            nthreads=-1,
            strict_version_check=False,
            verbose=False,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Failed to initialize local H2O cluster: {exc}") from exc


def _h2o_normalize_cell(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple, set)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    return value


def _h2o_normalize_algo_list(raw_algos: Optional[List[str]]) -> Tuple[List[str], List[str]]:
    normalized: List[str] = []
    invalid: List[str] = []
    seen = set()
    for algo in raw_algos or []:
        text = str(algo or "").strip()
        if not text:
            continue
        key = re.sub(r"[^a-z0-9]", "", text.lower())
        canonical = _H2O_ALGO_CANONICAL.get(key)
        if not canonical:
            invalid.append(text)
            continue
        if canonical in seen:
            continue
        seen.add(canonical)
        normalized.append(canonical)
    return normalized, invalid


def _h2o_rows_to_dataframe(rows: List[dict]):
    import pandas as pd

    tabular_rows = [r for r in (rows or []) if isinstance(r, dict)]
    if not tabular_rows:
        raise HTTPException(status_code=400, detail="No tabular rows available")

    normalized_rows: List[Dict[str, Any]] = []
    for row in tabular_rows:
        normalized = {str(k): _h2o_normalize_cell(v) for k, v in row.items()}
        normalized_rows.append(normalized)

    df = pd.DataFrame(normalized_rows)
    if df.empty:
        raise HTTPException(status_code=400, detail="Resolved source is empty after normalization")

    df = df.loc[:, ~df.columns.duplicated()]
    df = df.replace({"": None})

    empty_cols = [str(c) for c in df.columns if df[str(c)].isna().all()]
    if empty_cols and len(empty_cols) < len(df.columns):
        df = df.drop(columns=empty_cols)
    if df.empty or len(df.columns) == 0:
        raise HTTPException(status_code=400, detail="No usable columns were found for H2O training")

    return df


def _h2o_resolve_task(df: Any, target_column: str, requested_task: str) -> str:
    import pandas as pd

    normalized = str(requested_task or "auto").strip().lower()
    if normalized in {"classification", "regression"}:
        return normalized

    target_series = df[target_column]
    if target_series is None:
        return "classification"

    non_null = target_series.dropna()
    if non_null.empty:
        return "classification"

    numeric = pd.to_numeric(non_null, errors="coerce")
    numeric_ratio = float(numeric.notna().mean()) if len(numeric) else 0.0
    unique_count = int(non_null.nunique())
    if numeric_ratio >= 0.95 and unique_count > 20:
        return "regression"
    return "classification"


def _h2o_select_auto_target_column(df: Any, inferred_target: str) -> str:
    import pandas as pd

    columns = [str(c) for c in df.columns]
    if not columns:
        return inferred_target

    total_rows = max(int(len(df)), 1)
    skip_tokens = {
        "id", "uuid", "guid", "name", "title", "description", "desc",
        "url", "uri", "image", "email", "phone", "mobile", "address",
        "sku", "code", "hash", "token",
    }

    def _is_id_like(col: str) -> bool:
        lc = str(col).strip().lower()
        if lc in skip_tokens:
            return True
        return lc.endswith("_id") or lc == "id"

    best_col = inferred_target if inferred_target in columns else columns[-1]
    best_score = -1.0

    for col in columns:
        series = df[col].replace({"": None}).dropna()
        if len(series) < max(20, int(total_rows * 0.35)):
            continue
        try:
            nunique = int(series.astype(str).nunique())
        except Exception:
            nunique = int(series.nunique())
        if nunique < 2:
            continue

        unique_ratio = float(nunique / max(len(series), 1))
        numeric_ratio = float(pd.to_numeric(series, errors="coerce").notna().mean())
        id_like = _is_id_like(col)

        # Ignore near-unique identifier/text columns in auto mode.
        if id_like and unique_ratio >= 0.7:
            continue
        if numeric_ratio < 0.5 and unique_ratio >= 0.8:
            continue

        if numeric_ratio >= 0.95:
            # Prefer continuous-ish numeric targets for regression.
            score = 220.0 + min(nunique, 200)
            if unique_ratio >= 0.98 and id_like:
                score -= 300.0
        else:
            # Prefer manageable categorical targets for classification.
            if nunique > max(35, int(len(series) * 0.3)):
                continue
            score = 140.0 - float(nunique)

        if score > best_score:
            best_score = score
            best_col = col

    return str(best_col)


def _h2o_resolve_feature_columns(
    df: Any,
    target_column: str,
    feature_columns: Optional[List[str]],
    exclude_columns: Optional[List[str]],
) -> List[str]:
    available = [str(c) for c in df.columns]
    if target_column not in available:
        raise HTTPException(status_code=400, detail=f"Target column '{target_column}' does not exist in source data")

    excluded = {str(c).strip() for c in (exclude_columns or []) if str(c).strip()}
    excluded.add(target_column)
    if feature_columns:
        selected = [str(c).strip() for c in feature_columns if str(c).strip() and str(c).strip() in available and str(c).strip() not in excluded]
    else:
        selected = [c for c in available if c not in excluded]

    deduped: List[str] = []
    seen = set()
    for col in selected:
        if col in seen:
            continue
        seen.add(col)
        deduped.append(col)
    if not deduped:
        raise HTTPException(status_code=400, detail="No feature columns available after filtering")
    return deduped


def _h2o_source_rows_from_request(db: Session, body: Any) -> Dict[str, Any]:
    source_type = str(getattr(body, "source_type", "rows") or "rows").strip().lower()
    if source_type == "etl":
        source_type = "pipeline"
    if source_type not in H2O_REQUEST_SOURCE_TYPES:
        raise HTTPException(status_code=400, detail=f"Unsupported source_type '{source_type}'")

    if source_type == "rows":
        rows = [dict(r) for r in (getattr(body, "rows", []) or []) if isinstance(r, dict)]
        if not rows:
            raise HTTPException(status_code=400, detail="rows are required for source_type=rows")
        return {
            "rows": rows,
            "source": {
                "type": "rows",
                "row_count": len(rows),
            },
        }

    if source_type == "workflow":
        workflow_id = str(getattr(body, "workflow_id", "") or "").strip()
        if not workflow_id:
            raise HTTPException(status_code=400, detail="workflow_id is required for source_type=workflow")
        wf = db.query(models.MLOpsWorkflow).filter(models.MLOpsWorkflow.id == workflow_id).first()
        if not wf:
            raise HTTPException(status_code=404, detail="MLOps workflow not found")

        src = _mlops_extract_source_from_workflow(wf, db)
        rows = [r for r in src.get("rows", []) if isinstance(r, dict)]
        if bool(getattr(body, "use_workflow_preprocessing", True)):
            rows = _mlops_apply_preprocessing_nodes(wf, rows)

        return {
            "rows": rows,
            "source": {
                **(src.get("source") if isinstance(src.get("source"), dict) else {}),
                "type": "workflow",
                "workflow_id": wf.id,
                "workflow_name": wf.name,
                "row_count": len(rows),
                "preprocessed": bool(getattr(body, "use_workflow_preprocessing", True)),
            },
        }

    src = _mlops_source_rows(
        db=db,
        source_type=source_type,
        pipeline_id=str(getattr(body, "pipeline_id", "") or "").strip() or None,
        pipeline_node_id=str(getattr(body, "pipeline_node_id", "") or "").strip() or None,
        file_path=str(getattr(body, "file_path", "") or "").strip() or None,
        dataset=str(getattr(body, "dataset", "sales") or "sales"),
    )
    return {
        "rows": [r for r in src.get("rows", []) if isinstance(r, dict)],
        "source": src.get("source", {}),
    }


def _h2o_metric_payload(perf: Any, task: str) -> Dict[str, Any]:
    def _metric(method_name: str) -> Optional[float]:
        try:
            fn = getattr(perf, method_name, None)
            if not callable(fn):
                return None
            value = fn()
            if isinstance(value, list) and value:
                first = value[0]
                if isinstance(first, (list, tuple)) and len(first) > 1:
                    return _h2o_safe_float(first[1])
                return _h2o_safe_float(first)
            return _h2o_safe_float(value)
        except Exception:
            return None

    payload = {
        "rmse": _metric("rmse"),
        "mse": _metric("mse"),
        "mae": _metric("mae"),
        "r2": _metric("r2"),
    }
    if task == "classification":
        payload.update({
            "auc": _metric("auc"),
            "logloss": _metric("logloss"),
            "accuracy": _metric("accuracy"),
            "precision": _metric("precision"),
            "recall": _metric("recall"),
            "f1_score": _metric("F1"),
        })

    return {k: round(v, 6) if isinstance(v, float) else v for k, v in payload.items() if v is not None}


def _h2o_feature_importance(leader: Any) -> List[dict]:
    try:
        vip = leader.varimp(use_pandas=True)
    except Exception:
        return []
    if vip is None:
        return []
    try:
        rows = vip.to_dict(orient="records")
    except Exception:
        return []

    result: List[dict] = []
    for row in rows[:30]:
        feature = str(row.get("variable") or row.get("feature") or "")
        importance = _h2o_safe_float(row.get("relative_importance"))
        if not feature or importance is None:
            continue
        result.append({"feature": feature, "importance": round(float(importance), 6)})
    return result


def _h2o_leaderboard_payload(leaderboard: Any, limit: int = 25) -> List[dict]:
    try:
        df = leaderboard.as_data_frame(use_pandas=True)
    except Exception:
        return []

    rows: List[dict] = []
    if not hasattr(df, "to_dict"):
        return rows
    raw_rows = df.head(max(1, min(limit, 100))).to_dict(orient="records")
    for row in raw_rows:
        rows.append({str(k): _h2o_json_safe(v) for k, v in (row or {}).items()})
    return rows


def _h2o_resolve_leader_model(h2o_mod: Any, automl: Any) -> Any:
    if automl is None:
        return None
    try:
        leader = automl.leader
        if leader is not None:
            return leader
    except Exception:
        pass

    try:
        leaderboard = getattr(automl, "leaderboard", None)
        if leaderboard is None:
            return None
        df = leaderboard.as_data_frame(use_pandas=True)
    except Exception:
        return None

    if df is None or not hasattr(df, "empty") or df.empty:
        return None
    try:
        columns = [str(c) for c in list(getattr(df, "columns", []) or [])]
    except Exception:
        columns = []
    if not columns:
        return None

    model_id_col = "model_id" if "model_id" in columns else columns[0]
    try:
        first_row = df.iloc[0].to_dict() if hasattr(df.iloc[0], "to_dict") else dict(df.iloc[0])
    except Exception:
        return None
    model_id = _h2o_text_value((first_row or {}).get(model_id_col))
    if not model_id:
        return None

    try:
        return h2o_mod.get_model(model_id)
    except Exception:
        return None


def _h2o_event_log_messages(automl: Any, limit: int = 15) -> List[str]:
    if automl is None:
        return []
    try:
        event_log = getattr(automl, "event_log", None)
        if event_log is None:
            return []
        df = event_log.as_data_frame(use_pandas=True)
    except Exception:
        return []
    if df is None or not hasattr(df, "empty") or df.empty:
        return []

    raw_columns = getattr(df, "columns", None)
    if raw_columns is None:
        cols = set()
    else:
        try:
            cols = {str(c) for c in list(raw_columns)}
        except Exception:
            cols = set()
    level_col = "level" if "level" in cols else None
    stage_col = "stage" if "stage" in cols else None
    message_col = "message" if "message" in cols else None
    name_col = "name" if "name" in cols else None
    value_col = "value" if "value" in cols else None

    result: List[str] = []
    tail_df = df.tail(max(1, min(limit, 50)))
    for _, row in tail_df.iterrows():
        level = _h2o_text_value(row[level_col]) if level_col else ""
        stage = _h2o_text_value(row[stage_col]) if stage_col else ""
        message = _h2o_text_value(row[message_col]) if message_col else ""
        name = _h2o_text_value(row[name_col]) if name_col else ""
        value = _h2o_text_value(row[value_col]) if value_col else ""
        text = " | ".join(part for part in [level, stage, message, name, value] if part)
        text = " ".join(text.split())
        if not text:
            continue
        result.append(text[:280])
    return result


def _h2o_train_automl_once(
    H2OAutoML_cls: Any,
    automl_kwargs: Dict[str, Any],
    feature_columns: List[str],
    target_column: str,
    train_frame: Any,
    test_frame: Any,
):
    automl = H2OAutoML_cls(**automl_kwargs)
    started = datetime.utcnow()
    train_kwargs: Dict[str, Any] = {
        "x": feature_columns,
        "y": target_column,
        "training_frame": train_frame,
    }
    if test_frame is not None:
        train_kwargs["leaderboard_frame"] = test_frame
    try:
        automl.train(**train_kwargs)
        finished = datetime.utcnow()
        return automl, started, finished, None
    except Exception as exc:
        finished = datetime.utcnow()
        return automl, started, finished, exc


def _h2o_resolve_model_reference(
    db: Session,
    run_id: Optional[str],
    model_path: Optional[str],
):
    run_row = None
    resolved_path = str(model_path or "").strip()

    if run_id:
        run_row = db.query(models.MLOpsH2ORun).filter(models.MLOpsH2ORun.id == run_id).first()
        if not run_row:
            raise HTTPException(status_code=404, detail="H2O run not found")
        if run_row.status != "success":
            raise HTTPException(status_code=400, detail="Selected H2O run is not in success state")
        if not resolved_path:
            resolved_path = str(run_row.model_path or "").strip()

    if not resolved_path:
        raise HTTPException(status_code=400, detail="Provide run_id or model_path")

    path_obj = Path(resolved_path).expanduser()
    if not path_obj.is_absolute():
        path_obj = (_BACKEND_DIR / path_obj).resolve()
    resolved_path = str(path_obj)

    if not _os.path.exists(resolved_path):
        raise HTTPException(status_code=404, detail=f"Model path not found: {resolved_path}")

    return run_row, resolved_path


def _h2o_load_model(h2o_mod: Any, model_path: str):
    normalized = str(Path(model_path).expanduser().resolve())
    cached = _H2O_MODEL_CACHE.get(normalized)
    if cached is not None:
        try:
            model_id = getattr(cached, "model_id", None)
            if model_id:
                return h2o_mod.get_model(model_id)
        except Exception:
            pass

    try:
        model = h2o_mod.load_model(normalized)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Unable to load H2O model: {exc}") from exc
    _H2O_MODEL_CACHE[normalized] = model
    return model


def _h2o_prepare_prediction_df(
    rows: List[dict],
    feature_columns: List[str],
):
    df = _h2o_rows_to_dataframe(rows)
    if feature_columns:
        for col in feature_columns:
            if col not in df.columns:
                df[col] = None
        df = df[feature_columns]
    return df


def _h2o_model_domain_map(model: Any) -> Dict[str, Any]:
    try:
        model_json = getattr(model, "_model_json", None)
        if not isinstance(model_json, dict):
            return {}
        output = model_json.get("output")
        if not isinstance(output, dict):
            return {}
        names = output.get("names")
        domains = output.get("domains")
        if not isinstance(names, list) or not isinstance(domains, list):
            return {}
        out: Dict[str, Any] = {}
        for idx, name in enumerate(names):
            key = str(name or "").strip()
            if not key:
                continue
            out[key] = domains[idx] if idx < len(domains) else None
        return out
    except Exception:
        return {}


def _h2o_align_frame_to_model_schema(
    frame: Any,
    model: Any,
    feature_columns: List[str],
):
    domain_map = _h2o_model_domain_map(model)
    for col in feature_columns:
        if col not in list(getattr(frame, "columns", []) or []):
            continue
        expected_domain = domain_map.get(col, "__unknown__")
        try:
            if expected_domain is None:
                frame[col] = frame[col].asnumeric()
            elif isinstance(expected_domain, (list, tuple)):
                # Force categorical columns to factor to match training schema.
                frame[col] = frame[col].ascharacter().asfactor()
        except Exception:
            # Best effort alignment; keep original type if conversion fails.
            pass
    return frame


def _h2o_prepare_prediction_frame(
    h2o_mod: Any,
    model: Any,
    rows: List[dict],
    feature_columns: List[str],
):
    prepared_df = _h2o_prepare_prediction_df(rows, feature_columns)
    frame = h2o_mod.H2OFrame(prepared_df)
    cols = feature_columns if feature_columns else list(getattr(prepared_df, "columns", []) or [])
    return _h2o_align_frame_to_model_schema(frame, model, [str(c) for c in cols])


def _h2o_friendly_scoring_error(exc: Exception) -> str:
    message = str(exc or "").strip()
    lower = message.lower()
    if "categorical column" in lower and "real-valued in the training data" in lower:
        return (
            "Scoring failed because source column types do not match training schema "
            "(categorical vs numeric mismatch). Reload source columns and ensure numeric fields stay numeric."
        )
    if "real-valued column" in lower and "categorical in the training data" in lower:
        return (
            "Scoring failed because source column types do not match training schema "
            "(numeric vs categorical mismatch). Reload source columns and ensure categorical fields stay categorical."
        )
    return message or "Unknown H2O scoring error"


def _h2o_python_eval_metrics(task: str, y_true_raw: Any, pred_df: Any) -> Dict[str, Any]:
    import pandas as pd
    import numpy as np

    if "predict" not in pred_df.columns:
        return {"evaluated_rows": 0}

    y_true = pd.Series(y_true_raw)
    y_pred = pred_df["predict"]

    if task == "regression":
        y_true_num = pd.to_numeric(y_true, errors="coerce")
        y_pred_num = pd.to_numeric(y_pred, errors="coerce")
        mask = y_true_num.notna() & y_pred_num.notna()
        if int(mask.sum()) < 2:
            return {"evaluated_rows": int(mask.sum())}
        t = y_true_num[mask].to_numpy(dtype=float)
        p = y_pred_num[mask].to_numpy(dtype=float)
        err = t - p
        rmse = float(np.sqrt(np.mean(err ** 2)))
        mae = float(np.mean(np.abs(err)))
        with np.errstate(divide="ignore", invalid="ignore"):
            denom = np.where(np.abs(t) < 1e-9, 1.0, np.abs(t))
            mape = float(np.mean(np.abs(err) / denom) * 100.0)
        sst = float(np.sum((t - np.mean(t)) ** 2))
        sse = float(np.sum(err ** 2))
        r2 = 0.0 if sst <= 1e-12 else float(1.0 - sse / sst)
        return {
            "evaluated_rows": int(mask.sum()),
            "rmse": round(rmse, 6),
            "mae": round(mae, 6),
            "mape": round(mape, 6),
            "r2": round(r2, 6),
        }

    from sklearn.metrics import accuracy_score, precision_recall_fscore_support, roc_auc_score

    yt = y_true.astype(str).fillna("")
    yp = y_pred.astype(str).fillna("")
    mask = (yt != "") & (yp != "")
    if int(mask.sum()) == 0:
        return {"evaluated_rows": 0}

    yt_vals = yt[mask]
    yp_vals = yp[mask]
    accuracy = float(accuracy_score(yt_vals, yp_vals))
    precision, recall, f1, _ = precision_recall_fscore_support(
        yt_vals,
        yp_vals,
        average="weighted",
        zero_division=0,
    )
    metrics = {
        "evaluated_rows": int(mask.sum()),
        "accuracy": round(float(accuracy), 6),
        "precision": round(float(precision), 6),
        "recall": round(float(recall), 6),
        "f1_score": round(float(f1), 6),
    }

    prob_cols = [c for c in pred_df.columns if c != "predict"]
    if len(prob_cols) == 2:
        positive_label = sorted(prob_cols)[1]
        y_true_bin = (yt_vals == positive_label).astype(int)
        y_prob = pd.to_numeric(pred_df.loc[mask, positive_label], errors="coerce")
        if y_prob.notna().all() and y_true_bin.nunique() > 1:
            try:
                auc = float(roc_auc_score(y_true_bin, y_prob))
                metrics["auc"] = round(auc, 6)
            except Exception:
                pass
    return metrics


@app.get("/api/mlops/h2o/health")
async def mlops_h2o_health():
    try:
        h2o_mod, _ = _h2o_import_modules()
    except HTTPException as exc:
        return {
            "status": "unavailable",
            "detail": exc.detail,
            "h2o_available": False,
        }

    try:
        _h2o_init_cluster(h2o_mod)
        cluster = h2o_mod.cluster()
        return {
            "status": "ok",
            "h2o_available": True,
            "cluster_name": str(getattr(cluster, "cloud_name", None) or ""),
            "nodes": int(getattr(cluster, "cloud_size", 0) or 0),
            "version": str(getattr(cluster, "version", "") or ""),
        }
    except HTTPException as exc:
        return {
            "status": "unavailable",
            "detail": exc.detail,
            "h2o_available": False,
        }


@app.post("/api/mlops/h2o/source-columns")
async def mlops_h2o_source_columns(body: H2OSourceColumnsRequest, db: Session = Depends(get_db)):
    import pandas as pd

    source_bundle = _h2o_source_rows_from_request(db, body)
    source_rows = [r for r in source_bundle.get("rows", []) if isinstance(r, dict)]
    if not source_rows:
        raise HTTPException(status_code=400, detail="Resolved source has no tabular rows")

    max_profile_rows = max(50, min(int(body.max_profile_rows or 5000), 50000))
    max_preview_rows = max(5, min(int(body.max_preview_rows or 25), 200))
    profiled_rows = source_rows[:max_profile_rows]
    df = _h2o_rows_to_dataframe(profiled_rows)
    columns = [str(c) for c in df.columns]

    numeric_columns: List[str] = []
    datetime_columns: List[str] = []
    categorical_columns: List[str] = []
    text_columns: List[str] = []

    for col in columns:
        series = df[col].dropna()
        if series.empty:
            categorical_columns.append(col)
            continue
        numeric_ratio = float(pd.to_numeric(series, errors="coerce").notna().mean())
        datetime_ratio = 0.0
        try:
            datetime_ratio = float(pd.to_datetime(series, errors="coerce").notna().mean())
        except Exception:
            datetime_ratio = 0.0
        if numeric_ratio >= 0.9:
            numeric_columns.append(col)
        elif datetime_ratio >= 0.9:
            datetime_columns.append(col)
        elif float(series.astype(str).nunique() / max(len(series), 1)) >= 0.6:
            text_columns.append(col)
        else:
            categorical_columns.append(col)

    inferred_target = _mlops_infer_target_column(profiled_rows, None)
    suggested_target = _h2o_select_auto_target_column(df, inferred_target) if inferred_target in columns else inferred_target

    preview_df = df.head(max_preview_rows).copy()
    preview_df = preview_df.where(pd.notna(preview_df), None)

    return _h2o_json_safe({
        "status": "success",
        "source": source_bundle.get("source", {}),
        "row_count": int(len(source_rows)),
        "profiled_row_count": int(len(profiled_rows)),
        "columns": columns,
        "numeric_columns": numeric_columns,
        "datetime_columns": datetime_columns,
        "categorical_columns": categorical_columns,
        "text_columns": text_columns,
        "target_suggestion": suggested_target if suggested_target in columns else None,
        "preview_rows": preview_df.to_dict(orient="records") if hasattr(preview_df, "to_dict") else [],
    })


def _h2o_run_label(config: Any) -> Optional[str]:
    cfg = config if isinstance(config, dict) else {}
    label = str(cfg.get("label") or "").strip()
    return label or None


def _h2o_run_last_evaluation(config: Any) -> Dict[str, Any]:
    cfg = config if isinstance(config, dict) else {}
    last_eval = cfg.get("last_evaluation")
    return last_eval if isinstance(last_eval, dict) else {}


def _h2o_run_output_fields(config: Any, metrics: Any) -> List[str]:
    last_eval = _h2o_run_last_evaluation(config)
    output_fields = last_eval.get("output_fields")
    if isinstance(output_fields, list):
        fields = [str(v).strip() for v in output_fields if str(v).strip()]
        if fields:
            return fields
    metrics_obj = metrics if isinstance(metrics, dict) else {}
    fallback_fields = [str(k).strip() for k in metrics_obj.keys() if str(k).strip()]
    return fallback_fields[:200]


def _h2o_run_payload(run: models.MLOpsH2ORun, include_config: bool = False) -> Dict[str, Any]:
    config_obj = run.config if isinstance(run.config, dict) else {}
    last_eval = _h2o_run_last_evaluation(config_obj)
    payload: Dict[str, Any] = {
        "id": run.id,
        "workflow_id": run.workflow_id,
        "status": run.status,
        "source_type": run.source_type,
        "task": run.task,
        "target_column": run.target_column,
        "feature_columns": run.feature_columns or [],
        "row_count": int(run.row_count or 0),
        "train_rows": int(run.train_rows or 0),
        "test_rows": int(run.test_rows or 0),
        "model_id": run.model_id,
        "model_path": run.model_path,
        "mojo_path": run.mojo_path,
        "metrics": run.metrics or {},
        "label": _h2o_run_label(config_obj),
        "last_evaluated_at": last_eval.get("evaluated_at"),
        "last_evaluation_target_column": last_eval.get("target_column"),
        "last_evaluation_task": last_eval.get("task"),
        "output_fields": _h2o_run_output_fields(config_obj, run.metrics),
        "error_message": run.error_message,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "updated_at": run.updated_at.isoformat() if run.updated_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
    }
    if include_config:
        payload["source_meta"] = run.source_meta or {}
        payload["leaderboard"] = run.leaderboard or []
        payload["config"] = config_obj
        payload["last_evaluation"] = last_eval or None
    return _h2o_json_safe(payload)


@app.get("/api/mlops/h2o/runs")
async def list_mlops_h2o_runs(
    workflow_id: Optional[str] = None,
    limit: int = 50,
    db: Session = Depends(get_db),
):
    capped_limit = max(1, min(int(limit), 200))
    query = db.query(models.MLOpsH2ORun).order_by(models.MLOpsH2ORun.created_at.desc())
    if workflow_id:
        query = query.filter(models.MLOpsH2ORun.workflow_id == workflow_id)
    rows = query.limit(capped_limit).all()
    return [_h2o_run_payload(r, include_config=False) for r in rows]


@app.get("/api/mlops/h2o/runs/{run_id}")
async def get_mlops_h2o_run(run_id: str, db: Session = Depends(get_db)):
    r = db.query(models.MLOpsH2ORun).filter(models.MLOpsH2ORun.id == run_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="H2O run not found")
    return _h2o_run_payload(r, include_config=True)


@app.patch("/api/mlops/h2o/runs/{run_id}")
async def update_mlops_h2o_run(run_id: str, body: H2ORunManageRequest, db: Session = Depends(get_db)):
    run = db.query(models.MLOpsH2ORun).filter(models.MLOpsH2ORun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="H2O run not found")

    config_obj = dict(run.config or {}) if isinstance(run.config, dict) else {}
    if body.label is not None:
        label = str(body.label or "").strip()
        if label:
            config_obj["label"] = label[:120]
        else:
            config_obj.pop("label", None)
    run.config = config_obj
    db.commit()
    db.refresh(run)
    return _h2o_run_payload(run, include_config=True)


@app.delete("/api/mlops/h2o/runs/{run_id}", status_code=204)
async def delete_mlops_h2o_run(run_id: str, db: Session = Depends(get_db)):
    run = db.query(models.MLOpsH2ORun).filter(models.MLOpsH2ORun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="H2O run not found")

    for path in [run.model_path, run.mojo_path]:
        safe_path = str(path or "").strip()
        if not safe_path:
            continue
        try:
            if _os.path.isfile(safe_path):
                _os.remove(safe_path)
        except Exception:
            pass

    db.delete(run)
    db.commit()
    return None


@app.post("/api/mlops/h2o/train")
async def mlops_h2o_train(body: H2OAutoMLTrainRequest, db: Session = Depends(get_db)):
    run_id = str(uuid.uuid4())
    run_row = models.MLOpsH2ORun(
        id=run_id,
        workflow_id=str(body.workflow_id or "").strip() or None,
        status="running",
        source_type=str(body.source_type or "pipeline"),
        config={
            "max_models": int(body.max_models),
            "max_runtime_secs": int(body.max_runtime_secs),
            "train_ratio": float(body.train_ratio),
            "seed": int(body.seed),
            "nfolds": int(body.nfolds),
            "task": str(body.task or "auto"),
            "balance_classes": bool(body.balance_classes),
            "include_algos": body.include_algos or [],
            "exclude_algos": body.exclude_algos or [],
            "sort_metric": body.sort_metric,
            "stopping_metric": body.stopping_metric,
            "project_name": body.project_name,
            "use_workflow_preprocessing": bool(body.use_workflow_preprocessing),
        },
    )
    db.add(run_row)
    db.commit()
    db.refresh(run_row)

    try:
        source_bundle = _h2o_source_rows_from_request(db, body)
        source_rows = [r for r in source_bundle.get("rows", []) if isinstance(r, dict)]
        if len(source_rows) < 20:
            raise HTTPException(status_code=400, detail="At least 20 rows are required for H2O AutoML training")

        import pandas as pd
        from sklearn.model_selection import train_test_split

        df = _h2o_rows_to_dataframe(source_rows)
        target_column = _mlops_infer_target_column(df.to_dict(orient="records"), body.target_column)
        if not str(body.target_column or "").strip() and str(body.task or "auto").strip().lower() == "auto":
            target_column = _h2o_select_auto_target_column(df, target_column)
        task = _h2o_resolve_task(df, target_column, body.task)
        feature_columns = _h2o_resolve_feature_columns(df, target_column, body.feature_columns, body.exclude_columns)
        model_df = df[feature_columns + [target_column]].copy()
        dropped_constant_features: List[str] = []
        dropped_high_cardinality_features: List[str] = []
        stable_feature_columns: List[str] = []
        for col in feature_columns:
            series = model_df[col]
            non_null = series.dropna()
            if len(non_null) == 0:
                dropped_constant_features.append(col)
                continue
            try:
                nunique = int(non_null.astype(str).nunique())
            except Exception:
                nunique = int(non_null.nunique())
            if nunique <= 1:
                dropped_constant_features.append(col)
                continue
            # Drop ID-like text features (near-unique categorical values), which
            # frequently cause H2O to discard all predictors and yield empty leaderboards.
            try:
                numeric_ratio = float(pd.to_numeric(non_null, errors="coerce").notna().mean())
            except Exception:
                numeric_ratio = 0.0
            cardinality_ratio = float(nunique / max(len(non_null), 1))
            if numeric_ratio < 0.5 and len(non_null) >= 25 and cardinality_ratio >= 0.9:
                dropped_high_cardinality_features.append(col)
                continue
            stable_feature_columns.append(col)
        feature_columns = stable_feature_columns
        if not feature_columns:
            if dropped_high_cardinality_features:
                dropped_sample = ", ".join(dropped_high_cardinality_features[:10])
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "No usable feature columns remain after filtering. "
                        f"ID-like/high-cardinality text columns were dropped: {dropped_sample}. "
                        "Select numeric/aggregated features or exclude identifier/text columns."
                    ),
                )
            raise HTTPException(
                status_code=400,
                detail="No usable feature columns remain after removing constant/empty columns. Select different features or source data."
            )
        model_df = model_df[feature_columns + [target_column]].copy()
        model_df = model_df.replace({"": None}).dropna(subset=[target_column]).copy()
        if len(model_df) < 20:
            raise HTTPException(status_code=400, detail="At least 20 rows with non-empty target are required for H2O AutoML training")

        class_distribution: Optional[Dict[str, int]] = None
        force_nfolds_zero = False
        train_ratio = max(0.55, min(float(body.train_ratio), 0.95))
        task_auto_corrected_from: Optional[str] = None
        task_auto_correct_reason: Optional[str] = None

        stratify_labels = None
        if task == "classification":
            label_series = model_df[target_column].astype(str).str.strip()
            valid_mask = label_series != ""
            model_df = model_df.loc[valid_mask].copy()
            label_series = label_series.loc[valid_mask]
            if len(model_df) < 20:
                raise HTTPException(status_code=400, detail="Classification training needs at least 20 rows with non-empty class labels")

            class_counts = label_series.value_counts(dropna=False)
            class_distribution = {str(k): int(v) for k, v in class_counts.items()}
            class_count = len(class_distribution)
            numeric_label_ratio = float(pd.to_numeric(label_series, errors="coerce").notna().mean()) if len(label_series) else 0.0
            class_unique_ratio = float(class_count / max(len(label_series), 1))
            # Guardrail: numeric continuous targets should not be trained as classification.
            if numeric_label_ratio >= 0.95 and class_count > 15 and class_unique_ratio >= 0.35:
                task_auto_corrected_from = "classification"
                task = "regression"
                class_distribution = None
                task_auto_correct_reason = (
                    f"Target '{target_column}' looked continuous numeric "
                    f"({class_count} unique labels across {len(label_series)} rows)."
                )
                logger.warning(
                    f"H2O task auto-corrected from classification to regression for target '{target_column}' "
                    f"(numeric_ratio={numeric_label_ratio:.3f}, class_count={class_count}, unique_ratio={class_unique_ratio:.3f})."
                )
            else:
                if len(class_distribution) < 2:
                    single = next(iter(class_distribution.keys()), "unknown")
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"Target column '{target_column}' has only one class ('{single}'). "
                            "Choose a target with at least 2 classes, provide target_column explicitly, or use regression."
                        ),
                    )
                if class_count > max(80, int(len(model_df) * 0.45)):
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"Target column '{target_column}' has too many classes ({class_count}) for {len(model_df)} rows. "
                            "Use a target with repeated labels, set target_column explicitly, or switch task to regression."
                        ),
                    )
                if int(class_counts.min()) >= 2:
                    stratify_labels = label_series
                else:
                    force_nfolds_zero = True
                    logger.warning(
                        f"H2O classification target '{target_column}' has rare classes (min count < 2); "
                        "using non-stratified split."
                    )

        try:
            train_df, test_df = train_test_split(
                model_df,
                train_size=train_ratio,
                random_state=int(body.seed),
                shuffle=True,
                stratify=stratify_labels,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Unable to split training/test data: {exc}") from exc

        if int(len(train_df)) < 10:
            raise HTTPException(status_code=400, detail="Training split produced too few rows (< 10). Use larger data or adjust train_ratio.")

        if task == "classification":
            train_unique_classes = int(train_df[target_column].astype(str).str.strip().replace("", pd.NA).dropna().nunique())
            if train_unique_classes < 2:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Training split has only one class for target '{target_column}'. "
                        "Increase data, reduce train_ratio, or pick a different target column."
                    ),
                )

        h2o_mod, H2OAutoML = _h2o_import_modules()
        _h2o_init_cluster(h2o_mod)

        train_frame = h2o_mod.H2OFrame(train_df)
        full_train_frame = h2o_mod.H2OFrame(model_df)
        has_test_frame = int(len(test_df)) >= 5
        test_frame = h2o_mod.H2OFrame(test_df) if has_test_frame else None

        if task == "classification":
            train_frame[target_column] = train_frame[target_column].asfactor()
            full_train_frame[target_column] = full_train_frame[target_column].asfactor()
            if test_frame is not None:
                test_frame[target_column] = test_frame[target_column].asfactor()

        include_algos, invalid_include_algos = _h2o_normalize_algo_list(body.include_algos or [])
        exclude_algos, invalid_exclude_algos = _h2o_normalize_algo_list(body.exclude_algos or [])
        if invalid_include_algos:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unsupported include_algos: {', '.join(invalid_include_algos)}. "
                    f"Allowed values: {', '.join(_H2O_ALLOWED_ALGOS)}."
                ),
            )
        if invalid_exclude_algos:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unsupported exclude_algos: {', '.join(invalid_exclude_algos)}. "
                    f"Allowed values: {', '.join(_H2O_ALLOWED_ALGOS)}."
                ),
            )
        if include_algos and exclude_algos:
            raise HTTPException(status_code=400, detail="Provide include_algos or exclude_algos, not both")

        automl_kwargs: Dict[str, Any] = {
            "max_models": max(1, min(int(body.max_models), 500)),
            "max_runtime_secs": max(60, min(int(body.max_runtime_secs), 86400)),
            "seed": int(body.seed),
            "nfolds": 0 if force_nfolds_zero else max(0, min(int(body.nfolds), 20)),
            "balance_classes": bool(body.balance_classes),
            "project_name": str(body.project_name or f"mlops_h2o_{run_id[:8]}"),
        }
        if include_algos:
            automl_kwargs["include_algos"] = include_algos
        if exclude_algos:
            automl_kwargs["exclude_algos"] = exclude_algos
        if body.sort_metric:
            automl_kwargs["sort_metric"] = str(body.sort_metric).strip()
        if body.stopping_metric:
            automl_kwargs["stopping_metric"] = str(body.stopping_metric).strip()

        retry_used = False
        rescue_used = False
        retry_reason = ""
        failure_diagnostics: List[str] = []
        automl, train_started, train_finished, train_error = _h2o_train_automl_once(
            H2OAutoML,
            automl_kwargs,
            feature_columns,
            target_column,
            train_frame,
            test_frame,
        )
        initial_leader = _h2o_resolve_leader_model(h2o_mod, automl)
        if train_error is not None and initial_leader is not None:
            failure_diagnostics = _h2o_event_log_messages(automl, limit=20)
            failure_diagnostics.append(f"WARN | AutoML | Initial run returned warning: {str(train_error)[:260]}")

        if initial_leader is None:
            if train_error is not None:
                first_error = str(train_error)
            else:
                first_error = "AutoML completed but no leader model was produced."
            if not failure_diagnostics:
                failure_diagnostics = _h2o_event_log_messages(automl, limit=20)
            first_error_l = first_error.lower()
            auto_retry = (
                "too many consecutive model failures" in first_error_l
                or "unable to build any models" in first_error_l
                or initial_leader is None
            )
            if not auto_retry:
                details = " ; ".join(failure_diagnostics[:6]) if failure_diagnostics else first_error
                raise HTTPException(status_code=400, detail=f"H2O AutoML failed. Diagnostics: {details[:1200]}")

            retry_used = True
            retry_reason = "consecutive_model_failures" if train_error is not None else "no_leader_model"
            fallback_kwargs: Dict[str, Any] = dict(automl_kwargs)
            fallback_kwargs.pop("include_algos", None)
            fallback_kwargs["nfolds"] = 0
            fallback_kwargs["max_models"] = max(3, min(int(fallback_kwargs.get("max_models", 20)), 25))
            fallback_kwargs["max_runtime_secs"] = max(120, min(int(fallback_kwargs.get("max_runtime_secs", 300)), 1200))
            fallback_excludes = {str(a).strip() for a in (exclude_algos or []) if str(a).strip()}
            fallback_excludes.update({"DeepLearning", "StackedEnsemble"})
            if initial_leader is None:
                fallback_kwargs["include_algos"] = ["GLM", "GBM"]
                fallback_kwargs.pop("exclude_algos", None)
            elif fallback_excludes:
                fallback_kwargs["exclude_algos"] = sorted(fallback_excludes)
            else:
                fallback_kwargs.pop("exclude_algos", None)
            if task == "classification":
                fallback_kwargs["balance_classes"] = False

            automl_retry, retry_started, retry_finished, retry_error = _h2o_train_automl_once(
                H2OAutoML,
                fallback_kwargs,
                feature_columns,
                target_column,
                train_frame,
                test_frame,
            )
            retry_leader = _h2o_resolve_leader_model(h2o_mod, automl_retry)
            failure_diagnostics.extend(_h2o_event_log_messages(automl_retry, limit=20))
            if retry_leader is not None:
                if retry_error is not None:
                    failure_diagnostics.append(f"WARN | AutoML | Safe retry returned warning: {str(retry_error)[:260]}")
                    retry_reason = f"{retry_reason}_with_warning"
                automl = automl_retry
                train_started = retry_started
                train_finished = retry_finished
                automl_kwargs = fallback_kwargs
            else:
                retry_msg = str(retry_error) if retry_error is not None else "Safe retry completed but no leader model was produced."
                # Final rescue path for small/sparse datasets:
                # force a simple GLM on full data with no leaderboard frame.
                rescue_kwargs: Dict[str, Any] = dict(fallback_kwargs)
                rescue_kwargs["include_algos"] = ["GLM"]
                rescue_kwargs.pop("exclude_algos", None)
                rescue_kwargs["nfolds"] = 0
                rescue_kwargs["max_models"] = 1
                rescue_kwargs["max_runtime_secs"] = max(
                    90, min(int(rescue_kwargs.get("max_runtime_secs", 180)), 900)
                )
                if task == "classification":
                    rescue_kwargs["balance_classes"] = False

                rescue_automl, rescue_started, rescue_finished, rescue_error = _h2o_train_automl_once(
                    H2OAutoML,
                    rescue_kwargs,
                    feature_columns,
                    target_column,
                    full_train_frame,
                    None,
                )
                rescue_leader = _h2o_resolve_leader_model(h2o_mod, rescue_automl)
                failure_diagnostics.extend(_h2o_event_log_messages(rescue_automl, limit=20))
                if rescue_leader is None:
                    rescue_msg = str(rescue_error) if rescue_error is not None else "Final GLM rescue did not produce a leader model."
                    details = " ; ".join(failure_diagnostics[:10]) if failure_diagnostics else f"{retry_msg} ; {rescue_msg}"
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "H2O AutoML failed after safe retry and GLM rescue. "
                            f"Diagnostics: {details[:1600]}"
                        ),
                    )

                rescue_used = True
                if rescue_error is not None:
                    failure_diagnostics.append(f"WARN | AutoML | GLM rescue returned warning: {str(rescue_error)[:260]}")
                automl = rescue_automl
                train_started = rescue_started
                train_finished = rescue_finished
                automl_kwargs = rescue_kwargs
                train_frame = full_train_frame
                test_frame = None
                retry_reason = f"{retry_reason}_glm_rescue"

        leader = _h2o_resolve_leader_model(h2o_mod, automl)
        if leader is None:
            details = " ; ".join(_h2o_event_log_messages(automl, limit=8))
            if details:
                raise HTTPException(
                    status_code=400,
                    detail=f"H2O AutoML did not produce a leader model. Diagnostics: {details[:1200]}"
                )
            raise HTTPException(status_code=400, detail="H2O AutoML did not produce a leader model")

        if test_frame is not None:
            perf = leader.model_performance(test_data=test_frame)
        else:
            perf = leader.model_performance()
        metrics = _h2o_metric_payload(perf, task)
        metrics["training_seconds"] = round(float((train_finished - train_started).total_seconds()), 3)
        metrics["leader_model_id"] = str(getattr(leader, "model_id", "") or "")
        if class_distribution:
            metrics["class_distribution"] = class_distribution
        if dropped_constant_features:
            metrics["dropped_constant_features"] = dropped_constant_features
        if dropped_high_cardinality_features:
            metrics["dropped_high_cardinality_features"] = dropped_high_cardinality_features
        if retry_used:
            metrics["automl_retry_used"] = True
            metrics["automl_retry_reason"] = retry_reason
        if rescue_used:
            metrics["automl_rescue_used"] = True
        if task_auto_corrected_from:
            metrics["task_auto_corrected_from"] = task_auto_corrected_from
        if task_auto_correct_reason:
            metrics["task_auto_correct_reason"] = task_auto_correct_reason
        if failure_diagnostics:
            metrics["automl_failure_diagnostics"] = failure_diagnostics[:6]

        leaderboard_rows = _h2o_leaderboard_payload(automl.leaderboard)
        feature_importance = _h2o_feature_importance(leader)

        preview_frame = test_frame if test_frame is not None else train_frame
        pred_preview_df = leader.predict(preview_frame).as_data_frame(use_pandas=True)
        pred_preview_rows = _mlops_sanitize_sample_rows(
            pred_preview_df.head(25).to_dict(orient="records") if hasattr(pred_preview_df, "to_dict") else [],
            limit=25,
        )

        model_path = h2o_mod.save_model(model=leader, path=str(H2O_MODELS_DIR), force=True)
        mojo_path = None
        try:
            mojo_path = leader.download_mojo(path=str(H2O_MOJO_DIR), get_genmodel_jar=False)
        except Exception:
            mojo_path = None

        run_row.status = "success"
        run_row.finished_at = datetime.utcnow()
        run_row.source_meta = source_bundle.get("source", {})
        run_row.task = task
        run_row.target_column = target_column
        run_row.feature_columns = feature_columns
        run_row.row_count = int(len(model_df))
        run_row.train_rows = int(train_frame.nrows or 0)
        run_row.test_rows = int(test_frame.nrows or 0) if test_frame is not None else 0
        run_row.model_id = str(getattr(leader, "model_id", "") or "")
        run_row.model_path = str(model_path)
        run_row.mojo_path = str(mojo_path) if mojo_path else None
        run_row.leaderboard = leaderboard_rows
        run_row.metrics = metrics
        run_cfg = dict(run_row.config or {})
        run_cfg["effective_automl_kwargs"] = _h2o_json_safe(automl_kwargs)
        run_cfg["retry_used"] = retry_used
        run_cfg["rescue_used"] = rescue_used
        run_cfg["dropped_constant_features"] = dropped_constant_features
        run_cfg["dropped_high_cardinality_features"] = dropped_high_cardinality_features
        if task_auto_corrected_from:
            run_cfg["task_auto_corrected_from"] = task_auto_corrected_from
        if task_auto_correct_reason:
            run_cfg["task_auto_correct_reason"] = task_auto_correct_reason
        run_row.config = run_cfg
        db.commit()
        db.refresh(run_row)

        response_payload: Dict[str, Any] = {
            "status": "success",
            "run_id": run_row.id,
            "source": run_row.source_meta or {},
            "task": task,
            "target_column": target_column,
            "feature_columns": feature_columns,
            "row_count": int(run_row.row_count or 0),
            "train_rows": int(run_row.train_rows or 0),
            "test_rows": int(run_row.test_rows or 0),
            "used_leaderboard_frame": bool(test_frame is not None),
            "retry_used": retry_used,
            "rescue_used": rescue_used,
            "dropped_features": dropped_constant_features,
            "dropped_high_cardinality_features": dropped_high_cardinality_features,
            "model": {
                "model_id": run_row.model_id,
                "model_path": run_row.model_path,
                "mojo_path": run_row.mojo_path,
            },
            "metrics": metrics,
            "feature_importance": feature_importance,
            "leaderboard": leaderboard_rows,
            "prediction_preview": pred_preview_rows,
            "endpoints": {
                "single_predict": "/api/mlops/h2o/predict/single",
                "batch_predict": "/api/mlops/h2o/predict/batch",
                "evaluate": "/api/mlops/h2o/evaluate",
                "run_detail": f"/api/mlops/h2o/runs/{run_row.id}",
            },
            "trained_at": datetime.utcnow().isoformat(),
        }
        if task_auto_corrected_from:
            response_payload["task_auto_corrected_from"] = task_auto_corrected_from
        if task_auto_correct_reason:
            response_payload["task_auto_correct_reason"] = task_auto_correct_reason
        return response_payload
    except HTTPException as exc:
        run_row.status = "failed"
        run_row.finished_at = datetime.utcnow()
        run_row.error_message = str(exc.detail)
        db.commit()
        raise
    except Exception as exc:
        message = str(exc)
        if "Number of classes is equal to 1" in message:
            inferred_target = locals().get("target_column") or (body.target_column or "target")
            message = (
                f"Target column '{inferred_target}' has only one class after preprocessing/split. "
                "Choose a different target column with at least 2 classes, increase data volume, or switch task to regression."
            )
        if "too many consecutive model failures" in message.lower():
            message = (
                "H2O AutoML aborted after repeated model failures. "
                "Try explicit target_column, remove high-cardinality text fields, reduce feature set, or use regression task."
            )
        run_row.status = "failed"
        run_row.finished_at = datetime.utcnow()
        run_row.error_message = message
        db.commit()
        logger.error(f"H2O AutoML training failed: {exc}")
        if "Number of classes is equal to 1" in str(exc) or "too many consecutive model failures" in str(exc).lower():
            raise HTTPException(status_code=400, detail=message) from exc
        raise HTTPException(status_code=500, detail=f"H2O AutoML training failed: {message}") from exc


@app.post("/api/mlops/h2o/predict/single")
async def mlops_h2o_predict_single(body: H2OModelPredictSingleRequest, db: Session = Depends(get_db)):
    if not isinstance(body.row, dict) or len(body.row) == 0:
        raise HTTPException(status_code=400, detail="row payload is required for single prediction")

    h2o_mod, _ = _h2o_import_modules()
    _h2o_init_cluster(h2o_mod)

    run_row, resolved_path = _h2o_resolve_model_reference(db, body.run_id, body.model_path)
    model = _h2o_load_model(h2o_mod, resolved_path)

    feature_columns = [str(c) for c in (run_row.feature_columns or [])] if run_row and isinstance(run_row.feature_columns, list) else []
    input_row = dict(body.row)
    if feature_columns:
        rows = [{col: input_row.get(col) for col in feature_columns}]
    else:
        rows = [input_row]
    try:
        predict_frame = _h2o_prepare_prediction_frame(h2o_mod, model, rows, feature_columns)
        pred_df = model.predict(predict_frame).as_data_frame(use_pandas=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"H2O single prediction failed: {_h2o_friendly_scoring_error(exc)}") from exc

    pred_row = {}
    if hasattr(pred_df, "to_dict") and len(pred_df) > 0:
        pred_row = {str(k): _h2o_json_safe(v) for k, v in pred_df.iloc[0].to_dict().items()}
    merged_row = {str(k): _h2o_json_safe(v) for k, v in input_row.items()}
    for pk, pv in pred_row.items():
        merged_row[f"_prediction_{pk}"] = _h2o_json_safe(pv)

    return {
        "status": "success",
        "run_id": run_row.id if run_row else None,
        "model_path": resolved_path,
        "model_id": str(getattr(model, "model_id", "") or (run_row.model_id if run_row else "")),
        "input": {str(k): _h2o_json_safe(v) for k, v in input_row.items()},
        "prediction": pred_row,
        "prediction_preview": [merged_row],
        "predicted_at": datetime.utcnow().isoformat(),
    }


@app.post("/api/mlops/h2o/predict/batch")
async def mlops_h2o_predict_batch(body: H2OModelPredictBatchRequest, db: Session = Depends(get_db)):
    h2o_mod, _ = _h2o_import_modules()
    _h2o_init_cluster(h2o_mod)

    run_row, resolved_path = _h2o_resolve_model_reference(db, body.run_id, body.model_path)
    model = _h2o_load_model(h2o_mod, resolved_path)

    source_bundle = _h2o_source_rows_from_request(db, body)
    source_rows = [r for r in source_bundle.get("rows", []) if isinstance(r, dict)]
    if not source_rows:
        raise HTTPException(status_code=400, detail="Resolved source has no rows for batch prediction")

    max_rows = max(1, min(int(body.max_rows), 200000))
    truncated = 0
    if len(source_rows) > max_rows:
        truncated = len(source_rows) - max_rows
        source_rows = source_rows[:max_rows]

    feature_columns = [str(c) for c in (run_row.feature_columns or [])] if run_row and isinstance(run_row.feature_columns, list) else []
    try:
        predict_frame = _h2o_prepare_prediction_frame(h2o_mod, model, source_rows, feature_columns)
        pred_df = model.predict(predict_frame).as_data_frame(use_pandas=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"H2O batch prediction failed: {_h2o_friendly_scoring_error(exc)}") from exc

    pred_records = pred_df.to_dict(orient="records") if hasattr(pred_df, "to_dict") else []
    predictions = [{str(k): _h2o_json_safe(v) for k, v in row.items()} for row in pred_records]

    enriched_rows = []
    for idx, row in enumerate(source_rows):
        merged = {str(k): _h2o_json_safe(v) for k, v in row.items()}
        pred = predictions[idx] if idx < len(predictions) else {}
        for pk, pv in pred.items():
            merged[f"_prediction_{pk}"] = pv
        enriched_rows.append(merged)

    response_predictions = predictions if len(predictions) <= 5000 else predictions[:5000]
    predictions_truncated = len(predictions) - len(response_predictions)
    response_enriched_rows = enriched_rows if len(enriched_rows) <= 5000 else enriched_rows[:5000]
    enriched_rows_truncated = len(enriched_rows) - len(response_enriched_rows)

    return {
        "status": "success",
        "run_id": run_row.id if run_row else None,
        "model_path": resolved_path,
        "source": source_bundle.get("source", {}),
        "row_count": len(source_rows),
        "input_truncated_rows": truncated,
        "predictions_count": len(predictions),
        "predictions": response_predictions,
        "predictions_truncated": predictions_truncated,
        "enriched_rows": _h2o_json_safe(response_enriched_rows),
        "enriched_rows_truncated": enriched_rows_truncated,
        "enriched_preview": _mlops_sanitize_sample_rows(enriched_rows, limit=200),
        "predicted_at": datetime.utcnow().isoformat(),
    }


@app.post("/api/mlops/h2o/evaluate")
async def mlops_h2o_evaluate(body: H2OModelEvaluateRequest, db: Session = Depends(get_db)):
    h2o_mod, _ = _h2o_import_modules()
    _h2o_init_cluster(h2o_mod)

    run_row, resolved_path = _h2o_resolve_model_reference(db, body.run_id, body.model_path)
    model = _h2o_load_model(h2o_mod, resolved_path)

    source_bundle = _h2o_source_rows_from_request(db, body)
    rows = [r for r in source_bundle.get("rows", []) if isinstance(r, dict)]
    if not rows:
        raise HTTPException(status_code=400, detail="Resolved source has no rows for evaluation")

    max_rows = max(1, min(int(body.max_rows), 200000))
    if len(rows) > max_rows:
        rows = rows[:max_rows]

    all_df = _h2o_rows_to_dataframe(rows)
    target_column = str(body.target_column or (run_row.target_column if run_row else "") or "").strip() or None
    feature_columns = [str(c) for c in (run_row.feature_columns or [])] if run_row and isinstance(run_row.feature_columns, list) else []
    if not feature_columns:
        feature_columns = [str(c) for c in all_df.columns if str(c) != target_column]
    if not feature_columns:
        raise HTTPException(status_code=400, detail="No feature columns available for evaluation")

    try:
        predict_frame = _h2o_prepare_prediction_frame(h2o_mod, model, rows, feature_columns)
        pred_df = model.predict(predict_frame).as_data_frame(use_pandas=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"H2O evaluation prediction failed: {_h2o_friendly_scoring_error(exc)}") from exc
    pred_records = pred_df.to_dict(orient="records") if hasattr(pred_df, "to_dict") else []
    pred_records = [{str(k): _h2o_json_safe(v) for k, v in row.items()} for row in pred_records]
    task = str((run_row.task if run_row else None) or "auto").lower()
    if task not in {"classification", "regression"} and target_column and target_column in all_df.columns:
        task = _h2o_resolve_task(all_df, target_column, "auto")
    elif task not in {"classification", "regression"}:
        task = "classification"
    actual_vs_predicted_rows: List[Dict[str, Any]] = []
    evaluation_enriched_rows: List[Dict[str, Any]] = []
    limit_n = min(len(rows), len(pred_records))
    for idx in range(limit_n):
        src_row = rows[idx] if idx < len(rows) else {}
        pred_row = pred_records[idx] if idx < len(pred_records) else {}
        predicted_value = pred_row.get("predict")
        if predicted_value is None and target_column and target_column in pred_row:
            predicted_value = pred_row.get(target_column)
        if predicted_value is None and pred_row:
            first_key = next(iter(pred_row.keys()))
            predicted_value = pred_row.get(first_key)
        actual_value = src_row.get(target_column) if target_column and isinstance(src_row, dict) else None

        merged_row = {str(k): _h2o_json_safe(v) for k, v in (src_row.items() if isinstance(src_row, dict) else [])}
        for pk, pv in (pred_row or {}).items():
            merged_row[f"_prediction_{str(pk)}"] = _h2o_json_safe(pv)
        merged_row["_evaluation_actual"] = _h2o_json_safe(actual_value)
        merged_row["_evaluation_predicted"] = _h2o_json_safe(predicted_value)
        merged_row["_evaluation_index"] = idx + 1

        row_out = {
            "index": idx + 1,
            "actual": _h2o_json_safe(actual_value),
            "predicted": _h2o_json_safe(predicted_value),
        }
        if task == "classification":
            matched = str(row_out["actual"]) == str(row_out["predicted"])
            row_out["matched"] = matched
            merged_row["_evaluation_matched"] = matched

        actual_vs_predicted_rows.append(row_out)
        evaluation_enriched_rows.append(merged_row)

    response_avp_rows = actual_vs_predicted_rows if len(actual_vs_predicted_rows) <= 5000 else actual_vs_predicted_rows[:5000]
    avp_rows_truncated = len(actual_vs_predicted_rows) - len(response_avp_rows)
    response_enriched_rows = evaluation_enriched_rows if len(evaluation_enriched_rows) <= 5000 else evaluation_enriched_rows[:5000]
    enriched_rows_truncated = len(evaluation_enriched_rows) - len(response_enriched_rows)

    python_metrics: Dict[str, Any] = {}
    h2o_metrics: Dict[str, Any] = {}

    if target_column and target_column in all_df.columns:
        python_metrics = _h2o_python_eval_metrics(task, all_df[target_column], pred_df)
        try:
            eval_df = all_df[feature_columns + [target_column]].copy()
            eval_frame = h2o_mod.H2OFrame(eval_df)
            eval_frame = _h2o_align_frame_to_model_schema(eval_frame, model, feature_columns)
            if task == "classification":
                eval_frame[target_column] = eval_frame[target_column].ascharacter().asfactor()
            elif task == "regression":
                eval_frame[target_column] = eval_frame[target_column].asnumeric()
            perf = model.model_performance(eval_frame)
            h2o_metrics = _h2o_metric_payload(perf, task)
        except Exception:
            h2o_metrics = {}

    output_field_set = set()
    for row in response_enriched_rows[:5000]:
        if isinstance(row, dict):
            output_field_set.update(str(key) for key in row.keys())
    output_fields = sorted([field for field in output_field_set if field])
    evaluated_at = datetime.utcnow().isoformat()

    if run_row is not None:
        run_config = dict(run_row.config or {}) if isinstance(run_row.config, dict) else {}
        run_config["last_evaluation"] = _h2o_json_safe({
            "evaluated_at": evaluated_at,
            "task": task,
            "target_column": target_column,
            "feature_columns": feature_columns,
            "row_count": int(len(rows)),
            "source": source_bundle.get("source", {}),
            "metrics": {
                "python": python_metrics,
                "h2o": h2o_metrics,
            },
            "output_fields": output_fields,
            "actual_vs_predicted_preview": response_avp_rows,
            "enriched_rows": response_enriched_rows,
            "prediction_preview": _mlops_sanitize_sample_rows(pred_records, limit=5000),
        })
        run_row.config = run_config
        base_metrics = dict(run_row.metrics or {}) if isinstance(run_row.metrics, dict) else {}
        base_metrics["last_evaluation"] = {
            "evaluated_at": evaluated_at,
            "task": task,
            "target_column": target_column,
            "row_count": int(len(rows)),
            "python": python_metrics,
            "h2o": h2o_metrics,
        }
        run_row.metrics = _h2o_json_safe(base_metrics)
        db.commit()
        db.refresh(run_row)

    return {
        "status": "success",
        "run_id": run_row.id if run_row else None,
        "model_path": resolved_path,
        "task": task,
        "source": source_bundle.get("source", {}),
        "target_column": target_column,
        "feature_columns": feature_columns,
        "row_count": int(len(rows)),
        "metrics": {
            "python": python_metrics,
            "h2o": h2o_metrics,
        },
        "enriched_rows": _h2o_json_safe(response_enriched_rows),
        "enriched_rows_truncated": enriched_rows_truncated,
        "enriched_preview": _mlops_sanitize_sample_rows(evaluation_enriched_rows, limit=200),
        "prediction_preview": _mlops_sanitize_sample_rows(pred_records, limit=200),
        "output_fields": output_fields,
        "model_label": _h2o_run_label(run_row.config if run_row else {}),
        "actual_vs_predicted_preview": _h2o_json_safe(response_avp_rows),
        "actual_vs_predicted_truncated": avp_rows_truncated,
        "evaluated_at": evaluated_at,
    }


# ─── BUSINESS LOGIC WORKFLOWS ────────────────────────────────────────────────

def _business_topological_order(nodes: list, edges: list) -> list:
    node_ids = [str(n.get("id")) for n in nodes if isinstance(n, dict) and n.get("id")]
    if not node_ids:
        return []
    adj: Dict[str, List[str]] = {nid: [] for nid in node_ids}
    in_deg: Dict[str, int] = {nid: 0 for nid in node_ids}
    for e in edges or []:
        if not isinstance(e, dict):
            continue
        src = str(e.get("source") or "")
        tgt = str(e.get("target") or "")
        if src in adj and tgt in in_deg:
            adj[src].append(tgt)
            in_deg[tgt] += 1

    queue = [nid for nid, deg in in_deg.items() if deg == 0]
    order: List[str] = []
    while queue:
        nid = queue.pop(0)
        order.append(nid)
        for nxt in adj.get(nid, []):
            in_deg[nxt] -= 1
            if in_deg[nxt] == 0:
                queue.append(nxt)

    for nid in node_ids:
        if nid not in order:
            order.append(nid)
    return order


def _business_node_type(node: dict) -> str:
    node_data = node.get("data", {}) if isinstance(node.get("data", {}), dict) else {}
    return str(node_data.get("nodeType") or node.get("type") or "")


def _business_node_label(node: dict) -> str:
    node_data = node.get("data", {}) if isinstance(node.get("data", {}), dict) else {}
    return str(node_data.get("label") or _business_node_type(node) or "Business Node")


def _business_node_config(node: dict) -> Dict[str, Any]:
    node_data = node.get("data", {}) if isinstance(node.get("data", {}), dict) else {}
    cfg = node_data.get("config")
    return dict(cfg) if isinstance(cfg, dict) else {}


def _business_source_rows_from_pipeline(db: Session, config: Dict[str, Any]) -> List[dict]:
    pipeline_id = str(config.get("pipeline_id") or "").strip()
    if not pipeline_id:
        return []
    pipeline_node_id = str(config.get("pipeline_node_id") or "").strip() or None

    exec_row = db.query(models.Execution).filter(
        models.Execution.pipeline_id == pipeline_id,
        models.Execution.status == "success",
    ).order_by(models.Execution.started_at.desc()).first()
    if not exec_row or not exec_row.node_results:
        return []

    rows = _extract_rows_from_pipeline_execution(exec_row.node_results, pipeline_node_id)
    return [r for r in rows if isinstance(r, dict)]


def _business_rows_from_mlops_run(run: models.MLOpsRun, node_id: Optional[str] = None) -> List[dict]:
    logs = [log for log in (run.logs or []) if isinstance(log, dict)]
    candidates = []
    for log in logs:
        if node_id and str(log.get("nodeId") or "") != node_id:
            continue
        sample = log.get("output_sample")
        if isinstance(sample, list) and sample:
            score = _mlops_prediction_log_score(log)
            candidates.append((score, log))

    if candidates:
        selected = sorted(candidates, key=lambda item: item[0], reverse=True)[0][1]
        rows = selected.get("output_sample") or []
        return [r for r in rows if isinstance(r, dict)]

    metrics = run.metrics if isinstance(run.metrics, dict) else {}
    if metrics:
        return [dict(metrics)]
    return []


def _business_source_rows_from_mlops(db: Session, config: Dict[str, Any]) -> List[dict]:
    workflow_id = str(config.get("mlops_workflow_id") or config.get("workflow_id") or "").strip()
    if not workflow_id:
        return []
    run_id = str(config.get("mlops_run_id") or config.get("run_id") or "").strip() or None
    node_id = str(config.get("mlops_node_id") or config.get("node_id") or "").strip() or None

    run_query = db.query(models.MLOpsRun).filter(models.MLOpsRun.workflow_id == workflow_id)
    run_row = None
    if run_id:
        run_row = run_query.filter(models.MLOpsRun.id == run_id).first()
    if not run_row:
        run_row = run_query.filter(models.MLOpsRun.status == "success")\
            .order_by(models.MLOpsRun.started_at.desc()).first()
    if not run_row:
        run_row = run_query.order_by(models.MLOpsRun.started_at.desc()).first()
    if not run_row:
        return []

    return _business_rows_from_mlops_run(run_row, node_id)


def _business_to_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _business_eval_condition(left: Any, operator: str, right: Any, case_sensitive: bool = False) -> bool:
    op = str(operator or "contains").strip().lower()
    if op in {"gt", "gte", "lt", "lte"}:
        left_num = _business_to_number(left)
        right_num = _business_to_number(right)
        if left_num is None or right_num is None:
            return False
        if op == "gt":
            return left_num > right_num
        if op == "gte":
            return left_num >= right_num
        if op == "lt":
            return left_num < right_num
        return left_num <= right_num

    left_text = "" if left is None else str(left)
    right_text = "" if right is None else str(right)
    if not case_sensitive:
        left_text = left_text.lower()
        right_text = right_text.lower()

    if op == "equals":
        return left_text == right_text
    if op == "not_equals":
        return left_text != right_text
    if op == "starts_with":
        return left_text.startswith(right_text)
    if op == "ends_with":
        return left_text.endswith(right_text)
    if op == "regex":
        import re
        try:
            return bool(re.search(right_text, left_text))
        except Exception:
            return False
    return right_text in left_text


def _business_apply_decision(
    config: Dict[str, Any],
    input_rows: List[dict],
    llm_text: str,
) -> Dict[str, Any]:
    field_name = str(config.get("field_name") or "").strip()
    operator = str(config.get("operator") or "contains").strip().lower()
    compare_value = config.get("compare_value")
    case_sensitive = bool(config.get("case_sensitive", False))
    true_mode = str(config.get("true_mode") or "filter_rows").strip().lower()
    false_mode = str(config.get("false_mode") or "drop").strip().lower()

    matched_rows: List[dict] = []
    inspected = input_rows if input_rows else [{}]

    for row in inspected:
        left_value: Any
        if field_name:
            left_value = row.get(field_name) if isinstance(row, dict) else None
        elif llm_text:
            left_value = llm_text
        else:
            left_value = json.dumps(row, default=str) if isinstance(row, dict) else str(row)

        if _business_eval_condition(left_value, operator, compare_value, case_sensitive):
            if isinstance(row, dict):
                matched_rows.append(row)

    passed = len(matched_rows) > 0
    if passed and true_mode == "emit_summary":
        output_rows = [{
            "decision": "true",
            "matched_rows": len(matched_rows),
            "condition": {
                "field": field_name or "llm_response",
                "operator": operator,
                "value": compare_value,
            },
        }]
    elif passed:
        output_rows = matched_rows if matched_rows else input_rows
    else:
        if false_mode == "pass_through":
            output_rows = input_rows
        elif false_mode == "emit_summary":
            output_rows = [{
                "decision": "false",
                "matched_rows": 0,
                "condition": {
                    "field": field_name or "llm_response",
                    "operator": operator,
                    "value": compare_value,
                },
            }]
        else:
            output_rows = []

    summary_text = (
        f"Decision {'passed' if passed else 'failed'} "
        f"({len(matched_rows)} / {len(inspected)} matched)"
    )
    return {
        "passed": passed,
        "matched_rows": len(matched_rows),
        "inspected_rows": len(inspected),
        "output_rows": [r for r in output_rows if isinstance(r, dict)],
        "summary_text": summary_text,
    }


def _business_make_analytics_rows(rows: List[dict]) -> List[dict]:
    clean_rows = [r for r in rows if isinstance(r, dict)]
    if not clean_rows:
        return [{
            "summary": "No rows available for analytics.",
            "row_count": 0,
            "column_count": 0,
        }]

    columns = list(clean_rows[0].keys())
    numeric_columns: Dict[str, List[float]] = {}
    for col in columns:
        vals: List[float] = []
        for row in clean_rows:
            val = _business_to_number(row.get(col))
            if val is not None:
                vals.append(val)
        if vals:
            numeric_columns[col] = vals

    summary = {
        "summary": (
            f"Processed {len(clean_rows)} rows across {len(columns)} columns; "
            f"{len(numeric_columns)} numeric columns profiled."
        ),
        "row_count": len(clean_rows),
        "column_count": len(columns),
        "numeric_column_count": len(numeric_columns),
    }

    analytics_rows = [summary]
    for col, values in list(numeric_columns.items())[:8]:
        col_sum = float(sum(values))
        col_avg = col_sum / max(len(values), 1)
        analytics_rows.append({
            "metric": col,
            "count": len(values),
            "min": round(min(values), 6),
            "max": round(max(values), 6),
            "avg": round(col_avg, 6),
            "sum": round(col_sum, 6),
        })
    return analytics_rows


def _business_compact_json_rows(rows: List[dict], limit: int = 5) -> str:
    sample = [r for r in rows if isinstance(r, dict)][: max(1, limit)]
    if not sample:
        return "[]"
    try:
        return json.dumps(sample, ensure_ascii=False, default=str, indent=2)
    except Exception:
        return "[]"


def _business_build_prompt(config: Dict[str, Any], input_rows: List[dict], upstream_text: str) -> str:
    prompt = str(config.get("prompt") or "").strip()
    if not prompt:
        prompt = "Analyze the incoming business data and propose next best actions."
    include_rows = bool(config.get("include_sample_rows", True))
    sample_limit = int(config.get("sample_row_limit") or 3)
    sample_limit = max(1, min(sample_limit, 3))

    parts = [prompt]
    if upstream_text.strip():
        parts.append("Upstream context:\n" + upstream_text.strip())
    if input_rows:
        columns: List[str] = []
        for row in input_rows[:50]:
            if not isinstance(row, dict):
                continue
            for key in row.keys():
                key_text = str(key)
                if key_text not in columns:
                    columns.append(key_text)
        shown_cols = ", ".join(columns[:12]) if columns else "n/a"
        if len(columns) > 12:
            shown_cols += ", ..."
        parts.append(f"Input data profile: rows={len(input_rows)}, columns=[{shown_cols}]")
    if include_rows and input_rows:
        parts.append("Input sample rows (JSON):\n" + _business_compact_json_rows(input_rows, sample_limit))
    return "\n\n".join(parts).strip()


def _business_ollama_base_url() -> str:
    value = str(
        _os.getenv("OLLAMA_BASE_URL")
        or _os.getenv("OLLAMA_HOST")
        or "http://localhost:11434"
    ).strip()
    return value.rstrip("/")


def _business_prompt_fallback(prompt: str, reason: Optional[str] = None) -> str:
    compact = " ".join(str(prompt or "").strip().split())
    if not compact:
        return "No prompt content provided. Define objective and expected action."
    if len(compact) > 260:
        compact = compact[:260].rstrip() + "..."
    if reason:
        return f"Model call failed: {reason}. Prompt context: '{compact}'."
    return f"Model call returned no content. Prompt context: '{compact}'."


def _business_extract_sample_rows_from_prompt(prompt: str) -> List[dict]:
    text = str(prompt or "")
    marker = "Input sample rows (JSON):"
    marker_pos = text.find(marker)
    if marker_pos >= 0:
        text = text[marker_pos + len(marker):]
    text = text.strip()
    start = text.find("[")
    if start < 0:
        return []
    depth = 0
    end = -1
    for idx, ch in enumerate(text[start:], start=start):
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                end = idx
                break
    if end <= start:
        return []
    candidate = text[start:end + 1]
    try:
        parsed = json.loads(candidate)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [row for row in parsed if isinstance(row, dict)]


def _business_prompt_without_sample_rows(prompt: str) -> str:
    text = str(prompt or "").strip()
    marker = "Input sample rows (JSON):"
    marker_pos = text.find(marker)
    if marker_pos >= 0:
        text = text[:marker_pos].strip()
    return text


def _business_is_email_prompt(prompt: str) -> bool:
    lowered = _business_prompt_without_sample_rows(prompt).lower()
    if not lowered:
        return False
    strong_patterns = (
        "write email",
        "write an email",
        "write a email",
        "draft email",
        "compose email",
        "email draft",
        "mail draft",
        "send email",
    )
    if any(token in lowered for token in strong_patterns):
        return True
    return "email" in lowered and any(token in lowered for token in ("write", "draft", "compose", "send"))


def _business_email_subject_from_prompt(prompt: str) -> str:
    clean = _business_prompt_without_sample_rows(prompt)
    clean = re.sub(r"\b(write|draft|compose|send)\b", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\b(an?|the)\s+email\b", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\bemail\b", "", clean, flags=re.IGNORECASE)
    clean = " ".join(clean.split()).strip(" -:,.")
    if not clean:
        return "Business Update"
    subject = clean[0].upper() + clean[1:] if len(clean) > 1 else clean.upper()
    return subject[:90]


def _business_email_from_context(prompt: str, input_rows: List[dict], upstream_text: str) -> str:
    rows = [r for r in input_rows if isinstance(r, dict)] or _business_extract_sample_rows_from_prompt(prompt)
    subject = _business_email_subject_from_prompt(prompt)

    lines: List[str] = [f"Subject: {subject}", "", "Body:", "Hi Team,", ""]

    base_instruction = _business_prompt_without_sample_rows(prompt)
    base_instruction = " ".join(base_instruction.split()).strip()
    if base_instruction:
        lines.append(f"Please find the requested update regarding: {base_instruction}.")
    else:
        lines.append("Please find the requested update below.")

    if rows:
        lines.append("")
        lines.append("Key points:")
        lines.append(f"- Records considered: {len(rows)}")
        first = rows[0]
        shown = 0
        for key, value in first.items():
            if shown >= 5:
                break
            if isinstance(value, (list, dict)):
                continue
            rendered = "" if value is None else str(value)
            rendered = " ".join(rendered.split())
            if not rendered:
                continue
            if len(rendered) > 80:
                rendered = rendered[:77] + "..."
            lines.append(f"- {key}: {rendered}")
            shown += 1

    upstream = " ".join(str(upstream_text or "").split()).strip()
    if upstream:
        if len(upstream) > 180:
            upstream = upstream[:177] + "..."
        lines.append("")
        lines.append(f"Additional context: {upstream}")

    lines.extend(["", "Regards,", "Business Workflow Assistant"])
    return "\n".join(lines)


def _business_to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on", "enabled", "send"}:
        return True
    if text in {"0", "false", "no", "n", "off", "disabled", "draft"}:
        return False
    return default


def _business_parse_recipient_list(raw_value: str) -> List[str]:
    text = str(raw_value or "").strip()
    if not text:
        return []
    parts = re.split(r"[;,]", text)
    recipients = [part.strip() for part in parts if part and part.strip()]
    # Preserve order and de-duplicate.
    unique: List[str] = []
    for item in recipients:
        lowered = item.lower()
        if lowered not in [u.lower() for u in unique]:
            unique.append(item)
    return unique


def _business_parse_subject_body(text: str) -> tuple[Optional[str], str]:
    raw = str(text or "").strip()
    if not raw:
        return None, ""

    subject: Optional[str] = None
    body = raw

    subject_match = re.search(r"(?im)^\s*subject\s*:\s*(.+)$", raw)
    if subject_match:
        subject = subject_match.group(1).strip()

    body_match = re.search(r"(?is)^\s*(?:subject\s*:.*\n)?\s*body\s*:\s*(.+)$", raw)
    if body_match:
        body = body_match.group(1).strip()
    elif subject_match:
        trimmed = re.sub(r"(?im)^\s*subject\s*:.+\n?", "", raw, count=1).strip()
        body = re.sub(r"(?im)^\s*body\s*:\s*", "", trimmed, count=1).strip() or trimmed

    return subject, body


def _business_strip_markdown_code_fence(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    fenced = re.match(r"(?is)^```(?:html?)?\s*(.*?)\s*```$", raw)
    if fenced:
        return fenced.group(1).strip()
    return raw


def _business_body_has_html_markup(text: str) -> bool:
    raw = _business_strip_markdown_code_fence(text)
    if not raw:
        return False
    if re.search(r"(?is)<\s*(html|body|div|span|p|h[1-6]|table|tr|td|th|ul|ol|li|br|strong|em|a|img|style)\b", raw):
        return True
    if re.search(r"(?is)<[a-z][a-z0-9:_-]*[^>]*>.*</[a-z][a-z0-9:_-]*>", raw):
        return True
    return False


def _business_html_to_text(html_content: str) -> str:
    text = str(html_content or "")
    if not text:
        return ""
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n\n", text)
    text = re.sub(r"(?i)</(div|li|tr|h[1-6])\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", "", text)
    text = _html.unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _business_prepare_email_body(body: str) -> tuple[str, Optional[str]]:
    raw = _business_strip_markdown_code_fence(body)
    if not raw:
        return "No body content.", None

    if _business_body_has_html_markup(raw):
        html_body = raw
        if "<html" not in raw.lower():
            html_body = (
                "<html><body style=\"font-family:Segoe UI,Arial,sans-serif;line-height:1.5;\">"
                f"{raw}"
                "</body></html>"
            )
        plain_body = _business_html_to_text(raw) or "HTML content."
        return plain_body, html_body

    return raw, None


def _business_parse_image_data_url(data_url: str) -> Optional[Dict[str, Any]]:
    text = str(data_url or "").strip()
    if not text:
        return None
    match = re.match(r"^data:image/(png|jpe?g|webp);base64,([A-Za-z0-9+/=\r\n]+)$", text, flags=re.IGNORECASE)
    if not match:
        return None
    subtype_raw = match.group(1).lower()
    subtype = "jpeg" if subtype_raw in {"jpg", "jpeg"} else subtype_raw
    b64_value = re.sub(r"\s+", "", match.group(2))
    try:
        data_bytes = base64.b64decode(b64_value, validate=True)
    except Exception:
        return None
    if not data_bytes:
        return None
    extension = "jpg" if subtype == "jpeg" else subtype
    return {
        "bytes": data_bytes,
        "subtype": subtype,
        "filename": f"dashboard_snapshot.{extension}",
    }


def _business_plain_text_to_html(text: str) -> str:
    escaped = _html.escape(str(text or "").strip() or "No body content.")
    escaped = escaped.replace("\n", "<br/>")
    return (
        "<html><body style=\"font-family:Segoe UI,Arial,sans-serif;line-height:1.5;\">"
        f"{escaped}"
        "</body></html>"
    )


def _business_embed_snapshot_html(html_body: str, cid_ref: str) -> str:
    image_block = (
        "<div style=\"margin-top:16px;padding-top:12px;border-top:1px solid #e5e7eb;\">"
        "<div style=\"font-size:12px;color:#64748b;margin-bottom:8px;\">Dashboard Snapshot</div>"
        f"<img src=\"cid:{cid_ref}\" alt=\"Dashboard Snapshot\" "
        "style=\"max-width:100%;height:auto;border:1px solid #e5e7eb;border-radius:8px;display:block;\"/>"
        "</div>"
    )
    if re.search(r"(?is)</body>", html_body):
        return re.sub(r"(?is)</body>", f"{image_block}</body>", html_body, count=1)
    return f"{html_body}\n{image_block}"


def _business_mail_config_value(config: dict, key: str, env_names: List[str], default: Any = "") -> Any:
    value = config.get(key) if isinstance(config, dict) else None
    if value not in (None, ""):
        return value
    for env_name in env_names:
        env_value = _os.getenv(env_name)
        if env_value not in (None, ""):
            return env_value
    return default


def _business_whatsapp_config_value(config: dict, key: str, env_names: List[str], default: Any = "") -> Any:
    return _business_mail_config_value(config, key, env_names, default)


def _business_prepare_whatsapp_text(content: str) -> str:
    raw = _business_strip_markdown_code_fence(content or "")
    text = _business_html_to_text(raw) if _business_body_has_html_markup(raw) else raw
    return str(text or "").strip()


def _business_render_whatsapp_placeholders(value: Any, context: Dict[str, Any]) -> Any:
    if isinstance(value, str):
        template = str(value)

        def _replace(match: re.Match[str]) -> str:
            token = str(match.group(1) or "").strip()
            if not token:
                return ""
            replacement = context.get(token, "")
            if isinstance(replacement, (dict, list)):
                try:
                    return json.dumps(replacement, ensure_ascii=False)
                except Exception:
                    return str(replacement)
            return "" if replacement is None else str(replacement)

        return re.sub(r"\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}", _replace, template)

    if isinstance(value, list):
        return [_business_render_whatsapp_placeholders(item, context) for item in value]

    if isinstance(value, dict):
        rendered: Dict[str, Any] = {}
        for key, item in value.items():
            rendered[str(key)] = _business_render_whatsapp_placeholders(item, context)
        return rendered

    return value


def _business_build_whatsapp_payload(
    to_phone: str,
    message_type: str,
    text_body: str,
    template_name: str,
    template_language: str,
    preview_url: bool = False,
    request_payload_json: str = "",
    template_context: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], str, str]:
    normalized_to_phone = re.sub(r"\D+", "", str(to_phone or ""))
    if not normalized_to_phone:
        raise ValueError("WhatsApp recipient phone number is required in E.164 format.")

    normalized_type = str(message_type or "template").strip().lower() or "template"
    if normalized_type not in {"template", "text"}:
        raise ValueError("WhatsApp message_type must be 'template' or 'text'.")

    final_body = _business_prepare_whatsapp_text(text_body)
    normalized_template_name = str(template_name or "hello_world").strip() or "hello_world"
    normalized_template_language = str(template_language or "en_US").strip() or "en_US"
    context = {
        "to_phone": normalized_to_phone,
        "message_type": normalized_type,
        "text_body": final_body,
        "template_name": normalized_template_name,
        "template_language": normalized_template_language,
        "preview_url": bool(preview_url),
    }
    if isinstance(template_context, dict):
        context.update(template_context)

    custom_payload_raw = _business_strip_markdown_code_fence(str(request_payload_json or "").strip())
    if custom_payload_raw:
        try:
            parsed_payload = json.loads(custom_payload_raw)
        except Exception as exc:
            raise ValueError(f"Request JSON Payload is invalid JSON: {exc}") from exc
        if not isinstance(parsed_payload, dict):
            raise ValueError("Request JSON Payload must be a JSON object.")

        rendered_payload = _business_render_whatsapp_placeholders(parsed_payload, context)
        payload = rendered_payload if isinstance(rendered_payload, dict) else {}
        payload.setdefault("messaging_product", "whatsapp")
        payload.setdefault("to", normalized_to_phone)
        payload_type = str(payload.get("type") or normalized_type).strip().lower() or normalized_type
        return payload, str(payload.get("to") or normalized_to_phone), payload_type

    payload: Dict[str, Any] = {
        "messaging_product": "whatsapp",
        "to": normalized_to_phone,
    }

    if normalized_type == "template":
        payload["type"] = "template"
        payload["template"] = {
            "name": normalized_template_name,
            "language": {"code": normalized_template_language},
        }
    else:
        if not final_body:
            raise ValueError("WhatsApp text body is empty. Provide 'Text Body' or upstream prompt output.")
        payload["type"] = "text"
        payload["text"] = {
            "preview_url": bool(preview_url),
            "body": final_body,
        }

    return payload, normalized_to_phone, normalized_type


async def _business_send_whatsapp_message(
    config: dict,
    to_phone: str,
    message_type: str,
    text_body: str,
    template_name: str,
    template_language: str,
    preview_url: bool = False,
    request_payload_json: str = "",
    template_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    phone_number_id = str(
        _business_whatsapp_config_value(
            config,
            "phone_number_id",
            ["BUSINESS_WHATSAPP_PHONE_NUMBER_ID", "WHATSAPP_PHONE_NUMBER_ID"],
            "",
        )
    ).strip()
    access_token = str(
        _business_whatsapp_config_value(
            config,
            "access_token",
            ["BUSINESS_WHATSAPP_ACCESS_TOKEN", "WHATSAPP_ACCESS_TOKEN", "META_WHATSAPP_ACCESS_TOKEN"],
            "",
        )
    ).strip()
    api_version = str(
        _business_whatsapp_config_value(
            config,
            "api_version",
            ["BUSINESS_WHATSAPP_API_VERSION", "WHATSAPP_API_VERSION"],
            "v25.0",
        )
    ).strip().strip("/") or "v25.0"
    base_url = str(
        _business_whatsapp_config_value(
            config,
            "base_url",
            ["BUSINESS_WHATSAPP_BASE_URL", "WHATSAPP_BASE_URL"],
            "https://graph.facebook.com",
        )
    ).strip().rstrip("/")

    timeout_raw = _business_whatsapp_config_value(
        config,
        "request_timeout_sec",
        ["BUSINESS_WHATSAPP_TIMEOUT_SEC", "WHATSAPP_TIMEOUT_SEC"],
        30,
    )
    timeout_num = _business_to_number(timeout_raw)
    timeout_sec = max(5, min(int(timeout_num or 30), 180))

    if not phone_number_id:
        raise ValueError(
            "WhatsApp phone number id is missing. Set node 'Phone Number ID' or env BUSINESS_WHATSAPP_PHONE_NUMBER_ID."
        )
    if not access_token:
        raise ValueError(
            "WhatsApp access token is missing. Set node 'Access Token' or env BUSINESS_WHATSAPP_ACCESS_TOKEN."
        )

    payload, resolved_to_phone, resolved_message_type = _business_build_whatsapp_payload(
        to_phone=to_phone,
        message_type=message_type,
        text_body=text_body,
        template_name=template_name,
        template_language=template_language,
        preview_url=preview_url,
        request_payload_json=request_payload_json,
        template_context=template_context,
    )

    endpoint = f"{base_url}/{api_version}/{phone_number_id}/messages"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        import httpx
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            response = await client.post(endpoint, headers=headers, json=payload)
    except Exception as exc:
        raise ValueError(f"WhatsApp send failed: {exc}") from exc

    response_text = str(response.text or "").strip()
    if response.status_code >= 400:
        compact_detail = response_text if len(response_text) <= 500 else f"{response_text[:500]}..."
        raise ValueError(f"WhatsApp send failed ({response.status_code}): {compact_detail}")

    try:
        response_json = response.json()
    except Exception:
        response_json = {}

    message_id: Optional[str] = None
    messages = response_json.get("messages")
    if isinstance(messages, list) and messages:
        first = messages[0]
        if isinstance(first, dict):
            message_id = str(first.get("id") or "").strip() or None

    return {
        "transport": "meta_whatsapp",
        "endpoint": endpoint,
        "status_code": int(response.status_code),
        "message_id": message_id,
        "to": str(resolved_to_phone or ""),
        "message_type": str(resolved_message_type or ""),
        "request_payload_custom": bool(str(request_payload_json or "").strip()),
        "request_payload": payload,
        "response": response_json if isinstance(response_json, dict) else {},
    }


def _business_send_mail_via_smtp(
    config: dict,
    recipients: List[str],
    subject: str,
    body: str,
    dashboard_snapshot_data_url: Optional[str] = None,
) -> Dict[str, Any]:
    smtp_host = str(_business_mail_config_value(config, "smtp_host", ["BUSINESS_SMTP_HOST", "SMTP_HOST"], "")).strip()
    smtp_security = str(
        _business_mail_config_value(config, "smtp_security", ["BUSINESS_SMTP_SECURITY", "SMTP_SECURITY"], "tls")
    ).strip().lower() or "tls"
    smtp_user = str(
        _business_mail_config_value(config, "smtp_username", ["BUSINESS_SMTP_USERNAME", "SMTP_USERNAME"], "")
    ).strip()
    smtp_password = str(
        _business_mail_config_value(config, "smtp_password", ["BUSINESS_SMTP_PASSWORD", "SMTP_PASSWORD"], "")
    ).strip()
    from_email = str(
        _business_mail_config_value(
            config,
            "from_email",
            ["BUSINESS_SMTP_FROM_EMAIL", "SMTP_FROM_EMAIL", "BUSINESS_MAIL_FROM_EMAIL"],
            smtp_user,
        )
    ).strip()

    port_default = 465 if smtp_security == "ssl" else 587
    smtp_port_raw = _business_mail_config_value(config, "smtp_port", ["BUSINESS_SMTP_PORT", "SMTP_PORT"], port_default)
    smtp_port_num = _business_to_number(smtp_port_raw)
    smtp_port = int(smtp_port_num) if smtp_port_num else int(port_default)

    timeout_raw = _business_mail_config_value(
        config,
        "smtp_timeout_sec",
        ["BUSINESS_SMTP_TIMEOUT_SEC", "SMTP_TIMEOUT_SEC"],
        30,
    )
    timeout_num = _business_to_number(timeout_raw)
    timeout_sec = max(5, min(int(timeout_num or 30), 300))

    if not smtp_host:
        raise ValueError(
            "SMTP host is not configured. Set Mail Writer 'SMTP Host' or env BUSINESS_SMTP_HOST/SMTP_HOST."
        )
    if not recipients:
        raise ValueError("At least one recipient is required in Mail Writer 'To'.")
    if not from_email:
        raise ValueError(
            "Sender email is missing. Set Mail Writer 'From Email' or env BUSINESS_SMTP_FROM_EMAIL/SMTP_FROM_EMAIL."
        )
    if smtp_security not in {"tls", "ssl", "none"}:
        raise ValueError("SMTP security must be one of: tls, ssl, none.")
    if smtp_user and not smtp_password:
        raise ValueError("SMTP password is required when SMTP username is provided.")

    # Gmail App Passwords are often pasted with spaces (e.g., "abcd efgh ijkl mnop").
    # Gmail expects the same token without spaces during SMTP auth.
    if smtp_host.lower() == "smtp.gmail.com" and smtp_password:
        smtp_password = re.sub(r"\s+", "", smtp_password)

    plain_body, html_body = _business_prepare_email_body(body)
    snapshot_image = _business_parse_image_data_url(dashboard_snapshot_data_url or "")
    snapshot_cid = make_msgid(domain="framework.local")
    snapshot_cid_ref = snapshot_cid[1:-1] if snapshot_cid.startswith("<") and snapshot_cid.endswith(">") else snapshot_cid
    if snapshot_image:
        if not html_body:
            html_body = _business_plain_text_to_html(plain_body)
        html_body = _business_embed_snapshot_html(html_body, snapshot_cid_ref)

    message = EmailMessage()
    message["From"] = from_email
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject or "Business Workflow Recommendation"
    message.set_content(plain_body)
    if html_body:
        message.add_alternative(html_body, subtype="html")
        if snapshot_image:
            try:
                html_part = message.get_payload()[-1]
                html_part.add_related(
                    snapshot_image["bytes"],
                    maintype="image",
                    subtype=snapshot_image["subtype"],
                    cid=snapshot_cid,
                    filename=snapshot_image["filename"],
                    disposition="inline",
                )
            except Exception:
                # If inline embedding fails, mail still sends without the snapshot.
                pass

    try:
        if smtp_security == "ssl":
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=timeout_sec, context=context) as server:
                if smtp_user:
                    server.login(smtp_user, smtp_password)
                server.send_message(message)
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=timeout_sec) as server:
                server.ehlo()
                if smtp_security == "tls":
                    context = ssl.create_default_context()
                    server.starttls(context=context)
                    server.ehlo()
                if smtp_user:
                    server.login(smtp_user, smtp_password)
                server.send_message(message)
    except Exception as exc:
        raw_error = str(exc or "")
        lowered_error = raw_error.lower()
        if smtp_host.lower() == "smtp.gmail.com" and (
            "5.7.9" in raw_error or "application-specific password required" in lowered_error
        ):
            raise ValueError(
                "SMTP send failed: Gmail requires an App Password. "
                "Use Google 2-Step Verification, generate a 16-character App Password, "
                "and paste it into SMTP Password (no spaces)."
            ) from exc
        raise ValueError(f"SMTP send failed: {exc}") from exc

    return {
        "transport": "smtp",
        "smtp_host": smtp_host,
        "smtp_port": smtp_port,
        "smtp_security": smtp_security,
        "from_email": from_email,
        "recipients": recipients,
        "dashboard_snapshot_embedded": bool(snapshot_image),
        "body_format": "html" if html_body else "text",
    }


def _business_exact_literal_from_prompt(prompt: str) -> Optional[str]:
    base = _business_prompt_without_sample_rows(prompt)
    if not base:
        return None
    text = " ".join(base.split()).strip()

    quoted_patterns = [
        r"(?is)(?:respond|reply|output|return)\s+exactly\s*[:\-]?\s*['\"](.+?)['\"]\s*$",
        r"(?is)(?:exact\s+response|exact\s+output)\s*[:\-]\s*['\"](.+?)['\"]\s*$",
        r"(?is)(?:only\s+output)\s*[:\-]?\s*['\"](.+?)['\"]\s*$",
    ]
    for pattern in quoted_patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()

    plain_patterns = [
        r"(?is)^(?:respond|reply|output|return)\s+exactly\s*[:\-]?\s*(.+)$",
        r"(?is)^(?:exact\s+response|exact\s+output)\s*[:\-]\s*(.+)$",
        r"(?is)^only\s+output\s*[:\-]?\s*(.+)$",
    ]
    for pattern in plain_patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        candidate = match.group(1).strip().strip('"').strip("'").strip()
        if candidate:
            return candidate
    return None


def _business_extract_search_term(prompt: str) -> Optional[str]:
    base = _business_prompt_without_sample_rows(prompt)
    if not base:
        return None

    quoted = re.search(r"['\"]([^'\"]{2,80})['\"]", base)
    if quoted:
        return quoted.group(1).strip()

    patterns = [
        r"(?is)\bwhether\s+(.{2,80}?)\s+is\s+included\b",
        r"(?is)\bwhether\s+(.{2,80}?)\s+is\s+present\b",
        r"(?is)\bis\s+(.{2,80}?)\s+(?:in|inside)\s+(?:the\s+)?(?:data|dataset|source)\b",
        r"(?is)\bdoes\s+(.{2,80}?)\s+exist\b",
        r"(?is)\b(?:is|whether|does)\s+(.{2,80}?)\s+(?:included|present|available|exists?)\b",
        r"(?is)\b(?:contains?|include|find|search(?:\s+for)?)\s+(.{2,80})$",
    ]
    for pattern in patterns:
        match = re.search(pattern, base)
        if not match:
            continue
        candidate = " ".join(match.group(1).split()).strip(" .,:;!?")
        candidate = re.sub(r"^(?:the|a|an)\s+", "", candidate, flags=re.IGNORECASE).strip()
        candidate = re.sub(r"\b(?:is|are|was|were|be)\s*$", "", candidate, flags=re.IGNORECASE).strip()
        if candidate:
            return candidate
    return None


def _business_is_data_query_prompt(prompt: str) -> bool:
    normalized = _business_prompt_without_sample_rows(prompt).lower()
    if not normalized:
        return False
    return any(
        token in normalized
        for token in (
            "included",
            "include",
            "contains",
            "contain",
            "present",
            "exists",
            "available",
            "find",
            "search",
            "whether",
            "in dataset",
            "in data",
            "in source",
        )
    )


def _business_data_query_fallback(prompt: str, input_rows: List[dict]) -> Optional[str]:
    normalized = _business_prompt_without_sample_rows(prompt).lower()
    if not normalized:
        return None

    is_lookup = _business_is_data_query_prompt(prompt)
    if not is_lookup:
        return None

    rows = [r for r in input_rows if isinstance(r, dict)] or _business_extract_sample_rows_from_prompt(prompt)
    if not rows:
        return (
            "Data source check: no input rows received from upstream nodes. "
            "Verify ETL/MLOps source node output and connection to this prompt node."
        )

    term = _business_extract_search_term(prompt)
    if not term:
        return f"Data source connected. Received {len(rows)} rows for analysis."

    target = term.strip().lower()
    matched_rows = 0
    matched_columns: Dict[str, int] = {}
    for row in rows:
        row_hit = False
        for key, value in row.items():
            value_text = "" if value is None else str(value)
            if target in value_text.lower():
                row_hit = True
                key_text = str(key)
                matched_columns[key_text] = matched_columns.get(key_text, 0) + 1
        if row_hit:
            matched_rows += 1

    if matched_rows > 0:
        top_cols = sorted(matched_columns.items(), key=lambda item: item[1], reverse=True)[:4]
        top_cols_text = ", ".join([name for name, _ in top_cols]) if top_cols else "multiple fields"
        return (
            f"Data source check: '{term}' is included. "
            f"Matches found in {matched_rows} of {len(rows)} rows (fields: {top_cols_text})."
        )
    return f"Data source check: '{term}' is not found in {len(rows)} rows."


def _business_is_failure_like_response(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return True
    return (
        lowered.startswith("model call failed:")
        or lowered.startswith("model call returned no content")
        or "exhausted token limit before final response" in lowered
    )


def _business_finalize_prompt_output(
    prompt: str,
    input_rows: List[dict],
    upstream_text: str,
    model_text: str,
) -> str:
    text = str(model_text or "").strip()
    if not _business_is_email_prompt(prompt):
        return text

    if _business_is_failure_like_response(text):
        return _business_email_from_context(prompt, input_rows, upstream_text)

    lowered = text.lower()
    if any(token in lowered for token in ("could you share", "i'm not sure what you're working with", "i’m not sure what you’re working with")):
        if input_rows or _business_extract_sample_rows_from_prompt(prompt):
            return _business_email_from_context(prompt, input_rows, upstream_text)

    has_subject = "subject:" in lowered
    has_body = "body:" in lowered or "\n\n" in text
    if has_subject and has_body:
        return text

    subject = _business_email_subject_from_prompt(prompt)
    return f"Subject: {subject}\n\nBody:\n{text}"


def _business_output_format_language_hint(prompt: str) -> str:
    lowered = _business_prompt_without_sample_rows(prompt).lower()
    if "python" in lowered:
        return "python"
    if "sql" in lowered:
        return "sql"
    if "javascript" in lowered or "js " in lowered:
        return "javascript"
    if "typescript" in lowered:
        return "typescript"
    if "json" in lowered:
        return "json"
    if "html" in lowered:
        return "html"
    if "css" in lowered:
        return "css"
    if "bash" in lowered or "shell" in lowered:
        return "bash"
    return "text"


def _business_apply_output_format(
    output_format: str,
    prompt: str,
    input_rows: List[dict],
    upstream_text: str,
    text: str,
) -> str:
    fmt = str(output_format or "text").strip().lower()
    base_text = str(text or "").strip()
    if not fmt or fmt == "text":
        return base_text

    if fmt == "email":
        if _business_is_failure_like_response(base_text) or not base_text:
            return _business_email_from_context(prompt, input_rows, upstream_text)
        lowered = base_text.lower()
        if "subject:" in lowered and ("body:" in lowered or "\n\n" in base_text):
            return base_text
        subject = _business_email_subject_from_prompt(prompt)
        return f"Subject: {subject}\n\nBody:\n{base_text}"

    if fmt == "html":
        if base_text and re.search(r"<[a-zA-Z][^>]*>", base_text):
            return base_text
        escaped = _html.escape(base_text or _business_prompt_without_sample_rows(prompt) or "No content generated.")
        escaped = escaped.replace("\n", "<br/>")
        return (
            "<html><body>"
            "<div style=\"font-family:Segoe UI,Arial,sans-serif;line-height:1.55;"
            "padding:12px;color:#111;\">"
            f"{escaped}"
            "</div></body></html>"
        )

    if fmt == "code":
        if base_text.startswith("```") and base_text.endswith("```"):
            return base_text
        lang = _business_output_format_language_hint(prompt)
        return f"```{lang}\n{base_text}\n```"

    return base_text


def _business_deterministic_prompt_output(
    prompt: str,
    input_rows: List[dict],
    upstream_text: str,
) -> Optional[str]:
    exact_literal = _business_exact_literal_from_prompt(prompt)
    if exact_literal is not None:
        return exact_literal

    normalized = _business_prompt_without_sample_rows(prompt).lower()
    if not normalized:
        return None

    data_lookup = _business_data_query_fallback(prompt, input_rows)
    if data_lookup:
        return data_lookup

    row_extract = _business_prompt_rule_based_fallback(prompt)
    if row_extract:
        return row_extract

    return None


def _business_prompt_rule_based_fallback(prompt: str) -> Optional[str]:
    rows = _business_extract_sample_rows_from_prompt(prompt)
    if not rows:
        return None

    lowered = str(prompt or "").lower()
    index: Optional[int] = None
    if "first row" in lowered or "1st row" in lowered:
        index = 0
    elif "second row" in lowered or "2nd row" in lowered:
        index = 1
    elif "third row" in lowered or "3rd row" in lowered:
        index = 2
    else:
        match = re.search(r"\b(\d+)(?:st|nd|rd|th)\s+row\b", lowered)
        if match:
            index = int(match.group(1)) - 1

    if index is not None:
        if 0 <= index < len(rows):
            return json.dumps(rows[index], ensure_ascii=False)
        return f"Requested row {index + 1} is out of range. Available rows: {len(rows)}."

    if (
        "row count" in lowered
        or "count rows" in lowered
        or "how many rows" in lowered
        or "number of rows" in lowered
    ):
        return str(len(rows))

    return None


def _business_prompt_summary_fallback(prompt: str) -> Optional[str]:
    rows = _business_extract_sample_rows_from_prompt(prompt)
    if not rows:
        return None

    columns: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in columns:
                columns.append(str(key))

    if not columns:
        return f"Summary: {len(rows)} rows detected, but no columns were identified."

    numeric_summaries: List[str] = []
    categorical_summaries: List[str] = []
    for col in columns[:20]:
        values = [row.get(col) for row in rows]
        numeric_values: List[float] = []
        for value in values:
            parsed = _business_to_number(value)
            if parsed is not None:
                numeric_values.append(parsed)
        if numeric_values:
            col_min = min(numeric_values)
            col_max = max(numeric_values)
            col_avg = sum(numeric_values) / len(numeric_values)
            numeric_summaries.append(
                f"{col}: min={col_min:.3f}, max={col_max:.3f}, avg={col_avg:.3f}"
            )
            continue

        # Simple categorical profile for low-cardinality sample values.
        freq: Dict[str, int] = {}
        for value in values:
            text = "" if value is None else str(value).strip()
            if not text:
                continue
            freq[text] = freq.get(text, 0) + 1
        if freq:
            top_values = sorted(freq.items(), key=lambda item: item[1], reverse=True)[:2]
            top_text = ", ".join([f"{v} ({c})" for v, c in top_values])
            categorical_summaries.append(f"{col}: {top_text}")

    shown_cols = ", ".join(columns[:8])
    if len(columns) > 8:
        shown_cols += ", ..."

    lines = [
        f"Summary: rows={len(rows)}, columns={len(columns)} [{shown_cols}]",
    ]
    if numeric_summaries:
        lines.append("Numeric: " + "; ".join(numeric_summaries[:4]))
    if categorical_summaries:
        lines.append("Categorical: " + "; ".join(categorical_summaries[:4]))
    return "\n".join(lines)


def _business_is_placeholder_api_key(api_key: str) -> bool:
    key = str(api_key or "").strip().strip('"').strip("'")
    if not key:
        return True
    lowered = key.lower()
    if key.startswith("sk-etlflow-"):
        return True
    placeholder_tokens = (
        "your_real_key",
        "your_key_here",
        "replace_me",
        "replace_this",
        "changeme",
        "placeholder",
        "dummy",
        "sample_key",
    )
    return any(token in lowered for token in placeholder_tokens)


def _business_has_cloud_api_access() -> bool:
    api_key = str(_os.getenv("OPENAI_API_KEY") or "").strip()
    return not _business_is_placeholder_api_key(api_key)


def _business_cloud_base_url() -> str:
    base = str(
        _os.getenv("OPENAI_BUSINESS_BASE_URL")
        or _os.getenv("OPENAI_ANALYTICS_BASE_URL")
        or _os.getenv("OPENAI_API_BASE")
        or _os.getenv("OPENAI_BASE_URL")
        or "https://api.openai.com/v1"
    ).strip().rstrip("/")
    return base or "https://api.openai.com/v1"


def _business_default_prompt_max_tokens(model_name: str) -> int:
    default_raw = str(_os.getenv("BUSINESS_PROMPT_DEFAULT_MAX_TOKENS") or "").strip()
    cloud_default_raw = str(_os.getenv("BUSINESS_PROMPT_DEFAULT_MAX_TOKENS_CLOUD") or "").strip()
    local_default_raw = str(_os.getenv("BUSINESS_PROMPT_DEFAULT_MAX_TOKENS_LOCAL") or "").strip()

    if _business_is_gpt_oss20b_cloud_model(model_name):
        candidate = cloud_default_raw or default_raw or "1536"
    else:
        candidate = local_default_raw or default_raw or "1024"

    try:
        value = int(candidate)
    except Exception:
        value = 1536 if _business_is_gpt_oss20b_cloud_model(model_name) else 1024
    return max(64, min(value, 8192))


def _business_resolve_cloud_model(requested_model: str) -> str:
    configured = str(
        _os.getenv("OPENAI_BUSINESS_MODEL")
        or _os.getenv("OPENAI_ANALYTICS_MODEL")
        or _os.getenv("OPENAI_MODEL")
        or "gpt-5.3-codex"
    ).strip()
    requested = str(requested_model or "").strip()
    alias_set = {
        "gpt-oss20b-cloud",
        "gpt-0ss20b-cloud",
        "gpt-oss-20b-cloud",
        "oss20b-cloud",
    }
    lowered = requested.lower()
    if not requested or lowered in alias_set or lowered.endswith("-cloud"):
        return configured
    return requested


def _business_is_gpt_oss20b_cloud_model(model_name: str) -> bool:
    lowered = str(model_name or "").strip().lower()
    if not lowered:
        return False
    if lowered in {
        "gpt-oss20b-cloud",
        "gpt-0ss20b-cloud",
        "gpt-oss-20b-cloud",
        "oss20b-cloud",
        "gpt-oss:20b-cloud",
        "gpt-0ss:20b-cloud",
    }:
        return True
    compact = re.sub(r"[^a-z0-9]", "", lowered)
    return "gptoss20bcloud" in compact or "gpt0ss20bcloud" in compact


def _business_resolve_ollama_model(requested_model: str) -> str:
    configured_local = str(
        _os.getenv("OLLAMA_BUSINESS_MODEL")
        or _os.getenv("OLLAMA_MODEL")
        or "gpt-oss:20b"
    ).strip()
    configured_cloud = str(
        _os.getenv("OLLAMA_BUSINESS_MODEL_CLOUD")
        or _os.getenv("OLLAMA_CLOUD_MODEL")
        or "gpt-oss:20b-cloud"
    ).strip()
    requested = str(requested_model or "").strip()
    alias_set = {
        "gpt-oss20b",
        "gpt-0ss20b",
        "gpt-oss20b-local",
        "gpt-0ss20b-local",
        "gpt-oss-20b",
        "gpt-oss:20b",
    }
    lowered = requested.lower()
    if not requested or lowered in alias_set:
        return configured_local
    if _business_is_gpt_oss20b_cloud_model(lowered):
        return configured_cloud
    return requested


def _business_is_gpt_oss20b_model(model_name: str) -> bool:
    lowered = str(model_name or "").strip().lower()
    if not lowered:
        return False
    if "cloud" in lowered:
        return False
    if lowered in {
        "gpt-oss20b",
        "gpt-0ss20b",
        "gpt-oss20b-local",
        "gpt-0ss20b-local",
        "gpt-oss-20b",
        "gpt-oss:20b",
    }:
        return True
    compact = re.sub(r"[^a-z0-9]", "", lowered)
    return "gptoss20b" in compact or "gpt0ss20b" in compact


def _business_model_catalog_key(model_name: str) -> str:
    name = str(model_name or "").strip().lower()
    if _business_is_gpt_oss20b_cloud_model(name):
        return "gpt-oss20b-cloud"
    if _business_is_gpt_oss20b_model(name):
        return "gpt-oss20b"
    return name


def _business_extract_response_text(resp: dict) -> str:
    direct = resp.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    texts: List[str] = []
    for item in resp.get("output", []) or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []) or []:
            if not isinstance(content, dict):
                continue
            text_value = content.get("text") or content.get("output_text")
            if isinstance(text_value, str) and text_value.strip():
                texts.append(text_value.strip())
    return "\n".join(texts).strip()


def _business_extract_chat_text(resp: dict) -> str:
    choices = resp.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first = choices[0] if isinstance(choices[0], dict) else {}
    message = first.get("message") if isinstance(first.get("message"), dict) else {}
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
        return "\n".join(parts).strip()
    return ""


async def _business_call_cloud(
    model: str,
    prompt: str,
    system_prompt: str = "",
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    api_key = str(_os.getenv("OPENAI_API_KEY") or "").strip()
    if _business_is_placeholder_api_key(api_key):
        msg = (
            "OPENAI_API_KEY is missing or placeholder. "
            "Set a valid API key to use cloud model calls."
        )
        return {
            "provider": "cloud_error",
            "model": model,
            "effective_model": _business_resolve_cloud_model(model),
            "text": _business_prompt_fallback(prompt, msg),
            "error": msg,
        }

    base_url = _business_cloud_base_url()
    effective_model = _business_resolve_cloud_model(model)
    final_system_prompt = system_prompt.strip() or (
        "You are a business workflow assistant. Respond with concise, actionable output."
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if custom_key_header := str(_os.getenv("OPENAI_API_KEY_HEADER") or "").strip():
        headers[custom_key_header] = api_key

    req_temperature = float(temperature) if temperature is not None else 0.2
    req_tokens = int(max_tokens) if max_tokens is not None else 512

    try:
        import httpx
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                f"{base_url}/responses",
                headers=headers,
                json={
                    "model": effective_model,
                    "input": [
                        {"role": "system", "content": [{"type": "input_text", "text": final_system_prompt}]},
                        {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
                    ],
                    "temperature": req_temperature,
                    "max_output_tokens": req_tokens,
                },
            )
            if resp.status_code < 400:
                payload = resp.json() if resp.content else {}
                text = _business_extract_response_text(payload)
                if text:
                    return {
                        "provider": "cloud",
                        "model": model,
                        "effective_model": effective_model,
                        "text": text,
                    }

            # Compatibility fallback
            chat_resp = await client.post(
                f"{base_url}/chat/completions",
                headers=headers,
                json={
                    "model": effective_model,
                    "messages": [
                        {"role": "system", "content": final_system_prompt},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": req_temperature,
                    "max_tokens": req_tokens,
                },
            )
            if chat_resp.status_code >= 400:
                detail = chat_resp.text[:300] if chat_resp.text else f"HTTP {chat_resp.status_code}"
                raise RuntimeError(detail)

            chat_payload = chat_resp.json() if chat_resp.content else {}
            chat_text = _business_extract_chat_text(chat_payload)
            if chat_text:
                return {
                    "provider": "cloud",
                    "model": model,
                    "effective_model": effective_model,
                    "text": chat_text,
                }
            raise RuntimeError("Cloud API returned empty response")
    except Exception as exc:
        msg = f"Cloud model call failed: {exc}"
        return {
            "provider": "cloud_error",
            "model": model,
            "effective_model": effective_model,
            "text": _business_prompt_fallback(prompt, msg),
            "error": msg,
        }


async def _business_call_ollama(
    model: str,
    prompt: str,
    system_prompt: str = "",
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    base_url = _business_ollama_base_url()
    effective_model = _business_resolve_ollama_model(model)
    is_cloud_oss = _business_is_gpt_oss20b_cloud_model(model)
    keep_alive = str(_os.getenv("OLLAMA_KEEP_ALIVE") or "30m").strip() or "30m"
    try:
        request_timeout = float(str(_os.getenv("OLLAMA_REQUEST_TIMEOUT") or "300").strip())
    except Exception:
        request_timeout = 300.0
    # Large local models (like GPT-OSS20B) often need longer first-response time.
    if _business_is_gpt_oss20b_model(model) and request_timeout < 300.0:
        request_timeout = 300.0
    final_prompt = prompt
    if system_prompt.strip():
        final_prompt = f"{system_prompt.strip()}\n\n{prompt}"

    options: Dict[str, Any] = {}
    if temperature is not None:
        options["temperature"] = float(temperature)
    if max_tokens is not None:
        options["num_predict"] = int(max_tokens)
    # GPT-OSS cloud variants can spend many tokens in reasoning; enforce safer floors.
    if is_cloud_oss:
        current_predict = int(options.get("num_predict") or 0)
        min_predict = 512
        prompt_len = len(final_prompt or "")
        if prompt_len > 1200:
            min_predict = 1024
        elif prompt_len > 700:
            min_predict = 768
        if current_predict <= 0 or current_predict < min_predict:
            options["num_predict"] = min_predict
    if _business_is_gpt_oss20b_model(model):
        # Keep generation compact for interactive dashboard UX.
        options["num_ctx"] = int(_os.getenv("OLLAMA_NUM_CTX", "2048"))

    try:
        import httpx
        async with httpx.AsyncClient(timeout=request_timeout) as client:
            async def _ollama_chat_text(chat_options: Dict[str, Any]) -> Dict[str, Any]:
                chat_payload: Dict[str, Any] = {
                    "model": effective_model,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False,
                    "keep_alive": keep_alive,
                }
                if system_prompt.strip():
                    chat_payload["messages"] = [
                        {"role": "system", "content": system_prompt.strip()},
                        {"role": "user", "content": prompt},
                    ]
                if is_cloud_oss:
                    chat_payload["think"] = False
                if chat_options:
                    chat_payload["options"] = chat_options
                chat_resp = await client.post(f"{base_url}/api/chat", json=chat_payload)
                if chat_resp.status_code >= 400:
                    return {"text": "", "done_reason": "", "error": f"HTTP {chat_resp.status_code}"}
                chat_raw = chat_resp.json() if chat_resp.content else {}
                message_obj = chat_raw.get("message") if isinstance(chat_raw, dict) else {}
                chat_text = ""
                if isinstance(message_obj, dict):
                    chat_text = str(message_obj.get("content") or "").strip()
                return {
                    "text": chat_text,
                    "done_reason": str(chat_raw.get("done_reason") or "").strip().lower(),
                    "error": None,
                }

            # Cloud GPT-OSS: prefer chat path first (more stable than generate for final text).
            if is_cloud_oss:
                chat_primary = await _ollama_chat_text(dict(options))
                primary_text = str(chat_primary.get("text") or "").strip()
                primary_done_reason = str(chat_primary.get("done_reason") or "").strip().lower()
                if primary_text and primary_done_reason != "length":
                    return {
                        "provider": "ollama",
                        "model": model,
                        "effective_model": effective_model,
                        "text": primary_text,
                        "fallback": "chat_primary_for_cloud_model",
                    }

                boosted = dict(options)
                prior_predict = int(boosted.get("num_predict") or 0)
                boosted["num_predict"] = max(1024, prior_predict * 2 if prior_predict else 1024)
                chat_retry = await _ollama_chat_text(boosted)
                retry_text = str(chat_retry.get("text") or "").strip()
                if retry_text:
                    return {
                        "provider": "ollama",
                        "model": model,
                        "effective_model": effective_model,
                        "text": retry_text,
                        "fallback": "chat_retry_with_higher_num_predict",
                    }

                rule_text = _business_prompt_rule_based_fallback(prompt)
                if rule_text:
                    return {
                        "provider": "ollama",
                        "model": model,
                        "effective_model": effective_model,
                        "text": rule_text,
                        "fallback": "rule_based_prompt_fallback",
                    }

                summary_text = _business_prompt_summary_fallback(prompt)
                summary_hint = any(token in (prompt or "").lower() for token in ("summary", "summarize", "overview"))
                if summary_text and (summary_hint or not primary_text):
                    return {
                        "provider": "ollama",
                        "model": model,
                        "effective_model": effective_model,
                        "text": summary_text,
                        "fallback": "python_summary_fallback",
                    }

                if primary_text:
                    return {
                        "provider": "ollama",
                        "model": model,
                        "effective_model": effective_model,
                        "text": primary_text,
                        "fallback": "chat_length_partial_response",
                    }

            payload: Dict[str, Any] = {
                "model": effective_model,
                "prompt": final_prompt,
                "stream": False,
                "keep_alive": keep_alive,
            }
            if options:
                payload["options"] = options
            resp = await client.post(f"{base_url}/api/generate", json=payload)
            if resp.status_code >= 400:
                raise RuntimeError(f"Ollama HTTP {resp.status_code}: {resp.text[:300]}")
            raw = resp.json() if resp.content else {}
            text = str(raw.get("response") or "").strip()
            done_reason = str(raw.get("done_reason") or "").strip().lower()

            if is_cloud_oss and done_reason == "length":
                boosted = dict(options)
                prior_predict = int(boosted.get("num_predict") or 0)
                boosted["num_predict"] = max(1024, prior_predict * 2 if prior_predict else 1024)
                chat_retry = await _ollama_chat_text(boosted)
                retry_text = str(chat_retry.get("text") or "").strip()
                if retry_text:
                    text = retry_text

            if not text:
                chat_info = await _ollama_chat_text(dict(options))
                chat_text = str(chat_info.get("text") or "").strip()
                if chat_text:
                    text = chat_text

            if not text:
                rule_text = _business_prompt_rule_based_fallback(prompt)
                if rule_text:
                    text = rule_text

            if not text:
                summary_text = _business_prompt_summary_fallback(prompt)
                if summary_text:
                    text = summary_text

            if not text:
                if done_reason == "length":
                    text = _business_prompt_fallback(
                        prompt,
                        (
                            f"Ollama model '{effective_model}' exhausted token limit before final response"
                        ),
                    )
                else:
                    text = _business_prompt_fallback(
                        prompt,
                        f"Ollama model '{effective_model}' returned empty response",
                    )
            return {
                "provider": "ollama",
                "model": model,
                "effective_model": effective_model,
                "text": text,
            }
    except Exception as exc:
        detail = str(exc or "").strip()
        exc_name = type(exc).__name__
        if not detail:
            if "timeout" in exc_name.lower():
                detail = f"{exc_name}: request timed out after {int(request_timeout)}s"
            else:
                detail = exc_name

        timeout_like = "timeout" in exc_name.lower() or "timed out" in detail.lower()
        if _business_is_gpt_oss20b_model(model) and timeout_like:
            fallback_model = str(_os.getenv("OLLAMA_FAST_FALLBACK_MODEL") or "").strip()
            if fallback_model and not _business_is_gpt_oss20b_model(fallback_model):
                fallback_result = await _business_call_ollama(
                    model=fallback_model,
                    prompt=prompt,
                    system_prompt=system_prompt,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                if fallback_result.get("provider") == "ollama":
                    fallback_result["requested_model"] = model
                    fallback_result["fallback"] = "timeout_switch_to_fast_local_model"
                    return fallback_result
                fallback_err = str(
                    fallback_result.get("error")
                    or fallback_result.get("text")
                    or ""
                ).strip()
                if fallback_err:
                    detail = (
                        f"{detail}. Fast fallback model '{fallback_model}' also failed: "
                        f"{fallback_err[:220]}"
                    )

        msg = (
            f"Ollama call failed for model '{effective_model}': {detail}. "
            f"Ensure Ollama is running and install model with: ollama pull {effective_model}"
        )
        return {
            "provider": "ollama_error",
            "model": model,
            "effective_model": effective_model,
            "text": _business_prompt_fallback(prompt, msg),
            "error": msg,
        }


async def _business_call_model(
    model: str,
    prompt: str,
    system_prompt: str = "",
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    lowered = str(model or "").strip().lower()
    if _business_is_gpt_oss20b_cloud_model(lowered):
        ollama_cloud_result = await _business_call_ollama(
            model=model,
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        if ollama_cloud_result.get("provider") == "ollama":
            ollama_cloud_result["cloud_mode"] = "ollama_cloud"
            return ollama_cloud_result

        # Optional compatibility fallback for environments configured with OpenAI cloud keys.
        if _business_has_cloud_api_access():
            cloud_result = await _business_call_cloud(
                model=model,
                prompt=prompt,
                system_prompt=system_prompt,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            if cloud_result.get("provider") == "cloud":
                cloud_result["fallback"] = "ollama_cloud_error_used_openai_cloud"
                if ollama_cloud_result.get("error"):
                    cloud_result["ollama_error"] = ollama_cloud_result.get("error")
                return cloud_result
            if cloud_result.get("error"):
                ollama_cloud_result["openai_cloud_error"] = cloud_result.get("error")
        return ollama_cloud_result

    if "cloud" in lowered:
        cloud_result: Optional[Dict[str, Any]] = None
        if _business_has_cloud_api_access():
            cloud_result = await _business_call_cloud(
                model=model,
                prompt=prompt,
                system_prompt=system_prompt,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            if cloud_result.get("provider") == "cloud":
                return cloud_result

        local_result = await _business_call_ollama(
            model="gpt-oss20b",
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        if local_result.get("provider") == "ollama":
            local_result["fallback"] = (
                "cloud_error_used_local_ollama" if cloud_result else "cloud_unavailable_used_local_ollama"
            )
            local_result["requested_model"] = model
            if cloud_result and cloud_result.get("error"):
                local_result["cloud_error"] = cloud_result.get("error")
            return local_result

        if cloud_result:
            return cloud_result
        return {
            "provider": "cloud_error",
            "model": model,
            "text": _business_prompt_fallback(
                prompt,
                "Cloud credentials unavailable and local Ollama fallback failed",
            ),
            "error": "Cloud credentials unavailable and local Ollama fallback failed",
        }
    return await _business_call_ollama(
        model=model,
        prompt=prompt,
        system_prompt=system_prompt,
        temperature=temperature,
        max_tokens=max_tokens,
    )


def _business_static_model_catalog() -> List[dict]:
    cloud_available = _business_has_cloud_api_access()
    return [
        {
            "value": "gpt-oss20b",
            "label": "GPT-OSS20B (Local Ollama)",
            "provider": "ollama",
            "available": False,
        },
        {
            "value": "gpt-0ss20b",
            "label": "GPT-0SS20B (Local Ollama)",
            "provider": "ollama",
            "available": False,
        },
        {
            "value": "gpt-oss20b-cloud",
            "label": "GPT-OSS20B Cloud",
            "provider": "cloud",
            "available": cloud_available,
        },
        {
            "value": "gpt-0ss20b-cloud",
            "label": "GPT-0SS20B Cloud",
            "provider": "cloud",
            "available": cloud_available,
        },
        {
            "value": "llama3.1:8b",
            "label": "llama3.1:8b",
            "provider": "ollama",
            "available": False,
        },
        {
            "value": "qwen2.5:7b",
            "label": "qwen2.5:7b",
            "provider": "ollama",
            "available": False,
        },
        {
            "value": "mistral:7b",
            "label": "mistral:7b",
            "provider": "ollama",
            "available": False,
        },
    ]


async def _business_fetch_ollama_models() -> List[dict]:
    models_catalog = _business_static_model_catalog()
    seen_keys = {_business_model_catalog_key(str(m.get("value") or "")) for m in models_catalog}
    base_url = _business_ollama_base_url()
    resolved_local_model = _business_resolve_ollama_model("gpt-oss20b")
    resolved_cloud_model = _business_resolve_ollama_model("gpt-oss20b-cloud")

    try:
        import httpx
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(f"{base_url}/api/tags")
            if resp.status_code >= 400:
                return models_catalog
            payload = resp.json() if resp.content else {}
    except Exception:
        return models_catalog

    discovered = payload.get("models") if isinstance(payload, dict) else []
    for item in discovered or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("model") or "").strip()
        if not name:
            continue
        canonical = _business_model_catalog_key(name)
        if canonical in seen_keys:
            for model_item in models_catalog:
                if _business_model_catalog_key(str(model_item.get("value") or "")) == canonical:
                    model_item["available"] = True
                    model_item["provider"] = "ollama"
            continue
        models_catalog.append({
            "value": name,
            "label": name,
            "provider": "ollama",
            "available": True,
        })
        seen_keys.add(canonical)

    if _business_model_catalog_key(resolved_local_model) in seen_keys:
        for model_item in models_catalog:
            if str(model_item.get("value")) in {"gpt-oss20b", "gpt-0ss20b"}:
                model_item["available"] = True
                model_item["provider"] = "ollama"

    if _business_model_catalog_key(resolved_cloud_model) in seen_keys:
        for model_item in models_catalog:
            if str(model_item.get("value")) in {"gpt-oss20b-cloud", "gpt-0ss20b-cloud"}:
                model_item["available"] = True
                model_item["provider"] = "ollama"

    return models_catalog


async def _simulate_business_workflow_run(workflow_id: str, run_id: str):
    bg_db = SessionLocal()
    try:
        workflow = bg_db.query(models.BusinessWorkflow).filter(
            models.BusinessWorkflow.id == workflow_id
        ).first()
        run_row = bg_db.query(models.BusinessRun).filter(
            models.BusinessRun.id == run_id
        ).first()
        if not workflow or not run_row:
            return

        nodes = workflow.nodes or []
        edges = workflow.edges or []
        order = _business_topological_order(nodes, edges)
        if not order:
            order = [str(n.get("id")) for n in nodes if isinstance(n, dict) and n.get("id")]

        node_map: Dict[str, dict] = {
            str(n.get("id")): n for n in nodes if isinstance(n, dict) and n.get("id")
        }
        incoming_by_target: Dict[str, List[str]] = {}
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            source = str(edge.get("source") or "")
            target = str(edge.get("target") or "")
            if source and target:
                incoming_by_target.setdefault(target, []).append(source)

        logs: List[dict] = []
        rows_by_node: Dict[str, List[dict]] = {}
        text_by_node: Dict[str, str] = {}
        node_outputs: Dict[str, List[dict]] = {}
        metrics: Dict[str, Any] = {}
        selected_model = ""

        for node_id in order:
            node = node_map.get(node_id)
            if not node:
                continue
            node_type = _business_node_type(node).strip().lower()
            node_label = _business_node_label(node)
            config = _business_node_config(node)

            upstream_ids = incoming_by_target.get(node_id, [])
            input_rows: List[dict] = []
            upstream_text_chunks: List[str] = []
            for src_id in upstream_ids:
                input_rows.extend(rows_by_node.get(src_id, []))
                src_text = text_by_node.get(src_id)
                if src_text:
                    upstream_text_chunks.append(src_text)
            upstream_text = "\n".join([chunk for chunk in upstream_text_chunks if chunk.strip()])

            running_log = {
                "nodeId": node_id,
                "nodeLabel": node_label,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "running",
                "message": f"⟳ Running {node_label}…",
                "rows": 0,
                "input_sample": _mlops_sanitize_sample_rows(input_rows),
                "output_sample": [],
            }
            logs.append(running_log)
            run_row.logs = list(logs)
            bg_db.commit()

            await asyncio.sleep(0.25)

            output_rows: List[dict] = []
            try:
                if node_type in {"business_manual_trigger", "business_schedule_trigger"}:
                    output_rows = input_rows
                    step_message = f"✓ {node_label} initialized."

                elif node_type == "business_etl_source":
                    output_rows = _business_source_rows_from_pipeline(bg_db, config)
                    step_message = f"✓ {node_label} loaded {len(output_rows):,} rows from ETL pipeline."

                elif node_type == "business_mlops_source":
                    output_rows = _business_source_rows_from_mlops(bg_db, config)
                    step_message = f"✓ {node_label} loaded {len(output_rows):,} rows from MLOps output."

                elif node_type == "business_llm_prompt":
                    prompt = _business_build_prompt(config, input_rows, upstream_text)
                    model_name = str(config.get("model") or "gpt-oss20b").strip()
                    output_format = str(config.get("output_format") or "text").strip().lower() or "text"
                    system_prompt = str(config.get("system_prompt") or "").strip()
                    strict_mode = bool(config.get("strict_mode", True))
                    force_deterministic_mode = strict_mode or _business_is_data_query_prompt(prompt)
                    if strict_mode:
                        strict_prefix = (
                            "Follow the user's instruction exactly.\n"
                            "- Do not ask clarifying questions.\n"
                            "- Do not add extra explanation unless explicitly requested.\n"
                            "- Return only the requested output format/content."
                        )
                        if system_prompt:
                            system_prompt = f"{strict_prefix}\n\n{system_prompt}"
                        elif not _business_is_email_prompt(prompt):
                            system_prompt = strict_prefix
                    deterministic_output = (
                        _business_deterministic_prompt_output(
                            prompt=prompt,
                            input_rows=input_rows,
                            upstream_text=upstream_text,
                        )
                        if force_deterministic_mode
                        else None
                    )

                    if deterministic_output:
                        selected_model = model_name
                        llm_text = _business_apply_output_format(
                            output_format=output_format,
                            prompt=prompt,
                            input_rows=input_rows,
                            upstream_text=upstream_text,
                            text=deterministic_output,
                        )
                        provider_name = "python-deterministic"
                        text_by_node[node_id] = llm_text
                        output_rows = [{
                            "model": model_name,
                            "provider": provider_name,
                            "effective_model": model_name,
                            "output_format": output_format,
                            "prompt": prompt,
                            "response": llm_text,
                            "source_rows": len(input_rows),
                        }]
                        step_message = f"✓ {node_label} generated deterministic prompt response."
                        output_rows = [row for row in output_rows if isinstance(row, dict)]
                        rows_by_node[node_id] = output_rows
                        node_outputs[node_id] = _mlops_sanitize_sample_rows(output_rows, limit=200)

                        success_log = {
                            "nodeId": node_id,
                            "nodeLabel": node_label,
                            "timestamp": datetime.utcnow().isoformat(),
                            "status": "success",
                            "message": step_message,
                            "rows": len(output_rows),
                            "input_sample": _mlops_sanitize_sample_rows(input_rows),
                            "output_sample": _mlops_sanitize_sample_rows(output_rows),
                        }
                        logs[-1] = success_log
                        run_row.logs = list(logs)
                        run_row.node_outputs = dict(node_outputs)
                        run_row.metrics = dict(metrics)
                        run_row.model_name = selected_model or run_row.model_name
                        bg_db.commit()
                        continue

                    if not system_prompt and strict_mode:
                        system_prompt = (
                            "Follow the user instruction exactly. "
                            "Return only the requested final output with no extra commentary."
                        )
                    if not system_prompt and _business_is_email_prompt(prompt):
                        system_prompt = (
                            "You draft professional business emails. "
                            "Return output in this format only:\n"
                            "Subject: <one-line subject>\n\n"
                            "Body:\n"
                            "<clear concise email body with greeting, key points, and sign-off>"
                        )
                    temperature = _business_to_number(config.get("temperature"))
                    if strict_mode and temperature is None:
                        temperature = 0.0
                    requested_max_tokens = _business_to_number(config.get("max_tokens"))
                    if requested_max_tokens is None or requested_max_tokens <= 0:
                        max_tokens = _business_default_prompt_max_tokens(model_name)
                    else:
                        max_tokens = int(requested_max_tokens)
                    max_tokens = max(64, min(max_tokens, 8192))
                    if _business_is_gpt_oss20b_cloud_model(model_name) and max_tokens < 512:
                        max_tokens = 512
                    llm_result = await _business_call_model(
                        model=model_name,
                        prompt=prompt,
                        system_prompt=system_prompt,
                        temperature=temperature,
                        max_tokens=max_tokens,
                    )
                    selected_model = model_name
                    llm_text = str(llm_result.get("text") or "").strip()
                    provider_name = str(llm_result.get("provider") or "")
                    if provider_name in {"cloud_error", "ollama_error"}:
                        deterministic_fallback = _business_deterministic_prompt_output(
                            prompt=prompt,
                            input_rows=input_rows,
                            upstream_text=upstream_text,
                        ) if force_deterministic_mode else None
                        if deterministic_fallback:
                            llm_text = deterministic_fallback
                            provider_name = "python-deterministic-fallback"
                        else:
                            if not llm_text:
                                llm_text = _business_prompt_fallback(
                                    prompt,
                                    str(llm_result.get("error") or "Model execution failed"),
                                )
                            provider_name = "model-error-fallback"

                    llm_text = _business_finalize_prompt_output(
                        prompt=prompt,
                        input_rows=input_rows,
                        upstream_text=upstream_text,
                        model_text=llm_text,
                    )
                    llm_text = _business_apply_output_format(
                        output_format=output_format,
                        prompt=prompt,
                        input_rows=input_rows,
                        upstream_text=upstream_text,
                        text=llm_text,
                    )
                    text_by_node[node_id] = llm_text
                    output_rows = [{
                        "model": model_name,
                        "provider": provider_name,
                        "effective_model": llm_result.get("effective_model") or model_name,
                        "output_format": output_format,
                        "prompt": prompt,
                        "response": llm_text,
                        "source_rows": len(input_rows),
                    }]
                    if provider_name in {"model-error-fallback", "python-deterministic-fallback"}:
                        step_message = f"✓ {node_label} returned fallback output (model timeout/error) for {model_name}."
                    else:
                        step_message = f"✓ {node_label} generated prompt response using {model_name}."

                elif node_type == "business_decision":
                    latest_llm_text = upstream_text_chunks[-1] if upstream_text_chunks else ""
                    decision = _business_apply_decision(config, input_rows, latest_llm_text)
                    output_rows = decision["output_rows"]
                    metrics[f"decision_{node_id}"] = {
                        "passed": decision["passed"],
                        "matched_rows": decision["matched_rows"],
                        "inspected_rows": decision["inspected_rows"],
                    }
                    text_by_node[node_id] = decision["summary_text"]
                    step_message = f"✓ {node_label}: {decision['summary_text']}."

                elif node_type == "business_mail_writer":
                    to_email_raw = str(config.get("to_email") or "team@example.com").strip()
                    recipients = _business_parse_recipient_list(to_email_raw)
                    if not recipients:
                        raise ValueError("Mail Writer requires at least one valid recipient in 'To'.")

                    configured_subject = str(config.get("subject") or "").strip()
                    subject_is_default = not configured_subject or configured_subject.lower() in {
                        "business workflow recommendation",
                        "business recommendation",
                    }

                    body_template = str(config.get("body_template") or "").strip()
                    if body_template:
                        body_source = body_template
                    else:
                        body_source = upstream_text.strip() or "No upstream content available."
                    parsed_subject, parsed_body = _business_parse_subject_body(body_source)
                    subject = configured_subject or parsed_subject or "Business workflow recommendation"
                    if subject_is_default and parsed_subject:
                        subject = parsed_subject
                    body_text = parsed_body or body_source

                    send_mode = str(config.get("send_mode") or "send").strip().lower() or "send"
                    if config.get("send_email") is None:
                        should_send = send_mode not in {"draft", "preview", "disabled", "off"}
                    else:
                        should_send = _business_to_bool(config.get("send_email"), default=True)
                    generated_at = datetime.utcnow().isoformat()
                    dashboard_id = str(config.get("dashboard_id") or "").strip()
                    include_snapshot = _business_to_bool(
                        config.get("include_dashboard_snapshot"),
                        default=bool(dashboard_id),
                    )
                    dashboard_snapshot_data_url: Optional[str] = None
                    if include_snapshot:
                        if not dashboard_id:
                            raise ValueError("Enable snapshot embedding requires selecting 'Dashboard For Snapshot'.")
                        dashboard_row = bg_db.query(models.Dashboard).filter(models.Dashboard.id == dashboard_id).first()
                        if not dashboard_row:
                            raise ValueError(f"Dashboard '{dashboard_id}' not found for snapshot embedding.")
                        if not dashboard_row.thumbnail:
                            raise ValueError(
                                "Dashboard snapshot is missing. Open dashboard viewer and click 'Snapshot' first."
                            )
                        dashboard_snapshot_data_url = str(dashboard_row.thumbnail).strip()

                    if should_send:
                        transport = _business_send_mail_via_smtp(
                            config=config,
                            recipients=recipients,
                            subject=subject,
                            body=body_text,
                            dashboard_snapshot_data_url=dashboard_snapshot_data_url,
                        )
                        output_rows = [{
                            "to": ", ".join(recipients),
                            "subject": subject,
                            "body": body_text,
                            "status": "sent",
                            "generated_at": generated_at,
                            "transport": transport.get("transport"),
                            "smtp_host": transport.get("smtp_host"),
                            "smtp_port": transport.get("smtp_port"),
                            "smtp_security": transport.get("smtp_security"),
                            "from_email": transport.get("from_email"),
                            "body_format": transport.get("body_format", "text"),
                            "dashboard_snapshot_embedded": transport.get("dashboard_snapshot_embedded", False),
                            "dashboard_id": dashboard_id or None,
                        }]
                        step_message = f"✓ {node_label} sent email to {', '.join(recipients)}."
                    else:
                        output_rows = [{
                            "to": ", ".join(recipients),
                            "subject": subject,
                            "body": body_text,
                            "status": "draft",
                            "generated_at": generated_at,
                            "dashboard_snapshot_embedded": False,
                            "dashboard_id": dashboard_id or None,
                        }]
                        step_message = f"✓ {node_label} drafted email for {', '.join(recipients)}."

                    text_by_node[node_id] = body_text

                elif node_type == "business_whatsapp_sender":
                    to_phone_raw = str(config.get("to_phone") or config.get("to") or "").strip()
                    to_phone = re.sub(r"\D+", "", to_phone_raw)
                    if not to_phone:
                        raise ValueError("WhatsApp Sender requires a recipient phone number in 'To Phone Number'.")

                    send_mode = str(config.get("send_mode") or "send").strip().lower() or "send"
                    message_type = str(config.get("message_type") or "template").strip().lower() or "template"
                    if message_type not in {"template", "text"}:
                        message_type = "template"

                    template_name = str(config.get("template_name") or "hello_world").strip() or "hello_world"
                    template_language = str(config.get("template_language") or "en_US").strip() or "en_US"
                    preview_url = _business_to_bool(config.get("preview_url"), default=False)
                    request_payload_json = str(
                        config.get("request_payload_json")
                        or config.get("request_payload")
                        or ""
                    ).strip()

                    body_template = str(config.get("text_body") or config.get("body_template") or "").strip()
                    body_source = body_template or upstream_text.strip() or "No upstream content available."
                    text_body = _business_prepare_whatsapp_text(body_source)
                    generated_at = datetime.utcnow().isoformat()
                    payload_context = {
                        "to_phone": to_phone,
                        "message_type": message_type,
                        "template_name": template_name,
                        "template_language": template_language,
                        "text_body": text_body,
                        "upstream_text": upstream_text.strip(),
                        "generated_at": generated_at,
                        "node_id": node_id,
                        "node_label": node_label,
                    }
                    preview_payload, preview_to_phone, preview_message_type = _business_build_whatsapp_payload(
                        to_phone=to_phone,
                        message_type=message_type,
                        text_body=text_body,
                        template_name=template_name,
                        template_language=template_language,
                        preview_url=preview_url,
                        request_payload_json=request_payload_json,
                        template_context=payload_context,
                    )

                    if send_mode in {"draft", "preview", "disabled", "off"}:
                        output_rows = [{
                            "to": preview_to_phone or to_phone,
                            "status": "draft",
                            "message_type": preview_message_type or message_type,
                            "template_name": template_name if message_type == "template" else None,
                            "template_language": template_language if message_type == "template" else None,
                            "text_body": text_body if message_type == "text" else None,
                            "request_payload_custom": bool(request_payload_json),
                            "request_payload": preview_payload,
                            "generated_at": generated_at,
                        }]
                        step_message = f"✓ {node_label} drafted WhatsApp message for {to_phone}."
                    else:
                        transport = await _business_send_whatsapp_message(
                            config=config,
                            to_phone=to_phone,
                            message_type=message_type,
                            text_body=text_body,
                            template_name=template_name,
                            template_language=template_language,
                            preview_url=preview_url,
                            request_payload_json=request_payload_json,
                            template_context=payload_context,
                        )
                        output_rows = [{
                            "to": transport.get("to") or preview_to_phone or to_phone,
                            "status": "sent",
                            "message_type": transport.get("message_type") or preview_message_type or message_type,
                            "template_name": template_name if message_type == "template" else None,
                            "template_language": template_language if message_type == "template" else None,
                            "text_body": text_body if message_type == "text" else None,
                            "request_payload_custom": bool(transport.get("request_payload_custom")),
                            "request_payload": transport.get("request_payload") or preview_payload,
                            "whatsapp_response": transport.get("response") or {},
                            "generated_at": generated_at,
                            "transport": transport.get("transport"),
                            "endpoint": transport.get("endpoint"),
                            "api_status_code": transport.get("status_code"),
                            "message_id": transport.get("message_id"),
                        }]
                        response_for_log = transport.get("response")
                        if isinstance(response_for_log, (dict, list)):
                            try:
                                response_preview = json.dumps(response_for_log, ensure_ascii=False)
                            except Exception:
                                response_preview = str(response_for_log)
                        else:
                            response_preview = str(response_for_log or "").strip()
                        if len(response_preview) > 1200:
                            response_preview = response_preview[:1200] + "..."
                        if response_preview:
                            step_message = f"✓ {node_label} sent WhatsApp message to {to_phone}. Response: {response_preview}"
                        else:
                            step_message = f"✓ {node_label} sent WhatsApp message to {to_phone}."

                    effective_message_type = str(preview_message_type or message_type or "").strip().lower() or message_type
                    text_by_node[node_id] = text_body if effective_message_type == "text" else template_name

                elif node_type == "business_analytics":
                    output_rows = _business_make_analytics_rows(input_rows)
                    first_summary = output_rows[0].get("summary") if output_rows else ""
                    if first_summary:
                        text_by_node[node_id] = str(first_summary)
                    step_message = f"✓ {node_label} analyzed {len(input_rows):,} input rows."

                elif node_type == "business_merge":
                    output_rows = input_rows
                    step_message = f"✓ {node_label} merged {len(output_rows):,} rows."

                else:
                    output_rows = input_rows
                    step_message = f"✓ {node_label} processed {len(output_rows):,} rows."

                output_rows = [row for row in output_rows if isinstance(row, dict)]
                rows_by_node[node_id] = output_rows
                node_outputs[node_id] = _mlops_sanitize_sample_rows(output_rows, limit=200)

                success_log = {
                    "nodeId": node_id,
                    "nodeLabel": node_label,
                    "timestamp": datetime.utcnow().isoformat(),
                    "status": "success",
                    "message": step_message,
                    "rows": len(output_rows),
                    "input_sample": _mlops_sanitize_sample_rows(input_rows),
                    "output_sample": _mlops_sanitize_sample_rows(output_rows),
                }
                logs[-1] = success_log
                run_row.logs = list(logs)
                run_row.node_outputs = dict(node_outputs)
                run_row.metrics = dict(metrics)
                run_row.model_name = selected_model or run_row.model_name
                bg_db.commit()

            except Exception as step_exc:
                fail_log = {
                    "nodeId": node_id,
                    "nodeLabel": node_label,
                    "timestamp": datetime.utcnow().isoformat(),
                    "status": "error",
                    "message": f"✗ {node_label} failed: {step_exc}",
                    "rows": 0,
                    "input_sample": _mlops_sanitize_sample_rows(input_rows),
                    "output_sample": [],
                }
                logs[-1] = fail_log
                run_row.logs = list(logs)
                run_row.status = "failed"
                run_row.error_message = str(step_exc)
                run_row.finished_at = datetime.utcnow()
                run_row.node_outputs = dict(node_outputs)
                run_row.metrics = dict(metrics)
                run_row.model_name = selected_model or run_row.model_name
                bg_db.commit()
                return

        total_rows = 0
        for node_id in order:
            total_rows = max(total_rows, len(rows_by_node.get(node_id, [])))
        metrics["total_rows"] = total_rows
        metrics["node_count"] = len(order)
        metrics["completed_at"] = datetime.utcnow().isoformat()

        run_row.status = "success"
        run_row.finished_at = datetime.utcnow()
        run_row.logs = list(logs)
        run_row.node_outputs = dict(node_outputs)
        run_row.metrics = dict(metrics)
        run_row.model_name = selected_model or run_row.model_name
        bg_db.commit()

    except Exception as exc:
        logger.error(f"Business workflow run {run_id} failed: {exc}")
        run_row = bg_db.query(models.BusinessRun).filter(models.BusinessRun.id == run_id).first()
        if run_row:
            run_row.status = "failed"
            run_row.error_message = str(exc)
            run_row.finished_at = datetime.utcnow()
            bg_db.commit()
    finally:
        bg_db.close()


def _business_to_naive_utc(dt_value: Optional[datetime]) -> Optional[datetime]:
    if not dt_value:
        return None
    if dt_value.tzinfo is None:
        return dt_value
    return dt_value.astimezone(tz=None).replace(tzinfo=None)


def _business_parse_log_timestamp(raw_value: Any) -> Optional[datetime]:
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        return _business_to_naive_utc(parsed)
    except Exception:
        return None


def _business_last_activity_at(run_row: Any) -> Optional[datetime]:
    last_activity = _business_to_naive_utc(getattr(run_row, "started_at", None))
    logs = getattr(run_row, "logs", None)
    logs_list = logs if isinstance(logs, list) else []
    for item in reversed(logs_list):
        if not isinstance(item, dict):
            continue
        ts = _business_parse_log_timestamp(item.get("timestamp"))
        if ts:
            last_activity = ts
            break
    return last_activity


def _business_mark_stale_runs(db: Session):
    try:
        stale_minutes = int(str(_os.getenv("BUSINESS_RUN_STALE_MINUTES") or "10").strip())
    except Exception:
        stale_minutes = 10
    stale_minutes = max(2, stale_minutes)

    now = datetime.utcnow()
    cutoff = now - timedelta(minutes=stale_minutes)
    running_rows = db.query(models.BusinessRun).filter(models.BusinessRun.status == "running").all()
    updated = False

    for row in running_rows:
        last_activity = _business_last_activity_at(row)
        logs = row.logs if isinstance(row.logs, list) else []

        if last_activity is not None and last_activity >= cutoff:
            continue

        row.status = "failed"
        row.finished_at = now
        row.error_message = (
            row.error_message
            or f"Run marked failed after {stale_minutes} minutes without progress."
        )

        stale_log = {
            "nodeId": "__system__",
            "nodeLabel": "System",
            "timestamp": now.isoformat(),
            "status": "error",
            "message": row.error_message,
            "rows": 0,
            "input_sample": [],
            "output_sample": [],
        }
        if logs and isinstance(logs[-1], dict) and logs[-1].get("status") == "running":
            logs[-1] = stale_log
        else:
            logs.append(stale_log)
        row.logs = logs
        updated = True

    if updated:
        db.commit()


@app.get("/api/ollama/models")
async def list_ollama_models():
    models_catalog = await _business_fetch_ollama_models()
    return {
        "models": models_catalog,
        "base_url": _business_ollama_base_url(),
    }


@app.get("/api/business/workflows")
async def list_business_workflows(db: Session = Depends(get_db)):
    _business_mark_stale_runs(db)
    rows = db.query(models.BusinessWorkflow).order_by(models.BusinessWorkflow.updated_at.desc()).all()
    out = []
    for wf in rows:
        last_run = db.query(models.BusinessRun).filter(
            models.BusinessRun.workflow_id == wf.id
        ).order_by(models.BusinessRun.started_at.desc()).first()
        out.append({
            "id": wf.id,
            "name": wf.name,
            "description": wf.description,
            "status": wf.status,
            "tags": wf.tags or [],
            "nodeCount": len(wf.nodes or []),
            "created_at": wf.created_at.isoformat() if wf.created_at else None,
            "updated_at": wf.updated_at.isoformat() if wf.updated_at else None,
            "last_run": {
                "id": last_run.id,
                "status": last_run.status,
                "started_at": last_run.started_at.isoformat() if last_run.started_at else None,
                "model_name": last_run.model_name,
                "metrics": last_run.metrics or {},
            } if last_run else None,
        })
    return out


@app.post("/api/business/workflows", status_code=201)
async def create_business_workflow(body: BusinessWorkflowCreate, db: Session = Depends(get_db)):
    wf = models.BusinessWorkflow(
        id=str(uuid.uuid4()),
        name=body.name,
        description=body.description,
        nodes=body.nodes,
        edges=body.edges,
        tags=body.tags,
        status=body.status,
    )
    db.add(wf)
    db.commit()
    db.refresh(wf)
    return {"id": wf.id, "name": wf.name, "status": wf.status}


@app.get("/api/business/workflows/{workflow_id}")
async def get_business_workflow(workflow_id: str, db: Session = Depends(get_db)):
    wf = db.query(models.BusinessWorkflow).filter(models.BusinessWorkflow.id == workflow_id).first()
    if not wf:
        raise HTTPException(status_code=404, detail="Business workflow not found")
    return {
        "id": wf.id,
        "name": wf.name,
        "description": wf.description,
        "nodes": wf.nodes or [],
        "edges": wf.edges or [],
        "status": wf.status,
        "tags": wf.tags or [],
        "created_at": wf.created_at.isoformat() if wf.created_at else None,
        "updated_at": wf.updated_at.isoformat() if wf.updated_at else None,
    }


@app.put("/api/business/workflows/{workflow_id}")
async def update_business_workflow(workflow_id: str, body: BusinessWorkflowUpdate, db: Session = Depends(get_db)):
    wf = db.query(models.BusinessWorkflow).filter(models.BusinessWorkflow.id == workflow_id).first()
    if not wf:
        raise HTTPException(status_code=404, detail="Business workflow not found")
    for key, value in body.model_dump(exclude_none=True).items():
        setattr(wf, key, value)
    wf.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(wf)
    return {"id": wf.id, "name": wf.name, "status": wf.status}


@app.delete("/api/business/workflows/{workflow_id}", status_code=204)
async def delete_business_workflow(workflow_id: str, db: Session = Depends(get_db)):
    wf = db.query(models.BusinessWorkflow).filter(models.BusinessWorkflow.id == workflow_id).first()
    if wf:
        db.delete(wf)
        db.commit()


@app.post("/api/business/workflows/{workflow_id}/duplicate")
async def duplicate_business_workflow(workflow_id: str, db: Session = Depends(get_db)):
    wf = db.query(models.BusinessWorkflow).filter(models.BusinessWorkflow.id == workflow_id).first()
    if not wf:
        raise HTTPException(status_code=404, detail="Business workflow not found")
    copy = models.BusinessWorkflow(
        id=str(uuid.uuid4()),
        name=f"{wf.name} (Copy)",
        description=wf.description,
        nodes=wf.nodes,
        edges=wf.edges,
        tags=wf.tags,
        status="draft",
    )
    db.add(copy)
    db.commit()
    return {"id": copy.id, "name": copy.name}


@app.post("/api/business/workflows/{workflow_id}/execute")
async def execute_business_workflow(workflow_id: str, db: Session = Depends(get_db)):
    _business_mark_stale_runs(db)
    wf = db.query(models.BusinessWorkflow).filter(models.BusinessWorkflow.id == workflow_id).first()
    if not wf:
        raise HTTPException(status_code=404, detail="Business workflow not found")

    existing = db.query(models.BusinessRun).filter(
        models.BusinessRun.workflow_id == workflow_id,
        models.BusinessRun.status == "running",
    ).order_by(models.BusinessRun.started_at.desc()).first()
    if existing:
        now = datetime.utcnow()
        last_activity = _business_last_activity_at(existing)
        if last_activity is not None and last_activity < (now - timedelta(minutes=2)):
            stale_message = "Previous running instance was auto-closed due inactivity; restarted."
            logs = existing.logs if isinstance(existing.logs, list) else []
            logs.append({
                "nodeId": "__system__",
                "nodeLabel": "System",
                "timestamp": now.isoformat(),
                "status": "error",
                "message": stale_message,
                "rows": 0,
                "input_sample": [],
                "output_sample": [],
            })
            existing.status = "failed"
            existing.finished_at = now
            existing.error_message = stale_message
            existing.logs = logs
            db.commit()
        else:
            return {"run_id": existing.id, "status": "running", "already_running": True}

    run_id = str(uuid.uuid4())
    run = models.BusinessRun(
        id=run_id,
        workflow_id=workflow_id,
        status="running",
        triggered_by="manual",
    )
    db.add(run)
    db.commit()
    asyncio.create_task(_simulate_business_workflow_run(workflow_id, run_id))
    return {"run_id": run_id, "status": "running"}


@app.get("/api/business/runs")
async def list_business_runs(workflow_id: Optional[str] = None, db: Session = Depends(get_db)):
    _business_mark_stale_runs(db)
    query = db.query(models.BusinessRun).order_by(models.BusinessRun.started_at.desc())
    if workflow_id:
        query = query.filter(models.BusinessRun.workflow_id == workflow_id)
    runs = query.limit(300).all()
    out = []
    for run in runs:
        wf = db.query(models.BusinessWorkflow).filter(models.BusinessWorkflow.id == run.workflow_id).first()
        duration = None
        if run.started_at and run.finished_at:
            duration = (run.finished_at - run.started_at).total_seconds()
        out.append({
            "id": run.id,
            "workflow_id": run.workflow_id,
            "workflow_name": wf.name if wf else "Unknown",
            "status": run.status,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "duration": duration,
            "metrics": run.metrics or {},
            "model_name": run.model_name,
            "logs": run.logs or [],
            "node_outputs": run.node_outputs or {},
            "error_message": run.error_message,
            "triggered_by": run.triggered_by,
        })
    return out


@app.get("/api/business/runs/{run_id}")
async def get_business_run(run_id: str, db: Session = Depends(get_db)):
    _business_mark_stale_runs(db)
    run = db.query(models.BusinessRun).filter(models.BusinessRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Business run not found")
    return {
        "id": run.id,
        "workflow_id": run.workflow_id,
        "status": run.status,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "metrics": run.metrics or {},
        "model_name": run.model_name,
        "logs": run.logs or [],
        "node_outputs": run.node_outputs or {},
        "error_message": run.error_message,
        "triggered_by": run.triggered_by,
    }


# ─── EXECUTIONS ───────────────────────────────────────────────────────────────

@app.post("/api/pipelines/{pipeline_id}/execute")
async def execute_pipeline(pipeline_id: str, db: Session = Depends(get_db)):
    p = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    execution_id = _start_pipeline_execution(
        pipeline_id=pipeline_id,
        triggered_by="manual",
        skip_if_running=False,
    )
    if not execution_id:
        raise HTTPException(status_code=500, detail="Failed to start execution")
    return {"execution_id": execution_id, "status": "running"}

@app.get("/api/executions")
async def list_executions(
    pipeline_id: Optional[str] = None,
    include_logs: bool = False,
    db: Session = Depends(get_db),
):
    query = db.query(models.Execution).order_by(models.Execution.started_at.desc())
    if pipeline_id:
        query = query.filter(models.Execution.pipeline_id == pipeline_id)
    if not include_logs:
        query = query.options(load_only(
            models.Execution.id,
            models.Execution.pipeline_id,
            models.Execution.status,
            models.Execution.started_at,
            models.Execution.finished_at,
            models.Execution.rows_processed,
            models.Execution.triggered_by,
            models.Execution.error_message,
        ))
    execs = query.limit(200).all()
    pipeline_ids = list({e.pipeline_id for e in execs if getattr(e, "pipeline_id", None)})
    pipeline_name_by_id: Dict[str, str] = {}
    if pipeline_ids:
        rows = db.query(models.Pipeline.id, models.Pipeline.name).filter(models.Pipeline.id.in_(pipeline_ids)).all()
        pipeline_name_by_id = {pid: name for pid, name in rows}
    result = []
    for e in execs:
        duration = None
        if e.started_at and e.finished_at:
            duration = (e.finished_at - e.started_at).total_seconds()
        logs_payload = (e.logs or []) if include_logs else []
        result.append({
            "id": e.id,
            "pipeline_id": e.pipeline_id,
            "pipeline_name": pipeline_name_by_id.get(e.pipeline_id, "Unknown"),
            "status": e.status,
            "started_at": e.started_at.isoformat() if e.started_at else None,
            "finished_at": e.finished_at.isoformat() if e.finished_at else None,
            "duration": duration,
            "rows_processed": e.rows_processed,
            "triggered_by": e.triggered_by,
            "error_message": e.error_message,
            "logs": logs_payload
        })
    return result


def _try_finalize_stale_running_execution(execution: models.Execution, db: Session) -> None:
    """
    Safety net: if an execution row is stuck in `running` but logs are terminal,
    finalize it so frontend polling does not continue forever.
    """
    try:
        if execution is None or execution.status != "running":
            return
        logs = execution.logs if isinstance(execution.logs, list) else []
        if not logs:
            return
        stale_after_s = max(15, int(str(_os.getenv("EXECUTION_STALE_AUTOFINALIZE_SECONDS", "120")).strip() or "120"))
        now = datetime.utcnow()
        if execution.started_at is not None:
            try:
                started_at = execution.started_at
                if getattr(started_at, "tzinfo", None) is not None:
                    now_for_started = datetime.now(started_at.tzinfo)
                else:
                    now_for_started = now
                age_s = (now_for_started - started_at).total_seconds()
                if age_s < stale_after_s:
                    return
            except Exception:
                pass

        latest_log_ts: Optional[datetime] = None
        for entry in logs:
            if not isinstance(entry, dict):
                continue
            ts_raw = str(entry.get("timestamp") or "").strip()
            if not ts_raw:
                continue
            try:
                parsed = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except Exception:
                continue
            if latest_log_ts is None or parsed > latest_log_ts:
                latest_log_ts = parsed
        if latest_log_ts is not None:
            try:
                if getattr(latest_log_ts, "tzinfo", None) is not None:
                    now_for_log = datetime.now(latest_log_ts.tzinfo)
                else:
                    now_for_log = now
                if (now_for_log - latest_log_ts).total_seconds() < stale_after_s:
                    return
            except Exception:
                pass

        has_running_log = any(
            str((entry or {}).get("status") or "").strip().lower() == "running"
            for entry in logs
            if isinstance(entry, dict)
        )
        if has_running_log:
            return

        has_error = any(
            str((entry or {}).get("status") or "").strip().lower() == "error"
            for entry in logs
            if isinstance(entry, dict)
        )
        inferred_rows = 0
        for entry in logs:
            if not isinstance(entry, dict):
                continue
            try:
                inferred_rows += int(entry.get("rows") or 0)
            except Exception:
                continue

        execution.status = "failed" if has_error else "success"
        execution.finished_at = execution.finished_at or datetime.utcnow()
        if inferred_rows > 0 and (execution.rows_processed or 0) <= 0:
            execution.rows_processed = inferred_rows
        if has_error and not execution.error_message:
            error_logs = [
                str((entry or {}).get("message") or "").strip()
                for entry in logs
                if isinstance(entry, dict)
                and str((entry or {}).get("status") or "").strip().lower() == "error"
            ]
            if error_logs:
                execution.error_message = error_logs[-1]
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.warning(
            f"Failed stale execution auto-finalize for {getattr(execution, 'id', '<unknown>')}: {exc}"
        )


@app.get("/api/executions/{execution_id}")
async def get_execution(
    execution_id: str,
    include_node_results: bool = False,
    db: Session = Depends(get_db),
):
    e = db.query(models.Execution).filter(models.Execution.id == execution_id).first()
    if not e:
        raise HTTPException(status_code=404, detail="Execution not found")
    _try_finalize_stale_running_execution(e, db)
    node_results = e.node_results or {}
    return {
        "id": e.id,
        "pipeline_id": e.pipeline_id,
        "status": e.status,
        "started_at": e.started_at.isoformat() if e.started_at else None,
        "finished_at": e.finished_at.isoformat() if e.finished_at else None,
        "node_results": node_results if include_node_results else {},
        "node_result_keys": list(node_results.keys()) if isinstance(node_results, dict) else [],
        "logs": e.logs or [],
        "error_message": e.error_message,
        "rows_processed": e.rows_processed,
        "triggered_by": e.triggered_by,
    }

@app.delete("/api/executions/{execution_id}", status_code=204)
async def delete_execution(execution_id: str, db: Session = Depends(get_db)):
    e = db.query(models.Execution).filter(models.Execution.id == execution_id).first()
    if e:
        db.delete(e)
        db.commit()


# ─── CREDENTIALS ──────────────────────────────────────────────────────────────

@app.get("/api/credentials")
async def list_credentials(db: Session = Depends(get_db)):
    creds = db.query(models.Credential).all()
    return [{"id": c.id, "name": c.name, "type": c.type,
             "created_at": c.created_at.isoformat() if c.created_at else None} for c in creds]

@app.post("/api/credentials", status_code=201)
async def create_credential(body: CredentialCreate, db: Session = Depends(get_db)):
    cred = models.Credential(id=str(uuid.uuid4()), name=body.name, type=body.type, data=body.data)
    db.add(cred)
    db.commit()
    db.refresh(cred)
    return {"id": cred.id, "name": cred.name, "type": cred.type}

@app.put("/api/credentials/{cred_id}")
async def update_credential(cred_id: str, body: CredentialUpdate, db: Session = Depends(get_db)):
    c = db.query(models.Credential).filter(models.Credential.id == cred_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Credential not found")
    if body.name:
        c.name = body.name
    if body.data:
        c.data = body.data
    db.commit()
    return {"id": c.id, "name": c.name}

@app.delete("/api/credentials/{cred_id}", status_code=204)
async def delete_credential(cred_id: str, db: Session = Depends(get_db)):
    c = db.query(models.Credential).filter(models.Credential.id == cred_id).first()
    if c:
        db.delete(c)
        db.commit()


# ─── STATS & DASHBOARD ────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats(db: Session = Depends(get_db)):
    total_pipelines = db.query(models.Pipeline).count()
    total_mlops_workflows = db.query(models.MLOpsWorkflow).count()
    active_pipelines = db.query(models.Pipeline).filter(models.Pipeline.status == "active").count()
    total_executions = db.query(models.Execution).count()
    total_mlops_runs = db.query(models.MLOpsRun).count()
    successful = db.query(models.Execution).filter(models.Execution.status == "success").count()
    failed = db.query(models.Execution).filter(models.Execution.status == "failed").count()
    running = db.query(models.Execution).filter(models.Execution.status == "running").count()
    total_rows = db.query(models.Execution).with_entities(
        models.Execution.rows_processed).all()
    total_rows_processed = sum(r[0] or 0 for r in total_rows)
    return {
        "total_pipelines": total_pipelines,
        "active_pipelines": active_pipelines,
        "total_executions": total_executions,
        "successful_executions": successful,
        "failed_executions": failed,
        "running_executions": running,
        "total_mlops_workflows": total_mlops_workflows,
        "total_mlops_runs": total_mlops_runs,
        "total_rows_processed": total_rows_processed,
        "success_rate": round(successful / max(total_executions, 1) * 100, 1)
    }


# ─── WEBSOCKET ────────────────────────────────────────────────────────────────

@app.websocket("/ws/executions/{execution_id}")
async def execution_websocket(execution_id: str, ws: WebSocket):
    await ws_manager.connect(execution_id, ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(execution_id, ws)


# ─── TEMPLATES ────────────────────────────────────────────────────────────────

@app.get("/api/templates")
async def list_templates():
    return [
        {
            "id": "tpl-1", "name": "CSV to PostgreSQL", "category": "Database",
            "description": "Read CSV file and load into PostgreSQL table",
            "tags": ["csv", "postgres", "etl"]
        },
        {
            "id": "tpl-2", "name": "REST API to MongoDB", "category": "API",
            "description": "Fetch data from REST API and store in MongoDB",
            "tags": ["api", "mongodb", "rest"]
        },
        {
            "id": "tpl-3", "name": "Database Sync", "category": "Database",
            "description": "Synchronize data between two databases with transformation",
            "tags": ["postgres", "mysql", "sync"]
        },
        {
            "id": "tpl-4", "name": "S3 Data Lake Ingestion", "category": "Cloud",
            "description": "Ingest Parquet files from S3 into data warehouse",
            "tags": ["s3", "parquet", "cloud"]
        },
        {
            "id": "tpl-5", "name": "Kafka Stream Processing", "category": "Streaming",
            "description": "Process Kafka events and store aggregated results",
            "tags": ["kafka", "streaming", "aggregate"]
        },
        {
            "id": "tpl-6", "name": "Data Quality Check", "category": "Utility",
            "description": "Run quality checks, deduplication and type validation",
            "tags": ["quality", "deduplicate", "validate"]
        },
    ]


# ─── FILE UPLOAD ──────────────────────────────────────────────────────────────

_UPLOADS_DIR = _os.path.join(_os.path.dirname(__file__), "uploads")
_os.makedirs(_UPLOADS_DIR, exist_ok=True)


def _normalize_json_path_expr(path: Optional[str]) -> str:
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


def _extract_json_path_value(payload: Any, path: Optional[str]) -> Any:
    import re

    expr = _normalize_json_path_expr(path)
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


def _rows_from_json_value(value: Any) -> list:
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


def _parse_json_object(value: Any, field_name: str) -> dict:
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return {}
        try:
            parsed = json.loads(text)
        except Exception:
            if field_name == "params":
                # Support query-string style params (a=1&b=2) from the UI.
                import urllib.parse
                parsed_qs = dict(urllib.parse.parse_qsl(text, keep_blank_values=True))
                if parsed_qs:
                    return parsed_qs
            raise HTTPException(400, f"{field_name} must be valid JSON object")
        if isinstance(parsed, dict):
            return parsed
        raise HTTPException(400, f"{field_name} must be a JSON object")
    raise HTTPException(400, f"{field_name} must be a JSON object")


def _parse_json_payload(value: Any, field_name: str) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            raise HTTPException(400, f"{field_name} must be valid JSON")
    raise HTTPException(400, f"{field_name} must be valid JSON")


def _strip_sql_terminator(value: Any) -> str:
    query = str(value or "").strip()
    if not query:
        return ""
    while query.endswith(";"):
        query = query[:-1].rstrip()
    return query


def _build_limited_sql_query(query: str, limit: int, dialect: str) -> str:
    q = _strip_sql_terminator(query)
    if not q:
        raise HTTPException(400, "SQL query is required")
    if dialect == "oracle":
        return f"SELECT * FROM ({q}) src_q WHERE ROWNUM <= {int(limit)}"
    return f"SELECT * FROM ({q}) src_q LIMIT {int(limit)}"


def _normalize_source_rows(rows: list, max_rows: int) -> list:
    out: list = []
    for item in rows[:max_rows]:
        if isinstance(item, dict):
            out.append(item)
        else:
            out.append({"value": item})
    return out


async def _load_tabular_rows_for_source(node_type: str, config: dict, max_rows: int) -> list:
    ntype = (node_type or "").strip().lower()
    cfg = config or {}
    limit = max(1, min(int(max_rows or 200), 2000))

    if ntype == "postgres_source":
        import psycopg2
        conn = None
        try:
            conn = psycopg2.connect(
                host=cfg.get("host", "localhost"),
                port=int(cfg.get("port", 5432)),
                database=cfg.get("database", ""),
                user=cfg.get("user", ""),
                password=cfg.get("password", ""),
            )
            query = _strip_sql_terminator(cfg.get("query", ""))
            table = str(cfg.get("table", "") or "").strip()
            if not query:
                if not table:
                    raise HTTPException(400, "PostgreSQL source requires SQL query (or table).")
                query = f"SELECT * FROM {table}"
            limited_query = _build_limited_sql_query(query, limit, "postgres")
            with conn.cursor() as cursor:
                cursor.execute(limited_query)
                cols = [desc[0] for desc in (cursor.description or [])]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(400, f"PostgreSQL metadata detection failed: {exc}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    if ntype == "mysql_source":
        import pymysql
        conn = None
        try:
            conn = pymysql.connect(
                host=cfg.get("host", "localhost"),
                port=int(cfg.get("port", 3306)),
                database=cfg.get("database", ""),
                user=cfg.get("user", ""),
                password=cfg.get("password", ""),
                cursorclass=pymysql.cursors.DictCursor,
            )
            query = _strip_sql_terminator(cfg.get("query", ""))
            table = str(cfg.get("table", "") or "").strip()
            if not query:
                if not table:
                    raise HTTPException(400, "MySQL source requires SQL query (or table).")
                query = f"SELECT * FROM {table}"
            limited_query = _build_limited_sql_query(query, limit, "mysql")
            with conn.cursor() as cursor:
                cursor.execute(limited_query)
                return list(cursor.fetchall())
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(400, f"MySQL metadata detection failed: {exc}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    if ntype == "oracle_source":
        import oracledb
        conn = None
        try:
            dsn = etl_engine._build_oracle_dsn(cfg)
            conn = oracledb.connect(
                user=cfg.get("user", ""),
                password=cfg.get("password", ""),
                dsn=dsn,
            )
            query = _strip_sql_terminator(cfg.get("query", ""))
            table = str(cfg.get("table", "") or "").strip()
            if not query:
                if not table:
                    raise HTTPException(400, "Oracle source requires SQL query (or table).")
                query = f"SELECT * FROM {table}"
            limited_query = _build_limited_sql_query(query, limit, "oracle")
            cursor = conn.cursor()
            cursor.execute(limited_query)
            cols = [desc[0] for desc in (cursor.description or [])]
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
            cursor.close()
            return rows
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(400, f"Oracle metadata detection failed: {exc}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    if ntype == "mongodb_source":
        client = None
        try:
            from pymongo import MongoClient
            connection_string = str(cfg.get("connection_string", "mongodb://localhost:27017") or "").strip()
            database = str(cfg.get("database", "") or "").strip()
            collection = str(cfg.get("collection", "") or "").strip()
            if not database or not collection:
                raise HTTPException(400, "MongoDB source requires database and collection.")
            filter_query = _parse_json_payload(cfg.get("filter"), "filter")
            if filter_query is None:
                filter_query = {}
            if not isinstance(filter_query, dict):
                raise HTTPException(400, "MongoDB filter must be a JSON object.")
            client = MongoClient(connection_string)
            docs = list(client[database][collection].find(filter_query).limit(limit))
            for doc in docs:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
            return docs
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(400, f"MongoDB metadata detection failed: {exc}")
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

    if ntype == "redis_source":
        try:
            import redis as redis_lib
            r = redis_lib.Redis(
                host=cfg.get("host", "localhost"),
                port=int(cfg.get("port", 6379)),
                password=cfg.get("password"),
                decode_responses=True,
            )
            key_pattern = str(cfg.get("key_pattern", "*") or "*")
            keys = r.keys(key_pattern)[:limit]
            return [{"key": k, "value": r.get(k)} for k in keys]
        except Exception as exc:
            raise HTTPException(400, f"Redis metadata detection failed: {exc}")

    if ntype == "elasticsearch_source":
        try:
            from elasticsearch import Elasticsearch
            hosts_raw = cfg.get("hosts", "http://localhost:9200")
            if isinstance(hosts_raw, str):
                hosts = [h.strip() for h in hosts_raw.split(",") if h.strip()]
            elif isinstance(hosts_raw, list):
                hosts = [str(h).strip() for h in hosts_raw if str(h).strip()]
            else:
                hosts = ["http://localhost:9200"]
            if not hosts:
                hosts = ["http://localhost:9200"]
            index = str(cfg.get("index", "*") or "*").strip() or "*"
            query_obj = _parse_json_payload(cfg.get("query"), "query")
            if query_obj is None:
                query_obj = {"query": {"match_all": {}}}
            if not isinstance(query_obj, dict):
                raise HTTPException(400, "Elasticsearch query must be a JSON object.")
            es = Elasticsearch(hosts)
            result = es.search(index=index, body=query_obj, size=limit)
            hits = ((result or {}).get("hits") or {}).get("hits") or []
            rows = []
            for hit in hits:
                src = hit.get("_source", {})
                row = dict(src) if isinstance(src, dict) else {"value": src}
                row["_id"] = hit.get("_id")
                row["_score"] = hit.get("_score")
                rows.append(row)
            return rows
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(400, f"Elasticsearch metadata detection failed: {exc}")

    raise HTTPException(400, f"Unsupported node_type for source field detection: {node_type}")


async def _load_json_payload_for_source(node_type: str, config: dict) -> Any:
    ntype = (node_type or "").strip().lower()
    cfg = config or {}

    if ntype == "rest_api_source":
        import httpx

        method = str(cfg.get("method") or "GET").upper()
        url = str(cfg.get("url") or "").strip()
        if not url:
            raise HTTPException(400, "REST API source requires 'url'")

        headers = _parse_json_object(cfg.get("headers"), "headers")
        params = _parse_json_object(cfg.get("params"), "params")
        body_payload = _parse_json_payload(cfg.get("body"), "body")

        request_kwargs = {
            "method": method,
            "url": url,
        }
        if headers:
            request_kwargs["headers"] = headers
        if params:
            request_kwargs["params"] = params
        if body_payload is not None and method not in {"GET", "HEAD", "OPTIONS"}:
            request_kwargs["json"] = body_payload

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.request(**request_kwargs)
        if resp.status_code >= 400:
            raise HTTPException(resp.status_code, f"API request failed with status {resp.status_code}")
        try:
            return resp.json()
        except Exception:
            raise HTTPException(400, "API response is not valid JSON")

    if ntype == "graphql_source":
        import httpx

        endpoint = str(cfg.get("endpoint") or "").strip()
        query = str(cfg.get("query") or "").strip()
        if not endpoint:
            raise HTTPException(400, "GraphQL source requires 'endpoint'")
        if not query:
            raise HTTPException(400, "GraphQL source requires 'query'")

        headers = _parse_json_object(cfg.get("headers"), "headers")
        variables = _parse_json_object(cfg.get("variables"), "variables")

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                endpoint,
                json={"query": query, "variables": variables},
                headers=headers,
            )
        if resp.status_code >= 400:
            raise HTTPException(resp.status_code, f"GraphQL request failed with status {resp.status_code}")
        try:
            return resp.json()
        except Exception:
            raise HTTPException(400, "GraphQL response is not valid JSON")

    if ntype == "json_source":
        file_path = str(cfg.get("file_path") or "").strip()
        if not file_path:
            raise HTTPException(400, "JSON source requires 'file_path'")
        if not _os.path.isfile(file_path):
            raise HTTPException(404, f"JSON file not found: {file_path}")
        try:
            with open(file_path, "r", encoding="utf-8") as jf:
                return json.load(jf)
        except Exception:
            try:
                import pandas as pd
                df = pd.read_json(file_path)
                return df.fillna("").to_dict(orient="records")
            except Exception:
                raise HTTPException(400, "Failed to parse JSON source file")

    raise HTTPException(400, f"Unsupported node_type for JSON field detection: {node_type}")


def _collect_json_path_options(payload: Any, max_paths: int = 200) -> List[dict]:
    options: List[dict] = []
    seen = set()

    def add(path: str, kind: str, count: Optional[int] = None):
        if len(options) >= max_paths:
            return
        key = (path, kind)
        if key in seen:
            return
        seen.add(key)
        label = f"{path or '$'} ({kind})"
        if count is not None:
            label = f"{label} · {count}"
        options.append({"value": path or "$", "label": label, "kind": kind, "count": count})

    def walk(value: Any, path: str, depth: int):
        if len(options) >= max_paths or depth > 6:
            return
        if isinstance(value, dict):
            array_children = [v for v in value.values() if isinstance(v, list)]
            if array_children:
                lengths = {len(v) for v in array_children}
                if len(lengths) == 1 and next(iter(lengths)) > 0:
                    add(path, "object<arrays>", next(iter(lengths)))
                else:
                    add(path, "object", len(value))
            else:
                add(path, "object", len(value))
            for k, v in list(value.items())[:50]:
                next_path = f"{path}.{k}" if path else str(k)
                walk(v, next_path, depth + 1)
            return
        if isinstance(value, list):
            kind = "array<object>" if value and all(isinstance(x, dict) for x in value[: min(len(value), 8)]) else "array"
            add(path, kind, len(value))
            for item in value[:5]:
                if isinstance(item, (dict, list)):
                    walk(item, path + "[]" if path else "[]", depth + 1)

    walk(payload, "", 0)

    # Prefer array choices first in dropdown
    kind_priority = {"array<object>": 0, "object<arrays>": 1, "array": 2, "object": 3}
    options.sort(key=lambda o: (kind_priority.get(str(o.get("kind", "")), 4), len(str(o.get("value", "")))))
    return options


@app.post("/api/source/json-field-options")
async def detect_source_json_field_options(body: JsonFieldOptionsRequest):
    payload = await _load_json_payload_for_source(body.node_type, body.config)

    max_paths = max(20, min(int(body.max_paths or 200), 500))
    json_paths = _collect_json_path_options(payload, max_paths=max_paths)

    suggested = ""
    for preferred in ("array<object>", "object<arrays>", "array"):
        for opt in json_paths:
            kind = str(opt.get("kind", ""))
            if kind == preferred or (preferred == "array" and kind.startswith("array")):
                suggested = str(opt.get("value") or "")
                break
        if suggested:
            break

    selected_payload = _extract_json_path_value(payload, suggested)
    rows = _rows_from_json_value(selected_payload)
    if not rows:
        rows = _rows_from_json_value(payload)

    columns = list(rows[0].keys()) if rows and isinstance(rows[0], dict) else []
    return {
        "node_type": body.node_type,
        "json_paths": json_paths,
        "suggested_json_path": suggested,
        "columns": columns,
        "row_count": len(rows),
        "preview": rows[:10],
    }


@app.post("/api/source/field-options")
async def detect_source_field_options(body: SourceFieldOptionsRequest):
    max_rows = max(1, min(int(body.max_rows or 200), 2000))
    rows = await _load_tabular_rows_for_source(body.node_type, body.config, max_rows=max_rows)
    normalized_rows = _normalize_source_rows(rows, max_rows)
    columns = list(normalized_rows[0].keys()) if normalized_rows and isinstance(normalized_rows[0], dict) else []
    return {
        "node_type": body.node_type,
        "columns": columns,
        "row_count": len(normalized_rows),
        "preview": normalized_rows[:10],
        "sample_limited": True,
    }


@app.post("/api/custom-fields/validate")
async def validate_custom_fields(body: CustomFieldValidationRequest):
    config = body.config if isinstance(body.config, dict) else {}
    raw_custom_fields = config.get("custom_fields")
    parsed_specs = etl_engine._parse_custom_fields_config(raw_custom_fields)

    max_rows = max(1, min(int(body.max_rows or 30), 200))
    input_rows = body.rows if isinstance(body.rows, list) else []
    normalized_rows: List[Dict[str, Any]] = []
    for row in input_rows[:max_rows]:
        if isinstance(row, dict):
            normalized_rows.append(row)
    warnings: List[str] = []
    errors: List[str] = []

    if not normalized_rows:
        normalized_rows = [{}]
        warnings.append("No sample rows were provided. Validation ran with a single empty row.")

    raw_list: List[Any] = []
    if isinstance(raw_custom_fields, list):
        raw_list = raw_custom_fields
    elif isinstance(raw_custom_fields, str):
        text = raw_custom_fields.strip()
        if text:
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    raw_list = parsed
                else:
                    errors.append("custom_fields JSON must be an array.")
            except Exception as exc:
                errors.append(f"custom_fields is not valid JSON: {exc}")
    elif raw_custom_fields is None:
        raw_list = []
    else:
        errors.append("custom_fields must be an array.")

    seen_names: set = set()
    for idx, item in enumerate(raw_list):
        if not isinstance(item, dict):
            errors.append(f"Field #{idx + 1}: item is not an object.")
            continue
        enabled = bool(item.get("enabled", True))
        name = str(item.get("name") or item.get("field") or "").strip()
        if not name:
            errors.append(f"Field #{idx + 1}: name is required.")
            continue
        key = name.lower()
        if key in seen_names:
            errors.append(f"Field '{name}': duplicate field name.")
        else:
            seen_names.add(key)
        if not enabled:
            warnings.append(f"Field '{name}' is disabled and will be skipped.")
            continue

        mode = str(item.get("mode") or item.get("kind") or item.get("type") or "value").strip().lower()
        if mode not in {"value", "json"}:
            mode = "value"
        if mode == "value":
            expression = str(item.get("expression") or item.get("expr") or "").strip()
            if not expression:
                errors.append(f"Field '{name}': expression is required for Single Value mode.")
        else:
            template_value = item.get("json_template", item.get("template"))
            if isinstance(template_value, str):
                text = template_value.strip()
                if not text:
                    errors.append(f"Field '{name}': JSON template is required for JSON mode.")
                else:
                    try:
                        json.loads(text)
                    except Exception as exc:
                        errors.append(f"Field '{name}': invalid JSON template ({exc}).")
            elif isinstance(template_value, (dict, list)):
                pass
            else:
                errors.append(f"Field '{name}': JSON template must be object/array or JSON string.")

    if not parsed_specs:
        errors.append("No enabled custom fields found to validate.")

    if errors:
        return {
            "ok": False,
            "input_rows": len(normalized_rows),
            "output_rows": 0,
            "errors": errors,
            "warnings": warnings,
            "sample_input": _json_safe_value(normalized_rows[:10]),
            "sample_output": [],
        }

    try:
        result_rows = etl_engine._transform_custom_fields(
            normalized_rows,
            config,
            parsed_specs,
            execution_context={
                "node_id": "__custom_field_validation__",
                "profile_state_by_node": {},
            },
        )
    except Exception as exc:
        return {
            "ok": False,
            "input_rows": len(normalized_rows),
            "output_rows": 0,
            "errors": [str(exc)],
            "warnings": warnings,
            "sample_input": _json_safe_value(normalized_rows[:10]),
            "sample_output": [],
        }

    return {
        "ok": True,
        "input_rows": len(normalized_rows),
        "output_rows": len(result_rows) if isinstance(result_rows, list) else 0,
        "errors": [],
        "warnings": warnings,
        "sample_input": _json_safe_value(normalized_rows[:10]),
        "sample_output": _json_safe_value((result_rows or [])[:20] if isinstance(result_rows, list) else []),
    }


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    import pandas as pd

    def _count_delimited_rows_fast(path: str) -> int:
        """Best-effort row count for large delimited files without full pandas scan."""
        line_breaks = 0
        total_bytes = 0
        last_byte = b""
        with open(path, "rb") as reader:
            while True:
                chunk = reader.read(1024 * 1024)
                if not chunk:
                    break
                total_bytes += len(chunk)
                line_breaks += chunk.count(b"\n")
                last_byte = chunk[-1:]
        if total_bytes == 0:
            return 0
        total_lines = line_breaks + (0 if last_byte == b"\n" else 1)
        # subtract header line
        return max(0, total_lines - 1)

    def _read_csv_sample_with_fallback(path: str, sample_rows: int):
        requested = "utf-8"
        candidates = ["utf-8", "utf-8-sig", "cp1252", "latin1", "iso-8859-1"]
        last_err = None

        for enc in candidates:
            try:
                frame = pd.read_csv(path, nrows=sample_rows, low_memory=True, encoding=enc)
                return frame, enc
            except UnicodeDecodeError as exc:
                last_err = exc
                continue
            except Exception as exc:
                msg = str(exc).lower()
                if "codec can't decode" in msg or "unicode" in msg:
                    last_err = exc
                    continue
                raise

        # Final lossy fallback so upload still succeeds and user can proceed.
        try:
            frame = pd.read_csv(
                path,
                nrows=sample_rows,
                low_memory=True,
                encoding=requested,
                encoding_errors="replace",
            )
            return frame, f"{requested}-replace"
        except TypeError:
            # Older pandas without encoding_errors support.
            pass
        except Exception as exc:
            last_err = exc

        if last_err:
            raise last_err
        raise RuntimeError("Failed to decode CSV file.")

    # Use safe filename and persist inside uploads/
    safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in (file.filename or "upload"))
    unique_name = f"{uuid.uuid4().hex[:8]}_{safe_name}"
    dest_path = _os.path.join(_UPLOADS_DIR, unique_name)

    max_upload_mb = max(10, min(int(_os.getenv("UPLOAD_MAX_MB", "1024")), 4096))
    max_upload_bytes = max_upload_mb * 1024 * 1024
    written = 0
    try:
        with open(dest_path, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                written += len(chunk)
                if written > max_upload_bytes:
                    raise HTTPException(
                        status_code=413,
                        detail=f"File too large. Max upload size is {max_upload_mb} MB.",
                    )
                f.write(chunk)
    finally:
        try:
            await file.close()
        except Exception:
            pass

    suffix = "." + safe_name.rsplit(".", 1)[-1].lower() if "." in safe_name else ""
    try:
        if suffix == ".csv":
            csv_sample_rows = max(100, min(int(_os.getenv("UPLOAD_CSV_SAMPLE_ROWS", "5000")), 100000))
            df, detected_encoding = _read_csv_sample_with_fallback(dest_path, csv_sample_rows)
            row_count = _count_delimited_rows_fast(dest_path)
            return {
                "filename": file.filename,
                "rows": row_count,
                "columns": list(df.columns),
                "dtypes": {k: str(v) for k, v in df.dtypes.items()},
                "preview": df.head(10).fillna("").to_dict(orient="records"),
                "tmp_path": dest_path,
                "encoding": detected_encoding,
            }
        elif suffix in (".xls", ".xlsx"):
            df = pd.read_excel(dest_path)
        elif suffix == ".json":
            payload = None
            with open(dest_path, "r", encoding="utf-8") as jf:
                try:
                    payload = json.load(jf)
                except Exception:
                    payload = None
            if payload is None:
                try:
                    df_json = pd.read_json(dest_path, lines=True)
                    payload = df_json.fillna("").to_dict(orient="records")
                except Exception:
                    df_json = pd.read_json(dest_path)
                    payload = df_json.fillna("").to_dict(orient="records")
            json_paths = _collect_json_path_options(payload)
            suggested = ""
            for preferred in ("array<object>", "object<arrays>", "array"):
                for opt in json_paths:
                    kind = str(opt.get("kind", ""))
                    if kind == preferred or (preferred == "array" and kind.startswith("array")):
                        suggested = str(opt.get("value") or "")
                        break
                if suggested:
                    break
            selected = _extract_json_path_value(payload, suggested)
            rows = _rows_from_json_value(selected)
            if not rows:
                rows = _rows_from_json_value(payload)
            preview = rows[:10]
            columns = list(preview[0].keys()) if preview and isinstance(preview[0], dict) else []
            return {
                "filename": file.filename,
                "rows": len(rows),
                "columns": columns,
                "dtypes": {},
                "preview": preview,
                "tmp_path": dest_path,
                "json_paths": json_paths,
                "suggested_json_path": suggested,
            }
        elif suffix == ".parquet":
            df = pd.read_parquet(dest_path)
        else:
            raise ValueError(f"Unsupported file type: '{suffix}'. Supported: csv, xlsx, json, parquet")

        return {
            "filename": file.filename,
            "rows": len(df),
            "columns": list(df.columns),
            "dtypes": {k: str(v) for k, v in df.dtypes.items()},
            "preview": df.head(10).fillna("").to_dict(orient="records"),
            "tmp_path": dest_path,   # kept on disk — ETL engine reads from here
        }
    except Exception as e:
        # Clean up on parse failure
        if _os.path.exists(dest_path):
            _os.unlink(dest_path)
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/upload")
async def cleanup_uploads():
    """Optional: clean up old upload files (call manually or via cron)."""
    import time
    removed = 0
    for fname in _os.listdir(_UPLOADS_DIR):
        fpath = _os.path.join(_UPLOADS_DIR, fname)
        if _os.path.isfile(fpath) and (time.time() - _os.path.getmtime(fpath)) > 3600:
            _os.unlink(fpath)
            removed += 1
    return {"removed": removed}


# ─── FILE DOWNLOAD ────────────────────────────────────────────────────────────

from fastapi.responses import FileResponse
import urllib.parse

@app.get("/api/download")
async def download_file(path: str):
    """Download a file by absolute server path (output files from destinations)."""
    decoded = urllib.parse.unquote(path)
    if not _os.path.isfile(decoded):
        raise HTTPException(status_code=404, detail=f"File not found: {decoded!r}")
    filename = _os.path.basename(decoded)
    return FileResponse(
        decoded,
        filename=filename,
        media_type="application/octet-stream"
    )


# ─── DASHBOARDS ───────────────────────────────────────────────────────────────

class DashboardCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    theme: str = "dark"
    tags: list = []

class DashboardUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    widgets: Optional[list] = None
    layout: Optional[list] = None
    theme: Optional[str] = None
    global_filters: Optional[list] = None
    tags: Optional[list] = None
    is_public: Optional[bool] = None


class DashboardThumbnailUpdate(BaseModel):
    thumbnail: str


class PythonDashboardCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    theme: str = "dark"
    tags: list = []
    widgets: list = []
    layout: list = []
    global_filters: list = []


class PythonDashboardUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    theme: Optional[str] = None
    tags: Optional[list] = None
    widgets: Optional[list] = None
    layout: Optional[list] = None
    global_filters: Optional[list] = None


class AnalyticsDashboardRequest(BaseModel):
    source_type: str = "sample"
    dataset: Optional[str] = "sales"
    pipeline_id: Optional[str] = None
    pipeline_node_id: Optional[str] = None
    file_path: Optional[str] = None
    mlops_workflow_id: Optional[str] = None
    mlops_run_id: Optional[str] = None
    mlops_node_id: Optional[str] = None
    mlops_output_mode: Optional[str] = "predictions"
    mlops_prediction_start_date: Optional[str] = None
    mlops_prediction_end_date: Optional[str] = None
    sql: Optional[str] = None
    nlp_prompt: Optional[str] = None
    forecast_horizon: int = 30


class PythonAnalyticsRequest(BaseModel):
    source_type: str = "sample"
    dataset: Optional[str] = "sales"
    pipeline_id: Optional[str] = None
    pipeline_node_id: Optional[str] = None
    file_path: Optional[str] = None
    mlops_workflow_id: Optional[str] = None
    mlops_run_id: Optional[str] = None
    mlops_node_id: Optional[str] = None
    mlops_output_mode: Optional[str] = "predictions"
    mlops_prediction_start_date: Optional[str] = None
    mlops_prediction_end_date: Optional[str] = None
    sql: Optional[str] = None
    chart_type: Optional[str] = "auto"
    x_field: Optional[str] = None
    y_field: Optional[str] = None
    color_field: Optional[str] = None
    group_by: Optional[str] = None
    title: Optional[str] = None
    limit: int = 5000
    drill_filters: List[Dict[str, Any]] = Field(default_factory=list)


class NLPChatSchemaRequest(BaseModel):
    source_type: str = "sample"
    dataset: Optional[str] = "sales"
    pipeline_id: Optional[str] = None
    pipeline_node_id: Optional[str] = None
    file_path: Optional[str] = None
    mlops_workflow_id: Optional[str] = None
    mlops_run_id: Optional[str] = None
    mlops_node_id: Optional[str] = None
    mlops_output_mode: Optional[str] = "predictions"
    mlops_prediction_start_date: Optional[str] = None
    mlops_prediction_end_date: Optional[str] = None
    sql: Optional[str] = None
    sample_size: int = 300


class NLPChatMessage(BaseModel):
    role: str
    content: str


class NLPChatAskRequest(NLPChatSchemaRequest):
    question: str
    metric_fields: List[str] = Field(default_factory=list)
    matrix_fields: List[str] = Field(default_factory=list)  # alias accepted from UI wording
    dimension_fields: List[str] = Field(default_factory=list)
    domain_context: Optional[str] = None
    chat_history: List[NLPChatMessage] = Field(default_factory=list)
    top_k: int = 12
    include_rows: int = 8
    semantic_mode: str = "auto"  # auto | embedding | tfidf | token


class WorkflowChatMessage(BaseModel):
    role: str
    content: str


class WorkflowChatAskRequest(BaseModel):
    question: str
    context_type: str = "auto"  # auto | etl | mlops | business | visualization | application
    chat_history: List[WorkflowChatMessage] = Field(default_factory=list)


class WorkflowChatConfigureRequest(BaseModel):
    question: str
    context_type: str = "auto"  # auto | etl | mlops | business | visualization | application
    chat_history: List[WorkflowChatMessage] = Field(default_factory=list)
    include_dashboard: bool = True

_PYTHON_DASHBOARD_TAG = "__python_analytics__"


def _ensure_python_dashboard_tags(tags: Optional[list]) -> list:
    out: List[str] = []
    for item in (tags or []):
        text = str(item).strip()
        if text and text not in out:
            out.append(text)
    if _PYTHON_DASHBOARD_TAG not in out:
        out.insert(0, _PYTHON_DASHBOARD_TAG)
    return out


def _is_python_dashboard(row: Any) -> bool:
    tags = row.tags if row is not None else []
    if not isinstance(tags, list):
        return False
    return _PYTHON_DASHBOARD_TAG in [str(tag).strip() for tag in tags]

@app.get("/api/dashboards")
async def list_dashboards(db: Session = Depends(get_db)):
    rows = db.query(models.Dashboard).order_by(models.Dashboard.updated_at.desc()).all()
    return [{
        "id": d.id, "name": d.name, "description": d.description,
        "owner": d.owner, "theme": d.theme, "tags": d.tags or [],
        "widget_count": len(d.widgets or []),
        "is_public": d.is_public, "share_token": d.share_token,
        "created_at": d.created_at.isoformat() if d.created_at else None,
        "updated_at": d.updated_at.isoformat() if d.updated_at else None,
    } for d in rows]

@app.post("/api/dashboards", status_code=201)
async def create_dashboard(body: DashboardCreate, db: Session = Depends(get_db)):
    d = models.Dashboard(
        id=str(uuid.uuid4()), name=body.name, description=body.description,
        theme=body.theme, tags=body.tags
    )
    db.add(d); db.commit(); db.refresh(d)
    _audit(db, "admin", "create", "dashboard", d.id, d.name)
    return {"id": d.id, "name": d.name}

@app.get("/api/dashboards/{did}")
async def get_dashboard(did: str, db: Session = Depends(get_db)):
    d = db.query(models.Dashboard).filter(models.Dashboard.id == did).first()
    if not d: raise HTTPException(404, "Dashboard not found")
    return {
        "id": d.id, "name": d.name, "description": d.description,
        "owner": d.owner, "theme": d.theme, "tags": d.tags or [],
        "widgets": d.widgets or [], "layout": d.layout or [],
        "global_filters": d.global_filters or [],
        "is_public": d.is_public, "share_token": d.share_token,
        "created_at": d.created_at.isoformat() if d.created_at else None,
        "updated_at": d.updated_at.isoformat() if d.updated_at else None,
    }

@app.put("/api/dashboards/{did}")
async def update_dashboard(did: str, body: DashboardUpdate, db: Session = Depends(get_db)):
    d = db.query(models.Dashboard).filter(models.Dashboard.id == did).first()
    if not d: raise HTTPException(404, "Dashboard not found")
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(d, k, v)
    from datetime import datetime
    d.updated_at = datetime.utcnow()
    db.commit(); db.refresh(d)
    _audit(db, "admin", "update", "dashboard", d.id, d.name)
    return {"id": d.id, "name": d.name}


@app.put("/api/dashboards/{did}/thumbnail")
async def update_dashboard_thumbnail(did: str, body: DashboardThumbnailUpdate, db: Session = Depends(get_db)):
    d = db.query(models.Dashboard).filter(models.Dashboard.id == did).first()
    if not d:
        raise HTTPException(404, "Dashboard not found")

    thumbnail = str(body.thumbnail or "").strip()
    if not thumbnail:
        raise HTTPException(400, "Thumbnail is required")
    if not thumbnail.lower().startswith("data:image/"):
        raise HTTPException(400, "Thumbnail must be a data URL image")
    if len(thumbnail) > 8_000_000:
        raise HTTPException(400, "Thumbnail is too large")

    d.thumbnail = thumbnail
    d.updated_at = datetime.utcnow()
    db.commit()
    _audit(db, "admin", "update", "dashboard_thumbnail", d.id, d.name, detail="Snapshot thumbnail updated")
    return {"id": d.id, "has_thumbnail": True}

@app.delete("/api/dashboards/{did}", status_code=204)
async def delete_dashboard(did: str, db: Session = Depends(get_db)):
    d = db.query(models.Dashboard).filter(models.Dashboard.id == did).first()
    if d:
        _audit(db, "admin", "delete", "dashboard", d.id, d.name)
        db.delete(d); db.commit()

@app.post("/api/dashboards/{did}/share")
async def share_dashboard(did: str, db: Session = Depends(get_db)):
    d = db.query(models.Dashboard).filter(models.Dashboard.id == did).first()
    if not d: raise HTTPException(404, "Dashboard not found")
    if not d.share_token:
        d.share_token = uuid.uuid4().hex
    d.is_public = True
    db.commit()
    return {
        "share_token": d.share_token,
        "share_url": f"/share/{d.share_token}",
        "embed_url": f"/embed/{d.share_token}",
    }

@app.get("/api/share/{token}")
async def view_shared_dashboard(token: str, db: Session = Depends(get_db)):
    d = db.query(models.Dashboard).filter(
        models.Dashboard.share_token == token, models.Dashboard.is_public == True
    ).first()
    if not d: raise HTTPException(404, "Shared dashboard not found")
    return {
        "id": d.id,
        "name": d.name,
        "description": d.description,
        "widgets": d.widgets or [],
        "layout": d.layout or [],
        "theme": d.theme,
        "global_filters": d.global_filters or [],
    }

@app.post("/api/dashboards/{did}/duplicate")
async def duplicate_dashboard(did: str, db: Session = Depends(get_db)):
    src = db.query(models.Dashboard).filter(models.Dashboard.id == did).first()
    if not src: raise HTTPException(404, "Dashboard not found")
    copy = models.Dashboard(
        id=str(uuid.uuid4()), name=f"{src.name} (Copy)",
        description=src.description, theme=src.theme,
        widgets=src.widgets, layout=src.layout, tags=src.tags
    )
    db.add(copy); db.commit()
    return {"id": copy.id, "name": copy.name}


@app.get("/api/python-analytics/dashboards")
async def list_python_dashboards(db: Session = Depends(get_db)):
    rows = db.query(models.Dashboard).order_by(models.Dashboard.updated_at.desc()).all()
    python_rows = [row for row in rows if _is_python_dashboard(row)]
    return [{
        "id": d.id,
        "name": d.name,
        "description": d.description,
        "owner": d.owner,
        "theme": d.theme,
        "tags": d.tags or [],
        "widget_count": len(d.widgets or []),
        "is_public": d.is_public,
        "share_token": d.share_token,
        "created_at": d.created_at.isoformat() if d.created_at else None,
        "updated_at": d.updated_at.isoformat() if d.updated_at else None,
    } for d in python_rows]


@app.post("/api/python-analytics/dashboards", status_code=201)
async def create_python_dashboard(body: PythonDashboardCreate, db: Session = Depends(get_db)):
    name = str(body.name or "").strip() or "Python Analytics Dashboard"
    dashboard = models.Dashboard(
        id=str(uuid.uuid4()),
        name=name,
        description=body.description,
        theme=body.theme,
        tags=_ensure_python_dashboard_tags(body.tags),
        widgets=body.widgets or [],
        layout=body.layout or [],
        global_filters=body.global_filters or [],
    )
    db.add(dashboard)
    db.commit()
    db.refresh(dashboard)
    _audit(db, "admin", "create", "python_dashboard", dashboard.id, dashboard.name)
    return {"id": dashboard.id, "name": dashboard.name}


@app.get("/api/python-analytics/dashboards/{did}")
async def get_python_dashboard(did: str, db: Session = Depends(get_db)):
    dashboard = db.query(models.Dashboard).filter(models.Dashboard.id == did).first()
    if not dashboard or not _is_python_dashboard(dashboard):
        raise HTTPException(404, "Python analytics dashboard not found")
    return {
        "id": dashboard.id,
        "name": dashboard.name,
        "description": dashboard.description,
        "owner": dashboard.owner,
        "theme": dashboard.theme,
        "tags": dashboard.tags or [],
        "widgets": dashboard.widgets or [],
        "layout": dashboard.layout or [],
        "global_filters": dashboard.global_filters or [],
        "is_public": dashboard.is_public,
        "share_token": dashboard.share_token,
        "created_at": dashboard.created_at.isoformat() if dashboard.created_at else None,
        "updated_at": dashboard.updated_at.isoformat() if dashboard.updated_at else None,
    }


@app.put("/api/python-analytics/dashboards/{did}")
async def update_python_dashboard(did: str, body: PythonDashboardUpdate, db: Session = Depends(get_db)):
    dashboard = db.query(models.Dashboard).filter(models.Dashboard.id == did).first()
    if not dashboard or not _is_python_dashboard(dashboard):
        raise HTTPException(404, "Python analytics dashboard not found")

    update_data = body.model_dump(exclude_none=True)
    for key, value in update_data.items():
        setattr(dashboard, key, value)

    dashboard.tags = _ensure_python_dashboard_tags(dashboard.tags)
    dashboard.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(dashboard)
    _audit(db, "admin", "update", "python_dashboard", dashboard.id, dashboard.name)
    return {"id": dashboard.id, "name": dashboard.name}


@app.delete("/api/python-analytics/dashboards/{did}", status_code=204)
async def delete_python_dashboard(did: str, db: Session = Depends(get_db)):
    dashboard = db.query(models.Dashboard).filter(models.Dashboard.id == did).first()
    if dashboard and _is_python_dashboard(dashboard):
        _audit(db, "admin", "delete", "python_dashboard", dashboard.id, dashboard.name)
        db.delete(dashboard)
        db.commit()


@app.post("/api/python-analytics/dashboards/{did}/duplicate")
async def duplicate_python_dashboard(did: str, db: Session = Depends(get_db)):
    src = db.query(models.Dashboard).filter(models.Dashboard.id == did).first()
    if not src or not _is_python_dashboard(src):
        raise HTTPException(404, "Python analytics dashboard not found")

    copy = models.Dashboard(
        id=str(uuid.uuid4()),
        name=f"{src.name} (Copy)",
        description=src.description,
        theme=src.theme,
        widgets=src.widgets,
        layout=src.layout,
        global_filters=src.global_filters,
        tags=_ensure_python_dashboard_tags(src.tags),
    )
    db.add(copy)
    db.commit()
    db.refresh(copy)
    _audit(db, "admin", "create", "python_dashboard", copy.id, copy.name, detail=f"Duplicated from {src.id}")
    return {"id": copy.id, "name": copy.name}


# ─── QUERY ENGINE (for chart data) ────────────────────────────────────────────

_PIPELINE_METADATA_KEYS = {
    "status", "rows", "destination", "note", "path",
    "table", "collection", "index", "bucket", "key",
    "message", "error", "response_status",
}
_PIPELINE_METADATA_MARKERS = {
    "path", "destination", "table", "collection", "index", "bucket", "key", "note"
}


def _looks_like_pipeline_metadata_row(row: dict) -> bool:
    if not isinstance(row, dict) or not row:
        return False
    keys = {str(k) for k in row.keys()}
    if keys.issubset(_PIPELINE_METADATA_KEYS) and bool(keys.intersection(_PIPELINE_METADATA_MARKERS)):
        return True
    return False


def _safe_json_text(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return str(value)


def _flatten_json_row_for_visualization(
    row: Any,
    max_depth: int = 6,
    array_index_limit: int = 5,
) -> Dict[str, Any]:
    import math

    out: Dict[str, Any] = {}
    safe_depth = max(1, min(int(max_depth or 6), 12))
    safe_array_limit = max(0, min(int(array_index_limit or 5), 50))

    def _assign(path: str, value: Any) -> None:
        key = path if path else "value"
        if isinstance(value, (dict, list)):
            out[key] = _safe_json_text(value)
            return
        out[key] = value

    def _as_float(value: Any) -> Optional[float]:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            try:
                f = float(value)
            except Exception:
                return None
            return f if math.isfinite(f) else None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                f = float(text)
            except Exception:
                return None
            return f if math.isfinite(f) else None
        return None

    def _emit_numeric_array_stats(base_key: str, values: List[Any]) -> None:
        numeric_values: List[float] = []
        for item in values:
            as_num = _as_float(item)
            if as_num is not None:
                numeric_values.append(as_num)
        if not numeric_values:
            return
        out[f"{base_key}._sum"] = sum(numeric_values)
        out[f"{base_key}._avg"] = sum(numeric_values) / len(numeric_values)
        out[f"{base_key}._min"] = min(numeric_values)
        out[f"{base_key}._max"] = max(numeric_values)
        out[f"{base_key}._count_numeric"] = len(numeric_values)

    def _walk(value: Any, path: str, depth: int) -> None:
        if depth > safe_depth:
            _assign(path, value)
            return

        if isinstance(value, dict):
            if path:
                out[path] = _safe_json_text(value)
            if not value:
                _assign(path, {})
                return
            for raw_key, child in list(value.items())[:200]:
                key = str(raw_key)
                child_path = f"{path}.{key}" if path else key
                _walk(child, child_path, depth + 1)
            return

        if isinstance(value, list):
            count_key = f"{path}._count" if path else "value._count"
            out[count_key] = len(value)
            if path:
                out[path] = _safe_json_text(value)
            elif "value" not in out:
                out["value"] = _safe_json_text(value)

            if not value or safe_array_limit <= 0:
                return

            sample_items = value[:safe_array_limit]

            if all(not isinstance(item, (dict, list)) for item in sample_items):
                values_key = f"{path}._values" if path else "value._values"
                out[values_key] = _safe_json_text(sample_items)
                first_key = f"{path}._first" if path else "value._first"
                out[first_key] = sample_items[0] if sample_items else None
                _emit_numeric_array_stats(f"{path}._values" if path else "value._values", sample_items)
            else:
                dict_keys: List[str] = []
                for item in sample_items:
                    if not isinstance(item, dict):
                        continue
                    for k in list(item.keys())[:100]:
                        sk = str(k)
                        if sk not in dict_keys:
                            dict_keys.append(sk)
                for key in dict_keys[:100]:
                    values: List[Any] = []
                    for item in sample_items:
                        if not isinstance(item, dict) or key not in item:
                            continue
                        child_value = item.get(key)
                        values.append(child_value)
                    if values:
                        wildcard_key = f"{path}[].{key}" if path else f"value[].{key}"
                        first_value = None
                        for candidate in values:
                            if candidate is not None:
                                first_value = candidate
                                break
                        if isinstance(first_value, (dict, list)):
                            out[wildcard_key] = _safe_json_text(first_value)
                        else:
                            out[wildcard_key] = first_value
                        out[f"{wildcard_key}._values"] = _safe_json_text(values)
                        _emit_numeric_array_stats(wildcard_key, values)

            for idx, item in enumerate(sample_items):
                idx_path = f"{path}[{idx}]" if path else f"value[{idx}]"
                _walk(item, idx_path, depth + 1)
            return

        _assign(path, value)

    if isinstance(row, dict):
        _walk(row, "", 0)
    else:
        _walk(row, "value", 0)
    return out


def _rows_have_nested_json_values(rows: List[Any], sample_limit: int = 300) -> bool:
    for row in rows[: max(1, sample_limit)]:
        if not isinstance(row, dict):
            continue
        for value in row.values():
            if isinstance(value, (dict, list)):
                return True
    return False


def _flatten_json_rows_for_visualization(
    rows: List[Any],
    max_depth: int = 6,
    array_index_limit: int = 5,
) -> List[Dict[str, Any]]:
    flattened: List[Dict[str, Any]] = []
    for row in rows:
        flattened.append(
            _flatten_json_row_for_visualization(
                row,
                max_depth=max_depth,
                array_index_limit=array_index_limit,
            )
        )
    return flattened


def _read_tabular_output_file(file_path: str) -> list:
    if not file_path or not _os.path.isfile(file_path):
        return []

    suffix = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else ""
    try:
        import pandas as pd
        if suffix == "csv":
            df = pd.read_csv(file_path)
            return df.fillna("").to_dict(orient="records")
        if suffix in ("xlsx", "xls"):
            df = pd.read_excel(file_path)
            return df.fillna("").to_dict(orient="records")
        if suffix == "parquet":
            df = pd.read_parquet(file_path)
            return df.fillna("").to_dict(orient="records")
        if suffix == "json":
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                if isinstance(payload, list):
                    return [row for row in payload if isinstance(row, dict)]
                if isinstance(payload, dict):
                    nested = payload.get("data")
                    if isinstance(nested, list):
                        return [row for row in nested if isinstance(row, dict)]
                    return [payload]
            except Exception:
                pass
            df = pd.read_json(file_path)
            return df.fillna("").to_dict(orient="records")
    except Exception:
        return []
    return []


def _rows_from_pipeline_result(result_value: Any) -> list:
    if isinstance(result_value, list):
        if not result_value:
            return []

        if all(isinstance(item, dict) for item in result_value):
            first = result_value[0]

            if _looks_like_pipeline_metadata_row(first):
                path = first.get("path")
                if isinstance(path, str):
                    file_rows = _read_tabular_output_file(path)
                    if file_rows:
                        return file_rows
                nested = first.get("data")
                if isinstance(nested, list) and all(isinstance(row, dict) for row in nested):
                    return nested
                return []

            return [row for row in result_value if isinstance(row, dict)]

        return [{"value": row} for row in result_value]

    if isinstance(result_value, dict):
        if _looks_like_pipeline_metadata_row(result_value):
            path = result_value.get("path")
            if isinstance(path, str):
                return _read_tabular_output_file(path)
            nested = result_value.get("data")
            if isinstance(nested, list) and all(isinstance(row, dict) for row in nested):
                return nested
            return []
        nested = result_value.get("data")
        if isinstance(nested, list) and all(isinstance(row, dict) for row in nested):
            return nested
        return [result_value]

    return []


def _extract_rows_from_pipeline_execution(node_results: Any, preferred_node_id: Optional[str] = None) -> list:
    payload = node_results
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return []

    if not isinstance(payload, dict) or not payload:
        return []

    if preferred_node_id and preferred_node_id in payload:
        chosen_rows = _rows_from_pipeline_result(payload.get(preferred_node_id))
        if chosen_rows:
            return chosen_rows

    for _, result_value in reversed(list(payload.items())):
        rows = _rows_from_pipeline_result(result_value)
        if rows:
            return rows

    return []


def _mlops_prediction_log_score(log: dict) -> int:
    if not isinstance(log, dict):
        return -1

    output_sample = log.get("output_sample")
    if not isinstance(output_sample, list) or not output_sample:
        return -1

    keys = set()
    for row in output_sample[:5]:
        if isinstance(row, dict):
            keys.update(str(k).lower() for k in row.keys())

    label = str(log.get("nodeLabel") or "").lower()
    score = 0

    if "forecast" in label:
        score += 80
    if "predict" in label or "score" in label or "inference" in label:
        score += 60

    prediction_keys = {
        "prediction", "predicted", "forecast", "forecast_date",
        "lower_bound", "upper_bound", "probability", "score", "y_pred",
    }
    overlap = keys.intersection(prediction_keys)
    if overlap:
        score += 90 + (len(overlap) * 5)

    if any(k.endswith("_pred") or k.startswith("pred_") for k in keys):
        score += 45

    score += min(20, len(output_sample) * 2)
    return score


def _mlops_parse_date(raw: Optional[str]) -> Optional[date]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except Exception:
        pass
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _mlops_resolve_date_window(
    start_raw: Optional[str],
    end_raw: Optional[str],
) -> tuple[Optional[date], Optional[date]]:
    start_date = _mlops_parse_date(start_raw)
    end_date = _mlops_parse_date(end_raw)

    if start_date and not end_date:
        end_date = start_date
    elif end_date and not start_date:
        start_date = end_date

    if start_date and end_date and start_date > end_date:
        start_date, end_date = end_date, start_date

    return start_date, end_date


def _mlops_date_sequence(start_date: date, end_date: date, max_days: int = 3660) -> List[date]:
    if max_days < 1:
        max_days = 1
    day_span = (end_date - start_date).days
    day_span = max(0, min(day_span, max_days - 1))
    return [start_date + timedelta(days=idx) for idx in range(day_span + 1)]


def _mlops_apply_prediction_date_range(
    rows: List[dict],
    start_date: Optional[date],
    end_date: Optional[date],
    mode: str,
) -> List[dict]:
    if not rows or not start_date or not end_date:
        return rows
    expanded_dates = _mlops_date_sequence(start_date, end_date, 3660)
    if not expanded_dates:
        return rows

    date_field = "forecast_date"
    key_lookup: Dict[str, str] = {}
    for row in rows:
        for key in row.keys():
            lower = str(key).lower()
            if lower not in key_lookup:
                key_lookup[lower] = str(key)
    for candidate in ("forecast_date", "prediction_date", "date", "ds", "timestamp"):
        if candidate in key_lookup:
            date_field = key_lookup[candidate]
            break

    ranged_rows = []
    for idx, current_date in enumerate(expanded_dates):
        template = rows[idx % len(rows)]
        next_row = dict(template)
        next_row[date_field] = current_date.isoformat()
        next_row["_prediction_index"] = idx + 1
        next_row["_mlops_mode"] = mode
        next_row["_prediction_start_date"] = start_date.isoformat()
        next_row["_prediction_end_date"] = end_date.isoformat()
        ranged_rows.append(next_row)
    return ranged_rows


def _mlops_visualization_rows_from_h2o(
    run: models.MLOpsH2ORun,
    workflow: models.MLOpsWorkflow,
    mode: str,
    start_date: Optional[date],
    end_date: Optional[date],
) -> List[dict]:
    config_obj = run.config if isinstance(run.config, dict) else {}
    last_eval = _h2o_run_last_evaluation(config_obj)
    run_label = _h2o_run_label(config_obj)
    metrics = run.metrics if isinstance(run.metrics, dict) else {}
    metadata = {
        "_mlops_run_id": run.id,
        "_mlops_workflow_id": workflow.id,
        "_mlops_workflow_name": workflow.name,
        "_mlops_model_version": str(run.model_id or run.model_path or ""),
        "_mlops_started_at": run.created_at.isoformat() if run.created_at else None,
        "_mlops_task": run.task or "auto",
        "_mlops_target_column": run.target_column or "",
        "_mlops_model_label": run_label or "",
    }

    if mode == "evaluation":
        eval_rows_raw = last_eval.get("enriched_rows")
        eval_rows = [row for row in (eval_rows_raw or []) if isinstance(row, dict)]
        if eval_rows:
            enriched_rows: List[Dict[str, Any]] = []
            for idx, row in enumerate(eval_rows):
                next_row = dict(row)
                next_row.update(metadata)
                next_row["_prediction_index"] = idx + 1
                next_row["_mlops_mode"] = "h2o_evaluation"
                next_row["_mlops_source"] = "h2o"
                next_row["_mlops_last_evaluated_at"] = last_eval.get("evaluated_at")
                enriched_rows.append(next_row)
            return _mlops_apply_prediction_date_range(
                enriched_rows,
                start_date,
                end_date,
                "h2o_evaluation_range",
            )

        avp_rows_raw = last_eval.get("actual_vs_predicted_preview")
        avp_rows = [row for row in (avp_rows_raw or []) if isinstance(row, dict)]
        if avp_rows:
            out_rows: List[Dict[str, Any]] = []
            for idx, row in enumerate(avp_rows):
                next_row = dict(row)
                next_row.update(metadata)
                next_row["_prediction_index"] = idx + 1
                next_row["_mlops_mode"] = "h2o_evaluation_preview"
                next_row["_mlops_source"] = "h2o"
                next_row["_mlops_last_evaluated_at"] = last_eval.get("evaluated_at")
                out_rows.append(next_row)
            return out_rows

        eval_metrics = last_eval.get("metrics")
        if isinstance(eval_metrics, dict) and eval_metrics:
            row = dict(eval_metrics)
            row.update(metadata)
            row["_mlops_mode"] = "h2o_evaluation_metrics"
            row["_mlops_source"] = "h2o"
            row["_mlops_last_evaluated_at"] = last_eval.get("evaluated_at")
            row["_mlops_evaluation_target_column"] = last_eval.get("target_column")
            row["_mlops_output_fields"] = ",".join(last_eval.get("output_fields") or [])
            return [row]

    if mode == "metrics":
        if not metrics:
            return []
        row = dict(metrics)
        row.update(metadata)
        row["_mlops_mode"] = "h2o_metrics"
        row["_mlops_source"] = "h2o"
        row["_mlops_last_evaluated_at"] = last_eval.get("evaluated_at")
        row["_mlops_output_fields"] = ",".join(last_eval.get("output_fields") or [])
        return [row]

    leaderboard_rows = []
    for idx, entry in enumerate(run.leaderboard or []):
        if not isinstance(entry, dict):
            continue
        next_row = dict(entry)
        next_row.update(metadata)
        next_row["_prediction_index"] = idx + 1
        next_row["_mlops_mode"] = "h2o_predictions"
        next_row["_mlops_source"] = "h2o"
        leaderboard_rows.append(next_row)

    if leaderboard_rows:
        return _mlops_apply_prediction_date_range(
            leaderboard_rows,
            start_date,
            end_date,
            "h2o_predictions_range",
        )

    fallback_eval_rows = [row for row in (last_eval.get("enriched_rows") or []) if isinstance(row, dict)]
    if fallback_eval_rows:
        out_rows: List[Dict[str, Any]] = []
        for idx, row in enumerate(fallback_eval_rows):
            next_row = dict(row)
            next_row.update(metadata)
            next_row["_prediction_index"] = idx + 1
            next_row["_mlops_mode"] = "h2o_evaluation_fallback"
            next_row["_mlops_source"] = "h2o"
            next_row["_mlops_last_evaluated_at"] = last_eval.get("evaluated_at")
            out_rows.append(next_row)
        return _mlops_apply_prediction_date_range(
            out_rows,
            start_date,
            end_date,
            "h2o_evaluation_fallback_range",
        )

    if metrics:
        row = dict(metrics)
        row.update(metadata)
        row["_mlops_mode"] = "h2o_metrics_fallback"
        row["_mlops_source"] = "h2o"
        return [row]

    return [{
        "status": run.status,
        "task": run.task or "auto",
        "target_column": run.target_column or "",
        "feature_count": len(run.feature_columns or []),
        "row_count": int(run.row_count or 0),
        "train_rows": int(run.train_rows or 0),
        "test_rows": int(run.test_rows or 0),
        "_mlops_mode": "h2o_summary",
        "_mlops_source": "h2o",
        **metadata,
    }]


def _mlops_visualization_rows(
    db: Session,
    workflow_id: str,
    run_id: Optional[str] = None,
    node_id: Optional[str] = None,
    output_mode: str = "predictions",
    prediction_start_date: Optional[str] = None,
    prediction_end_date: Optional[str] = None,
) -> list:
    wf = db.query(models.MLOpsWorkflow).filter(models.MLOpsWorkflow.id == workflow_id).first()
    if not wf:
        raise HTTPException(404, "MLOps workflow not found")

    start_date, end_date = _mlops_resolve_date_window(prediction_start_date, prediction_end_date)
    mode = (output_mode or "predictions").strip().lower()

    run_query = db.query(models.MLOpsRun).filter(models.MLOpsRun.workflow_id == workflow_id)
    h2o_run_query = db.query(models.MLOpsH2ORun).filter(models.MLOpsH2ORun.workflow_id == workflow_id)

    nodes_by_id: Dict[str, Dict[str, str]] = {}
    for node in wf.nodes or []:
        if not isinstance(node, dict):
            continue
        nid = str(node.get("id") or "")
        if not nid:
            continue
        node_data = node.get("data", {}) if isinstance(node.get("data", {}), dict) else {}
        nodes_by_id[nid] = {
            "label": str(node_data.get("label") or node.get("label") or nid),
            "type": str(node_data.get("nodeType") or node.get("type") or ""),
        }

    if mode == "monitor":
        runs = run_query.order_by(models.MLOpsRun.started_at.asc()).all()
        h2o_runs = h2o_run_query.order_by(models.MLOpsH2ORun.created_at.asc()).all()
        rows = []
        combined_rows: List[tuple[float, Dict[str, Any]]] = []
        for idx, run in enumerate(runs):
            run_started = run.started_at
            run_date = run_started.date() if run_started else None
            if start_date and (not run_date or run_date < start_date):
                continue
            if end_date and (not run_date or run_date > end_date):
                continue

            duration_seconds = None
            if run.started_at and run.finished_at:
                duration_seconds = (run.finished_at - run.started_at).total_seconds()

            row: Dict[str, Any] = {
                "run_index": idx + 1,
                "workflow_id": wf.id,
                "workflow_name": wf.name,
                "run_id": run.id,
                "status": run.status,
                "artifact_rows": run.artifact_rows or 0,
                "model_version": run.model_version or "",
                "started_at": run.started_at.isoformat() if run.started_at else None,
                "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                "run_date": run_date.isoformat() if run_date else None,
                "duration_seconds": duration_seconds,
                "success_flag": 1 if run.status == "success" else 0,
                "failed_flag": 1 if run.status == "failed" else 0,
                "_mlops_mode": "monitor",
            }
            metrics = run.metrics if isinstance(run.metrics, dict) else {}
            for mk, mv in metrics.items():
                if mv is None or isinstance(mv, (str, int, float, bool)):
                    row[str(mk)] = mv
            row["_mlops_source"] = "mlops"
            sort_ts = (run.started_at.timestamp() if run.started_at else 0.0)
            combined_rows.append((sort_ts, row))

        for run in h2o_runs:
            run_started = run.created_at or run.updated_at or run.finished_at
            run_date = run_started.date() if run_started else None
            if start_date and (not run_date or run_date < start_date):
                continue
            if end_date and (not run_date or run_date > end_date):
                continue

            duration_seconds = None
            if run.created_at and run.finished_at:
                duration_seconds = (run.finished_at - run.created_at).total_seconds()

            row: Dict[str, Any] = {
                "workflow_id": wf.id,
                "workflow_name": wf.name,
                "run_id": run.id,
                "status": run.status,
                "artifact_rows": int(run.row_count or 0),
                "model_version": str(run.model_id or ""),
                "started_at": run.created_at.isoformat() if run.created_at else None,
                "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                "run_date": run_date.isoformat() if run_date else None,
                "duration_seconds": duration_seconds,
                "success_flag": 1 if run.status == "success" else 0,
                "failed_flag": 1 if run.status == "failed" else 0,
                "task": run.task or "auto",
                "_mlops_mode": "monitor",
                "_mlops_source": "h2o",
            }
            metrics = run.metrics if isinstance(run.metrics, dict) else {}
            for mk, mv in metrics.items():
                if mv is None or isinstance(mv, (str, int, float, bool)):
                    row[str(mk)] = mv
            sort_ts = (run_started.timestamp() if run_started else 0.0)
            combined_rows.append((sort_ts, row))

        combined_rows.sort(key=lambda item: item[0])
        for idx, (_, row) in enumerate(combined_rows):
            row["run_index"] = idx + 1
            rows.append(row)
        return rows

    standard_run = None
    h2o_run = None

    if run_id:
        standard_run = run_query.filter(models.MLOpsRun.id == run_id).first()
        if not standard_run:
            h2o_run = h2o_run_query.filter(models.MLOpsH2ORun.id == run_id).first()
        if not standard_run and not h2o_run:
            raise HTTPException(404, "MLOps run not found for this workflow")
    else:
        standard_run = run_query.filter(models.MLOpsRun.status == "success")\
            .order_by(models.MLOpsRun.started_at.desc()).first()
        if not standard_run:
            standard_run = run_query.order_by(models.MLOpsRun.started_at.desc()).first()
        if not standard_run:
            h2o_run = h2o_run_query.filter(models.MLOpsH2ORun.status == "success")\
                .order_by(models.MLOpsH2ORun.created_at.desc()).first()
        if not h2o_run:
            h2o_run = h2o_run_query.order_by(models.MLOpsH2ORun.created_at.desc()).first()

    if h2o_run and not standard_run:
        return _mlops_visualization_rows_from_h2o(
            run=h2o_run,
            workflow=wf,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
        )

    run = standard_run
    if not run:
        raise HTTPException(400, "No MLOps runs found for selected workflow")

    if mode == "metrics":
        metrics = run.metrics if isinstance(run.metrics, dict) else {}
        if not metrics:
            return []
        row = dict(metrics)
        row["_mlops_run_id"] = run.id
        row["_mlops_workflow_id"] = wf.id
        row["_mlops_workflow_name"] = wf.name
        row["_mlops_model_version"] = run.model_version or ""
        row["_mlops_started_at"] = run.started_at.isoformat() if run.started_at else None
        row["_mlops_mode"] = "metrics"
        return [row]

    logs = [log for log in (run.logs or []) if isinstance(log, dict)]
    log_candidates = [
        log for log in logs
        if isinstance(log.get("output_sample"), list) and len(log.get("output_sample") or []) > 0
    ]

    chosen_log = None
    if node_id:
        by_node = [log for log in log_candidates if str(log.get("nodeId") or "") == node_id]
        if by_node:
            chosen_log = max(by_node, key=_mlops_prediction_log_score)
    if not chosen_log and log_candidates:
        chosen_log = max(log_candidates, key=_mlops_prediction_log_score)

    if not chosen_log:
        metrics = run.metrics if isinstance(run.metrics, dict) else {}
        if metrics:
            row = dict(metrics)
            row["_mlops_run_id"] = run.id
            row["_mlops_workflow_id"] = wf.id
            row["_mlops_workflow_name"] = wf.name
            row["_mlops_model_version"] = run.model_version or ""
            row["_mlops_started_at"] = run.started_at.isoformat() if run.started_at else None
            row["_mlops_mode"] = "metrics_fallback"
            return [row]
        return []

    selected_node_id = str(chosen_log.get("nodeId") or "")
    node_meta = nodes_by_id.get(selected_node_id) if selected_node_id else None
    node_label = str(chosen_log.get("nodeLabel") or (node_meta or {}).get("label") or "")
    node_type = str((node_meta or {}).get("type") or "")

    rows = []
    for idx, row in enumerate(chosen_log.get("output_sample") or []):
        if not isinstance(row, dict):
            continue
        enriched = dict(row)
        enriched["_prediction_index"] = idx + 1
        enriched["_mlops_run_id"] = run.id
        enriched["_mlops_workflow_id"] = wf.id
        enriched["_mlops_workflow_name"] = wf.name
        enriched["_mlops_model_version"] = run.model_version or ""
        enriched["_mlops_node_id"] = selected_node_id
        enriched["_mlops_node_label"] = node_label
        enriched["_mlops_node_type"] = node_type
        enriched["_mlops_started_at"] = run.started_at.isoformat() if run.started_at else None
        enriched["_mlops_mode"] = "predictions"
        rows.append(enriched)

    return _mlops_apply_prediction_date_range(rows, start_date, end_date, "predictions_range")


def _load_source_rows_for_query(body: dict, db: Session, apply_sql: bool = True) -> list:
    import pandas as pd

    source_type = str(body.get("source_type", "sample")).strip().lower()
    dataset = body.get("dataset", "sales")

    if source_type == "sample":
        data = _get_sample_data(dataset)
    elif source_type == "pipeline":
        pid = body.get("pipeline_id")
        pipeline_node_id = body.get("pipeline_node_id")
        p = db.query(models.Pipeline).filter(models.Pipeline.id == pid).first()
        if not p:
            raise HTTPException(404, "Pipeline not found")
        exec_row = db.query(models.Execution).filter(
            models.Execution.pipeline_id == pid,
            models.Execution.status == "success"
        ).order_by(models.Execution.started_at.desc()).first()
        if not exec_row or not exec_row.node_results:
            raise HTTPException(400, "No successful execution found with data")
        data = _extract_rows_from_pipeline_execution(exec_row.node_results, pipeline_node_id)
        if not data:
            raise HTTPException(400, "No tabular pipeline output found in latest successful execution")
    elif source_type == "file":
        file_path = str(body.get("file_path", "")).strip()
        if not _os.path.isfile(file_path):
            raise HTTPException(404, f"File not found: {file_path}")
        suffix = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else ""
        if suffix == "csv":
            df = pd.read_csv(file_path)
        elif suffix in ("xlsx", "xls"):
            df = pd.read_excel(file_path)
        elif suffix == "json":
            df = pd.read_json(file_path)
        elif suffix == "parquet":
            df = pd.read_parquet(file_path)
        else:
            raise HTTPException(400, f"Unsupported file: .{suffix}")
        data = df.fillna("").to_dict(orient="records")
    elif source_type == "mlops":
        workflow_id = str(body.get("mlops_workflow_id") or body.get("workflow_id") or "").strip()
        if not workflow_id:
            raise HTTPException(400, "mlops_workflow_id is required for source_type=mlops")
        run_id = str(body.get("mlops_run_id") or body.get("run_id") or "").strip() or None
        node_id = str(body.get("mlops_node_id") or body.get("node_id") or "").strip() or None
        output_mode = str(body.get("mlops_output_mode") or body.get("output_mode") or "predictions")
        prediction_start_date = str(
            body.get("mlops_prediction_start_date")
            or body.get("prediction_start_date")
            or body.get("start_date")
            or ""
        ).strip() or None
        prediction_end_date = str(
            body.get("mlops_prediction_end_date")
            or body.get("prediction_end_date")
            or body.get("end_date")
            or ""
        ).strip() or None
        data = _mlops_visualization_rows(
            db=db,
            workflow_id=workflow_id,
            run_id=run_id,
            node_id=node_id,
            output_mode=output_mode,
            prediction_start_date=prediction_start_date,
            prediction_end_date=prediction_end_date,
        )
    else:
        data = _get_sample_data(dataset)

    if not data:
        return []

    if apply_sql:
        sql = body.get("sql")
        if sql:
            try:
                from pandasql import sqldf
                df2 = pd.DataFrame(data)
                result_df = sqldf(str(sql), {"data": df2})
                data = result_df.fillna("").to_dict(orient="records")
            except Exception:
                pass

    return data


def _apply_drill_filters_to_rows(rows: list, drill_filters: Any) -> list:
    if not rows or not isinstance(drill_filters, list):
        return rows

    normalized_filters: List[Dict[str, Any]] = []
    for raw_filter in drill_filters:
        if not isinstance(raw_filter, dict):
            continue
        field = str(raw_filter.get("field") or "").strip()
        if not field:
            continue
        normalized_filters.append({
            "field": field,
            "value": raw_filter.get("value"),
        })

    if not normalized_filters:
        return rows

    def _values_match(left: Any, right: Any) -> bool:
        if right is None:
            return left is None or str(left).strip() == ""

        left_num = None
        right_num = None
        try:
            left_num = float(left)
        except Exception:
            left_num = None
        try:
            right_num = float(right)
        except Exception:
            right_num = None

        if left_num is not None and right_num is not None:
            return abs(left_num - right_num) < 1e-12

        return str(left).strip().lower() == str(right).strip().lower()

    filtered_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        include = True
        for drill_filter in normalized_filters:
            field = drill_filter["field"]
            target_value = drill_filter["value"]

            # Keep row when filter field does not exist in this datasource.
            if field not in row:
                continue

            if not _values_match(row.get(field), target_value):
                include = False
                break
        if include:
            filtered_rows.append(row)

    return filtered_rows


# ─── NLP CHAT STUDIO ──────────────────────────────────────────────────────────

_NLP_SENTENCE_MODEL: Any = None
_NLP_QUERY_STOPWORDS = {
    "a", "an", "the", "is", "are", "am", "was", "were", "be", "to", "of",
    "in", "on", "for", "from", "with", "and", "or", "by", "at", "as", "it",
    "this", "that", "these", "those", "show", "find", "give", "get", "what",
    "which", "who", "whom", "how", "please", "me", "us", "about", "into",
    "using", "use", "between", "compare", "versus", "vs",
}


def _nlp_tokens(text: Any) -> List[str]:
    import re

    return [tok.lower() for tok in re.findall(r"[A-Za-z0-9_]+", str(text or ""))]


def _nlp_query_tokens(text: Any) -> List[str]:
    return [tok for tok in _nlp_tokens(text) if tok and not tok.isdigit() and tok not in _NLP_QUERY_STOPWORDS]


def _nlp_limit_rows(rows: list, max_rows: int = 3000) -> list:
    if not isinstance(rows, list):
        return []
    if len(rows) <= max_rows:
        return rows
    step = max(1, len(rows) // max_rows)
    return rows[::step][:max_rows]


def _nlp_row_to_text(row: dict, columns: List[str]) -> str:
    if not isinstance(row, dict):
        return ""
    parts: List[str] = []
    for col in columns:
        value = row.get(col)
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        if len(text) > 140:
            text = text[:137] + "..."
        parts.append(f"{col}: {text}")
    return " | ".join(parts)


def _nlp_infer_schema(rows: list) -> dict:
    import pandas as pd

    tabular_rows = [r for r in rows if isinstance(r, dict)]
    if not tabular_rows:
        return {
            "columns": [],
            "column_details": [],
            "numeric_columns": [],
            "datetime_columns": [],
            "categorical_columns": [],
            "text_columns": [],
        }

    df = pd.DataFrame(tabular_rows)
    columns = [str(c) for c in df.columns]
    numeric_cols: List[str] = []
    datetime_cols: List[str] = []
    categorical_cols: List[str] = []
    text_cols: List[str] = []
    details: List[dict] = []

    for col in columns:
        series = df[col]
        non_null = series.dropna()
        non_empty = non_null[non_null.astype(str).str.strip() != ""]
        total = max(len(non_empty), 1)

        num_series = pd.to_numeric(non_empty, errors="coerce")
        num_ratio = float(num_series.notna().sum() / total)
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            dt_series = pd.to_datetime(non_empty, errors="coerce", utc=True)
        dt_ratio = float(dt_series.notna().sum() / total)
        unique_count = int(non_empty.astype(str).nunique()) if len(non_empty) else 0
        null_count = int(series.isna().sum())

        col_type = "text"
        if num_ratio >= 0.8:
            col_type = "numeric"
            numeric_cols.append(col)
        elif dt_ratio >= 0.8:
            col_type = "datetime"
            datetime_cols.append(col)
        elif unique_count <= max(30, int(len(non_empty) * 0.5)):
            col_type = "categorical"
            categorical_cols.append(col)
        else:
            text_cols.append(col)

        details.append({
            "name": col,
            "type": col_type,
            "null_count": null_count,
            "unique_count": unique_count,
            "numeric_ratio": round(num_ratio, 4),
            "datetime_ratio": round(dt_ratio, 4),
        })

    return {
        "columns": columns,
        "column_details": details,
        "numeric_columns": numeric_cols,
        "datetime_columns": datetime_cols,
        "categorical_columns": categorical_cols,
        "text_columns": text_cols,
    }


def _nlp_build_vocabulary(rows: list, schema: dict, value_cap: int = 250) -> List[str]:
    vocab = set()
    columns = schema.get("columns") or []
    categorical_cols = schema.get("categorical_columns") or []

    for col in columns:
        col_text = str(col).strip().lower()
        if col_text:
            vocab.add(col_text)
        for tok in _nlp_tokens(col):
            if tok:
                vocab.add(tok)

    for col in categorical_cols[:8]:
        seen = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            value = row.get(col)
            if value is None:
                continue
            value_text = str(value).strip().lower()
            if not value_text:
                continue
            if len(value_text) <= 48:
                vocab.add(value_text)
            for tok in _nlp_tokens(value_text):
                if len(tok) >= 3:
                    vocab.add(tok)
            seen += 1
            if seen >= value_cap:
                break

    return sorted(vocab)


def _nlp_best_fuzzy_match(token: str, vocabulary: List[str]) -> tuple[str, float]:
    if not token or not vocabulary:
        return "", 0.0
    try:
        from rapidfuzz import process, fuzz

        match = process.extractOne(token, vocabulary, scorer=fuzz.WRatio)
        if not match:
            return "", 0.0
        # rapidfuzz returns (choice, score, index)
        return str(match[0]), float(match[1])
    except Exception:
        import difflib

        best = difflib.get_close_matches(token, vocabulary, n=1, cutoff=0.78)
        if not best:
            return "", 0.0
        ratio = difflib.SequenceMatcher(None, token, best[0]).ratio()
        return str(best[0]), float(ratio * 100.0)


def _nlp_autocorrect_question(question: str, vocabulary: List[str]) -> tuple[str, Dict[str, str]]:
    import re

    vocab_set = {str(v).lower() for v in vocabulary}
    corrections: Dict[str, str] = {}

    def _replace(match: Any) -> str:
        original = str(match.group(0) or "")
        lower = original.lower()
        if len(lower) < 3 or lower.isdigit() or lower in vocab_set or lower in _NLP_QUERY_STOPWORDS:
            return original
        best, score = _nlp_best_fuzzy_match(lower, vocabulary)
        threshold = 90.0 if len(lower) <= 4 else 84.0
        if best and score >= threshold and best != lower:
            corrections[original] = best
            return best
        return original

    corrected = re.sub(r"\b[A-Za-z_][A-Za-z0-9_]*\b", _replace, str(question or ""))
    return corrected, corrections


def _nlp_semantic_rank_rows(
    rows: list,
    query: str,
    columns: List[str],
    top_k: int = 12,
    mode: str = "auto",
) -> tuple[List[dict], str, int, int]:
    import numpy as np

    filtered_rows = [r for r in rows if isinstance(r, dict)]
    total_rows = len(filtered_rows)
    indexed_rows = _nlp_limit_rows(filtered_rows, 3000)
    if not indexed_rows:
        return [], "none", total_rows, 0

    texts = [_nlp_row_to_text(row, columns) for row in indexed_rows]
    query_text = str(query or "").strip()
    normalized_mode = str(mode or "auto").strip().lower()
    engine = "token_overlap"
    scores: List[float] = []

    if normalized_mode in {"auto", "embedding"}:
        try:
            global _NLP_SENTENCE_MODEL
            if _NLP_SENTENCE_MODEL is None:
                from sentence_transformers import SentenceTransformer

                model_name = str(_os.getenv("NLP_CHAT_EMBEDDING_MODEL") or "all-MiniLM-L6-v2").strip()
                _NLP_SENTENCE_MODEL = SentenceTransformer(model_name)
            embeds = _NLP_SENTENCE_MODEL.encode([query_text] + texts, normalize_embeddings=True)
            q_vec = np.array(embeds[0], dtype=float)
            d_vec = np.array(embeds[1:], dtype=float)
            raw_scores = d_vec @ q_vec
            scores = [float(v) for v in raw_scores]
            engine = "sentence-transformers"
        except Exception:
            scores = []

    if not scores and normalized_mode in {"auto", "embedding", "tfidf"}:
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics.pairwise import cosine_similarity

            vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=1, stop_words="english")
            matrix = vectorizer.fit_transform(texts + [query_text])
            query_vec = matrix[-1]
            sims = cosine_similarity(matrix[:-1], query_vec).reshape(-1)
            scores = [float(v) for v in sims]
            engine = "tfidf"
        except Exception:
            scores = []

    if not scores:
        q_tokens = set(_nlp_tokens(query_text))
        for text in texts:
            t_tokens = set(_nlp_tokens(text))
            if not q_tokens:
                scores.append(0.0)
                continue
            overlap = len(q_tokens.intersection(t_tokens))
            union = max(len(q_tokens.union(t_tokens)), 1)
            scores.append(float(overlap / union))
        engine = "token_overlap"

    safe_top_k = max(1, min(int(top_k or 12), 50))
    ranked_pairs = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)[:safe_top_k]
    ranked_rows: List[dict] = []
    for idx, score in ranked_pairs:
        ranked_rows.append({
            "row_index": int(idx),
            "score": round(float(score), 6),
            "row": indexed_rows[idx],
        })

    return ranked_rows, engine, total_rows, len(indexed_rows)


def _nlp_pick_fields_from_question(
    question: str,
    columns: List[str],
    numeric_columns: List[str],
    dimension_columns: List[str],
) -> tuple[List[str], List[str]]:
    q_lower = str(question or "").lower()
    q_tokens = _nlp_query_tokens(q_lower)
    if not columns or not q_tokens:
        return [], []

    alias_groups = [
        {"revenue", "sales", "income", "turnover"},
        {"profit", "margin", "earnings"},
        {"cost", "expense", "spend"},
        {"units", "quantity", "volume", "count"},
        {"price", "rate", "value", "amount"},
        {"region", "country", "state", "city", "location", "segment", "category", "channel"},
        {"date", "time", "month", "quarter", "year", "day", "week"},
    ]

    score_by_col: Dict[str, float] = {col: 0.0 for col in columns}
    q_token_set = set(q_tokens)
    lower_to_col = {str(col).lower(): str(col) for col in columns}
    lower_column_names = list(lower_to_col.keys())

    for col in columns:
        col_lower = str(col).lower()
        col_tokens = set(_nlp_tokens(col_lower))
        if col_lower in q_lower:
            score_by_col[col] += 8.0
        overlap = len(col_tokens.intersection(q_token_set))
        if overlap:
            score_by_col[col] += overlap * 2.5

    for token in q_tokens:
        best_col, score = _nlp_best_fuzzy_match(token, lower_column_names)
        if best_col and score >= 86.0:
            matched_col = lower_to_col.get(best_col)
            if matched_col:
                score_by_col[matched_col] += 2.0

    for group in alias_groups:
        if not q_token_set.intersection(group):
            continue
        for col in columns:
            col_lower = str(col).lower()
            if any(alias in col_lower for alias in group):
                score_by_col[col] += 3.0

    ranked = sorted(score_by_col.items(), key=lambda x: x[1], reverse=True)
    metric_ranked = [col for col, score in ranked if col in numeric_columns and score > 0.0]
    dimension_ranked = [col for col, score in ranked if col in dimension_columns and score > 0.0]

    return metric_ranked[:4], dimension_ranked[:4]


def _nlp_relevant_columns(question: str, columns: List[str], metrics: List[str], dimensions: List[str]) -> List[str]:
    q_lower = str(question or "").lower()
    q_tokens = set(_nlp_query_tokens(q_lower))
    selected = [c for c in (metrics + dimensions) if c in columns]

    if not columns:
        return []

    score_by_col: Dict[str, float] = {col: 0.0 for col in columns}
    for col in columns:
        col_lower = str(col).lower()
        col_tokens = set(_nlp_tokens(col_lower))
        if col in selected:
            score_by_col[col] += 6.0
        if col_lower in q_lower:
            score_by_col[col] += 6.0
        overlap = len(col_tokens.intersection(q_tokens))
        if overlap:
            score_by_col[col] += overlap * 2.0

        for token in q_tokens:
            best, score = _nlp_best_fuzzy_match(token, list(col_tokens) + [col_lower])
            if best and score >= 88.0:
                score_by_col[col] += 1.2

    ranked_cols = [col for col, score in sorted(score_by_col.items(), key=lambda x: x[1], reverse=True) if score > 0]
    if not ranked_cols:
        return columns[: min(10, len(columns))]
    return ranked_cols[:12]


def _nlp_one_line_highlight(metric_fields: List[str], group_summary: List[dict], metric_summary: Dict[str, dict]) -> str:
    if group_summary and metric_fields:
        top = sorted(group_summary, key=lambda r: float(r.get("sum") or 0.0), reverse=True)[0]
        metric = metric_fields[0]
        return f"{top.get('dimension')} is leading for {metric} (sum={top.get('sum')}, avg={top.get('avg')})."
    if metric_summary:
        metric = next(iter(metric_summary.keys()))
        stats = metric_summary[metric]
        return f"{metric}: avg={stats.get('avg')}, total={stats.get('sum')}, range={stats.get('min')}..{stats.get('max')}."
    return "No dominant trend detected in current field selection."


def _nlp_format_number(value: Any) -> str:
    try:
        num = float(value)
    except Exception:
        return str(value)
    if abs(num) >= 1000:
        return f"{num:,.2f}"
    return f"{num:.2f}"


def _nlp_to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip().replace(",", "")
            if not text:
                return None
            return float(text)
        return float(value)
    except Exception:
        return None


def _nlp_pick_primary_dimension(question: str, dimension_fields: List[str]) -> Optional[str]:
    if not dimension_fields:
        return None
    q_lower = str(question or "").lower()
    for col in dimension_fields:
        if str(col).lower() in q_lower:
            return col
    return dimension_fields[0]


def _nlp_parse_requested_limit(question: str, default_limit: int, max_limit: int) -> int:
    import re

    safe_default = max(1, min(int(default_limit or 12), max_limit))
    q_lower = str(question or "").lower()
    match = re.search(r"\b(?:top|bottom|first|last)\s+(\d{1,3})\b", q_lower)
    if not match:
        return safe_default
    try:
        requested = int(match.group(1))
    except Exception:
        return safe_default
    return max(1, min(requested, max_limit))


def _nlp_parse_sort_intent(question: str, candidate_columns: List[str], default_key: str) -> tuple[str, bool]:
    import re

    q_lower = str(question or "").lower()
    desc_tokens = ["descending", "desc", "top", "highest", "largest", "max", "greatest"]
    asc_tokens = ["ascending", "asc", "bottom", "lowest", "smallest", "min", "least"]
    descending = True
    if any(tok in q_lower for tok in asc_tokens):
        descending = False
    if any(tok in q_lower for tok in desc_tokens):
        descending = True

    key = default_key
    column_by_lower = {str(col).lower(): str(col) for col in candidate_columns}
    sort_by_match = re.search(r"\bsort\s+by\s+([a-zA-Z0-9_]+)\b", q_lower)
    if sort_by_match:
        requested = sort_by_match.group(1).strip().lower()
        if requested in column_by_lower:
            return column_by_lower[requested], descending

    for special in ["avg", "average", "mean", "sum", "total", "count", "score", "rank"]:
        if special in q_lower:
            mapped = {"average": "avg", "mean": "avg", "total": "sum", "score": "_score", "rank": "_rank"}.get(special, special)
            if mapped in candidate_columns:
                return mapped, descending

    return key, descending


def _nlp_wants_grouping(question: str, dimension_fields: List[str]) -> bool:
    q_lower = str(question or "").lower()
    if any(token in q_lower for token in ["group by", "groupby", "grouped by", "breakdown", "distribution", "compare", "segment", "per ", "wise"]):
        return True
    if "group " in q_lower and any(str(dim).lower() in q_lower for dim in dimension_fields):
        return True

    for dim in dimension_fields:
        dim_lower = str(dim).lower()
        dim_space = dim_lower.replace("_", " ")
        if f"by {dim_lower}" in q_lower or f"by {dim_space}" in q_lower:
            return True
        if f"per {dim_lower}" in q_lower or f"per {dim_space}" in q_lower:
            return True
    return False


def _nlp_parse_group_columns(question: str, dimension_fields: List[str]) -> List[str]:
    import re

    q_lower = str(question or "").lower()
    if not q_lower:
        return []

    dims = [str(d) for d in (dimension_fields or [])]
    if not dims:
        return []

    normalized_map: Dict[str, str] = {}
    for dim in dims:
        dim_lower = dim.lower()
        normalized_map[dim_lower] = dim
        normalized_map[dim_lower.replace("_", " ")] = dim

    stop_tokens = [
        " sort by ", " order by ", " where ", " with ", " having ", " top ", " bottom ",
        " above ", " below ", " over ", " under ", " greater than ", " less than ",
        " between ", " from ", " limit ",
    ]

    def _cut_phrase(text: str) -> str:
        cut = len(text)
        for tok in stop_tokens:
            idx = text.find(tok)
            if idx >= 0:
                cut = min(cut, idx)
        return text[:cut].strip(" ,.;")

    candidates: List[str] = []
    pattern = re.search(r"(?:group\s*by|grouped\s*by|by|per)\s+(.+)", q_lower)
    if pattern:
        phrase = _cut_phrase(pattern.group(1))
        chunks = re.split(r",| and | & |/", phrase)
        for chunk in chunks:
            token = chunk.strip(" ,.;")
            if not token:
                continue
            for key, dim in normalized_map.items():
                if key in token:
                    candidates.append(dim)
                    break

    if not candidates:
        for key, dim in normalized_map.items():
            if re.search(rf"\b{re.escape(key)}\b", q_lower):
                candidates.append(dim)

    unique = [c for c in dict.fromkeys(candidates) if c in dims]
    return unique[:3]


def _nlp_parse_group_agg(question: str) -> str:
    q_lower = str(question or "").lower()
    if any(tok in q_lower for tok in ["average", "avg", "mean"]):
        return "avg"
    if any(tok in q_lower for tok in ["count", "number of"]):
        return "count"
    if any(tok in q_lower for tok in ["minimum", "lowest", "min"]):
        return "min"
    if any(tok in q_lower for tok in ["maximum", "highest", "max"]):
        return "max"
    return "sum"


def _nlp_build_group_rows(
    rows: List[dict],
    question: str,
    dimension_fields: List[str],
    metric_fields: List[str],
    row_limit: int,
) -> tuple[List[str], List[dict]]:
    import pandas as pd

    if not rows:
        return [], []

    group_cols = _nlp_parse_group_columns(question, dimension_fields) or (dimension_fields[:1] if dimension_fields else [])
    if not group_cols:
        return [], []

    df = pd.DataFrame(rows)
    group_cols = [col for col in group_cols if col in df.columns]
    if not group_cols:
        return [], []

    metric_col = next((m for m in metric_fields if m in df.columns), None)
    if metric_col:
        tmp = df[group_cols + [metric_col]].copy()
        tmp[metric_col] = pd.to_numeric(tmp[metric_col], errors="coerce")
        tmp = tmp.dropna(subset=[metric_col])
    else:
        tmp = df[group_cols].copy()

    if tmp.empty:
        return [], []

    for col in group_cols:
        tmp[col] = tmp[col].astype(str).fillna("")

    if metric_col:
        grouped = tmp.groupby(group_cols, dropna=False)[metric_col].agg(["sum", "mean", "count", "min", "max"]).reset_index()
        grouped = grouped.rename(columns={"mean": "avg"})
    else:
        grouped = tmp.groupby(group_cols, dropna=False).size().reset_index(name="count")

    agg_choice = _nlp_parse_group_agg(question)
    numeric_candidates = ["sum", "avg", "count", "min", "max"]
    available_numeric = [c for c in numeric_candidates if c in grouped.columns]
    sort_default = agg_choice if agg_choice in available_numeric else ("sum" if "sum" in available_numeric else "count")
    sort_key, descending = _nlp_parse_sort_intent(
        question=question,
        candidate_columns=group_cols + available_numeric,
        default_key=sort_default,
    )

    if sort_key in available_numeric:
        grouped = grouped.sort_values(by=sort_key, ascending=not descending, na_position="last")
    else:
        grouped = grouped.sort_values(by=sort_key if sort_key in group_cols else group_cols[0], ascending=not descending, na_position="last")

    grouped = grouped.head(max(1, row_limit))
    if grouped.empty:
        return [], []

    table_cols = list(group_cols)
    rows_out: List[dict] = []
    for idx, rec in enumerate(grouped.to_dict(orient="records"), start=1):
        out: Dict[str, Any] = {"_rank": idx}
        score_val = None
        for key in [sort_key, sort_default, "sum", "avg", "count"]:
            if key in rec and _nlp_to_float(rec.get(key)) is not None:
                score_val = _nlp_to_float(rec.get(key))
                break
        out["_score"] = round(float(score_val), 6) if score_val is not None else float(idx)

        for col in group_cols:
            out[col] = rec.get(col)

        if metric_col:
            for col in ["sum", "avg", "count", "min", "max"]:
                if col in rec:
                    out[col] = rec.get(col)
            table_cols = group_cols + [c for c in ["sum", "avg", "count", "min", "max"] if c in rec]
        else:
            out["count"] = rec.get("count", 0)
            table_cols = group_cols + ["count"]

        rows_out.append(out)

    return table_cols, rows_out


def _nlp_extract_question_filters(
    question: str,
    rows: List[dict],
    dimension_columns: List[str],
    metric_candidates: List[str],
    all_columns: Optional[List[str]] = None,
    numeric_columns: Optional[List[str]] = None,
) -> dict:
    import re

    q_lower = str(question or "").lower()
    if not q_lower or not rows:
        return {"categorical": {}, "numeric": None, "numeric_rules": []}

    categorical_filters: Dict[str, str] = {}
    sample_rows = rows[: min(len(rows), 5000)]

    for col in dimension_columns:
        col_lower = str(col).lower()
        value_counts: Dict[str, int] = {}
        for row in sample_rows:
            if not isinstance(row, dict):
                continue
            value = row.get(col)
            if value is None:
                continue
            text = str(value).strip()
            if not text:
                continue
            lower_text = text.lower()
            value_counts[lower_text] = value_counts.get(lower_text, 0) + 1
            if len(value_counts) >= 300:
                break

        if not value_counts:
            continue

        best_value = ""
        best_score = 0.0
        for value, freq in value_counts.items():
            if len(value) < 2:
                continue
            matched = False
            score = 0.0
            if " " in value:
                if value in q_lower:
                    matched = True
                    score += 100.0 + min(len(value), 25)
            else:
                if re.search(rf"\b{re.escape(value)}\b", q_lower):
                    matched = True
                    score += 80.0
            if not matched:
                continue

            if re.search(rf"\b{re.escape(value)}\s+{re.escape(col_lower)}\b", q_lower):
                score += 40.0
            if re.search(rf"\b{re.escape(col_lower)}\s+(?:is|=|equals|equal to|:)?\s*{re.escape(value)}\b", q_lower):
                score += 40.0
            score += min(float(freq) / 20.0, 8.0)
            if value in _NLP_QUERY_STOPWORDS:
                score -= 50.0

            if score > best_score:
                best_score = score
                best_value = value

        if best_value and best_score >= 80.0:
            categorical_filters[col] = best_value

    metric_for_numeric = ""
    for col in metric_candidates:
        if str(col).lower() in q_lower:
            metric_for_numeric = col
            break
    if not metric_for_numeric and metric_candidates:
        metric_for_numeric = metric_candidates[0]

    numeric_rules: List[dict] = []
    numeric_filter = None
    between_match = re.search(
        r"\bbetween\s+(-?\d[\d,]*(?:\.\d+)?)\s+(?:and|to)\s+(-?\d[\d,]*(?:\.\d+)?)\b",
        q_lower,
    )
    if between_match and metric_for_numeric:
        low = _nlp_to_float(between_match.group(1))
        high = _nlp_to_float(between_match.group(2))
        if low is not None and high is not None:
            lo, hi = sorted([low, high])
            numeric_filter = {"column": metric_for_numeric, "op": "between", "low": lo, "high": hi}
            numeric_rules.append(numeric_filter)
    else:
        greater_match = re.search(
            r"\b(?:above|over|greater than|more than|at least|>=|>)\s*(-?\d[\d,]*(?:\.\d+)?)\b",
            q_lower,
        )
        less_match = re.search(
            r"\b(?:below|under|less than|fewer than|at most|<=|<)\s*(-?\d[\d,]*(?:\.\d+)?)\b",
            q_lower,
        )
        if greater_match and metric_for_numeric:
            val = _nlp_to_float(greater_match.group(1))
            if val is not None:
                numeric_filter = {"column": metric_for_numeric, "op": "gte", "value": val}
                numeric_rules.append(numeric_filter)
        elif less_match and metric_for_numeric:
            val = _nlp_to_float(less_match.group(1))
            if val is not None:
                numeric_filter = {"column": metric_for_numeric, "op": "lte", "value": val}
                numeric_rules.append(numeric_filter)

    candidate_columns = list(all_columns or []) or list(dimension_columns or []) + list(metric_candidates or [])
    numeric_set = set(numeric_columns or metric_candidates or [])
    for col in candidate_columns:
        col_text = str(col).strip()
        if not col_text:
            continue
        col_lower = col_text.lower()
        col_space = col_lower.replace("_", " ")
        variants = [col_lower, col_space]

        for col_variant in variants:
            if col in numeric_set:
                greater_patterns = [
                    rf"\b{re.escape(col_variant)}\s*(?:>=|>|at least|greater than|more than)\s*(-?\d[\d,]*(?:\.\d+)?)\b",
                ]
                less_patterns = [
                    rf"\b{re.escape(col_variant)}\s*(?:<=|<|at most|less than|under|below)\s*(-?\d[\d,]*(?:\.\d+)?)\b",
                ]
                between_patterns = [
                    rf"\b{re.escape(col_variant)}\s*(?:between)\s*(-?\d[\d,]*(?:\.\d+)?)\s*(?:and|to)\s*(-?\d[\d,]*(?:\.\d+)?)\b",
                ]
                for p in between_patterns:
                    m = re.search(p, q_lower)
                    if not m:
                        continue
                    lo = _nlp_to_float(m.group(1))
                    hi = _nlp_to_float(m.group(2))
                    if lo is not None and hi is not None:
                        low, high = sorted([lo, hi])
                        numeric_rules.append({"column": col_text, "op": "between", "low": low, "high": high})
                for p in greater_patterns:
                    m = re.search(p, q_lower)
                    if not m:
                        continue
                    val = _nlp_to_float(m.group(1))
                    if val is not None:
                        numeric_rules.append({"column": col_text, "op": "gte", "value": val})
                for p in less_patterns:
                    m = re.search(p, q_lower)
                    if not m:
                        continue
                    val = _nlp_to_float(m.group(1))
                    if val is not None:
                        numeric_rules.append({"column": col_text, "op": "lte", "value": val})
            else:
                eq_patterns = [
                    rf"\b{re.escape(col_variant)}\s*(?:=|is|equals|equal to|:)\s*([a-zA-Z0-9_.\- /]+?)(?=\s+(?:and|or|sort|order|group|by|top|bottom|limit)\b|$)",
                ]
                for p in eq_patterns:
                    m = re.search(p, q_lower)
                    if not m:
                        continue
                    val_text = str(m.group(1) or "").strip(" .,:;\"'")
                    if val_text and val_text not in _NLP_QUERY_STOPWORDS:
                        categorical_filters[col_text] = val_text.lower()

    dedup_rules: List[dict] = []
    seen_rules = set()
    for rule in numeric_rules:
        if not isinstance(rule, dict):
            continue
        key = (
            str(rule.get("column") or ""),
            str(rule.get("op") or ""),
            str(rule.get("value") or ""),
            str(rule.get("low") or ""),
            str(rule.get("high") or ""),
        )
        if key in seen_rules:
            continue
        seen_rules.add(key)
        dedup_rules.append(rule)

    numeric_primary = dedup_rules[0] if dedup_rules else None
    return {"categorical": categorical_filters, "numeric": numeric_primary, "numeric_rules": dedup_rules}


def _nlp_apply_row_filters(rows: List[dict], filters: dict) -> List[dict]:
    categorical = dict((filters or {}).get("categorical") or {})
    numeric = (filters or {}).get("numeric")
    numeric_rules = list((filters or {}).get("numeric_rules") or [])
    if numeric and not numeric_rules:
        numeric_rules = [numeric]

    if not categorical and not numeric_rules:
        return rows

    filtered: List[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        passed = True

        for col, wanted in categorical.items():
            value = str(row.get(col) or "").strip().lower()
            if value != str(wanted).strip().lower():
                passed = False
                break
        if not passed:
            continue

        numeric_ok = True
        for rule in numeric_rules:
            if not isinstance(rule, dict):
                continue
            col = str(rule.get("column") or "")
            if not col:
                continue
            numeric_value = _nlp_to_float(row.get(col))
            if numeric_value is None:
                numeric_ok = False
                break
            op = str(rule.get("op") or "")
            if op == "between":
                low = _nlp_to_float(rule.get("low"))
                high = _nlp_to_float(rule.get("high"))
                if low is None or high is None or not (low <= numeric_value <= high):
                    numeric_ok = False
                    break
            elif op == "gte":
                threshold = _nlp_to_float(rule.get("value"))
                if threshold is None or numeric_value < threshold:
                    numeric_ok = False
                    break
            elif op == "lte":
                threshold = _nlp_to_float(rule.get("value"))
                if threshold is None or numeric_value > threshold:
                    numeric_ok = False
                    break
        if not numeric_ok:
            continue

        filtered.append(row)

    return filtered


def _nlp_one_line_recommendation(
    question: str,
    metric_fields: List[str],
    dimension_fields: List[str],
    group_summary: List[dict],
    metric_summary: Dict[str, dict],
) -> str:
    lower_q = str(question or "").lower()
    wants_bottom = any(token in lower_q for token in ["bottom", "lowest", "worst", "least"])
    wants_group = _nlp_wants_grouping(question, dimension_fields)
    metric = metric_fields[0] if metric_fields else "metric"
    dimension = dimension_fields[0] if dimension_fields else "segment"

    if group_summary and wants_group:
        ranked = sorted(group_summary, key=lambda r: float(r.get("sum") or 0.0), reverse=not wants_bottom)
        focus = ranked[0]
        dim_value = str(focus.get("dimension") or "N/A")
        sum_value = _nlp_format_number(focus.get("sum") or 0.0)
        if wants_bottom:
            return (
                f"Recommendation: prioritize improving {dim_value} in {dimension} to raise {metric}, "
                f"currently the weakest segment (total {sum_value})."
            )
        return (
            f"Recommendation: replicate the playbook from {dim_value} in {dimension} to improve {metric}, "
            f"currently the strongest segment (total {sum_value})."
        )

    if metric_summary:
        metric_name = next(iter(metric_summary.keys()))
        stats = metric_summary.get(metric_name) or {}
        avg = float(stats.get("avg") or 0.0)
        total = float(stats.get("sum") or 0.0)
        std = float(stats.get("std") or 0.0)
        variation = (abs(std / avg) if abs(avg) > 1e-9 else 0.0)
        if variation >= 0.6:
            return (
                f"Recommendation: stabilize {metric_name}; volatility is high "
                f"(avg {_nlp_format_number(avg)}, std {_nlp_format_number(std)})."
            )
        return (
            f"Recommendation: continue optimizing {metric_name} with periodic checks "
            f"(avg {_nlp_format_number(avg)}, total {_nlp_format_number(total)})."
        )

    return "Recommendation: define at least one metric and one dimension for more actionable insights."


def _nlp_build_table_response(
    question: str,
    ranking: List[dict],
    analysis_rows: List[dict],
    relevant_columns: List[str],
    metric_fields: List[str],
    dimension_fields: List[str],
    group_summary: List[dict],
    max_rows: int = 12,
    max_cols: int = 8,
) -> tuple[List[str], List[dict]]:
    wants_group = bool(group_summary) and _nlp_wants_grouping(question, dimension_fields)
    row_limit = _nlp_parse_requested_limit(question, default_limit=max_rows, max_limit=50)

    if wants_group:
        grouped_cols, grouped_rows = _nlp_build_group_rows(
            rows=analysis_rows,
            question=question,
            dimension_fields=dimension_fields,
            metric_fields=metric_fields,
            row_limit=row_limit,
        )
        if grouped_cols and grouped_rows:
            return grouped_cols, grouped_rows

    columns = list(relevant_columns or [])[:max_cols]
    rows: List[dict] = []
    for idx, item in enumerate((ranking or [])[:50], start=1):
        row = item.get("row") if isinstance(item, dict) else None
        if not isinstance(row, dict):
            continue
        out = {"_rank": idx, "_score": round(float(item.get("score") or 0.0), 6)}
        for col in columns:
            out[col] = row.get(col)
        rows.append(out)

    sort_key, descending = _nlp_parse_sort_intent(
        question=question,
        candidate_columns=["_rank", "_score"] + columns,
        default_key="_score",
    )
    if sort_key in {"_score", "_rank"} or sort_key in columns:
        numeric_sort = all(_nlp_to_float(r.get(sort_key)) is not None for r in rows[: min(len(rows), 20)] if r.get(sort_key) is not None)
        if numeric_sort:
            rows = sorted(
                rows,
                key=lambda r: _nlp_to_float(r.get(sort_key)) if _nlp_to_float(r.get(sort_key)) is not None else float("-inf"),
                reverse=descending,
            )
        else:
            rows = sorted(rows, key=lambda r: str(r.get(sort_key) or "").lower(), reverse=descending)

    final_rows = rows[:row_limit]
    for idx, row in enumerate(final_rows, start=1):
        row["_rank"] = idx
    return columns, final_rows


def _nlp_metric_summary(df: Any, metric_fields: List[str]) -> Dict[str, dict]:
    import pandas as pd

    summary: Dict[str, dict] = {}
    for col in metric_fields:
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            continue
        summary[col] = {
            "count": int(series.shape[0]),
            "sum": round(float(series.sum()), 6),
            "avg": round(float(series.mean()), 6),
            "min": round(float(series.min()), 6),
            "max": round(float(series.max()), 6),
            "std": round(float(series.std(ddof=0)), 6),
        }
    return summary


def _nlp_group_summary(df: Any, metric_fields: List[str], dimension_fields: List[str]) -> List[dict]:
    import pandas as pd

    if not metric_fields or not dimension_fields:
        return []
    metric = metric_fields[0]
    dimension = dimension_fields[0]
    if metric not in df.columns or dimension not in df.columns:
        return []

    tmp = df[[dimension, metric]].copy()
    tmp[metric] = pd.to_numeric(tmp[metric], errors="coerce")
    tmp = tmp.dropna(subset=[metric])
    if tmp.empty:
        return []
    tmp[dimension] = tmp[dimension].astype(str).fillna("")
    grouped = tmp.groupby(dimension)[metric].agg(["sum", "mean", "count"]).sort_values("sum", ascending=False).head(10)

    rows = []
    for idx, row in grouped.iterrows():
        rows.append({
            "dimension": str(idx),
            "sum": round(float(row["sum"]), 6),
            "avg": round(float(row["mean"]), 6),
            "count": int(row["count"]),
        })
    return rows


def _nlp_chat_fallback_answer(
    question: str,
    corrected_question: str,
    corrections: Dict[str, str],
    metric_summary: Dict[str, dict],
    group_summary: List[dict],
    matched_rows: List[dict],
    domain_context: str,
    metric_fields: List[str],
    dimension_fields: List[str],
    semantic_engine: str,
) -> str:
    lower_q = str(corrected_question or question).lower()
    wants_top = any(token in lower_q for token in ["top", "highest", "best", "leader"])
    wants_bottom = any(token in lower_q for token in ["bottom", "lowest", "worst", "least"])
    wants_avg = any(token in lower_q for token in ["avg", "average", "mean"])
    wants_total = any(token in lower_q for token in ["sum", "total", "overall"])
    wants_anomaly = any(token in lower_q for token in ["anomaly", "outlier", "spike", "drop"])
    wants_trend = any(token in lower_q for token in ["trend", "over time", "forecast", "growth"])

    lines: List[str] = []
    lines.append(f"Query interpreted: {corrected_question or question}")
    lines.append(f"Reasoning engine: python_semantic ({semantic_engine})")

    if corrections:
        correction_text = ", ".join([f"{src}→{dst}" for src, dst in list(corrections.items())[:8]])
        lines.append(f"Auto-corrections applied: {correction_text}")

    if domain_context:
        lines.append(f"Domain context considered: {domain_context[:240]}")

    if metric_fields:
        lines.append(f"Metric fields in scope: {', '.join(metric_fields[:6])}")
    if dimension_fields:
        lines.append(f"Dimension fields in scope: {', '.join(dimension_fields[:6])}")

    if metric_summary:
        for metric_name, m in list(metric_summary.items())[:3]:
            lines.append(
                f"Metric '{metric_name}': total={m.get('sum')}, avg={m.get('avg')}, min={m.get('min')}, max={m.get('max')}, std={m.get('std')}."
            )

    if group_summary:
        if wants_bottom:
            focus = sorted(group_summary, key=lambda r: float(r.get("sum") or 0.0))[:3]
            label = "Lowest segments"
        else:
            focus = sorted(group_summary, key=lambda r: float(r.get("sum") or 0.0), reverse=True)[:3]
            label = "Top segments"

        if wants_top or wants_bottom or wants_total or wants_avg:
            summary_parts = [
                f"{row.get('dimension')} (sum={row.get('sum')}, avg={row.get('avg')}, count={row.get('count')})"
                for row in focus
            ]
            lines.append(f"{label}: " + "; ".join(summary_parts))
        else:
            top = focus[0]
            lines.append(
                f"Leading segment: {top.get('dimension')} (sum={top.get('sum')}, avg={top.get('avg')}, count={top.get('count')})."
            )

    if matched_rows:
        snippet_rows = matched_rows[:2]
        for idx, sample in enumerate(snippet_rows, start=1):
            keys = list(sample.keys())[:6]
            snippet = ", ".join([f"{k}={sample.get(k)}" for k in keys])
            lines.append(f"Relevant sample row {idx}: {snippet}")

    if wants_trend and not any("date" in dim.lower() or "time" in dim.lower() for dim in dimension_fields):
        lines.append("Trend note: add a date/time dimension field for stronger trend analysis.")
    if wants_anomaly and metric_summary:
        lines.append("Anomaly tip: inspect rows where metric deviates beyond ~2 standard deviations from average.")

    if not (wants_top or wants_bottom or wants_avg or wants_total or wants_anomaly or wants_trend):
        lines.append("Ask follow-up with intent keywords (top/bottom/trend/anomaly/average/total) for a more targeted answer.")

    return "\n".join(lines)


def _analytics_openai_model() -> str:
    return str(
        _os.getenv("OPENAI_ANALYTICS_MODEL")
        or _os.getenv("OPENAI_MODEL")
        or "gpt-5.3-codex"
    ).strip()


def _analytics_openai_base_url() -> str:
    base = str(
        _os.getenv("OPENAI_ANALYTICS_BASE_URL")
        or _os.getenv("OPENAI_API_BASE")
        or _os.getenv("OPENAI_BASE_URL")
        or "https://api.openai.com/v1"
    ).strip().rstrip("/")
    return base or "https://api.openai.com/v1"


def _analytics_is_placeholder_api_key(api_key: str) -> bool:
    key = str(api_key or "").strip().strip('"').strip("'")
    if not key:
        return True
    lowered = key.lower()
    if key.startswith("sk-etlflow-"):
        return True
    placeholder_tokens = (
        "your_real_key",
        "your_key_here",
        "replace_me",
        "replace_this",
        "changeme",
        "placeholder",
        "dummy",
        "sample_key",
    )
    return any(token in lowered for token in placeholder_tokens)


def _analytics_openai_headers(api_key: str) -> Dict[str, str]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    # Optional compatibility for providers expecting non-standard key header names.
    custom_key_header = str(_os.getenv("OPENAI_API_KEY_HEADER") or "").strip()
    if custom_key_header:
        headers[custom_key_header] = api_key
    return headers


def _analytics_extract_chat_completion_text(resp: dict) -> str:
    choices = resp.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first = choices[0] if isinstance(choices[0], dict) else {}
    message = first.get("message") if isinstance(first.get("message"), dict) else {}
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        chunks: List[str] = []
        for part in content:
            if not isinstance(part, dict):
                continue
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                chunks.append(text.strip())
        return "\n".join(chunks).strip()
    return ""


def _analytics_extract_response_text(resp: dict) -> str:
    # Compatible with current and legacy response shapes.
    direct = resp.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()

    texts: List[str] = []
    for item in resp.get("output", []) or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []) or []:
            if not isinstance(content, dict):
                continue
            text_value = content.get("text") or content.get("output_text")
            if isinstance(text_value, str) and text_value.strip():
                texts.append(text_value.strip())
    return "\n".join(texts).strip()


def _analytics_parse_json_object(text: str) -> Optional[dict]:
    if not text:
        return None
    text = text.strip()
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        parsed = json.loads(text[start:end + 1])
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _analytics_finalize_llm_plan(parsed: dict, columns: List[str], model: str, api_kind: str) -> dict:
    valid_chart_types = {"line", "bar", "scatter", "heatmap", "pie", "table", "area"}
    chart_type = str(parsed.get("chart_type") or "").strip().lower()
    x_field = str(parsed.get("x_field") or "").strip()
    y_field = str(parsed.get("y_field") or "").strip()
    group_by = str(parsed.get("group_by") or "").strip()
    insight = str(parsed.get("insight") or "").strip()

    return {
        "used": True,
        "provider": "openai",
        "model": model,
        "api_kind": api_kind,
        "chart_type": chart_type if chart_type in valid_chart_types else "",
        "x_field": x_field if x_field in columns else "",
        "y_field": y_field if y_field in columns else "",
        "group_by": group_by if group_by in columns else "",
        "insight": insight,
    }


async def _analytics_plan_with_openai(
    prompt: str,
    columns: List[str],
    numeric_cols: List[str],
    datetime_cols: List[str],
    categorical_cols: List[str],
    row_count: int,
) -> dict:
    api_key = str(_os.getenv("OPENAI_API_KEY") or "").strip()
    if not prompt:
        return {"used": False, "reason": "missing_prompt"}
    if _analytics_is_placeholder_api_key(api_key):
        return {"used": False, "reason": "invalid_or_placeholder_openai_api_key"}

    model = _analytics_openai_model()
    base_url = _analytics_openai_base_url()
    headers = _analytics_openai_headers(api_key)
    system_instruction = (
        "You are an analytics assistant for dashboard auto-generation. "
        "Return ONLY JSON with keys: chart_type, x_field, y_field, group_by, insight. "
        "Rules: choose fields only from provided columns. chart_type one of: "
        "line, bar, scatter, heatmap, pie, table, area. "
        "Prefer line for time trend, bar for ranking/comparison, scatter for relationship."
    )
    user_context = {
        "prompt": prompt,
        "row_count": int(row_count),
        "columns": columns[:80],
        "numeric_columns": numeric_cols[:40],
        "datetime_columns": datetime_cols[:20],
        "categorical_columns": categorical_cols[:40],
    }
    request_payload = {
        "model": model,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": system_instruction}]},
            {"role": "user", "content": [{"type": "input_text", "text": json.dumps(user_context)}]},
        ],
        "temperature": 0.1,
        "max_output_tokens": 260,
    }
    chat_payload_base = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": json.dumps(user_context)},
        ],
        "temperature": 0.1,
        "max_tokens": 260,
    }

    try:
        import httpx
        last_reason = "openai_unavailable"
        with httpx.Client(timeout=25.0) as client:
            # Attempt 1: Responses API.
            resp = client.post(
                f"{base_url}/responses",
                headers=headers,
                json=request_payload,
            )
            if resp.status_code < 400:
                raw = resp.json() if resp.content else {}
                output_text = _analytics_extract_response_text(raw)
                parsed = _analytics_parse_json_object(output_text)
                if parsed:
                    return _analytics_finalize_llm_plan(parsed, columns, model, "responses")
                last_reason = "openai_responses_non_json"
            else:
                last_reason = f"openai_responses_http_{resp.status_code}"

            # Attempt 2: Chat Completions API (compatibility with proxy providers).
            for use_json_mode in (True, False):
                chat_payload = dict(chat_payload_base)
                if use_json_mode:
                    chat_payload["response_format"] = {"type": "json_object"}
                chat_resp = client.post(
                    f"{base_url}/chat/completions",
                    headers=headers,
                    json=chat_payload,
                )
                if chat_resp.status_code >= 400:
                    last_reason = f"openai_chat_http_{chat_resp.status_code}"
                    if use_json_mode and chat_resp.status_code in (400, 415, 422):
                        # Retry once without strict JSON mode for compatibility.
                        continue
                    break

                raw_chat = chat_resp.json() if chat_resp.content else {}
                chat_text = _analytics_extract_chat_completion_text(raw_chat)
                parsed_chat = _analytics_parse_json_object(chat_text)
                if parsed_chat:
                    return _analytics_finalize_llm_plan(parsed_chat, columns, model, "chat_completions")
                last_reason = "openai_chat_non_json"
                if use_json_mode:
                    continue
                break

        return {"used": False, "reason": last_reason}
    except Exception as exc:
        return {"used": False, "reason": f"openai_error:{exc}"}


def _analytics_pick_column_from_prompt(prompt: str, candidates: List[str]) -> Optional[str]:
    import re
    import difflib

    if not prompt or not candidates:
        return None

    text = prompt.lower()
    tokens = set(re.findall(r"[a-zA-Z_]+", text))
    best_col = None
    best_score = -1.0

    for col in candidates:
        col_text = str(col).lower()
        col_tokens = set(re.findall(r"[a-zA-Z_]+", col_text))
        score = 0.0

        if col_text in text:
            score += 10.0

        overlap = len(tokens.intersection(col_tokens))
        score += overlap * 3.0

        if tokens:
            similarity = max(
                [difflib.SequenceMatcher(None, token, col_text).ratio() for token in tokens],
                default=0.0
            )
            score += similarity * 2.0

        if score > best_score:
            best_score = score
            best_col = col

    return best_col if best_score > 0 else None


def _analytics_chart_type_from_prompt(prompt: str, has_date: bool) -> str:
    text = (prompt or "").strip().lower()
    if not text:
        return "line" if has_date else "bar"
    if any(k in text for k in ["forecast", "trend", "time series", "over time"]):
        return "line"
    if any(k in text for k in ["correlation", "relationship", "impact"]):
        return "scatter"
    if any(k in text for k in ["heat", "matrix", "intensity"]):
        return "heatmap"
    if any(k in text for k in ["share", "composition", "percentage"]):
        return "pie"
    if any(k in text for k in ["table", "tabular", "detail rows"]):
        return "table"
    if any(k in text for k in ["distribution", "compare", "ranking", "top", "bottom"]):
        return "bar"
    return "line" if has_date else "bar"


def _analytics_source_config_from_request(payload: dict) -> dict:
    return {
        "type": str(payload.get("source_type", "sample")).strip().lower(),
        "dataset": payload.get("dataset"),
        "pipeline_id": payload.get("pipeline_id"),
        "pipeline_node_id": payload.get("pipeline_node_id"),
        "file_path": payload.get("file_path"),
        "mlops_workflow_id": payload.get("mlops_workflow_id"),
        "mlops_run_id": payload.get("mlops_run_id"),
        "mlops_node_id": payload.get("mlops_node_id"),
        "mlops_output_mode": payload.get("mlops_output_mode"),
        "mlops_prediction_start_date": payload.get("mlops_prediction_start_date"),
        "mlops_prediction_end_date": payload.get("mlops_prediction_end_date"),
        "sql": payload.get("sql"),
    }


def _analytics_layout_for_widgets(widget_ids: List[str]) -> list:
    layout = []
    cursor_y = 0

    placements = [
        (0, 0, 8, 4),
        (8, 0, 16, 4),
        (0, 4, 12, 8),
        (12, 4, 12, 8),
        (0, 12, 12, 8),
        (12, 12, 12, 8),
        (0, 20, 24, 10),
    ]

    for idx, wid in enumerate(widget_ids):
        if idx < len(placements):
            x, y, w, h = placements[idx]
            cursor_y = max(cursor_y, y + h)
        else:
            x, y, w, h = 0, cursor_y, 24, 8
            cursor_y += h

        layout.append({
            "i": wid,
            "x": x,
            "y": y,
            "w": w,
            "h": h,
            "minW": 4,
            "minH": 3,
        })
    return layout


async def _analytics_generate_dashboard(rows: list, payload: dict) -> dict:
    import numpy as np
    import pandas as pd

    records = [r for r in rows if isinstance(r, dict)]
    if not records:
        raise HTTPException(400, "Selected source has no tabular records for analytics")

    df = pd.DataFrame(records)
    if df.empty:
        raise HTTPException(400, "Selected source has no rows for analytics")

    df.columns = [str(c) for c in df.columns]
    columns = list(df.columns)

    numeric_cols: List[str] = []
    datetime_cols: List[str] = []

    for col in columns:
        series = df[col]
        numeric_series = pd.to_numeric(series, errors="coerce")
        numeric_ratio = float(numeric_series.notna().mean()) if len(series) else 0.0
        if numeric_ratio >= 0.65:
            df[col] = numeric_series
            numeric_cols.append(col)
            continue

        datetime_series = pd.to_datetime(series, errors="coerce", utc=True)
        datetime_ratio = float(datetime_series.notna().mean()) if len(series) else 0.0
        if datetime_ratio >= 0.65:
            try:
                df[col] = datetime_series.dt.tz_convert(None)
            except Exception:
                try:
                    df[col] = datetime_series.dt.tz_localize(None)
                except Exception:
                    df[col] = datetime_series
            datetime_cols.append(col)

    categorical_cols = [c for c in columns if c not in numeric_cols and c not in datetime_cols]
    prompt = str(payload.get("nlp_prompt") or "").strip()

    preferred_measure = _analytics_pick_column_from_prompt(prompt, numeric_cols) if numeric_cols else None
    preferred_date = _analytics_pick_column_from_prompt(prompt, datetime_cols) if datetime_cols else None
    preferred_category = _analytics_pick_column_from_prompt(prompt, categorical_cols) if categorical_cols else None

    measure_col = preferred_measure or (numeric_cols[0] if numeric_cols else (columns[0] if columns else "value"))
    second_measure = numeric_cols[1] if len(numeric_cols) > 1 else measure_col
    date_col = preferred_date or (datetime_cols[0] if datetime_cols else None)
    category_col = preferred_category or (categorical_cols[0] if categorical_cols else (columns[0] if columns else measure_col))

    descriptive_rows = []
    for col in numeric_cols[:12]:
        s = pd.to_numeric(df[col], errors="coerce")
        valid = s.dropna()
        if valid.empty:
            continue
        descriptive_rows.append({
            "field": col,
            "mean": round(float(valid.mean()), 6),
            "median": round(float(valid.median()), 6),
            "std_dev": round(float(valid.std(ddof=0)), 6),
            "min": round(float(valid.min()), 6),
            "max": round(float(valid.max()), 6),
            "missing_pct": round(float((1.0 - (valid.count() / max(len(s), 1))) * 100.0), 2),
        })

    correlation_rows = []
    if len(numeric_cols) >= 2:
        corr_df = df[numeric_cols].corr(numeric_only=True)
        for i in range(len(numeric_cols)):
            for j in range(i + 1, len(numeric_cols)):
                value = corr_df.iloc[i, j]
                if pd.isna(value):
                    continue
                correlation_rows.append({
                    "field_x": numeric_cols[i],
                    "field_y": numeric_cols[j],
                    "correlation": round(float(value), 6),
                    "abs_correlation": round(abs(float(value)), 6),
                })
        correlation_rows.sort(key=lambda r: abs(r["correlation"]), reverse=True)
        correlation_rows = correlation_rows[:10]

    outlier_rows = []
    for col in numeric_cols[:10]:
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if len(s) < 8:
            continue
        q1 = float(s.quantile(0.25))
        q3 = float(s.quantile(0.75))
        iqr = q3 - q1
        if iqr <= 0:
            continue
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        outliers = int(((s < lower) | (s > upper)).sum())
        outlier_rows.append({
            "field": col,
            "outlier_count": outliers,
            "outlier_pct": round((outliers / max(len(s), 1)) * 100.0, 2),
            "lower_bound": round(lower, 6),
            "upper_bound": round(upper, 6),
        })

    forecast_horizon = max(3, min(int(payload.get("forecast_horizon") or 30), 3650))
    start_date, end_date = _mlops_resolve_date_window(
        str(payload.get("mlops_prediction_start_date") or ""),
        str(payload.get("mlops_prediction_end_date") or ""),
    )
    if start_date and end_date:
        forecast_dates = _mlops_date_sequence(start_date, end_date, 3660)
    elif start_date and not end_date:
        forecast_dates = [start_date + timedelta(days=i) for i in range(forecast_horizon)]
    else:
        forecast_dates = []

    forecast_rows = []
    forecast_slope = 0.0
    if measure_col in df.columns:
        measure_series = pd.to_numeric(df[measure_col], errors="coerce")
        if date_col and date_col in df.columns:
            ts_df = df[[date_col, measure_col]].copy()
            ts_df[measure_col] = pd.to_numeric(ts_df[measure_col], errors="coerce")
            ts_df = ts_df.dropna(subset=[date_col, measure_col]).sort_values(date_col)
            if not ts_df.empty:
                grouped = ts_df.groupby(date_col, as_index=False)[measure_col].mean()
                y = grouped[measure_col].astype(float).values
                x = np.arange(len(y), dtype=float)
                if len(y) >= 2:
                    coef = np.polyfit(x, y, 1)
                    slope = float(coef[0])
                    intercept = float(coef[1])
                else:
                    slope = 0.0
                    intercept = float(y[0])
                residual = y - (slope * x + intercept)
                sigma = float(np.std(residual)) if len(residual) > 1 else float(np.std(y))
                sigma = sigma if np.isfinite(sigma) else 0.0
                forecast_slope = slope
                if not forecast_dates:
                    last_date = pd.Timestamp(grouped[date_col].iloc[-1]).date()
                    forecast_dates = [last_date + timedelta(days=i + 1) for i in range(forecast_horizon)]
                for idx, dt_value in enumerate(forecast_dates):
                    step = len(y) + idx
                    pred = float(slope * step + intercept)
                    forecast_rows.append({
                        "forecast_date": dt_value.isoformat(),
                        "prediction": round(pred, 6),
                        "lower_bound": round(pred - 1.96 * sigma, 6),
                        "upper_bound": round(pred + 1.96 * sigma, 6),
                        "forecast_step": idx + 1,
                    })
        else:
            y = measure_series.dropna().astype(float).values
            if len(y) >= 2:
                x = np.arange(len(y), dtype=float)
                coef = np.polyfit(x, y, 1)
                slope = float(coef[0])
                intercept = float(coef[1])
                residual = y - (slope * x + intercept)
                sigma = float(np.std(residual)) if len(residual) > 1 else float(np.std(y))
                sigma = sigma if np.isfinite(sigma) else 0.0
                forecast_slope = slope
                for idx in range(forecast_horizon):
                    step = len(y) + idx
                    pred = float(slope * step + intercept)
                    forecast_rows.append({
                        "forecast_step": idx + 1,
                        "prediction": round(pred, 6),
                        "lower_bound": round(pred - 1.96 * sigma, 6),
                        "upper_bound": round(pred + 1.96 * sigma, 6),
                    })

    missing_rows = []
    for col in columns[:30]:
        missing_count = int(df[col].isna().sum())
        if missing_count > 0:
            missing_rows.append({
                "field": col,
                "missing_count": missing_count,
                "missing_pct": round((missing_count / max(len(df), 1)) * 100.0, 2),
            })
    missing_rows.sort(key=lambda r: r["missing_count"], reverse=True)

    recommendations: List[str] = []
    if missing_rows and missing_rows[0]["missing_pct"] >= 15:
        recommendations.append(
            f"Data quality first: column '{missing_rows[0]['field']}' has {missing_rows[0]['missing_pct']}% missing values. "
            "Prioritize imputation or source data fixes before model retraining."
        )
    if correlation_rows and abs(correlation_rows[0]["correlation"]) >= 0.7:
        top_corr = correlation_rows[0]
        recommendations.append(
            f"Driver focus: '{top_corr['field_x']}' and '{top_corr['field_y']}' are strongly correlated "
            f"({top_corr['correlation']}). Monitor drift and potential multicollinearity."
        )
    if outlier_rows:
        most_outlier = max(outlier_rows, key=lambda r: r["outlier_pct"])
        if most_outlier["outlier_pct"] >= 8:
            recommendations.append(
                f"Outlier control: '{most_outlier['field']}' shows {most_outlier['outlier_pct']}% outliers. "
                "Add winsorization/robust scaling in the upstream ETL+MLOps pipeline."
            )
    if forecast_rows:
        if forecast_slope > 0:
            recommendations.append(
                f"Capacity planning: '{measure_col}' has positive projected trend. Prepare inventory and staffing for sustained growth."
            )
        elif forecast_slope < 0:
            recommendations.append(
                f"Risk mitigation: '{measure_col}' has declining projected trend. Trigger root-cause diagnostics and retention campaigns."
            )
        else:
            recommendations.append(
                f"Stability play: '{measure_col}' projection is flat. Optimize margin/cost efficiency while maintaining baseline performance."
            )
    if not recommendations:
        recommendations.append(
            "Set a weekly analytics cadence: monitor top KPI, drift indicators, and retrain trigger thresholds."
        )

    chart_type = _analytics_chart_type_from_prompt(prompt, bool(date_col))
    if chart_type == "scatter" and len(numeric_cols) < 2:
        chart_type = "bar"
    if chart_type == "heatmap" and not category_col:
        chart_type = "bar"

    nlp_x_field = _analytics_pick_column_from_prompt(prompt, datetime_cols + categorical_cols) or (date_col or category_col or (columns[0] if columns else ""))
    nlp_y_field = _analytics_pick_column_from_prompt(prompt, numeric_cols) or measure_col
    nlp_group_field = _analytics_pick_column_from_prompt(prompt, [c for c in categorical_cols if c != nlp_x_field]) or (
        categorical_cols[1] if len(categorical_cols) > 1 else category_col
    )
    nlp_engine = {"provider": "heuristic", "model": "local-nlp-rules", "used": True}
    nlp_llm_result = {}

    llm_plan = await _analytics_plan_with_openai(
        prompt=prompt,
        columns=columns,
        numeric_cols=numeric_cols,
        datetime_cols=datetime_cols,
        categorical_cols=categorical_cols,
        row_count=len(df),
    )
    if llm_plan.get("used"):
        llm_chart = str(llm_plan.get("chart_type") or "").strip().lower()
        llm_x = str(llm_plan.get("x_field") or "").strip()
        llm_y = str(llm_plan.get("y_field") or "").strip()
        llm_group = str(llm_plan.get("group_by") or "").strip()
        llm_insight = str(llm_plan.get("insight") or "").strip()

        if llm_chart:
            chart_type = llm_chart
        if llm_x:
            nlp_x_field = llm_x
        if llm_y:
            nlp_y_field = llm_y
        if llm_group:
            nlp_group_field = llm_group

        if llm_insight:
            recommendations.insert(0, f"NLP insight ({_analytics_openai_model()}): {llm_insight}")

        nlp_engine = {
            "provider": str(llm_plan.get("provider") or "openai"),
            "model": str(llm_plan.get("model") or _analytics_openai_model()),
            "api_kind": str(llm_plan.get("api_kind") or "responses"),
            "used": True,
            "base_url": _analytics_openai_base_url(),
        }
        nlp_llm_result = {
            "chart_type": llm_chart,
            "x_field": llm_x,
            "y_field": llm_y,
            "group_by": llm_group,
            "insight": llm_insight,
        }
    else:
        nlp_engine = {
            "provider": "heuristic",
            "model": "local-nlp-rules",
            "used": True,
            "fallback_reason": llm_plan.get("reason") or "not_used",
            "requested_model": _analytics_openai_model(),
            "requested_base_url": _analytics_openai_base_url(),
        }
        if str(llm_plan.get("reason") or "") == "invalid_or_placeholder_openai_api_key":
            recommendations.insert(
                0,
                "NLP LLM unavailable: set a valid OPENAI_API_KEY in backend/.env to enable GPT-based prompt understanding."
            )

    source_cfg = _analytics_source_config_from_request(payload)
    widget_style = {"show_legend": True, "show_grid": True, "show_labels": False}

    headline_lines = [
        "## Data Science Analytics Summary",
        f"- Rows analyzed: **{len(df):,}**",
        f"- Columns analyzed: **{len(columns)}**",
        f"- Numeric fields: {', '.join(numeric_cols[:8]) if numeric_cols else 'None'}",
        f"- Date fields: {', '.join(datetime_cols[:5]) if datetime_cols else 'None'}",
        f"- Categorical fields: {', '.join(categorical_cols[:8]) if categorical_cols else 'None'}",
    ]

    diagnostic_lines = []
    if correlation_rows:
        top = correlation_rows[0]
        diagnostic_lines.append(
            f"Top correlation: **{top['field_x']} ↔ {top['field_y']} = {top['correlation']}**"
        )
    if outlier_rows:
        top_out = max(outlier_rows, key=lambda r: r["outlier_pct"])
        diagnostic_lines.append(
            f"Highest outlier ratio: **{top_out['field']} ({top_out['outlier_pct']}%)**"
        )
    if missing_rows:
        top_missing = missing_rows[0]
        diagnostic_lines.append(
            f"Most missing data: **{top_missing['field']} ({top_missing['missing_pct']}%)**"
        )

    forecast_lines = []
    if forecast_rows:
        first_f = forecast_rows[0]
        last_f = forecast_rows[-1]
        start_label = first_f.get("forecast_date", "step 1")
        end_label = last_f.get("forecast_date", f"step {len(forecast_rows)}")
        forecast_lines.append(
            f"Forecast horizon: **{start_label} → {end_label}**"
        )
        forecast_lines.append(
            f"Projected {measure_col}: **{round(float(first_f['prediction']), 2)} → {round(float(last_f['prediction']), 2)}**"
        )
    else:
        forecast_lines.append("Forecast could not be computed from the selected source.")

    prescriptive_text = "## Prescriptive Recommendations\n" + "\n".join([f"- {line}" for line in recommendations])

    widgets = []

    overview_widget_id = str(uuid.uuid4())
    widgets.append({
        "id": overview_widget_id,
        "type": "text",
        "title": "Analytics Overview",
        "subtitle": "Descriptive + Diagnostic summary",
        "data_source": dict(source_cfg),
        "chart_config": {
            "text_content": "\n".join(headline_lines + [""] + [f"- {line}" for line in diagnostic_lines + forecast_lines])
        },
        "style": dict(widget_style),
    })

    kpi_widget_id = str(uuid.uuid4())
    widgets.append({
        "id": kpi_widget_id,
        "type": "kpi",
        "title": f"Descriptive KPI: {measure_col}",
        "subtitle": "Primary measure snapshot",
        "data_source": dict(source_cfg),
        "chart_config": {
            "kpi_field": measure_col,
            "y_field": measure_col,
            "kpi_label": f"{measure_col} (latest)",
        },
        "style": dict(widget_style),
    })

    descriptive_chart_id = str(uuid.uuid4())
    widgets.append({
        "id": descriptive_chart_id,
        "type": "bar",
        "title": "Descriptive Distribution",
        "subtitle": f"{measure_col} by {category_col}",
        "data_source": dict(source_cfg),
        "chart_config": {
            "x_field": category_col,
            "y_field": measure_col,
            "aggregation": "sum",
            "sort_by": "value",
            "sort_order": "desc",
            "limit": 20,
        },
        "style": dict(widget_style),
    })

    diagnostic_widget_id = str(uuid.uuid4())
    if len(numeric_cols) >= 2:
        diagnostic_type = "scatter"
        diagnostic_config = {
            "x_field": second_measure,
            "y_field": measure_col,
            "limit": 500,
        }
        diagnostic_subtitle = f"{second_measure} vs {measure_col}"
    else:
        diagnostic_type = "heatmap"
        diagnostic_config = {
            "x_field": date_col or category_col,
            "group_by": category_col,
            "y_field": measure_col,
            "limit": 300,
        }
        diagnostic_subtitle = "Intensity / anomaly surface"

    widgets.append({
        "id": diagnostic_widget_id,
        "type": diagnostic_type,
        "title": "Diagnostic View",
        "subtitle": diagnostic_subtitle,
        "data_source": dict(source_cfg),
        "chart_config": diagnostic_config,
        "style": dict(widget_style),
    })

    forecasting_widget_id = str(uuid.uuid4())
    widgets.append({
        "id": forecasting_widget_id,
        "type": "line",
        "title": "Forecasting Trend",
        "subtitle": f"Trend baseline for {measure_col}",
        "data_source": dict(source_cfg),
        "chart_config": {
            "x_field": date_col or category_col,
            "y_field": measure_col,
            "aggregation": "avg",
            "limit": 400,
        },
        "style": dict(widget_style),
    })

    nlp_widget_id = str(uuid.uuid4())
    widgets.append({
        "id": nlp_widget_id,
        "type": chart_type,
        "title": "NLP Context Visualization",
        "subtitle": prompt or "Prompt-driven analytical lens",
        "data_source": dict(source_cfg),
        "chart_config": {
            "x_field": nlp_x_field,
            "y_field": nlp_y_field,
            "group_by": nlp_group_field if chart_type in ("heatmap", "radar") else None,
            "aggregation": "sum",
            "limit": 300,
        },
        "style": dict(widget_style),
    })

    prescriptive_widget_id = str(uuid.uuid4())
    widgets.append({
        "id": prescriptive_widget_id,
        "type": "text",
        "title": "Prescriptive Actions",
        "subtitle": "What to do next",
        "data_source": dict(source_cfg),
        "chart_config": {
            "text_content": prescriptive_text
        },
        "style": dict(widget_style),
    })

    table_widget_id = str(uuid.uuid4())
    widgets.append({
        "id": table_widget_id,
        "type": "table",
        "title": "Diagnostic Data Explorer",
        "subtitle": "Detailed row-level inspection",
        "data_source": dict(source_cfg),
        "chart_config": {
            "limit": 200,
        },
        "style": dict(widget_style),
    })

    widget_ids = [w["id"] for w in widgets]
    layout = _analytics_layout_for_widgets(widget_ids)

    return {
        "summary": f"Analytics dashboard generated with {len(widgets)} widgets.",
        "columns": {
            "numeric": numeric_cols,
            "datetime": datetime_cols,
            "categorical": categorical_cols,
        },
        "insights": {
            "descriptive": descriptive_rows,
            "diagnostic_correlations": correlation_rows,
            "diagnostic_outliers": outlier_rows,
            "forecasting": forecast_rows[:200],
            "prescriptive": recommendations,
            "nlp": {
                "prompt": prompt,
                "chart_type": chart_type,
                "x_field": nlp_x_field,
                "y_field": nlp_y_field,
                "group_by": nlp_group_field,
                "engine": nlp_engine,
                "llm_result": nlp_llm_result,
            },
        },
        "nlp_engine": nlp_engine,
        "widgets": widgets,
        "layout": layout,
    }


def _python_analytics_profile_columns(df: Any) -> Dict[str, List[str]]:
    import pandas as pd

    numeric_cols: List[str] = []
    datetime_cols: List[str] = []

    for col in df.columns:
        series = df[col]
        numeric_series = pd.to_numeric(series, errors="coerce")
        numeric_ratio = float(numeric_series.notna().mean()) if len(series) else 0.0
        if numeric_ratio >= 0.65:
            df[col] = numeric_series
            numeric_cols.append(str(col))
            continue

        datetime_series = pd.to_datetime(series, errors="coerce", utc=True)
        datetime_ratio = float(datetime_series.notna().mean()) if len(series) else 0.0
        if datetime_ratio >= 0.65:
            try:
                df[col] = datetime_series.dt.tz_convert(None)
            except Exception:
                try:
                    df[col] = datetime_series.dt.tz_localize(None)
                except Exception:
                    df[col] = datetime_series
            datetime_cols.append(str(col))

    categorical_cols = [str(c) for c in df.columns if str(c) not in numeric_cols and str(c) not in datetime_cols]
    return {
        "numeric": numeric_cols,
        "datetime": datetime_cols,
        "categorical": categorical_cols,
    }


def _python_analytics_resolve_chart_type(
    requested: str,
    numeric_cols: List[str],
    datetime_cols: List[str],
    categorical_cols: List[str],
) -> str:
    chart = str(requested or "auto").strip().lower()
    if chart and chart != "auto":
        return chart
    if datetime_cols and numeric_cols:
        return "line"
    if categorical_cols and numeric_cols:
        return "bar"
    if len(numeric_cols) >= 2:
        return "scatter"
    if len(numeric_cols) == 1:
        return "histogram"
    return "table"


def _python_analytics_build_plot(rows: list, payload: dict) -> Dict[str, Any]:
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import plotly.io as pio

    records = [r for r in rows if isinstance(r, dict)]
    if not records:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            template="plotly_white",
            margin=dict(l=18, r=14, t=40, b=24),
            autosize=True,
            paper_bgcolor="#ffffff",
            plot_bgcolor="#ffffff",
            font=dict(size=12),
        )
        empty_fig.add_annotation(
            text="No data available for the current drill selection",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=13, color="#64748b"),
        )

        empty_json = json.loads(empty_fig.to_json())
        inner_html = pio.to_html(
            empty_fig,
            include_plotlyjs="cdn",
            full_html=False,
            config={"responsive": True, "displayModeBar": True, "displaylogo": False},
            default_width="100%",
            default_height="100%",
        )
        wrapped_html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    html, body, #python-viz-root {{
      width: 100%;
      height: 100%;
      margin: 0;
      padding: 0;
      overflow: hidden;
      background: #ffffff;
    }}
    #python-viz-root .plotly-graph-div {{
      width: 100% !important;
      height: 100% !important;
    }}
  </style>
</head>
<body>
  <div id="python-viz-root">{inner_html}</div>
</body>
</html>"""

        return {
            "figure_json": empty_json,
            "figure_html": wrapped_html,
            "chart_type": "table",
            "x_field": None,
            "y_field": None,
            "color_field": None,
            "group_by": None,
            "title": str(payload.get("title") or "Python Analytics Visualization").strip() or "Python Analytics Visualization",
            "row_count": 0,
            "sampled_row_count": 0,
            "columns": [],
            "column_profile": {"numeric": [], "datetime": [], "categorical": []},
            "preview_rows": [],
        }

    limit = max(100, min(int(payload.get("limit") or 5000), 100000))
    sampled_rows = records[:limit]
    df = pd.DataFrame(sampled_rows)
    if df.empty:
        raise HTTPException(400, "Selected source has no usable rows")

    df.columns = [str(c) for c in df.columns]
    profile = _python_analytics_profile_columns(df)
    numeric_cols = profile["numeric"]
    datetime_cols = profile["datetime"]
    categorical_cols = profile["categorical"]
    all_columns = list(df.columns)

    chart_type = _python_analytics_resolve_chart_type(
        str(payload.get("chart_type") or "auto"),
        numeric_cols,
        datetime_cols,
        categorical_cols,
    )

    requested_x = str(payload.get("x_field") or "").strip()
    requested_y = str(payload.get("y_field") or "").strip()
    requested_color = str(payload.get("color_field") or "").strip()
    requested_group = str(payload.get("group_by") or "").strip()

    x_field = requested_x if requested_x in all_columns else ""
    y_field = requested_y if requested_y in all_columns else ""
    color_field = requested_color if requested_color in all_columns else ""
    group_by = requested_group if requested_group in all_columns else ""

    if not x_field:
        if chart_type in {"line"} and datetime_cols:
            x_field = datetime_cols[0]
        elif categorical_cols:
            x_field = categorical_cols[0]
        elif datetime_cols:
            x_field = datetime_cols[0]
        elif all_columns:
            x_field = all_columns[0]

    if not y_field and numeric_cols:
        y_field = numeric_cols[0]
    if not color_field and chart_type in {"line", "bar", "scatter"}:
        fallback_color = [c for c in categorical_cols if c != x_field]
        color_field = fallback_color[0] if fallback_color else ""
    if not group_by and chart_type == "heatmap":
        fallback_group = [c for c in categorical_cols if c != x_field]
        group_by = fallback_group[0] if fallback_group else ""

    title = str(payload.get("title") or "").strip() or "Python Analytics Visualization"
    fig = None

    if chart_type == "line" and x_field and y_field:
        line_df = df[[x_field, y_field] + ([color_field] if color_field else [])].copy()
        if x_field in datetime_cols:
            line_df = line_df.sort_values(x_field)
        if color_field:
            fig = px.line(line_df, x=x_field, y=y_field, color=color_field, title=title)
        else:
            fig = px.line(line_df, x=x_field, y=y_field, title=title)
    elif chart_type == "bar" and x_field:
        if y_field and y_field in numeric_cols:
            bar_df = df[[x_field, y_field] + ([color_field] if color_field else [])].copy()
            if color_field:
                grouped = bar_df.groupby([x_field, color_field], as_index=False)[y_field].sum()
                grouped = grouped.sort_values(y_field, ascending=False).head(100)
                fig = px.bar(grouped, x=x_field, y=y_field, color=color_field, title=title)
            else:
                grouped = bar_df.groupby(x_field, as_index=False)[y_field].sum()
                grouped = grouped.sort_values(y_field, ascending=False).head(100)
                fig = px.bar(grouped, x=x_field, y=y_field, title=title)
        else:
            grouped = df.groupby(x_field, as_index=False).size().rename(columns={"size": "count"})
            grouped = grouped.sort_values("count", ascending=False).head(100)
            fig = px.bar(grouped, x=x_field, y="count", title=title)
    elif chart_type == "scatter" and len(numeric_cols) >= 2:
        x_num = x_field if x_field in numeric_cols else numeric_cols[0]
        y_num = y_field if y_field in numeric_cols and y_field != x_num else (
            numeric_cols[1] if len(numeric_cols) > 1 else numeric_cols[0]
        )
        scatter_cols = [x_num, y_num] + ([color_field] if color_field else [])
        scatter_df = df[scatter_cols].copy()
        if color_field:
            fig = px.scatter(scatter_df, x=x_num, y=y_num, color=color_field, title=title)
        else:
            fig = px.scatter(scatter_df, x=x_num, y=y_num, title=title)
        x_field = x_num
        y_field = y_num
    elif chart_type == "histogram" and numeric_cols:
        hist_col = y_field if y_field in numeric_cols else numeric_cols[0]
        hist_df = df[[hist_col]].copy()
        fig = px.histogram(hist_df, x=hist_col, nbins=40, title=title)
        x_field = hist_col
        y_field = ""
    elif chart_type == "box" and numeric_cols:
        box_y = y_field if y_field in numeric_cols else numeric_cols[0]
        box_x = x_field if x_field in categorical_cols else (categorical_cols[0] if categorical_cols else None)
        box_cols = [box_y] + ([box_x] if box_x else [])
        box_df = df[box_cols].copy()
        if box_x:
            fig = px.box(box_df, x=box_x, y=box_y, title=title)
            x_field = box_x
        else:
            fig = px.box(box_df, y=box_y, title=title)
            x_field = ""
        y_field = box_y
    elif chart_type == "heatmap" and x_field and group_by and y_field and y_field in numeric_cols:
        heat_df = df[[x_field, group_by, y_field]].copy()
        pivot = heat_df.pivot_table(index=group_by, columns=x_field, values=y_field, aggfunc="mean").fillna(0)
        fig = px.imshow(
            pivot,
            labels={"x": x_field, "y": group_by, "color": y_field},
            title=title,
            aspect="auto",
        )
    else:
        preview_columns = all_columns[: min(len(all_columns), 12)]
        preview_df = df[preview_columns].head(200)
        fig = go.Figure(
            data=[
                go.Table(
                    header=dict(values=preview_columns, fill_color="#1f2937", font=dict(color="#e5e7eb", size=12)),
                    cells=dict(
                        values=[preview_df[col].astype(str).tolist() for col in preview_columns],
                        fill_color="#0b1220",
                        font=dict(color="#cbd5e1", size=11),
                        align="left",
                    ),
                )
            ]
        )
        chart_type = "table"

    fig.update_layout(
        template="plotly_white",
        margin=dict(l=32, r=16, t=52, b=36),
        autosize=True,
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        font=dict(size=12),
    )

    figure_json = json.loads(fig.to_json())
    inner_html = pio.to_html(
        fig,
        include_plotlyjs="cdn",
        full_html=False,
        config={"responsive": True, "displayModeBar": True, "displaylogo": False},
        default_width="100%",
        default_height="100%",
    )
    figure_html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    html, body, #python-viz-root {{
      width: 100%;
      height: 100%;
      margin: 0;
      padding: 0;
      overflow: hidden;
      background: #ffffff;
    }}
    #python-viz-root .plotly-graph-div {{
      width: 100% !important;
      height: 100% !important;
    }}
  </style>
</head>
<body>
  <div id="python-viz-root">{inner_html}</div>
  <script>
    (function() {{
      function findPlotDiv() {{
        return document.querySelector('#python-viz-root .plotly-graph-div');
      }}

      function resizePlot() {{
        var div = findPlotDiv();
        if (div && window.Plotly && window.Plotly.Plots) {{
          window.Plotly.Plots.resize(div);
        }}
      }}

      function postClick(eventName, point) {{
        if (!point || !window.parent) return;
        window.parent.postMessage({{
          type: eventName,
          payload: {{
            x: point.x,
            y: point.y,
            z: point.z,
            label: point.label,
            value: point.value,
            text: point.text,
            curveNumber: point.curveNumber,
            pointNumber: point.pointNumber,
            seriesName: point.fullData && point.fullData.name ? point.fullData.name : undefined
          }}
        }}, '*');
      }}

      function bindEvents() {{
        var div = findPlotDiv();
        if (!div || typeof div.on !== 'function') {{
          setTimeout(bindEvents, 120);
          return;
        }}

        div.on('plotly_click', function(evt) {{
          var point = evt && evt.points && evt.points.length ? evt.points[0] : null;
          postClick('python-plot-click', point);
        }});

        div.on('plotly_selected', function(evt) {{
          var point = evt && evt.points && evt.points.length ? evt.points[0] : null;
          postClick('python-plot-selected', point);
        }});
      }}

      window.addEventListener('resize', resizePlot);
      setTimeout(resizePlot, 60);
      setTimeout(resizePlot, 260);
      bindEvents();
    }})();
  </script>
</body>
</html>"""
    preview_df = df.head(300).copy()
    preview_df = preview_df.where(pd.notna(preview_df), None)

    return {
        "figure_json": figure_json,
        "figure_html": figure_html,
        "chart_type": chart_type,
        "x_field": x_field or None,
        "y_field": y_field or None,
        "color_field": color_field or None,
        "group_by": group_by or None,
        "title": title,
        "row_count": len(records),
        "sampled_row_count": len(sampled_rows),
        "columns": all_columns,
        "column_profile": profile,
        "preview_rows": _mlops_sanitize_sample_rows(
            preview_df.to_dict(orient="records"),
            limit=300,
        ),
    }


@app.post("/api/analytics/auto-dashboard")
async def generate_analytics_dashboard(body: AnalyticsDashboardRequest, db: Session = Depends(get_db)):
    payload = body.model_dump(exclude_none=True)
    rows = _load_source_rows_for_query(payload, db, apply_sql=True)
    if not rows:
        raise HTTPException(400, "Selected source returned no rows")
    return await _analytics_generate_dashboard(rows, payload)


@app.post("/api/analytics/python-visualization")
async def generate_python_visualization(body: PythonAnalyticsRequest, db: Session = Depends(get_db)):
    payload = body.model_dump(exclude_none=True)
    rows = _load_source_rows_for_query(payload, db, apply_sql=True)
    rows = _apply_drill_filters_to_rows(rows, payload.get("drill_filters"))
    tabular_rows = [r for r in rows if isinstance(r, dict)]

    try:
        import plotly  # noqa: F401
    except Exception:
        raise HTTPException(
            500,
            "Plotly is not installed in backend environment. Install with: pip install plotly",
        )

    result = _python_analytics_build_plot(tabular_rows, payload)
    return {
        "status": "success",
        "source_type": payload.get("source_type", "sample"),
        **result,
    }


@app.post("/api/query")
async def run_query(body: dict, db: Session = Depends(get_db)):
    """Execute a query against selected source and return tabular rows."""
    data = _load_source_rows_for_query(body, db, apply_sql=True)
    data = _apply_drill_filters_to_rows(data, body.get("drill_filters"))

    if not data:
        return {"data": [], "columns": [], "row_count": 0}

    output_rows = data[:5000]
    ordered_columns: List[str] = []
    seen_columns: set[str] = set()

    def _add_column(name: Any) -> None:
        col = str(name or "").strip()
        if not col or col in seen_columns:
            return
        seen_columns.add(col)
        ordered_columns.append(col)

    for row in output_rows:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            _add_column(key)

        # H2O evaluation metrics rows may expose source fields as a CSV in metadata.
        raw_output_fields = row.get("_mlops_output_fields")
        if isinstance(raw_output_fields, str):
            for token in raw_output_fields.split(","):
                _add_column(token)
        elif isinstance(raw_output_fields, list):
            for token in raw_output_fields:
                _add_column(token)

    return {
        "data": output_rows,
        "columns": ordered_columns,
        "row_count": len(data)
    }


_WORKFLOW_CHAT_CONTEXT_KEYWORDS: Dict[str, List[str]] = {
    "etl": [
        "etl", "extract", "transform", "load", "pipeline", "api source", "csv", "json", "source", "destination",
    ],
    "mlops": [
        "mlops", "model", "training", "evaluate", "evaluation", "deploy", "deployment", "prediction", "feature",
    ],
    "business": [
        "business", "decision", "approval", "mail", "email", "whatsapp", "notification", "action", "workflow",
    ],
    "visualization": [
        "visualization", "dashboard", "chart", "graph", "kpi", "widget", "analytics", "tableau", "studio",
    ],
    "application": [
        "application", "app", "platform", "end to end", "end-to-end", "complete", "full product", "architecture",
    ],
}


def _workflow_chat_extract_url(question: str) -> Optional[str]:
    match = re.search(r"https?://[^\s)]+", str(question or "").strip(), flags=re.IGNORECASE)
    if not match:
        return None
    return str(match.group(0)).strip().rstrip(".,;")


def _workflow_chat_detect_context(question: str, preferred_context: str = "auto") -> str:
    valid_contexts = {"etl", "mlops", "business", "visualization", "application"}
    preferred = str(preferred_context or "auto").strip().lower()
    if preferred in valid_contexts:
        return preferred

    lowered = str(question or "").lower()
    scores: Dict[str, int] = {ctx: 0 for ctx in valid_contexts}
    for ctx, keywords in _WORKFLOW_CHAT_CONTEXT_KEYWORDS.items():
        for keyword in keywords:
            if keyword in lowered:
                scores[ctx] += 1

    # If user asks broadly ("complete", "end-to-end"), bias to application orchestration.
    if any(token in lowered for token in ("complete", "end-to-end", "full app", "full product", "entire app")):
        scores["application"] += 2

    top_score = max(scores.values()) if scores else 0
    if top_score <= 0:
        return "application"

    tie_priority = ["application", "visualization", "business", "mlops", "etl"]
    winners = [ctx for ctx, score in scores.items() if score == top_score]
    for candidate in tie_priority:
        if candidate in winners:
            return candidate
    return winners[0]


_WORKFLOW_CHAT_VALID_CONTEXTS = {"etl", "mlops", "business", "visualization", "application"}
_WORKFLOW_CHAT_ALLOWED_CHART_TYPES = {"kpi", "bar", "line", "table", "area", "pie", "scatter", "heatmap"}


def _workflow_chat_openai_model() -> str:
    return str(
        _os.getenv("OPENAI_WORKFLOW_CHAT_MODEL")
        or _os.getenv("OPENAI_ANALYTICS_MODEL")
        or _os.getenv("OPENAI_MODEL")
        or "gpt-5.3-codex"
    ).strip()


def _workflow_chat_openai_base_url() -> str:
    base = str(
        _os.getenv("OPENAI_WORKFLOW_CHAT_BASE_URL")
        or _os.getenv("OPENAI_ANALYTICS_BASE_URL")
        or _os.getenv("OPENAI_API_BASE")
        or _os.getenv("OPENAI_BASE_URL")
        or "https://api.openai.com/v1"
    ).strip().rstrip("/")
    return base or "https://api.openai.com/v1"


def _workflow_chat_short_text(value: Any, max_len: int = 260) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_len:
        return text
    return text[:max_len].rstrip() + "..."


def _workflow_chat_normalize_blueprint(
    parsed: Any,
    fallback: Dict[str, Any],
    question: str,
) -> Dict[str, Any]:
    if not isinstance(parsed, dict):
        return dict(fallback)

    out = dict(fallback)

    context_value = str(parsed.get("context_type") or "").strip().lower()
    if context_value in _WORKFLOW_CHAT_VALID_CONTEXTS:
        out["context_type"] = context_value

    answer = _workflow_chat_short_text(parsed.get("answer"), max_len=1200)
    if answer:
        out["answer"] = answer

    recommendation = _workflow_chat_short_text(parsed.get("one_line_recommendation"), max_len=240)
    if recommendation:
        out["one_line_recommendation"] = recommendation

    steps_raw = parsed.get("steps")
    if isinstance(steps_raw, list):
        normalized_steps: List[Dict[str, str]] = []
        for item in steps_raw[:12]:
            if not isinstance(item, dict):
                continue
            title = _workflow_chat_short_text(item.get("title"), max_len=120)
            detail = _workflow_chat_short_text(item.get("detail"), max_len=320)
            module = _workflow_chat_short_text(item.get("module"), max_len=60) or "Workflow"
            if title and detail:
                normalized_steps.append({
                    "module": module,
                    "title": title,
                    "detail": detail,
                })
        if normalized_steps:
            out["steps"] = normalized_steps

    nodes_raw = parsed.get("flow_nodes")
    if isinstance(nodes_raw, list):
        normalized_nodes: List[Dict[str, str]] = []
        used_ids = set()
        for idx, item in enumerate(nodes_raw[:16]):
            if not isinstance(item, dict):
                continue
            node_id = _workflow_chat_slug(
                item.get("id") or item.get("label") or f"n{idx + 1}",
                fallback=f"n{idx + 1}",
            )
            if node_id in used_ids:
                node_id = f"{node_id}-{idx + 1}"
            used_ids.add(node_id)
            label = _workflow_chat_short_text(item.get("label"), max_len=90)
            module = _workflow_chat_short_text(item.get("module"), max_len=60) or "Workflow"
            node_type = _workflow_chat_slug(item.get("node_type"), fallback="workflow_node")
            if label:
                normalized_nodes.append({
                    "id": node_id,
                    "label": label,
                    "module": module,
                    "node_type": node_type,
                })
        if normalized_nodes:
            out["flow_nodes"] = normalized_nodes

    edges_raw = parsed.get("flow_edges")
    if isinstance(edges_raw, list):
        node_ids = {
            str(node.get("id"))
            for node in (out.get("flow_nodes") or [])
            if isinstance(node, dict) and node.get("id")
        }
        normalized_edges: List[Dict[str, str]] = []
        for item in edges_raw[:32]:
            if not isinstance(item, dict):
                continue
            source = str(item.get("source") or "").strip()
            target = str(item.get("target") or "").strip()
            if not source or not target:
                continue
            if node_ids and (source not in node_ids or target not in node_ids):
                continue
            label = _workflow_chat_short_text(item.get("label"), max_len=80)
            edge: Dict[str, str] = {"source": source, "target": target}
            if label:
                edge["label"] = label
            normalized_edges.append(edge)
        if normalized_edges:
            out["flow_edges"] = normalized_edges

    charts_raw = parsed.get("suggested_charts")
    if isinstance(charts_raw, list):
        normalized_charts: List[Dict[str, str]] = []
        for item in charts_raw[:12]:
            if not isinstance(item, dict):
                continue
            chart_type = str(item.get("type") or "").strip().lower()
            if chart_type not in _WORKFLOW_CHAT_ALLOWED_CHART_TYPES:
                continue
            title = _workflow_chat_short_text(item.get("title"), max_len=120)
            reason = _workflow_chat_short_text(item.get("reason"), max_len=220)
            x_field = _workflow_chat_short_text(item.get("x_field"), max_len=80)
            y_field = _workflow_chat_short_text(item.get("y_field"), max_len=80)
            if title and reason:
                normalized_charts.append({
                    "type": chart_type,
                    "title": title,
                    "x_field": x_field or "x",
                    "y_field": y_field or "y",
                    "reason": reason,
                })
        if normalized_charts:
            out["suggested_charts"] = normalized_charts

    actions_raw = parsed.get("suggested_actions")
    if isinstance(actions_raw, list):
        normalized_actions: List[Dict[str, str]] = []
        for item in actions_raw[:10]:
            if not isinstance(item, dict):
                continue
            label = _workflow_chat_short_text(item.get("label"), max_len=80)
            route = str(item.get("route") or "").strip()
            description = _workflow_chat_short_text(item.get("description"), max_len=180)
            if not route.startswith("/"):
                continue
            if label:
                normalized_actions.append({
                    "label": label,
                    "route": route,
                    "description": description or "Open module",
                })
        if normalized_actions:
            out["suggested_actions"] = normalized_actions

    detected_url = str(parsed.get("detected_url") or "").strip()
    if detected_url and re.match(r"^https?://", detected_url, flags=re.IGNORECASE):
        out["detected_url"] = detected_url
    else:
        out["detected_url"] = _workflow_chat_extract_url(question) or out.get("detected_url")

    return out


def _workflow_chat_llm_user_payload(
    question: str,
    context_type: str,
    chat_history: List[Dict[str, str]],
    fallback_blueprint: Dict[str, Any],
) -> str:
    return json.dumps(
        {
            "question": question,
            "context_type": context_type,
            "chat_history": chat_history[-12:],
            "fallback_blueprint": {
                "context_type": fallback_blueprint.get("context_type"),
                "steps": fallback_blueprint.get("steps") or [],
                "flow_nodes": fallback_blueprint.get("flow_nodes") or [],
                "flow_edges": fallback_blueprint.get("flow_edges") or [],
                "suggested_charts": fallback_blueprint.get("suggested_charts") or [],
                "suggested_actions": fallback_blueprint.get("suggested_actions") or [],
                "detected_url": fallback_blueprint.get("detected_url"),
            },
            "constraints": {
                "valid_contexts": sorted(list(_WORKFLOW_CHAT_VALID_CONTEXTS)),
                "chart_types": sorted(list(_WORKFLOW_CHAT_ALLOWED_CHART_TYPES)),
                "max_steps": 12,
                "max_nodes": 16,
                "max_edges": 32,
                "max_charts": 12,
                "max_actions": 10,
            },
        },
        ensure_ascii=False,
    )


async def _workflow_chat_generate_with_codex(
    question: str,
    context_type: str,
    chat_history: List[WorkflowChatMessage],
    fallback_blueprint: Dict[str, Any],
) -> Dict[str, Any]:
    api_key = str(_os.getenv("OPENAI_API_KEY") or "").strip()
    if _analytics_is_placeholder_api_key(api_key):
        return {"used": False, "reason": "invalid_or_placeholder_openai_api_key"}

    model = _workflow_chat_openai_model()
    base_url = _workflow_chat_openai_base_url()
    headers = _analytics_openai_headers(api_key)
    system_instruction = (
        "You are a workflow architect for ETL, MLOps, Business Logic and Visualization. "
        "Return ONLY JSON with keys: context_type, answer, one_line_recommendation, steps, flow_nodes, flow_edges, "
        "suggested_charts, suggested_actions, detected_url. "
        "Each step item must contain module,title,detail. "
        "Each flow_nodes item must contain id,label,module,node_type. "
        "Each flow_edges item must contain source,target and optional label. "
        "Each suggested_charts item must contain type,title,x_field,y_field,reason. "
        "Each suggested_actions item must contain label,route,description."
    )
    normalized_history = [
        {
            "role": str(item.role or "").strip().lower(),
            "content": _workflow_chat_short_text(item.content, max_len=1200),
        }
        for item in (chat_history or [])
        if str(item.role or "").strip() and str(item.content or "").strip()
    ]
    user_payload = _workflow_chat_llm_user_payload(
        question=question,
        context_type=context_type,
        chat_history=normalized_history,
        fallback_blueprint=fallback_blueprint,
    )

    response_payload = {
        "model": model,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": system_instruction}]},
            {"role": "user", "content": [{"type": "input_text", "text": user_payload}]},
        ],
        "temperature": 0.2,
        "max_output_tokens": 1800,
    }
    chat_payload_base = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": user_payload},
        ],
        "temperature": 0.2,
        "max_tokens": 1800,
    }

    try:
        import httpx

        async with httpx.AsyncClient(timeout=45.0) as client:
            raw_text = ""
            api_kind = "responses"

            resp = await client.post(
                f"{base_url}/responses",
                headers=headers,
                json=response_payload,
            )
            if resp.status_code < 400:
                raw = resp.json() if resp.content else {}
                raw_text = _analytics_extract_response_text(raw)
            else:
                api_kind = "chat_completions"

            if not raw_text:
                for use_json_mode in (True, False):
                    payload = dict(chat_payload_base)
                    if use_json_mode:
                        payload["response_format"] = {"type": "json_object"}
                    chat_resp = await client.post(
                        f"{base_url}/chat/completions",
                        headers=headers,
                        json=payload,
                    )
                    if chat_resp.status_code >= 400:
                        if use_json_mode and chat_resp.status_code in (400, 415, 422):
                            continue
                        return {
                            "used": False,
                            "reason": f"openai_chat_http_{chat_resp.status_code}",
                            "model": model,
                        }
                    chat_raw = chat_resp.json() if chat_resp.content else {}
                    raw_text = _analytics_extract_chat_completion_text(chat_raw)
                    api_kind = "chat_completions"
                    if raw_text:
                        break

            parsed = _analytics_parse_json_object(raw_text)
            if not parsed:
                return {"used": False, "reason": "openai_non_json", "model": model}

            blueprint = _workflow_chat_normalize_blueprint(
                parsed=parsed,
                fallback=fallback_blueprint,
                question=question,
            )
            return {
                "used": True,
                "provider": "openai",
                "model": model,
                "api_kind": api_kind,
                "blueprint": blueprint,
            }
    except Exception as exc:
        return {"used": False, "reason": f"openai_error:{exc}", "model": model}


def _workflow_chat_blueprint(context_type: str, question: str) -> Dict[str, Any]:
    context_key = str(context_type or "application").strip().lower()
    url = _workflow_chat_extract_url(question)
    url_hint = f" using datasource `{url}`" if url else ""

    if context_key == "etl":
        steps = [
            {"module": "ETL", "title": "Create Pipeline", "detail": "Add a new pipeline with trigger, source, transform and destination nodes."},
            {"module": "ETL", "title": "Connect API Source", "detail": f"Configure REST API source{url_hint} and set JSON path to row array."},
            {"module": "ETL", "title": "Normalize Data", "detail": "Select fields, rename columns, apply filters, deduplicate and sort records."},
            {"module": "ETL", "title": "Persist Output", "detail": "Write curated rows to CSV/DB destination and execute pipeline once for validation."},
            {"module": "Visualization", "title": "Enable Consumption", "detail": "Use pipeline output as datasource for dashboard widgets."},
        ]
        nodes = [
            {"id": "n1", "label": "Manual/Schedule Trigger", "module": "ETL", "node_type": "trigger"},
            {"id": "n2", "label": "REST API Source", "module": "ETL", "node_type": "rest_api_source"},
            {"id": "n3", "label": "Select/Rename Fields", "module": "ETL", "node_type": "map_rename_transform"},
            {"id": "n4", "label": "Filter + Deduplicate", "module": "ETL", "node_type": "quality_transform"},
            {"id": "n5", "label": "CSV/DB Destination", "module": "ETL", "node_type": "destination"},
        ]
        edges = [
            {"source": "n1", "target": "n2", "label": "start"},
            {"source": "n2", "target": "n3", "label": "raw rows"},
            {"source": "n3", "target": "n4", "label": "mapped rows"},
            {"source": "n4", "target": "n5", "label": "curated rows"},
        ]
        charts = [
            {"type": "table", "title": "Curated Output Preview", "x_field": "row_index", "y_field": "key_metrics", "reason": "Validate ETL output quickly."},
            {"type": "bar", "title": "Rows by Category", "x_field": "category", "y_field": "count", "reason": "Check distribution after transformation."},
            {"type": "line", "title": "Trend Over Time", "x_field": "date", "y_field": "measure", "reason": "Spot time-series anomalies in transformed data."},
        ]
    elif context_key == "mlops":
        steps = [
            {"module": "MLOps", "title": "Load Training Source", "detail": "Select ETL output or file source for model development."},
            {"module": "MLOps", "title": "Feature Engineering", "detail": "Configure column-level transformations and feature selection."},
            {"module": "MLOps", "title": "Train and Evaluate", "detail": "Train model, review metrics and compare candidate models."},
            {"module": "MLOps", "title": "Deploy Endpoint", "detail": "Deploy best model and validate inference endpoint with test payloads."},
            {"module": "Visualization", "title": "Monitor Performance", "detail": "Track prediction trends and model metrics in dashboards."},
        ]
        nodes = [
            {"id": "n1", "label": "Pipeline Output Source", "module": "MLOps", "node_type": "ml_pipeline_output_source"},
            {"id": "n2", "label": "Feature Engineering", "module": "MLOps", "node_type": "ml_feature_engineering"},
            {"id": "n3", "label": "Model Training", "module": "MLOps", "node_type": "ml_model_training"},
            {"id": "n4", "label": "Model Evaluation", "module": "MLOps", "node_type": "ml_model_evaluation"},
            {"id": "n5", "label": "Deployment", "module": "MLOps", "node_type": "ml_model_deployment"},
        ]
        edges = [
            {"source": "n1", "target": "n2", "label": "training rows"},
            {"source": "n2", "target": "n3", "label": "features"},
            {"source": "n3", "target": "n4", "label": "candidate model"},
            {"source": "n4", "target": "n5", "label": "approved model"},
        ]
        charts = [
            {"type": "line", "title": "Training/Eval Metric Trend", "x_field": "epoch_or_run", "y_field": "metric_value", "reason": "Track learning and stability."},
            {"type": "bar", "title": "Feature Importance", "x_field": "feature", "y_field": "importance", "reason": "Explain model signal drivers."},
            {"type": "table", "title": "Prediction Samples", "x_field": "input", "y_field": "prediction", "reason": "Validate endpoint output quality."},
        ]
    elif context_key == "business":
        steps = [
            {"module": "Business Logic", "title": "Connect Source", "detail": "Use ETL/MLOps output as input for decision automation."},
            {"module": "Business Logic", "title": "Prompt Decision", "detail": "Generate action recommendation, summary or response text."},
            {"module": "Business Logic", "title": "Apply Decision Gate", "detail": "Branch by condition and route to communication channels."},
            {"module": "Business Logic", "title": "Send Actions", "detail": "Trigger WhatsApp Sender and/or Mail Writer from approved branch."},
            {"module": "Visualization", "title": "Track Outcomes", "detail": "Monitor execution status, message counts and conversion KPIs."},
        ]
        nodes = [
            {"id": "n1", "label": "ETL/MLOps Source", "module": "Business", "node_type": "business_source"},
            {"id": "n2", "label": "Prompt Decision Model", "module": "Business", "node_type": "business_llm_prompt"},
            {"id": "n3", "label": "Decision Gate", "module": "Business", "node_type": "business_decision"},
            {"id": "n4", "label": "WhatsApp Sender", "module": "Business", "node_type": "business_whatsapp_sender"},
            {"id": "n5", "label": "Mail Writer", "module": "Business", "node_type": "business_mail_writer"},
        ]
        edges = [
            {"source": "n1", "target": "n2", "label": "input rows"},
            {"source": "n2", "target": "n3", "label": "recommended action"},
            {"source": "n3", "target": "n4", "label": "approved path"},
            {"source": "n3", "target": "n5", "label": "notification path"},
        ]
        charts = [
            {"type": "bar", "title": "Actions by Status", "x_field": "status", "y_field": "count", "reason": "Track sent/draft/failure rates."},
            {"type": "line", "title": "Workflow Throughput", "x_field": "time", "y_field": "executions", "reason": "Monitor automation volume over time."},
            {"type": "table", "title": "Decision Audit Trail", "x_field": "decision", "y_field": "destination", "reason": "Review end-to-end action traceability."},
        ]
    elif context_key == "visualization":
        steps = [
            {"module": "Visualization", "title": "Select Datasource", "detail": "Bind dashboard to ETL/MLOps/file source with clean columns."},
            {"module": "Visualization", "title": "Create Core Widgets", "detail": "Add KPI, trend, category and table widgets for primary insights."},
            {"module": "Visualization", "title": "Enable Interactions", "detail": "Configure drill-down, drill-through and filters across widgets."},
            {"module": "Visualization", "title": "Tune Layout", "detail": "Adjust widget size/position and save dashboard state."},
            {"module": "Sharing", "title": "Publish", "detail": "Use share/embed URL for website or iframe integration."},
        ]
        nodes = [
            {"id": "n1", "label": "Datasource Binding", "module": "Visualization", "node_type": "viz_source"},
            {"id": "n2", "label": "KPI Widgets", "module": "Visualization", "node_type": "viz_kpi"},
            {"id": "n3", "label": "Bar/Line Widgets", "module": "Visualization", "node_type": "viz_trend"},
            {"id": "n4", "label": "Table Widget", "module": "Visualization", "node_type": "viz_table"},
            {"id": "n5", "label": "Share/Embed", "module": "Visualization", "node_type": "viz_publish"},
        ]
        edges = [
            {"source": "n1", "target": "n2", "label": "metrics"},
            {"source": "n1", "target": "n3", "label": "dimensions + metrics"},
            {"source": "n1", "target": "n4", "label": "detail rows"},
            {"source": "n2", "target": "n5", "label": "dashboard output"},
            {"source": "n3", "target": "n5", "label": "dashboard output"},
            {"source": "n4", "target": "n5", "label": "dashboard output"},
        ]
        charts = [
            {"type": "kpi", "title": "Executive KPI Strip", "x_field": "metric", "y_field": "value", "reason": "Fast at-a-glance business status."},
            {"type": "bar", "title": "Category Performance", "x_field": "category", "y_field": "measure", "reason": "Compare segments clearly."},
            {"type": "line", "title": "Trend Timeline", "x_field": "date", "y_field": "measure", "reason": "Show movement and seasonality."},
            {"type": "table", "title": "Detailed Records", "x_field": "row", "y_field": "columns", "reason": "Enable drill-through validation."},
        ]
    else:
        steps = [
            {"module": "ETL", "title": "Build Data Pipeline", "detail": f"Ingest and transform source data{url_hint}, then persist curated output."},
            {"module": "MLOps", "title": "Train + Deploy Model", "detail": "Use curated dataset for feature engineering, training and deployment."},
            {"module": "Business Logic", "title": "Automate Decisions", "detail": "Trigger prompt-driven decisions and communication actions."},
            {"module": "Visualization", "title": "Create Interactive Dashboard", "detail": "Build KPI/trend/table dashboards with drill interactions."},
            {"module": "Operations", "title": "Schedule + Monitor", "detail": "Enable schedules, execution monitoring and share/embed endpoints."},
        ]
        nodes = [
            {"id": "n1", "label": "ETL Pipeline", "module": "Application", "node_type": "etl_pipeline"},
            {"id": "n2", "label": "MLOps Workflow", "module": "Application", "node_type": "mlops_workflow"},
            {"id": "n3", "label": "Business Workflow", "module": "Application", "node_type": "business_workflow"},
            {"id": "n4", "label": "Visualization Dashboard", "module": "Application", "node_type": "dashboard"},
            {"id": "n5", "label": "Share/Embed + Monitoring", "module": "Application", "node_type": "ops"},
        ]
        edges = [
            {"source": "n1", "target": "n2", "label": "curated data"},
            {"source": "n1", "target": "n4", "label": "analytics source"},
            {"source": "n2", "target": "n4", "label": "predictions"},
            {"source": "n2", "target": "n3", "label": "model outputs"},
            {"source": "n3", "target": "n5", "label": "action logs"},
            {"source": "n4", "target": "n5", "label": "published dashboards"},
        ]
        charts = [
            {"type": "line", "title": "End-to-End Throughput", "x_field": "time", "y_field": "records_processed", "reason": "Shows pipeline + app health."},
            {"type": "bar", "title": "Module Success Rate", "x_field": "module", "y_field": "success_rate", "reason": "Quick quality monitoring by module."},
            {"type": "table", "title": "Operational Events", "x_field": "timestamp", "y_field": "event", "reason": "Track deployment and workflow actions."},
        ]

    actions = [
        {"label": "Open Pipelines", "route": "/pipelines", "description": "Configure ETL sources and transforms."},
        {"label": "Open MLOps Studio", "route": "/mlops", "description": "Train/evaluate/deploy models."},
        {"label": "Open Business Logic", "route": "/business", "description": "Configure prompt-driven action workflows."},
        {"label": "Open Visualizations", "route": "/dashboards", "description": "Create and publish dashboards."},
    ]

    recommendation = (
        f"Recommended path: start with {steps[0]['module']} and implement the flow chart in order for a stable build."
    )
    answer = (
        f"Created a {context_key} implementation plan with chart-ready workflow steps. "
        f"Use the flow chart and suggested charts below to configure modules without code changes."
    )
    if url:
        answer += f" Detected datasource URL: {url}."

    return {
        "context_type": context_key,
        "answer": answer,
        "one_line_recommendation": recommendation,
        "steps": steps,
        "flow_nodes": nodes,
        "flow_edges": edges,
        "suggested_charts": charts,
        "suggested_actions": actions,
        "detected_url": url,
    }


@app.post("/api/workflow-chat/ask")
async def workflow_chat_ask(body: WorkflowChatAskRequest):
    question = str(body.question or "").strip()
    if not question:
        raise HTTPException(400, "question is required")

    context_type = _workflow_chat_detect_context(question, body.context_type)
    fallback_blueprint = _workflow_chat_blueprint(context_type, question)
    llm_result = await _workflow_chat_generate_with_codex(
        question=question,
        context_type=context_type,
        chat_history=body.chat_history or [],
        fallback_blueprint=fallback_blueprint,
    )
    blueprint = llm_result.get("blueprint") if llm_result.get("used") else fallback_blueprint
    if not isinstance(blueprint, dict):
        blueprint = fallback_blueprint

    if llm_result.get("used"):
        engine = {
            "used": True,
            "provider": str(llm_result.get("provider") or "openai"),
            "model": str(llm_result.get("model") or _workflow_chat_openai_model()),
            "api_kind": str(llm_result.get("api_kind") or "responses"),
        }
    else:
        engine = {
            "used": True,
            "provider": "python_fallback",
            "model": "workflow-blueprint-engine",
            "requested_model": _workflow_chat_openai_model(),
            "fallback_reason": str(llm_result.get("reason") or "openai_unavailable"),
        }

    return {
        "question": question,
        "context_type": blueprint.get("context_type") or context_type,
        "answer": blueprint.get("answer") or "",
        "one_line_recommendation": blueprint.get("one_line_recommendation") or "",
        "steps": blueprint.get("steps") or [],
        "flow_nodes": blueprint.get("flow_nodes") or [],
        "flow_edges": blueprint.get("flow_edges") or [],
        "suggested_charts": blueprint.get("suggested_charts") or [],
        "suggested_actions": blueprint.get("suggested_actions") or [],
        "detected_url": blueprint.get("detected_url"),
        "chat_history_count": len(body.chat_history or []),
        "engine": engine,
    }


def _workflow_chat_slug(text: str, fallback: str = "workflow") -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", str(text or "").lower()).strip("-")
    cleaned = cleaned[:48].strip("-")
    return cleaned or fallback


def _workflow_chat_default_api_url(detected_url: Optional[str]) -> str:
    cleaned = str(detected_url or "").strip()
    if cleaned:
        return cleaned
    return "https://dummyjson.com/products"


def _workflow_chat_dataset_profile(url: Optional[str]) -> Dict[str, Any]:
    lowered = str(url or "").lower()
    profile: Dict[str, Any] = {
        "json_path": "",
        "select_fields": [],
        "x_field": "id",
        "category_field": "category",
        "metric_field": "value",
        "target_field": "value",
    }

    if "dummyjson.com/products" in lowered:
        profile.update({
            "json_path": "products",
            "select_fields": ["id", "title", "category", "brand", "price", "stock", "rating"],
            "x_field": "id",
            "category_field": "category",
            "metric_field": "price",
            "target_field": "price",
        })
        return profile

    if "escuelajs.co" in lowered and "/products" in lowered:
        profile.update({
            "json_path": "",
            "select_fields": ["id", "title", "price", "category.name", "creationAt", "updatedAt"],
            "x_field": "id",
            "category_field": "category.name",
            "metric_field": "price",
            "target_field": "price",
        })
        return profile

    if "open-meteo" in lowered or "weather" in lowered:
        profile.update({
            "json_path": "daily",
            "select_fields": ["time", "weather_code", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
            "x_field": "time",
            "category_field": "weather_code",
            "metric_field": "temperature_2m_max",
            "target_field": "temperature_2m_max",
        })
        return profile

    return profile


def _workflow_chat_node(
    node_id: str,
    node_type: str,
    label: str,
    x: int,
    y: int,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "id": node_id,
        "type": "etlNode",
        "position": {"x": int(x), "y": int(y)},
        "data": {
            "nodeType": node_type,
            "label": label,
            "config": dict(config or {}),
            "status": "idle",
        },
    }


def _workflow_chat_edge(source: str, target: str, label: Optional[str] = None) -> Dict[str, Any]:
    edge: Dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "source": source,
        "target": target,
        "animated": True,
    }
    if label:
        edge["label"] = label
    return edge


def _workflow_chat_create_etl_configuration(
    db: Session,
    question: str,
    detected_url: Optional[str],
    context_key: str,
) -> Dict[str, Any]:
    source_url = _workflow_chat_default_api_url(detected_url)
    profile = _workflow_chat_dataset_profile(source_url)
    now_key = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    slug = _workflow_chat_slug(context_key, "workflow")

    trigger_id = "trigger_1"
    source_id = "source_1"
    select_id = "select_1"
    destination_id = "destination_1"

    source_config: Dict[str, Any] = {
        "method": "GET",
        "url": source_url,
        "headers": {},
        "params": {},
        "body": "",
    }
    json_path = str(profile.get("json_path") or "").strip()
    if json_path:
        source_config["json_path"] = json_path

    select_config: Dict[str, Any] = {}
    select_fields = profile.get("select_fields") or []
    if isinstance(select_fields, list) and select_fields:
        select_config["fields"] = select_fields

    destination_config = {
        "file_path": f"workflow_chat_{slug}_{now_key}.csv",
        "delimiter": ",",
    }

    nodes = [
        _workflow_chat_node(trigger_id, "manual_trigger", "Manual Trigger", 80, 220, {}),
        _workflow_chat_node(source_id, "rest_api_source", "REST API Source", 350, 220, source_config),
        _workflow_chat_node(select_id, "map_transform", "Select Fields", 620, 220, select_config),
        _workflow_chat_node(destination_id, "csv_destination", "CSV Destination", 900, 220, destination_config),
    ]
    edges = [
        _workflow_chat_edge(trigger_id, source_id, "start"),
        _workflow_chat_edge(source_id, select_id, "rows"),
        _workflow_chat_edge(select_id, destination_id, "curated"),
    ]

    pipeline = models.Pipeline(
        id=str(uuid.uuid4()),
        name=f"Workflow Chat ETL {now_key}",
        description=f"Auto-configured ETL pipeline from Workflow Chat for: {question[:240]}",
        nodes=nodes,
        edges=edges,
        tags=["workflow-chat", "auto-config", "etl"],
        status="draft",
        schedule_enabled=False,
    )
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    _sync_pipeline_schedule_job(pipeline)

    return {
        "pipeline": pipeline,
        "pipeline_node_id": select_id,
        "profile": profile,
        "source_url": source_url,
    }


def _workflow_chat_create_mlops_configuration(
    db: Session,
    pipeline_id: str,
    pipeline_node_id: Optional[str],
    profile: Dict[str, Any],
    question: str,
) -> models.MLOpsWorkflow:
    target_column = str(profile.get("target_field") or "value")
    now_key = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    endpoint_slug = _workflow_chat_slug(target_column, "prediction")

    nodes = [
        _workflow_chat_node("ml_trigger_1", "ml_manual_trigger", "Manual Run", 80, 220, {}),
        _workflow_chat_node(
            "ml_source_1",
            "ml_pipeline_output_source",
            "ETL Pipeline Output",
            330,
            220,
            {"pipeline_id": pipeline_id, "pipeline_node_id": pipeline_node_id},
        ),
        _workflow_chat_node(
            "ml_staging_1",
            "ml_data_staging",
            "Data Staging",
            580,
            220,
            {"deduplicate": True, "drop_null_threshold": 30, "target_column": target_column},
        ),
        _workflow_chat_node(
            "ml_feature_1",
            "ml_feature_engineering",
            "Feature Engineering",
            830,
            220,
            {"categorical_strategy": "one_hot", "scaler": "standard"},
        ),
        _workflow_chat_node(
            "ml_train_1",
            "ml_model_training",
            "Model Training",
            1080,
            220,
            {"task_type": "regression", "algorithm": "random_forest", "target_column": target_column},
        ),
        _workflow_chat_node(
            "ml_eval_1",
            "ml_model_evaluation",
            "Model Evaluation",
            1330,
            220,
            {"primary_metric": "rmse", "threshold": 0.8},
        ),
        _workflow_chat_node(
            "ml_deploy_1",
            "ml_deployment_endpoint",
            "Deploy Endpoint",
            1580,
            220,
            {"endpoint_name": f"{endpoint_slug}-endpoint", "autoscale_min": 1, "autoscale_max": 2},
        ),
    ]
    edges = [
        _workflow_chat_edge("ml_trigger_1", "ml_source_1", "start"),
        _workflow_chat_edge("ml_source_1", "ml_staging_1", "rows"),
        _workflow_chat_edge("ml_staging_1", "ml_feature_1", "clean"),
        _workflow_chat_edge("ml_feature_1", "ml_train_1", "features"),
        _workflow_chat_edge("ml_train_1", "ml_eval_1", "model"),
        _workflow_chat_edge("ml_eval_1", "ml_deploy_1", "approved"),
    ]

    workflow = models.MLOpsWorkflow(
        id=str(uuid.uuid4()),
        name=f"Workflow Chat MLOps {now_key}",
        description=f"Auto-configured MLOps workflow for: {question[:240]}",
        nodes=nodes,
        edges=edges,
        tags=["workflow-chat", "auto-config", "mlops"],
        status="draft",
        schedule_enabled=False,
    )
    db.add(workflow)
    db.commit()
    db.refresh(workflow)
    return workflow


def _workflow_chat_create_business_configuration(
    db: Session,
    pipeline_id: Optional[str],
    mlops_workflow_id: Optional[str],
    question: str,
) -> models.BusinessWorkflow:
    now_key = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    source_is_mlops = bool(str(mlops_workflow_id or "").strip())
    source_type = "business_mlops_source" if source_is_mlops else "business_etl_source"
    source_label = "MLOps Output Source" if source_is_mlops else "ETL Output Source"

    source_config: Dict[str, Any] = {}
    if source_is_mlops:
        source_config["mlops_workflow_id"] = str(mlops_workflow_id)
    elif pipeline_id:
        source_config["pipeline_id"] = str(pipeline_id)

    prompt_text = str(question or "").strip()
    if len(prompt_text) > 500:
        prompt_text = prompt_text[:500].rstrip() + "..."
    if not prompt_text:
        prompt_text = "Analyze incoming data and recommend next best action."

    nodes = [
        _workflow_chat_node("biz_trigger_1", "business_manual_trigger", "Manual Trigger", 80, 220, {}),
        _workflow_chat_node("biz_source_1", source_type, source_label, 330, 220, source_config),
        _workflow_chat_node(
            "biz_prompt_1",
            "business_llm_prompt",
            "Prompt Decision Model",
            580,
            220,
            {
                "model": "gpt-oss20b",
                "strict_mode": True,
                "output_format": "text",
                "include_sample_rows": True,
                "sample_row_limit": 2,
                "max_tokens": 1024,
                "prompt": prompt_text,
            },
        ),
        _workflow_chat_node(
            "biz_decision_1",
            "business_decision",
            "Decision Gate",
            830,
            220,
            {
                "field_name": "",
                "operator": "contains",
                "compare_value": "yes",
                "true_mode": "pass_through",
                "false_mode": "emit_summary",
            },
        ),
        _workflow_chat_node(
            "biz_mail_1",
            "business_mail_writer",
            "Mail Writer",
            1080,
            150,
            {
                "to_email": "team@example.com",
                "subject": "Business Workflow Recommendation",
                "send_mode": "draft",
            },
        ),
        _workflow_chat_node(
            "biz_whatsapp_1",
            "business_whatsapp_sender",
            "WhatsApp Sender",
            1080,
            300,
            {
                "to_phone": "919999999999",
                "send_mode": "draft",
                "message_type": "text",
                "text_body": "Workflow Chat generated notification.",
            },
        ),
    ]
    edges = [
        _workflow_chat_edge("biz_trigger_1", "biz_source_1", "start"),
        _workflow_chat_edge("biz_source_1", "biz_prompt_1", "rows"),
        _workflow_chat_edge("biz_prompt_1", "biz_decision_1", "decision"),
        _workflow_chat_edge("biz_decision_1", "biz_mail_1", "notify"),
        _workflow_chat_edge("biz_decision_1", "biz_whatsapp_1", "notify"),
    ]

    workflow = models.BusinessWorkflow(
        id=str(uuid.uuid4()),
        name=f"Workflow Chat Business {now_key}",
        description=f"Auto-configured business workflow for: {question[:240]}",
        nodes=nodes,
        edges=edges,
        tags=["workflow-chat", "auto-config", "business"],
        status="draft",
    )
    db.add(workflow)
    db.commit()
    db.refresh(workflow)
    return workflow


def _workflow_chat_widget_base_style() -> Dict[str, Any]:
    return {
        "theme": "dark",
        "show_legend": True,
        "show_grid": True,
        "show_labels": True,
        "color_palette": ["#3b82f6", "#22c55e", "#f59e0b", "#a855f7", "#ef4444", "#06b6d4"],
    }


def _workflow_chat_create_dashboard_configuration(
    db: Session,
    context_key: str,
    question: str,
    source_type: str,
    pipeline_id: Optional[str],
    mlops_workflow_id: Optional[str],
    profile: Dict[str, Any],
) -> models.Dashboard:
    metric_field = str(profile.get("metric_field") or "value")
    category_field = str(profile.get("category_field") or "category")
    x_field = str(profile.get("x_field") or "id")
    now_key = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    data_source: Dict[str, Any] = {"type": source_type}
    if source_type == "pipeline" and pipeline_id:
        data_source["pipeline_id"] = pipeline_id
    elif source_type == "mlops" and mlops_workflow_id:
        data_source["mlops_workflow_id"] = mlops_workflow_id
        data_source["mlops_output_mode"] = "predictions"
    else:
        data_source["type"] = "sample"
        data_source["dataset"] = "sales"

    widget_style = _workflow_chat_widget_base_style()

    widget_kpi_id = str(uuid.uuid4())
    widget_bar_id = str(uuid.uuid4())
    widget_line_id = str(uuid.uuid4())
    widget_table_id = str(uuid.uuid4())

    widgets = [
        {
            "id": widget_kpi_id,
            "type": "kpi",
            "title": "Primary KPI",
            "subtitle": "Auto-configured from Workflow Chat",
            "data_source": dict(data_source),
            "chart_config": {
                "kpi_field": metric_field,
                "kpi_label": f"Average {metric_field}",
                "aggregation": "avg",
                "trend_field": x_field,
            },
            "style": dict(widget_style),
        },
        {
            "id": widget_bar_id,
            "type": "bar",
            "title": "Category Distribution",
            "data_source": dict(data_source),
            "chart_config": {
                "x_field": category_field,
                "y_field": metric_field,
                "aggregation": "sum",
                "limit": 25,
            },
            "style": dict(widget_style),
        },
        {
            "id": widget_line_id,
            "type": "line",
            "title": "Trend Analysis",
            "data_source": dict(data_source),
            "chart_config": {
                "x_field": x_field,
                "y_field": metric_field,
                "aggregation": "avg",
                "limit": 100,
            },
            "style": dict(widget_style),
        },
        {
            "id": widget_table_id,
            "type": "table",
            "title": "Detailed Records",
            "data_source": dict(data_source),
            "chart_config": {"limit": 200},
            "style": dict(widget_style),
        },
    ]
    layout = [
        {"i": widget_kpi_id, "x": 0, "y": 0, "w": 6, "h": 4, "minW": 4, "minH": 3},
        {"i": widget_bar_id, "x": 0, "y": 4, "w": 12, "h": 8, "minW": 4, "minH": 4},
        {"i": widget_line_id, "x": 12, "y": 4, "w": 12, "h": 8, "minW": 4, "minH": 4},
        {"i": widget_table_id, "x": 0, "y": 12, "w": 24, "h": 9, "minW": 6, "minH": 4},
    ]

    dashboard = models.Dashboard(
        id=str(uuid.uuid4()),
        name=f"Workflow Chat Dashboard {now_key}",
        description=f"Auto-configured {context_key} dashboard for: {question[:240]}",
        theme="dark",
        tags=["workflow-chat", "auto-config", context_key],
        widgets=widgets,
        layout=layout,
        global_filters=[],
    )
    db.add(dashboard)
    db.commit()
    db.refresh(dashboard)
    return dashboard


def _workflow_chat_append_resource(
    resources: List[Dict[str, Any]],
    module: str,
    kind: str,
    resource_id: str,
    name: str,
    route: str,
):
    resources.append({
        "module": module,
        "kind": kind,
        "id": resource_id,
        "name": name,
        "route": route,
    })


@app.post("/api/workflow-chat/configure")
async def workflow_chat_configure(body: WorkflowChatConfigureRequest, db: Session = Depends(get_db)):
    question = str(body.question or "").strip()
    if not question:
        raise HTTPException(400, "question is required")

    context_type = _workflow_chat_detect_context(question, body.context_type)
    blueprint = _workflow_chat_blueprint(context_type, question)
    detected_url = str(blueprint.get("detected_url") or "").strip() or None

    created_resources: List[Dict[str, Any]] = []
    warnings: List[str] = []

    latest_pipeline = db.query(models.Pipeline).order_by(models.Pipeline.updated_at.desc()).first()
    latest_mlops = db.query(models.MLOpsWorkflow).order_by(models.MLOpsWorkflow.updated_at.desc()).first()

    profile = _workflow_chat_dataset_profile(detected_url)
    pipeline_row: Optional[models.Pipeline] = None
    pipeline_node_id: Optional[str] = None
    mlops_row: Optional[models.MLOpsWorkflow] = None
    business_row: Optional[models.BusinessWorkflow] = None

    should_build_etl = context_type in {"etl", "application"} or (
        context_type == "visualization" and bool(detected_url)
    )
    if should_build_etl:
        etl_cfg = _workflow_chat_create_etl_configuration(db, question, detected_url, context_type)
        pipeline_row = etl_cfg["pipeline"]
        pipeline_node_id = etl_cfg["pipeline_node_id"]
        profile = etl_cfg["profile"]
        detected_url = etl_cfg["source_url"]
        _workflow_chat_append_resource(
            created_resources,
            "ETL",
            "pipeline",
            pipeline_row.id,
            pipeline_row.name,
            f"/pipelines/{pipeline_row.id}/edit",
        )
    else:
        pipeline_row = latest_pipeline

    if context_type in {"mlops", "application"}:
        if not pipeline_row:
            etl_cfg = _workflow_chat_create_etl_configuration(db, question, detected_url, "mlops")
            pipeline_row = etl_cfg["pipeline"]
            pipeline_node_id = etl_cfg["pipeline_node_id"]
            profile = etl_cfg["profile"]
            detected_url = etl_cfg["source_url"]
            warnings.append("No ETL pipeline was available, so a new ETL configuration was created automatically.")
            _workflow_chat_append_resource(
                created_resources,
                "ETL",
                "pipeline",
                pipeline_row.id,
                pipeline_row.name,
                f"/pipelines/{pipeline_row.id}/edit",
            )

        mlops_row = _workflow_chat_create_mlops_configuration(
            db=db,
            pipeline_id=pipeline_row.id,
            pipeline_node_id=pipeline_node_id,
            profile=profile,
            question=question,
        )
        _workflow_chat_append_resource(
            created_resources,
            "MLOps",
            "workflow",
            mlops_row.id,
            mlops_row.name,
            f"/mlops/{mlops_row.id}/edit",
        )
    elif context_type == "business":
        mlops_row = latest_mlops
        if not mlops_row and not pipeline_row:
            etl_cfg = _workflow_chat_create_etl_configuration(db, question, detected_url, "business")
            pipeline_row = etl_cfg["pipeline"]
            pipeline_node_id = etl_cfg["pipeline_node_id"]
            profile = etl_cfg["profile"]
            detected_url = etl_cfg["source_url"]
            warnings.append("No ETL/MLOps source was available, so a new ETL configuration was created automatically.")
            _workflow_chat_append_resource(
                created_resources,
                "ETL",
                "pipeline",
                pipeline_row.id,
                pipeline_row.name,
                f"/pipelines/{pipeline_row.id}/edit",
            )

    if context_type in {"business", "application"}:
        if not pipeline_row and not mlops_row:
            etl_cfg = _workflow_chat_create_etl_configuration(db, question, detected_url, "business")
            pipeline_row = etl_cfg["pipeline"]
            pipeline_node_id = etl_cfg["pipeline_node_id"]
            profile = etl_cfg["profile"]
            detected_url = etl_cfg["source_url"]
            warnings.append("Business workflow needs source data, so a new ETL configuration was created automatically.")
            _workflow_chat_append_resource(
                created_resources,
                "ETL",
                "pipeline",
                pipeline_row.id,
                pipeline_row.name,
                f"/pipelines/{pipeline_row.id}/edit",
            )

        business_row = _workflow_chat_create_business_configuration(
            db=db,
            pipeline_id=pipeline_row.id if pipeline_row else None,
            mlops_workflow_id=mlops_row.id if mlops_row else None,
            question=question,
        )
        _workflow_chat_append_resource(
            created_resources,
            "Business",
            "workflow",
            business_row.id,
            business_row.name,
            f"/business/{business_row.id}/edit",
        )

    dashboard_row: Optional[models.Dashboard] = None
    if body.include_dashboard:
        dashboard_source_type = "sample"
        dashboard_pipeline_id: Optional[str] = None
        dashboard_mlops_id: Optional[str] = None

        if mlops_row:
            dashboard_source_type = "mlops"
            dashboard_mlops_id = mlops_row.id
        elif pipeline_row:
            dashboard_source_type = "pipeline"
            dashboard_pipeline_id = pipeline_row.id
        else:
            warnings.append("No pipeline/model source found; dashboard was configured with sample datasource.")

        dashboard_row = _workflow_chat_create_dashboard_configuration(
            db=db,
            context_key=context_type,
            question=question,
            source_type=dashboard_source_type,
            pipeline_id=dashboard_pipeline_id,
            mlops_workflow_id=dashboard_mlops_id,
            profile=profile,
        )
        _workflow_chat_append_resource(
            created_resources,
            "Visualization",
            "dashboard",
            dashboard_row.id,
            dashboard_row.name,
            f"/dashboards/{dashboard_row.id}/edit",
        )

    summary = (
        f"Configured {len(created_resources)} resource(s) for {context_type}. "
        "Open each module route to review and run."
    )
    return {
        "question": question,
        "context_type": context_type,
        "summary": summary,
        "detected_url": detected_url,
        "created_resources": created_resources,
        "warnings": warnings,
        "steps": blueprint.get("steps") or [],
        "chat_history_count": len(body.chat_history or []),
        "engine": {"used": True, "provider": "python", "model": "workflow-config-engine"},
    }


@app.post("/api/nlp-chat/schema")
async def nlp_chat_schema(body: NLPChatSchemaRequest, db: Session = Depends(get_db)):
    payload = body.model_dump(exclude_none=True)
    rows = _load_source_rows_for_query(payload, db, apply_sql=True)
    tabular_rows = [r for r in rows if isinstance(r, dict)]
    if not tabular_rows:
        raise HTTPException(400, "Selected datasource returned no tabular rows")

    sample_size = max(50, min(int(body.sample_size or 300), 2000))
    sample_rows = tabular_rows[:sample_size]
    schema = _nlp_infer_schema(sample_rows)

    suggested_dimensions = (
        list(schema.get("datetime_columns") or [])
        + list(schema.get("categorical_columns") or [])
        + list(schema.get("text_columns") or [])
    )

    return {
        "row_count": len(tabular_rows),
        "sample_size": len(sample_rows),
        "columns": schema.get("columns") or [],
        "column_details": schema.get("column_details") or [],
        "suggested_metrics": (schema.get("numeric_columns") or [])[:20],
        "suggested_dimensions": suggested_dimensions[:30],
        "sample_rows": sample_rows[:25],
    }


@app.post("/api/nlp-chat/ask")
async def nlp_chat_ask(body: NLPChatAskRequest, db: Session = Depends(get_db)):
    question = str(body.question or "").strip()
    if not question:
        raise HTTPException(400, "question is required")
    query_text = question

    payload = body.model_dump(exclude_none=True)
    rows = _load_source_rows_for_query(payload, db, apply_sql=True)
    tabular_rows = [r for r in rows if isinstance(r, dict)]
    if not tabular_rows:
        raise HTTPException(400, "Selected datasource returned no tabular rows")

    schema_rows = tabular_rows[: min(1200, len(tabular_rows))]
    schema = _nlp_infer_schema(schema_rows)
    columns = list(schema.get("columns") or [])
    if not columns:
        raise HTTPException(400, "No columns found in datasource")

    vocab_rows = tabular_rows[: min(1200, len(tabular_rows))]
    vocabulary = _nlp_build_vocabulary(vocab_rows, schema)
    corrected_question, corrections = _nlp_autocorrect_question(question, vocabulary)
    query_text = corrected_question or question
    numeric_columns = list(schema.get("numeric_columns") or [])
    dimension_candidates = (
        list(schema.get("datetime_columns") or [])
        + list(schema.get("categorical_columns") or [])
        + list(schema.get("text_columns") or [])
    )

    requested_metrics = list(body.metric_fields or []) + list(body.matrix_fields or [])
    metric_fields = [c for c in dict.fromkeys(requested_metrics) if c in columns]
    requested_dimensions = list(body.dimension_fields or [])
    dimension_fields = [c for c in dict.fromkeys(requested_dimensions) if c in columns]

    q_metric_fields, q_dimension_fields = _nlp_pick_fields_from_question(
        query_text,
        columns=columns,
        numeric_columns=numeric_columns,
        dimension_columns=dimension_candidates,
    )
    if q_metric_fields:
        metric_fields = list(dict.fromkeys(q_metric_fields + metric_fields))
    if q_dimension_fields:
        dimension_fields = list(dict.fromkeys(q_dimension_fields + dimension_fields))

    if not metric_fields:
        metric_fields = numeric_columns[:4]
    if not dimension_fields:
        dimension_fields = dimension_candidates[:4]

    parsed_group_dims = _nlp_parse_group_columns(query_text, dimension_candidates)
    if parsed_group_dims:
        dimension_fields = parsed_group_dims + [d for d in dimension_fields if d not in parsed_group_dims]

    primary_dimension = _nlp_pick_primary_dimension(query_text, dimension_fields)
    if primary_dimension:
        dimension_fields = [primary_dimension] + [d for d in dimension_fields if d != primary_dimension]

    question_filters = _nlp_extract_question_filters(
        question=query_text,
        rows=tabular_rows,
        dimension_columns=dimension_candidates or dimension_fields,
        metric_candidates=metric_fields or numeric_columns,
        all_columns=columns,
        numeric_columns=numeric_columns,
    )
    filtered_rows = _nlp_apply_row_filters(tabular_rows, question_filters)
    analysis_rows = filtered_rows if filtered_rows else tabular_rows

    relevant_columns = _nlp_relevant_columns(query_text, columns, metric_fields, dimension_fields)
    requested_row_limit = _nlp_parse_requested_limit(query_text, default_limit=12, max_limit=100)
    semantic_top_k = max(
        int(body.top_k or 12),
        requested_row_limit * 5,
        int(body.include_rows or 8) * 3,
        30,
    )
    semantic_top_k = min(max(10, semantic_top_k), 300)
    ranking, semantic_engine, total_rows, indexed_rows = _nlp_semantic_rank_rows(
        rows=analysis_rows,
        query=query_text,
        columns=relevant_columns or columns,
        top_k=semantic_top_k,
        mode=body.semantic_mode,
    )

    include_rows = max(3, min(int(body.include_rows or 8), 20))
    matched_rows = [item.get("row") for item in ranking[:include_rows] if isinstance(item, dict) and isinstance(item.get("row"), dict)]

    import pandas as pd
    df = pd.DataFrame(analysis_rows)
    metric_summary = _nlp_metric_summary(df, metric_fields)
    group_summary = _nlp_group_summary(df, metric_fields, dimension_fields[:1])
    one_line_highlight = _nlp_one_line_highlight(metric_fields, group_summary, metric_summary)
    recommendation = _nlp_one_line_recommendation(
        question=query_text,
        metric_fields=metric_fields,
        dimension_fields=dimension_fields,
        group_summary=group_summary,
        metric_summary=metric_summary,
    )
    table_columns, table_rows = _nlp_build_table_response(
        question=query_text,
        ranking=ranking,
        analysis_rows=analysis_rows,
        relevant_columns=relevant_columns,
        metric_fields=metric_fields,
        dimension_fields=dimension_fields,
        group_summary=group_summary,
        max_rows=12,
        max_cols=8,
    )

    domain_context = str(body.domain_context or "").strip()
    detailed_answer = _nlp_chat_fallback_answer(
        question=question,
        corrected_question=corrected_question,
        corrections=corrections,
        metric_summary=metric_summary,
        group_summary=group_summary,
        matched_rows=matched_rows,
        domain_context=domain_context,
        metric_fields=metric_fields,
        dimension_fields=dimension_fields,
        semantic_engine=semantic_engine,
    )

    response_engine = {
        "used": True,
        "provider": "python",
        "model": "python-semantic-analytics",
        "api_kind": "rule_based",
        "reason": None,
    }

    return {
        "answer": recommendation,
        "recommendation": recommendation,
        "detailed_answer": detailed_answer,
        "question": question,
        "corrected_question": corrected_question,
        "corrections": corrections,
        "semantic_engine": semantic_engine,
        "response_engine": response_engine,
        # Backward-compatible key used by existing frontend components.
        "llm_engine": response_engine,
        "one_line_highlight": one_line_highlight,
        "table_response_columns": ["_rank", "_score"] + table_columns,
        "table_response_rows": table_rows,
        "selected_fields": {
            "metrics": metric_fields,
            "dimensions": dimension_fields,
            "relevant_columns": relevant_columns,
        },
        "applied_filters": question_filters,
        "domain_context": domain_context,
        "row_count": total_rows,
        "source_row_count": len(tabular_rows),
        "filtered_row_count": len(analysis_rows),
        "indexed_rows": indexed_rows,
        "retrieval": ranking[:include_rows],
        "metric_summary": metric_summary,
        "group_summary": group_summary,
    }

def _get_sample_data(dataset: str) -> list:
    import random
    random.seed(42)
    datasets = {
        "sales": [
            {"region": r, "product": p, "year": y, "quarter": q,
             "revenue": round(random.uniform(50000, 500000), 2),
             "profit": round(random.uniform(5000, 120000), 2),
             "units": random.randint(100, 5000),
             "growth": round(random.uniform(-15, 40), 1)}
            for r in ["North", "South", "East", "West", "Central"]
            for p in ["Product A", "Product B", "Product C", "Product D"]
            for y in [2022, 2023, 2024]
            for q in ["Q1", "Q2", "Q3", "Q4"]
        ],
        "web_analytics": [
            {"date": f"2024-{m:02d}-{d:02d}",
             "sessions": random.randint(800, 8000),
             "users": random.randint(600, 6000),
             "pageviews": random.randint(2000, 20000),
             "bounce_rate": round(random.uniform(25, 65), 1),
             "avg_session": round(random.uniform(1.5, 8.5), 2),
             "channel": random.choice(["Organic", "Direct", "Referral", "Social", "Email"]),
             "country": random.choice(["USA", "UK", "India", "Germany", "France", "Canada"])}
            for m in range(1, 13) for d in range(1, 29)
        ],
        "financials": [
            {"month": f"2024-{m:02d}", "month_name": ["Jan","Feb","Mar","Apr","May","Jun",
              "Jul","Aug","Sep","Oct","Nov","Dec"][m-1],
             "revenue": round(random.uniform(800000, 2000000), 2),
             "expenses": round(random.uniform(500000, 1200000), 2),
             "net_profit": round(random.uniform(100000, 800000), 2),
             "cash_flow": round(random.uniform(-200000, 600000), 2),
             "department": random.choice(["Sales","Engineering","Marketing","Operations","HR"])}
            for m in range(1, 13)
        ],
        "employees": [
            {"employee_id": i, "name": f"Employee {i}",
             "department": random.choice(["Engineering","Sales","Marketing","Finance","HR","Operations"]),
             "level": random.choice(["Junior","Mid","Senior","Lead","Manager","Director"]),
             "salary": round(random.uniform(45000, 250000), 0),
             "tenure_years": round(random.uniform(0.5, 15), 1),
             "performance": round(random.uniform(2.5, 5.0), 1),
             "location": random.choice(["New York","London","Bangalore","Singapore","Berlin","Toronto"])}
            for i in range(1, 501)
        ],
        "supply_chain": [
            {"supplier": f"Supplier {chr(65+i%10)}", "product_category": cat,
             "lead_time_days": random.randint(3, 45),
             "defect_rate": round(random.uniform(0.1, 8.5), 2),
             "on_time_delivery": round(random.uniform(75, 99), 1),
             "cost_per_unit": round(random.uniform(2, 250), 2),
             "order_quantity": random.randint(100, 10000)}
            for i in range(100)
            for cat in [random.choice(["Electronics","Packaging","Raw Materials","Components","Services"])]
        ],
        "customer": [
            {"customer_id": i,
             "segment": random.choice(["Enterprise","SMB","Startup","Individual"]),
             "country": random.choice(["USA","UK","Germany","France","India","Japan","Brazil","Australia"]),
             "acquisition_channel": random.choice(["Inbound","Outbound","Referral","Partnership","Event"]),
             "ltv": round(random.uniform(500, 50000), 2),
             "churn_risk": random.choice(["Low","Medium","High"]),
             "satisfaction_score": round(random.uniform(3.0, 5.0), 1),
             "orders": random.randint(1, 120)}
            for i in range(1, 1001)
        ],
    }
    return datasets.get(dataset, datasets["sales"])


# ─── USERS & ADMIN ────────────────────────────────────────────────────────────

class UserCreate(BaseModel):
    name: str
    email: str
    role: str = "viewer"

class UserUpdate(BaseModel):
    name: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None

@app.get("/api/users")
async def list_users(db: Session = Depends(get_db)):
    users = db.query(models.AppUser).order_by(models.AppUser.created_at.desc()).all()
    if not users:
        # Seed default admin
        admin = models.AppUser(id=str(uuid.uuid4()), name="Admin User",
                               email="admin@etlflow.io", role="admin", is_active=True)
        db.add(admin); db.commit()
        users = [admin]
    return [{"id": u.id, "name": u.name, "email": u.email, "role": u.role,
             "is_active": u.is_active,
             "last_login": u.last_login.isoformat() if u.last_login else None,
             "created_at": u.created_at.isoformat() if u.created_at else None} for u in users]

@app.post("/api/users", status_code=201)
async def create_user(body: UserCreate, db: Session = Depends(get_db)):
    u = models.AppUser(id=str(uuid.uuid4()), name=body.name,
                       email=body.email, role=body.role)
    db.add(u); db.commit(); db.refresh(u)
    _audit(db, "admin", "create", "user", u.id, u.email)
    return {"id": u.id, "name": u.name, "email": u.email}

@app.put("/api/users/{uid}")
async def update_user(uid: str, body: UserUpdate, db: Session = Depends(get_db)):
    u = db.query(models.AppUser).filter(models.AppUser.id == uid).first()
    if not u: raise HTTPException(404, "User not found")
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(u, k, v)
    db.commit()
    _audit(db, "admin", "update", "user", u.id, u.email)
    return {"id": u.id, "name": u.name}

@app.delete("/api/users/{uid}", status_code=204)
async def delete_user(uid: str, db: Session = Depends(get_db)):
    u = db.query(models.AppUser).filter(models.AppUser.id == uid).first()
    if u:
        _audit(db, "admin", "delete", "user", u.id, u.email)
        db.delete(u); db.commit()

@app.get("/api/audit-logs")
async def list_audit_logs(limit: int = 200, db: Session = Depends(get_db)):
    rows = db.query(models.AuditLog).order_by(models.AuditLog.created_at.desc()).limit(limit).all()
    return [{"id": r.id, "user": r.user, "action": r.action,
             "resource_type": r.resource_type, "resource_name": r.resource_name,
             "detail": r.detail,
             "created_at": r.created_at.isoformat() if r.created_at else None} for r in rows]

@app.get("/api/admin/stats")
async def admin_stats(db: Session = Depends(get_db)):
    return {
        "users": db.query(models.AppUser).count(),
        "active_users": db.query(models.AppUser).filter(models.AppUser.is_active == True).count(),
        "dashboards": db.query(models.Dashboard).count(),
        "public_dashboards": db.query(models.Dashboard).filter(models.Dashboard.is_public == True).count(),
        "pipelines": db.query(models.Pipeline).count(),
        "mlops_workflows": db.query(models.MLOpsWorkflow).count(),
        "mlops_runs": db.query(models.MLOpsRun).count(),
        "total_executions": db.query(models.Execution).count(),
        "audit_events": db.query(models.AuditLog).count(),
    }

def _audit(db, user: str, action: str, resource_type: str, resource_id: str, resource_name: str = "", detail: str = ""):
    try:
        log = models.AuditLog(id=str(uuid.uuid4()), user=user, action=action,
                              resource_type=resource_type, resource_id=resource_id,
                              resource_name=resource_name, detail=detail)
        db.add(log); db.commit()
    except Exception:
        pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True, log_level="info")
