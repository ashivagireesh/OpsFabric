from sqlalchemy import Column, String, Text, DateTime, Boolean, JSON, Integer, ForeignKey, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base
import enum

class PipelineStatus(str, enum.Enum):
    active = "active"
    inactive = "inactive"
    draft = "draft"

class ExecutionStatus(str, enum.Enum):
    pending = "pending"
    running = "running"
    success = "success"
    failed = "failed"
    cancelled = "cancelled"

class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    nodes = Column(JSON, default=[])
    edges = Column(JSON, default=[])
    status = Column(String, default=PipelineStatus.draft)
    tags = Column(JSON, default=[])
    schedule_cron = Column(String, nullable=True)
    schedule_enabled = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    executions = relationship("Execution", back_populates="pipeline", cascade="all, delete-orphan")

class Execution(Base):
    __tablename__ = "executions"

    id = Column(String, primary_key=True, index=True)
    pipeline_id = Column(String, ForeignKey("pipelines.id"), nullable=False)
    status = Column(String, default=ExecutionStatus.pending)
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
    node_results = Column(JSON, default={})
    logs = Column(JSON, default=[])
    error_message = Column(Text, nullable=True)
    triggered_by = Column(String, default="manual")  # manual, schedule, webhook
    rows_processed = Column(Integer, default=0)
    pipeline = relationship("Pipeline", back_populates="executions")


class MLOpsWorkflow(Base):
    __tablename__ = "mlops_workflows"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    nodes = Column(JSON, default=[])
    edges = Column(JSON, default=[])
    status = Column(String, default=PipelineStatus.draft)
    tags = Column(JSON, default=[])
    schedule_cron = Column(String, nullable=True)
    schedule_enabled = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    runs = relationship("MLOpsRun", back_populates="workflow", cascade="all, delete-orphan")
    h2o_runs = relationship("MLOpsH2ORun", back_populates="workflow", cascade="all, delete-orphan")


class MLOpsRun(Base):
    __tablename__ = "mlops_runs"

    id = Column(String, primary_key=True, index=True)
    workflow_id = Column(String, ForeignKey("mlops_workflows.id"), nullable=False)
    status = Column(String, default=ExecutionStatus.pending)
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
    logs = Column(JSON, default=[])
    metrics = Column(JSON, default={})
    artifact_rows = Column(Integer, default=0)
    model_version = Column(String, nullable=True)
    triggered_by = Column(String, default="manual")
    error_message = Column(Text, nullable=True)
    workflow = relationship("MLOpsWorkflow", back_populates="runs")


class MLOpsH2ORun(Base):
    __tablename__ = "mlops_h2o_runs"

    id = Column(String, primary_key=True, index=True)
    workflow_id = Column(String, ForeignKey("mlops_workflows.id"), nullable=True)
    status = Column(String, default=ExecutionStatus.pending)
    source_type = Column(String, nullable=True)
    source_meta = Column(JSON, default={})
    task = Column(String, nullable=True)
    target_column = Column(String, nullable=True)
    feature_columns = Column(JSON, default=[])
    row_count = Column(Integer, default=0)
    train_rows = Column(Integer, default=0)
    test_rows = Column(Integer, default=0)
    model_id = Column(String, nullable=True)
    model_path = Column(Text, nullable=True)
    mojo_path = Column(Text, nullable=True)
    leaderboard = Column(JSON, default=[])
    metrics = Column(JSON, default={})
    config = Column(JSON, default={})
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)

    workflow = relationship("MLOpsWorkflow", back_populates="h2o_runs")


class BusinessWorkflow(Base):
    __tablename__ = "business_workflows"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    nodes = Column(JSON, default=[])
    edges = Column(JSON, default=[])
    status = Column(String, default=PipelineStatus.draft)
    tags = Column(JSON, default=[])
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    runs = relationship("BusinessRun", back_populates="workflow", cascade="all, delete-orphan")


class BusinessRun(Base):
    __tablename__ = "business_runs"

    id = Column(String, primary_key=True, index=True)
    workflow_id = Column(String, ForeignKey("business_workflows.id"), nullable=False)
    status = Column(String, default=ExecutionStatus.pending)
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
    logs = Column(JSON, default=[])
    node_outputs = Column(JSON, default={})
    metrics = Column(JSON, default={})
    model_name = Column(String, nullable=True)
    triggered_by = Column(String, default="manual")
    error_message = Column(Text, nullable=True)
    workflow = relationship("BusinessWorkflow", back_populates="runs")


class Credential(Base):
    __tablename__ = "credentials"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)  # postgres, mysql, s3, etc.
    data = Column(JSON, default={})  # encrypted credential data
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())


class SystemSetting(Base):
    __tablename__ = "system_settings"

    key = Column(String, primary_key=True, index=True)
    value = Column(JSON, default={})
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())


class Template(Base):
    __tablename__ = "templates"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    category = Column(String, nullable=True)
    nodes = Column(JSON, default=[])
    edges = Column(JSON, default=[])
    tags = Column(JSON, default=[])
    created_at = Column(DateTime(timezone=True), server_default=func.now())


# ─── VISUALISATION / DASHBOARD ────────────────────────────────────────────────

class Dashboard(Base):
    __tablename__ = "dashboards"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    owner = Column(String, default="admin")
    widgets = Column(JSON, default=[])      # list of widget configs
    layout = Column(JSON, default=[])       # react-grid-layout positions
    theme = Column(String, default="dark")
    global_filters = Column(JSON, default=[])
    tags = Column(JSON, default=[])
    is_public = Column(Boolean, default=False)
    share_token = Column(String, nullable=True, unique=True)
    thumbnail = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())


class AppUser(Base):
    __tablename__ = "app_users"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False, unique=True)
    role = Column(String, default="viewer")   # admin | editor | viewer
    avatar = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    last_login = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(String, primary_key=True, index=True)
    user = Column(String, nullable=False)
    action = Column(String, nullable=False)    # create | update | delete | view | share | execute
    resource_type = Column(String, nullable=True)
    resource_id = Column(String, nullable=True)
    resource_name = Column(String, nullable=True)
    detail = Column(Text, nullable=True)
    ip_address = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ApiGatewayRoute(Base):
    __tablename__ = "api_gateway_routes"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    enabled = Column(Boolean, default=True)
    protocol = Column(String, default="rest")
    version = Column(String, default="v1")
    method = Column(String, default="GET")
    path = Column(String, nullable=False)
    status = Column(String, default="active")
    pipeline_id = Column(String, nullable=True, index=True)
    gateway_node_id = Column(String, nullable=True, index=True)
    source_node_id = Column(String, nullable=True, index=True)
    source_type = Column(String, nullable=True)
    source_config = Column(JSON, default={})
    request_mapping = Column(JSON, default={})
    response_mapping = Column(JSON, default={})
    auth_config = Column(JSON, default={})
    rbac_config = Column(JSON, default={})
    rate_limit_config = Column(JSON, default={})
    standard_response = Column(Boolean, default=True)
    audit_enabled = Column(Boolean, default=True)
    logging_enabled = Column(Boolean, default=True)
    managed_by_pipeline = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())


class ApiGatewayLog(Base):
    __tablename__ = "api_gateway_logs"

    id = Column(String, primary_key=True, index=True)
    route_id = Column(String, ForeignKey("api_gateway_routes.id"), nullable=True, index=True)
    request_id = Column(String, nullable=False, index=True)
    method = Column(String, nullable=True)
    path = Column(Text, nullable=True)
    version = Column(String, nullable=True)
    status_code = Column(Integer, nullable=True)
    success = Column(Boolean, default=False)
    duration_ms = Column(Integer, default=0)
    client_ip = Column(String, nullable=True)
    user_id = Column(String, nullable=True)
    role = Column(String, nullable=True)
    error_message = Column(Text, nullable=True)
    request_meta = Column(JSON, default={})
    response_meta = Column(JSON, default={})
    created_at = Column(DateTime(timezone=True), server_default=func.now())
