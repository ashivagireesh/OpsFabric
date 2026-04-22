import type { Node, Edge } from 'reactflow'

// ─── NODE SYSTEM ──────────────────────────────────────────────────────────────

export type NodeCategory = 'trigger' | 'source' | 'transform' | 'destination' | 'flow'

export interface ConfigField {
  name: string
  label: string
  type: 'text' | 'password' | 'number' | 'select' | 'textarea' | 'code' | 'toggle' | 'json' | 'file'
  required?: boolean
  options?: { value: string; label: string }[]
  placeholder?: string
  defaultValue?: unknown
  description?: string
  language?: string // for code fields
}

export interface NodeTypeDefinition {
  type: string
  label: string
  category: NodeCategory
  icon: string        // emoji or icon name
  color: string       // hex color for the node accent
  bgColor: string     // card background tint
  description: string
  inputs: number
  outputs: number
  configFields: ConfigField[]
  tags?: string[]
}

// ReactFlow node data
export interface ETLNodeData {
  nodeType: string
  label: string
  definition: NodeTypeDefinition
  config: Record<string, unknown>
  status?: 'idle' | 'running' | 'success' | 'error'
  executionRows?: number
  executionProcessedRows?: number
  executionValidatedRows?: number
  executionStartedAt?: string
  executionFinishedAt?: string
  executionDurationMs?: number
  executionSampleInput?: Record<string, unknown>[]
  executionSampleOutput?: Record<string, unknown>[]
  executionError?: string
}

export type ETLNode = Node<ETLNodeData>
export type ETLEdge = Edge

// ─── PIPELINE ─────────────────────────────────────────────────────────────────

export interface Pipeline {
  id: string
  name: string
  description?: string
  nodes: ETLNode[]
  edges: ETLEdge[]
  status: 'active' | 'inactive' | 'draft'
  tags: string[]
  schedule_cron?: string
  schedule_enabled?: boolean
  execution_mode?: 'batch' | 'incremental' | 'streaming'
  batch_size?: number
  incremental_field?: string
  streaming_interval_seconds?: number
  streaming_max_batches?: number
  next_scheduled_at?: string | null
  created_at: string
  updated_at: string
  nodeCount?: number
  last_execution?: ExecutionSummary
}

export interface ExecutionSummary {
  id: string
  status: ExecutionStatus
  started_at: string
  rows_processed: number
}

// ─── MLOPS ────────────────────────────────────────────────────────────────────

export interface MLOpsRunSummary {
  id: string
  status: ExecutionStatus
  started_at: string
  artifact_rows: number
  model_version?: string
  metrics?: Record<string, unknown>
}

export interface MLOpsWorkflow {
  id: string
  name: string
  description?: string
  nodes: ETLNode[]
  edges: ETLEdge[]
  status: 'active' | 'inactive' | 'draft'
  tags: string[]
  schedule_cron?: string
  schedule_enabled?: boolean
  created_at: string
  updated_at: string
  nodeCount?: number
  last_run?: MLOpsRunSummary | null
}

export interface MLOpsRun {
  id: string
  workflow_id: string
  workflow_name?: string
  status: ExecutionStatus
  started_at: string
  finished_at?: string
  duration?: number
  artifact_rows: number
  model_version?: string
  metrics: Record<string, unknown>
  logs: LogEntry[]
  error_message?: string
  triggered_by: string
}

// ─── BUSINESS WORKFLOW ───────────────────────────────────────────────────────

export interface BusinessRunSummary {
  id: string
  status: ExecutionStatus
  started_at: string
  model_name?: string
  metrics?: Record<string, unknown>
}

export interface BusinessWorkflow {
  id: string
  name: string
  description?: string
  nodes: ETLNode[]
  edges: ETLEdge[]
  status: 'active' | 'inactive' | 'draft'
  tags: string[]
  created_at: string
  updated_at: string
  nodeCount?: number
  last_run?: BusinessRunSummary | null
}

export interface BusinessRun {
  id: string
  workflow_id: string
  workflow_name?: string
  status: ExecutionStatus
  started_at: string
  finished_at?: string
  duration?: number
  metrics: Record<string, unknown>
  model_name?: string
  logs: LogEntry[]
  node_outputs?: Record<string, unknown>
  error_message?: string
  triggered_by: string
}

// ─── EXECUTION ────────────────────────────────────────────────────────────────

export type ExecutionStatus = 'pending' | 'running' | 'success' | 'failed' | 'cancelled'

export interface LogEntry {
  nodeId: string
  nodeLabel: string
  timestamp: string
  status: 'running' | 'success' | 'error'
  message: string
  rows: number
  input_sample?: Record<string, unknown>[]
  output_sample?: Record<string, unknown>[]
}

export interface Execution {
  id: string
  pipeline_id: string
  pipeline_name?: string
  status: ExecutionStatus
  started_at: string
  finished_at?: string
  duration?: number
  node_results: Record<string, unknown[]>
  logs: LogEntry[]
  error_message?: string
  rows_processed: number
  triggered_by: string
}

// ─── CREDENTIALS ──────────────────────────────────────────────────────────────

export interface Credential {
  id: string
  name: string
  type: string
  created_at: string
}

// ─── STATS ────────────────────────────────────────────────────────────────────

export interface DashboardStats {
  total_pipelines: number
  active_pipelines: number
  total_executions: number
  successful_executions: number
  failed_executions: number
  running_executions: number
  total_rows_processed: number
  success_rate: number
}
