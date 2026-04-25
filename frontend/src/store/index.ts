import { create } from 'zustand'
import { parseTimestampMs, parseTimestampMsOrNaN } from '../utils/time'

// ─── ROW COUNTS per category for realistic simulation ─────────────────────────
const MOCK_ROW_COUNTS: Record<string, number[]> = {
  trigger:     [0],
  source:      [500, 1200, 3400, 8900, 12450, 25000, 89320],
  transform:   [],   // derived from upstream
  destination: [0],
  flow:        [],
}

function randomRows(category: string, upstream: number): number {
  if (category === 'trigger') return 0
  if (category === 'destination') return upstream
  if (category === 'transform') return Math.floor(upstream * (0.3 + Math.random() * 0.7))
  if (category === 'source') {
    const pool = MOCK_ROW_COUNTS.source
    return pool[Math.floor(Math.random() * pool.length)]
  }
  return upstream
}

const TERMINAL_EXECUTION_STATUSES = new Set(['success', 'failed', 'cancelled'])
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const EXECUTION_POLL_INTERVAL_MS = 1500
const LMDB_LAST_ENV_PATH_KEY = 'framework_lmdb_env_path'
const ROCKSDB_LAST_ENV_PATH_KEY = 'framework_rocksdb_env_path'
const LMDB_AUTO_METADATA_TTL_MS = 5 * 60 * 1000

type ExecutionLogEntry = {
  nodeId: string
  nodeLabel: string
  timestamp: string
  status: string
  message: string
  rows: number
  output_path?: string
}

function mapNodeLogStatusForExecution(
  rawNodeStatus: unknown,
  executionStatus: unknown,
): 'running' | 'success' | 'error' {
  const raw = String(rawNodeStatus || '').trim().toLowerCase()
  const exec = String(executionStatus || '').trim().toLowerCase()

  if (raw === 'success') return 'success'
  if (raw === 'error' || raw === 'failed') return 'error'
  if (raw === 'running') {
    if (!TERMINAL_EXECUTION_STATUSES.has(exec)) return 'running'
    return exec === 'success' ? 'success' : 'error'
  }
  return exec === 'success' ? 'success' : 'error'
}

function terminalFallbackNodeStatus(executionStatus: unknown): 'success' | 'error' {
  return String(executionStatus || '').trim().toLowerCase() === 'success' ? 'success' : 'error'
}

function deriveEffectiveExecutionStatus(
  rawExecutionStatus: unknown,
  logs: Array<Record<string, any>> | null | undefined,
): string {
  const statusNorm = String(rawExecutionStatus || '').trim().toLowerCase()
  if (TERMINAL_EXECUTION_STATUSES.has(statusNorm)) return statusNorm
  if (statusNorm !== 'running' && statusNorm !== 'cancelling') return statusNorm
  if (!Array.isArray(logs) || logs.length === 0) return statusNorm

  const latestByNode = selectLatestNodeLogByTerminalPriority(logs)
  const latestStatusByNode = new Map<string, string>()
  latestByNode.forEach((entry, nodeIdRaw) => {
    const nodeId = String(nodeIdRaw || '').trim().toLowerCase()
    // Ignore system/meta log lines; they should not keep execution in "running".
    if (!nodeId || nodeId === '__system__' || nodeId === 'system') return
    const entryStatus = String(entry?.status || '').trim().toLowerCase()
    latestStatusByNode.set(nodeId, entryStatus)
  })
  if (latestStatusByNode.size === 0) return statusNorm

  let hasRunning = false
  let hasError = false
  let hasSuccess = false
  latestStatusByNode.forEach((entryStatus) => {
    if (entryStatus === 'running') hasRunning = true
    else if (entryStatus === 'error' || entryStatus === 'failed') hasError = true
    else if (entryStatus === 'success') hasSuccess = true
  })

  if (hasRunning) return statusNorm
  if (hasError) return statusNorm === 'cancelling' ? 'cancelled' : 'failed'
  if (hasSuccess) return statusNorm === 'cancelling' ? 'cancelled' : 'success'
  return statusNorm
}

function normalizeExecutionLogsForDisplay(
  logs: Array<Record<string, any>>,
  executionStatus: unknown,
): ExecutionLogEntry[] {
  if (!Array.isArray(logs) || logs.length === 0) return []
  const statusNorm = String(executionStatus || '').trim().toLowerCase()
  return logs.map((log): ExecutionLogEntry => {
    if (!log || typeof log !== 'object') {
      return {
        nodeId: '',
        nodeLabel: 'Node',
        timestamp: new Date().toISOString(),
        status: mapNodeLogStatusForExecution('error', statusNorm),
        message: '',
        rows: 0,
      }
    }
    const mappedStatus = mapNodeLogStatusForExecution(log.status, statusNorm)
    const rows = typeof log.rows === 'number' ? log.rows : Number(log.rows || 0) || 0
    const next: ExecutionLogEntry = {
      nodeId: String(log.nodeId || ''),
      nodeLabel: String(log.nodeLabel || log.nodeId || 'Node'),
      timestamp: String(log.timestamp || new Date().toISOString()),
      status: mappedStatus,
      message: String(log.message || ''),
      rows,
      output_path: typeof log.output_path === 'string' ? log.output_path : undefined,
    }
    const rawStatus = String(log.status || '').trim().toLowerCase()
    const message = String(log.message || '').trim()
    const nodeLabel = String(log.nodeLabel || log.nodeId || 'Node').trim() || 'Node'
    if (rawStatus === 'running' && mappedStatus === 'success' && (!message || message.startsWith('⟳ Running '))) {
      next.message = rows > 0 ? `✓ ${nodeLabel} — ${rows.toLocaleString()} rows` : `✓ ${nodeLabel}`
    } else if (
      rawStatus === 'running'
      && mappedStatus === 'error'
      && (!message || message.startsWith('⟳ Running '))
      && statusNorm === 'cancelled'
    ) {
      next.message = `✗ ${nodeLabel} cancelled`
    } else if (
      rawStatus === 'running'
      && mappedStatus === 'error'
      && (!message || message.startsWith('⟳ Running '))
    ) {
      next.message = `✗ ${nodeLabel} failed`
    }
    return next
  })
}

function isTerminalLikeRunningLog(log: Record<string, any> | null | undefined): boolean {
  if (!log || typeof log !== 'object') return false
  const rawStatus = String(log.status || '').trim().toLowerCase()
  if (rawStatus !== 'running') return false
  const message = String(log.message || '').trim().toLowerCase()
  if (!message) return false
  if (message.startsWith('✓')) return true
  const hasTerminalWord = /\b(completed|complete|done|finished|success)\b/.test(message)
  const hasActiveWord = /\b(running|processing|queued|queue|pending|starting|in progress)\b/.test(message)
  return hasTerminalWord && !hasActiveWord
}

function selectLatestNodeLogByTerminalPriority(
  logs: Array<Record<string, any>>,
): Map<string, Record<string, any>> {
  const latestByNode = new Map<string, Record<string, any>>()
  const latestTerminalByNode = new Map<string, Record<string, any>>()
  logs.forEach((log) => {
    if (!log || typeof log !== 'object') return
    const nodeId = String(log.nodeId || '').trim()
    if (!nodeId) return
    latestByNode.set(nodeId, log)
    const rawStatus = String(log.status || '').trim().toLowerCase()
    if (rawStatus === 'success' || rawStatus === 'error' || rawStatus === 'failed' || isTerminalLikeRunningLog(log)) {
      latestTerminalByNode.set(nodeId, log)
    }
  })

  const merged = new Map<string, Record<string, any>>()
  latestByNode.forEach((latestLog, nodeId) => {
    const preferred = latestTerminalByNode.get(nodeId) || latestLog
    if (isTerminalLikeRunningLog(preferred)) {
      merged.set(nodeId, { ...preferred, status: 'success' })
      return
    }
    merged.set(nodeId, preferred)
  })
  return merged
}

function collectMaxNodeCounters(
  logs: Array<Record<string, any>>,
): Map<string, { processed?: number; validated?: number }> {
  const byNode = new Map<string, { processed?: number; validated?: number }>()
  logs.forEach((log) => {
    if (!log || typeof log !== 'object') return
    const nodeId = String(log.nodeId || '').trim()
    const nodeKey = nodeId.toLowerCase()
    if (!nodeId || nodeKey === '__system__' || nodeKey === 'system') return
    const next = { ...(byNode.get(nodeId) || {}) }
    const processed = Number(log.processed_rows)
    if (Number.isFinite(processed) && processed >= 0) {
      next.processed = Math.max(Number(next.processed ?? 0), Math.floor(processed))
    }
    const validated = Number(log.validated_rows)
    if (Number.isFinite(validated) && validated >= 0) {
      next.validated = Math.max(Number(next.validated ?? 0), Math.floor(validated))
    }
    byNode.set(nodeId, next)
  })
  return byNode
}

type NodeRuntimeSnapshot = {
  status: 'running' | 'success' | 'error'
  rows?: number
  processedRows?: number
  validatedRows?: number
  startedAt?: string
  finishedAt?: string
  durationMs?: number
}

function toLogTimestampMs(value: unknown): number | undefined {
  return parseTimestampMs(value)
}

function buildNodeRuntimeSnapshotsFromLogs(
  logs: Array<Record<string, any>>,
  executionStatus: unknown,
  nowMs: number = Date.now(),
): Map<string, NodeRuntimeSnapshot> {
  const snapshots = new Map<string, NodeRuntimeSnapshot>()
  if (!Array.isArray(logs) || logs.length === 0) return snapshots

  const statusNorm = String(executionStatus || '').trim().toLowerCase()
  const latestByNode = selectLatestNodeLogByTerminalPriority(logs)
  const maxCountersByNode = collectMaxNodeCounters(logs)

  const firstEventMsByNode = new Map<string, number>()
  const firstRunningMsByNode = new Map<string, number>()
  const lastEventMsByNode = new Map<string, number>()
  const lastTerminalMsByNode = new Map<string, number>()
  const explicitDurationMsByNode = new Map<string, number>()

  logs.forEach((log) => {
    if (!log || typeof log !== 'object') return
    const nodeId = String(log.nodeId || '').trim()
    const nodeKey = nodeId.toLowerCase()
    if (!nodeId || nodeKey === '__system__' || nodeKey === 'system') return

    const eventTsMs = toLogTimestampMs(log.timestamp)
    const startedTsMs = toLogTimestampMs((log as any).started_at) ?? eventTsMs
    const finishedTsMs = toLogTimestampMs((log as any).finished_at)
    const explicitDurationMs = Number((log as any).duration_ms ?? (log as any).durationMs)

    if (typeof startedTsMs === 'number') {
      if (!firstEventMsByNode.has(nodeId) || startedTsMs < Number(firstEventMsByNode.get(nodeId))) {
        firstEventMsByNode.set(nodeId, startedTsMs)
      }
    }
    if (typeof eventTsMs === 'number') {
      if (!lastEventMsByNode.has(nodeId) || eventTsMs > Number(lastEventMsByNode.get(nodeId))) {
        lastEventMsByNode.set(nodeId, eventTsMs)
      }
    }

    const rawStatus = String(log.status || '').trim().toLowerCase()
    if (rawStatus === 'running' && typeof startedTsMs === 'number') {
      if (!firstRunningMsByNode.has(nodeId) || startedTsMs < Number(firstRunningMsByNode.get(nodeId))) {
        firstRunningMsByNode.set(nodeId, startedTsMs)
      }
    }
    if (
      (rawStatus === 'success' || rawStatus === 'error' || rawStatus === 'failed' || isTerminalLikeRunningLog(log))
      && (typeof finishedTsMs === 'number' || typeof eventTsMs === 'number')
    ) {
      const terminalTsMs = (
        typeof finishedTsMs === 'number'
          ? finishedTsMs
          : Number(eventTsMs)
      )
      if (!lastTerminalMsByNode.has(nodeId) || terminalTsMs > Number(lastTerminalMsByNode.get(nodeId))) {
        lastTerminalMsByNode.set(nodeId, terminalTsMs)
      }
    }
    if (Number.isFinite(explicitDurationMs) && explicitDurationMs >= 0) {
      const prev = Number(explicitDurationMsByNode.get(nodeId) ?? 0)
      if (explicitDurationMs >= prev) {
        explicitDurationMsByNode.set(nodeId, explicitDurationMs)
      }
    }
  })

  latestByNode.forEach((log, nodeId) => {
    const mappedStatus = mapNodeLogStatusForExecution(log?.status, statusNorm)
    const rows = typeof log?.rows === 'number' ? log.rows : undefined
    const counters = maxCountersByNode.get(nodeId)
    const processedRows = (
      typeof log?.processed_rows === 'number'
        ? Math.max(log.processed_rows, Number(counters?.processed ?? 0))
        : counters?.processed
    )
    const validatedRows = (
      typeof log?.validated_rows === 'number'
        ? Math.max(log.validated_rows, Number(counters?.validated ?? 0))
        : counters?.validated
    )

    const logStartedMs = toLogTimestampMs((log as any)?.started_at)
    const logFinishedMs = toLogTimestampMs((log as any)?.finished_at)
    const startedMs = logStartedMs ?? firstRunningMsByNode.get(nodeId) ?? firstEventMsByNode.get(nodeId)
    const finishedMs = mappedStatus === 'running'
      ? undefined
      : (logFinishedMs ?? lastTerminalMsByNode.get(nodeId) ?? lastEventMsByNode.get(nodeId))

    let durationMs: number | undefined
    const explicitDurationMs = explicitDurationMsByNode.get(nodeId)
    if (typeof explicitDurationMs === 'number' && Number.isFinite(explicitDurationMs) && explicitDurationMs >= 0) {
      durationMs = explicitDurationMs
    } else if (typeof startedMs === 'number' && typeof finishedMs === 'number' && finishedMs >= startedMs) {
      durationMs = Math.max(0, finishedMs - startedMs)
    } else if (mappedStatus === 'running' && typeof startedMs === 'number') {
      durationMs = Math.max(0, nowMs - startedMs)
    }

    snapshots.set(nodeId, {
      status: mappedStatus,
      rows,
      processedRows,
      validatedRows,
      startedAt: typeof startedMs === 'number' ? new Date(startedMs).toISOString() : undefined,
      finishedAt: typeof finishedMs === 'number' ? new Date(finishedMs).toISOString() : undefined,
      durationMs,
    })
  })

  return snapshots
}

function buildLmdbMetadataFingerprint(config: Record<string, unknown>): string {
  const normalized: Record<string, unknown> = {
    env_path: String(config.env_path || config.file_path || '').trim(),
    db_name: String(config.db_name || '').trim(),
    value_format: String(config.value_format || 'auto').trim(),
    flatten_json_values: Boolean(config.flatten_json_values ?? true),
    expand_profile_documents: Boolean(config.expand_profile_documents ?? true),
    include_value_kind: Boolean(config.include_value_kind ?? true),
    key_prefix: String(config.key_prefix || '').trim(),
    key_contains: String(config.key_contains || '').trim(),
    start_key: String(config.start_key || '').trim(),
    end_key: String(config.end_key || '').trim(),
    global_filter_column: String(config.global_filter_column || '').trim(),
    global_filter_values: Array.isArray(config.global_filter_values)
      ? config.global_filter_values.map((item) => String(item || '').trim()).filter(Boolean).sort()
      : [],
  }
  try {
    return JSON.stringify(normalized)
  } catch {
    return String(normalized.env_path || '')
  }
}

function cloneDeep<T>(value: T): T {
  try {
    if (typeof structuredClone === 'function') {
      return structuredClone(value)
    }
  } catch {
    // fallback below
  }
  return JSON.parse(JSON.stringify(value)) as T
}

function isConfigValueEqual(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true
  const aIsObj = a !== null && typeof a === 'object'
  const bIsObj = b !== null && typeof b === 'object'
  if (!aIsObj || !bIsObj) return false
  try {
    return JSON.stringify(a) === JSON.stringify(b)
  } catch {
    return false
  }
}

function getPersistedLmdbEnvPath(): string {
  if (typeof window === 'undefined') return ''
  try {
    return String(window.localStorage.getItem(LMDB_LAST_ENV_PATH_KEY) || '').trim()
  } catch {
    return ''
  }
}

function persistLmdbEnvPath(value: unknown): void {
  if (typeof window === 'undefined') return
  const path = String(value || '').trim()
  try {
    if (path) {
      window.localStorage.setItem(LMDB_LAST_ENV_PATH_KEY, path)
    } else {
      window.localStorage.removeItem(LMDB_LAST_ENV_PATH_KEY)
    }
  } catch {
    // ignore localStorage errors
  }
}

function getPersistedRocksdbEnvPath(): string {
  if (typeof window === 'undefined') return ''
  try {
    return String(window.localStorage.getItem(ROCKSDB_LAST_ENV_PATH_KEY) || '').trim()
  } catch {
    return ''
  }
}

function persistRocksdbEnvPath(value: unknown): void {
  if (typeof window === 'undefined') return
  const path = String(value || '').trim()
  try {
    if (path) {
      window.localStorage.setItem(ROCKSDB_LAST_ENV_PATH_KEY, path)
    } else {
      window.localStorage.removeItem(ROCKSDB_LAST_ENV_PATH_KEY)
    }
  } catch {
    // ignore localStorage errors
  }
}

async function hydrateLmdbSourceMetadata(nodes: ETLNode[]): Promise<{ nodes: ETLNode[]; changed: boolean }> {
  const persistedPath = getPersistedLmdbEnvPath()
  const persistedRocksPath = getPersistedRocksdbEnvPath()
  const now = Date.now()
  let changed = false

  const nextNodes = await Promise.all(
    nodes.map(async (node) => {
      const nodeType = String(node.data?.nodeType || '')
      const isLmdbSource = nodeType === 'lmdb_source'
      const isRocksdbSource = nodeType === 'rocksdb_source'
      if (!isLmdbSource && !isRocksdbSource) {
        return node
      }
      const sourceType = isRocksdbSource ? 'rocksdb_source' : 'lmdb_source'

      const config = (
        node.data?.config && typeof node.data.config === 'object'
          ? (node.data.config as Record<string, unknown>)
          : {}
      )

      const explicitEnvPath = String(config.env_path || config.file_path || '').trim()
      const effectiveEnvPath = explicitEnvPath || (isRocksdbSource ? persistedRocksPath : persistedPath)
      if (!effectiveEnvPath) return node

      const nextConfig: Record<string, unknown> = { ...config }
      let nodeChanged = false

      if (String(config.env_path || '').trim() !== effectiveEnvPath) {
        nextConfig.env_path = effectiveEnvPath
        nodeChanged = true
      }
      if (isRocksdbSource) persistRocksdbEnvPath(effectiveEnvPath)
      else persistLmdbEnvPath(effectiveEnvPath)

      const hasDetectedColumns = String(config._detected_columns || '').trim().length > 0
      const hasRowCount = Number.isFinite(Number(config._row_count))
      const currentFingerprint = buildLmdbMetadataFingerprint(nextConfig)
      const previousFingerprint = String(config._lmdb_schema_fingerprint || '').trim()
      const previousHydratedAt = Number(config._lmdb_schema_hydrated_at || 0)
      const isFresh = (
        hasDetectedColumns
        && hasRowCount
        && previousFingerprint === currentFingerprint
        && Number.isFinite(previousHydratedAt)
        && previousHydratedAt > 0
        && (now - previousHydratedAt) < LMDB_AUTO_METADATA_TTL_MS
      )
      if (isFresh) {
        if (!nodeChanged) return node
        changed = true
        return {
          ...node,
          data: {
            ...node.data,
            config: nextConfig,
          },
        }
      }

      try {
        const response = await api.detectSourceFieldOptions(sourceType, nextConfig, 1500, {
          page: 1,
          previewRows: 25,
          includeSchemaScan: false,
          schemaScanLimit: 250,
          previewCompact: true,
          previewMaxCellChars: 1200,
          previewMaxCollectionItems: 32,
          timeoutMs: 4000,
        })

        const columns = Array.isArray(response?.columns)
          ? Array.from(new Set(
            response.columns
              .map((item: unknown) => String(item || '').trim())
              .filter(Boolean),
          ))
          : []
        const detectedColumns = columns.join(', ')
        if (String(config._detected_columns || '') !== detectedColumns) {
          nextConfig._detected_columns = detectedColumns
          nodeChanged = true
        }

        const rowCountRaw = Number((response as any)?.row_count)
        if (Number.isFinite(rowCountRaw) && rowCountRaw >= 0) {
          const rowCount = Math.max(0, Math.floor(rowCountRaw))
          if (Number(config._row_count ?? NaN) !== rowCount) {
            nextConfig._row_count = rowCount
            nodeChanged = true
          }
        }
        if (String(config._lmdb_schema_fingerprint || '') !== currentFingerprint) {
          nextConfig._lmdb_schema_fingerprint = currentFingerprint
          nodeChanged = true
        }
        if (Number(config._lmdb_schema_hydrated_at || 0) !== now) {
          nextConfig._lmdb_schema_hydrated_at = now
          nodeChanged = true
        }
      } catch (err) {
        console.warn(`[LMDB] auto field detection skipped for node ${node.id}:`, err)
      }

      if (!nodeChanged) return node
      changed = true
      return {
        ...node,
        data: {
          ...node.data,
          config: nextConfig,
        },
      }
    }),
  )

  return { nodes: nextNodes, changed }
}

async function simulateLocalExecution(
  get: () => WorkflowState,
  set: (partial: Partial<WorkflowState> | ((s: WorkflowState) => Partial<WorkflowState>)) => void
) {
  const { nodes, edges } = get()
  if (nodes.length === 0) {
    set({ isExecuting: false })
    return
  }

  // Topological sort
  const adj: Record<string, string[]> = {}
  const inDeg: Record<string, number> = {}
  nodes.forEach(n => { adj[n.id] = []; inDeg[n.id] = 0 })
  edges.forEach(e => {
    if (adj[e.source]) adj[e.source].push(e.target)
    if (inDeg[e.target] !== undefined) inDeg[e.target]++
  })
  const queue = nodes.filter(n => inDeg[n.id] === 0).map(n => n.id)
  const order: string[] = []
  while (queue.length) {
    const id = queue.shift()!
    order.push(id)
    for (const nb of adj[id] || []) {
      if (--inDeg[nb] === 0) queue.push(nb)
    }
  }
  // Any nodes not reached
  nodes.forEach(n => { if (!order.includes(n.id)) order.push(n.id) })

  const rowsByNode: Record<string, number> = {}

  for (const nodeId of order) {
    const node = nodes.find(n => n.id === nodeId)
    if (!node) continue
    const cat = node.data?.definition?.category || 'source'
    const label = node.data?.label || 'Node'

    // Mark as running
    set((state: WorkflowState) => ({
      nodes: state.nodes.map(n =>
        n.id === nodeId ? { ...n, data: { ...n.data, status: 'running' as const } } : n
      ),
      executionLogs: [...state.executionLogs, {
        nodeId,
        nodeLabel: label,
        timestamp: new Date().toISOString(),
        status: 'running' as const,
        message: `⟳ Running ${label}…`,
        rows: 0,
      }],
    }))

    // Simulate processing time
    const delay = cat === 'trigger' ? 200 : 400 + Math.random() * 800
    await new Promise(r => setTimeout(r, delay))

    // Calculate upstream rows
    const upstreamRows = edges
      .filter(e => e.target === nodeId)
      .reduce((sum, e) => sum + (rowsByNode[e.source] || 0), 0)

    const rows = randomRows(cat, upstreamRows || (cat === 'source' ? 1000 : 0))
    rowsByNode[nodeId] = rows

    // Mark as success
    set((state: WorkflowState) => ({
      nodes: state.nodes.map(n =>
        n.id === nodeId
          ? { ...n, data: { ...n.data, status: 'success' as const, executionRows: rows } }
          : n
      ),
      executionLogs: state.executionLogs.map(l =>
        l.nodeId === nodeId && l.status === 'running'
          ? { ...l, status: 'success' as const, message: `✓ ${label} — ${rows.toLocaleString()} rows`, rows }
          : l
      ),
    }))
  }

  set({ isExecuting: false })
}
import { addEdge, applyNodeChanges, applyEdgeChanges, reconnectEdge } from 'reactflow'
import type { NodeChange, EdgeChange, Connection, Edge } from 'reactflow'
import { v4 as uuidv4 } from 'uuid'
import type { ETLNode, ETLEdge, Pipeline, Execution, DashboardStats, Credential } from '../types'
import { getNodeDef } from '../constants/nodeTypes'
import {
  DEFAULT_WORKFLOW_CONNECTOR_TYPE,
  applyConnectorTypeToEdges,
  resolveConnectorTypeFromEdges,
  type WorkflowConnectorType,
} from '../constants/workflowConnectors'
import api from '../api/client'

// ─── WORKFLOW STORE (Canvas State) ────────────────────────────────────────────

interface WorkflowState {
  nodes: ETLNode[]
  edges: ETLEdge[]
  connectorType: WorkflowConnectorType
  selectedNodeId: string | null
  pipeline: Pipeline | null
  isDirty: boolean
  isExecuting: boolean
  executionId: string | null
  executionAbortRequested: boolean
  executionLogs: ExecutionLogEntry[]
  showLogs: boolean

  setNodes: (nodes: ETLNode[]) => void
  setEdges: (edges: ETLEdge[]) => void
  setConnectorType: (connectorType: WorkflowConnectorType) => void
  onNodesChange: (changes: NodeChange[]) => void
  onEdgesChange: (changes: EdgeChange[]) => void
  onConnect: (connection: Connection) => void
  onReconnect: (oldEdge: Edge, connection: Connection) => void
  addNode: (type: string, position?: { x: number; y: number }) => void
  duplicateNode: (nodeId: string) => void
  updateNodeConfig: (nodeId: string, config: Record<string, unknown>) => void
  updateNodeConfigSilent: (nodeId: string, config: Record<string, unknown>) => void
  updateNodeLabel: (nodeId: string, label: string) => void
  removeNode: (nodeId: string) => void
  setSelectedNode: (nodeId: string | null) => void
  setNodeStatus: (
    nodeId: string,
    status: string,
    rows?: number,
    processedRows?: number,
    validatedRows?: number
  ) => void

  loadPipeline: (pipelineId: string) => Promise<void>
  savePipeline: () => Promise<void>
  executePipeline: () => Promise<void>
  resumeExecutionForPipeline: (pipelineId: string) => Promise<boolean>
  abortExecution: () => Promise<void>
  setShowLogs: (show: boolean) => void
  resetCanvas: () => void
}

export const useWorkflowStore = create<WorkflowState>((set, get) => ({
  nodes: [],
  edges: [],
  connectorType: DEFAULT_WORKFLOW_CONNECTOR_TYPE,
  selectedNodeId: null,
  pipeline: null,
  isDirty: false,
  isExecuting: false,
  executionId: null,
  executionAbortRequested: false,
  executionLogs: [],
  showLogs: false,

  setNodes: (nodes) => set({ nodes }),
  setEdges: (edges) => set((state) => ({
    edges: applyConnectorTypeToEdges(edges, state.connectorType),
  })),
  setConnectorType: (connectorType) => set((state) => ({
    connectorType,
    edges: applyConnectorTypeToEdges(state.edges, connectorType),
    isDirty: true,
  })),

  onNodesChange: (changes) =>
    set((state) => ({ nodes: applyNodeChanges(changes, state.nodes) as ETLNode[], isDirty: true })),

  onEdgesChange: (changes) =>
    set((state) => ({ edges: applyEdgeChanges(changes, state.edges) as ETLEdge[], isDirty: true })),

  onConnect: (connection) =>
    set((state) => ({
      edges: addEdge(
        {
          ...connection,
          animated: true,
          type: state.connectorType,
          reconnectable: true,
          updatable: true,
          interactionWidth: 44,
          style: { stroke: '#6366f1', strokeWidth: 2 },
        },
        state.edges,
      ) as ETLEdge[],
      isDirty: true,
    })),

  onReconnect: (oldEdge, connection) =>
    set((state) => {
      const nextSource = connection?.source ?? oldEdge?.source
      const nextTarget = connection?.target ?? oldEdge?.target
      if (!nextSource || !nextTarget) {
        return {}
      }
      const safeConnection: Connection = {
        ...connection,
        source: nextSource,
        target: nextTarget,
        sourceHandle: connection?.sourceHandle ?? oldEdge?.sourceHandle ?? null,
        targetHandle: connection?.targetHandle ?? oldEdge?.targetHandle ?? null,
      }
      return {
        edges: reconnectEdge(oldEdge, safeConnection, state.edges) as ETLEdge[],
        isDirty: true,
      }
    }),

  addNode: (type, position) => {
    const def = getNodeDef(type)
    if (!def) return
    const id = uuidv4()
    const defaultConfig: Record<string, unknown> = {}
    def.configFields.forEach((f) => {
      if (f.defaultValue !== undefined) defaultConfig[f.name] = f.defaultValue
    })
    if (type === 'lmdb_source') {
      const persistedLmdbPath = getPersistedLmdbEnvPath()
      if (persistedLmdbPath && !String(defaultConfig.env_path || '').trim()) {
        defaultConfig.env_path = persistedLmdbPath
      }
    } else if (type === 'rocksdb_source') {
      const persistedRocksPath = getPersistedRocksdbEnvPath()
      if (persistedRocksPath && !String(defaultConfig.env_path || '').trim()) {
        defaultConfig.env_path = persistedRocksPath
      }
    }
    const newNode: ETLNode = {
      id,
      type: 'etlNode',
      position: position || {
        x: 200 + Math.random() * 200,
        y: 100 + Math.random() * 200,
      },
      data: {
        nodeType: type,
        label: def.label,
        definition: def,
        config: defaultConfig,
        status: 'idle',
      },
    }
    set((state) => ({ nodes: [...state.nodes, newNode], selectedNodeId: id, isDirty: true }))
  },

  duplicateNode: (nodeId) =>
    set((state) => {
      const source = state.nodes.find((node) => node.id === nodeId)
      if (!source) return {}

      const nextId = uuidv4()
      const nextDef = getNodeDef(source.data?.nodeType) || source.data?.definition
      const sourceLabel = String(source.data?.label || 'Node').trim() || 'Node'
      const labelBase = `${sourceLabel} Copy`
      let nextLabel = labelBase
      let suffix = 2
      const usedLabels = new Set(state.nodes.map((node) => String(node.data?.label || '').trim()))
      while (usedLabels.has(nextLabel)) {
        nextLabel = `${labelBase} ${suffix}`
        suffix += 1
      }

      let nextX = Number(source.position?.x || 0) + 56
      let nextY = Number(source.position?.y || 0) + 56
      const occupied = new Set(
        state.nodes.map((node) => `${Math.round(Number(node.position?.x || 0))}:${Math.round(Number(node.position?.y || 0))}`),
      )
      let safety = 0
      while (occupied.has(`${Math.round(nextX)}:${Math.round(nextY)}`) && safety < 16) {
        nextX += 28
        nextY += 28
        safety += 1
      }

      const duplicatedNode: ETLNode = {
        ...source,
        id: nextId,
        position: { x: nextX, y: nextY },
        data: {
          ...cloneDeep(source.data),
          label: nextLabel,
          definition: nextDef || source.data?.definition,
          status: 'idle',
          executionRows: undefined,
          executionProcessedRows: undefined,
          executionValidatedRows: undefined,
          executionSampleInput: undefined,
          executionSampleOutput: undefined,
          executionError: undefined,
          config: cloneDeep((source.data?.config || {}) as Record<string, unknown>),
        },
        selected: false,
        dragging: false,
      }

      return {
        nodes: [...state.nodes, duplicatedNode],
        selectedNodeId: nextId,
        isDirty: true,
      }
    }),

  updateNodeConfig: (nodeId, config) =>
    set((state) => {
      if (!config || Object.keys(config).length === 0) return {}
      const targetNode = state.nodes.find((n) => n.id === nodeId)
      if (!targetNode) return {}
      const currentConfig = (
        targetNode.data?.config && typeof targetNode.data.config === 'object'
          ? (targetNode.data.config as Record<string, unknown>)
          : {}
      )
      const hasPatchDelta = Object.entries(config).some(([key, value]) => !isConfigValueEqual(currentConfig[key], value))
      const nodeType = String(targetNode?.data?.nodeType || '')
      const isLmdbNode = nodeType === 'lmdb_source'
      const isRocksNode = nodeType === 'rocksdb_source'
      if (isLmdbNode || isRocksNode) {
        const mergedConfig = {
          ...currentConfig,
          ...config,
        } as Record<string, unknown>
        const pathToPersist = String(mergedConfig.env_path || mergedConfig.file_path || '').trim()
        if (isRocksNode) persistRocksdbEnvPath(pathToPersist)
        else persistLmdbEnvPath(pathToPersist)
      }
      if (!hasPatchDelta) return {}
      return {
        nodes: state.nodes.map((n) =>
          n.id === nodeId ? { ...n, data: { ...n.data, config: { ...currentConfig, ...config } } } : n
        ),
        isDirty: true,
      }
    }),

  updateNodeConfigSilent: (nodeId, config) =>
    set((state) => {
      if (!config || Object.keys(config).length === 0) return {}
      const targetNode = state.nodes.find((n) => n.id === nodeId)
      if (!targetNode) return {}
      const currentConfig = (
        targetNode.data?.config && typeof targetNode.data.config === 'object'
          ? (targetNode.data.config as Record<string, unknown>)
          : {}
      )
      const hasPatchDelta = Object.entries(config).some(([key, value]) => !isConfigValueEqual(currentConfig[key], value))
      const nodeType = String(targetNode?.data?.nodeType || '')
      const isLmdbNode = nodeType === 'lmdb_source'
      const isRocksNode = nodeType === 'rocksdb_source'
      if (isLmdbNode || isRocksNode) {
        const mergedConfig = {
          ...currentConfig,
          ...config,
        } as Record<string, unknown>
        const pathToPersist = String(mergedConfig.env_path || mergedConfig.file_path || '').trim()
        if (isRocksNode) persistRocksdbEnvPath(pathToPersist)
        else persistLmdbEnvPath(pathToPersist)
      }
      if (!hasPatchDelta) return {}
      return {
        nodes: state.nodes.map((n) =>
          n.id === nodeId ? { ...n, data: { ...n.data, config: { ...currentConfig, ...config } } } : n
        ),
      }
    }),

  updateNodeLabel: (nodeId, label) =>
    set((state) => ({
      nodes: state.nodes.map((n) =>
        n.id === nodeId ? { ...n, data: { ...n.data, label } } : n
      ),
      isDirty: true,
    })),

  removeNode: (nodeId) =>
    set((state) => ({
      nodes: state.nodes.filter((n) => n.id !== nodeId),
      edges: state.edges.filter((e) => e.source !== nodeId && e.target !== nodeId),
      selectedNodeId: state.selectedNodeId === nodeId ? null : state.selectedNodeId,
      isDirty: true,
    })),

  setSelectedNode: (nodeId) => set({ selectedNodeId: nodeId }),

  setNodeStatus: (nodeId, status, rows, processedRows, validatedRows) =>
    set((state) => {
      const statusNorm = String(status || '').toLowerCase()
      const nowMs = Date.now()
      const nowIso = new Date(nowMs).toISOString()
      return {
        nodes: state.nodes.map((n) => {
          if (n.id !== nodeId) return n

          const previousStatusNorm = String(n.data.status || '').toLowerCase()
          const previousStart = String(n.data.executionStartedAt || '').trim()
          const previousStartMs = previousStart ? parseTimestampMsOrNaN(previousStart) : Number.NaN
          const hasValidStart = Number.isFinite(previousStartMs)
          const hasFinalizedDuration =
            typeof n.data.executionDurationMs === 'number'
            && Number.isFinite(n.data.executionDurationMs)
            && n.data.executionDurationMs >= 0
          const hasFinishedAt = String(n.data.executionFinishedAt || '').trim().length > 0

          let nextStartAt = previousStart || undefined
          let nextFinishedAt = n.data.executionFinishedAt
          let nextDurationMs = n.data.executionDurationMs

          if (statusNorm === 'idle') {
            nextStartAt = undefined
            nextFinishedAt = undefined
            nextDurationMs = undefined
          } else if (statusNorm === 'running') {
            // Ignore stale/late running transitions after node has already finalized.
            if (
              previousStatusNorm === 'success'
              || previousStatusNorm === 'error'
              || hasFinishedAt
            ) {
              return n
            }
            nextStartAt = previousStart || nowIso
            nextFinishedAt = undefined
            nextDurationMs = hasValidStart ? Math.max(0, nowMs - Number(previousStartMs)) : 0
          } else if (statusNorm === 'success' || statusNorm === 'error') {
            // Only compute terminal duration from "now - start" when closing an active running node.
            // This prevents duration inflation on page re-open/replay.
            if (previousStatusNorm === 'running' && hasValidStart && !hasFinalizedDuration && !hasFinishedAt) {
              nextFinishedAt = nowIso
              nextDurationMs = Math.max(0, nowMs - Number(previousStartMs))
            } else {
              nextFinishedAt = n.data.executionFinishedAt || nowIso
              nextDurationMs = hasFinalizedDuration ? n.data.executionDurationMs : 0
            }
          }

          return {
            ...n,
            data: {
              ...n.data,
              status: status as ETLNode['data']['status'],
              executionRows: rows ?? n.data.executionRows,
              executionProcessedRows: statusNorm === 'idle'
                ? undefined
                : (processedRows ?? n.data.executionProcessedRows),
              executionValidatedRows: statusNorm === 'idle'
                ? undefined
                : (validatedRows ?? n.data.executionValidatedRows),
              executionStartedAt: nextStartAt,
              executionFinishedAt: nextFinishedAt,
              executionDurationMs: nextDurationMs,
            },
          }
        }),
      }
    }),

  loadPipeline: async (pipelineId) => {
    try {
      const pipeline = await api.getPipeline(pipelineId)
      const loadedEdges = (pipeline.edges || []) as ETLEdge[]
      const connectorType = resolveConnectorTypeFromEdges(loadedEdges)
      const normalizedNodes = (pipeline.nodes || []).map((n: ETLNode) => {
        const def = getNodeDef(n.data?.nodeType)
        return { ...n, type: 'etlNode', data: { ...n.data, definition: def || n.data.definition } }
      })
      const lmdbPathFromPipeline = normalizedNodes
        .map((node: ETLNode) => {
          const nodeType = String(node.data?.nodeType || '')
          if (!['lmdb_source', 'rocksdb_source'].includes(nodeType)) return ''
          const cfg = (
            node.data?.config && typeof node.data.config === 'object'
              ? (node.data.config as Record<string, unknown>)
            : {}
          )
          return String(cfg.env_path || cfg.file_path || '').trim()
        })
        .find((path: string) => Boolean(path))
      if (lmdbPathFromPipeline) {
        const lmdbNode = normalizedNodes.find((node: ETLNode) => String(node.data?.nodeType || '') === 'lmdb_source')
        const rocksNode = normalizedNodes.find((node: ETLNode) => String(node.data?.nodeType || '') === 'rocksdb_source')
        if (rocksNode && !lmdbNode) persistRocksdbEnvPath(lmdbPathFromPipeline)
        else persistLmdbEnvPath(lmdbPathFromPipeline)
      }
      set({
        pipeline,
        nodes: normalizedNodes,
        edges: applyConnectorTypeToEdges(loadedEdges, connectorType),
        connectorType,
        isDirty: false,
      })
    } catch (e) {
      console.error('Failed to load pipeline:', e)
    }
  },

  savePipeline: async () => {
    const { pipeline, nodes, edges } = get()
    if (!pipeline?.id) return
    try {
      await api.updatePipeline(pipeline.id, { nodes, edges })
      set({ isDirty: false })
    } catch (e) {
      console.error('Failed to save pipeline:', e)
    }
  },

  executePipeline: async () => {
    const { pipeline } = get()
    if (!pipeline?.id) return

    // Reset all node statuses
    set((state) => ({
      nodes: state.nodes.map((n) => ({
        ...n,
        data: {
          ...n.data,
          status: 'idle',
          executionRows: undefined,
          executionProcessedRows: undefined,
          executionValidatedRows: undefined,
          executionStartedAt: undefined,
          executionFinishedAt: undefined,
          executionDurationMs: undefined,
        },
      })),
      isExecuting: true,
      executionAbortRequested: false,
      executionLogs: [],
      showLogs: true,
    }))

    try {
      let executionNodes = get().nodes
      const executionEdges = get().edges
      let shouldPersistBeforeRun = Boolean(get().isDirty)

      // Always refresh LMDB schema hints before run so field listings stay up-to-date
      // without requiring explicit "Detect Fields" clicks.
      const hasKvSourceNodes = executionNodes.some((node) => {
        const nodeType = String(node.data?.nodeType || '')
        return nodeType === 'lmdb_source' || nodeType === 'rocksdb_source'
      })
      if (hasKvSourceNodes) {
        const lmdbHydrationBudgetMs = 800
        const lmdbHydrated = await Promise.race<
          { nodes: ETLNode[]; changed: boolean } | null
        >([
          hydrateLmdbSourceMetadata(executionNodes),
          sleep(lmdbHydrationBudgetMs).then(() => null),
        ])
        if (lmdbHydrated) {
          executionNodes = lmdbHydrated.nodes
          if (lmdbHydrated.changed) {
            set({ nodes: executionNodes })
            shouldPersistBeforeRun = true
          }
        }
      }

      // ── If this pipeline only exists locally (offline-created), register it
      //    in the backend now so execution can proceed ─────────────────────────
      let activePipelineId = pipeline.id
      const isLocalOnly = pipeline.id.startsWith('p_')
      if (isLocalOnly) {
        try {
          const created = await api.createPipeline({
            name: pipeline.name || 'Untitled Pipeline',
            description: (pipeline as any).description || '',
          })
          // Push current canvas to the new backend record
          try {
            await api.updatePipeline(created.id, { nodes: executionNodes, edges: executionEdges })
            set({ isDirty: false })
          } catch (saveErr) {
            console.error('Failed to persist pipeline before execution:', saveErr)
          }
          activePipelineId = created.id
          // Upgrade the store so future saves go to the real backend record
          set({ pipeline: { ...pipeline, id: created.id } })
        } catch {
          // backend truly unreachable — will be caught below as offline
        }
      } else {
        if (shouldPersistBeforeRun) {
          try {
            await api.updatePipeline(activePipelineId, { nodes: executionNodes, edges: executionEdges })
            set({ isDirty: false })
          } catch (saveErr) {
            console.error('Failed to persist pipeline before execution:', saveErr)
          }
        }
      }

      const result = await api.executePipeline(activePipelineId)
      const execId = result.execution_id

      // ── OFFLINE: backend not running ─────────────────────────────────────
      if (result.offline) {
        set({
          isExecuting: false,
          executionAbortRequested: false,
          showLogs: true,
          executionLogs: [{
            nodeId: 'system',
            nodeLabel: 'System',
            timestamp: new Date().toISOString(),
            status: 'error',
            message: '✗ Backend not running. Start the Python backend to execute real ETL operations: cd backend && python main.py',
            rows: 0,
          }]
        })
        return
      }

      // ── ONLINE: real backend ───────────────────────────────────────────────
      set({ executionId: execId, executionAbortRequested: false })

      let ws: WebSocket | null = null
      let wsConnected = false
      let wsMessageSeen = false
      const forceStopRunningNodes = (fallbackStatus: 'success' | 'error' = 'error') => {
        const nowMs = Date.now()
        const nowIso = new Date(nowMs).toISOString()
        set((state) => ({
          nodes: state.nodes.map((n) => (
            n.data?.status === 'running'
              ? {
                ...n,
                data: {
                  ...n.data,
                  status: fallbackStatus,
                  executionFinishedAt: nowIso,
                  executionDurationMs: (() => {
                    const startedAt = String(n.data?.executionStartedAt || '').trim()
                    if (!startedAt) return n.data?.executionDurationMs
                    const startedMs = parseTimestampMsOrNaN(startedAt)
                    if (!Number.isFinite(startedMs)) return n.data?.executionDurationMs
                    return Math.max(0, nowMs - Number(startedMs))
                  })(),
                },
              }
              : n
          )),
        }))
      }
      const applyLiveNodeStatusFromLogs = (
        logs: Array<Record<string, any>>,
        executionStatus?: string,
      ) => {
        if (!Array.isArray(logs) || logs.length === 0) return
        const statusNorm = String(executionStatus || '').trim().toLowerCase()
        const isTerminal = TERMINAL_EXECUTION_STATUSES.has(statusNorm)
        const snapshots = buildNodeRuntimeSnapshotsFromLogs(logs, statusNorm)
        set((state) => ({
          nodes: state.nodes.map((node) => {
            const snapshot = snapshots.get(String(node.id || '').trim())
            if (!snapshot) return node
            return {
              ...node,
              data: {
                ...node.data,
                status: snapshot.status,
                executionRows: snapshot.rows ?? node.data.executionRows,
                executionProcessedRows: snapshot.processedRows ?? node.data.executionProcessedRows,
                executionValidatedRows: snapshot.validatedRows ?? node.data.executionValidatedRows,
                executionStartedAt: snapshot.startedAt ?? node.data.executionStartedAt,
                executionFinishedAt: snapshot.finishedAt ?? node.data.executionFinishedAt,
                executionDurationMs: (
                  typeof snapshot.durationMs === 'number'
                    ? snapshot.durationMs
                    : node.data.executionDurationMs
                ),
              },
            }
          }),
        }))
        if (isTerminal) {
          forceStopRunningNodes(terminalFallbackNodeStatus(statusNorm))
        }
      }
      const finalizePreviousRunningNodes = (startedNodeId: string) => {
        const targetId = String(startedNodeId || '').trim()
        if (!targetId) return
        const nowMs = Date.now()
        const nowIso = new Date(nowMs).toISOString()
        set((state) => {
          const runningNodeIds = new Set(
            state.nodes
              .filter((node) => String(node.id || '').trim() !== targetId && String(node?.data?.status || '').toLowerCase() === 'running')
              .map((node) => String(node.id || '').trim())
              .filter(Boolean),
          )
          if (runningNodeIds.size === 0) return {}
          let changed = false
          const nextNodes = state.nodes.map((node) => {
            if (!runningNodeIds.has(String(node.id || '').trim())) return node
            if (node.data?.status !== 'running') return node
            const startedAt = String(node.data?.executionStartedAt || '').trim()
            const startedMs = startedAt ? parseTimestampMsOrNaN(startedAt) : Number.NaN
            changed = true
            return {
              ...node,
              data: {
                ...node.data,
                status: 'success' as const,
                executionFinishedAt: nowIso,
                executionDurationMs: Number.isFinite(startedMs)
                  ? Math.max(0, nowMs - Number(startedMs))
                  : node.data?.executionDurationMs,
              },
            }
          })
          return changed ? { nodes: nextNodes } : {}
        })
      }
      try {
        ws = new WebSocket(`ws://localhost:8001/ws/executions/${execId}`)
        ws.onopen = () => { wsConnected = true }
        ws.onclose = () => { wsConnected = false }

        ws.onmessage = (event) => {
          wsMessageSeen = true
          const liveState = get()
          if (!liveState.isExecuting || liveState.executionId !== execId) {
            // Ignore late websocket events from an already finished/replaced execution.
            return
          }
          const msg = JSON.parse(event.data)
          const entry = msg.log_entry  // full log entry streamed from backend
          const liveRows = typeof msg.rows === 'number'
            ? msg.rows
            : (entry && typeof entry.rows === 'number' ? entry.rows : undefined)
          const liveProcessedRows = typeof msg.processed_rows === 'number'
            ? msg.processed_rows
            : (entry && typeof entry.processed_rows === 'number' ? entry.processed_rows : undefined)
          const liveValidatedRows = typeof msg.validated_rows === 'number'
            ? msg.validated_rows
            : (entry && typeof entry.validated_rows === 'number' ? entry.validated_rows : undefined)

          if (msg.type === 'node_start') {
            finalizePreviousRunningNodes(String(msg.nodeId || ''))
            get().setNodeStatus(msg.nodeId, 'running', liveRows, liveProcessedRows, liveValidatedRows)
            // Add "running" log entry immediately
            if (entry) {
              set((state) => ({
                executionLogs: [...state.executionLogs, entry]
              }))
            }
          }
          else if (msg.type === 'node_success') {
            get().setNodeStatus(msg.nodeId, 'success', liveRows, liveProcessedRows, liveValidatedRows)
            // Replace the running entry with the success entry
            if (entry) {
              set((state) => {
                const idx = state.executionLogs.findLastIndex(
                  (l) => l.nodeId === msg.nodeId && l.status === 'running'
                )
                if (idx >= 0) {
                  const updated = [...state.executionLogs]
                  updated[idx] = entry
                  return { executionLogs: updated }
                }
                return { executionLogs: [...state.executionLogs, entry] }
              })
            }
          }
          else if (msg.type === 'node_progress') {
            const currentNode = get().nodes.find((n) => n.id === msg.nodeId)
            const currentStatus = String(currentNode?.data?.status || '').toLowerCase()
            const canMoveToRunningFromProgress = currentStatus === 'idle' || currentStatus === 'running'
            const progressLooksTerminal = isTerminalLikeRunningLog(entry as Record<string, any> | undefined)
            const progressStatus: 'running' | 'success' = progressLooksTerminal ? 'success' : 'running'

            if (entry) {
              set((state) => {
                const idx = state.executionLogs.findLastIndex(
                  (l) => l.nodeId === msg.nodeId && l.status === 'running'
                )
                if (idx >= 0) {
                  const updated = [...state.executionLogs]
                  updated[idx] = progressLooksTerminal ? { ...entry, status: 'success' } : entry
                  return { executionLogs: updated }
                }
                // Do not append stale progress after node reached terminal status.
                if (!canMoveToRunningFromProgress) {
                  return {}
                }
                return {
                  executionLogs: [
                    ...state.executionLogs,
                    progressLooksTerminal ? { ...entry, status: 'success' } : entry,
                  ],
                }
              })
            }
            if (canMoveToRunningFromProgress) {
              get().setNodeStatus(msg.nodeId, progressStatus, liveRows, liveProcessedRows, liveValidatedRows)
            }
          }
          else if (msg.type === 'node_error') {
            get().setNodeStatus(msg.nodeId, 'error', liveRows, liveProcessedRows, liveValidatedRows)
            // Replace the running entry with the error entry
            if (entry) {
              set((state) => {
                const idx = state.executionLogs.findLastIndex(
                  (l) => l.nodeId === msg.nodeId && l.status === 'running'
                )
                if (idx >= 0) {
                  const updated = [...state.executionLogs]
                  updated[idx] = entry
                  return { executionLogs: updated }
                }
                return { executionLogs: [...state.executionLogs, entry] }
              })
            }
          }
          else if (msg.type === 'execution_cancelled' && entry) {
            forceStopRunningNodes()
            set((state) => ({
              executionLogs: [...state.executionLogs, entry],
            }))
          }
          else if (msg.type === 'execution_terminal') {
            void (async () => {
              const terminalStatus = String(msg.status || '').trim().toLowerCase()
              try {
                const finalExec = await api.getExecution(execId, {
                  includeLogs: true,
                  logTail: 5000,
                })
                if (Array.isArray(finalExec.logs) && finalExec.logs.length > 0) {
                  set({
                    executionLogs: normalizeExecutionLogsForDisplay(
                      finalExec.logs as Array<Record<string, any>>,
                      String(finalExec.status || msg.status || ''),
                    ),
                  })
                  applyLiveNodeStatusFromLogs(
                    finalExec.logs as Array<Record<string, any>>,
                    String(finalExec.status || ''),
                  )
                }
              } catch {
                const status = terminalStatus
                if (status === 'cancelled' && entry) {
                  forceStopRunningNodes()
                  set((state) => ({
                    executionLogs: [...state.executionLogs, entry],
                  }))
                }
              } finally {
                if (TERMINAL_EXECUTION_STATUSES.has(terminalStatus)) {
                  forceStopRunningNodes(terminalFallbackNodeStatus(terminalStatus))
                } else {
                  forceStopRunningNodes('error')
                }
                set({ isExecuting: false, executionId: null, executionAbortRequested: false })
              }
            })()
          }
        }

        ws.onerror = () => { wsConnected = false }
      } catch { /* WS unavailable */ }

      let consecutivePollErrors = 0
      let pollTick = 0
      while (true) {
        const startupOrNoWsSignal = !wsConnected || !wsMessageSeen
        await sleep(startupOrNoWsSignal ? EXECUTION_POLL_INTERVAL_MS : EXECUTION_POLL_INTERVAL_MS)
        const state = get()
        if (!state.isExecuting || state.executionId !== execId) break
        try {
          pollTick += 1
          const includeLogsInPoll = startupOrNoWsSignal || (pollTick % 4 === 0)
          const exec = await api.getExecution(execId, {
            includeLogs: includeLogsInPoll,
            logTail: includeLogsInPoll ? 300 : 80,
          })
          consecutivePollErrors = 0
          const rawExecStatus = String(exec.status || '').toLowerCase()
          const effectiveExecStatus = includeLogsInPoll
            ? deriveEffectiveExecutionStatus(
              rawExecStatus,
              Array.isArray(exec.logs) ? (exec.logs as Array<Record<string, any>>) : [],
            )
            : rawExecStatus
          if (includeLogsInPoll && Array.isArray(exec.logs) && exec.logs.length > 0) {
            set({
              executionLogs: normalizeExecutionLogsForDisplay(
                exec.logs as Array<Record<string, any>>,
                effectiveExecStatus,
              ),
            })
            applyLiveNodeStatusFromLogs(exec.logs as Array<Record<string, any>>, effectiveExecStatus)
          }
          if (effectiveExecStatus === 'cancelling') {
            forceStopRunningNodes()
          }
          if (TERMINAL_EXECUTION_STATUSES.has(effectiveExecStatus)) {
            let finalExec = exec
            if (!Array.isArray(finalExec.logs) || finalExec.logs.length === 0) {
              finalExec = await api.getExecution(execId, {
                includeLogs: true,
                logTail: 5000,
              })
              const finalStatusNorm = deriveEffectiveExecutionStatus(
                String(finalExec.status || ''),
                Array.isArray(finalExec.logs) ? (finalExec.logs as Array<Record<string, any>>) : [],
              )
              if (Array.isArray(finalExec.logs) && finalExec.logs.length > 0) {
                set({
                  executionLogs: normalizeExecutionLogsForDisplay(
                    finalExec.logs as Array<Record<string, any>>,
                    finalStatusNorm,
                  ),
                })
                applyLiveNodeStatusFromLogs(
                  finalExec.logs as Array<Record<string, any>>,
                  finalStatusNorm,
                )
              }
            }
            const finalStatusNorm = deriveEffectiveExecutionStatus(
              String(finalExec.status || effectiveExecStatus || ''),
              Array.isArray(finalExec.logs) ? (finalExec.logs as Array<Record<string, any>>) : [],
            )
            applyLiveNodeStatusFromLogs(
              (finalExec.logs || []) as Array<Record<string, any>>,
              finalStatusNorm,
            )
            forceStopRunningNodes(
              terminalFallbackNodeStatus(finalStatusNorm),
            )
            if (
              finalStatusNorm === 'cancelled'
              && !finalExec.logs?.some((log: any) => String(log?.message || '').includes('Execution cancelled'))
            ) {
              set((current) => ({
                executionLogs: [
                  ...current.executionLogs,
                  {
                    nodeId: '__system__',
                    nodeLabel: 'System',
                    timestamp: new Date().toISOString(),
                    status: 'error',
                    message: '✗ Execution cancelled by user.',
                    rows: 0,
                  },
                ],
              }))
            }
            set({ isExecuting: false, executionId: null, executionAbortRequested: false })
            ws?.close()
            return
          }
        } catch (err) {
          consecutivePollErrors += 1
          if (consecutivePollErrors >= 5) {
            set((current) => ({
              isExecuting: false,
              executionId: null,
              executionAbortRequested: false,
              executionLogs: [
                ...current.executionLogs,
                {
                  nodeId: '__system__',
                  nodeLabel: 'System',
                  timestamp: new Date().toISOString(),
                  status: 'error',
                  message: `✗ Execution monitor failed: ${String((err as any)?.message || 'Unable to poll backend status')}`,
                  rows: 0,
                },
              ],
            }))
            ws?.close()
            return
          }
        }
      }
      ws?.close()

    } catch (e) {
      const errMsg = String((e as any)?.message || 'Failed to start pipeline execution')
      set((state) => ({
        isExecuting: false,
        executionId: null,
        executionAbortRequested: false,
        showLogs: true,
        executionLogs: [
          ...state.executionLogs,
          {
            nodeId: '__system__',
            nodeLabel: 'System',
            timestamp: new Date().toISOString(),
            status: 'error',
            message: `✗ ${errMsg}`,
            rows: 0,
          },
        ],
      }))
      console.error('Execution failed:', e)
    }
  },

  resumeExecutionForPipeline: async (pipelineId) => {
    const targetPipelineId = String(pipelineId || '').trim()
    if (!targetPipelineId) return false

    const forceStopRunningNodes = (fallbackStatus: 'success' | 'error' = 'error') => {
      const nowMs = Date.now()
      const nowIso = new Date(nowMs).toISOString()
      set((state) => ({
        nodes: state.nodes.map((n) => (
          n.data?.status === 'running'
            ? {
              ...n,
              data: {
                ...n.data,
                status: fallbackStatus,
                executionFinishedAt: nowIso,
                executionDurationMs: (() => {
                  const startedAt = String(n.data?.executionStartedAt || '').trim()
                  if (!startedAt) return n.data?.executionDurationMs
                  const startedMs = parseTimestampMsOrNaN(startedAt)
                  if (!Number.isFinite(startedMs)) return n.data?.executionDurationMs
                  return Math.max(0, nowMs - Number(startedMs))
                })(),
              },
            }
            : n
        )),
      }))
    }

    const applyLiveNodeStatusFromLogs = (
      logs: Array<Record<string, any>>,
      executionStatus?: string,
    ) => {
      if (!Array.isArray(logs) || logs.length === 0) return
      const statusNorm = String(executionStatus || '').trim().toLowerCase()
      const isTerminal = TERMINAL_EXECUTION_STATUSES.has(statusNorm)
      const snapshots = buildNodeRuntimeSnapshotsFromLogs(logs, statusNorm)
      set((state) => ({
        nodes: state.nodes.map((node) => {
          const snapshot = snapshots.get(String(node.id || '').trim())
          if (!snapshot) return node
          return {
            ...node,
            data: {
              ...node.data,
              status: snapshot.status,
              executionRows: snapshot.rows ?? node.data.executionRows,
              executionProcessedRows: snapshot.processedRows ?? node.data.executionProcessedRows,
              executionValidatedRows: snapshot.validatedRows ?? node.data.executionValidatedRows,
              executionStartedAt: snapshot.startedAt ?? node.data.executionStartedAt,
              executionFinishedAt: snapshot.finishedAt ?? node.data.executionFinishedAt,
              executionDurationMs: (
                typeof snapshot.durationMs === 'number'
                  ? snapshot.durationMs
                  : node.data.executionDurationMs
              ),
            },
          }
        }),
      }))
      if (isTerminal) {
        forceStopRunningNodes(terminalFallbackNodeStatus(statusNorm))
      }
    }

    try {
      const executions = await api.listExecutions(targetPipelineId)
      const executionItems = Array.isArray(executions) ? executions : []
      const latestExecution = executionItems[0]
      const latestStatusNorm = String(latestExecution?.status || '').trim().toLowerCase()
      const activeExecution = (
        latestStatusNorm === 'running' || latestStatusNorm === 'cancelling'
      )
        ? latestExecution
        : null
      if (!activeExecution?.id) {
        if (latestExecution?.id) {
          const latestExecutionId = String(latestExecution.id)
          const latestSnapshot = await api.getExecution(latestExecutionId, {
            includeLogs: true,
            logTail: 5000,
          })
          const latestStatus = String(
            latestSnapshot?.status || latestExecution.status || '',
          ).trim().toLowerCase()
          const latestLogs = Array.isArray(latestSnapshot?.logs) ? latestSnapshot.logs : []

          set({
            isExecuting: false,
            executionId: null,
            executionAbortRequested: false,
            showLogs: true,
            executionLogs: normalizeExecutionLogsForDisplay(
              latestLogs as Array<Record<string, any>>,
              latestStatus,
            ),
          })
          applyLiveNodeStatusFromLogs(latestLogs as Array<Record<string, any>>, latestStatus)
          if (!latestLogs.length) {
            forceStopRunningNodes(terminalFallbackNodeStatus(latestStatus))
          }
          return true
        }

        const live = get()
        const livePipelineId = live.pipeline?.id ? String(live.pipeline.id) : ''
        if (
          livePipelineId === targetPipelineId
          && (live.isExecuting || live.executionAbortRequested || Boolean(live.executionId))
        ) {
          forceStopRunningNodes()
          set({ isExecuting: false, executionId: null, executionAbortRequested: false })
        }
        return false
      }

      const executionId = String(activeExecution.id)
      const executionSnapshot = await api.getExecution(executionId, {
        includeLogs: true,
        logTail: 5000,
      })
      const logs = Array.isArray(executionSnapshot?.logs) ? executionSnapshot.logs : []
      const snapshotStatus = deriveEffectiveExecutionStatus(
        String(executionSnapshot?.status || activeExecution.status || '').trim().toLowerCase(),
        logs as Array<Record<string, any>>,
      )

      set({
        isExecuting: snapshotStatus === 'running' || snapshotStatus === 'cancelling',
        executionId: snapshotStatus === 'running' || snapshotStatus === 'cancelling'
          ? executionId
          : null,
        executionAbortRequested: snapshotStatus === 'cancelling',
        showLogs: true,
        executionLogs: normalizeExecutionLogsForDisplay(
          logs as Array<Record<string, any>>,
          snapshotStatus,
        ),
      })
      applyLiveNodeStatusFromLogs(logs as Array<Record<string, any>>, snapshotStatus)

      if (TERMINAL_EXECUTION_STATUSES.has(snapshotStatus)) {
        return true
      }

      void (async () => {
        let consecutivePollErrors = 0
        while (true) {
          await sleep(EXECUTION_POLL_INTERVAL_MS)
          const state = get()
          if (!state.isExecuting || state.executionId !== executionId) break
          try {
            const exec = await api.getExecution(executionId, {
              includeLogs: true,
              logTail: 300,
            })
            consecutivePollErrors = 0
            const logs = Array.isArray(exec.logs) ? (exec.logs as Array<Record<string, any>>) : []
            const execStatus = deriveEffectiveExecutionStatus(String(exec.status || '').toLowerCase(), logs)
            if (Array.isArray(exec.logs) && exec.logs.length > 0) {
              set({
                executionLogs: normalizeExecutionLogsForDisplay(
                  exec.logs as Array<Record<string, any>>,
                  execStatus,
                ),
              })
              applyLiveNodeStatusFromLogs(exec.logs as Array<Record<string, any>>, execStatus)
            }
            if (execStatus === 'cancelling') {
              forceStopRunningNodes()
              set({ executionAbortRequested: true })
            }
            if (TERMINAL_EXECUTION_STATUSES.has(execStatus)) {
              forceStopRunningNodes(terminalFallbackNodeStatus(execStatus))
              set({ isExecuting: false, executionId: null, executionAbortRequested: false })
              return
            }
          } catch (err) {
            consecutivePollErrors += 1
            if (consecutivePollErrors >= 5) {
              set((liveState) => ({
                isExecuting: false,
                executionId: null,
                executionAbortRequested: false,
                executionLogs: [
                  ...liveState.executionLogs,
                  {
                    nodeId: '__system__',
                    nodeLabel: 'System',
                    timestamp: new Date().toISOString(),
                    status: 'error',
                    message: `✗ Execution monitor failed: ${String((err as any)?.message || 'Unable to poll backend status')}`,
                    rows: 0,
                  },
                ],
              }))
              return
            }
          }
        }
      })()

      return true
    } catch (err) {
      console.error('Failed to resume running execution:', err)
      return false
    }
  },

  abortExecution: async () => {
    const { executionId, isExecuting } = get()
    if (!isExecuting || !executionId) return
    set({ executionAbortRequested: true })
    try {
      const response = await api.abortExecution(executionId)
      const responseStatus = String(response?.status || '').trim().toLowerCase()
      if (responseStatus && responseStatus !== 'running' && responseStatus !== 'cancelling') {
        set((state) => ({
          isExecuting: false,
          executionId: null,
          executionAbortRequested: false,
          executionLogs: [
            ...state.executionLogs,
            {
              nodeId: '__system__',
              nodeLabel: 'System',
              timestamp: new Date().toISOString(),
              status: responseStatus === 'cancelled' ? 'error' : 'success',
              message: String(response?.message || `Execution is already ${responseStatus}.`),
              rows: 0,
            },
          ],
        }))
        return
      }
      set((state) => ({
        executionLogs: [
          ...state.executionLogs,
          {
            nodeId: '__system__',
            nodeLabel: 'System',
            timestamp: new Date().toISOString(),
            status: 'running',
            message: '⏹ Abort requested. Waiting for current processing step to finish...',
            rows: 0,
          },
        ],
      }))
    } catch (err) {
      set((state) => ({
        executionAbortRequested: false,
        executionLogs: [
          ...state.executionLogs,
          {
            nodeId: '__system__',
            nodeLabel: 'System',
            timestamp: new Date().toISOString(),
            status: 'error',
            message: `✗ Abort request failed: ${String((err as any)?.message || 'Unknown error')}`,
            rows: 0,
          },
        ],
      }))
    }
  },

  setShowLogs: (show) => set({ showLogs: show }),

  resetCanvas: () => set({
    nodes: [],
    edges: [],
    pipeline: null,
    connectorType: DEFAULT_WORKFLOW_CONNECTOR_TYPE,
    selectedNodeId: null,
    isDirty: false,
    isExecuting: false,
    executionId: null,
    executionAbortRequested: false,
    executionLogs: [],
  }),
}))

// ─── PIPELINE STORE ───────────────────────────────────────────────────────────

interface PipelineState {
  pipelines: Pipeline[]
  loading: boolean
  fetchPipelines: () => Promise<void>
  createPipeline: (name: string, description?: string) => Promise<Pipeline>
  deletePipeline: (id: string) => Promise<void>
  duplicatePipeline: (id: string) => Promise<void>
}

export const usePipelineStore = create<PipelineState>((set, get) => ({
  pipelines: [],
  loading: false,

  fetchPipelines: async () => {
    set({ loading: true })
    try {
      const pipelines = await api.listPipelines()
      set({ pipelines })
    } finally {
      set({ loading: false })
    }
  },

  createPipeline: async (name, description = '') => {
    const pipeline = await api.createPipeline({ name, description })
    await get().fetchPipelines()
    return pipeline
  },

  deletePipeline: async (id) => {
    await api.deletePipeline(id)
    set((state) => ({ pipelines: state.pipelines.filter((p) => p.id !== id) }))
  },

  duplicatePipeline: async (id) => {
    await api.duplicatePipeline(id)
    await get().fetchPipelines()
  },
}))

// ─── EXECUTION STORE ──────────────────────────────────────────────────────────

interface ExecutionState {
  executions: Execution[]
  loading: boolean
  fetchExecutions: (pipelineId?: string) => Promise<void>
  deleteExecution: (id: string) => Promise<void>
}

export const useExecutionStore = create<ExecutionState>((set) => ({
  executions: [],
  loading: false,

  fetchExecutions: async (pipelineId) => {
    set({ loading: true })
    try {
      const listed = await api.listExecutions(pipelineId)
      const baseExecutions = Array.isArray(listed) ? listed : []
      const liveExecutions = baseExecutions
        .filter((execution: any) => {
          const status = String(execution?.status || '').trim().toLowerCase()
          return (status === 'running' || status === 'cancelling') && String(execution?.id || '').trim().length > 0
        })
        .slice(0, 12)

      if (liveExecutions.length === 0) {
        set({ executions: baseExecutions })
        return
      }

      const detailResults = await Promise.allSettled(
        liveExecutions.map((execution: any) => (
          api.getExecution(String(execution.id), { includeLogs: true, logTail: 500 })
        )),
      )

      const detailById = new Map<string, any>()
      detailResults.forEach((result) => {
        if (result.status !== 'fulfilled') return
        const execution = result.value
        const id = String(execution?.id || '').trim()
        if (!id) return
        detailById.set(id, execution)
      })

      const resolvedExecutions = baseExecutions.map((execution: any) => {
        const executionId = String(execution?.id || '').trim()
        if (!executionId) return execution
        const details = detailById.get(executionId)
        if (!details) return execution

        const effectiveStatus = deriveEffectiveExecutionStatus(
          String(details?.status || execution?.status || ''),
          Array.isArray(details?.logs) ? (details.logs as Array<Record<string, any>>) : [],
        )
        const baseStatus = String(execution?.status || '').trim().toLowerCase()
        if (effectiveStatus === baseStatus) return execution

        const finishedAt = details?.finished_at || execution?.finished_at
        let duration = execution?.duration
        if (finishedAt && execution?.started_at) {
          const startedMs = parseTimestampMsOrNaN(String(execution.started_at))
          const finishedMs = parseTimestampMsOrNaN(String(finishedAt))
          if (Number.isFinite(startedMs) && Number.isFinite(finishedMs)) {
            duration = Math.max(0, Math.floor((Number(finishedMs) - Number(startedMs)) / 1000))
          }
        }

        return {
          ...execution,
          status: effectiveStatus,
          finished_at: finishedAt,
          duration,
          rows_processed: Number(details?.rows_processed ?? execution?.rows_processed ?? 0),
          error_message: details?.error_message ?? execution?.error_message,
        }
      })

      set({ executions: resolvedExecutions })
    } finally {
      set({ loading: false })
    }
  },

  deleteExecution: async (id) => {
    await api.deleteExecution(id)
    set((state) => ({ executions: state.executions.filter((e) => e.id !== id) }))
  },
}))

// ─── STATS STORE ──────────────────────────────────────────────────────────────

interface StatsState {
  stats: DashboardStats | null
  fetchStats: () => Promise<void>
}

export const useStatsStore = create<StatsState>((set) => ({
  stats: null,
  fetchStats: async () => {
    try {
      const stats = await api.getStats()
      set({ stats })
    } catch {
      set({
        stats: {
          total_pipelines: 0, active_pipelines: 0, total_executions: 0,
          successful_executions: 0, failed_executions: 0, running_executions: 0,
          total_rows_processed: 0, success_rate: 0
        }
      })
    }
  },
}))

// ─── CREDENTIALS STORE ────────────────────────────────────────────────────────

interface CredentialState {
  credentials: Credential[]
  fetchCredentials: () => Promise<void>
  createCredential: (data: Omit<Credential, 'id' | 'created_at'> & { data: Record<string, unknown> }) => Promise<void>
  deleteCredential: (id: string) => Promise<void>
}

export const useCredentialStore = create<CredentialState>((set) => ({
  credentials: [],
  fetchCredentials: async () => {
    try {
      const credentials = await api.listCredentials()
      set({ credentials })
    } catch {
      set({ credentials: [] })
    }
  },
  createCredential: async (data) => {
    await api.createCredential(data)
    const credentials = await api.listCredentials()
    set({ credentials })
  },
  deleteCredential: async (id) => {
    await api.deleteCredential(id)
    set((state) => ({ credentials: state.credentials.filter((c) => c.id !== id) }))
  },
}))
