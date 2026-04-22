import { create } from 'zustand'

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

          const previousStart = String(n.data.executionStartedAt || '').trim()
          const previousStartMs = previousStart ? Date.parse(previousStart) : Number.NaN
          const hasValidStart = Number.isFinite(previousStartMs)

          let nextStartAt = previousStart || undefined
          let nextFinishedAt = n.data.executionFinishedAt
          let nextDurationMs = n.data.executionDurationMs

          if (statusNorm === 'idle') {
            nextStartAt = undefined
            nextFinishedAt = undefined
            nextDurationMs = undefined
          } else if (statusNorm === 'running') {
            nextStartAt = previousStart || nowIso
            nextFinishedAt = undefined
            nextDurationMs = hasValidStart ? Math.max(0, nowMs - Number(previousStartMs)) : 0
          } else if (statusNorm === 'success' || statusNorm === 'error') {
            nextFinishedAt = nowIso
            nextDurationMs = hasValidStart ? Math.max(0, nowMs - Number(previousStartMs)) : n.data.executionDurationMs
          }

          return {
            ...n,
            data: {
              ...n.data,
              status: status as ETLNode['data']['status'],
              executionRows: rows ?? n.data.executionRows,
              executionProcessedRows: statusNorm === 'running'
                ? (processedRows ?? n.data.executionProcessedRows)
                : undefined,
              executionValidatedRows: statusNorm === 'running'
                ? (validatedRows ?? n.data.executionValidatedRows)
                : undefined,
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
                    const startedMs = Date.parse(startedAt)
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
        const latestByNode = new Map<string, Record<string, any>>()
        logs.forEach((log) => {
          const nodeId = String(log?.nodeId || '').trim()
          if (!nodeId) return
          latestByNode.set(nodeId, log)
        })
        latestByNode.forEach((log, nodeId) => {
          const mappedStatus = mapNodeLogStatusForExecution(log?.status, statusNorm)
          const rows = typeof log?.rows === 'number' ? log.rows : undefined
          const processedRows = typeof log?.processed_rows === 'number' ? log.processed_rows : undefined
          const validatedRows = typeof log?.validated_rows === 'number' ? log.validated_rows : undefined
          get().setNodeStatus(nodeId, mappedStatus, rows, processedRows, validatedRows)
        })
        if (isTerminal) {
          forceStopRunningNodes(terminalFallbackNodeStatus(statusNorm))
        }
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
                // Do not append stale progress after node reached terminal status.
                if (!canMoveToRunningFromProgress) {
                  return {}
                }
                return { executionLogs: [...state.executionLogs, entry] }
              })
            }
            if (canMoveToRunningFromProgress) {
              get().setNodeStatus(msg.nodeId, 'running', liveRows, liveProcessedRows, liveValidatedRows)
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
        await sleep(startupOrNoWsSignal ? 2000 : 2000)
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
          if (includeLogsInPoll && Array.isArray(exec.logs) && exec.logs.length > 0) {
            set({
              executionLogs: normalizeExecutionLogsForDisplay(
                exec.logs as Array<Record<string, any>>,
                String(exec.status || ''),
              ),
            })
            applyLiveNodeStatusFromLogs(exec.logs as Array<Record<string, any>>, String(exec.status || ''))
          }
          if (String(exec.status || '').toLowerCase() === 'cancelling') {
            forceStopRunningNodes()
          }
          if (TERMINAL_EXECUTION_STATUSES.has(String(exec.status || '').toLowerCase())) {
            let finalExec = exec
            if (!Array.isArray(finalExec.logs) || finalExec.logs.length === 0) {
              finalExec = await api.getExecution(execId, {
                includeLogs: true,
                logTail: 5000,
              })
              if (Array.isArray(finalExec.logs) && finalExec.logs.length > 0) {
                set({
                  executionLogs: normalizeExecutionLogsForDisplay(
                    finalExec.logs as Array<Record<string, any>>,
                    String(finalExec.status || ''),
                  ),
                })
                applyLiveNodeStatusFromLogs(
                  finalExec.logs as Array<Record<string, any>>,
                  String(finalExec.status || ''),
                )
              }
            }
            applyLiveNodeStatusFromLogs(
              (finalExec.logs || []) as Array<Record<string, any>>,
              String(finalExec.status || ''),
            )
            forceStopRunningNodes(
              terminalFallbackNodeStatus(String(finalExec.status || exec.status || '')),
            )
            if (
              String(finalExec.status || '').toLowerCase() === 'cancelled'
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

    const current = get()
    const currentPipelineId = current.pipeline?.id ? String(current.pipeline.id) : ''
    if (current.isExecuting && current.executionId && currentPipelineId === targetPipelineId) {
      return true
    }

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
                  const startedMs = Date.parse(startedAt)
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
      const latestByNode = new Map<string, Record<string, any>>()
      logs.forEach((log) => {
        const nodeId = String(log?.nodeId || '').trim()
        if (!nodeId) return
        latestByNode.set(nodeId, log)
      })
      latestByNode.forEach((log, nodeId) => {
        const mappedStatus = mapNodeLogStatusForExecution(log?.status, statusNorm)
        const rows = typeof log?.rows === 'number' ? log.rows : undefined
        const processedRows = typeof log?.processed_rows === 'number' ? log.processed_rows : undefined
        const validatedRows = typeof log?.validated_rows === 'number' ? log.validated_rows : undefined
        get().setNodeStatus(nodeId, mappedStatus, rows, processedRows, validatedRows)
      })
      if (isTerminal) {
        forceStopRunningNodes(terminalFallbackNodeStatus(statusNorm))
      }
    }

    try {
      const executions = await api.listExecutions(targetPipelineId)
      const activeExecution = (Array.isArray(executions) ? executions : []).find((item: any) => {
        const status = String(item?.status || '').trim().toLowerCase()
        return status === 'running' || status === 'cancelling'
      })
      if (!activeExecution?.id) return false

      const executionId = String(activeExecution.id)
      const executionSnapshot = await api.getExecution(executionId, {
        includeLogs: true,
        logTail: 300,
      })
      const snapshotStatus = String(executionSnapshot?.status || activeExecution.status || '').trim().toLowerCase()
      const logs = Array.isArray(executionSnapshot?.logs) ? executionSnapshot.logs : []

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
          await sleep(2000)
          const state = get()
          if (!state.isExecuting || state.executionId !== executionId) break
          try {
            const exec = await api.getExecution(executionId, {
              includeLogs: true,
              logTail: 300,
            })
            consecutivePollErrors = 0
            if (Array.isArray(exec.logs) && exec.logs.length > 0) {
              set({
                executionLogs: normalizeExecutionLogsForDisplay(
                  exec.logs as Array<Record<string, any>>,
                  String(exec.status || ''),
                ),
              })
              applyLiveNodeStatusFromLogs(exec.logs as Array<Record<string, any>>, String(exec.status || ''))
            }
            const execStatus = String(exec.status || '').toLowerCase()
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
      await api.abortExecution(executionId)
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
      const executions = await api.listExecutions(pipelineId)
      set({ executions })
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
