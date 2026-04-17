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
import { addEdge, applyNodeChanges, applyEdgeChanges } from 'reactflow'
import type { NodeChange, EdgeChange, Connection } from 'reactflow'
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
  executionLogs: Array<{ nodeId: string; nodeLabel: string; timestamp: string; status: string; message: string; rows: number; output_path?: string }>
  showLogs: boolean

  setNodes: (nodes: ETLNode[]) => void
  setEdges: (edges: ETLEdge[]) => void
  setConnectorType: (connectorType: WorkflowConnectorType) => void
  onNodesChange: (changes: NodeChange[]) => void
  onEdgesChange: (changes: EdgeChange[]) => void
  onConnect: (connection: Connection) => void
  addNode: (type: string, position?: { x: number; y: number }) => void
  updateNodeConfig: (nodeId: string, config: Record<string, unknown>) => void
  updateNodeLabel: (nodeId: string, label: string) => void
  removeNode: (nodeId: string) => void
  setSelectedNode: (nodeId: string | null) => void
  setNodeStatus: (nodeId: string, status: string, rows?: number) => void

  loadPipeline: (pipelineId: string) => Promise<void>
  savePipeline: () => Promise<void>
  executePipeline: () => Promise<void>
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
          style: { stroke: '#6366f1', strokeWidth: 2 },
        },
        state.edges,
      ) as ETLEdge[],
      isDirty: true,
    })),

  addNode: (type, position) => {
    const def = getNodeDef(type)
    if (!def) return
    const id = uuidv4()
    const defaultConfig: Record<string, unknown> = {}
    def.configFields.forEach((f) => {
      if (f.defaultValue !== undefined) defaultConfig[f.name] = f.defaultValue
    })
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

  updateNodeConfig: (nodeId, config) =>
    set((state) => ({
      nodes: state.nodes.map((n) =>
        n.id === nodeId ? { ...n, data: { ...n.data, config: { ...n.data.config, ...config } } } : n
      ),
      isDirty: true,
    })),

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

  setNodeStatus: (nodeId, status, rows) =>
    set((state) => ({
      nodes: state.nodes.map((n) =>
        n.id === nodeId
          ? { ...n, data: { ...n.data, status: status as ETLNode['data']['status'], executionRows: rows ?? n.data.executionRows } }
          : n
      ),
    })),

  loadPipeline: async (pipelineId) => {
    try {
      const pipeline = await api.getPipeline(pipelineId)
      const loadedEdges = (pipeline.edges || []) as ETLEdge[]
      const connectorType = resolveConnectorTypeFromEdges(loadedEdges)
      set({
        pipeline,
        nodes: (pipeline.nodes || []).map((n: ETLNode) => {
          const def = getNodeDef(n.data?.nodeType)
          return { ...n, type: 'etlNode', data: { ...n.data, definition: def || n.data.definition } }
        }),
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
    const { pipeline, nodes, edges } = get()
    if (!pipeline?.id) return

    // Reset all node statuses
    set((state) => ({
      nodes: state.nodes.map((n) => ({ ...n, data: { ...n.data, status: 'idle', executionRows: undefined } })),
      isExecuting: true,
      executionLogs: [],
      showLogs: true,
    }))

    try {
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
          await api.updatePipeline(created.id, { nodes, edges })
          activePipelineId = created.id
          // Upgrade the store so future saves go to the real backend record
          set({ pipeline: { ...pipeline, id: created.id } })
        } catch {
          // backend truly unreachable — will be caught below as offline
        }
      } else {
        await get().savePipeline()
      }

      const result = await api.executePipeline(activePipelineId)
      const execId = result.execution_id

      // ── OFFLINE: backend not running ─────────────────────────────────────
      if (result.offline) {
        set({
          isExecuting: false,
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
      set({ executionId: execId })

      let ws: WebSocket | null = null
      try {
        ws = new WebSocket(`ws://localhost:8001/ws/executions/${execId}`)

        ws.onmessage = (event) => {
          const msg = JSON.parse(event.data)
          const entry = msg.log_entry  // full log entry streamed from backend

          if (msg.type === 'node_start') {
            get().setNodeStatus(msg.nodeId, 'running')
            // Add "running" log entry immediately
            if (entry) {
              set((state) => ({
                executionLogs: [...state.executionLogs, entry]
              }))
            }
          }
          else if (msg.type === 'node_success') {
            get().setNodeStatus(msg.nodeId, 'success', msg.rows)
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
          else if (msg.type === 'node_error') {
            get().setNodeStatus(msg.nodeId, 'error')
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
        }

        ws.onerror = () => { /* ignore WS errors gracefully */ }
      } catch { /* WS unavailable */ }

      const poll = async () => {
        for (let i = 0; i < 60; i++) {
          await new Promise((r) => setTimeout(r, 1000))
          try {
            const exec = await api.getExecution(execId)
            if (exec.logs?.length > 0) set({ executionLogs: exec.logs })
            if (exec.status !== 'running') {
              exec.logs?.forEach((log: { nodeId: string; status: string; rows: number }) => {
                get().setNodeStatus(log.nodeId, log.status === 'success' ? 'success' : 'error', log.rows)
              })
              set({ isExecuting: false, executionId: null })
              ws?.close()
              return
            }
          } catch { break }
        }
        set({ isExecuting: false, executionId: null })
        ws?.close()
      }
      poll()

    } catch (e) {
      set({ isExecuting: false, executionId: null })
      console.error('Execution failed:', e)
    }
  },

  setShowLogs: (show) => set({ showLogs: show }),

  resetCanvas: () => set({
    nodes: [],
    edges: [],
    connectorType: DEFAULT_WORKFLOW_CONNECTOR_TYPE,
    selectedNodeId: null,
    isDirty: false,
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
