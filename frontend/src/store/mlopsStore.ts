import { create } from 'zustand'
import { addEdge, applyNodeChanges, applyEdgeChanges } from 'reactflow'
import type { NodeChange, EdgeChange, Connection } from 'reactflow'
import { v4 as uuidv4 } from 'uuid'
import api from '../api/client'
import { getMLOpsNodeDef } from '../constants/mlopsNodeTypes'
import {
  DEFAULT_WORKFLOW_CONNECTOR_TYPE,
  applyConnectorTypeToEdges,
  resolveConnectorTypeFromEdges,
  type WorkflowConnectorType,
} from '../constants/workflowConnectors'
import type { ETLEdge, ETLNode, MLOpsWorkflow } from '../types'

interface MLOpsLogEntry {
  nodeId: string
  nodeLabel: string
  timestamp: string
  status: string
  message: string
  rows: number
  input_sample?: Record<string, unknown>[]
  output_sample?: Record<string, unknown>[]
}

interface NodeSamplePreview {
  input: Record<string, unknown>[]
  output: Record<string, unknown>[]
}

interface MLOpsWorkflowState {
  nodes: ETLNode[]
  edges: ETLEdge[]
  connectorType: WorkflowConnectorType
  selectedNodeId: string | null
  workflow: MLOpsWorkflow | null
  isDirty: boolean
  isExecuting: boolean
  runId: string | null
  runLogs: MLOpsLogEntry[]
  showLogs: boolean
  runMetrics: Record<string, unknown>
  nodeSamples: Record<string, NodeSamplePreview>

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
  setNodeStatus: (
    nodeId: string,
    status: string,
    rows?: number,
    inputSample?: Record<string, unknown>[],
    outputSample?: Record<string, unknown>[]
  ) => void

  loadWorkflow: (workflowId: string) => Promise<void>
  saveWorkflow: () => Promise<void>
  executeWorkflow: () => Promise<void>
  setShowLogs: (show: boolean) => void
  resetCanvas: () => void
}

function sanitizeSampleRows(rows: Record<string, unknown>[], limit?: number): Record<string, unknown>[] {
  const selectedRows = typeof limit === 'number' ? (rows || []).slice(0, Math.max(limit, 0)) : (rows || [])
  return selectedRows.map((row) => {
    const next: Record<string, unknown> = {}
    Object.entries(row || {}).forEach(([key, value]) => {
      if (value === null || ['string', 'number', 'boolean'].includes(typeof value)) {
        next[key] = value
      } else {
        next[key] = String(value)
      }
    })
    return next
  })
}

function syntheticSourceSample(): Record<string, unknown>[] {
  return Array.from({ length: 30 }, (_, i) => ({
    record_id: `R-${1000 + i}`,
    event_date: `2026-03-${String((10 + i) % 28 || 28).padStart(2, '0')}`,
    region: ['North', 'South', 'East', 'West'][i % 4],
    amount: Number((25 + Math.random() * 500).toFixed(2)),
    target: i % 2,
  }))
}

function buildNodeOutputSample(
  nodeType: string,
  label: string,
  inputSample: Record<string, unknown>[],
  rows: number,
  metrics: Record<string, unknown>
): Record<string, unknown>[] {
  const lower = nodeType.toLowerCase()
  const base = inputSample.length > 0 ? inputSample : syntheticSourceSample()

  if (lower.includes('training') || lower.includes('train')) {
    return [{
      model_name: label,
      algorithm: 'xgboost',
      train_rows: rows,
      accuracy: metrics.accuracy,
      precision: metrics.precision,
      recall: metrics.recall,
    }]
  }
  if (lower.includes('evaluation')) {
    return [{
      rmse: metrics.rmse,
      mape: metrics.mape,
      f1_score: metrics.f1_score,
      evaluated_rows: rows,
    }]
  }
  if (lower.includes('forecast')) {
    return Array.from({ length: 30 }, (_, i) => ({
      forecast_day: i + 1,
      forecast_date: `2026-04-${String(i + 1).padStart(2, '0')}`,
      prediction: Number((80 + Math.random() * 540).toFixed(2)),
      lower_bound: Number((50 + Math.random() * 420).toFixed(2)),
      upper_bound: Number((120 + Math.random() * 690).toFixed(2)),
    }))
  }
  if (lower.includes('deploy') || lower.includes('endpoint') || lower.includes('registry')) {
    return [{
      model_version: metrics.model_version || `v${new Date().toISOString().replace(/[-:TZ.]/g, '').slice(0, 14)}`,
      endpoint: `/mlops/inference/${label.toLowerCase().replace(/\s+/g, '_')}`,
      status: 'deployed',
    }]
  }
  if (lower.includes('feature')) {
    return base.map((row, idx) => ({
      ...row,
      feature_score: Number((0.3 + Math.random() * 0.65).toFixed(3)),
      feature_bucket: ['low', 'mid', 'high'][idx % 3],
    }))
  }
  if (lower.includes('split')) {
    return base.map((row, idx) => ({ ...row, split: idx % 5 === 0 ? 'test' : 'train' }))
  }
  if (lower.includes('staging') || lower.includes('quality')) {
    return base.map((row) => ({ ...row, quality_status: 'pass' }))
  }
  return base
}

async function simulateLocalMLOpsExecution(
  get: () => MLOpsWorkflowState,
  set: (partial: Partial<MLOpsWorkflowState> | ((s: MLOpsWorkflowState) => Partial<MLOpsWorkflowState>)) => void
) {
  const { nodes, edges } = get()
  if (nodes.length === 0) {
    set({ isExecuting: false })
    return
  }

  const adj: Record<string, string[]> = {}
  const inDeg: Record<string, number> = {}
  nodes.forEach((n) => { adj[n.id] = []; inDeg[n.id] = 0 })
  edges.forEach((e) => {
    if (adj[e.source]) adj[e.source].push(e.target)
    if (inDeg[e.target] !== undefined) inDeg[e.target] += 1
  })
  const queue = nodes.filter((n) => inDeg[n.id] === 0).map((n) => n.id)
  const order: string[] = []
  while (queue.length > 0) {
    const id = queue.shift()!
    order.push(id)
    for (const nb of adj[id] || []) {
      inDeg[nb] -= 1
      if (inDeg[nb] === 0) queue.push(nb)
    }
  }
  nodes.forEach((n) => {
    if (!order.includes(n.id)) order.push(n.id)
  })

  const rowsByNode: Record<string, number> = {}
  const samplesByNode: Record<string, Record<string, unknown>[]> = {}
  const metrics: Record<string, unknown> = {}

  for (const nodeId of order) {
    const node = nodes.find((n) => n.id === nodeId)
    if (!node) continue
    const nodeType = String(node.data?.nodeType || '').toLowerCase()
    const label = node.data?.label || 'MLOps Step'

    set((state) => ({
      nodes: state.nodes.map((n) => n.id === nodeId ? { ...n, data: { ...n.data, status: 'running' as const } } : n),
      runLogs: [...state.runLogs, {
        nodeId,
        nodeLabel: label,
        timestamp: new Date().toISOString(),
        status: 'running',
        message: `⟳ Running ${label}…`,
        rows: 0,
      }],
    }))

    await new Promise((resolve) => setTimeout(resolve, 450 + Math.random() * 350))

    const upstreamRows = edges
      .filter((e) => e.target === nodeId)
      .reduce((sum, e) => sum + (rowsByNode[e.source] || 0), 0)
    const inputSample = sanitizeSampleRows(
      edges
        .filter((e) => e.target === nodeId)
        .map((e) => samplesByNode[e.source])
        .find((rows) => Array.isArray(rows) && rows.length > 0) || []
    )

    let rows = upstreamRows || 20000 + Math.floor(Math.random() * 100000)
    if (nodeType.includes('staging')) rows = Math.floor(rows * 0.9)
    if (nodeType.includes('feature')) {
      rows = Math.floor(rows * 0.85)
      metrics.feature_count = 12 + Math.floor(Math.random() * 150)
    }
    if (nodeType.includes('training') || nodeType.includes('train')) {
      rows = Math.floor(rows * 0.8)
      metrics.accuracy = Number((0.78 + Math.random() * 0.18).toFixed(3))
      metrics.precision = Number((0.75 + Math.random() * 0.2).toFixed(3))
      metrics.recall = Number((0.73 + Math.random() * 0.2).toFixed(3))
    }
    if (nodeType.includes('evaluation')) {
      metrics.rmse = Number((1 + Math.random() * 14).toFixed(3))
      metrics.mape = Number((3 + Math.random() * 14).toFixed(2))
    }
    if (nodeType.includes('forecast')) {
      metrics.forecast_horizon_days = [7, 14, 30, 60][Math.floor(Math.random() * 4)]
    }
    if (nodeType.includes('deployment') || nodeType.includes('endpoint') || nodeType.includes('registry')) {
      metrics.model_version = `v${new Date().toISOString().replace(/[-:TZ.]/g, '').slice(0, 14)}`
    }

    const outputSample = sanitizeSampleRows(
      nodeType.includes('source') || nodeType.includes('dataset')
        ? syntheticSourceSample()
        : buildNodeOutputSample(nodeType, label, inputSample, rows, metrics)
    )

    rowsByNode[nodeId] = Math.max(0, rows)
    samplesByNode[nodeId] = outputSample
    const message = nodeType.includes('training')
      ? `✓ ${label} — trained model on ${rows.toLocaleString()} rows`
      : nodeType.includes('evaluation')
      ? `✓ ${label} — evaluated model metrics`
      : nodeType.includes('deployment') || nodeType.includes('endpoint')
      ? `✓ ${label} — deployed model endpoint`
      : `✓ ${label} — processed ${rows.toLocaleString()} rows`

    set((state) => {
      const idx = state.runLogs.findLastIndex((l) => l.nodeId === nodeId && l.status === 'running')
      const successLog: MLOpsLogEntry = {
        nodeId,
        nodeLabel: label,
        timestamp: new Date().toISOString(),
        status: 'success',
        message,
        rows,
        input_sample: inputSample,
        output_sample: outputSample,
      }
      const nextLogs = [...state.runLogs]
      if (idx >= 0) nextLogs[idx] = successLog
      else nextLogs.push(successLog)
      const nodeSampleState = {
        ...state.nodeSamples,
        [nodeId]: {
          input: inputSample,
          output: outputSample,
        },
      }
      return {
        nodes: state.nodes.map((n) => (
          n.id === nodeId
            ? {
                ...n,
                data: {
                  ...n.data,
                  status: 'success' as const,
                  executionRows: rows,
                  executionSampleInput: inputSample,
                  executionSampleOutput: outputSample,
                },
              }
            : n
        )),
        runLogs: nextLogs,
        runMetrics: { ...state.runMetrics, ...metrics },
        nodeSamples: nodeSampleState,
      }
    })
  }

  set({ isExecuting: false })
}

export const useMLOpsWorkflowStore = create<MLOpsWorkflowState>((set, get) => ({
  nodes: [],
  edges: [],
  connectorType: DEFAULT_WORKFLOW_CONNECTOR_TYPE,
  selectedNodeId: null,
  workflow: null,
  isDirty: false,
  isExecuting: false,
  runId: null,
  runLogs: [],
  showLogs: false,
  runMetrics: {},
  nodeSamples: {},

  setNodes: (nodes) => set({ nodes }),
  setEdges: (edges) => set((state) => ({
    edges: applyConnectorTypeToEdges(edges, state.connectorType),
  })),
  setConnectorType: (connectorType) => set((state) => ({
    connectorType,
    edges: applyConnectorTypeToEdges(state.edges, connectorType),
    isDirty: true,
  })),

  onNodesChange: (changes) => set((state) => ({
    nodes: applyNodeChanges(changes, state.nodes) as ETLNode[],
    isDirty: true,
  })),

  onEdgesChange: (changes) => set((state) => ({
    edges: applyEdgeChanges(changes, state.edges) as ETLEdge[],
    isDirty: true,
  })),

  onConnect: (connection) => set((state) => ({
    edges: addEdge(
      {
        ...connection,
        animated: true,
        type: state.connectorType,
        style: { stroke: '#22c55e', strokeWidth: 2 },
      },
      state.edges
    ) as ETLEdge[],
    isDirty: true,
  })),

  addNode: (type, position) => {
    const def = getMLOpsNodeDef(type)
    if (!def) return
    const id = uuidv4()
    const defaultConfig: Record<string, unknown> = {}
    def.configFields.forEach((field) => {
      if (field.defaultValue !== undefined) defaultConfig[field.name] = field.defaultValue
    })
    const newNode: ETLNode = {
      id,
      type: 'etlNode',
      position: position || { x: 200 + Math.random() * 260, y: 100 + Math.random() * 220 },
      data: {
        nodeType: type,
        label: def.label,
        definition: def,
        config: defaultConfig,
        status: 'idle',
      },
    }
    set((state) => ({
      nodes: [...state.nodes, newNode],
      selectedNodeId: id,
      isDirty: true,
    }))
  },

  updateNodeConfig: (nodeId, config) => set((state) => ({
    nodes: state.nodes.map((node) => (
      node.id === nodeId
        ? { ...node, data: { ...node.data, config: { ...node.data.config, ...config } } }
        : node
    )),
    isDirty: true,
  })),

  updateNodeLabel: (nodeId, label) => set((state) => ({
    nodes: state.nodes.map((node) => (
      node.id === nodeId ? { ...node, data: { ...node.data, label } } : node
    )),
    isDirty: true,
  })),

  removeNode: (nodeId) => set((state) => ({
    nodes: state.nodes.filter((n) => n.id !== nodeId),
    edges: state.edges.filter((e) => e.source !== nodeId && e.target !== nodeId),
    selectedNodeId: state.selectedNodeId === nodeId ? null : state.selectedNodeId,
    isDirty: true,
  })),

  setSelectedNode: (nodeId) => set({ selectedNodeId: nodeId }),

  setNodeStatus: (nodeId, status, rows, inputSample, outputSample) => set((state) => {
    const previous = state.nodeSamples[nodeId] || { input: [], output: [] }
    const nextInput = inputSample ?? previous.input
    const nextOutput = outputSample ?? previous.output
    return {
      nodes: state.nodes.map((node) => (
        node.id === nodeId
          ? {
              ...node,
              data: {
                ...node.data,
                status: status as ETLNode['data']['status'],
                executionRows: rows ?? node.data.executionRows,
                executionSampleInput: nextInput,
                executionSampleOutput: nextOutput,
              },
            }
          : node
      )),
      nodeSamples: {
        ...state.nodeSamples,
        [nodeId]: { input: nextInput, output: nextOutput },
      },
    }
  }),

  loadWorkflow: async (workflowId) => {
    try {
      const workflow = await api.getMLOpsWorkflow(workflowId)
      const loadedEdges = (workflow.edges || []) as ETLEdge[]
      const connectorType = resolveConnectorTypeFromEdges(loadedEdges)
      set({
        workflow,
        nodes: (workflow.nodes || []).map((node: ETLNode) => {
          const def = getMLOpsNodeDef(node.data?.nodeType)
          return {
            ...node,
            type: 'etlNode',
            data: {
              ...node.data,
              definition: def || node.data.definition,
              executionSampleInput: undefined,
              executionSampleOutput: undefined,
            },
          }
        }),
        edges: applyConnectorTypeToEdges(loadedEdges, connectorType),
        connectorType,
        isDirty: false,
        nodeSamples: {},
        runLogs: [],
        runMetrics: {},
      })
    } catch (error) {
      console.error('Failed to load MLOps workflow:', error)
    }
  },

  saveWorkflow: async () => {
    const { workflow, nodes, edges } = get()
    if (!workflow?.id) return
    try {
      await api.updateMLOpsWorkflow(workflow.id, { nodes, edges })
      set({ isDirty: false })
    } catch (error) {
      console.error('Failed to save MLOps workflow:', error)
    }
  },

  executeWorkflow: async () => {
    const { workflow, nodes, edges } = get()
    if (!workflow?.id) return

    set((state) => ({
      nodes: state.nodes.map((node) => ({
        ...node,
        data: {
          ...node.data,
          status: 'idle',
          executionRows: undefined,
          executionSampleInput: undefined,
          executionSampleOutput: undefined,
        },
      })),
      isExecuting: true,
      runLogs: [],
      runMetrics: {},
      nodeSamples: {},
      showLogs: true,
    }))

    try {
      let activeWorkflowId = workflow.id
      const localOnly = workflow.id.startsWith('mw_')
      if (localOnly) {
        try {
          const created = await api.createMLOpsWorkflow({
            name: workflow.name || 'Untitled MLOps Workflow',
            description: workflow.description || '',
          })
          await api.updateMLOpsWorkflow(created.id, { nodes, edges })
          activeWorkflowId = created.id
          set({ workflow: { ...workflow, id: created.id } })
        } catch {
          // backend unavailable; fallback below
        }
      } else {
        await get().saveWorkflow()
      }

      const result = await api.executeMLOpsWorkflow(activeWorkflowId)
      if (result.offline) {
        await simulateLocalMLOpsExecution(get, set)
        return
      }

      const runId = result.run_id
      set({ runId })
      for (let i = 0; i < 120; i++) {
        await new Promise((resolve) => setTimeout(resolve, 1000))
        const run = await api.getMLOpsRun(runId)
        if (run.logs?.length > 0) set({ runLogs: run.logs })
        if (run.metrics) set({ runMetrics: run.metrics })
        if (run.logs?.length > 0) {
          run.logs.forEach((log: MLOpsLogEntry) => {
            get().setNodeStatus(
              log.nodeId,
              log.status === 'success' ? 'success' : log.status === 'error' ? 'error' : 'running',
              log.rows,
              log.input_sample,
              log.output_sample
            )
          })
        }
        if (run.status !== 'running') {
          set({ isExecuting: false })
          return
        }
      }
      set({ isExecuting: false })
    } catch (error) {
      console.error('MLOps execution failed:', error)
      set({ isExecuting: false })
    }
  },

  setShowLogs: (show) => set({ showLogs: show }),

  resetCanvas: () => set({
    nodes: [],
    edges: [],
    connectorType: DEFAULT_WORKFLOW_CONNECTOR_TYPE,
    selectedNodeId: null,
    isDirty: false,
    runLogs: [],
    runMetrics: {},
    nodeSamples: {},
  }),
}))

interface MLOpsCatalogState {
  workflows: MLOpsWorkflow[]
  loading: boolean
  fetchWorkflows: () => Promise<void>
  createWorkflow: (name: string, description?: string) => Promise<MLOpsWorkflow>
  deleteWorkflow: (id: string) => Promise<void>
  duplicateWorkflow: (id: string) => Promise<void>
}

export const useMLOpsCatalogStore = create<MLOpsCatalogState>((set, get) => ({
  workflows: [],
  loading: false,

  fetchWorkflows: async () => {
    set({ loading: true })
    try {
      const workflows = await api.listMLOpsWorkflows()
      set({ workflows })
    } finally {
      set({ loading: false })
    }
  },

  createWorkflow: async (name, description = '') => {
    const workflow = await api.createMLOpsWorkflow({ name, description })
    await get().fetchWorkflows()
    return workflow
  },

  deleteWorkflow: async (id) => {
    await api.deleteMLOpsWorkflow(id)
    set((state) => ({ workflows: state.workflows.filter((wf) => wf.id !== id) }))
  },

  duplicateWorkflow: async (id) => {
    await api.duplicateMLOpsWorkflow(id)
    await get().fetchWorkflows()
  },
}))
