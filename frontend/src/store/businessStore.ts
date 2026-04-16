import { create } from 'zustand'
import { addEdge, applyNodeChanges, applyEdgeChanges } from 'reactflow'
import type { NodeChange, EdgeChange, Connection } from 'reactflow'
import { v4 as uuidv4 } from 'uuid'
import api from '../api/client'
import { getBusinessNodeDef } from '../constants/businessNodeTypes'
import {
  DEFAULT_WORKFLOW_CONNECTOR_TYPE,
  applyConnectorTypeToEdges,
  resolveConnectorTypeFromEdges,
  type WorkflowConnectorType,
} from '../constants/workflowConnectors'
import type { BusinessWorkflow, ETLEdge, ETLNode } from '../types'

interface BusinessLogEntry {
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

interface BusinessWorkflowState {
  nodes: ETLNode[]
  edges: ETLEdge[]
  connectorType: WorkflowConnectorType
  selectedNodeId: string | null
  workflow: BusinessWorkflow | null
  isDirty: boolean
  isExecuting: boolean
  runId: string | null
  runLogs: BusinessLogEntry[]
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

function syntheticBusinessRows(): Record<string, unknown>[] {
  return Array.from({ length: 40 }, (_, i) => ({
    record_id: `B-${1200 + i}`,
    region: ['North', 'South', 'East', 'West'][i % 4],
    segment: ['Enterprise', 'SMB', 'Startup'][i % 3],
    revenue: Number((1800 + Math.random() * 24000).toFixed(2)),
    churn_risk: ['Low', 'Medium', 'High'][i % 3],
  }))
}

async function simulateLocalBusinessExecution(
  get: () => BusinessWorkflowState,
  set: (partial: Partial<BusinessWorkflowState> | ((s: BusinessWorkflowState) => Partial<BusinessWorkflowState>)) => void
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
  nodes.forEach((n) => { if (!order.includes(n.id)) order.push(n.id) })

  const rowsByNode: Record<string, Record<string, unknown>[]> = {}
  const textByNode: Record<string, string> = {}

  for (const nodeId of order) {
    const node = nodes.find((n) => n.id === nodeId)
    if (!node) continue
    const nodeType = String(node.data?.nodeType || '').toLowerCase()
    const label = node.data?.label || 'Business Step'

    const inputRows = edges
      .filter((e) => e.target === nodeId)
      .flatMap((e) => rowsByNode[e.source] || [])

    set((state) => ({
      nodes: state.nodes.map((n) => n.id === nodeId ? { ...n, data: { ...n.data, status: 'running' as const } } : n),
      runLogs: [...state.runLogs, {
        nodeId,
        nodeLabel: label,
        timestamp: new Date().toISOString(),
        status: 'running',
        message: `⟳ Running ${label}…`,
        rows: 0,
        input_sample: sanitizeSampleRows(inputRows),
        output_sample: [],
      }],
    }))

    await new Promise((resolve) => setTimeout(resolve, 350 + Math.random() * 250))

    let outputRows: Record<string, unknown>[] = []
    let message = `✓ ${label} completed`

    if (nodeType.includes('etl') || nodeType.includes('mlops') || nodeType.includes('source')) {
      outputRows = syntheticBusinessRows()
      message = `✓ ${label} loaded ${outputRows.length.toLocaleString()} rows`
    } else if (nodeType.includes('prompt') || nodeType.includes('llm')) {
      const prompt = String(node.data?.config?.prompt || '').trim() || 'Analyze business data and recommend actions.'
      const model = String(node.data?.config?.model || 'gpt-oss20b')
      const text = `Local fallback response (${model}): ${prompt.slice(0, 100)}${prompt.length > 100 ? '...' : ''}`
      textByNode[nodeId] = text
      outputRows = [{ model, provider: 'local-fallback', response: text, source_rows: inputRows.length }]
      message = `✓ ${label} generated model response using ${model}`
    } else if (nodeType.includes('decision')) {
      const compareValue = String(node.data?.config?.compare_value || '').toLowerCase()
      const fieldName = String(node.data?.config?.field_name || '').trim()
      const candidateRows = inputRows.length > 0 ? inputRows : syntheticBusinessRows()
      outputRows = candidateRows.filter((row) => {
        if (!compareValue) return true
        if (fieldName) return String(row[fieldName] ?? '').toLowerCase().includes(compareValue)
        const latestText = Object.values(textByNode).slice(-1)[0] || ''
        return String(latestText).toLowerCase().includes(compareValue)
      })
      message = `✓ ${label} matched ${outputRows.length.toLocaleString()} rows`
    } else if (nodeType.includes('mail')) {
      const to = String(node.data?.config?.to_email || 'team@example.com')
      const subject = String(node.data?.config?.subject || 'Business Recommendation')
      const body = String(node.data?.config?.body_template || Object.values(textByNode).slice(-1)[0] || 'No recommendation generated.')
      const sendMode = String(node.data?.config?.send_mode || 'send').toLowerCase()
      const status = sendMode === 'draft' ? 'draft' : 'sent (simulated)'
      outputRows = [{ to, subject, body, status, generated_at: new Date().toISOString() }]
      message = sendMode === 'draft'
        ? `✓ ${label} drafted email for ${to}`
        : `✓ ${label} sent email (simulated) to ${to}`
    } else if (nodeType.includes('whatsapp')) {
      const to = String(node.data?.config?.to_phone || '').trim()
      const messageType = String(node.data?.config?.message_type || 'template').toLowerCase()
      const sendMode = String(node.data?.config?.send_mode || 'send').toLowerCase()
      const textBody = String(
        node.data?.config?.text_body
        || Object.values(textByNode).slice(-1)[0]
        || 'No recommendation generated.'
      )
      const templateName = String(node.data?.config?.template_name || 'hello_world')
      const templateLanguage = String(node.data?.config?.template_language || 'en_US')
      const requestPayloadRaw = String(node.data?.config?.request_payload_json || '').trim()
      const contextValues: Record<string, unknown> = {
        to_phone: to,
        message_type: messageType,
        text_body: textBody,
        template_name: templateName,
        template_language: templateLanguage,
      }
      const renderPayloadTemplate = (value: unknown): unknown => {
        if (typeof value === 'string') {
          return value.replace(/\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}/g, (_m, key: string) => {
            const replacement = contextValues[String(key)] ?? ''
            if (typeof replacement === 'object') return JSON.stringify(replacement)
            return String(replacement)
          })
        }
        if (Array.isArray(value)) return value.map(renderPayloadTemplate)
        if (value && typeof value === 'object') {
          const next: Record<string, unknown> = {}
          Object.entries(value as Record<string, unknown>).forEach(([k, v]) => {
            next[k] = renderPayloadTemplate(v)
          })
          return next
        }
        return value
      }
      let requestPayload: Record<string, unknown> | null = null
      let requestPayloadError = ''
      if (requestPayloadRaw) {
        try {
          const parsed = JSON.parse(requestPayloadRaw)
          if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
            requestPayload = renderPayloadTemplate(parsed) as Record<string, unknown>
          } else {
            requestPayloadError = 'Request JSON payload must be an object'
          }
        } catch (error) {
          requestPayloadError = `Invalid Request JSON payload: ${String((error as Error)?.message || error)}`
        }
      }
      const simulatedResponse = {
        messaging_product: 'whatsapp',
        contacts: to ? [{ input: to, wa_id: to }] : [],
        messages: [{ id: `wamid.SIMULATED.${Date.now()}` }],
      }
      outputRows = [{
        to,
        status: sendMode === 'draft' ? 'draft' : 'sent (simulated)',
        message_type: messageType,
        template_name: messageType === 'template' ? templateName : '',
        template_language: messageType === 'template' ? templateLanguage : '',
        text_body: messageType === 'text' ? textBody : '',
        request_payload_custom: Boolean(requestPayloadRaw),
        request_payload: requestPayload || undefined,
        request_payload_error: requestPayloadError || undefined,
        whatsapp_response: sendMode === 'draft' ? undefined : simulatedResponse,
        generated_at: new Date().toISOString(),
      }]
      message = sendMode === 'draft'
        ? `✓ ${label} drafted WhatsApp message for ${to || 'unknown recipient'}`
        : `✓ ${label} sent WhatsApp message (simulated) to ${to || 'unknown recipient'}. Response: ${JSON.stringify(simulatedResponse)}`
    } else if (nodeType.includes('analytics')) {
      const candidateRows = inputRows.length > 0 ? inputRows : syntheticBusinessRows()
      const numericKeys = Object.keys(candidateRows[0] || {}).filter((k) => typeof candidateRows[0]?.[k] === 'number')
      outputRows = [{
        summary: `Processed ${candidateRows.length} rows and ${Object.keys(candidateRows[0] || {}).length} columns.`,
        row_count: candidateRows.length,
        numeric_columns: numericKeys.length,
      }]
      message = `✓ ${label} generated business analytics summary`
    } else {
      outputRows = inputRows
      message = `✓ ${label} passed ${outputRows.length.toLocaleString()} rows`
    }

    outputRows = sanitizeSampleRows(outputRows)
    rowsByNode[nodeId] = outputRows

    set((state) => {
      const idx = state.runLogs.findLastIndex((l) => l.nodeId === nodeId && l.status === 'running')
      const successLog: BusinessLogEntry = {
        nodeId,
        nodeLabel: label,
        timestamp: new Date().toISOString(),
        status: 'success',
        message,
        rows: outputRows.length,
        input_sample: sanitizeSampleRows(inputRows),
        output_sample: sanitizeSampleRows(outputRows),
      }
      const nextLogs = [...state.runLogs]
      if (idx >= 0) nextLogs[idx] = successLog
      else nextLogs.push(successLog)
      return {
        nodes: state.nodes.map((n) => (
          n.id === nodeId
            ? {
                ...n,
                data: {
                  ...n.data,
                  status: 'success' as const,
                  executionRows: outputRows.length,
                  executionSampleInput: sanitizeSampleRows(inputRows),
                  executionSampleOutput: sanitizeSampleRows(outputRows),
                },
              }
            : n
        )),
        runLogs: nextLogs,
        nodeSamples: {
          ...state.nodeSamples,
          [nodeId]: {
            input: sanitizeSampleRows(inputRows),
            output: sanitizeSampleRows(outputRows),
          },
        },
      }
    })
  }

  set({ isExecuting: false, runMetrics: { total_rows: Object.values(rowsByNode).reduce((acc, rows) => acc + rows.length, 0), node_count: order.length } })
}

export const useBusinessWorkflowStore = create<BusinessWorkflowState>((set, get) => ({
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
        style: { stroke: '#f59e0b', strokeWidth: 2 },
      },
      state.edges,
    ) as ETLEdge[],
    isDirty: true,
  })),

  addNode: (type, position) => {
    const def = getBusinessNodeDef(type)
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
      const workflow = await api.getBusinessWorkflow(workflowId)
      const loadedEdges = (workflow.edges || []) as ETLEdge[]
      const connectorType = resolveConnectorTypeFromEdges(loadedEdges)
      set({
        workflow,
        nodes: (workflow.nodes || []).map((node: ETLNode) => {
          const def = getBusinessNodeDef(node.data?.nodeType)
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
      console.error('Failed to load business workflow:', error)
    }
  },

  saveWorkflow: async () => {
    const { workflow, nodes, edges } = get()
    if (!workflow?.id) return
    try {
      await api.updateBusinessWorkflow(workflow.id, { nodes, edges })
      set({ isDirty: false })
    } catch (error) {
      console.error('Failed to save business workflow:', error)
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
      const localOnly = workflow.id.startsWith('bw_')
      if (localOnly) {
        try {
          const created = await api.createBusinessWorkflow({
            name: workflow.name || 'Untitled Business Workflow',
            description: workflow.description || '',
          })
          await api.updateBusinessWorkflow(created.id, { nodes, edges })
          activeWorkflowId = created.id
          set({ workflow: { ...workflow, id: created.id } })
        } catch {
          // backend unavailable; fallback below
        }
      } else {
        await get().saveWorkflow()
      }

      const result = await api.executeBusinessWorkflow(activeWorkflowId)
      if (result.offline) {
        await simulateLocalBusinessExecution(get, set)
        return
      }

      const runId = result.run_id
      set({ runId })
      for (let i = 0; i < 240; i++) {
        await new Promise((resolve) => setTimeout(resolve, 1000))
        const run = await api.getBusinessRun(runId)
        if (run.logs?.length > 0) set({ runLogs: run.logs })
        if (run.metrics) set({ runMetrics: run.metrics })
        if (run.logs?.length > 0) {
          run.logs.forEach((log: BusinessLogEntry) => {
            get().setNodeStatus(
              log.nodeId,
              log.status === 'success' ? 'success' : log.status === 'error' ? 'error' : 'running',
              log.rows,
              log.input_sample,
              log.output_sample,
            )
          })
        }
        if (run.status !== 'running') {
          set({ isExecuting: false })
          return
        }
      }
      set((state) => {
        const timeoutMessage = '✗ Business workflow timed out while waiting for run completion.'
        const timestamp = new Date().toISOString()
        const hasTimeoutLog = state.runLogs.some((log) => log.message === timeoutMessage)
        return {
          isExecuting: false,
          nodes: state.nodes.map((node) => (
            node.data?.status === 'running'
              ? { ...node, data: { ...node.data, status: 'error' as const } }
              : node
          )),
          runLogs: hasTimeoutLog
            ? state.runLogs
            : [
                ...state.runLogs,
                {
                  nodeId: '__system__',
                  nodeLabel: 'System',
                  timestamp,
                  status: 'error',
                  message: timeoutMessage,
                  rows: 0,
                  input_sample: [],
                  output_sample: [],
                },
              ],
        }
      })
    } catch (error) {
      console.error('Business workflow execution failed:', error)
      const errorText = error instanceof Error ? error.message : String(error || 'Unknown error')
      set((state) => ({
        isExecuting: false,
        nodes: state.nodes.map((node) => (
          node.data?.status === 'running'
            ? { ...node, data: { ...node.data, status: 'error' as const } }
            : node
        )),
        runLogs: [
          ...state.runLogs,
          {
            nodeId: '__system__',
            nodeLabel: 'System',
            timestamp: new Date().toISOString(),
            status: 'error',
            message: `✗ Business workflow execution failed: ${errorText}`,
            rows: 0,
            input_sample: [],
            output_sample: [],
          },
        ],
      }))
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

interface BusinessCatalogState {
  workflows: BusinessWorkflow[]
  loading: boolean
  fetchWorkflows: () => Promise<void>
  createWorkflow: (name: string, description?: string) => Promise<BusinessWorkflow>
  deleteWorkflow: (id: string) => Promise<void>
  duplicateWorkflow: (id: string) => Promise<void>
}

export const useBusinessCatalogStore = create<BusinessCatalogState>((set, get) => ({
  workflows: [],
  loading: false,

  fetchWorkflows: async () => {
    set({ loading: true })
    try {
      const workflows = await api.listBusinessWorkflows()
      set({ workflows })
    } finally {
      set({ loading: false })
    }
  },

  createWorkflow: async (name, description = '') => {
    const workflow = await api.createBusinessWorkflow({ name, description })
    await get().fetchWorkflows()
    return workflow
  },

  deleteWorkflow: async (id) => {
    await api.deleteBusinessWorkflow(id)
    set((state) => ({ workflows: state.workflows.filter((wf) => wf.id !== id) }))
  },

  duplicateWorkflow: async (id) => {
    await api.duplicateBusinessWorkflow(id)
    await get().fetchWorkflows()
  },
}))
