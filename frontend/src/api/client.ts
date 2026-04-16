import axios from 'axios'

const envApiBase = String(import.meta.env.VITE_API_BASE_URL || '').trim()
const BASE = envApiBase || (import.meta.env.DEV ? 'http://localhost:8001' : '')
const http = axios.create({ baseURL: BASE, timeout: 30000 })
const H2O_LONG_TIMEOUT_MS = 10 * 60 * 1000

// ─── In-memory store for offline pipeline creation ──────────────────────────
const localPipelineStore: Record<string, any> = {}
const localMLOpsStore: Record<string, any> = {}
const localBusinessStore: Record<string, any> = {}

// ─── Mock data ───────────────────────────────────────────────────────────────
const mockPipelines = [
  {
    id: 'p1', name: 'Customer Data Sync', description: 'Sync customers from PostgreSQL to MongoDB',
    status: 'active', tags: ['crm', 'sync'], nodeCount: 4, nodes: [], edges: [],
    created_at: '2024-01-15T10:00:00Z', updated_at: '2024-03-20T14:30:00Z',
    last_execution: { id: 'e1', status: 'success', started_at: '2024-03-20T14:30:00Z', rows_processed: 12450 }
  },
  {
    id: 'p2', name: 'Sales Report ETL', description: 'Extract sales data, aggregate by region and load to warehouse',
    status: 'active', tags: ['sales', 'reporting'], nodeCount: 6, nodes: [], edges: [],
    created_at: '2024-02-10T09:00:00Z', updated_at: '2024-03-19T11:00:00Z',
    last_execution: { id: 'e2', status: 'success', started_at: '2024-03-19T11:00:00Z', rows_processed: 89320 }
  },
  {
    id: 'p3', name: 'Kafka Event Processing', description: 'Process real-time events from Kafka topic',
    status: 'active', tags: ['streaming', 'events'], nodeCount: 5, nodes: [], edges: [],
    created_at: '2024-03-01T08:00:00Z', updated_at: '2024-03-21T09:15:00Z',
    last_execution: { id: 'e3', status: 'failed', started_at: '2024-03-21T09:15:00Z', rows_processed: 0 }
  },
  {
    id: 'p4', name: 'S3 Data Lake Ingestion', description: 'Load parquet files from S3 into PostgreSQL',
    status: 'draft', tags: ['cloud', 's3'], nodeCount: 3, nodes: [], edges: [],
    created_at: '2024-03-15T12:00:00Z', updated_at: '2024-03-15T12:00:00Z',
    last_execution: null
  },
  {
    id: 'p5', name: 'API Data Collector', description: 'Collect data from multiple REST APIs and merge',
    status: 'inactive', tags: ['api', 'collect'], nodeCount: 7, nodes: [], edges: [],
    created_at: '2024-02-20T11:00:00Z', updated_at: '2024-03-18T16:00:00Z',
    last_execution: { id: 'e5', status: 'success', started_at: '2024-03-18T16:00:00Z', rows_processed: 5600 }
  },
]

const mockExecutions = [
  { id: 'e1', pipeline_id: 'p1', pipeline_name: 'Customer Data Sync', status: 'success',
    started_at: '2024-03-20T14:30:00Z', finished_at: '2024-03-20T14:32:15Z', duration: 135,
    rows_processed: 12450, triggered_by: 'schedule', logs: [] },
  { id: 'e2', pipeline_id: 'p2', pipeline_name: 'Sales Report ETL', status: 'success',
    started_at: '2024-03-19T11:00:00Z', finished_at: '2024-03-19T11:05:30Z', duration: 330,
    rows_processed: 89320, triggered_by: 'manual', logs: [] },
  { id: 'e3', pipeline_id: 'p3', pipeline_name: 'Kafka Event Processing', status: 'failed',
    started_at: '2024-03-21T09:15:00Z', finished_at: '2024-03-21T09:15:45Z', duration: 45,
    rows_processed: 0, triggered_by: 'schedule',
    error_message: 'Connection to Kafka broker refused: Connection timed out', logs: [] },
  { id: 'e4', pipeline_id: 'p1', pipeline_name: 'Customer Data Sync', status: 'success',
    started_at: '2024-03-19T14:30:00Z', finished_at: '2024-03-19T14:32:20Z', duration: 140,
    rows_processed: 11980, triggered_by: 'schedule', logs: [] },
  { id: 'e5', pipeline_id: 'p5', pipeline_name: 'API Data Collector', status: 'success',
    started_at: '2024-03-18T16:00:00Z', finished_at: '2024-03-18T16:01:00Z', duration: 60,
    rows_processed: 5600, triggered_by: 'manual', logs: [] },
  { id: 'e6', pipeline_id: 'p2', pipeline_name: 'Sales Report ETL', status: 'success',
    started_at: '2024-03-18T11:00:00Z', finished_at: '2024-03-18T11:06:00Z', duration: 360,
    rows_processed: 87100, triggered_by: 'schedule', logs: [] },
]

const mockStats = {
  total_pipelines: 5, active_pipelines: 3, total_executions: 24,
  successful_executions: 21, failed_executions: 3, running_executions: 0,
  total_rows_processed: 1284560, success_rate: 87.5
}

const mockMLOpsWorkflows = [
  {
    id: 'mw1',
    name: 'Demand Forecasting Pipeline',
    description: 'Sales demand forecasting workflow from staged ETL features',
    status: 'active',
    tags: ['forecasting', 'retail'],
    nodeCount: 6,
    nodes: [],
    edges: [],
    created_at: '2025-11-12T08:00:00Z',
    updated_at: '2026-03-28T06:30:00Z',
    last_run: {
      id: 'mr1',
      status: 'success',
      started_at: '2026-03-28T06:00:00Z',
      artifact_rows: 45200,
      model_version: 'v20260328060000',
      metrics: { accuracy: 0.912, rmse: 4.381, mape: 7.2 },
    },
  },
  {
    id: 'mw2',
    name: 'Customer Churn Training',
    description: 'Classification workflow with feature engineering and model deployment',
    status: 'draft',
    tags: ['classification', 'churn'],
    nodeCount: 7,
    nodes: [],
    edges: [],
    created_at: '2025-12-03T10:10:00Z',
    updated_at: '2026-03-24T09:12:00Z',
    last_run: null,
  },
]

const mockMLOpsRuns = [
  {
    id: 'mr1',
    workflow_id: 'mw1',
    workflow_name: 'Demand Forecasting Pipeline',
    status: 'success',
    started_at: '2026-03-28T06:00:00Z',
    finished_at: '2026-03-28T06:05:00Z',
    duration: 300,
    artifact_rows: 45200,
    model_version: 'v20260328060000',
    metrics: { accuracy: 0.912, rmse: 4.381, mape: 7.2, forecast_horizon_days: 30 },
    logs: [],
    triggered_by: 'manual',
  },
]

const mockBusinessWorkflows = [
  {
    id: 'bw1',
    name: 'Lead Qualification Workflow',
    description: 'Use ETL output + prompt decision + mail draft for sales qualification.',
    status: 'active',
    tags: ['business', 'lead', 'ai'],
    nodeCount: 6,
    nodes: [],
    edges: [],
    created_at: '2026-04-01T08:00:00Z',
    updated_at: '2026-04-04T09:20:00Z',
    last_run: {
      id: 'br1',
      status: 'success',
      started_at: '2026-04-04T09:00:00Z',
      model_name: 'gpt-oss20b',
      metrics: { total_rows: 240, node_count: 6 },
    },
  },
]

const mockBusinessRuns = [
  {
    id: 'br1',
    workflow_id: 'bw1',
    workflow_name: 'Lead Qualification Workflow',
    status: 'success',
    started_at: '2026-04-04T09:00:00Z',
    finished_at: '2026-04-04T09:00:38Z',
    duration: 38,
    metrics: { total_rows: 240, node_count: 6 },
    model_name: 'gpt-oss20b',
    logs: [],
    node_outputs: {},
    triggered_by: 'manual',
  },
]

async function safeGet<T>(fn: () => Promise<T>, fallback: T): Promise<T> {
  try { return await fn() } catch { return fallback }
}

function isOfflineError(error: unknown): boolean {
  if (!axios.isAxiosError(error)) return false
  if (error.response) return false
  const code = String(error.code || '')
  if (['ERR_NETWORK', 'ECONNABORTED', 'ECONNREFUSED', 'ENOTFOUND', 'EHOSTUNREACH'].includes(code)) {
    return true
  }
  const msg = String(error.message || '').toLowerCase()
  return msg.includes('network error') || msg.includes('failed to fetch')
}

// ─── API CLIENT ───────────────────────────────────────────────────────────────
const api = {

  // ── Pipelines ──────────────────────────────────────────────────────────────
  listPipelines: () => safeGet(async () => {
    const r = await http.get('/api/pipelines')
    return r.data
  }, [...mockPipelines, ...Object.values(localPipelineStore)]),

  getPipeline: async (id: string) => {
    try {
      const r = await http.get(`/api/pipelines/${id}`)
      return r.data
    } catch {
      // Check local store first (newly created pipelines)
      if (localPipelineStore[id]) return localPipelineStore[id]
      return mockPipelines.find(p => p.id === id) || {
        id, name: 'Untitled Pipeline', description: '',
        nodes: [], edges: [], status: 'draft', tags: [],
        created_at: new Date().toISOString(), updated_at: new Date().toISOString()
      }
    }
  },

  createPipeline: async (data: { name: string; description?: string }) => {
    try {
      const r = await http.post('/api/pipelines', data)
      const p = r.data
      localPipelineStore[p.id] = { ...p, nodes: [], edges: [], tags: [] }
      return p
    } catch {
      // Offline: create locally with full shape
      const id = `p_${Date.now()}`
      const p = {
        id,
        name: data.name || 'Untitled Pipeline',
        description: data.description || '',
        status: 'draft',
        nodes: [], edges: [], tags: [],
        nodeCount: 0,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        last_execution: null,
      }
      localPipelineStore[id] = p
      return p
    }
  },

  updatePipeline: async (id: string, data: Record<string, unknown>) => {
    // Keep local store in sync
    if (localPipelineStore[id]) {
      localPipelineStore[id] = { ...localPipelineStore[id], ...data, updated_at: new Date().toISOString() }
    }
    try {
      const r = await http.put(`/api/pipelines/${id}`, data)
      return r.data
    } catch {
      return data
    }
  },

  deletePipeline: async (id: string) => {
    delete localPipelineStore[id]
    try { await http.delete(`/api/pipelines/${id}`) } catch { /* offline */ }
  },

  duplicatePipeline: async (id: string) => {
    try {
      const r = await http.post(`/api/pipelines/${id}/duplicate`)
      return r.data
    } catch {
      const src = localPipelineStore[id] || mockPipelines.find(p => p.id === id)
      if (src) {
        const newId = `p_${Date.now()}`
        localPipelineStore[newId] = { ...src, id: newId, name: `${src.name} (Copy)`, status: 'draft' }
        return localPipelineStore[newId]
      }
      return { id: `p_${Date.now()}` }
    }
  },

  executePipeline: async (id: string) => {
    try {
      const r = await http.post(`/api/pipelines/${id}/execute`)
      return r.data
    } catch {
      // Signal to caller that backend is offline
      return { execution_id: `local_exec_${Date.now()}`, status: 'running', offline: true }
    }
  },

  detectSourceJsonFieldOptions: async (nodeType: string, config: Record<string, unknown>) => {
    try {
      const r = await http.post('/api/source/json-field-options', {
        node_type: nodeType,
        config,
      })
      return r.data
    } catch (err: any) {
      const detail = err?.response?.data?.detail
      const message = typeof detail === 'string'
        ? detail
        : String(err?.message || 'Failed to detect JSON fields from source')
      throw new Error(message)
    }
  },

  detectSourceFieldOptions: async (nodeType: string, config: Record<string, unknown>, maxRows = 200) => {
    try {
      const r = await http.post('/api/source/field-options', {
        node_type: nodeType,
        config,
        max_rows: maxRows,
      })
      return r.data
    } catch (err: any) {
      const detail = err?.response?.data?.detail
      const message = typeof detail === 'string'
        ? detail
        : String(err?.message || 'Failed to detect source fields')
      throw new Error(message)
    }
  },

  // ── MLOps Workflows ───────────────────────────────────────────────────────
  listMLOpsWorkflows: () => safeGet(async () => {
    const r = await http.get('/api/mlops/workflows')
    return r.data
  }, [...mockMLOpsWorkflows, ...Object.values(localMLOpsStore)]),

  getMLOpsWorkflow: async (id: string) => {
    try {
      const r = await http.get(`/api/mlops/workflows/${id}`)
      return r.data
    } catch {
      if (localMLOpsStore[id]) return localMLOpsStore[id]
      return mockMLOpsWorkflows.find((wf) => wf.id === id) || {
        id,
        name: 'Untitled MLOps Workflow',
        description: '',
        nodes: [],
        edges: [],
        status: 'draft',
        tags: [],
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }
    }
  },

  createMLOpsWorkflow: async (data: { name: string; description?: string }) => {
    try {
      const r = await http.post('/api/mlops/workflows', data)
      const wf = r.data
      localMLOpsStore[wf.id] = { ...wf, nodes: [], edges: [], tags: [] }
      return wf
    } catch {
      const id = `mw_${Date.now()}`
      const wf = {
        id,
        name: data.name || 'Untitled MLOps Workflow',
        description: data.description || '',
        status: 'draft',
        nodes: [],
        edges: [],
        tags: [],
        nodeCount: 0,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        last_run: null,
      }
      localMLOpsStore[id] = wf
      return wf
    }
  },

  updateMLOpsWorkflow: async (id: string, data: Record<string, unknown>) => {
    if (localMLOpsStore[id]) {
      localMLOpsStore[id] = { ...localMLOpsStore[id], ...data, updated_at: new Date().toISOString() }
    }
    try {
      const r = await http.put(`/api/mlops/workflows/${id}`, data)
      return r.data
    } catch {
      return data
    }
  },

  deleteMLOpsWorkflow: async (id: string) => {
    delete localMLOpsStore[id]
    try { await http.delete(`/api/mlops/workflows/${id}`) } catch { /* offline */ }
  },

  duplicateMLOpsWorkflow: async (id: string) => {
    try {
      const r = await http.post(`/api/mlops/workflows/${id}/duplicate`)
      return r.data
    } catch {
      const src = localMLOpsStore[id] || mockMLOpsWorkflows.find((wf) => wf.id === id)
      if (src) {
        const newId = `mw_${Date.now()}`
        localMLOpsStore[newId] = { ...src, id: newId, name: `${src.name} (Copy)`, status: 'draft' }
        return localMLOpsStore[newId]
      }
      return { id: `mw_${Date.now()}` }
    }
  },

  executeMLOpsWorkflow: async (id: string) => {
    try {
      const r = await http.post(`/api/mlops/workflows/${id}/execute`)
      return r.data
    } catch {
      return { run_id: `local_ml_run_${Date.now()}`, status: 'running', offline: true }
    }
  },

  listMLOpsRuns: (workflowId?: string) => safeGet(async () => {
    const params = workflowId ? { workflow_id: workflowId } : {}
    const r = await http.get('/api/mlops/runs', { params })
    return r.data
  }, workflowId ? mockMLOpsRuns.filter((r) => r.workflow_id === workflowId) : mockMLOpsRuns),

  getMLOpsRun: async (id: string) => {
    try {
      const r = await http.get(`/api/mlops/runs/${id}`)
      return r.data
    } catch {
      return mockMLOpsRuns.find((r) => r.id === id) || { id, status: 'success', logs: [], artifact_rows: 0, metrics: {} }
    }
  },

  getMLOpsFeatureProfile: async (workflowId: string, payload?: { node_id?: string; sample_size?: number }) => {
    try {
      const r = await http.post(`/api/mlops/workflows/${workflowId}/feature-profile`, payload || {})
      return r.data
    } catch {
      return {
        workflow_id: workflowId,
        available_operations: [],
        profile: { row_count: 0, sample_size: 0, columns: [], recommendations: [] },
        sample_rows: [],
      }
    }
  },

  getMLOpsH2OHealth: async () => {
    try {
      const r = await http.get('/api/mlops/h2o/health')
      return r.data
    } catch {
      return { status: 'unavailable', h2o_available: false, detail: 'Backend unavailable' }
    }
  },

  getMLOpsH2OSourceColumns: async (payload: Record<string, unknown>) => {
    const r = await http.post('/api/mlops/h2o/source-columns', payload)
    return r.data
  },

  listMLOpsH2ORuns: async (workflowId?: string, limit = 50) => {
    const params: Record<string, unknown> = { limit }
    if (workflowId) params.workflow_id = workflowId
    try {
      const r = await http.get('/api/mlops/h2o/runs', { params })
      return Array.isArray(r.data) ? r.data : []
    } catch {
      return []
    }
  },

  getMLOpsH2ORun: async (runId: string) => {
    const r = await http.get(`/api/mlops/h2o/runs/${runId}`)
    return r.data
  },

  updateMLOpsH2ORun: async (runId: string, payload: { label?: string | null }) => {
    const r = await http.patch(`/api/mlops/h2o/runs/${runId}`, payload)
    return r.data
  },

  deleteMLOpsH2ORun: async (runId: string) => {
    await http.delete(`/api/mlops/h2o/runs/${runId}`)
    return true
  },

  trainMLOpsH2O: async (payload: Record<string, unknown>) => {
    const r = await http.post('/api/mlops/h2o/train', payload, { timeout: H2O_LONG_TIMEOUT_MS })
    return r.data
  },

  predictMLOpsH2OSingle: async (payload: Record<string, unknown>) => {
    const r = await http.post('/api/mlops/h2o/predict/single', payload)
    return r.data
  },

  predictMLOpsH2OBatch: async (payload: Record<string, unknown>) => {
    const r = await http.post('/api/mlops/h2o/predict/batch', payload, { timeout: H2O_LONG_TIMEOUT_MS })
    return r.data
  },

  evaluateMLOpsH2O: async (payload: Record<string, unknown>) => {
    const r = await http.post('/api/mlops/h2o/evaluate', payload, { timeout: H2O_LONG_TIMEOUT_MS })
    return r.data
  },

  listOllamaModels: async () => {
    try {
      const r = await http.get('/api/ollama/models')
      const rows = Array.isArray(r.data?.models) ? r.data.models : []
      if (rows.length > 0) return rows
    } catch { /* offline */ }
    return [
      { value: 'gpt-oss20b', label: 'GPT-OSS20B (Local Ollama)', provider: 'ollama', available: false },
      { value: 'gpt-0ss20b', label: 'GPT-0SS20B (Local Ollama)', provider: 'ollama', available: false },
      { value: 'gpt-oss20b-cloud', label: 'GPT-OSS20B Cloud', provider: 'cloud', available: true },
      { value: 'gpt-0ss20b-cloud', label: 'GPT-0SS20B Cloud', provider: 'cloud', available: true },
      { value: 'llama3.1:8b', label: 'llama3.1:8b', provider: 'ollama', available: false },
      { value: 'qwen2.5:7b', label: 'qwen2.5:7b', provider: 'ollama', available: false },
      { value: 'mistral:7b', label: 'mistral:7b', provider: 'ollama', available: false },
    ]
  },

  listDashboards: () => safeGet(async () => {
    const r = await http.get('/api/dashboards')
    return r.data
  }, []),

  saveDashboardThumbnail: async (id: string, thumbnail: string) => {
    const r = await http.put(`/api/dashboards/${id}/thumbnail`, { thumbnail })
    return r.data
  },

  // ── Business Workflows ────────────────────────────────────────────────────
  listBusinessWorkflows: () => safeGet(async () => {
    const r = await http.get('/api/business/workflows')
    return r.data
  }, [...mockBusinessWorkflows, ...Object.values(localBusinessStore)]),

  getBusinessWorkflow: async (id: string) => {
    try {
      const r = await http.get(`/api/business/workflows/${id}`)
      return r.data
    } catch {
      if (localBusinessStore[id]) return localBusinessStore[id]
      return mockBusinessWorkflows.find((wf) => wf.id === id) || {
        id,
        name: 'Untitled Business Workflow',
        description: '',
        nodes: [],
        edges: [],
        status: 'draft',
        tags: [],
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }
    }
  },

  createBusinessWorkflow: async (data: { name: string; description?: string }) => {
    try {
      const r = await http.post('/api/business/workflows', data)
      const wf = r.data
      localBusinessStore[wf.id] = { ...wf, nodes: [], edges: [], tags: [] }
      return wf
    } catch (error) {
      if (!isOfflineError(error)) throw error
      const id = `bw_${Date.now()}`
      const wf = {
        id,
        name: data.name || 'Untitled Business Workflow',
        description: data.description || '',
        status: 'draft',
        nodes: [],
        edges: [],
        tags: [],
        nodeCount: 0,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        last_run: null,
      }
      localBusinessStore[id] = wf
      return wf
    }
  },

  updateBusinessWorkflow: async (id: string, data: Record<string, unknown>) => {
    if (localBusinessStore[id]) {
      localBusinessStore[id] = { ...localBusinessStore[id], ...data, updated_at: new Date().toISOString() }
    }
    try {
      const r = await http.put(`/api/business/workflows/${id}`, data)
      return r.data
    } catch (error) {
      if (!isOfflineError(error)) throw error
      return data
    }
  },

  deleteBusinessWorkflow: async (id: string) => {
    delete localBusinessStore[id]
    try { await http.delete(`/api/business/workflows/${id}`) } catch { /* offline */ }
  },

  duplicateBusinessWorkflow: async (id: string) => {
    try {
      const r = await http.post(`/api/business/workflows/${id}/duplicate`)
      return r.data
    } catch {
      const src = localBusinessStore[id] || mockBusinessWorkflows.find((wf) => wf.id === id)
      if (src) {
        const newId = `bw_${Date.now()}`
        localBusinessStore[newId] = { ...src, id: newId, name: `${src.name} (Copy)`, status: 'draft' }
        return localBusinessStore[newId]
      }
      return { id: `bw_${Date.now()}` }
    }
  },

  executeBusinessWorkflow: async (id: string) => {
    try {
      const r = await http.post(`/api/business/workflows/${id}/execute`)
      return r.data
    } catch (error) {
      if (!isOfflineError(error)) throw error
      const target = BASE || 'same-origin backend'
      throw new Error(`Business API is unreachable at ${target}. Start backend and retry.`)
    }
  },

  listBusinessRuns: (workflowId?: string) => safeGet(async () => {
    const params = workflowId ? { workflow_id: workflowId } : {}
    const r = await http.get('/api/business/runs', { params })
    return r.data
  }, workflowId ? mockBusinessRuns.filter((r) => r.workflow_id === workflowId) : mockBusinessRuns),

  getBusinessRun: async (id: string) => {
    try {
      const r = await http.get(`/api/business/runs/${id}`)
      return r.data
    } catch (error) {
      if (!isOfflineError(error)) throw error
      return mockBusinessRuns.find((r) => r.id === id) || { id, status: 'success', logs: [], metrics: {} }
    }
  },

  // ── Executions ─────────────────────────────────────────────────────────────
  listExecutions: (pipelineId?: string) => safeGet(async () => {
    const params = pipelineId ? { pipeline_id: pipelineId } : {}
    const r = await http.get('/api/executions', { params })
    return r.data
  }, pipelineId ? mockExecutions.filter(e => e.pipeline_id === pipelineId) : mockExecutions),

  getExecution: async (id: string) => {
    try {
      const r = await http.get(`/api/executions/${id}`, {
        params: { include_node_results: false },
      })
      return r.data
    } catch {
      return mockExecutions.find(e => e.id === id) || { id, status: 'success', logs: [], rows_processed: 0 }
    }
  },

  deleteExecution: async (id: string) => {
    try { await http.delete(`/api/executions/${id}`) } catch { /* offline */ }
  },

  // ── Credentials ────────────────────────────────────────────────────────────
  listCredentials: () => safeGet(async () => {
    const r = await http.get('/api/credentials')
    return r.data
  }, [
    { id: 'c1', name: 'Production PostgreSQL', type: 'postgres', created_at: '2024-01-01T00:00:00Z' },
    { id: 'c2', name: 'MongoDB Atlas', type: 'mongodb', created_at: '2024-01-10T00:00:00Z' },
    { id: 'c3', name: 'AWS S3 Production', type: 's3', created_at: '2024-02-01T00:00:00Z' },
    { id: 'c4', name: 'Elasticsearch Cloud', type: 'elasticsearch', created_at: '2024-03-01T00:00:00Z' },
  ]),

  createCredential: async (data: Record<string, unknown>) => {
    try { const r = await http.post('/api/credentials', data); return r.data }
    catch { return { id: `c_${Date.now()}`, ...data } }
  },

  deleteCredential: async (id: string) => {
    try { await http.delete(`/api/credentials/${id}`) } catch { /* offline */ }
  },

  // ── Stats ──────────────────────────────────────────────────────────────────
  getStats: () => safeGet(async () => {
    const r = await http.get('/api/stats')
    return r.data
  }, mockStats),

  listTemplates: () => safeGet(async () => {
    const r = await http.get('/api/templates'); return r.data
  }, []),

  // ── File download ───────────────────────────────────────────────────────────
  downloadOutputFile: (serverPath: string) => {
    const url = `${BASE}/api/download?path=${encodeURIComponent(serverPath)}`
    const a = document.createElement('a')
    a.href = url
    a.download = serverPath.split('/').pop() || 'output'
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
  },
}

export default api
