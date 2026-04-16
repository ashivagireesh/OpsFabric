import { useEffect, useMemo, useState, type CSSProperties, type Dispatch, type SetStateAction } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import {
  Alert,
  Button,
  Dropdown,
  Form,
  Input,
  InputNumber,
  Modal,
  Select,
  Space,
  Spin,
  Switch,
  Tabs,
  Tag,
  Tooltip,
  Typography,
  notification,
} from 'antd'
import {
  ArrowLeftOutlined,
  SaveOutlined,
  PlayCircleOutlined,
  LoadingOutlined,
  CheckCircleFilled,
  CloseCircleFilled,
  EllipsisOutlined,
  ScheduleOutlined,
  DeploymentUnitOutlined,
  BarChartOutlined,
  ReloadOutlined,
  DownloadOutlined,
} from '@ant-design/icons'
import { ReactFlowProvider } from 'reactflow'
import ReactECharts from 'echarts-for-react'
import api from '../../api/client'
import { useMLOpsWorkflowStore } from '../../store/mlopsStore'
import {
  WORKFLOW_CONNECTOR_OPTIONS,
  type WorkflowConnectorType,
} from '../../constants/workflowConnectors'
import MLNodePalette from '../../components/mlops/MLNodePalette'
import MLWorkflowCanvas from '../../components/mlops/MLWorkflowCanvas'
import MLConfigDrawer from '../../components/mlops/MLConfigDrawer'
import MLExecutionPanel from '../../components/mlops/MLExecutionPanel'

const { Text } = Typography

type H2OSourceType = 'pipeline' | 'file' | 'sample' | 'workflow' | 'rows'
type H2OTaskMode = 'auto' | 'classification' | 'regression' | 'clustering' | 'forecasting'

interface H2OSourceConfig {
  source_type: H2OSourceType
  pipeline_id?: string
  pipeline_node_id?: string
  file_path: string
  dataset: string
  workflow_id?: string
  use_workflow_preprocessing: boolean
  rows_json: string
}

interface H2ORunRow {
  id: string
  status?: string
  model_id?: string
  label?: string
  task?: string
  target_column?: string
  last_evaluated_at?: string
  last_evaluation_target_column?: string
  output_fields?: string[]
  created_at?: string
}

interface H2OHealth {
  status?: string
  h2o_available?: boolean
  detail?: string
  version?: string
  nodes?: number
}

interface H2OSourceColumnsProfile {
  columns?: string[]
  numeric_columns?: string[]
  categorical_columns?: string[]
  datetime_columns?: string[]
  text_columns?: string[]
  target_suggestion?: string
  row_count?: number
  profiled_row_count?: number
}

function normalizeH2OTaskMode(raw: unknown): H2OTaskMode {
  const v = String(raw || '').trim().toLowerCase()
  if (!v) return 'auto'
  if (v.includes('forecast')) return 'forecasting'
  if (v.includes('cluster')) return 'clustering'
  if (v === 'classification') return 'classification'
  if (v === 'regression') return 'regression'
  return 'auto'
}

const SAMPLE_DATASET_OPTIONS = [
  { label: 'Sales', value: 'sales' },
  { label: 'Web Analytics', value: 'web_analytics' },
  { label: 'Financials', value: 'financials' },
  { label: 'Employees', value: 'employees' },
  { label: 'Supply Chain', value: 'supply_chain' },
  { label: 'Customer', value: 'customer' },
]

const H2O_SOURCE_OPTIONS: { label: string; value: H2OSourceType }[] = [
  { label: 'ETL Pipeline Output', value: 'pipeline' },
  { label: 'CSV/Excel/Parquet File', value: 'file' },
  { label: 'Sample Dataset', value: 'sample' },
  { label: 'MLOps Workflow Source', value: 'workflow' },
  { label: 'Inline JSON Rows', value: 'rows' },
]

const H2O_EXAMPLE_ROWS = Array.from({ length: 36 }, (_, idx) => {
  const monthNo = (idx % 12) + 1
  const month = `2026-${String(monthNo).padStart(2, '0')}`
  const region = ['North', 'South', 'West'][idx % 3]
  const visitors = 36000 + idx * 900 + (idx % 5) * 550
  const adSpend = 12000 + idx * 320 + (idx % 4) * 140
  const conversionRate = 2.1 + ((idx * 0.07) % 1.4)
  const sales = Math.round(visitors * (conversionRate / 100) * (68 + (idx % 7) * 3) + adSpend * 1.2)
  return {
    month,
    region,
    visitors,
    ad_spend: adSpend,
    conversion_rate: Number(conversionRate.toFixed(2)),
    sales,
  }
})

const H2O_EXAMPLE_SOURCE_JSON = JSON.stringify(H2O_EXAMPLE_ROWS, null, 2)

function emptySourceConfig(workflowId?: string): H2OSourceConfig {
  return {
    source_type: 'pipeline',
    pipeline_id: undefined,
    pipeline_node_id: '',
    file_path: '',
    dataset: 'sales',
    workflow_id: workflowId,
    use_workflow_preprocessing: true,
    rows_json: '[\n  {\n    "feature_1": 10,\n    "feature_2": 25\n  }\n]',
  }
}

function parseCsvList(raw: string): string[] {
  return String(raw || '')
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean)
}

function maybeValue(raw: string): string | undefined {
  const value = String(raw || '').trim()
  return value ? value : undefined
}

function firstSelected(values: string[]): string | undefined {
  if (!Array.isArray(values) || values.length === 0) return undefined
  return maybeValue(String(values[0] || ''))
}

function formatLocalDateTime(value: unknown): string {
  const raw = String(value || '').trim()
  if (!raw) return '-'
  try {
    return new Date(raw).toLocaleString()
  } catch {
    return raw
  }
}

function errorMessage(err: unknown): string {
  const maybe = err as {
    response?: { data?: { detail?: unknown } }
    message?: string
  }
  const detail = maybe?.response?.data?.detail
  if (typeof detail === 'string' && detail.trim()) return detail
  if (Array.isArray(detail) && detail.length > 0) {
    return String(detail[0])
  }
  if (maybe?.message) return maybe.message
  return 'Request failed'
}

type AnyRow = Record<string, unknown>

function toCsvCell(value: unknown): string {
  if (value === null || value === undefined) return ''
  const text = typeof value === 'string'
    ? value
    : (typeof value === 'number' || typeof value === 'boolean')
      ? String(value)
      : JSON.stringify(value)
  const escaped = text.replace(/"/g, '""')
  return `"${escaped}"`
}

function downloadRowsAsCsv(rows: AnyRow[], baseName: string) {
  if (!Array.isArray(rows) || rows.length === 0) return
  const headers = Array.from(new Set(rows.flatMap((row) => Object.keys(row || {}))))
  const lines: string[] = [headers.map((h) => toCsvCell(h)).join(',')]
  rows.forEach((row) => {
    lines.push(headers.map((h) => toCsvCell((row || {})[h])).join(','))
  })
  const csv = lines.join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
  link.href = url
  link.download = `${baseName}-${timestamp}.csv`
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
  URL.revokeObjectURL(url)
}

function toNum(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string') {
    const n = Number(value)
    if (Number.isFinite(n)) return n
  }
  return null
}

function buildSinglePredictionChartOption(response: Record<string, unknown> | null, taskMode: H2OTaskMode = 'auto') {
  const pred = response?.prediction
  if (!pred || typeof pred !== 'object' || Array.isArray(pred)) return null

  const predObj = pred as AnyRow
  const numericEntries = Object.entries(predObj)
    .map(([key, value]) => ({ key, value: toNum(value) }))
    .filter((item) => item.value !== null) as Array<{ key: string; value: number }>
  const predictedLabel = String(predObj.predict ?? predObj.cluster ?? '').trim()

  if (taskMode === 'classification' || taskMode === 'clustering') {
    const probEntries = numericEntries.filter((entry) => entry.key !== 'predict')
    if (probEntries.length > 0) {
      return {
        backgroundColor: 'transparent',
        tooltip: { trigger: 'axis' },
        grid: { left: 30, right: 16, top: 34, bottom: 34, containLabel: true },
        xAxis: { type: 'category', data: probEntries.map((e) => e.key), axisLabel: { color: 'var(--app-text-subtle)' } },
        yAxis: { type: 'value', axisLabel: { color: 'var(--app-text-subtle)' } },
        series: [{ type: 'bar', data: probEntries.map((e) => e.value), itemStyle: { color: taskMode === 'clustering' ? '#f59e0b' : '#7c3aed' } }],
        title: { text: taskMode === 'clustering' ? 'Cluster Score' : 'Class Probability', left: 'center', textStyle: { color: 'var(--app-text)', fontSize: 12 } },
      }
    }
    if (predictedLabel) {
      return {
        backgroundColor: 'transparent',
        tooltip: { trigger: 'axis' },
        grid: { left: 30, right: 16, top: 34, bottom: 34, containLabel: true },
        xAxis: { type: 'category', data: [predictedLabel], axisLabel: { color: 'var(--app-text-subtle)' } },
        yAxis: { type: 'value', axisLabel: { color: 'var(--app-text-subtle)' } },
        series: [{ type: 'bar', data: [1], itemStyle: { color: taskMode === 'clustering' ? '#f59e0b' : '#7c3aed' } }],
        title: { text: taskMode === 'clustering' ? 'Predicted Cluster' : 'Predicted Class', left: 'center', textStyle: { color: 'var(--app-text)', fontSize: 12 } },
      }
    }
    return null
  }

  if (numericEntries.length === 0) return null
  return {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    grid: { left: 30, right: 16, top: 30, bottom: 34, containLabel: true },
    xAxis: { type: 'category', data: numericEntries.map((e) => e.key), axisLabel: { color: 'var(--app-text-subtle)' } },
    yAxis: { type: 'value', axisLabel: { color: 'var(--app-text-subtle)' } },
    series: [
      {
        type: taskMode === 'forecasting' ? 'line' : 'bar',
        smooth: taskMode === 'forecasting',
        data: numericEntries.map((e) => e.value),
        itemStyle: { color: '#22c55e' },
      },
    ],
    title: { text: taskMode === 'forecasting' ? 'Forecast Output' : 'Regression Output', left: 'center', textStyle: { color: 'var(--app-text)', fontSize: 12 } },
  }
}

function buildBatchPredictionChartOption(response: Record<string, unknown> | null, taskMode: H2OTaskMode = 'auto') {
  const list = Array.isArray(response?.predictions) ? (response?.predictions as AnyRow[]) : []
  if (list.length === 0) return null

  if (taskMode === 'classification' || taskMode === 'clustering') {
    const bucket = new Map<string, number>()
    list.slice(0, 2000).forEach((row) => {
      const label = String((row.predict ?? row.cluster ?? row.prediction ?? 'unknown') as unknown).trim() || 'unknown'
      bucket.set(label, (bucket.get(label) || 0) + 1)
    })
    const top = Array.from(bucket.entries()).sort((a, b) => b[1] - a[1]).slice(0, 20)
    if (top.length === 0) return null
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      grid: { left: 30, right: 16, top: 30, bottom: 44, containLabel: true },
      xAxis: { type: 'category', data: top.map((t) => t[0]), axisLabel: { color: 'var(--app-text-subtle)', rotate: 25 } },
      yAxis: { type: 'value', axisLabel: { color: 'var(--app-text-subtle)' } },
      series: [{ type: 'bar', data: top.map((t) => t[1]), itemStyle: { color: taskMode === 'clustering' ? '#f59e0b' : '#7c3aed' } }],
      title: { text: taskMode === 'clustering' ? 'Cluster Distribution' : 'Class Distribution', left: 'center', textStyle: { color: 'var(--app-text)', fontSize: 12 } },
    }
  }

  const first = list[0] || {}
  const candidateKeys = Object.keys(first).filter((k) => toNum(first[k]) !== null)
  if (candidateKeys.length === 0) return null
  const key = candidateKeys.includes('predict') ? 'predict' : candidateKeys[0]
  const points = list.slice(0, 360).map((row) => toNum(row[key]))
  return {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    grid: { left: 30, right: 16, top: 30, bottom: 34, containLabel: true },
    xAxis: { type: 'category', data: points.map((_, i) => i + 1), axisLabel: { color: 'var(--app-text-subtle)' } },
    yAxis: { type: 'value', axisLabel: { color: 'var(--app-text-subtle)' } },
    series: [{
      type: 'line',
      smooth: true,
      data: points,
      lineStyle: { color: taskMode === 'forecasting' ? '#f59e0b' : '#2563eb' },
    }],
    title: {
      text: taskMode === 'forecasting' ? `Forecast Trend (${key})` : `Prediction Trend (${key})`,
      left: 'center',
      textStyle: { color: 'var(--app-text)', fontSize: 12 },
    },
  }
}

function buildEvaluateChartOption(response: Record<string, unknown> | null, taskMode: H2OTaskMode = 'auto') {
  const avp = Array.isArray(response?.actual_vs_predicted_preview)
    ? (response?.actual_vs_predicted_preview as AnyRow[])
    : []
  const predictionPreview = Array.isArray(response?.prediction_preview)
    ? (response?.prediction_preview as AnyRow[])
    : []

  if (taskMode === 'clustering') {
    const bucket = new Map<string, number>()
    const sourceRows = avp.length > 0 ? avp : predictionPreview
    sourceRows.slice(0, 4000).forEach((row) => {
      const label = String((row.predicted ?? row.predict ?? row.cluster ?? row.prediction ?? 'unknown') as unknown).trim() || 'unknown'
      bucket.set(label, (bucket.get(label) || 0) + 1)
    })
    const top = Array.from(bucket.entries()).sort((a, b) => b[1] - a[1]).slice(0, 20)
    if (top.length === 0) return null
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      grid: { left: 30, right: 16, top: 34, bottom: 44, containLabel: true },
      xAxis: { type: 'category', data: top.map((t) => t[0]), axisLabel: { color: 'var(--app-text-subtle)', rotate: 25 } },
      yAxis: { type: 'value', axisLabel: { color: 'var(--app-text-subtle)' } },
      series: [{ type: 'bar', data: top.map((t) => t[1]), itemStyle: { color: '#f59e0b' } }],
      title: { text: 'Cluster Distribution', left: 'center', textStyle: { color: 'var(--app-text)', fontSize: 12 } },
    }
  }

  if (avp.length === 0) return null
  const actualNums = avp.map((r) => toNum(r.actual))
  const predNums = avp.map((r) => toNum(r.predicted))
  const numeric = actualNums.every((v) => v !== null) && predNums.every((v) => v !== null)

  if (numeric) {
    const points = avp.slice(0, 400)
    const isForecast = taskMode === 'forecasting'
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { data: ['Actual', 'Predicted'], top: 6, textStyle: { color: 'var(--app-text-subtle)' } },
      grid: { left: 30, right: 16, top: 34, bottom: 34, containLabel: true },
      xAxis: { type: 'category', data: points.map((r) => r.index), axisLabel: { color: 'var(--app-text-subtle)' } },
      yAxis: { type: 'value', axisLabel: { color: 'var(--app-text-subtle)' } },
      series: [
        { name: 'Actual', type: 'line', smooth: true, data: points.map((r) => toNum(r.actual)), lineStyle: { color: '#059669' } },
        { name: 'Predicted', type: 'line', smooth: true, data: points.map((r) => toNum(r.predicted)), lineStyle: { color: isForecast ? '#f59e0b' : '#2563eb' } },
      ],
      title: {
        text: isForecast ? 'Forecast vs Actual' : 'Actual vs Predicted',
        left: 'center',
        textStyle: { color: 'var(--app-text)', fontSize: 12 },
      },
    }
  }

  if (taskMode === 'classification' || taskMode === 'auto') {
    const total = avp.length
    const matched = avp.reduce((acc, r) => (r.matched ? acc + 1 : acc), 0)
    const mismatched = Math.max(0, total - matched)
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      grid: { left: 30, right: 16, top: 34, bottom: 34, containLabel: true },
      xAxis: { type: 'category', data: ['Matched', 'Mismatched'], axisLabel: { color: 'var(--app-text-subtle)' } },
      yAxis: { type: 'value', axisLabel: { color: 'var(--app-text-subtle)' } },
      series: [{ type: 'bar', data: [matched, mismatched], itemStyle: { color: '#7c3aed' } }],
      title: { text: 'Classification Match', left: 'center', textStyle: { color: 'var(--app-text)', fontSize: 12 } },
    }
  }

  const bucket = new Map<string, number>()
  avp.slice(0, 2000).forEach((row) => {
    const label = String((row.predicted ?? 'unknown') as unknown).trim() || 'unknown'
    bucket.set(label, (bucket.get(label) || 0) + 1)
  })
  const top = Array.from(bucket.entries()).sort((a, b) => b[1] - a[1]).slice(0, 20)
  if (top.length === 0) return null
  return {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    grid: { left: 30, right: 16, top: 34, bottom: 44, containLabel: true },
    xAxis: { type: 'category', data: top.map((t) => t[0]), axisLabel: { color: 'var(--app-text-subtle)', rotate: 25 } },
    yAxis: { type: 'value', axisLabel: { color: 'var(--app-text-subtle)' } },
    series: [{ type: 'bar', data: top.map((t) => t[1]), itemStyle: { color: '#7c3aed' } }],
    title: { text: 'Predicted Label Distribution', left: 'center', textStyle: { color: 'var(--app-text)', fontSize: 12 } },
  }
}

export default function MLOpsEditor() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const {
    workflow,
    loadWorkflow,
    saveWorkflow,
    executeWorkflow,
    isDirty,
    isExecuting,
    selectedNodeId,
    setSelectedNode,
    resetCanvas,
    runLogs,
    connectorType,
    setConnectorType,
  } = useMLOpsWorkflowStore()

  const [saving, setSaving] = useState(false)
  const [scheduleModal, setScheduleModal] = useState(false)
  const [workflowName, setWorkflowName] = useState('')
  const [form] = Form.useForm()

  const [h2oModalOpen, setH2OModalOpen] = useState(false)
  const [h2oHealth, setH2OHealth] = useState<H2OHealth | null>(null)
  const [h2oHealthLoading, setH2OHealthLoading] = useState(false)
  const [pipelineOptions, setPipelineOptions] = useState<Array<{ value: string; label: string }>>([])
  const [pipelinesLoading, setPipelinesLoading] = useState(false)
  const [h2oRuns, setH2ORuns] = useState<H2ORunRow[]>([])
  const [h2oRunsLoading, setH2ORunsLoading] = useState(false)
  const [selectedH2ORun, setSelectedH2ORun] = useState<string | undefined>(undefined)
  const [selectedH2ORunDetail, setSelectedH2ORunDetail] = useState<Record<string, unknown> | null>(null)

  const [trainSource, setTrainSource] = useState<H2OSourceConfig>(() => emptySourceConfig(id))
  const [trainTargetColumns, setTrainTargetColumns] = useState<string[]>([])
  const [trainFeatureColumns, setTrainFeatureColumns] = useState<string[]>([])
  const [trainExcludeColumns, setTrainExcludeColumns] = useState<string[]>([])
  const [trainTask, setTrainTask] = useState<H2OTaskMode>('auto')
  const [trainRatio, setTrainRatio] = useState(0.8)
  const [trainMaxModels, setTrainMaxModels] = useState(20)
  const [trainMaxRuntime, setTrainMaxRuntime] = useState(300)
  const [trainSeed, setTrainSeed] = useState(42)
  const [trainNFolds, setTrainNFolds] = useState(5)
  const [trainBalanceClasses, setTrainBalanceClasses] = useState(false)
  const [trainIncludeAlgos, setTrainIncludeAlgos] = useState('')
  const [trainExcludeAlgos, setTrainExcludeAlgos] = useState('')
  const [trainProjectName, setTrainProjectName] = useState('')
  const [trainLoading, setTrainLoading] = useState(false)
  const [trainResponse, setTrainResponse] = useState<Record<string, unknown> | null>(null)

  const [singleRowJson, setSingleRowJson] = useState('{\n  "feature_1": 10,\n  "feature_2": 25\n}')
  const [singlePredictLoading, setSinglePredictLoading] = useState(false)
  const [singlePredictResponse, setSinglePredictResponse] = useState<Record<string, unknown> | null>(null)

  const [batchSource, setBatchSource] = useState<H2OSourceConfig>(() => ({ ...emptySourceConfig(id), source_type: 'rows' }))
  const [batchMaxRows, setBatchMaxRows] = useState(50000)
  const [batchPredictLoading, setBatchPredictLoading] = useState(false)
  const [batchPredictResponse, setBatchPredictResponse] = useState<Record<string, unknown> | null>(null)

  const [evaluateSource, setEvaluateSource] = useState<H2OSourceConfig>(() => ({ ...emptySourceConfig(id), source_type: 'rows' }))
  const [evaluateTargetColumns, setEvaluateTargetColumns] = useState<string[]>([])
  const [evaluateMaxRows, setEvaluateMaxRows] = useState(50000)
  const [evaluateLoading, setEvaluateLoading] = useState(false)
  const [evaluateResponse, setEvaluateResponse] = useState<Record<string, unknown> | null>(null)
  const [singleRowTouched, setSingleRowTouched] = useState(false)
  const [evaluateTargetTouched, setEvaluateTargetTouched] = useState(false)
  const [trainSourceColumns, setTrainSourceColumns] = useState<string[]>([])
  const [trainSourceColumnsLoading, setTrainSourceColumnsLoading] = useState(false)
  const [trainSourceColumnsProfile, setTrainSourceColumnsProfile] = useState<H2OSourceColumnsProfile | null>(null)
  const [trainSourceColumnsError, setTrainSourceColumnsError] = useState('')
  const [evaluateSourceColumns, setEvaluateSourceColumns] = useState<string[]>([])
  const [evaluateSourceColumnsLoading, setEvaluateSourceColumnsLoading] = useState(false)
  const [evaluateSourceColumnsProfile, setEvaluateSourceColumnsProfile] = useState<H2OSourceColumnsProfile | null>(null)
  const [evaluateSourceColumnsError, setEvaluateSourceColumnsError] = useState('')

  const [h2oError, setH2OError] = useState('')
  const [h2oActiveTab, setH2OActiveTab] = useState('train')
  const [runLabelDraft, setRunLabelDraft] = useState('')
  const [h2oRunSearch, setH2ORunSearch] = useState('')
  const [h2oRunPatchLoading, setH2ORunPatchLoading] = useState(false)
  const [h2oRunDeleteLoading, setH2ORunDeleteLoading] = useState(false)

  useEffect(() => {
    if (!id) return
    resetCanvas()
    void loadWorkflow(id).then(() => {
      const state = useMLOpsWorkflowStore.getState()
      setWorkflowName(state.workflow?.name || 'Untitled MLOps Workflow')
    })
  }, [id, loadWorkflow, resetCanvas])

  useEffect(() => {
    if (workflow?.name) setWorkflowName(workflow.name)
  }, [workflow?.name])

  useEffect(() => {
    if (!isDirty) return
    const timer = setTimeout(async () => {
      await saveWorkflow()
    }, 2500)
    return () => clearTimeout(timer)
  }, [isDirty, saveWorkflow])

  useEffect(() => {
    if (!workflow?.id) return
    setTrainSource((prev) => ({ ...prev, workflow_id: workflow.id }))
    setBatchSource((prev) => ({ ...prev, workflow_id: workflow.id }))
    setEvaluateSource((prev) => ({ ...prev, workflow_id: workflow.id }))
  }, [workflow?.id])

  const h2oRunOptions = useMemo(() => (
    h2oRuns.map((run) => ({
      value: run.id,
      label: `${run.label ? `${run.label} · ` : ''}${run.id.slice(0, 8)} · ${run.status || 'unknown'}${run.model_id ? ` · ${run.model_id}` : ''}`,
    }))
  ), [h2oRuns])

  const filteredH2ORuns = useMemo(() => {
    const q = String(h2oRunSearch || '').trim().toLowerCase()
    if (!q) return h2oRuns
    return h2oRuns.filter((run) => {
      const hay = [
        run.id,
        run.label,
        run.status,
        run.model_id,
        run.task,
        run.target_column,
        run.last_evaluation_target_column,
      ]
        .map((value) => String(value || '').toLowerCase())
        .join(' ')
      return hay.includes(q)
    })
  }, [h2oRuns, h2oRunSearch])

  const cloneSourceConfig = (source: H2OSourceConfig): H2OSourceConfig => ({
    ...source,
    workflow_id: source.workflow_id || workflow?.id,
    rows_json: String(source.rows_json || ''),
  })

  const runFeatureColumns = useMemo(() => {
    const raw = selectedH2ORunDetail?.feature_columns
    if (!Array.isArray(raw)) return []
    return raw
      .map((v) => String(v || '').trim())
      .filter(Boolean)
      .slice(0, 20)
  }, [selectedH2ORunDetail])
  const selectedRunTask = maybeValue(String(selectedH2ORunDetail?.task || ''))
  const selectedRunRequestedTask = maybeValue(String((selectedH2ORunDetail?.config as Record<string, unknown> | undefined)?.task || ''))
  const selectedRunTarget = maybeValue(String(selectedH2ORunDetail?.target_column || ''))
  const activeChartTaskMode = useMemo(
    () => normalizeH2OTaskMode(selectedRunRequestedTask || selectedRunTask || trainTask),
    [selectedRunRequestedTask, selectedRunTask, trainTask],
  )
  const singlePredictionRows = useMemo(() => {
    const preview = Array.isArray(singlePredictResponse?.prediction_preview) ? (singlePredictResponse?.prediction_preview as AnyRow[]) : []
    if (preview.length > 0) return preview
    const input = singlePredictResponse?.input
    const pred = singlePredictResponse?.prediction
    if (input && typeof input === 'object' && !Array.isArray(input) && pred && typeof pred === 'object' && !Array.isArray(pred)) {
      const merged: AnyRow = { ...(input as AnyRow) }
      Object.entries(pred as AnyRow).forEach(([k, v]) => { merged[`_prediction_${k}`] = v })
      return [merged]
    }
    return []
  }, [singlePredictResponse])
  const batchPredictionRows = useMemo(() => {
    const enriched = Array.isArray(batchPredictResponse?.enriched_rows) ? (batchPredictResponse?.enriched_rows as AnyRow[]) : []
    if (enriched.length > 0) return enriched
    const preview = Array.isArray(batchPredictResponse?.enriched_preview) ? (batchPredictResponse?.enriched_preview as AnyRow[]) : []
    return preview
  }, [batchPredictResponse])
  const evaluateActualVsPredRows = useMemo(() => {
    const enriched = Array.isArray(evaluateResponse?.enriched_rows)
      ? (evaluateResponse?.enriched_rows as AnyRow[])
      : []
    if (enriched.length > 0) return enriched
    const rows = Array.isArray(evaluateResponse?.actual_vs_predicted_preview)
      ? (evaluateResponse?.actual_vs_predicted_preview as AnyRow[])
      : []
    if (rows.length > 0) return rows
    return Array.isArray(evaluateResponse?.prediction_preview)
      ? (evaluateResponse?.prediction_preview as AnyRow[])
      : []
  }, [evaluateResponse])
  const singlePredictionChartOption = useMemo(
    () => buildSinglePredictionChartOption(singlePredictResponse, activeChartTaskMode),
    [singlePredictResponse, activeChartTaskMode],
  )
  const batchPredictionChartOption = useMemo(
    () => buildBatchPredictionChartOption(batchPredictResponse, activeChartTaskMode),
    [batchPredictResponse, activeChartTaskMode],
  )
  const evaluateChartOption = useMemo(
    () => buildEvaluateChartOption(evaluateResponse, activeChartTaskMode),
    [evaluateResponse, activeChartTaskMode],
  )
  const trainSourceColumnOptions = useMemo(
    () => trainSourceColumns.map((col) => ({ value: col, label: col })),
    [trainSourceColumns],
  )
  const trainTargetOptions = trainSourceColumnOptions
  const evaluateTargetOptions = useMemo(() => {
    const merged = new Set<string>([
      ...trainSourceColumns,
      ...evaluateSourceColumns,
      ...(selectedRunTarget ? [selectedRunTarget] : []),
      ...evaluateTargetColumns,
    ])
    return Array.from(merged).map((col) => ({ value: col, label: col }))
  }, [trainSourceColumns, evaluateSourceColumns, selectedRunTarget, evaluateTargetColumns])
  const selectedTrainTargetColumn = firstSelected(trainTargetColumns)
  const selectedEvaluateTargetColumn = firstSelected(evaluateTargetColumns)

  const buildSingleRowTemplate = (columns: string[]): string => {
    if (columns.length === 0) {
      return '{\n  "feature_1": 10,\n  "feature_2": 25\n}'
    }
    const row = columns.reduce<Record<string, unknown>>((acc, col) => {
      acc[col] = null
      return acc
    }, {})
    return JSON.stringify(row, null, 2)
  }

  const applyTrainSourceToDownstream = (showToast = true) => {
    const copied = cloneSourceConfig(trainSource)
    setBatchSource(copied)
    setEvaluateSource(copied)
    if (copied.source_type === 'rows' && !singleRowTouched) {
      try {
        const parsed = JSON.parse(copied.rows_json || '[]')
        const first = Array.isArray(parsed) ? parsed.find((r) => r && typeof r === 'object' && !Array.isArray(r)) : parsed
        if (first && typeof first === 'object' && !Array.isArray(first)) {
          setSingleRowJson(JSON.stringify(first, null, 2))
        }
      } catch {
        // no-op; user may be editing rows_json manually.
      }
    }
    if (showToast) {
      notification.success({ message: 'Predict and Evaluate source copied from Train', placement: 'bottomRight', duration: 2 })
    }
  }

  const applyRunDefaultsToForms = () => {
    const target = maybeValue(String(selectedH2ORunDetail?.target_column || ''))
    if (target) {
      setEvaluateTargetColumns([target])
      setEvaluateTargetTouched(false)
    }
    const template = buildSingleRowTemplate(runFeatureColumns)
    setSingleRowJson(template)
    setSingleRowTouched(false)
    notification.success({ message: 'Run defaults applied to Predict/Evaluate', placement: 'bottomRight', duration: 2 })
  }

  const loadExampleSourceData = () => {
    const rowsSource: H2OSourceConfig = {
      source_type: 'rows',
      pipeline_id: undefined,
      pipeline_node_id: '',
      file_path: '',
      dataset: 'sales',
      workflow_id: workflow?.id,
      use_workflow_preprocessing: true,
      rows_json: H2O_EXAMPLE_SOURCE_JSON,
    }
    setTrainSource(rowsSource)
    setBatchSource(rowsSource)
    setEvaluateSource(rowsSource)
    setTrainTask('regression')
    setTrainTargetColumns(['sales'])
    setTrainFeatureColumns(['visitors', 'ad_spend', 'conversion_rate'])
    setTrainExcludeColumns(['month', 'region'])
    setEvaluateTargetColumns(['sales'])
    setEvaluateTargetTouched(false)
    setSingleRowJson(JSON.stringify({ visitors: 54000, ad_spend: 21000, conversion_rate: 3.35 }, null, 2))
    setSingleRowTouched(false)
    notification.success({ message: 'Example source data loaded', placement: 'bottomRight', duration: 2 })
  }

  const loadH2OHealth = async () => {
    setH2OHealthLoading(true)
    try {
      const health = await api.getMLOpsH2OHealth()
      setH2OHealth(health)
    } finally {
      setH2OHealthLoading(false)
    }
  }

  const loadPipelines = async () => {
    setPipelinesLoading(true)
    try {
      const rows = await api.listPipelines()
      const options = (Array.isArray(rows) ? rows : [])
        .filter((row: any) => row?.id && row?.name)
        .map((row: any) => ({
          value: String(row.id),
          label: String(row.name),
        }))
      setPipelineOptions(options)
    } catch {
      setPipelineOptions([])
    } finally {
      setPipelinesLoading(false)
    }
  }

  const loadH2ORuns = async () => {
    setH2ORunsLoading(true)
    try {
      const scopedRows = await api.listMLOpsH2ORuns(workflow?.id, 100)
      const scoped = Array.isArray(scopedRows) ? scopedRows : []
      if (scoped.length > 0 || !workflow?.id) {
        setH2ORuns(scoped)
      } else {
        // Backward compatibility: older runs may not have workflow_id stored.
        const globalRows = await api.listMLOpsH2ORuns(undefined, 100)
        setH2ORuns(Array.isArray(globalRows) ? globalRows : [])
      }
    } finally {
      setH2ORunsLoading(false)
    }
  }

  useEffect(() => {
    if (!h2oModalOpen) return
    void Promise.all([loadH2OHealth(), loadPipelines(), loadH2ORuns()])
  }, [h2oModalOpen, workflow?.id])

  useEffect(() => {
    if (!h2oModalOpen) return
    if (!selectedH2ORun && h2oRuns.length > 0) {
      setSelectedH2ORun(h2oRuns[0].id)
      return
    }
    if (selectedH2ORun && !h2oRuns.some((run) => run.id === selectedH2ORun)) {
      setSelectedH2ORun(h2oRuns.length > 0 ? h2oRuns[0].id : undefined)
    }
  }, [h2oModalOpen, h2oRuns, selectedH2ORun])

  useEffect(() => {
    if (!h2oModalOpen || !selectedH2ORun) {
      setSelectedH2ORunDetail(null)
      return
    }
    let active = true
    void api.getMLOpsH2ORun(selectedH2ORun)
      .then((detail) => {
        if (active) setSelectedH2ORunDetail(detail)
      })
      .catch(() => {
        if (active) setSelectedH2ORunDetail(null)
      })
    return () => { active = false }
  }, [h2oModalOpen, selectedH2ORun])

  useEffect(() => {
    if (!selectedH2ORunDetail) return
    if (!evaluateTargetTouched) {
      const runTarget = maybeValue(String(selectedH2ORunDetail.target_column || ''))
      if (runTarget) setEvaluateTargetColumns([runTarget])
    }
    if (!singleRowTouched && runFeatureColumns.length > 0) {
      setSingleRowJson(buildSingleRowTemplate(runFeatureColumns))
    }
  }, [selectedH2ORunDetail, evaluateTargetTouched, singleRowTouched, runFeatureColumns])

  useEffect(() => {
    const label = maybeValue(String((selectedH2ORunDetail as Record<string, unknown> | null)?.label || '')) || ''
    setRunLabelDraft(label)
  }, [selectedH2ORunDetail])

  const handleSave = async () => {
    setSaving(true)
    try {
      await saveWorkflow()
      notification.success({ message: 'Saved', placement: 'bottomRight', duration: 2 })
    } finally {
      setSaving(false)
    }
  }

  const handleRun = async () => {
    if (isExecuting) return
    await executeWorkflow()
  }

  const hasError = runLogs.some((l) => l.status === 'error')
  const hasSuccess = !isExecuting && runLogs.length > 0 && !hasError

  const moreMenuItems = [
    { key: 'schedule', icon: <ScheduleOutlined />, label: 'Configure Schedule' },
    { key: 'runs', icon: <BarChartOutlined />, label: 'View Runs (coming soon)' },
    { type: 'divider' as const },
    { key: 'delete', icon: <CloseCircleFilled />, label: 'Delete Workflow', danger: true },
  ]

  const applySourceConfig = (
    source: H2OSourceConfig,
    payload: Record<string, unknown>,
    requireRowsForRowsSource: boolean,
  ) => {
    payload.source_type = source.source_type
    if (source.source_type === 'pipeline') {
      const pipelineId = maybeValue(source.pipeline_id || '')
      if (!pipelineId) throw new Error('Pipeline source requires pipeline_id')
      payload.pipeline_id = pipelineId
      const pipelineNodeId = maybeValue(source.pipeline_node_id || '')
      if (pipelineNodeId) payload.pipeline_node_id = pipelineNodeId
      return
    }
    if (source.source_type === 'file') {
      const filePath = maybeValue(source.file_path)
      if (!filePath) throw new Error('File source requires file_path')
      payload.file_path = filePath
      return
    }
    if (source.source_type === 'sample') {
      payload.dataset = maybeValue(source.dataset) || 'sales'
      return
    }
    if (source.source_type === 'workflow') {
      const workflowId = maybeValue(source.workflow_id || '') || workflow?.id
      if (!workflowId) throw new Error('Workflow source requires workflow_id')
      payload.workflow_id = workflowId
      payload.use_workflow_preprocessing = source.use_workflow_preprocessing
      return
    }

    const raw = String(source.rows_json || '').trim()
    if (!raw && requireRowsForRowsSource) {
      throw new Error('Rows JSON is required for rows source')
    }
    if (!raw) {
      payload.rows = []
      return
    }

    let parsed: unknown
    try {
      parsed = JSON.parse(raw)
    } catch {
      throw new Error('Rows JSON must be valid JSON')
    }

    const rows = Array.isArray(parsed)
      ? parsed.filter((r): r is Record<string, unknown> => !!r && typeof r === 'object' && !Array.isArray(r))
      : (parsed && typeof parsed === 'object' ? [parsed as Record<string, unknown>] : [])

    if (requireRowsForRowsSource && rows.length === 0) {
      throw new Error('Rows JSON must contain one object or an array of objects')
    }
    payload.rows = rows
  }

  const loadTrainSourceColumns = async () => {
    setTrainSourceColumnsError('')
    let payload: Record<string, unknown> = {}
    try {
      applySourceConfig(trainSource, payload, true)
    } catch (err) {
      const message = errorMessage(err)
      const incompleteSource =
        message.includes('requires pipeline_id') ||
        message.includes('requires file_path') ||
        message.includes('requires workflow_id') ||
        message.includes('Rows JSON is required for rows source')
      setTrainSourceColumns([])
      setTrainSourceColumnsProfile(null)
      if (!incompleteSource) {
        setTrainSourceColumnsError(message)
      }
      return
    }

    payload.max_profile_rows = 5000
    payload.max_preview_rows = 25
    setTrainSourceColumnsLoading(true)
    try {
      const response = await api.getMLOpsH2OSourceColumns(payload)
      const columns: string[] = Array.isArray(response?.columns)
        ? (response.columns as unknown[]).map((v: unknown) => String(v || '').trim()).filter((v): v is string => Boolean(v))
        : []
      const targetSuggestion = maybeValue(String(response?.target_suggestion || ''))
      const numericColumns: string[] = Array.isArray(response?.numeric_columns)
        ? (response.numeric_columns as unknown[]).map((v: unknown) => String(v || '').trim()).filter((v): v is string => Boolean(v))
        : []

      setTrainSourceColumns(columns)
      setTrainSourceColumnsProfile(response as H2OSourceColumnsProfile)
      setTrainFeatureColumns((prev) => {
        const kept = prev.filter((col) => columns.includes(col))
        if (kept.length > 0) return kept
        const inferred = (numericColumns.length > 0 ? numericColumns : columns)
          .filter((col) => col !== targetSuggestion)
          .slice(0, 20)
        return inferred
      })
      setTrainExcludeColumns((prev) => prev.filter((col) => columns.includes(col)))
      setTrainTargetColumns((prev) => {
        const kept = prev.filter((col) => columns.includes(col))
        if (kept.length > 0) return kept
        if (targetSuggestion && columns.includes(targetSuggestion)) return [targetSuggestion]
        return []
      })
    } catch (err) {
      const message = errorMessage(err)
      setTrainSourceColumns([])
      setTrainSourceColumnsProfile(null)
      setTrainSourceColumnsError(message)
    } finally {
      setTrainSourceColumnsLoading(false)
    }
  }

  const loadEvaluateSourceColumns = async () => {
    setEvaluateSourceColumnsError('')
    let payload: Record<string, unknown> = {}
    try {
      applySourceConfig(evaluateSource, payload, true)
    } catch (err) {
      const message = errorMessage(err)
      const incompleteSource =
        message.includes('requires pipeline_id') ||
        message.includes('requires file_path') ||
        message.includes('requires workflow_id') ||
        message.includes('Rows JSON is required for rows source')
      setEvaluateSourceColumns([])
      setEvaluateSourceColumnsProfile(null)
      if (!incompleteSource) {
        setEvaluateSourceColumnsError(message)
      }
      return
    }

    payload.max_profile_rows = 5000
    payload.max_preview_rows = 25
    setEvaluateSourceColumnsLoading(true)
    try {
      const response = await api.getMLOpsH2OSourceColumns(payload)
      const columns: string[] = Array.isArray(response?.columns)
        ? (response.columns as unknown[]).map((v: unknown) => String(v || '').trim()).filter((v): v is string => Boolean(v))
        : []
      const targetSuggestion = maybeValue(String(response?.target_suggestion || ''))

      setEvaluateSourceColumns(columns)
      setEvaluateSourceColumnsProfile(response as H2OSourceColumnsProfile)
      setEvaluateTargetColumns((prev) => {
        const kept = prev.filter((col) => columns.includes(col))
        if (kept.length > 0) return kept
        if (targetSuggestion && columns.includes(targetSuggestion) && !evaluateTargetTouched) {
          return [targetSuggestion]
        }
        return prev
      })
    } catch (err) {
      const message = errorMessage(err)
      setEvaluateSourceColumns([])
      setEvaluateSourceColumnsProfile(null)
      setEvaluateSourceColumnsError(message)
    } finally {
      setEvaluateSourceColumnsLoading(false)
    }
  }

  useEffect(() => {
    if (!h2oModalOpen) return
    const timer = setTimeout(() => {
      void loadTrainSourceColumns()
    }, 450)
    return () => clearTimeout(timer)
  }, [
    h2oModalOpen,
    trainSource.source_type,
    trainSource.pipeline_id,
    trainSource.pipeline_node_id,
    trainSource.file_path,
    trainSource.dataset,
    trainSource.workflow_id,
    trainSource.use_workflow_preprocessing,
    trainSource.rows_json,
  ])

  useEffect(() => {
    if (!h2oModalOpen) return
    const timer = setTimeout(() => {
      void loadEvaluateSourceColumns()
    }, 450)
    return () => clearTimeout(timer)
  }, [
    h2oModalOpen,
    evaluateSource.source_type,
    evaluateSource.pipeline_id,
    evaluateSource.pipeline_node_id,
    evaluateSource.file_path,
    evaluateSource.dataset,
    evaluateSource.workflow_id,
    evaluateSource.use_workflow_preprocessing,
    evaluateSource.rows_json,
    evaluateTargetTouched,
  ])

  const runH2OTrain = async () => {
    setH2OError('')
    setTrainLoading(true)
    try {
      const payload: Record<string, unknown> = {
        task: trainTask,
        train_ratio: trainRatio,
        max_models: trainMaxModels,
        max_runtime_secs: trainMaxRuntime,
        seed: trainSeed,
        nfolds: trainNFolds,
        balance_classes: trainBalanceClasses,
      }
      if (workflow?.id) payload.workflow_id = workflow.id
      applySourceConfig(trainSource, payload, true)

      const target = selectedTrainTargetColumn
      if (target) payload.target_column = target

      const features = trainFeatureColumns
        .map((v) => String(v || '').trim())
        .filter(Boolean)
      if (features.length > 0) payload.feature_columns = features

      const excludes = trainExcludeColumns
        .map((v) => String(v || '').trim())
        .filter(Boolean)
      if (excludes.length > 0) payload.exclude_columns = excludes

      const includeAlgos = parseCsvList(trainIncludeAlgos)
      const excludeAlgos = parseCsvList(trainExcludeAlgos)
      if (includeAlgos.length > 0 && excludeAlgos.length > 0) {
        throw new Error('Provide include_algos or exclude_algos, not both')
      }
      if (includeAlgos.length > 0) payload.include_algos = includeAlgos
      if (excludeAlgos.length > 0) payload.exclude_algos = excludeAlgos

      const projectName = maybeValue(trainProjectName)
      if (projectName) payload.project_name = projectName

      const response = await api.trainMLOpsH2O(payload)
      setTrainResponse(response)
      const runId = String((response as any)?.run_id || '').trim()
      if (runId) setSelectedH2ORun(runId)
      if (!evaluateTargetTouched) {
        const trainedTarget = maybeValue(String((response as any)?.target_column || ''))
        if (trainedTarget) setEvaluateTargetColumns([trainedTarget])
      }
      const trainedFeatures = Array.isArray((response as any)?.feature_columns)
        ? (response as any).feature_columns.map((v: unknown) => String(v || '').trim()).filter(Boolean)
        : []
      if (!singleRowTouched && trainedFeatures.length > 0) {
        setSingleRowJson(buildSingleRowTemplate(trainedFeatures.slice(0, 20)))
      }
      applyTrainSourceToDownstream(false)
      await loadH2ORuns()
      notification.success({ message: 'H2O AutoML training completed', placement: 'bottomRight' })
    } catch (err) {
      const message = errorMessage(err)
      setH2OError(message)
      notification.error({ message: 'H2O AutoML training failed', description: message, placement: 'bottomRight' })
    } finally {
      setTrainLoading(false)
    }
  }

  const runSinglePrediction = async () => {
    setH2OError('')
    setSinglePredictLoading(true)
    try {
      const runId = maybeValue(selectedH2ORun || '')
      if (!runId) throw new Error('Select an H2O run first')

      let parsed: unknown
      try {
        parsed = JSON.parse(singleRowJson)
      } catch {
        throw new Error('Single input row must be valid JSON object')
      }
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        throw new Error('Single input row must be a JSON object')
      }

      const response = await api.predictMLOpsH2OSingle({ run_id: runId, row: parsed })
      setSinglePredictResponse(response)
      notification.success({ message: 'Single prediction generated', placement: 'bottomRight' })
    } catch (err) {
      const message = errorMessage(err)
      setH2OError(message)
      notification.error({ message: 'Single prediction failed', description: message, placement: 'bottomRight' })
    } finally {
      setSinglePredictLoading(false)
    }
  }

  const runBatchPrediction = async () => {
    setH2OError('')
    setBatchPredictLoading(true)
    try {
      const runId = maybeValue(selectedH2ORun || '')
      if (!runId) throw new Error('Select an H2O run first')

      const payload: Record<string, unknown> = {
        run_id: runId,
        max_rows: batchMaxRows,
      }
      applySourceConfig(batchSource, payload, true)
      const response = await api.predictMLOpsH2OBatch(payload)
      setBatchPredictResponse(response)
      notification.success({ message: 'Batch prediction completed', placement: 'bottomRight' })
    } catch (err) {
      const message = errorMessage(err)
      setH2OError(message)
      notification.error({ message: 'Batch prediction failed', description: message, placement: 'bottomRight' })
    } finally {
      setBatchPredictLoading(false)
    }
  }

  const runEvaluation = async () => {
    setH2OError('')
    setEvaluateLoading(true)
    try {
      const runId = maybeValue(selectedH2ORun || '')
      if (!runId) throw new Error('Select an H2O run first')

      const payload: Record<string, unknown> = {
        run_id: runId,
        max_rows: evaluateMaxRows,
      }
      applySourceConfig(evaluateSource, payload, true)
      const target = selectedEvaluateTargetColumn
      const taskMode = normalizeH2OTaskMode(selectedRunRequestedTask || selectedRunTask || trainTask)
      if ((taskMode === 'classification' || taskMode === 'regression') && !target) {
        throw new Error('Target column is required for evaluation in regression/classification mode')
      }
      if (target) payload.target_column = target

      const response = await api.evaluateMLOpsH2O(payload)
      setEvaluateResponse(response)
      notification.success({ message: 'Evaluation completed', placement: 'bottomRight' })
    } catch (err) {
      const message = errorMessage(err)
      setH2OError(message)
      notification.error({ message: 'Evaluation failed', description: message, placement: 'bottomRight' })
    } finally {
      setEvaluateLoading(false)
    }
  }

  const useSelectedRunForEvaluate = () => {
    if (!selectedH2ORun) {
      notification.warning({ message: 'Select an H2O run first', placement: 'bottomRight' })
      return
    }
    applyRunDefaultsToForms()
    setH2OActiveTab('evaluate')
    notification.success({ message: 'Selected run applied to Evaluate', placement: 'bottomRight', duration: 2 })
  }

  const saveSelectedRunLabel = async () => {
    const runId = maybeValue(selectedH2ORun || '')
    if (!runId) {
      notification.warning({ message: 'Select an H2O run first', placement: 'bottomRight' })
      return
    }
    setH2ORunPatchLoading(true)
    try {
      const payload = { label: maybeValue(runLabelDraft || '') || null }
      const updated = await api.updateMLOpsH2ORun(runId, payload)
      setSelectedH2ORunDetail(updated)
      await loadH2ORuns()
      notification.success({ message: 'Model label updated', placement: 'bottomRight', duration: 2 })
    } catch (err) {
      notification.error({ message: 'Failed to update model label', description: errorMessage(err), placement: 'bottomRight' })
    } finally {
      setH2ORunPatchLoading(false)
    }
  }

  const deleteSelectedRun = async () => {
    const runId = maybeValue(selectedH2ORun || '')
    if (!runId) {
      notification.warning({ message: 'Select an H2O run first', placement: 'bottomRight' })
      return
    }
    const confirmed = window.confirm('Delete selected H2O model/run? This removes saved model artifacts.')
    if (!confirmed) return

    setH2ORunDeleteLoading(true)
    try {
      await api.deleteMLOpsH2ORun(runId)
      if (selectedH2ORun === runId) {
        setSelectedH2ORun(undefined)
        setSelectedH2ORunDetail(null)
      }
      await loadH2ORuns()
      notification.success({ message: 'H2O model/run deleted', placement: 'bottomRight', duration: 2 })
    } catch (err) {
      notification.error({ message: 'Failed to delete H2O model/run', description: errorMessage(err), placement: 'bottomRight' })
    } finally {
      setH2ORunDeleteLoading(false)
    }
  }

  const jsonPreviewStyle: CSSProperties = {
    margin: 0,
    padding: 12,
    borderRadius: 8,
    border: '1px solid var(--app-border-strong)',
    background: 'var(--app-input-bg)',
    color: 'var(--app-text)',
    fontFamily: 'monospace',
    fontSize: 12,
    maxHeight: 280,
    overflow: 'auto',
    whiteSpace: 'pre-wrap',
    wordBreak: 'break-word',
  }

  const renderSourceConfig = (
    source: H2OSourceConfig,
    setSource: Dispatch<SetStateAction<H2OSourceConfig>>,
    keyPrefix: string,
  ) => (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: 10 }}>
      <div style={{ gridColumn: 'span 2' }}>
        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Data Source</Text>
        <Select<H2OSourceType>
          value={source.source_type}
          onChange={(value) => setSource((prev) => ({ ...prev, source_type: value }))}
          options={H2O_SOURCE_OPTIONS}
          style={{ width: '100%', marginTop: 6 }}
        />
      </div>

      {source.source_type === 'pipeline' && (
        <>
          <div>
            <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Pipeline</Text>
            <Select
              value={source.pipeline_id || undefined}
              onChange={(value) => setSource((prev) => ({ ...prev, pipeline_id: value }))}
              options={pipelineOptions}
              loading={pipelinesLoading}
              showSearch
              optionFilterProp="label"
              style={{ width: '100%', marginTop: 6 }}
              placeholder="Select ETL pipeline"
            />
          </div>
          <div>
            <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Pipeline Node Id (optional)</Text>
            <Input
              value={source.pipeline_node_id || ''}
              onChange={(e) => setSource((prev) => ({ ...prev, pipeline_node_id: e.target.value }))}
              placeholder="node-id"
              style={{ marginTop: 6 }}
              id={`${keyPrefix}-pipeline-node-id`}
            />
          </div>
        </>
      )}

      {source.source_type === 'file' && (
        <div style={{ gridColumn: 'span 2' }}>
          <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>File Path (backend-accessible)</Text>
          <Input
            value={source.file_path}
            onChange={(e) => setSource((prev) => ({ ...prev, file_path: e.target.value }))}
            placeholder="/Users/.../data.csv"
            style={{ marginTop: 6 }}
            id={`${keyPrefix}-file-path`}
          />
        </div>
      )}

      {source.source_type === 'sample' && (
        <div style={{ gridColumn: 'span 2' }}>
          <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Sample Dataset</Text>
          <Select
            value={source.dataset}
            onChange={(value) => setSource((prev) => ({ ...prev, dataset: value }))}
            options={SAMPLE_DATASET_OPTIONS}
            style={{ width: '100%', marginTop: 6 }}
          />
        </div>
      )}

      {source.source_type === 'workflow' && (
        <>
          <div>
            <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Workflow Id</Text>
            <Input
              value={source.workflow_id || ''}
              onChange={(e) => setSource((prev) => ({ ...prev, workflow_id: e.target.value }))}
              placeholder={workflow?.id || 'Current workflow id'}
              style={{ marginTop: 6 }}
              id={`${keyPrefix}-workflow-id`}
            />
          </div>
          <div>
            <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Use Workflow Preprocessing</Text>
            <div style={{ marginTop: 6, minHeight: 32, display: 'flex', alignItems: 'center' }}>
              <Switch
                checked={source.use_workflow_preprocessing}
                onChange={(value) => setSource((prev) => ({ ...prev, use_workflow_preprocessing: value }))}
              />
            </div>
          </div>
        </>
      )}

      {source.source_type === 'rows' && (
        <div style={{ gridColumn: 'span 2' }}>
          <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Rows JSON</Text>
          <Input.TextArea
            value={source.rows_json}
            onChange={(e) => setSource((prev) => ({ ...prev, rows_json: e.target.value }))}
            rows={8}
            style={{ marginTop: 6, fontFamily: 'monospace' }}
            id={`${keyPrefix}-rows-json`}
          />
        </div>
      )}
    </div>
  )

  return (
    <ReactFlowProvider>
      <div style={{ height: '100vh', background: '#0a0a10', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        <div style={{
          background: 'var(--app-panel-bg)',
          borderBottom: '1px solid var(--app-border)',
          padding: '0 16px',
          height: 52,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          flexShrink: 0,
          zIndex: 10,
        }}>
          <Space size={12}>
            <Tooltip title="Back to MLOps workflows">
              <Button
                type="text"
                icon={<ArrowLeftOutlined />}
                style={{ color: 'var(--app-text-subtle)' }}
                onClick={() => navigate('/mlops')}
              />
            </Tooltip>
            <div style={{ width: 1, height: 20, background: 'var(--app-border)' }} />
            <Input
              value={workflowName}
              onChange={(e) => setWorkflowName(e.target.value)}
              onBlur={async () => {
                if (workflow?.id && workflowName !== workflow?.name) {
                  await api.updateMLOpsWorkflow(workflow.id, { name: workflowName })
                  notification.success({ message: 'Workflow renamed', placement: 'bottomRight', duration: 2 })
                }
              }}
              style={{
                background: 'transparent',
                border: '1px solid transparent',
                color: 'var(--app-text)',
                fontWeight: 600,
                fontSize: 14,
                padding: '2px 8px',
                width: 320,
                borderRadius: 6,
              }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = 'var(--app-border-strong)' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'transparent' }}
              onFocus={(e) => { e.currentTarget.style.borderColor = '#22c55e' }}
            />

            {isDirty && !saving && (
              <Tag style={{ background: '#f59e0b15', border: '1px solid #f59e0b30', color: '#f59e0b', fontSize: 11, borderRadius: 4 }}>
                Unsaved
              </Tag>
            )}
            {saving && <LoadingOutlined style={{ color: '#22c55e', fontSize: 12 }} spin />}
            {!isDirty && !saving && (
              <Tag style={{ background: '#22c55e10', border: '1px solid #22c55e20', color: '#22c55e', fontSize: 11, borderRadius: 4 }}>
                Saved
              </Tag>
            )}
          </Space>

          <Space size={8}>
            <Tooltip title="Connector style">
              <Select<WorkflowConnectorType>
                value={connectorType}
                onChange={(value) => setConnectorType(value)}
                options={WORKFLOW_CONNECTOR_OPTIONS}
                size="small"
                style={{ width: 124 }}
                dropdownStyle={{ background: 'var(--app-card-bg)' }}
              />
            </Tooltip>

            {hasError && (
              <Tag icon={<CloseCircleFilled />} style={{ background: '#ef444415', border: '1px solid #ef444430', color: '#ef4444', borderRadius: 6 }}>
                Failed
              </Tag>
            )}
            {hasSuccess && (
              <Tag icon={<CheckCircleFilled />} style={{ background: '#22c55e15', border: '1px solid #22c55e30', color: '#22c55e', borderRadius: 6 }}>
                Run Completed
              </Tag>
            )}

            <Button
              icon={<DeploymentUnitOutlined />}
              onClick={() => setH2OModalOpen(true)}
              style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)' }}
            >
              H2O AutoML Service
            </Button>

            <Button
              icon={<SaveOutlined />}
              loading={saving}
              onClick={handleSave}
              style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)' }}
            >
              Save
            </Button>

            <Button
              type="primary"
              icon={isExecuting ? <LoadingOutlined spin /> : <PlayCircleOutlined />}
              onClick={handleRun}
              disabled={isExecuting}
              style={{
                background: isExecuting ? 'rgba(34,197,94,0.45)' : 'linear-gradient(135deg, #22c55e, #16a34a)',
                border: 'none',
                minWidth: 110,
              }}
            >
              {isExecuting ? 'Running…' : 'Train / Deploy'}
            </Button>

            <Dropdown
              menu={{
                items: moreMenuItems,
                onClick: ({ key }) => {
                  if (key === 'schedule') setScheduleModal(true)
                  if (key === 'delete') {
                    Modal.confirm({
                      title: 'Delete this workflow?',
                      content: 'This action cannot be undone.',
                      okText: 'Delete',
                      okButtonProps: { danger: true },
                      onOk: async () => {
                        if (!workflow?.id) return
                        await api.deleteMLOpsWorkflow(workflow.id)
                        notification.success({ message: 'Workflow deleted', placement: 'bottomRight' })
                        navigate('/mlops')
                      },
                    })
                  }
                },
                style: { background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)' },
              }}
              trigger={['click']}
            >
              <Button
                type="text"
                icon={<EllipsisOutlined />}
                style={{ color: 'var(--app-text-subtle)', border: '1px solid var(--app-border-strong)', background: 'var(--app-card-bg)' }}
              />
            </Dropdown>
          </Space>
        </div>

        <div style={{ flex: 1, display: 'flex', overflow: 'hidden', position: 'relative' }}>
          <MLNodePalette />
          <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
            <MLWorkflowCanvas />
            <MLExecutionPanel />
          </div>
          <MLConfigDrawer open={!!selectedNodeId} onClose={() => setSelectedNode(null)} />
        </div>
      </div>

      <Modal
        title={<span style={{ color: 'var(--app-text)' }}>Configure MLOps Schedule</span>}
        open={scheduleModal}
        onOk={async () => {
          const values = await form.validateFields()
          if (workflow?.id) {
            await api.updateMLOpsWorkflow(workflow.id, {
              schedule_enabled: values.enabled,
              schedule_cron: values.cron,
            })
            notification.success({ message: `Schedule saved: ${values.cron}`, placement: 'bottomRight' })
          }
          setScheduleModal(false)
        }}
        onCancel={() => setScheduleModal(false)}
        okText="Save Schedule"
        okButtonProps={{ style: { background: 'linear-gradient(135deg, #22c55e, #16a34a)', border: 'none' } }}
        styles={{
          content: { background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)' },
          header: { background: 'var(--app-card-bg)', borderBottom: '1px solid var(--app-border-strong)' },
          footer: { background: 'var(--app-card-bg)', borderTop: '1px solid var(--app-border-strong)' },
          mask: { backdropFilter: 'blur(4px)' },
        }}
      >
        <Form form={form} layout="vertical" style={{ marginTop: 16 }}>
          <Form.Item
            name="enabled"
            valuePropName="checked"
            label={<span style={{ color: 'var(--app-text-muted)' }}>Enable Schedule</span>}
            initialValue={workflow?.schedule_enabled}
          >
            <Switch />
          </Form.Item>
          <Form.Item
            name="cron"
            label={<span style={{ color: 'var(--app-text-muted)' }}>Cron Expression</span>}
            initialValue={workflow?.schedule_cron || '0 2 * * *'}
            rules={[{ required: true }]}
          >
            <Input
              placeholder="0 2 * * *"
              style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)', fontFamily: 'monospace' }}
            />
          </Form.Item>
          <div style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '12px 16px' }}>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Common retraining schedules:</Text>
            <div style={{ marginTop: 8, display: 'flex', flexWrap: 'wrap', gap: 6 }}>
              {[
                { label: 'Daily 2am', val: '0 2 * * *' },
                { label: 'Every 6 hours', val: '0 */6 * * *' },
                { label: 'Weekly Monday', val: '0 3 * * 1' },
                { label: 'Monthly', val: '0 4 1 * *' },
              ].map((s) => (
                <Tag
                  key={s.val}
                  style={{ cursor: 'pointer', background: 'var(--app-border)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)', fontSize: 11 }}
                  onClick={() => form.setFieldValue('cron', s.val)}
                >
                  {s.label}
                </Tag>
              ))}
            </div>
          </div>
        </Form>
      </Modal>

      <Modal
        title={<span style={{ color: 'var(--app-text)' }}>H2O AutoML Service</span>}
        open={h2oModalOpen}
        onCancel={() => setH2OModalOpen(false)}
        footer={null}
        width="94vw"
        styles={{
          content: { background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', top: 12 },
          header: { background: 'var(--app-card-bg)', borderBottom: '1px solid var(--app-border-strong)' },
          body: { background: 'var(--app-card-bg)', maxHeight: '84vh', overflowY: 'auto', paddingTop: 12 },
          mask: { backdropFilter: 'blur(4px)' },
        }}
      >
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
            <Space>
              {h2oHealthLoading ? (
                <Spin size="small" />
              ) : (
                <Tag color={h2oHealth?.h2o_available ? 'success' : 'error'}>
                  {h2oHealth?.h2o_available ? 'H2O Ready' : 'H2O Unavailable'}
                </Tag>
              )}
              {h2oHealth?.version && <Tag color="blue">Version: {h2oHealth.version}</Tag>}
              {!!h2oHealth?.nodes && <Tag color="purple">Nodes: {h2oHealth.nodes}</Tag>}
            </Space>
            <Button icon={<ReloadOutlined />} onClick={() => void Promise.all([loadH2OHealth(), loadH2ORuns()])}>
              Refresh Health/Runs
            </Button>
          </div>

          {!h2oHealth?.h2o_available && (
            <Alert
              type="warning"
              showIcon
              message="H2O cluster is unavailable"
              description={h2oHealth?.detail || 'Install h2o in backend environment and ensure Java/H2O cluster is reachable.'}
            />
          )}

          {h2oError && <Alert type="error" showIcon message={h2oError} />}

          <Tabs
            activeKey={h2oActiveTab}
            onChange={setH2OActiveTab}
            items={[
              {
                key: 'train',
                label: 'Train',
                children: (
                  <Space direction="vertical" size={12} style={{ width: '100%' }}>
                    <Alert
                      type="info"
                      showIcon
                      message="Quick Start"
                      description="1) Load Example Source Data, 2) Run H2O AutoML Training, 3) Predict and Evaluate will auto-use selected run values."
                    />
                    <Space wrap>
                      <Button onClick={loadExampleSourceData}>Load Example Source Data</Button>
                      <Button onClick={() => applyTrainSourceToDownstream(true)}>Copy Train Source to Predict/Evaluate</Button>
                    </Space>
                    {renderSourceConfig(trainSource, setTrainSource, 'train-source')}

                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: 10 }}>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Target Column (multi-select)</Text>
                        <Select
                          mode="multiple"
                          value={trainTargetColumns}
                          onChange={(values: string[]) => setTrainTargetColumns(values)}
                          options={trainTargetOptions}
                          loading={trainSourceColumnsLoading}
                          showSearch
                          optionFilterProp="label"
                          maxTagCount="responsive"
                          placeholder={trainSourceColumnsLoading ? 'Loading columns…' : 'Select one or more targets'}
                          style={{ width: '100%', marginTop: 6 }}
                        />
                      </div>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Feature Columns (optional)</Text>
                        <Select
                          mode="multiple"
                          value={trainFeatureColumns}
                          onChange={(values: string[]) => setTrainFeatureColumns(values)}
                          options={trainSourceColumnOptions}
                          loading={trainSourceColumnsLoading}
                          showSearch
                          optionFilterProp="label"
                          maxTagCount="responsive"
                          placeholder={trainSourceColumnsLoading ? 'Loading columns…' : 'Select feature columns'}
                          style={{ width: '100%', marginTop: 6 }}
                        />
                      </div>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Exclude Columns</Text>
                        <Select
                          mode="multiple"
                          value={trainExcludeColumns}
                          onChange={(values: string[]) => setTrainExcludeColumns(values)}
                          options={trainSourceColumnOptions}
                          loading={trainSourceColumnsLoading}
                          showSearch
                          optionFilterProp="label"
                          maxTagCount="responsive"
                          placeholder={trainSourceColumnsLoading ? 'Loading columns…' : 'Select columns to exclude'}
                          style={{ width: '100%', marginTop: 6 }}
                        />
                      </div>
                    </div>

                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                        Source columns detected: {trainSourceColumns.length}
                        {trainSourceColumnsProfile?.profiled_row_count ? ` · Profiled rows: ${trainSourceColumnsProfile.profiled_row_count}` : ''}
                      </Text>
                      <Button size="small" icon={<ReloadOutlined />} onClick={() => void loadTrainSourceColumns()} loading={trainSourceColumnsLoading}>
                        Refresh Columns
                      </Button>
                    </div>
                    {trainSourceColumnsError && (
                      <Alert type="warning" showIcon message={trainSourceColumnsError} />
                    )}

                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, minmax(0, 1fr))', gap: 10 }}>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Task</Text>
                        <Select
                          value={trainTask}
                          onChange={(v) => setTrainTask(v as H2OTaskMode)}
                          options={[
                            { label: 'Auto', value: 'auto' },
                            { label: 'Classification', value: 'classification' },
                            { label: 'Regression', value: 'regression' },
                            { label: 'Clustering', value: 'clustering' },
                            { label: 'Forecasting', value: 'forecasting' },
                          ]}
                          style={{ width: '100%', marginTop: 6 }}
                        />
                      </div>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Max Models</Text>
                        <InputNumber min={1} max={500} value={trainMaxModels} onChange={(v) => setTrainMaxModels(Number(v || 20))} style={{ width: '100%', marginTop: 6 }} />
                      </div>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Max Runtime (sec)</Text>
                        <InputNumber min={30} max={86400} value={trainMaxRuntime} onChange={(v) => setTrainMaxRuntime(Number(v || 300))} style={{ width: '100%', marginTop: 6 }} />
                      </div>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Train Ratio</Text>
                        <InputNumber min={0.55} max={0.95} step={0.05} value={trainRatio} onChange={(v) => setTrainRatio(Number(v || 0.8))} style={{ width: '100%', marginTop: 6 }} />
                      </div>
                    </div>

                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, minmax(0, 1fr))', gap: 10 }}>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Seed</Text>
                        <InputNumber value={trainSeed} onChange={(v) => setTrainSeed(Number(v || 42))} style={{ width: '100%', marginTop: 6 }} />
                      </div>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>N-Folds</Text>
                        <InputNumber min={0} max={20} value={trainNFolds} onChange={(v) => setTrainNFolds(Number(v || 5))} style={{ width: '100%', marginTop: 6 }} />
                      </div>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Project Name (optional)</Text>
                        <Input value={trainProjectName} onChange={(e) => setTrainProjectName(e.target.value)} style={{ marginTop: 6 }} />
                      </div>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Balance Classes</Text>
                        <div style={{ marginTop: 6, minHeight: 32, display: 'flex', alignItems: 'center' }}>
                          <Switch checked={trainBalanceClasses} onChange={setTrainBalanceClasses} />
                        </div>
                      </div>
                    </div>

                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: 10 }}>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Include Algorithms CSV (optional)</Text>
                        <Input value={trainIncludeAlgos} onChange={(e) => setTrainIncludeAlgos(e.target.value)} placeholder="GBM,DRF,XGBoost" style={{ marginTop: 6 }} />
                      </div>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Exclude Algorithms CSV (optional)</Text>
                        <Input value={trainExcludeAlgos} onChange={(e) => setTrainExcludeAlgos(e.target.value)} placeholder="DeepLearning,StackedEnsemble" style={{ marginTop: 6 }} />
                      </div>
                    </div>

                    <div>
                      <Button type="primary" loading={trainLoading} onClick={runH2OTrain}>
                        Run H2O AutoML Training
                      </Button>
                    </div>

                    {trainResponse && (
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Training Response</Text>
                        <pre style={jsonPreviewStyle}>{JSON.stringify(trainResponse, null, 2)}</pre>
                      </div>
                    )}
                  </Space>
                ),
              },
              {
                key: 'predict',
                label: 'Predict',
                children: (
                  <Space direction="vertical" size={12} style={{ width: '100%' }}>
                    {h2oRuns.length === 0 && (
                      <Alert
                        type="warning"
                        showIcon
                        message="No H2O runs available"
                        description="Run training first, or click Refresh Health/Runs. Predict requires one trained run."
                      />
                    )}
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: 10 }}>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Select H2O Run</Text>
                        <Select
                          value={selectedH2ORun}
                          onChange={(value) => setSelectedH2ORun(value)}
                          options={h2oRunOptions}
                          loading={h2oRunsLoading}
                          style={{ width: '100%', marginTop: 6 }}
                          placeholder="Select run"
                        />
                      </div>
                    </div>
                    <Space wrap>
                      <Button onClick={applyRunDefaultsToForms} disabled={!selectedH2ORunDetail}>Use Selected Run Defaults</Button>
                      <Button onClick={() => applyTrainSourceToDownstream(true)}>Use Train Source for Batch/Evaluate</Button>
                      <Button onClick={loadExampleSourceData}>Load Example Source Data</Button>
                    </Space>
                    {selectedH2ORunDetail && (
                      <Space wrap size={6}>
                        {(selectedRunRequestedTask || selectedRunTask) && <Tag color="blue">Task: {selectedRunRequestedTask || selectedRunTask}</Tag>}
                        {selectedRunRequestedTask && selectedRunTask && selectedRunRequestedTask !== selectedRunTask && (
                          <Tag color="geekblue">Resolved: {selectedRunTask}</Tag>
                        )}
                        {selectedRunTarget && <Tag color="purple">Target: {selectedRunTarget}</Tag>}
                        {runFeatureColumns.length > 0 && <Tag color="green">Features: {runFeatureColumns.slice(0, 5).join(', ')}</Tag>}
                      </Space>
                    )}

                    <div>
                      <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Single Row JSON</Text>
                      <Input.TextArea
                        value={singleRowJson}
                        onChange={(e) => {
                          setSingleRowTouched(true)
                          setSingleRowJson(e.target.value)
                        }}
                        rows={6}
                        style={{ marginTop: 6, fontFamily: 'monospace' }}
                      />
                      <div style={{ marginTop: 8 }}>
                        <Button loading={singlePredictLoading} onClick={runSinglePrediction}>Single Prediction</Button>
                      </div>
                    </div>

                    <div style={{ borderTop: '1px solid var(--app-border)', paddingTop: 10 }}>
                      <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Batch Prediction</Text>
                      <div style={{ marginTop: 8 }}>{renderSourceConfig(batchSource, setBatchSource, 'batch-source')}</div>
                      <div style={{ marginTop: 10, display: 'flex', alignItems: 'center', gap: 10 }}>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Max Rows</Text>
                        <InputNumber min={1} max={200000} value={batchMaxRows} onChange={(v) => setBatchMaxRows(Number(v || 50000))} />
                        <Button loading={batchPredictLoading} onClick={runBatchPrediction}>Batch Prediction</Button>
                      </div>
                    </div>

                    {singlePredictResponse && (
                      <div>
                        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
                          <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Single Prediction Response</Text>
                          <Button
                            icon={<DownloadOutlined />}
                            onClick={() => downloadRowsAsCsv(singlePredictionRows, 'h2o-single-prediction')}
                            disabled={singlePredictionRows.length === 0}
                          >
                            Download CSV
                          </Button>
                        </div>
                        {singlePredictionChartOption && (
                          <div style={{ marginTop: 8, border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: 8 }}>
                            <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Graphical View</Text>
                            <ReactECharts option={singlePredictionChartOption} style={{ height: 260, marginTop: 6 }} notMerge lazyUpdate />
                          </div>
                        )}
                        <pre style={jsonPreviewStyle}>{JSON.stringify(singlePredictResponse, null, 2)}</pre>
                      </div>
                    )}

                    {batchPredictResponse && (
                      <div>
                        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
                          <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Batch Prediction Response</Text>
                          <Button
                            icon={<DownloadOutlined />}
                            onClick={() => downloadRowsAsCsv(batchPredictionRows, 'h2o-batch-prediction')}
                            disabled={batchPredictionRows.length === 0}
                          >
                            Download CSV
                          </Button>
                        </div>
                        {batchPredictionChartOption && (
                          <div style={{ marginTop: 8, border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: 8 }}>
                            <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Graphical View</Text>
                            <ReactECharts option={batchPredictionChartOption} style={{ height: 280, marginTop: 6 }} notMerge lazyUpdate />
                          </div>
                        )}
                        <pre style={jsonPreviewStyle}>{JSON.stringify(batchPredictResponse, null, 2)}</pre>
                      </div>
                    )}
                  </Space>
                ),
              },
              {
                key: 'evaluate',
                label: 'Evaluate',
                children: (
                  <Space direction="vertical" size={12} style={{ width: '100%' }}>
                    {h2oRuns.length === 0 && (
                      <Alert
                        type="warning"
                        showIcon
                        message="No H2O runs available"
                        description="Evaluate requires a trained run. Train once, then select run and source."
                      />
                    )}
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: 10 }}>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Select H2O Run</Text>
                        <Select
                          value={selectedH2ORun}
                          onChange={(value) => setSelectedH2ORun(value)}
                          options={h2oRunOptions}
                          loading={h2oRunsLoading}
                          style={{ width: '100%', marginTop: 6 }}
                          placeholder="Select run"
                        />
                      </div>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>
                          Target Column (required for regression/classification)
                        </Text>
                        <Select
                          mode="multiple"
                          value={evaluateTargetColumns}
                          onChange={(values: string[]) => {
                            setEvaluateTargetTouched(true)
                            setEvaluateTargetColumns(values)
                          }}
                          options={evaluateTargetOptions}
                          loading={evaluateSourceColumnsLoading}
                          showSearch
                          optionFilterProp="label"
                          maxTagCount="responsive"
                          placeholder={evaluateSourceColumnsLoading ? 'Loading columns…' : 'Select one or more targets'}
                          style={{ width: '100%', marginTop: 6 }}
                        />
                      </div>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Max Rows</Text>
                        <InputNumber min={1} max={200000} value={evaluateMaxRows} onChange={(v) => setEvaluateMaxRows(Number(v || 50000))} style={{ width: '100%', marginTop: 6 }} />
                      </div>
                    </div>
                    <Space wrap>
                      <Button onClick={applyRunDefaultsToForms} disabled={!selectedH2ORunDetail}>Use Selected Run Defaults</Button>
                      <Button onClick={() => applyTrainSourceToDownstream(true)}>Use Train Source</Button>
                      <Button onClick={loadExampleSourceData}>Load Example Source Data</Button>
                    </Space>
                    {selectedH2ORunDetail && (
                      <Space wrap size={6}>
                        {(selectedRunRequestedTask || selectedRunTask) && <Tag color="blue">Task: {selectedRunRequestedTask || selectedRunTask}</Tag>}
                        {selectedRunRequestedTask && selectedRunTask && selectedRunRequestedTask !== selectedRunTask && (
                          <Tag color="geekblue">Resolved: {selectedRunTask}</Tag>
                        )}
                        {selectedRunTarget && <Tag color="purple">Target: {selectedRunTarget}</Tag>}
                        {runFeatureColumns.length > 0 && <Tag color="green">Features: {runFeatureColumns.slice(0, 5).join(', ')}</Tag>}
                      </Space>
                    )}

                    {renderSourceConfig(evaluateSource, setEvaluateSource, 'eval-source')}
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8, flexWrap: 'wrap' }}>
                      <Text style={{ color: 'var(--app-text-dim)', fontSize: 12 }}>
                        Evaluate source columns detected: {evaluateSourceColumns.length}
                        {evaluateSourceColumnsProfile?.profiled_row_count ? ` · Profiled rows: ${evaluateSourceColumnsProfile.profiled_row_count}` : ''}
                      </Text>
                      <Button
                        size="small"
                        icon={<ReloadOutlined />}
                        onClick={() => void loadEvaluateSourceColumns()}
                        loading={evaluateSourceColumnsLoading}
                      >
                        Refresh Fields
                      </Button>
                    </div>
                    {evaluateSourceColumnsError && (
                      <Alert type="warning" showIcon message={evaluateSourceColumnsError} />
                    )}

                    <div>
                      <Button loading={evaluateLoading} onClick={runEvaluation}>Run Evaluation</Button>
                    </div>

                    {evaluateResponse && (
                      <div>
                        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
                          <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Evaluation Response (Actual vs Predicted)</Text>
                          <Button
                            icon={<DownloadOutlined />}
                            onClick={() => downloadRowsAsCsv(evaluateActualVsPredRows, 'h2o-evaluation-actual-vs-predicted')}
                            disabled={evaluateActualVsPredRows.length === 0}
                          >
                            Download CSV
                          </Button>
                        </div>
                        {evaluateChartOption && (
                          <div style={{ marginTop: 8, border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: 8 }}>
                            <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Graphical View</Text>
                            <ReactECharts option={evaluateChartOption} style={{ height: 320, marginTop: 6 }} notMerge lazyUpdate />
                          </div>
                        )}
                        <pre style={jsonPreviewStyle}>{JSON.stringify(evaluateResponse, null, 2)}</pre>
                      </div>
                    )}
                  </Space>
                ),
              },
              {
                key: 'runs',
                label: 'Runs',
                children: (
                  <Space direction="vertical" size={12} style={{ width: '100%' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 10, flexWrap: 'wrap' }}>
                      <Select
                        value={selectedH2ORun}
                        onChange={(value) => setSelectedH2ORun(value)}
                        options={h2oRunOptions}
                        loading={h2oRunsLoading}
                        style={{ minWidth: 380 }}
                        placeholder="Select run"
                      />
                      <Button icon={<ReloadOutlined />} onClick={() => void loadH2ORuns()} loading={h2oRunsLoading}>Refresh Runs</Button>
                    </div>

                    <Input
                      value={h2oRunSearch}
                      onChange={(e) => setH2ORunSearch(e.target.value)}
                      placeholder="Search run/model by id, label, status, task, target..."
                    />

                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: 10 }}>
                      <div style={{ gridColumn: 'span 2' }}>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Model Label</Text>
                        <Input
                          value={runLabelDraft}
                          onChange={(e) => setRunLabelDraft(e.target.value)}
                          placeholder="Give model a business-friendly label"
                          disabled={!selectedH2ORun}
                          style={{ marginTop: 6 }}
                        />
                      </div>
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Selected Run</Text>
                        <div style={{ marginTop: 8, color: 'var(--app-text-subtle)', fontSize: 12 }}>
                          {selectedH2ORun ? selectedH2ORun.slice(0, 8) : 'None'}
                        </div>
                      </div>
                    </div>

                    <Space wrap>
                      <Button onClick={useSelectedRunForEvaluate} disabled={!selectedH2ORun}>
                        Use Selected Run in Evaluate
                      </Button>
                      <Button loading={h2oRunPatchLoading} onClick={() => void saveSelectedRunLabel()} disabled={!selectedH2ORun}>
                        Save Label
                      </Button>
                      <Button danger loading={h2oRunDeleteLoading} onClick={() => void deleteSelectedRun()} disabled={!selectedH2ORun}>
                        Delete Run/Model
                      </Button>
                    </Space>

                    <div style={{
                      border: '1px solid var(--app-border-strong)',
                      borderRadius: 8,
                      overflow: 'auto',
                      maxHeight: 280,
                      padding: 8,
                    }}>
                      {filteredH2ORuns.length === 0 ? (
                        <Text style={{ color: 'var(--app-text-dim)', fontSize: 12 }}>No H2O runs found</Text>
                      ) : filteredH2ORuns.map((run) => (
                        <div
                          key={run.id}
                          onClick={() => setSelectedH2ORun(run.id)}
                          style={{
                            border: run.id === selectedH2ORun ? '1px solid #6366f1' : '1px solid var(--app-border)',
                            borderRadius: 8,
                            padding: '8px 10px',
                            marginBottom: 8,
                            cursor: 'pointer',
                            background: run.id === selectedH2ORun ? 'rgba(99,102,241,0.08)' : 'var(--app-panel-bg)',
                          }}
                        >
                          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, flexWrap: 'wrap' }}>
                            <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 600 }}>
                              {run.label || run.id.slice(0, 8)}
                            </Text>
                            <Space size={4} wrap>
                              <Tag color={run.status === 'success' ? 'success' : run.status === 'failed' ? 'error' : 'default'}>
                                {run.status || 'unknown'}
                              </Tag>
                              {run.task && <Tag color="blue">{run.task}</Tag>}
                              {run.target_column && <Tag color="purple">Target: {run.target_column}</Tag>}
                            </Space>
                          </div>
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                            ID: {run.id} {run.model_id ? ` · Model: ${run.model_id}` : ''}
                            {run.last_evaluated_at ? ` · Last Eval: ${formatLocalDateTime(run.last_evaluated_at)}` : ''}
                          </Text>
                          {!!run.output_fields?.length && (
                            <div style={{ marginTop: 4 }}>
                              <Text style={{ color: 'var(--app-text-dim)', fontSize: 11 }}>
                                Output fields: {run.output_fields.slice(0, 8).join(', ')}
                                {run.output_fields.length > 8 ? ' ...' : ''}
                              </Text>
                            </div>
                          )}
                        </div>
                      ))}
                    </div>

                    {selectedH2ORunDetail && (
                      <div>
                        <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Run Detail</Text>
                        <pre style={jsonPreviewStyle}>{JSON.stringify(selectedH2ORunDetail, null, 2)}</pre>
                      </div>
                    )}
                  </Space>
                ),
              },
            ]}
          />
        </Space>
      </Modal>
    </ReactFlowProvider>
  )
}
