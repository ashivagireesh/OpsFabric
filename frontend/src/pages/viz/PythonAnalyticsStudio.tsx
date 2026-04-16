import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Button,
  Divider,
  Empty,
  Input,
  InputNumber,
  Modal,
  Popconfirm,
  Select,
  Segmented,
  Spin,
  Table,
  Tag,
  Typography,
  message,
} from 'antd'
import {
  CodeOutlined,
  CopyOutlined,
  DatabaseOutlined,
  DeleteOutlined,
  HolderOutlined,
  PlayCircleOutlined,
  PlusOutlined,
  ReloadOutlined,
  SaveOutlined,
} from '@ant-design/icons'
import axios from 'axios'
import React from 'react'
import { useSearchParams } from 'react-router-dom'
import { ResponsiveGridLayout as RGLResponsive } from 'react-grid-layout'
import { useContainerWidth } from 'react-grid-layout'
import 'react-grid-layout/css/styles.css'
import 'react-resizable/css/styles.css'
import type { LayoutItem } from '../../store/vizStore'

const { Text } = Typography
const RGL = RGLResponsive as unknown as React.ComponentType<any>

const ROW_HEIGHT = 30
const MARGIN: [number, number] = [8, 8]
const COLS: Record<string, number> = { lg: 24, md: 20, sm: 12, xs: 8, xxs: 4 }
const PY_DASHBOARDS_API = '/api/python-analytics/dashboards'

const API_BASE = import.meta.env.VITE_API_BASE || (import.meta.env.DEV ? 'http://localhost:8001' : '')
const http = axios.create({ baseURL: API_BASE, timeout: 60000 })

type SourceType = 'sample' | 'pipeline' | 'file' | 'mlops'

interface PipelineOption {
  value: string
  label: string
}

interface PipelineNodeOption {
  value: string
  label: string
}

interface MLOpsRunOption {
  value: string
  label: string
}

interface SourceConfig {
  source_type: SourceType
  dataset?: string
  pipeline_id?: string
  pipeline_node_id?: string
  file_path?: string
  mlops_workflow_id?: string
  mlops_run_id?: string
  mlops_node_id?: string
  mlops_output_mode?: 'predictions' | 'metrics' | 'monitor' | 'evaluation'
  mlops_prediction_start_date?: string
  mlops_prediction_end_date?: string
  sql?: string
}

interface PythonChartConfig {
  chart_type: 'auto' | 'line' | 'bar' | 'scatter' | 'histogram' | 'box' | 'heatmap' | 'table'
  x_field?: string
  y_field?: string
  color_field?: string
  group_by?: string
  title?: string
  limit: number
}

interface PythonVizResult {
  status: string
  source_type: string
  chart_type: string
  x_field?: string | null
  y_field?: string | null
  color_field?: string | null
  group_by?: string | null
  title?: string
  row_count: number
  sampled_row_count: number
  columns: string[]
  preview_rows: Record<string, unknown>[]
  column_profile?: {
    numeric?: string[]
    datetime?: string[]
    categorical?: string[]
  }
  figure_html: string
}

interface FieldSchema {
  columns: string[]
  metrics: string[]
  dimensions: string[]
}

interface PythonDashboardWidget {
  id: string
  title: string
  source: SourceConfig
  config: PythonChartConfig
  result?: PythonVizResult
  loading?: boolean
  error?: string
}

type DrillMode = 'down' | 'through'

interface DrillStep {
  field: string
  value: string | number | boolean
}

interface PythonDashboardListItem {
  id: string
  name: string
  description?: string
  owner?: string
  theme?: string
  tags?: string[]
  widget_count?: number
  is_public?: boolean
  share_token?: string | null
  created_at?: string | null
  updated_at?: string | null
}

interface PythonDashboardDetail extends PythonDashboardListItem {
  widgets: Array<{
    id: string
    title: string
    source?: SourceConfig
    config?: PythonChartConfig
  }>
  layout: LayoutItem[]
  global_filters?: any[]
}

const SAMPLE_DATASETS = [
  { value: 'sales', label: 'Sales' },
  { value: 'web_analytics', label: 'Web Analytics' },
  { value: 'financials', label: 'Financials' },
  { value: 'employees', label: 'Employees' },
  { value: 'supply_chain', label: 'Supply Chain' },
  { value: 'customer', label: 'Customer' },
]

function uid(prefix: string): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return `${prefix}-${crypto.randomUUID()}`
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
}

function computeBodyHeight(h: number): number {
  return h * ROW_HEIGHT + (h - 1) * MARGIN[1] - 30 - 2
}

function nextLayoutY(layout: LayoutItem[]): number {
  return layout.reduce((maxY, item) => Math.max(maxY, item.y + item.h), 0)
}

function normalizeLayout(layout: LayoutItem[]): LayoutItem[] {
  return layout.map((item, idx) => {
    const minW = Math.max(2, Number(item.minW || 4))
    const minH = Math.max(2, Number(item.minH || 3))
    const width = Math.max(minW, Math.min(24, Number(item.w || 12)))
    const height = Math.max(minH, Number(item.h || 9))
    const x = Math.max(0, Math.min(24 - width, Number(item.x || 0)))
    const y = Math.max(0, Number(item.y ?? idx * 3))
    return {
      i: String(item.i),
      x,
      y,
      w: width,
      h: height,
      minW,
      minH,
    }
  })
}

function defaultSource(): SourceConfig {
  return {
    source_type: 'sample',
    dataset: 'sales',
    mlops_output_mode: 'predictions',
  }
}

function defaultConfig(): PythonChartConfig {
  return {
    chart_type: 'auto',
    x_field: '',
    y_field: '',
    color_field: '',
    group_by: '',
    title: '',
    limit: 5000,
  }
}

function formatRunTimestamp(value: unknown): string {
  if (!value) return 'unknown time'
  try {
    return new Date(String(value)).toLocaleString()
  } catch {
    return String(value)
  }
}

function mapStandardMLOpsRunOptions(rows: unknown[]): MLOpsRunOption[] {
  return (Array.isArray(rows) ? rows : [])
    .filter((run: any) => run?.id)
    .map((run: any) => {
      const version = String(run?.model_version || 'model')
      const status = String(run?.status || 'unknown')
      const started = formatRunTimestamp(run?.started_at)
      return { value: String(run.id), label: `${version} • ${started} (${status})` }
    })
}

function mapH2OMLOpsRunOptions(rows: unknown[]): MLOpsRunOption[] {
  return (Array.isArray(rows) ? rows : [])
    .filter((run: any) => run?.id)
    .map((run: any) => {
      const modelLabel = String(run?.label || '').trim()
      const task = String(run?.task || 'auto').toUpperCase()
      const target = String(run?.target_column || '').trim()
      const status = String(run?.status || 'unknown')
      const created = formatRunTimestamp(run?.created_at || run?.updated_at || run?.finished_at)
      const evaluatedAt = formatRunTimestamp(run?.last_evaluated_at)
      const targetSuffix = target ? ` • ${target}` : ''
      return {
        value: String(run.id),
        label: `${modelLabel ? `${modelLabel} • ` : ''}H2O ${task}${targetSuffix} • ${created}${run?.last_evaluated_at ? ` • Eval ${evaluatedAt}` : ''} (${status})`,
      }
    })
}

async function loadMLOpsRunOptions(workflowId: string): Promise<MLOpsRunOption[]> {
  const trimmedWorkflowId = String(workflowId || '').trim()
  if (!trimmedWorkflowId) return []

  const standardPromise = http.get('/api/mlops/runs', { params: { workflow_id: trimmedWorkflowId } })
  const scopedH2OPromise = http.get('/api/mlops/h2o/runs', { params: { workflow_id: trimmedWorkflowId, limit: 100 } })
  const [standardResult, scopedH2OResult] = await Promise.allSettled([standardPromise, scopedH2OPromise])

  const standardRows = standardResult.status === 'fulfilled' && Array.isArray(standardResult.value.data)
    ? standardResult.value.data
    : []

  let h2oRows = scopedH2OResult.status === 'fulfilled' && Array.isArray(scopedH2OResult.value.data)
    ? scopedH2OResult.value.data
    : []

  if (h2oRows.length === 0) {
    const globalResult = await Promise.allSettled([http.get('/api/mlops/h2o/runs', { params: { limit: 100 } })])
    const globalRows = globalResult[0]
    if (globalRows.status === 'fulfilled' && Array.isArray(globalRows.value.data)) {
      h2oRows = globalRows.value.data
    }
  }

  return [...mapStandardMLOpsRunOptions(standardRows), ...mapH2OMLOpsRunOptions(h2oRows)]
}

function createWidget(index: number): PythonDashboardWidget {
  return {
    id: uid('py-widget'),
    title: `Python Widget ${index + 1}`,
    source: defaultSource(),
    config: defaultConfig(),
  }
}

function createWidgetLayout(widgetId: string, layout: LayoutItem[]): LayoutItem {
  const baseIndex = layout.length
  return {
    i: widgetId,
    x: (baseIndex * 6) % 24,
    y: nextLayoutY(layout),
    w: 12,
    h: 10,
    minW: 4,
    minH: 4,
  }
}

function sourcePayload(source: SourceConfig): Record<string, unknown> {
  return {
    source_type: source.source_type,
    dataset: source.dataset,
    pipeline_id: source.pipeline_id,
    pipeline_node_id: source.pipeline_node_id,
    file_path: source.file_path,
    mlops_workflow_id: source.mlops_workflow_id,
    mlops_run_id: source.mlops_run_id,
    mlops_node_id: source.mlops_node_id,
    mlops_output_mode: source.mlops_output_mode,
    mlops_prediction_start_date: source.mlops_prediction_start_date,
    mlops_prediction_end_date: source.mlops_prediction_end_date,
    sql: source.sql,
  }
}

function sourceMissing(source: SourceConfig): boolean {
  if (source.source_type === 'pipeline' && !source.pipeline_id) return true
  if (source.source_type === 'file' && !(source.file_path || '').trim()) return true
  if (source.source_type === 'mlops' && !source.mlops_workflow_id) return true
  return false
}

function extractDrillSelection(
  widget: PythonDashboardWidget | null,
  payload: Record<string, unknown> | null,
): DrillStep | null {
  if (!widget || !payload) return null

  const primaryField = String(widget.result?.x_field || widget.config.x_field || widget.result?.group_by || widget.config.group_by || '').trim()
  const fallbackField = String(widget.result?.group_by || widget.config.group_by || '').trim()

  const xValue = payload.x
  const labelValue = payload.label
  const textValue = payload.text
  const yValue = payload.y

  const firstValue = xValue ?? labelValue ?? textValue ?? yValue
  if (!primaryField && !fallbackField) return null
  if (firstValue === null || firstValue === undefined || String(firstValue).trim() === '') return null

  const field = primaryField || fallbackField
  if (!field) return null

  if (typeof firstValue === 'number' || typeof firstValue === 'boolean') {
    return { field, value: firstValue }
  }
  return { field, value: String(firstValue) }
}

function toRGL(layout: LayoutItem[]): Record<string, any[]> {
  const base = layout.map((item) => ({
    i: item.i,
    x: item.x,
    y: item.y,
    w: item.w,
    h: item.h,
    minW: item.minW,
    minH: item.minH,
  }))
  const clone = () => base.map((item) => ({ ...item }))
  return {
    lg: clone(),
    md: clone(),
    sm: clone().map((item) => ({ ...item, w: Math.min(item.w, 20), x: Math.min(item.x, 20) })),
    xs: clone().map((item) => ({ ...item, w: Math.min(item.w, 8), x: 0 })),
    xxs: clone().map((item) => ({ ...item, w: Math.min(item.w, 4), x: 0 })),
  }
}

function normalizeWidgetFromApi(raw: any, idx: number): PythonDashboardWidget {
  const source = (raw?.source && typeof raw.source === 'object')
    ? (raw.source as SourceConfig)
    : defaultSource()
  const config = (raw?.config && typeof raw.config === 'object')
    ? ({ ...defaultConfig(), ...(raw.config as PythonChartConfig) })
    : defaultConfig()

  return {
    id: String(raw?.id || uid(`py-widget-${idx + 1}`)),
    title: String(raw?.title || `Python Widget ${idx + 1}`),
    source,
    config,
  }
}

function serializeWidgetsForSave(widgets: PythonDashboardWidget[]): Array<{
  id: string
  title: string
  source: SourceConfig
  config: PythonChartConfig
}> {
  return widgets.map((widget) => ({
    id: widget.id,
    title: widget.title,
    source: widget.source,
    config: widget.config,
  }))
}

function hydrateLayoutWithWidgets(
  widgetList: PythonDashboardWidget[],
  rawLayout: unknown,
): LayoutItem[] {
  const incoming = Array.isArray(rawLayout) ? normalizeLayout(rawLayout as LayoutItem[]) : []
  const allowed = new Set(widgetList.map((widget) => widget.id))
  const filtered = incoming.filter((item) => allowed.has(item.i))
  const used = new Set(filtered.map((item) => item.i))
  widgetList.forEach((widget) => {
    if (!used.has(widget.id)) {
      filtered.push(createWidgetLayout(widget.id, filtered))
      used.add(widget.id)
    }
  })
  return normalizeLayout(filtered)
}

export default function PythonAnalyticsStudio() {
  const [searchParams, setSearchParams] = useSearchParams()
  const requestedDashboardId = (searchParams.get('dashboardId') || '').trim()
  const [messageApi, contextHolder] = message.useMessage()
  const [dashboardName, setDashboardName] = useState('Python Analytics Dashboard')
  const [widgets, setWidgets] = useState<PythonDashboardWidget[]>([])
  const [layout, setLayout] = useState<LayoutItem[]>([])
  const [selectedWidgetId, setSelectedWidgetId] = useState<string | null>(null)
  const [dirty, setDirty] = useState(false)
  const [activeDashboardId, setActiveDashboardId] = useState<string | null>(null)
  const [dashboards, setDashboards] = useState<PythonDashboardListItem[]>([])
  const [dashboardsLoading, setDashboardsLoading] = useState(false)
  const [dashboardLoading, setDashboardLoading] = useState(false)
  const [dashboardSaving, setDashboardSaving] = useState(false)
  const [drillMode, setDrillMode] = useState<DrillMode>('down')
  const [drillPath, setDrillPath] = useState<DrillStep[]>([])
  const [drillThroughOpen, setDrillThroughOpen] = useState(false)
  const [drillThroughRows, setDrillThroughRows] = useState<Record<string, unknown>[]>([])
  const [drillThroughColumns, setDrillThroughColumns] = useState<string[]>([])
  const [drillThroughTitle, setDrillThroughTitle] = useState('Drill Through')
  const [drillThroughLoading, setDrillThroughLoading] = useState(false)

  const [pipelinesLoading, setPipelinesLoading] = useState(false)
  const [pipelineOptions, setPipelineOptions] = useState<PipelineOption[]>([])
  const [pipelineNodesLoading, setPipelineNodesLoading] = useState(false)
  const [pipelineNodeOptions, setPipelineNodeOptions] = useState<PipelineNodeOption[]>([])

  const [mlopsWorkflowsLoading, setMLOpsWorkflowsLoading] = useState(false)
  const [mlopsWorkflowOptions, setMLOpsWorkflowOptions] = useState<PipelineOption[]>([])
  const [mlopsRunsLoading, setMLOpsRunsLoading] = useState(false)
  const [mlopsRunOptions, setMLOpsRunOptions] = useState<MLOpsRunOption[]>([])
  const [mlopsNodesLoading, setMLOpsNodesLoading] = useState(false)
  const [mlopsNodeOptions, setMLOpsNodeOptions] = useState<PipelineNodeOption[]>([])

  const [schemaLoading, setSchemaLoading] = useState(false)
  const [schema, setSchema] = useState<FieldSchema | null>(null)
  const iframeRefs = React.useRef<Record<string, HTMLIFrameElement | null>>({})

  const {
    width: gridWidth,
    containerRef: gridContainerRef,
    mounted: gridMounted,
  } = useContainerWidth({ initialWidth: 1280, measureBeforeMount: false })

  const selectedWidget = useMemo(
    () => widgets.find((widget) => widget.id === selectedWidgetId) || null,
    [widgets, selectedWidgetId],
  )

  const selectedLayout = useMemo(
    () => layout.find((item) => item.i === selectedWidgetId),
    [layout, selectedWidgetId],
  )

  const widgetLayoutMap = useMemo(() => {
    const map = new Map<string, LayoutItem>()
    layout.forEach((item) => map.set(item.i, item))
    return map
  }, [layout])

  const rglLayouts = useMemo(() => toRGL(layout), [layout])

  const schemaColumns = useMemo(() => {
    const base = schema?.columns || []
    const resultCols = Array.isArray(selectedWidget?.result?.columns) ? selectedWidget.result.columns : []
    return Array.from(new Set([...base, ...resultCols].map((col) => String(col).trim()).filter(Boolean)))
  }, [schema?.columns, selectedWidget?.result?.columns])

  const schemaMetrics = useMemo(() => {
    const profileMetrics = Array.isArray(selectedWidget?.result?.column_profile?.numeric)
      ? selectedWidget.result.column_profile.numeric
      : []
    const merged = Array.from(new Set([...(schema?.metrics || []), ...profileMetrics].map((col) => String(col).trim()).filter(Boolean)))
    return merged.length > 0 ? merged : schemaColumns
  }, [schema?.metrics, selectedWidget?.result?.column_profile?.numeric, schemaColumns])

  const schemaDimensions = useMemo(() => {
    const dateCols = Array.isArray(selectedWidget?.result?.column_profile?.datetime)
      ? selectedWidget.result.column_profile.datetime
      : []
    const catCols = Array.isArray(selectedWidget?.result?.column_profile?.categorical)
      ? selectedWidget.result.column_profile.categorical
      : []
    const merged = Array.from(new Set([...(schema?.dimensions || []), ...dateCols, ...catCols].map((col) => String(col).trim()).filter(Boolean)))
    return merged.length > 0 ? merged : schemaColumns
  }, [schema?.dimensions, selectedWidget?.result?.column_profile?.datetime, selectedWidget?.result?.column_profile?.categorical, schemaColumns])

  const drillPathLabel = useMemo(
    () => drillPath.map((step) => `${step.field}: ${String(step.value)}`).join(' / '),
    [drillPath],
  )

  const commitLayout = useCallback((nextLayout: any[]) => {
    const mapped: LayoutItem[] = normalizeLayout(
      nextLayout.map((item: any) => ({
        i: String(item.i),
        x: Number(item.x || 0),
        y: Number(item.y || 0),
        w: Number(item.w || 12),
        h: Number(item.h || 9),
        minW: Number(item.minW || 4),
        minH: Number(item.minH || 4),
      })),
    )
    setLayout(mapped)
    setDirty(true)
  }, [])

  const updateWidget = useCallback((widgetId: string, updater: (widget: PythonDashboardWidget) => PythonDashboardWidget) => {
    setWidgets((prev) => prev.map((widget) => (widget.id === widgetId ? updater(widget) : widget)))
  }, [])

  const runWidget = useCallback(async (widgetId: string, silent = false, overrideDrillPath?: DrillStep[]) => {
    const target = widgets.find((widget) => widget.id === widgetId)
    if (!target) return

    if (sourceMissing(target.source)) {
      if (!silent) messageApi.warning('Configure datasource before running widget')
      return
    }

    const activeDrillPath = Array.isArray(overrideDrillPath) ? overrideDrillPath : drillPath
    const drillFilters = activeDrillPath.map((step) => ({
      field: step.field,
      value: step.value,
    }))

    updateWidget(widgetId, (widget) => ({ ...widget, loading: true, error: undefined }))
    try {
      const payload = {
        ...sourcePayload(target.source),
        chart_type: target.config.chart_type,
        x_field: (target.config.x_field || '').trim() || undefined,
        y_field: (target.config.y_field || '').trim() || undefined,
        color_field: (target.config.color_field || '').trim() || undefined,
        group_by: (target.config.group_by || '').trim() || undefined,
        title: (target.config.title || '').trim() || undefined,
        limit: target.config.limit,
        drill_filters: drillFilters,
      }
      const response = await http.post('/api/analytics/python-visualization', payload)
      const result = response.data as PythonVizResult
      updateWidget(widgetId, (widget) => ({
        ...widget,
        result,
        title: (widget.title || '').trim() ? widget.title : (result.title || widget.title),
        loading: false,
        error: undefined,
      }))
      if (!silent) messageApi.success('Python widget updated')
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      const text = typeof detail === 'string' ? detail : 'Failed to run python visualization'
      updateWidget(widgetId, (widget) => ({ ...widget, loading: false, error: text }))
      if (!silent) messageApi.error(text)
    }
  }, [widgets, messageApi, updateWidget, drillPath])

  const runAllWidgets = useCallback(async (silent = false, overrideDrillPath?: DrillStep[]) => {
    if (widgets.length === 0) {
      if (!silent) messageApi.info('No widgets to run')
      return
    }
    for (const widget of widgets) {
      // Sequential execution keeps backend load stable for large widgets.
      // eslint-disable-next-line no-await-in-loop
      await runWidget(widget.id, true, overrideDrillPath)
    }
    if (!silent) messageApi.success('All python widgets refreshed')
  }, [widgets, runWidget, messageApi])

  const resetDashboardDrill = useCallback(async () => {
    setDrillPath([])
    await runAllWidgets(true, [])
    messageApi.success('Dashboard drill reset')
  }, [runAllWidgets, messageApi])

  const stepUpDashboardDrill = useCallback(async () => {
    if (drillPath.length === 0) return
    const nextPath = drillPath.slice(0, -1)
    setDrillPath(nextPath)
    await runAllWidgets(true, nextPath)
    messageApi.success('Dashboard drill level updated')
  }, [drillPath, runAllWidgets, messageApi])

  const openDrillThroughRows = useCallback(async (widget: PythonDashboardWidget, step: DrillStep) => {
    setDrillThroughOpen(true)
    setDrillThroughTitle(`Drill Through - ${widget.title}`)
    setDrillThroughLoading(true)
    setDrillThroughRows([])
    setDrillThroughColumns([])
    try {
      const drillFilters = [...drillPath, step].map((item) => ({
        field: item.field,
        value: item.value,
      }))
      const response = await http.post('/api/query', {
        ...sourcePayload(widget.source),
        drill_filters: drillFilters,
      })
      const rows = Array.isArray(response.data?.data)
        ? response.data.data.filter((row: unknown) => !!row && typeof row === 'object')
        : []
      const columns = Array.isArray(response.data?.columns)
        ? response.data.columns.map((col: unknown) => String(col))
        : (rows.length > 0 ? Object.keys(rows[0] as Record<string, unknown>) : [])
      setDrillThroughRows(rows.slice(0, 800))
      setDrillThroughColumns(columns.slice(0, 20))
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      messageApi.error(typeof detail === 'string' ? detail : 'Failed to load drill-through rows')
    } finally {
      setDrillThroughLoading(false)
    }
  }, [drillPath, messageApi])

  const addNewWidget = useCallback(() => {
    const widget = createWidget(widgets.length)
    const widgetLayout = createWidgetLayout(widget.id, layout)
    setWidgets((prev) => [...prev, widget])
    setLayout((prev) => [...prev, widgetLayout])
    setSelectedWidgetId(widget.id)
    setDirty(true)
  }, [widgets.length, layout])

  const duplicateSelectedWidget = useCallback(() => {
    if (!selectedWidget) return
    const copy: PythonDashboardWidget = {
      ...selectedWidget,
      id: uid('py-widget'),
      title: `${selectedWidget.title} Copy`,
      loading: false,
      error: undefined,
    }
    const widgetLayout = createWidgetLayout(copy.id, layout)
    setWidgets((prev) => [...prev, copy])
    setLayout((prev) => [...prev, widgetLayout])
    setSelectedWidgetId(copy.id)
    setDirty(true)
    messageApi.success('Widget duplicated')
  }, [selectedWidget, layout, messageApi])

  const removeWidget = useCallback((widgetId: string) => {
    delete iframeRefs.current[widgetId]
    setWidgets((prev) => prev.filter((widget) => widget.id !== widgetId))
    setLayout((prev) => prev.filter((item) => item.i !== widgetId))
    if (selectedWidgetId === widgetId) {
      const remaining = widgets.filter((widget) => widget.id !== widgetId)
      setSelectedWidgetId(remaining.length > 0 ? remaining[0].id : null)
      setSchema(null)
    }
    setDirty(true)
  }, [selectedWidgetId, widgets])

  const createBlankDashboard = useCallback((clearUrl = true) => {
    const widget = createWidget(0)
    setActiveDashboardId(null)
    if (clearUrl) setSearchParams({}, { replace: true })
    setDashboardName('Python Analytics Dashboard')
    setWidgets([widget])
    setLayout([createWidgetLayout(widget.id, [])])
    setSelectedWidgetId(widget.id)
    setSchema(null)
    setDrillPath([])
    setDirty(false)
  }, [setSearchParams])

  const applyDashboardDetail = useCallback((detail: PythonDashboardDetail, nextDashboardId: string | null) => {
    const rawWidgets = Array.isArray(detail.widgets) ? detail.widgets : []
    const nextWidgets = rawWidgets.length > 0
      ? rawWidgets.map((widget, idx) => normalizeWidgetFromApi(widget, idx))
      : [createWidget(0)]
    const nextLayout = hydrateLayoutWithWidgets(nextWidgets, detail.layout)
    setActiveDashboardId(nextDashboardId)
    setDashboardName((detail.name || '').trim() || 'Python Analytics Dashboard')
    setWidgets(nextWidgets)
    setLayout(nextLayout)
    setSelectedWidgetId(nextWidgets.length > 0 ? nextWidgets[0].id : null)
    setSchema(null)
    setDrillPath([])
    setDirty(false)
  }, [])

  const fetchDashboards = useCallback(async (silent = false): Promise<PythonDashboardListItem[]> => {
    setDashboardsLoading(true)
    try {
      const response = await http.get(PY_DASHBOARDS_API)
      const rows = Array.isArray(response.data)
        ? response.data
            .filter((item: any) => item?.id && item?.name)
            .map((item: any) => ({
              id: String(item.id),
              name: String(item.name),
              description: item.description ? String(item.description) : '',
              owner: item.owner ? String(item.owner) : 'admin',
              theme: item.theme ? String(item.theme) : 'dark',
              tags: Array.isArray(item.tags) ? item.tags.map((tag: unknown) => String(tag)) : [],
              widget_count: Number(item.widget_count || 0),
              is_public: Boolean(item.is_public),
              share_token: item.share_token ? String(item.share_token) : null,
              created_at: item.created_at ? String(item.created_at) : null,
              updated_at: item.updated_at ? String(item.updated_at) : null,
            }))
        : []
      setDashboards(rows)
      return rows
    } catch (error: any) {
      setDashboards([])
      if (!silent) {
        const detail = error?.response?.data?.detail
        messageApi.error(typeof detail === 'string' ? detail : 'Failed to load python dashboards')
      }
      return []
    } finally {
      setDashboardsLoading(false)
    }
  }, [messageApi])

  const loadDashboard = useCallback(async (dashboardId: string, silent = false): Promise<boolean> => {
    if (!dashboardId) return false
    setDashboardLoading(true)
    try {
      const response = await http.get(`${PY_DASHBOARDS_API}/${dashboardId}`)
      const detail = (response.data || {}) as PythonDashboardDetail
      applyDashboardDetail(detail, dashboardId)
      if (!silent) messageApi.success('Python dashboard loaded')
      return true
    } catch (error: any) {
      if (!silent) {
        const detail = error?.response?.data?.detail
        messageApi.error(typeof detail === 'string' ? detail : 'Failed to load python dashboard')
      }
      return false
    } finally {
      setDashboardLoading(false)
    }
  }, [applyDashboardDetail, messageApi])

  const saveDashboard = useCallback(async () => {
    if (widgets.length === 0) {
      messageApi.warning('Add at least one widget before saving')
      return
    }
    setDashboardSaving(true)
    try {
      const normalizedName = (dashboardName || '').trim() || 'Python Analytics Dashboard'
      const payload = {
        name: normalizedName,
        description: '',
        theme: 'dark',
        widgets: serializeWidgetsForSave(widgets),
        layout: normalizeLayout(layout),
        global_filters: [],
      }
      let savedId = activeDashboardId
      if (savedId) {
        await http.put(`${PY_DASHBOARDS_API}/${savedId}`, payload)
      } else {
        const response = await http.post(PY_DASHBOARDS_API, payload)
        savedId = response?.data?.id ? String(response.data.id) : null
      }
      if (savedId) setActiveDashboardId(savedId)
      if (savedId) setSearchParams({ dashboardId: savedId }, { replace: true })
      await fetchDashboards(true)
      setDashboardName(normalizedName)
      setDirty(false)
      messageApi.success(savedId && activeDashboardId ? 'Dashboard updated' : 'Dashboard saved')
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      messageApi.error(typeof detail === 'string' ? detail : 'Failed to save dashboard')
    } finally {
      setDashboardSaving(false)
    }
  }, [widgets, dashboardName, layout, activeDashboardId, fetchDashboards, messageApi, setSearchParams])

  const duplicateDashboard = useCallback(async () => {
    if (!activeDashboardId) {
      messageApi.warning('Save dashboard first, then duplicate')
      return
    }
    setDashboardSaving(true)
    try {
      const response = await http.post(`${PY_DASHBOARDS_API}/${activeDashboardId}/duplicate`)
      const duplicateId = response?.data?.id ? String(response.data.id) : ''
      const rows = await fetchDashboards(true)
      if (duplicateId) {
        setSearchParams({ dashboardId: duplicateId }, { replace: true })
        const loaded = await loadDashboard(duplicateId, true)
        if (!loaded && rows.length > 0) {
          setSearchParams({ dashboardId: rows[0].id }, { replace: true })
          await loadDashboard(rows[0].id, true)
        }
      }
      messageApi.success('Dashboard duplicated')
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      messageApi.error(typeof detail === 'string' ? detail : 'Failed to duplicate dashboard')
    } finally {
      setDashboardSaving(false)
    }
  }, [activeDashboardId, fetchDashboards, loadDashboard, messageApi, setSearchParams])

  const deleteDashboard = useCallback(async () => {
    if (!activeDashboardId) return
    setDashboardSaving(true)
    try {
      await http.delete(`${PY_DASHBOARDS_API}/${activeDashboardId}`)
      const rows = await fetchDashboards(true)
      if (rows.length > 0) {
        setSearchParams({ dashboardId: rows[0].id }, { replace: true })
        await loadDashboard(rows[0].id, true)
      } else {
        createBlankDashboard()
      }
      messageApi.success('Dashboard deleted')
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      messageApi.error(typeof detail === 'string' ? detail : 'Failed to delete dashboard')
    } finally {
      setDashboardSaving(false)
    }
  }, [activeDashboardId, fetchDashboards, loadDashboard, createBlankDashboard, messageApi, setSearchParams])

  const resetDashboard = useCallback(async () => {
    if (activeDashboardId) {
      const loaded = await loadDashboard(activeDashboardId, true)
      if (loaded) {
        messageApi.success('Unsaved changes reset')
      }
      return
    }
    createBlankDashboard()
    messageApi.success('Reset to new dashboard draft')
  }, [activeDashboardId, loadDashboard, createBlankDashboard, messageApi])

  const createNewDashboard = useCallback(() => {
    createBlankDashboard()
    messageApi.info('New dashboard draft')
  }, [createBlankDashboard, messageApi])

  const handleDashboardSelect = useCallback((dashboardId: string) => {
    if (!dashboardId) return
    setSearchParams({ dashboardId }, { replace: true })
  }, [setSearchParams])

  const updateSelectedSource = useCallback((patch: Partial<SourceConfig>) => {
    if (!selectedWidget) return
    updateWidget(selectedWidget.id, (widget) => ({
      ...widget,
      source: { ...widget.source, ...patch },
    }))
    setSchema(null)
    setDirty(true)
  }, [selectedWidget, updateWidget])

  const updateSelectedConfig = useCallback((patch: Partial<PythonChartConfig>) => {
    if (!selectedWidget) return
    updateWidget(selectedWidget.id, (widget) => ({
      ...widget,
      config: { ...widget.config, ...patch },
    }))
    setDirty(true)
  }, [selectedWidget, updateWidget])

  const updateSelectedTitle = useCallback((title: string) => {
    if (!selectedWidget) return
    updateWidget(selectedWidget.id, (widget) => ({ ...widget, title }))
    setDirty(true)
  }, [selectedWidget, updateWidget])

  const handleSourceTypeChange = useCallback((nextType: SourceType) => {
    if (!selectedWidget) return
    if (nextType === 'sample') {
      updateSelectedSource({
        source_type: nextType,
        dataset: selectedWidget.source.dataset || 'sales',
        pipeline_id: undefined,
        pipeline_node_id: undefined,
        file_path: undefined,
        mlops_workflow_id: undefined,
        mlops_run_id: undefined,
        mlops_node_id: undefined,
        mlops_prediction_start_date: undefined,
        mlops_prediction_end_date: undefined,
      })
      return
    }
    if (nextType === 'pipeline') {
      updateSelectedSource({
        source_type: nextType,
        dataset: undefined,
        file_path: undefined,
        mlops_workflow_id: undefined,
        mlops_run_id: undefined,
        mlops_node_id: undefined,
        mlops_prediction_start_date: undefined,
        mlops_prediction_end_date: undefined,
      })
      return
    }
    if (nextType === 'mlops') {
      updateSelectedSource({
        source_type: nextType,
        dataset: undefined,
        pipeline_id: undefined,
        pipeline_node_id: undefined,
        file_path: undefined,
        mlops_output_mode: selectedWidget.source.mlops_output_mode || 'predictions',
      })
      return
    }
    updateSelectedSource({
      source_type: nextType,
      dataset: undefined,
      pipeline_id: undefined,
      pipeline_node_id: undefined,
      mlops_workflow_id: undefined,
      mlops_run_id: undefined,
      mlops_node_id: undefined,
      mlops_prediction_start_date: undefined,
      mlops_prediction_end_date: undefined,
    })
  }, [selectedWidget, updateSelectedSource])

  const loadFieldSchema = useCallback(async (widget: PythonDashboardWidget | null, silent = false) => {
    if (!widget) return
    if (sourceMissing(widget.source)) {
      if (!silent) messageApi.warning('Configure datasource before loading fields')
      return
    }
    setSchemaLoading(true)
    try {
      const response = await http.post('/api/nlp-chat/schema', {
        ...sourcePayload(widget.source),
        sample_size: 800,
      })
      const columns = Array.isArray(response.data?.columns)
        ? response.data.columns.map((col: unknown) => String(col))
        : []
      const metrics = Array.isArray(response.data?.suggested_metrics)
        ? response.data.suggested_metrics.map((col: unknown) => String(col))
        : []
      const dimensions = Array.isArray(response.data?.suggested_dimensions)
        ? response.data.suggested_dimensions.map((col: unknown) => String(col))
        : []
      setSchema({ columns, metrics, dimensions })

      updateWidget(widget.id, (prevWidget) => {
        const nextX = prevWidget.config.x_field || dimensions[0] || columns[0] || ''
        const nextY = prevWidget.config.y_field || metrics[0] || columns[1] || columns[0] || ''
        const nextColor = prevWidget.config.color_field || dimensions[1] || ''
        if (
          prevWidget.config.x_field === nextX
          && prevWidget.config.y_field === nextY
          && prevWidget.config.color_field === nextColor
        ) {
          return prevWidget
        }
        return {
          ...prevWidget,
          config: {
            ...prevWidget.config,
            x_field: nextX,
            y_field: nextY,
            color_field: nextColor,
          },
        }
      })

      if (!silent) messageApi.success(`${columns.length} fields loaded`)
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      if (!silent) messageApi.error(typeof detail === 'string' ? detail : 'Failed to load fields')
      setSchema(null)
    } finally {
      setSchemaLoading(false)
    }
  }, [messageApi, updateWidget])

  useEffect(() => {
    let mounted = true
    const boot = async () => {
      const rows = await fetchDashboards(true)
      if (!mounted) return
      if (rows.length === 0) {
        createBlankDashboard()
        return
      }

      const hasRequested = Boolean(requestedDashboardId)
      const targetId = hasRequested && rows.some((row) => row.id === requestedDashboardId)
        ? requestedDashboardId
        : rows[0].id
      const loaded = await loadDashboard(targetId, true)
      if (!mounted) return
      if (!loaded) {
        createBlankDashboard()
        return
      }
      if (targetId !== requestedDashboardId) {
        setSearchParams({ dashboardId: targetId }, { replace: true })
      }
    }
    void boot()
    return () => {
      mounted = false
    }
  }, [fetchDashboards, loadDashboard, createBlankDashboard, setSearchParams])

  useEffect(() => {
    if (!requestedDashboardId || requestedDashboardId === activeDashboardId) return
    if (dashboardLoading || dashboardsLoading) return
    if (!dashboards.some((row) => row.id === requestedDashboardId)) return
    void loadDashboard(requestedDashboardId, true)
  }, [requestedDashboardId, activeDashboardId, dashboards, dashboardLoading, dashboardsLoading, loadDashboard])

  useEffect(() => {
    let mounted = true
    const loadSources = async () => {
      setPipelinesLoading(true)
      setMLOpsWorkflowsLoading(true)
      try {
        const [pipelineResp, mlopsResp] = await Promise.all([
          http.get('/api/pipelines'),
          http.get('/api/mlops/workflows'),
        ])
        if (!mounted) return

        const pOptions = Array.isArray(pipelineResp.data)
          ? pipelineResp.data
              .filter((item: any) => item?.id && item?.name)
              .map((item: any) => ({ value: String(item.id), label: String(item.name) }))
          : []
        const mOptions = Array.isArray(mlopsResp.data)
          ? mlopsResp.data
              .filter((item: any) => item?.id && item?.name)
              .map((item: any) => ({ value: String(item.id), label: String(item.name) }))
          : []

        setPipelineOptions(pOptions)
        setMLOpsWorkflowOptions(mOptions)
      } catch {
        if (mounted) {
          setPipelineOptions([])
          setMLOpsWorkflowOptions([])
        }
      } finally {
        if (mounted) {
          setPipelinesLoading(false)
          setMLOpsWorkflowsLoading(false)
        }
      }
    }
    void loadSources()
    return () => {
      mounted = false
    }
  }, [])

  useEffect(() => {
    if (!selectedWidget || selectedWidget.source.source_type !== 'pipeline') {
      setPipelineNodeOptions([])
      return
    }
    const pipelineId = (selectedWidget.source.pipeline_id || '').trim()
    if (!pipelineId) {
      setPipelineNodeOptions([])
      return
    }
    let mounted = true
    setPipelineNodesLoading(true)
    const loadNodes = async () => {
      try {
        const response = await http.get(`/api/pipelines/${pipelineId}`)
        if (!mounted) return
        const nodes = Array.isArray(response.data?.nodes) ? response.data.nodes : []
        const options = nodes
          .filter((node: any) => node?.id)
          .map((node: any) => {
            const nodeType = String(node?.data?.nodeType || 'node')
            const label = String(node?.data?.label || nodeType)
            return { value: String(node.id), label: `${label} [${nodeType}]` }
          })
        setPipelineNodeOptions(options)
      } catch {
        if (mounted) setPipelineNodeOptions([])
      } finally {
        if (mounted) setPipelineNodesLoading(false)
      }
    }
    void loadNodes()
    return () => {
      mounted = false
    }
  }, [selectedWidget?.id, selectedWidget?.source.source_type, selectedWidget?.source.pipeline_id])

  useEffect(() => {
    if (!selectedWidget || selectedWidget.source.source_type !== 'mlops') {
      setMLOpsRunOptions([])
      setMLOpsNodeOptions([])
      return
    }
    const workflowId = (selectedWidget.source.mlops_workflow_id || '').trim()
    if (!workflowId) {
      setMLOpsRunOptions([])
      setMLOpsNodeOptions([])
      return
    }
    let mounted = true
    setMLOpsRunsLoading(true)
    setMLOpsNodesLoading(true)
    const loadWorkflowData = async () => {
      try {
        const [runOptions, workflowResp] = await Promise.all([
          loadMLOpsRunOptions(workflowId),
          http.get(`/api/mlops/workflows/${workflowId}`),
        ])
        if (!mounted) return

        const nodes = Array.isArray(workflowResp.data?.nodes) ? workflowResp.data.nodes : []
        const nodeOptions = nodes
          .filter((node: any) => node?.id)
          .map((node: any) => {
            const nodeType = String(node?.data?.nodeType || 'node')
            const label = String(node?.data?.label || nodeType)
            return { value: String(node.id), label: `${label} [${nodeType}]` }
          })

        setMLOpsRunOptions(runOptions)
        if (
          selectedWidget.source.mlops_run_id
          && !runOptions.some((option) => option.value === selectedWidget.source.mlops_run_id)
        ) {
          updateSelectedSource({ mlops_run_id: runOptions[0]?.value })
        }
        setMLOpsNodeOptions(nodeOptions)
      } catch {
        if (mounted) {
          setMLOpsRunOptions([])
          setMLOpsNodeOptions([])
        }
      } finally {
        if (mounted) {
          setMLOpsRunsLoading(false)
          setMLOpsNodesLoading(false)
        }
      }
    }
    void loadWorkflowData()
    return () => {
      mounted = false
    }
  }, [selectedWidget?.id, selectedWidget?.source.source_type, selectedWidget?.source.mlops_workflow_id, updateSelectedSource])

  useEffect(() => {
    if (!selectedWidget) {
      setSchema(null)
      return
    }
    if (sourceMissing(selectedWidget.source)) {
      setSchema(null)
      return
    }
    void loadFieldSchema(selectedWidget, true)
  }, [
    selectedWidget?.id,
    selectedWidget?.source.source_type,
    selectedWidget?.source.dataset,
    selectedWidget?.source.pipeline_id,
    selectedWidget?.source.pipeline_node_id,
    selectedWidget?.source.file_path,
    selectedWidget?.source.mlops_workflow_id,
    selectedWidget?.source.mlops_run_id,
    selectedWidget?.source.mlops_node_id,
    selectedWidget?.source.mlops_output_mode,
    selectedWidget?.source.mlops_prediction_start_date,
    selectedWidget?.source.mlops_prediction_end_date,
    loadFieldSchema,
  ])

  useEffect(() => {
    const allowed = new Set(widgets.map((widget) => widget.id))
    Object.keys(iframeRefs.current).forEach((key) => {
      if (!allowed.has(key)) delete iframeRefs.current[key]
    })
  }, [widgets])

  useEffect(() => {
    const onMessage = (event: MessageEvent) => {
      const payload = event?.data && typeof event.data === 'object' ? (event.data as Record<string, unknown>) : null
      const eventType = String(payload?.type || '')
      if (eventType !== 'python-plot-click' && eventType !== 'python-plot-selected') return

      const sourceWindow = event.source as Window | null
      if (!sourceWindow) return

      const widgetId = Object.keys(iframeRefs.current).find((id) => iframeRefs.current[id]?.contentWindow === sourceWindow)
      if (!widgetId) return

      const widget = widgets.find((item) => item.id === widgetId) || null
      const pointPayload = payload?.payload && typeof payload.payload === 'object'
        ? (payload.payload as Record<string, unknown>)
        : null
      const step = extractDrillSelection(widget, pointPayload)
      if (!step || !widget) return

      if (drillMode === 'through') {
        void openDrillThroughRows(widget, step)
        return
      }

      const nextPath = [...drillPath, step]
      const last = drillPath[drillPath.length - 1]
      if (last && last.field === step.field && String(last.value) === String(step.value)) {
        return
      }
      setDrillPath(nextPath)
      void runAllWidgets(true, nextPath)
      messageApi.success(`Dashboard drill: ${step.field} = ${String(step.value)}`)
    }

    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [widgets, drillMode, drillPath, runAllWidgets, openDrillThroughRows, messageApi])

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: 'var(--app-shell-bg-2)' }}>
      {contextHolder}

      <style>{`
        .py-grid .react-resizable-handle {
          position: absolute !important;
          width: 16px !important;
          height: 16px !important;
          bottom: 2px !important;
          right: 2px !important;
          cursor: se-resize !important;
          opacity: 0;
          transition: opacity 0.15s ease;
          background: transparent !important;
          border-right: 2px solid #6366f1 !important;
          border-bottom: 2px solid #6366f1 !important;
          border-radius: 0 0 3px 0 !important;
        }
        .py-grid .react-grid-item:hover .react-resizable-handle,
        .py-grid .react-grid-item.resizing .react-resizable-handle {
          opacity: 1;
        }
      `}</style>

      <div
        style={{
          height: 54,
          minHeight: 54,
          borderBottom: '1px solid var(--app-border)',
          background: 'var(--app-panel-bg)',
          display: 'flex',
          alignItems: 'center',
          padding: '0 12px',
          gap: 10,
        }}
      >
        <CodeOutlined style={{ color: '#6366f1', fontSize: 17 }} />
        <Select
          value={activeDashboardId || undefined}
          onChange={(value) => handleDashboardSelect(String(value))}
          placeholder="Select Python Dashboard"
          style={{ width: 280 }}
          options={dashboards.map((item) => ({ value: item.id, label: item.name }))}
          loading={dashboardsLoading || dashboardLoading}
          showSearch
          optionFilterProp="label"
          allowClear={false}
          notFoundContent={dashboardsLoading ? 'Loading dashboards...' : 'No dashboards'}
        />
        <Input
          value={dashboardName}
          onChange={(e) => {
            setDashboardName(e.target.value)
            setDirty(true)
          }}
          style={{
            width: 240,
            background: 'var(--app-input-bg)',
            border: '1px solid var(--app-border-strong)',
            color: 'var(--app-text)',
          }}
        />
        {dirty && <Tag color="orange">Unsaved</Tag>}
        {activeDashboardId ? <Tag color="blue">Saved</Tag> : <Tag color="default">Draft</Tag>}
        <Segmented
          size="small"
          value={drillMode}
          options={[
            { label: 'Drill Down', value: 'down' },
            { label: 'Drill Through', value: 'through' },
          ]}
          onChange={(value) => setDrillMode(value as DrillMode)}
        />
        {drillPath.length > 0 && (
          <>
            <Button size="small" onClick={() => void stepUpDashboardDrill()}>
              Drill Up
            </Button>
            <Button size="small" onClick={() => void resetDashboardDrill()}>
              Reset Drill
            </Button>
            <Text
              style={{
                maxWidth: 360,
                color: 'var(--app-text-subtle)',
                fontSize: 11,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
              title={drillPathLabel}
            >
              {drillPathLabel}
            </Text>
          </>
        )}
        <div style={{ flex: 1 }} />
        <Button icon={<PlusOutlined />} onClick={addNewWidget}>Add Widget</Button>
        <Button
          icon={<PlayCircleOutlined />}
          disabled={!selectedWidget}
          onClick={() => selectedWidget && void runWidget(selectedWidget.id)}
        >
          Run Selected
        </Button>
        <Button icon={<ReloadOutlined />} onClick={() => void runAllWidgets()}>
          Run All
        </Button>
        <Button icon={<SaveOutlined />} type="primary" onClick={() => void saveDashboard()} loading={dashboardSaving}>
          Save Dashboard
        </Button>
        <Button onClick={createNewDashboard} disabled={dashboardSaving || dashboardLoading}>
          New
        </Button>
        <Button icon={<CopyOutlined />} onClick={() => void duplicateDashboard()} disabled={!activeDashboardId || dashboardSaving || dashboardLoading}>
          Duplicate
        </Button>
        <Popconfirm
          title="Delete dashboard?"
          description="This will permanently remove the saved Python dashboard."
          onConfirm={() => void deleteDashboard()}
          okButtonProps={{ danger: true, loading: dashboardSaving }}
          disabled={!activeDashboardId || dashboardSaving || dashboardLoading}
        >
          <Button danger icon={<DeleteOutlined />} disabled={!activeDashboardId || dashboardSaving || dashboardLoading}>
            Delete
          </Button>
        </Popconfirm>
        <Button onClick={() => void resetDashboard()} disabled={dashboardSaving || dashboardLoading}>
          Reset Changes
        </Button>
      </div>

      <div style={{ display: 'flex', flex: 1, minHeight: 0 }}>
        <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column' }}>
          <div
            ref={gridContainerRef as React.RefObject<HTMLDivElement>}
            style={{
              flex: 1,
              minHeight: 0,
              overflow: 'auto',
              padding: 10,
              background: 'var(--app-shell-bg-2)',
              backgroundImage: 'radial-gradient(circle, var(--app-border) 1px, transparent 1px)',
              backgroundSize: '24px 24px',
            }}
          >
            {widgets.length === 0 ? (
              <div style={{ minHeight: 420, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <Empty
                  description={<Text style={{ color: 'var(--app-text-subtle)' }}>No python widgets yet</Text>}
                >
                  <Button type="primary" icon={<PlusOutlined />} onClick={addNewWidget}>
                    Create First Widget
                  </Button>
                </Empty>
              </div>
            ) : (
              <RGL
                className="py-grid"
                layouts={rglLayouts}
                breakpoints={{ lg: 1200, md: 996, sm: 768, xs: 480, xxs: 0 }}
                cols={COLS}
                rowHeight={ROW_HEIGHT}
                margin={MARGIN}
                containerPadding={[0, 0]}
                draggableHandle=".py-drag-handle"
                onDragStop={(current: any[]) => commitLayout(current)}
                onResizeStop={(current: any[]) => commitLayout(current)}
                width={gridWidth}
                useCSSTransforms={gridMounted}
                measureBeforeMount={false}
                isDraggable
                isResizable
              >
                {widgets.map((widget) => {
                  const currentLayout = widgetLayoutMap.get(widget.id)
                  const chartHeight = computeBodyHeight(currentLayout?.h || 10)
                  const selected = widget.id === selectedWidgetId
                  return (
                    <div key={widget.id} style={{ overflow: 'hidden' }}>
                      <div
                        style={{
                          height: '100%',
                          border: `1px solid ${selected ? '#6366f1' : 'var(--app-border)'}`,
                          borderRadius: 10,
                          overflow: 'hidden',
                          background: 'var(--app-panel-bg)',
                          display: 'flex',
                          flexDirection: 'column',
                          boxShadow: selected ? '0 0 0 1px #6366f140, 0 10px 24px #6366f11a' : 'none',
                          transition: 'border-color 0.15s, box-shadow 0.15s',
                        }}
                        onClick={() => setSelectedWidgetId(widget.id)}
                      >
                        <div
                          className="py-drag-handle"
                          style={{
                            height: 30,
                            minHeight: 30,
                            display: 'flex',
                            alignItems: 'center',
                            gap: 6,
                            padding: '0 8px',
                            borderBottom: '1px solid var(--app-border)',
                            background: 'var(--app-panel-2)',
                            cursor: 'move',
                          }}
                        >
                          <HolderOutlined style={{ color: 'var(--app-text-faint)', fontSize: 12 }} />
                          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 600, flex: 1 }} ellipsis>
                            {widget.title}
                          </Text>
                          <Button
                            size="small"
                            type="text"
                            icon={<PlayCircleOutlined />}
                            onClick={(e) => {
                              e.stopPropagation()
                              void runWidget(widget.id)
                            }}
                            style={{ color: '#22c55e' }}
                          />
                          <Button
                            size="small"
                            type="text"
                            icon={<DeleteOutlined />}
                            onClick={(e) => {
                              e.stopPropagation()
                              removeWidget(widget.id)
                            }}
                            style={{ color: '#ef4444' }}
                          />
                        </div>

                        <div style={{ flex: 1, minHeight: 0, background: '#0b1220' }}>
                          {widget.loading ? (
                            <div style={{ height: chartHeight, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 10 }}>
                              <Spin />
                              <Text style={{ color: 'var(--app-text-subtle)' }}>Running python analytics...</Text>
                            </div>
                          ) : widget.error ? (
                            <div style={{ height: chartHeight, padding: 12 }}>
                              <Text style={{ color: '#ef4444', fontSize: 12 }}>{widget.error}</Text>
                            </div>
                          ) : widget.result?.figure_html ? (
                            <iframe
                              title={`py-frame-${widget.id}`}
                              srcDoc={widget.result.figure_html}
                              ref={(el) => {
                                iframeRefs.current[widget.id] = el
                              }}
                              style={{ border: 0, width: '100%', height: chartHeight, background: '#ffffff' }}
                              scrolling="no"
                            />
                          ) : (
                            <div style={{ height: chartHeight, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 12 }}>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                                Configure datasource and run widget
                              </Text>
                            </div>
                          )}
                        </div>

                        <div
                          style={{
                            height: 24,
                            minHeight: 24,
                            borderTop: '1px solid var(--app-border)',
                            background: 'var(--app-panel-2)',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'space-between',
                            padding: '0 8px',
                          }}
                        >
                          <Text style={{ color: 'var(--app-text-faint)', fontSize: 11 }}>
                            Source: {widget.source.source_type.toUpperCase()}
                          </Text>
                          <Text style={{ color: 'var(--app-text-faint)', fontSize: 11 }}>
                            {widget.result ? `${Number(widget.result.row_count || 0).toLocaleString()} rows` : 'Not run'}
                          </Text>
                        </div>
                      </div>
                    </div>
                  )
                })}
              </RGL>
            )}
          </div>
        </div>

        <div
          style={{
            width: 370,
            minWidth: 370,
            borderLeft: '1px solid var(--app-border)',
            background: 'var(--app-panel-bg)',
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden',
          }}
        >
          {selectedWidget ? (
            <>
              <div style={{ height: 42, minHeight: 42, borderBottom: '1px solid var(--app-border)', padding: '0 12px', display: 'flex', alignItems: 'center' }}>
                <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Widget Configuration</Text>
              </div>
              <div style={{ flex: 1, minHeight: 0, overflowY: 'auto', padding: 12 }}>
                <div style={{ marginBottom: 12 }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Title</Text>
                  <Input
                    value={selectedWidget.title}
                    onChange={(e) => updateSelectedTitle(e.target.value)}
                    style={{
                      background: 'var(--app-input-bg)',
                      border: '1px solid var(--app-border-strong)',
                      color: 'var(--app-text)',
                    }}
                  />
                </div>

                <Divider style={{ margin: '10px 0' }} />
                <Text style={{ color: 'var(--app-text)', fontWeight: 600, display: 'block', marginBottom: 10 }}>
                  Data Source
                </Text>

                <div style={{ marginBottom: 10 }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Source Type</Text>
                  <Select
                    value={selectedWidget.source.source_type}
                    onChange={(value) => handleSourceTypeChange(value as SourceType)}
                    style={{ width: '100%' }}
                    options={[
                      { value: 'sample', label: 'Sample Dataset' },
                      { value: 'pipeline', label: 'ETL Pipeline Output' },
                      { value: 'mlops', label: 'MLOps Model Output' },
                      { value: 'file', label: 'File Path' },
                    ]}
                  />
                </div>

                {selectedWidget.source.source_type === 'sample' && (
                  <div style={{ marginBottom: 10 }}>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Dataset</Text>
                    <Select
                      value={selectedWidget.source.dataset || 'sales'}
                      onChange={(value) => updateSelectedSource({ dataset: value })}
                      style={{ width: '100%' }}
                      options={SAMPLE_DATASETS}
                    />
                  </div>
                )}

                {selectedWidget.source.source_type === 'pipeline' && (
                  <>
                    <div style={{ marginBottom: 10 }}>
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Pipeline</Text>
                      <Select
                        value={selectedWidget.source.pipeline_id || undefined}
                        onChange={(value) => updateSelectedSource({ pipeline_id: value, pipeline_node_id: undefined })}
                        style={{ width: '100%' }}
                        options={pipelineOptions}
                        loading={pipelinesLoading}
                        notFoundContent={pipelinesLoading ? 'Loading pipelines...' : 'No pipelines found'}
                        allowClear
                        showSearch
                        optionFilterProp="label"
                      />
                    </div>
                    <div style={{ marginBottom: 10 }}>
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Pipeline Node (optional)</Text>
                      <Select
                        value={selectedWidget.source.pipeline_node_id || undefined}
                        onChange={(value) => updateSelectedSource({ pipeline_node_id: value })}
                        style={{ width: '100%' }}
                        options={pipelineNodeOptions}
                        loading={pipelineNodesLoading}
                        notFoundContent={pipelineNodesLoading ? 'Loading nodes...' : 'No nodes found'}
                        allowClear
                        showSearch
                        optionFilterProp="label"
                        disabled={!selectedWidget.source.pipeline_id}
                      />
                    </div>
                  </>
                )}

                {selectedWidget.source.source_type === 'mlops' && (
                  <>
                    <div style={{ marginBottom: 10 }}>
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Workflow</Text>
                      <Select
                        value={selectedWidget.source.mlops_workflow_id || undefined}
                        onChange={(value) =>
                          updateSelectedSource({
                            mlops_workflow_id: value,
                            mlops_run_id: undefined,
                            mlops_node_id: undefined,
                          })
                        }
                        style={{ width: '100%' }}
                        options={mlopsWorkflowOptions}
                        loading={mlopsWorkflowsLoading}
                        notFoundContent={mlopsWorkflowsLoading ? 'Loading workflows...' : 'No workflows found'}
                        allowClear
                        showSearch
                        optionFilterProp="label"
                      />
                    </div>
                    <div style={{ marginBottom: 10 }}>
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Run (optional)</Text>
                      <Select
                        value={selectedWidget.source.mlops_run_id || undefined}
                        onChange={(value) => updateSelectedSource({ mlops_run_id: value })}
                        style={{ width: '100%' }}
                        options={mlopsRunOptions}
                        loading={mlopsRunsLoading}
                        notFoundContent={mlopsRunsLoading ? 'Loading runs...' : 'No runs found'}
                        allowClear
                        showSearch
                        optionFilterProp="label"
                        disabled={!selectedWidget.source.mlops_workflow_id}
                      />
                    </div>
                    <div style={{ marginBottom: 10 }}>
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Node (optional)</Text>
                      <Select
                        value={selectedWidget.source.mlops_node_id || undefined}
                        onChange={(value) => updateSelectedSource({ mlops_node_id: value })}
                        style={{ width: '100%' }}
                        options={mlopsNodeOptions}
                        loading={mlopsNodesLoading}
                        notFoundContent={mlopsNodesLoading ? 'Loading nodes...' : 'No nodes found'}
                        allowClear
                        showSearch
                        optionFilterProp="label"
                        disabled={!selectedWidget.source.mlops_workflow_id}
                      />
                    </div>
                    <div style={{ marginBottom: 10 }}>
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Output Mode</Text>
                      <Select
                        value={selectedWidget.source.mlops_output_mode || 'predictions'}
                        onChange={(value) => updateSelectedSource({ mlops_output_mode: value as 'predictions' | 'metrics' | 'monitor' | 'evaluation' })}
                        style={{ width: '100%' }}
                        options={[
                          { value: 'predictions', label: 'Predictions' },
                          { value: 'metrics', label: 'Metrics Summary' },
                          { value: 'monitor', label: 'Monitoring Runs' },
                          { value: 'evaluation', label: 'Evaluation Output' },
                        ]}
                      />
                    </div>
                    <div style={{ display: 'flex', gap: 8, marginBottom: 10 }}>
                      <div style={{ flex: 1 }}>
                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Prediction Start</Text>
                        <Input
                          type="date"
                          value={selectedWidget.source.mlops_prediction_start_date || ''}
                          onChange={(e) => updateSelectedSource({ mlops_prediction_start_date: e.target.value || undefined })}
                        />
                      </div>
                      <div style={{ flex: 1 }}>
                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Prediction End</Text>
                        <Input
                          type="date"
                          value={selectedWidget.source.mlops_prediction_end_date || ''}
                          onChange={(e) => updateSelectedSource({ mlops_prediction_end_date: e.target.value || undefined })}
                        />
                      </div>
                    </div>
                  </>
                )}

                {selectedWidget.source.source_type === 'file' && (
                  <div style={{ marginBottom: 10 }}>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>File Path</Text>
                    <Input
                      value={selectedWidget.source.file_path || ''}
                      onChange={(e) => updateSelectedSource({ file_path: e.target.value })}
                      placeholder="/absolute/path/to/file.csv"
                      style={{
                        background: 'var(--app-input-bg)',
                        border: '1px solid var(--app-border-strong)',
                        color: 'var(--app-text)',
                      }}
                    />
                  </div>
                )}

                <div style={{ marginBottom: 10 }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Optional SQL</Text>
                  <Input.TextArea
                    rows={3}
                    value={selectedWidget.source.sql || ''}
                    onChange={(e) => updateSelectedSource({ sql: e.target.value || undefined })}
                    placeholder="SELECT * FROM data WHERE ..."
                    style={{
                      background: 'var(--app-input-bg)',
                      border: '1px solid var(--app-border-strong)',
                      color: 'var(--app-text)',
                    }}
                  />
                </div>

                <Divider style={{ margin: '10px 0' }} />
                <Text style={{ color: 'var(--app-text)', fontWeight: 600, display: 'block', marginBottom: 10 }}>
                  Python Visualization
                </Text>

                <div style={{ marginBottom: 10, display: 'flex', gap: 8, alignItems: 'center' }}>
                  <Button
                    size="small"
                    loading={schemaLoading}
                    disabled={sourceMissing(selectedWidget.source)}
                    icon={<DatabaseOutlined />}
                    onClick={() => void loadFieldSchema(selectedWidget, false)}
                  >
                    Load Fields
                  </Button>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                    {schemaColumns.length > 0 ? `${schemaColumns.length} fields` : 'No fields loaded'}
                  </Text>
                </div>

                <div style={{ marginBottom: 10 }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Chart Type</Text>
                  <Select
                    value={selectedWidget.config.chart_type}
                    onChange={(value) => updateSelectedConfig({ chart_type: value as PythonChartConfig['chart_type'] })}
                    style={{ width: '100%' }}
                    options={[
                      { value: 'auto', label: 'Auto' },
                      { value: 'line', label: 'Line' },
                      { value: 'bar', label: 'Bar' },
                      { value: 'scatter', label: 'Scatter' },
                      { value: 'histogram', label: 'Histogram' },
                      { value: 'box', label: 'Box' },
                      { value: 'heatmap', label: 'Heatmap' },
                      { value: 'table', label: 'Table' },
                    ]}
                  />
                </div>

                <div style={{ marginBottom: 10 }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>X Field</Text>
                  <Select
                    value={selectedWidget.config.x_field || undefined}
                    onChange={(value) => updateSelectedConfig({ x_field: value || '' })}
                    style={{ width: '100%' }}
                    options={schemaDimensions.map((col) => ({ value: col, label: col }))}
                    notFoundContent={schemaLoading ? 'Loading fields...' : 'No fields found'}
                    allowClear
                    showSearch
                    optionFilterProp="label"
                  />
                </div>

                <div style={{ marginBottom: 10 }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Y Field</Text>
                  <Select
                    value={selectedWidget.config.y_field || undefined}
                    onChange={(value) => updateSelectedConfig({ y_field: value || '' })}
                    style={{ width: '100%' }}
                    options={schemaMetrics.map((col) => ({ value: col, label: col }))}
                    notFoundContent={schemaLoading ? 'Loading fields...' : 'No fields found'}
                    allowClear
                    showSearch
                    optionFilterProp="label"
                  />
                </div>

                <div style={{ marginBottom: 10 }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Color Field</Text>
                  <Select
                    value={selectedWidget.config.color_field || undefined}
                    onChange={(value) => updateSelectedConfig({ color_field: value || '' })}
                    style={{ width: '100%' }}
                    options={schemaDimensions.map((col) => ({ value: col, label: col }))}
                    notFoundContent={schemaLoading ? 'Loading fields...' : 'No fields found'}
                    allowClear
                    showSearch
                    optionFilterProp="label"
                  />
                </div>

                <div style={{ marginBottom: 10 }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Group By</Text>
                  <Select
                    value={selectedWidget.config.group_by || undefined}
                    onChange={(value) => updateSelectedConfig({ group_by: value || '' })}
                    style={{ width: '100%' }}
                    options={schemaDimensions.map((col) => ({ value: col, label: col }))}
                    notFoundContent={schemaLoading ? 'Loading fields...' : 'No fields found'}
                    allowClear
                    showSearch
                    optionFilterProp="label"
                  />
                </div>

                <div style={{ marginBottom: 10 }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Chart Title</Text>
                  <Input
                    value={selectedWidget.config.title || ''}
                    onChange={(e) => updateSelectedConfig({ title: e.target.value })}
                    placeholder="Python chart title"
                    style={{
                      background: 'var(--app-input-bg)',
                      border: '1px solid var(--app-border-strong)',
                      color: 'var(--app-text)',
                    }}
                  />
                </div>

                <div style={{ marginBottom: 10 }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Row Limit</Text>
                  <InputNumber
                    min={100}
                    max={100000}
                    value={selectedWidget.config.limit}
                    onChange={(value) => updateSelectedConfig({ limit: Number(value) || 5000 })}
                    style={{ width: '100%' }}
                  />
                </div>

                <div style={{ display: 'flex', gap: 8, marginTop: 12 }}>
                  <Button
                    type="primary"
                    icon={<PlayCircleOutlined />}
                    disabled={sourceMissing(selectedWidget.source)}
                    onClick={() => void runWidget(selectedWidget.id)}
                  >
                    Run Widget
                  </Button>
                  <Button icon={<CopyOutlined />} onClick={duplicateSelectedWidget}>
                    Duplicate
                  </Button>
                  <Button danger icon={<DeleteOutlined />} onClick={() => removeWidget(selectedWidget.id)}>
                    Delete
                  </Button>
                </div>
              </div>
            </>
          ) : (
            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 16 }}>
              <Empty
                description={<Text style={{ color: 'var(--app-text-subtle)' }}>Select a widget to configure</Text>}
              />
            </div>
          )}
        </div>
      </div>

      <Modal
        open={drillThroughOpen}
        title={drillThroughTitle}
        onCancel={() => setDrillThroughOpen(false)}
        footer={null}
        width={1080}
      >
        {drillThroughLoading ? (
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: 16 }}>
            <Spin />
            <Text style={{ color: 'var(--app-text-subtle)' }}>Loading drill-through rows...</Text>
          </div>
        ) : (
          <Table
            size="small"
            dataSource={drillThroughRows.map((row, index) => ({ key: `drill-row-${index}`, ...row }))}
            columns={drillThroughColumns.map((column) => ({
              title: column,
              dataIndex: column,
              key: column,
              width: 150,
              ellipsis: true,
              render: (value: unknown) => {
                if (value === null || value === undefined || String(value) === '') return '-'
                if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return String(value)
                try {
                  return JSON.stringify(value)
                } catch {
                  return String(value)
                }
              },
            }))}
            pagination={{ pageSize: 25, showSizeChanger: true, pageSizeOptions: ['25', '50', '100'] }}
            scroll={{ x: true, y: 520 }}
          />
        )}
      </Modal>
    </div>
  )
}
