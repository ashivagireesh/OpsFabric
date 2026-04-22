import { useEffect, useState, useCallback, useRef, useMemo } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  Button, Input, Switch, Tooltip, Select, InputNumber, Tabs, message,
  Spin, Typography, Divider, ColorPicker, Modal, Table,
} from 'antd'
import type { InputRef } from 'antd'
import type { Color } from 'antd/es/color-picker'
import {
  ArrowLeftOutlined, SaveOutlined, EyeOutlined, FilterOutlined,
  EditOutlined, DeleteOutlined, HolderOutlined, CloseOutlined,
  ReloadOutlined, SettingOutlined, BulbOutlined, CodeOutlined,
} from '@ant-design/icons'
import axios from 'axios'
import React from 'react'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
import { ResponsiveGridLayout as RGLResponsive } from 'react-grid-layout'
import { useContainerWidth } from 'react-grid-layout'
import 'react-grid-layout/css/styles.css'
import 'react-resizable/css/styles.css'

import { useVizStore } from '../../store/vizStore'
import type { Widget, LayoutItem } from '../../store/vizStore'
import WidgetPalette from '../../components/viz/WidgetPalette'
import ChartWidget from '../../components/viz/ChartWidget'

// ─── RGL types (minimal, avoids import issues) ────────────────────────────────

interface RGLLayoutItem {
  i: string
  x: number
  y: number
  w: number
  h: number
  minW?: number
  minH?: number
  static?: boolean
  isDraggable?: boolean
  isResizable?: boolean
}

interface RGLLayouts {
  [breakpoint: string]: RGLLayoutItem[]
}

interface RGLResponsiveProps {
  className?: string
  layouts?: RGLLayouts
  breakpoints?: Record<string, number>
  cols?: Record<string, number>
  rowHeight?: number
  isDraggable?: boolean
  isResizable?: boolean
  margin?: [number, number]
  containerPadding?: [number, number]
  draggableHandle?: string
  useCSSTransforms?: boolean
  measureBeforeMount?: boolean
  onLayoutChange?: (currentLayout: RGLLayoutItem[], allLayouts: RGLLayouts) => void
  style?: React.CSSProperties
  children?: React.ReactNode
}

const RGL = RGLResponsive as unknown as React.ComponentType<any>

// ─── constants ──────────────────────────────────────────────────────────────

const ROW_HEIGHT = 30
const MARGIN: [number, number] = [8, 8]

const COLS: Record<string, number> = { lg: 24, md: 20, sm: 12, xs: 8, xxs: 4 }

const SAMPLE_DATASETS = [
  { key: 'sales', label: 'Sales by Region', icon: '📊' },
  { key: 'web_analytics', label: 'Web Analytics', icon: '🌐' },
  { key: 'financials', label: 'Financials', icon: '💰' },
  { key: 'employees', label: 'Employee Data', icon: '👥' },
  { key: 'supply_chain', label: 'Supply Chain', icon: '🔗' },
  { key: 'customer', label: 'Customer Data', icon: '🎯' },
]

const VIZ_API_BASE = import.meta.env.VITE_API_BASE || (import.meta.env.DEV ? 'http://localhost:8001' : '')
const vizHttp = axios.create({ baseURL: VIZ_API_BASE, timeout: 30000 })
const PYTHON_DASHBOARD_TAG = '__python_analytics__'

interface PipelineNodeOption {
  value: string
  label: string
}

interface PipelineOption {
  value: string
  label: string
}

interface MLOpsWorkflowOption {
  value: string
  label: string
}

interface MLOpsRunOption {
  value: string
  label: string
  workflowId?: string
  runKind?: 'standard' | 'h2o'
}

type AnalyticsSourceType = 'sample' | 'pipeline' | 'file' | 'mlops'

interface AnalyticsAutoConfig {
  source_type: AnalyticsSourceType
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
  nlp_prompt?: string
  forecast_horizon: number
}

interface PythonAnalyticsConfig {
  chart_type: 'auto' | 'line' | 'bar' | 'scatter' | 'histogram' | 'box' | 'heatmap' | 'table'
  x_field?: string
  y_field?: string
  color_field?: string
  group_by?: string
  title?: string
  limit: number
}

interface PythonAnalyticsResult {
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

function mapPythonChartToWidgetType(chartType: string): Widget['type'] {
  const normalized = String(chartType || '').trim().toLowerCase()
  if (normalized === 'line') return 'line'
  if (normalized === 'bar') return 'bar'
  if (normalized === 'scatter') return 'scatter'
  if (normalized === 'heatmap') return 'heatmap'
  if (normalized === 'table') return 'table'
  if (normalized === 'histogram') return 'bar'
  if (normalized === 'box') return 'bar'
  return 'table'
}

function normalizeFieldName(value: unknown): string {
  const text = typeof value === 'string' ? value.trim() : ''
  return text
}

type PythonWidgetTargetType = 'auto' | Widget['type']

const COLOR_PALETTES: { key: string; label: string; colors: string[] }[] = [
  { key: 'indigo', label: 'Indigo', colors: ['#6366f1', '#8b5cf6', '#a855f7', '#ec4899', '#f43f5e', '#f97316'] },
  { key: 'ocean', label: 'Ocean', colors: ['#06b6d4', '#0891b2', '#0e7490', '#155e75', '#0f766e', '#14b8a6'] },
  { key: 'forest', label: 'Forest', colors: ['#22c55e', '#16a34a', '#15803d', '#4ade80', '#86efac', '#bbf7d0'] },
  { key: 'sunset', label: 'Sunset', colors: ['#f97316', '#ea580c', '#ef4444', '#dc2626', '#f59e0b', '#d97706'] },
  { key: 'slate', label: 'Slate', colors: ['var(--app-text-muted)', 'var(--app-text-subtle)', 'var(--app-text-dim)', 'var(--app-text-faint)', '#cbd5e1', 'var(--app-text)'] },
  { key: 'vivid', label: 'Vivid', colors: ['#3b82f6', '#8b5cf6', '#ec4899', '#ef4444', '#f97316', '#22c55e'] },
]

// ─── styles ─────────────────────────────────────────────────────────────────

const S = {
  root: {
    display: 'flex',
    flexDirection: 'column' as const,
    height: '100vh',
    overflow: 'hidden',
    background: 'var(--app-shell-bg-2)',
    fontFamily: "'Inter', 'SF Pro', system-ui, sans-serif",
  },
  toolbar: {
    height: 48,
    minHeight: 48,
    background: 'var(--app-panel-bg)',
    borderBottom: '1px solid var(--app-border)',
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    padding: '0 12px',
    flexShrink: 0,
    zIndex: 100,
    overflowX: 'auto' as const,
    overflowY: 'hidden' as const,
  },
  body: {
    display: 'flex',
    flex: 1,
    overflow: 'hidden',
  },
  sidebar: {
    width: 240,
    minWidth: 240,
    background: 'var(--app-panel-bg)',
    borderRight: '1px solid var(--app-border)',
    display: 'flex',
    flexDirection: 'column' as const,
    overflow: 'hidden',
  },
  canvas: {
    flex: 1,
    overflowY: 'auto' as const,
    overflowX: 'hidden' as const,
    background: 'var(--app-shell-bg-2)',
    backgroundImage: 'radial-gradient(circle, var(--app-border) 1px, transparent 1px)',
    backgroundSize: '24px 24px',
    position: 'relative' as const,
  },
  rightPanel: (open: boolean): React.CSSProperties => ({
    width: open ? 320 : 0,
    minWidth: open ? 320 : 0,
    background: 'var(--app-panel-bg)',
    borderLeft: '1px solid var(--app-border)',
    display: 'flex',
    flexDirection: 'column',
    overflow: 'hidden',
    transition: 'width 0.2s ease, min-width 0.2s ease',
    flexShrink: 0,
  }),
  widgetCard: (selected: boolean): React.CSSProperties => ({
    background: 'var(--app-panel-bg)',
    border: `1px solid ${selected ? '#6366f1' : 'var(--app-border)'}`,
    borderRadius: 8,
    overflow: 'hidden',
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
    boxShadow: selected ? '0 0 0 2px #6366f130' : 'none',
    transition: 'border-color 0.15s, box-shadow 0.15s',
  }),
  widgetHeader: {
    height: 28,
    minHeight: 28,
    background: 'var(--app-panel-2)',
    display: 'flex',
    alignItems: 'center',
    padding: '0 6px',
    gap: 4,
    borderBottom: '1px solid var(--app-border)',
    userSelect: 'none' as const,
  },
  formRow: {
    display: 'flex',
    flexDirection: 'column' as const,
    gap: 4,
    marginBottom: 12,
  },
  formLabel: {
    color: 'var(--app-text-subtle)',
    fontSize: 12,
  } as React.CSSProperties,
  panelScroll: {
    flex: 1,
    overflowY: 'auto' as const,
    padding: '12px 14px',
  },
  inputDark: {
    background: 'var(--app-input-bg)',
    border: '1px solid var(--app-border-strong)',
    color: 'var(--app-text)',
    fontSize: 13,
  },
}

// ─── helpers ─────────────────────────────────────────────────────────────────

function computeBodyHeight(h: number): number {
  // h grid units * rowHeight + (h-1) gaps - header - borders
  return h * ROW_HEIGHT + (h - 1) * MARGIN[1] - 28 - 2
}

function toRGLItem(l: LayoutItem): RGLLayoutItem {
  return { i: l.i, x: l.x, y: l.y, w: l.w, h: l.h, minW: l.minW, minH: l.minH }
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
      const modelVersion = String(run?.model_version || 'model')
      const status = String(run?.status || 'unknown')
      const startedAt = formatRunTimestamp(run?.started_at)
      return {
        value: String(run.id),
        label: `${modelVersion} • ${startedAt} (${status})`,
        workflowId: String(run?.workflow_id || '').trim() || undefined,
        runKind: 'standard',
      }
    })
}

function mapH2OMLOpsRunOptions(rows: unknown[], activeWorkflowId: string): MLOpsRunOption[] {
  return (Array.isArray(rows) ? rows : [])
    .filter((run: any) => run?.id)
    .map((run: any) => {
      const modelLabel = String(run?.label || '').trim()
      const task = String(run?.task || 'auto').toUpperCase()
      const target = String(run?.target_column || '').trim()
      const status = String(run?.status || 'unknown')
      const createdAt = formatRunTimestamp(run?.created_at || run?.updated_at || run?.finished_at)
      const evaluatedAt = formatRunTimestamp(run?.last_evaluated_at)
      const workflowId = String(run?.workflow_id || '').trim()
      const activeWf = String(activeWorkflowId || '').trim()
      const workflowHint = workflowId && activeWf && workflowId !== activeWf
        ? ` • wf ${workflowId.slice(0, 8)}`
        : ''
      const targetSuffix = target ? ` • ${target}` : ''
      return {
        value: String(run.id),
        label: `${modelLabel ? `${modelLabel} • ` : ''}H2O ${task}${targetSuffix}${workflowHint} • ${createdAt}${run?.last_evaluated_at ? ` • Eval ${evaluatedAt}` : ''} (${status})`,
        workflowId: workflowId || undefined,
        runKind: 'h2o',
      }
    })
}

async function loadMLOpsRunOptions(workflowId: string): Promise<MLOpsRunOption[]> {
  const trimmedWorkflowId = String(workflowId || '').trim()
  if (!trimmedWorkflowId) return []

  const standardPromise = vizHttp.get('/api/mlops/runs', { params: { workflow_id: trimmedWorkflowId } })
  const scopedH2OPromise = vizHttp.get('/api/mlops/h2o/runs', {
    params: { workflow_id: trimmedWorkflowId, limit: 100 },
  })

  const [standardResult, scopedH2OResult] = await Promise.allSettled([standardPromise, scopedH2OPromise])

  const standardRows = standardResult.status === 'fulfilled' && Array.isArray(standardResult.value.data)
    ? standardResult.value.data
    : []

  let h2oRows = scopedH2OResult.status === 'fulfilled' && Array.isArray(scopedH2OResult.value.data)
    ? scopedH2OResult.value.data
    : []

  if (h2oRows.length === 0) {
    const globalH2OResult = await Promise.allSettled([
      vizHttp.get('/api/mlops/h2o/runs', { params: { limit: 100 } }),
    ])
    const globalRows = globalH2OResult[0]
    if (globalRows.status === 'fulfilled' && Array.isArray(globalRows.value.data)) {
      const allRows = globalRows.value.data as any[]
      const scoredRows = [...allRows].sort((a, b) => {
        const aMatch = String(a?.workflow_id || '').trim() === trimmedWorkflowId ? 0 : 1
        const bMatch = String(b?.workflow_id || '').trim() === trimmedWorkflowId ? 0 : 1
        if (aMatch !== bMatch) return aMatch - bMatch
        return 0
      })
      h2oRows = scoredRows
    }
  }

  return [...mapStandardMLOpsRunOptions(standardRows), ...mapH2OMLOpsRunOptions(h2oRows, trimmedWorkflowId)]
}

// ─── Config Panel ─────────────────────────────────────────────────────────────

interface ConfigPanelProps {
  widget: Widget
  layoutItem: LayoutItem | undefined
  columns: string[]
  onUpdate: (updates: Partial<Widget>) => void
  onSizeChange: (w: number, h: number) => void
  onRemove: () => void
  onClose: () => void
  onRefresh: () => void
}

function ConfigPanel({ widget, layoutItem, columns, onUpdate, onSizeChange, onRemove, onClose, onRefresh }: ConfigPanelProps) {
  const cfg = widget.chart_config
  const ds = widget.data_source
  const sty = widget.style
  const [pipelineOptions, setPipelineOptions] = useState<PipelineOption[]>([])
  const [pipelineNodes, setPipelineNodes] = useState<PipelineNodeOption[]>([])
  const [pipelinesLoading, setPipelinesLoading] = useState(false)
  const [pipelineNodesLoading, setPipelineNodesLoading] = useState(false)
  const [mlopsWorkflowOptions, setMLOpsWorkflowOptions] = useState<MLOpsWorkflowOption[]>([])
  const [mlopsRunOptions, setMLOpsRunOptions] = useState<MLOpsRunOption[]>([])
  const [mlopsNodeOptions, setMLOpsNodeOptions] = useState<PipelineNodeOption[]>([])
  const [mlopsWorkflowsLoading, setMLOpsWorkflowsLoading] = useState(false)
  const [mlopsRunsLoading, setMLOpsRunsLoading] = useState(false)
  const [mlopsNodesLoading, setMLOpsNodesLoading] = useState(false)

  const colOptions = columns.map(c => ({ label: c, value: c }))

  const updateCfg = (patch: Partial<Widget['chart_config']>) =>
    onUpdate({ chart_config: { ...cfg, ...patch } })

  const updateDs = (patch: Partial<Widget['data_source']>) =>
    onUpdate({ data_source: { ...ds, ...patch } })

  const updateSty = (patch: Partial<Widget['style']>) =>
    onUpdate({ style: { ...sty, ...patch } })

  useEffect(() => {
    if (ds.type !== 'pipeline') return
    let active = true
    setPipelinesLoading(true)

    const loadPipelines = async () => {
      try {
        const response = await vizHttp.get('/api/pipelines')
        if (!active) return
        const rows = Array.isArray(response.data) ? response.data : []
        const options: PipelineOption[] = rows
          .filter((row: any) => row?.id && row?.name)
          .map((row: any) => {
            const status = row?.last_execution?.status
            const suffix = status === 'success' ? '' : (status ? ` (${status})` : ' (no runs)')
            return {
              value: String(row.id),
              label: `${String(row.name)}${suffix}`,
            }
          })
        setPipelineOptions(options)
      } catch {
        if (active) setPipelineOptions([])
      } finally {
        if (active) setPipelinesLoading(false)
      }
    }

    void loadPipelines()
    return () => {
      active = false
    }
  }, [ds.type])

  useEffect(() => {
    if (ds.type !== 'pipeline') return
    const pipelineId = (ds.pipeline_id || '').trim()
    if (!pipelineId) {
      setPipelineNodes([])
      return
    }

    let active = true
    setPipelineNodesLoading(true)
    const loadPipelineNodes = async () => {
      try {
        const response = await vizHttp.get(`/api/pipelines/${pipelineId}`)
        if (!active) return
        const nodes = Array.isArray(response.data?.nodes) ? response.data.nodes : []
        const nodeOptions: PipelineNodeOption[] = nodes
          .filter((node: any) => node?.id)
          .map((node: any) => {
            const nodeType = String(node?.data?.nodeType || 'node')
            const label = String(node?.data?.label || nodeType)
            return {
              value: String(node.id),
              label: `${label} [${nodeType}]`,
            }
          })
        setPipelineNodes(nodeOptions)
      } catch {
        if (active) setPipelineNodes([])
      } finally {
        if (active) setPipelineNodesLoading(false)
      }
    }

    void loadPipelineNodes()
    return () => {
      active = false
    }
  }, [ds.type, ds.pipeline_id])

  useEffect(() => {
    if (ds.type !== 'mlops') return
    let active = true
    setMLOpsWorkflowsLoading(true)

    const loadWorkflows = async () => {
      try {
        const response = await vizHttp.get('/api/mlops/workflows')
        if (!active) return
        const rows = Array.isArray(response.data) ? response.data : []
        const options: MLOpsWorkflowOption[] = rows
          .filter((row: any) => row?.id && row?.name)
          .map((row: any) => {
            const lastRunStatus = row?.last_run?.status
            const modelVersion = row?.last_run?.model_version
            let suffix = ' (no runs)'
            if (lastRunStatus) {
              suffix = modelVersion
                ? ` (${lastRunStatus} • ${modelVersion})`
                : ` (${lastRunStatus})`
            }
            return {
              value: String(row.id),
              label: `${String(row.name)}${suffix}`,
            }
          })
        setMLOpsWorkflowOptions(options)
      } catch {
        if (active) setMLOpsWorkflowOptions([])
      } finally {
        if (active) setMLOpsWorkflowsLoading(false)
      }
    }

    void loadWorkflows()
    return () => {
      active = false
    }
  }, [ds.type])

  useEffect(() => {
    if (ds.type !== 'mlops') return
    const workflowId = (ds.mlops_workflow_id || '').trim()
    if (!workflowId) {
      setMLOpsRunOptions([])
      setMLOpsNodeOptions([])
      return
    }

    let active = true
    setMLOpsRunsLoading(true)
    setMLOpsNodesLoading(true)

    const loadWorkflowDetails = async () => {
      try {
        const [runOptions, workflowResponse] = await Promise.all([
          loadMLOpsRunOptions(workflowId),
          vizHttp.get(`/api/mlops/workflows/${workflowId}`),
        ])
        if (!active) return

        setMLOpsRunOptions(runOptions)
        if (!ds.mlops_run_id && runOptions.length > 0) {
          const first = runOptions[0]
          updateDs({
            mlops_workflow_id: first.workflowId || ds.mlops_workflow_id,
            mlops_run_id: first.value,
            mlops_node_id: undefined,
          })
        }
        if (ds.mlops_run_id && !runOptions.some((option) => option.value === ds.mlops_run_id)) {
          const first = runOptions[0]
          updateDs({
            mlops_workflow_id: first?.workflowId || ds.mlops_workflow_id,
            mlops_run_id: first?.value,
            mlops_node_id: undefined,
          })
        }

        const nodes = Array.isArray(workflowResponse.data?.nodes) ? workflowResponse.data.nodes : []
        const nodeOptions: PipelineNodeOption[] = nodes
          .filter((node: any) => node?.id)
          .map((node: any) => {
            const nodeType = String(node?.data?.nodeType || 'node')
            const label = String(node?.data?.label || nodeType)
            return {
              value: String(node.id),
              label: `${label} [${nodeType}]`,
            }
          })
        setMLOpsNodeOptions(nodeOptions)
      } catch {
        if (active) {
          setMLOpsRunOptions([])
          setMLOpsNodeOptions([])
        }
      } finally {
        if (active) {
          setMLOpsRunsLoading(false)
          setMLOpsNodesLoading(false)
        }
      }
    }

    void loadWorkflowDetails()
    return () => {
      active = false
    }
  }, [ds.type, ds.mlops_workflow_id])

  const handleDataSourceTypeChange = (nextType: Widget['data_source']['type']) => {
    if (nextType === 'sample') {
      updateDs({
        type: nextType,
        dataset: ds.dataset || 'sales',
        pipeline_id: undefined,
        pipeline_node_id: undefined,
        file_path: undefined,
        mlops_workflow_id: undefined,
        mlops_run_id: undefined,
        mlops_node_id: undefined,
        mlops_output_mode: undefined,
        mlops_prediction_start_date: undefined,
        mlops_prediction_end_date: undefined,
      })
      return
    }
    if (nextType === 'pipeline') {
      updateDs({
        type: nextType,
        dataset: undefined,
        file_path: undefined,
        mlops_workflow_id: undefined,
        mlops_run_id: undefined,
        mlops_node_id: undefined,
        mlops_output_mode: undefined,
        mlops_prediction_start_date: undefined,
        mlops_prediction_end_date: undefined,
      })
      return
    }
    if (nextType === 'mlops') {
      updateDs({
        type: nextType,
        dataset: undefined,
        pipeline_id: undefined,
        pipeline_node_id: undefined,
        file_path: undefined,
        mlops_output_mode: ds.mlops_output_mode || 'predictions',
        mlops_prediction_start_date: ds.mlops_prediction_start_date,
        mlops_prediction_end_date: ds.mlops_prediction_end_date,
      })
      return
    }
    updateDs({
      type: nextType,
      dataset: undefined,
      pipeline_id: undefined,
      pipeline_node_id: undefined,
      file_path: ds.file_path,
      mlops_workflow_id: undefined,
      mlops_run_id: undefined,
      mlops_node_id: undefined,
      mlops_output_mode: undefined,
      mlops_prediction_start_date: undefined,
      mlops_prediction_end_date: undefined,
    })
  }

  const tabItems = [
    {
      key: 'data',
      label: 'Data',
      children: (
        <div style={{ paddingTop: 8 }}>
          {/* Data Source type */}
          <div style={S.formRow}>
            <Text style={S.formLabel}>Data Source</Text>
            <Select
              size="small"
              value={ds.type}
              onChange={v => handleDataSourceTypeChange(v as Widget['data_source']['type'])}
              options={[
                { label: 'Sample Dataset', value: 'sample' },
                { label: 'Pipeline', value: 'pipeline' },
                { label: 'MLOps Model', value: 'mlops' },
                { label: 'File', value: 'file' },
              ]}
              style={{ width: '100%' }}
            />
          </div>

          {ds.type === 'sample' && (
            <div style={S.formRow}>
              <Text style={S.formLabel}>Dataset</Text>
              <Select
                size="small"
                value={ds.dataset || 'sales'}
                onChange={v => updateDs({ dataset: v })}
                options={SAMPLE_DATASETS.map(d => ({ label: `${d.icon} ${d.label}`, value: d.key }))}
                style={{ width: '100%' }}
              />
            </div>
          )}

          {ds.type === 'pipeline' && (
            <>
              <div style={S.formRow}>
                <Text style={S.formLabel}>Pipeline Output</Text>
                <Select
                  size="small"
                  value={ds.pipeline_id || undefined}
                  onChange={(value) => updateDs({ pipeline_id: value, pipeline_node_id: undefined })}
                  placeholder="Select pipeline"
                  loading={pipelinesLoading}
                  options={pipelineOptions}
                  showSearch
                  optionFilterProp="label"
                  allowClear
                  style={{ width: '100%' }}
                />
              </div>

              <div style={S.formRow}>
                <Text style={S.formLabel}>Pipeline Node (optional)</Text>
                <Select
                  size="small"
                  value={ds.pipeline_node_id || undefined}
                  onChange={(value) => updateDs({ pipeline_node_id: value })}
                  placeholder="Auto-detect latest tabular node"
                  loading={pipelineNodesLoading}
                  disabled={!ds.pipeline_id}
                  options={pipelineNodes}
                  showSearch
                  optionFilterProp="label"
                  allowClear
                  style={{ width: '100%' }}
                />
              </div>

              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11, marginBottom: 10, display: 'block' }}>
                Uses the latest successful execution output from the selected ETL pipeline.
              </Text>
            </>
          )}

          {ds.type === 'mlops' && (
            <>
              <div style={S.formRow}>
                <Text style={S.formLabel}>MLOps Workflow</Text>
                <Select
                  size="small"
                  value={ds.mlops_workflow_id || undefined}
                  onChange={(value) => updateDs({
                    mlops_workflow_id: value,
                    mlops_run_id: undefined,
                    mlops_node_id: undefined,
                  })}
                  placeholder="Select workflow/model"
                  loading={mlopsWorkflowsLoading}
                  options={mlopsWorkflowOptions}
                  showSearch
                  optionFilterProp="label"
                  allowClear
                  style={{ width: '100%' }}
                />
              </div>

              <div style={S.formRow}>
                <Text style={S.formLabel}>Model Version / Run (optional)</Text>
                <Select
                  size="small"
                  value={ds.mlops_run_id || undefined}
                  onChange={(value, option) => {
                    const selected = option as MLOpsRunOption | undefined
                    const runWorkflowId = String(selected?.workflowId || '').trim()
                    const currentWorkflowId = String(ds.mlops_workflow_id || '').trim()
                    if (runWorkflowId && runWorkflowId !== currentWorkflowId) {
                      updateDs({
                        mlops_workflow_id: runWorkflowId,
                        mlops_run_id: value,
                        mlops_node_id: undefined,
                      })
                      return
                    }
                    updateDs({ mlops_run_id: value })
                  }}
                  placeholder="Latest successful model run"
                  loading={mlopsRunsLoading}
                  disabled={!ds.mlops_workflow_id}
                  options={mlopsRunOptions}
                  showSearch
                  optionFilterProp="label"
                  allowClear
                  style={{ width: '100%' }}
                />
              </div>

              <div style={S.formRow}>
                <Text style={S.formLabel}>Prediction Node (optional)</Text>
                <Select
                  size="small"
                  value={ds.mlops_node_id || undefined}
                  onChange={(value) => updateDs({ mlops_node_id: value })}
                  placeholder="Auto-select best prediction node"
                  loading={mlopsNodesLoading}
                  disabled={!ds.mlops_workflow_id}
                  options={mlopsNodeOptions}
                  showSearch
                  optionFilterProp="label"
                  allowClear
                  style={{ width: '100%' }}
                />
              </div>

              <div style={S.formRow}>
                <Text style={S.formLabel}>Output Mode</Text>
                <Select
                  size="small"
                  value={ds.mlops_output_mode || 'predictions'}
                  onChange={(value) => updateDs({ mlops_output_mode: value as 'predictions' | 'metrics' | 'monitor' | 'evaluation' })}
                  options={[
                    { label: 'Predictions', value: 'predictions' },
                    { label: 'Metrics Summary', value: 'metrics' },
                    { label: 'Monitoring Runs', value: 'monitor' },
                    { label: 'Evaluation Output', value: 'evaluation' },
                  ]}
                  style={{ width: '100%' }}
                />
              </div>

              <div style={S.formRow}>
                <Text style={S.formLabel}>Prediction Start Date</Text>
                <Input
                  size="small"
                  type="date"
                  value={ds.mlops_prediction_start_date || ''}
                  onChange={(e) => updateDs({ mlops_prediction_start_date: e.target.value || undefined })}
                  style={S.inputDark}
                />
              </div>

              <div style={S.formRow}>
                <Text style={S.formLabel}>Prediction End Date</Text>
                <Input
                  size="small"
                  type="date"
                  value={ds.mlops_prediction_end_date || ''}
                  onChange={(e) => updateDs({ mlops_prediction_end_date: e.target.value || undefined })}
                  style={S.inputDark}
                />
              </div>

              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11, marginBottom: 10, display: 'block' }}>
                Uses the selected MLOps/H2O run output so widgets can visualize predictions, metrics, monitoring, or evaluation rows.
              </Text>
            </>
          )}

          {ds.type === 'file' && (
            <div style={S.formRow}>
              <Text style={S.formLabel}>File Path</Text>
              <Input
                size="small"
                value={ds.file_path || ''}
                onChange={e => updateDs({ file_path: e.target.value })}
                placeholder="/data/file.csv"
                style={S.inputDark}
              />
            </div>
          )}

          <Divider style={{ borderColor: 'var(--app-border)', margin: '8px 0' }} />

          {/* Field mapping */}
          <div style={S.formRow}>
            <Text style={S.formLabel}>X Axis Field</Text>
            <Select
              size="small"
              value={cfg.x_field || undefined}
              placeholder="Select field"
              onChange={v => updateCfg({ x_field: v })}
              options={colOptions}
              style={{ width: '100%' }}
              allowClear
            />
          </div>

          {(widget.type === 'kpi' || widget.type === 'gauge') ? (
            <>
              <div style={S.formRow}>
                <Text style={S.formLabel}>Metric Field</Text>
                <Select
                  size="small"
                  value={cfg.kpi_field || cfg.y_field || undefined}
                  placeholder="Select numeric field"
                  onChange={v => updateCfg({ kpi_field: v, y_field: v })}
                  options={colOptions}
                  style={{ width: '100%' }}
                  allowClear
                />
              </div>

              <div style={S.formRow}>
                <Text style={S.formLabel}>Metric Label</Text>
                <Input
                  size="small"
                  value={cfg.kpi_label || ''}
                  onChange={e => updateCfg({ kpi_label: e.target.value })}
                  placeholder="e.g. Total Revenue"
                  style={S.inputDark}
                />
              </div>
            </>
          ) : (
            <div style={S.formRow}>
              <Text style={S.formLabel}>Y Axis Field</Text>
              <Select
                size="small"
                value={cfg.y_field || undefined}
                placeholder="Select field"
                onChange={v => updateCfg({ y_field: v, y_fields: v ? [v] : [] })}
                options={colOptions}
                style={{ width: '100%' }}
                allowClear
              />
            </div>
          )}

          <div style={S.formRow}>
            <Text style={S.formLabel}>Group By</Text>
            <Select
              size="small"
              value={cfg.group_by || undefined}
              placeholder="None"
              onChange={v => updateCfg({ group_by: v })}
              options={colOptions}
              style={{ width: '100%' }}
              allowClear
            />
          </div>

          <div style={S.formRow}>
            <Text style={S.formLabel}>Aggregation</Text>
            <Select
              size="small"
              value={cfg.aggregation || 'sum'}
              onChange={v => updateCfg({ aggregation: v })}
              options={[
                { label: 'Sum', value: 'sum' },
                { label: 'Average', value: 'avg' },
                { label: 'Count', value: 'count' },
                { label: 'Min', value: 'min' },
                { label: 'Max', value: 'max' },
              ]}
              style={{ width: '100%' }}
            />
          </div>

          <div style={S.formRow}>
            <Text style={S.formLabel}>Row Limit</Text>
            <InputNumber
              size="small"
              value={cfg.limit ?? 50}
              min={1}
              max={10000}
              onChange={v => updateCfg({ limit: v ?? 50 })}
              style={{ width: '100%', background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)', color: 'var(--app-text)' }}
            />
          </div>

          <Button
            size="small"
            icon={<ReloadOutlined />}
            onClick={onRefresh}
            style={{ width: '100%', background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)', color: 'var(--app-text-muted)' }}
          >
            Refresh Data
          </Button>
        </div>
      ),
    },
    {
      key: 'chart',
      label: 'Chart',
      children: (
        <div style={{ paddingTop: 8 }}>
          <div style={S.formRow}>
            <Text style={S.formLabel}>Sort By</Text>
            <div style={{ display: 'flex', gap: 6 }}>
              <Select
                size="small"
                value={cfg.sort_by || undefined}
                placeholder="None"
                onChange={v => updateCfg({ sort_by: v })}
                options={[
                  { label: 'Value', value: 'value' },
                  { label: 'Name', value: 'name' },
                  ...colOptions,
                ]}
                style={{ flex: 1 }}
                allowClear
              />
              <Select
                size="small"
                value={cfg.sort_order || 'desc'}
                onChange={v => updateCfg({ sort_order: v })}
                options={[
                  { label: '↓ Desc', value: 'desc' },
                  { label: '↑ Asc', value: 'asc' },
                ]}
                style={{ width: 90 }}
              />
            </div>
          </div>

          <div style={S.formRow}>
            <Text style={S.formLabel}>Y Fields (multi-series)</Text>
            <Select
              size="small"
              mode="multiple"
              value={cfg.y_fields ?? []}
              onChange={v => updateCfg({ y_fields: v, y_field: v.length > 0 ? v[0] : undefined })}
              options={colOptions}
              style={{ width: '100%' }}
              placeholder="Select fields"
            />
          </div>

          <Divider style={{ borderColor: 'var(--app-border)', margin: '8px 0' }} />

          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Text style={S.formLabel}>Show Legend</Text>
              <Switch
                size="small"
                checked={sty.show_legend !== false}
                onChange={v => updateSty({ show_legend: v })}
              />
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Text style={S.formLabel}>Show Grid</Text>
              <Switch
                size="small"
                checked={sty.show_grid !== false}
                onChange={v => updateSty({ show_grid: v })}
              />
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Text style={S.formLabel}>Show Labels</Text>
              <Switch
                size="small"
                checked={sty.show_labels === true}
                onChange={v => updateSty({ show_labels: v })}
              />
            </div>
          </div>
        </div>
      ),
    },
    {
      key: 'style',
      label: 'Style',
      children: (
        <div style={{ paddingTop: 8 }}>
          <div style={S.formRow}>
            <Text style={S.formLabel}>Color Palette</Text>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 6, marginTop: 4 }}>
              {COLOR_PALETTES.map(p => {
                const isActive = JSON.stringify(sty.color_palette) === JSON.stringify(p.colors)
                return (
                  <Tooltip key={p.key} title={p.label}>
                    <div
                      onClick={() => updateSty({ color_palette: p.colors })}
                      style={{
                        borderRadius: 6,
                        border: `2px solid ${isActive ? '#6366f1' : 'var(--app-border)'}`,
                        cursor: 'pointer',
                        overflow: 'hidden',
                        display: 'flex',
                        height: 24,
                        transition: 'border-color 0.15s',
                      }}
                    >
                      {p.colors.slice(0, 6).map((c, i) => (
                        <div key={i} style={{ flex: 1, background: c }} />
                      ))}
                    </div>
                  </Tooltip>
                )
              })}
            </div>
          </div>

          <div style={S.formRow}>
            <Text style={S.formLabel}>Background Color</Text>
            <ColorPicker
              size="small"
              value={sty.bg_color || 'var(--app-panel-bg)'}
              onChange={(c: Color) => updateSty({ bg_color: c.toHexString() })}
              presets={[{
                label: 'Recommended',
                colors: ['var(--app-panel-bg)', 'var(--app-shell-bg-2)', '#0f0f1a', '#1a1a2e', '#16213e', 'var(--app-panel-2)'],
              }]}
            />
          </div>
        </div>
      ),
    },
  ]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' }}>
      {/* Panel header */}
      <div style={{
        height: 48,
        minHeight: 48,
        borderBottom: '1px solid var(--app-border)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '0 14px',
        gap: 8,
        flexShrink: 0,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <SettingOutlined style={{ color: '#6366f1', fontSize: 14 }} />
          <Text style={{ color: 'var(--app-text)', fontSize: 13, fontWeight: 600 }}>Widget Config</Text>
        </div>
        <Button
          type="text"
          size="small"
          icon={<CloseOutlined />}
          onClick={onClose}
          style={{ color: 'var(--app-text-dim)' }}
        />
      </div>

      <div style={S.panelScroll}>
        {/* Title */}
        <div style={S.formRow}>
          <Text style={S.formLabel}>Widget Title</Text>
          <Input
            size="small"
            value={widget.title}
            onChange={e => onUpdate({ title: e.target.value })}
            style={S.inputDark}
          />
        </div>
        {/* Subtitle */}
        <div style={S.formRow}>
          <Text style={S.formLabel}>Subtitle</Text>
          <Input
            size="small"
            value={widget.subtitle || ''}
            onChange={e => onUpdate({ subtitle: e.target.value })}
            placeholder="Optional subtitle"
            style={S.inputDark}
          />
        </div>

        {/* Size controls */}
        <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
          <div style={{ flex: 1 }}>
            <Text style={{ ...S.formLabel, display: 'block', marginBottom: 4 }}>Width (cols)</Text>
            <InputNumber
              size="small"
              min={2}
              max={24}
              value={layoutItem?.w ?? 12}
              onChange={v => onSizeChange(v ?? 12, layoutItem?.h ?? 9)}
              style={{ width: '100%', background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)' }}
            />
          </div>
          <div style={{ flex: 1 }}>
            <Text style={{ ...S.formLabel, display: 'block', marginBottom: 4 }}>Height (rows)</Text>
            <InputNumber
              size="small"
              min={2}
              max={40}
              value={layoutItem?.h ?? 9}
              onChange={v => onSizeChange(layoutItem?.w ?? 12, v ?? 9)}
              style={{ width: '100%', background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)' }}
            />
          </div>
        </div>

        <Divider style={{ borderColor: 'var(--app-border)', margin: '8px 0' }} />

        <Tabs
          size="small"
          defaultActiveKey="data"
          items={tabItems}
          tabBarStyle={{ borderBottom: '1px solid var(--app-border)', marginBottom: 0 }}
        />
      </div>

      {/* Remove widget */}
      <div style={{ padding: '10px 14px', borderTop: '1px solid var(--app-border)', flexShrink: 0 }}>
        <Button
          danger
          size="small"
          icon={<DeleteOutlined />}
          onClick={onRemove}
          style={{ width: '100%' }}
        >
          Remove Widget
        </Button>
      </div>
    </div>
  )
}

const { Text } = Typography

// ─── DashboardEditor ─────────────────────────────────────────────────────────

export default function DashboardEditor() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const [messageApi, contextHolder] = message.useMessage()

  const {
    activeDashboard,
    isDirty,
    loading,
    widgetData,
    loadDashboard,
    saveDashboard,
    updateLayout,
    addWidget,
    updateWidget,
    removeWidget,
    fetchWidgetData,
    setActiveDashboard,
  } = useVizStore()

  const [selectedWidgetId, setSelectedWidgetId] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [analyticsOpen, setAnalyticsOpen] = useState(false)
  const [analyticsGenerating, setAnalyticsGenerating] = useState(false)
  const [pythonVizOpen, setPythonVizOpen] = useState(false)
  const [pythonVizLoading, setPythonVizLoading] = useState(false)
  const [pythonVizResult, setPythonVizResult] = useState<PythonAnalyticsResult | null>(null)
  const [pythonVizError, setPythonVizError] = useState<string | null>(null)
  const [pythonFieldSchemaLoading, setPythonFieldSchemaLoading] = useState(false)
  const [pythonFieldColumns, setPythonFieldColumns] = useState<string[]>([])
  const [pythonFieldMetrics, setPythonFieldMetrics] = useState<string[]>([])
  const [pythonFieldDimensions, setPythonFieldDimensions] = useState<string[]>([])
  const [pythonWidgetTargetType, setPythonWidgetTargetType] = useState<PythonWidgetTargetType>('auto')
  const [analyticsConfig, setAnalyticsConfig] = useState<AnalyticsAutoConfig>({
    source_type: 'sample',
    dataset: 'sales',
    mlops_output_mode: 'predictions',
    forecast_horizon: 30,
    nlp_prompt: '',
  })
  const [pythonVizConfig, setPythonVizConfig] = useState<PythonAnalyticsConfig>({
    chart_type: 'auto',
    x_field: '',
    y_field: '',
    color_field: '',
    group_by: '',
    title: '',
    limit: 5000,
  })
  const [analyticsPipelineOptions, setAnalyticsPipelineOptions] = useState<PipelineOption[]>([])
  const [analyticsPipelineNodes, setAnalyticsPipelineNodes] = useState<PipelineNodeOption[]>([])
  const [analyticsMLOpsWorkflows, setAnalyticsMLOpsWorkflows] = useState<MLOpsWorkflowOption[]>([])
  const [analyticsMLOpsRuns, setAnalyticsMLOpsRuns] = useState<MLOpsRunOption[]>([])
  const [analyticsMLOpsNodes, setAnalyticsMLOpsNodes] = useState<PipelineNodeOption[]>([])
  const [analyticsPipelinesLoading, setAnalyticsPipelinesLoading] = useState(false)
  const [analyticsPipelineNodesLoading, setAnalyticsPipelineNodesLoading] = useState(false)
  const [analyticsMLOpsWorkflowsLoading, setAnalyticsMLOpsWorkflowsLoading] = useState(false)
  const [analyticsMLOpsRunsLoading, setAnalyticsMLOpsRunsLoading] = useState(false)
  const [analyticsMLOpsNodesLoading, setAnalyticsMLOpsNodesLoading] = useState(false)
  const [editingName, setEditingName] = useState(false)
  const [nameValue, setNameValue] = useState('')
  const [rulerGuide, setRulerGuide] = useState<{ x: number; y: number; visible: boolean }>({
    x: 0,
    y: 0,
    visible: false,
  })
  const lastRulerUpdateAtRef = useRef(0)
  const nameInputRef = useRef<InputRef>(null)
  const latestLayoutRef = useRef<LayoutItem[]>([])
  const {
    width: gridWidth,
    containerRef: gridContainerRef,
    mounted: gridMounted,
  } = useContainerWidth({ initialWidth: 1280, measureBeforeMount: false })

  // Load dashboard on mount
  useEffect(() => {
    if (id) {
      loadDashboard(id)
    }
  }, [id, loadDashboard])

  useEffect(() => {
    if (!id || !activeDashboard || activeDashboard.id !== id) return
    const isPythonDashboard = Array.isArray(activeDashboard.tags)
      && activeDashboard.tags.some((tag) => String(tag).trim() === PYTHON_DASHBOARD_TAG)
    if (!isPythonDashboard) return
    navigate(`/python-analytics?dashboardId=${encodeURIComponent(id)}`, { replace: true })
  }, [id, activeDashboard, navigate])

  // Sync name state
  useEffect(() => {
    if (activeDashboard) {
      setNameValue(activeDashboard.name)
    }
  }, [activeDashboard?.name])

  // Focus name input when editing starts
  useEffect(() => {
    if (editingName) {
      nameInputRef.current?.focus()
    }
  }, [editingName])

  useEffect(() => {
    if (!(analyticsOpen || pythonVizOpen)) return
    let active = true
    setAnalyticsPipelinesLoading(true)
    const load = async () => {
      try {
        const response = await vizHttp.get('/api/pipelines')
        if (!active) return
        const rows = Array.isArray(response.data) ? response.data : []
        const options = rows
          .filter((row: any) => row?.id && row?.name)
          .map((row: any) => {
            const status = row?.last_execution?.status
            const suffix = status === 'success' ? '' : (status ? ` (${status})` : ' (no runs)')
            return { value: String(row.id), label: `${String(row.name)}${suffix}` }
          })
        setAnalyticsPipelineOptions(options)
      } catch {
        if (active) setAnalyticsPipelineOptions([])
      } finally {
        if (active) setAnalyticsPipelinesLoading(false)
      }
    }
    void load()
    return () => {
      active = false
    }
  }, [analyticsOpen, pythonVizOpen])

  useEffect(() => {
    if (!(analyticsOpen || pythonVizOpen) || analyticsConfig.source_type !== 'pipeline') return
    const pipelineId = (analyticsConfig.pipeline_id || '').trim()
    if (!pipelineId) {
      setAnalyticsPipelineNodes([])
      return
    }
    let active = true
    setAnalyticsPipelineNodesLoading(true)
    const load = async () => {
      try {
        const response = await vizHttp.get(`/api/pipelines/${pipelineId}`)
        if (!active) return
        const nodes = Array.isArray(response.data?.nodes) ? response.data.nodes : []
        const options = nodes
          .filter((node: any) => node?.id)
          .map((node: any) => {
            const nodeType = String(node?.data?.nodeType || 'node')
            const label = String(node?.data?.label || nodeType)
            return { value: String(node.id), label: `${label} [${nodeType}]` }
          })
        setAnalyticsPipelineNodes(options)
      } catch {
        if (active) setAnalyticsPipelineNodes([])
      } finally {
        if (active) setAnalyticsPipelineNodesLoading(false)
      }
    }
    void load()
    return () => {
      active = false
    }
  }, [analyticsOpen, pythonVizOpen, analyticsConfig.source_type, analyticsConfig.pipeline_id])

  useEffect(() => {
    if (!(analyticsOpen || pythonVizOpen)) return
    let active = true
    setAnalyticsMLOpsWorkflowsLoading(true)
    const load = async () => {
      try {
        const response = await vizHttp.get('/api/mlops/workflows')
        if (!active) return
        const rows = Array.isArray(response.data) ? response.data : []
        const options = rows
          .filter((row: any) => row?.id && row?.name)
          .map((row: any) => {
            const status = row?.last_run?.status
            const modelVersion = row?.last_run?.model_version
            const suffix = status ? (modelVersion ? ` (${status} • ${modelVersion})` : ` (${status})`) : ' (no runs)'
            return { value: String(row.id), label: `${String(row.name)}${suffix}` }
          })
        setAnalyticsMLOpsWorkflows(options)
      } catch {
        if (active) setAnalyticsMLOpsWorkflows([])
      } finally {
        if (active) setAnalyticsMLOpsWorkflowsLoading(false)
      }
    }
    void load()
    return () => {
      active = false
    }
  }, [analyticsOpen, pythonVizOpen])

  useEffect(() => {
    if (!(analyticsOpen || pythonVizOpen) || analyticsConfig.source_type !== 'mlops') return
    const workflowId = (analyticsConfig.mlops_workflow_id || '').trim()
    if (!workflowId) {
      setAnalyticsMLOpsRuns([])
      setAnalyticsMLOpsNodes([])
      return
    }
    let active = true
    setAnalyticsMLOpsRunsLoading(true)
    setAnalyticsMLOpsNodesLoading(true)
    const load = async () => {
      try {
        const [runOptions, workflowResponse] = await Promise.all([
          loadMLOpsRunOptions(workflowId),
          vizHttp.get(`/api/mlops/workflows/${workflowId}`),
        ])
        if (!active) return
        setAnalyticsMLOpsRuns(runOptions)
        if (!analyticsConfig.mlops_run_id && runOptions.length > 0) {
          const first = runOptions[0]
          setAnalyticsConfig((prev) => ({
            ...prev,
            mlops_workflow_id: first.workflowId || prev.mlops_workflow_id,
            mlops_run_id: first.value,
            mlops_node_id: undefined,
          }))
        }
        if (analyticsConfig.mlops_run_id && !runOptions.some((option) => option.value === analyticsConfig.mlops_run_id)) {
          const first = runOptions[0]
          setAnalyticsConfig((prev) => ({
            ...prev,
            mlops_workflow_id: first?.workflowId || prev.mlops_workflow_id,
            mlops_run_id: first?.value,
            mlops_node_id: undefined,
          }))
        }

        const nodes = Array.isArray(workflowResponse.data?.nodes) ? workflowResponse.data.nodes : []
        const nodeOptions = nodes
          .filter((node: any) => node?.id)
          .map((node: any) => {
            const nodeType = String(node?.data?.nodeType || 'node')
            const label = String(node?.data?.label || nodeType)
            return { value: String(node.id), label: `${label} [${nodeType}]` }
          })
        setAnalyticsMLOpsNodes(nodeOptions)
      } catch {
        if (active) {
          setAnalyticsMLOpsRuns([])
          setAnalyticsMLOpsNodes([])
        }
      } finally {
        if (active) {
          setAnalyticsMLOpsRunsLoading(false)
          setAnalyticsMLOpsNodesLoading(false)
        }
      }
    }
    void load()
    return () => {
      active = false
    }
  }, [analyticsOpen, pythonVizOpen, analyticsConfig.source_type, analyticsConfig.mlops_workflow_id])

  const triggerSave = useCallback(async () => {
    setSaving(true)
    try {
      await saveDashboard(latestLayoutRef.current)
      messageApi.success('Dashboard saved successfully')
    } catch {
      messageApi.error('Save failed — please try again')
    } finally {
      setSaving(false)
    }
  }, [saveDashboard, messageApi])

  // Ctrl/Cmd+S keyboard shortcut
  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 's') {
        e.preventDefault()
        triggerSave()
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [triggerSave])

  const handleNameCommit = useCallback(() => {
    setEditingName(false)
    if (!activeDashboard) return
    setActiveDashboard({ ...activeDashboard, name: nameValue })
  }, [activeDashboard, nameValue, setActiveDashboard])

  const commitGridLayout = useCallback((currentLayout: readonly RGLLayoutItem[]) => {
    const mapped: LayoutItem[] = currentLayout.map(l => ({
      i: l.i,
      x: l.x,
      y: l.y,
      w: l.w,
      h: l.h,
      minW: l.minW,
      minH: l.minH,
    }))
    latestLayoutRef.current = mapped
    updateLayout(mapped)
  }, [updateLayout])

  const handleDragOrResizeStop = useCallback(
    (currentLayout: readonly RGLLayoutItem[]) => {
      commitGridLayout(currentLayout)
    },
    [commitGridLayout]
  )

  const handleThemeToggle = useCallback(
    (checked: boolean) => {
      if (!activeDashboard) return
      setActiveDashboard({ ...activeDashboard, theme: checked ? 'light' : 'dark' })
    },
    [activeDashboard, setActiveDashboard]
  )

  const selectedWidget = activeDashboard?.widgets.find(w => w.id === selectedWidgetId) ?? null

  const updateAnalyticsConfig = useCallback((patch: Partial<AnalyticsAutoConfig>) => {
    setAnalyticsConfig((prev) => ({ ...prev, ...patch }))
  }, [])

  const handleAnalyticsSourceTypeChange = useCallback((nextType: AnalyticsSourceType) => {
    if (nextType === 'sample') {
      updateAnalyticsConfig({
        source_type: nextType,
        dataset: analyticsConfig.dataset || 'sales',
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
      updateAnalyticsConfig({
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
      updateAnalyticsConfig({
        source_type: nextType,
        dataset: undefined,
        pipeline_id: undefined,
        pipeline_node_id: undefined,
        file_path: undefined,
        mlops_output_mode: analyticsConfig.mlops_output_mode || 'predictions',
      })
      return
    }
    updateAnalyticsConfig({
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
  }, [analyticsConfig.dataset, analyticsConfig.mlops_output_mode, updateAnalyticsConfig])

  const openAnalyticsBuilder = useCallback(() => {
    const source = selectedWidget?.data_source || activeDashboard?.widgets?.[0]?.data_source
    const sourceType = source?.type
    const normalizedSourceType: AnalyticsSourceType =
      sourceType === 'sample' || sourceType === 'pipeline' || sourceType === 'file' || sourceType === 'mlops'
        ? sourceType
        : (analyticsConfig.source_type || 'sample')
    setAnalyticsConfig((prev) => ({
      ...prev,
      source_type: normalizedSourceType,
      dataset: source?.dataset || prev.dataset || 'sales',
      pipeline_id: source?.pipeline_id,
      pipeline_node_id: source?.pipeline_node_id,
      file_path: source?.file_path,
      mlops_workflow_id: source?.mlops_workflow_id,
      mlops_run_id: source?.mlops_run_id,
      mlops_node_id: source?.mlops_node_id,
      mlops_output_mode: (source?.mlops_output_mode as 'predictions' | 'metrics' | 'monitor' | 'evaluation') || prev.mlops_output_mode || 'predictions',
      mlops_prediction_start_date: source?.mlops_prediction_start_date || prev.mlops_prediction_start_date,
      mlops_prediction_end_date: source?.mlops_prediction_end_date || prev.mlops_prediction_end_date,
      forecast_horizon: prev.forecast_horizon || 30,
      nlp_prompt: prev.nlp_prompt || '',
    }))
    setAnalyticsOpen(true)
  }, [selectedWidget, activeDashboard, analyticsConfig.source_type])

  const openPythonAnalyticsOverlay = useCallback(() => {
    const source = selectedWidget?.data_source || activeDashboard?.widgets?.[0]?.data_source
    const sourceType = source?.type
    const normalizedSourceType: AnalyticsSourceType =
      sourceType === 'sample' || sourceType === 'pipeline' || sourceType === 'file' || sourceType === 'mlops'
        ? sourceType
        : (analyticsConfig.source_type || 'sample')
    setAnalyticsConfig((prev) => ({
      ...prev,
      source_type: normalizedSourceType,
      dataset: source?.dataset || prev.dataset || 'sales',
      pipeline_id: source?.pipeline_id,
      pipeline_node_id: source?.pipeline_node_id,
      file_path: source?.file_path,
      mlops_workflow_id: source?.mlops_workflow_id,
      mlops_run_id: source?.mlops_run_id,
      mlops_node_id: source?.mlops_node_id,
      mlops_output_mode: (source?.mlops_output_mode as 'predictions' | 'metrics' | 'monitor' | 'evaluation') || prev.mlops_output_mode || 'predictions',
      mlops_prediction_start_date: source?.mlops_prediction_start_date || prev.mlops_prediction_start_date,
      mlops_prediction_end_date: source?.mlops_prediction_end_date || prev.mlops_prediction_end_date,
      sql: source?.sql || prev.sql,
    }))
    setPythonVizError(null)
    setPythonVizResult(null)
    setPythonWidgetTargetType('auto')
    setPythonFieldColumns([])
    setPythonFieldMetrics([])
    setPythonFieldDimensions([])
    setPythonVizOpen(true)
  }, [selectedWidget, activeDashboard, analyticsConfig.source_type])

  const buildAnalyticsSourcePayload = useCallback((): Record<string, unknown> => ({
    source_type: analyticsConfig.source_type,
    dataset: analyticsConfig.dataset,
    pipeline_id: analyticsConfig.pipeline_id,
    pipeline_node_id: analyticsConfig.pipeline_node_id,
    file_path: analyticsConfig.file_path,
    mlops_workflow_id: analyticsConfig.mlops_workflow_id,
    mlops_run_id: analyticsConfig.mlops_run_id,
    mlops_node_id: analyticsConfig.mlops_node_id,
    mlops_output_mode: analyticsConfig.mlops_output_mode,
    mlops_prediction_start_date: analyticsConfig.mlops_prediction_start_date,
    mlops_prediction_end_date: analyticsConfig.mlops_prediction_end_date,
    sql: analyticsConfig.sql,
  }), [analyticsConfig])

  const handleLoadPythonFieldSchema = useCallback(async (silent = false) => {
    const sourceMissing =
      (analyticsConfig.source_type === 'pipeline' && !analyticsConfig.pipeline_id) ||
      (analyticsConfig.source_type === 'file' && !(analyticsConfig.file_path || '').trim()) ||
      (analyticsConfig.source_type === 'mlops' && !analyticsConfig.mlops_workflow_id)
    if (sourceMissing) return
    setPythonFieldSchemaLoading(true)
    try {
      const response = await vizHttp.post('/api/nlp-chat/schema', {
        ...buildAnalyticsSourcePayload(),
        sample_size: 600,
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

      setPythonFieldColumns(columns)
      setPythonFieldMetrics(metrics)
      setPythonFieldDimensions(dimensions)

      setPythonVizConfig((prev) => ({
        ...prev,
        x_field: prev.x_field || dimensions[0] || columns[0] || '',
        y_field: prev.y_field || metrics[0] || columns[1] || columns[0] || '',
        color_field: prev.color_field || dimensions[1] || '',
      }))
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      if (!silent) {
        messageApi.error(typeof detail === 'string' ? detail : 'Failed to load source fields')
      }
      setPythonFieldColumns([])
      setPythonFieldMetrics([])
      setPythonFieldDimensions([])
    } finally {
      setPythonFieldSchemaLoading(false)
    }
  }, [analyticsConfig, buildAnalyticsSourcePayload, messageApi])

  useEffect(() => {
    if (!pythonVizOpen) return
    const sourceMissing =
      (analyticsConfig.source_type === 'pipeline' && !analyticsConfig.pipeline_id) ||
      (analyticsConfig.source_type === 'file' && !(analyticsConfig.file_path || '').trim()) ||
      (analyticsConfig.source_type === 'mlops' && !analyticsConfig.mlops_workflow_id)
    if (sourceMissing) {
      setPythonFieldColumns([])
      setPythonFieldMetrics([])
      setPythonFieldDimensions([])
      return
    }
    void handleLoadPythonFieldSchema(true)
  }, [
    pythonVizOpen,
    analyticsConfig.source_type,
    analyticsConfig.dataset,
    analyticsConfig.pipeline_id,
    analyticsConfig.pipeline_node_id,
    analyticsConfig.file_path,
    analyticsConfig.mlops_workflow_id,
    analyticsConfig.mlops_run_id,
    analyticsConfig.mlops_node_id,
    analyticsConfig.mlops_output_mode,
    analyticsConfig.mlops_prediction_start_date,
    analyticsConfig.mlops_prediction_end_date,
    handleLoadPythonFieldSchema,
  ])

  const handleGenerateAnalyticsDashboard = useCallback(async () => {
    if (!activeDashboard) return
    setAnalyticsGenerating(true)
    try {
      const payload: Record<string, unknown> = {
        ...buildAnalyticsSourcePayload(),
        nlp_prompt: analyticsConfig.nlp_prompt,
        forecast_horizon: analyticsConfig.forecast_horizon,
      }

      const response = await vizHttp.post('/api/analytics/auto-dashboard', payload)
      const generatedWidgets = Array.isArray(response.data?.widgets) ? response.data.widgets : []
      const generatedLayout = Array.isArray(response.data?.layout) ? response.data.layout : []
      const nlpEngine = (response.data?.nlp_engine && typeof response.data.nlp_engine === 'object')
        ? response.data.nlp_engine as Record<string, unknown>
        : null
      const nlpFallbackReason = nlpEngine && typeof nlpEngine.fallback_reason === 'string'
        ? nlpEngine.fallback_reason
        : ''
      if (generatedWidgets.length === 0 || generatedLayout.length === 0) {
        messageApi.warning('Analytics service did not return dashboard widgets')
        return
      }

      const summary = typeof response.data?.summary === 'string' ? response.data.summary : ''
      const nextDescription = summary
        ? [activeDashboard.description || '', summary].filter(Boolean).join(' · ')
        : (activeDashboard.description || '')
      const generatedName = `${activeDashboard.name} · Analytics`

      const createResponse = await vizHttp.post('/api/dashboards', {
        name: generatedName,
        description: nextDescription,
        theme: activeDashboard.theme,
        tags: activeDashboard.tags,
      })
      const newDashboardId = String(createResponse.data?.id || '').trim()
      if (!newDashboardId) {
        throw new Error('Failed to create analytics dashboard')
      }

      await vizHttp.put(`/api/dashboards/${newDashboardId}`, {
        name: generatedName,
        description: nextDescription,
        widgets: generatedWidgets,
        layout: generatedLayout,
        theme: activeDashboard.theme,
        global_filters: activeDashboard.global_filters,
        tags: activeDashboard.tags,
        is_public: false,
      })

      setSelectedWidgetId(null)
      setAnalyticsOpen(false)
      navigate(`/dashboards/${newDashboardId}/edit`)
      messageApi.success(`Analytics dashboard created with ${generatedWidgets.length} widgets`)
      if (nlpFallbackReason) {
        messageApi.warning(`NLP LLM fallback active: ${nlpFallbackReason}. Set valid OPENAI_API_KEY in backend/.env`)
      }
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      messageApi.error(typeof detail === 'string' ? detail : 'Failed to generate analytics dashboard')
    } finally {
      setAnalyticsGenerating(false)
    }
  }, [activeDashboard, analyticsConfig, buildAnalyticsSourcePayload, messageApi, navigate])

  const handleRunPythonVisualization = useCallback(async () => {
    setPythonVizLoading(true)
    setPythonVizError(null)
    try {
      const payload: Record<string, unknown> = {
        ...buildAnalyticsSourcePayload(),
        chart_type: pythonVizConfig.chart_type,
        x_field: (pythonVizConfig.x_field || '').trim() || undefined,
        y_field: (pythonVizConfig.y_field || '').trim() || undefined,
        color_field: (pythonVizConfig.color_field || '').trim() || undefined,
        group_by: (pythonVizConfig.group_by || '').trim() || undefined,
        title: (pythonVizConfig.title || '').trim() || undefined,
        limit: pythonVizConfig.limit,
      }
      const response = await vizHttp.post('/api/analytics/python-visualization', payload)
      setPythonVizResult(response.data as PythonAnalyticsResult)
      messageApi.success('Python analytics visualization generated')
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      const text = typeof detail === 'string' ? detail : 'Failed to generate python analytics visualization'
      setPythonVizError(text)
      messageApi.error(text)
    } finally {
      setPythonVizLoading(false)
    }
  }, [buildAnalyticsSourcePayload, pythonVizConfig, messageApi])

  const handleAddPythonWidgetToDashboard = useCallback(() => {
    if (!activeDashboard) return
    if (!pythonVizResult) {
      messageApi.warning('Run Python Analytics first, then add the chart to dashboard')
      return
    }

    const columns = Array.isArray(pythonVizResult.columns) ? pythonVizResult.columns.map((col) => String(col)) : []
    const columnProfile = pythonVizResult.column_profile || {}
    const numericColumns = Array.isArray(columnProfile.numeric) ? columnProfile.numeric.map((col) => String(col)) : []
    const datetimeColumns = Array.isArray(columnProfile.datetime) ? columnProfile.datetime.map((col) => String(col)) : []
    const categoricalColumns = Array.isArray(columnProfile.categorical) ? columnProfile.categorical.map((col) => String(col)) : []

    const pickField = (...candidates: Array<string | null | undefined>): string => {
      for (const candidate of candidates) {
        const normalized = normalizeFieldName(candidate)
        if (!normalized) continue
        if (columns.length === 0 || columns.includes(normalized)) return normalized
      }
      return ''
    }

    const firstExisting = (candidates: string[]): string => {
      for (const candidate of candidates) {
        const normalized = normalizeFieldName(candidate)
        if (!normalized) continue
        if (columns.includes(normalized)) return normalized
      }
      return ''
    }

    const defaultDimension = firstExisting([...datetimeColumns, ...categoricalColumns, ...columns])
    const defaultMeasure = firstExisting([...numericColumns, ...columns])
    const preferredX = pickField(
      pythonVizResult.x_field,
      pythonVizConfig.x_field,
      defaultDimension,
      columns[0] || '',
    )
    const preferredY = pickField(
      pythonVizResult.y_field,
      pythonVizConfig.y_field,
      defaultMeasure,
      columns[1] || columns[0] || '',
    )
    const preferredColor = pickField(
      pythonVizResult.color_field,
      pythonVizConfig.color_field,
      firstExisting(categoricalColumns.filter((col) => col !== preferredX)),
    )
    const preferredGroupBy = pickField(
      pythonVizResult.group_by,
      pythonVizConfig.group_by,
      firstExisting(categoricalColumns.filter((col) => col !== preferredX)),
    )

    const rawChartType = String(pythonVizResult.chart_type || '').trim().toLowerCase()
    const mappedType = mapPythonChartToWidgetType(rawChartType)
    const widgetType = pythonWidgetTargetType !== 'auto' ? pythonWidgetTargetType : mappedType
    const sourceLabel = analyticsConfig.source_type === 'pipeline'
      ? 'ETL'
      : (analyticsConfig.source_type === 'mlops' ? 'MLOps' : 'Source')

    const widgetId = (
      typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
        ? crypto.randomUUID()
        : `py-widget-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
    )

    let xField = pickField(preferredX, defaultDimension, columns[0] || '')
    let yField = pickField(preferredY, defaultMeasure, columns[1] || columns[0] || '')
    let groupByField = pickField(preferredGroupBy, preferredColor, defaultDimension)
    let aggregation: string | undefined = 'sum'

    if (widgetType === 'scatter') {
      xField = pickField(preferredX, numericColumns[0], columns[0] || '')
      yField = pickField(
        preferredY,
        numericColumns.find((col) => col !== xField) || numericColumns[0] || '',
        columns.find((col) => col !== xField) || columns[0] || '',
      )
      aggregation = undefined
    } else if (widgetType === 'line' || widgetType === 'area') {
      xField = pickField(preferredX, defaultDimension, columns[0] || '')
      yField = pickField(preferredY, defaultMeasure, columns[1] || columns[0] || '')
      aggregation = undefined
    } else if (widgetType === 'heatmap') {
      xField = pickField(preferredX, defaultDimension, columns[0] || '')
      groupByField = pickField(
        preferredGroupBy,
        categoricalColumns.find((col) => col !== xField) || '',
        datetimeColumns.find((col) => col !== xField) || '',
      )
      yField = pickField(preferredY, defaultMeasure, numericColumns[0] || columns[0] || '')
      aggregation = 'sum'
    } else if (widgetType === 'bar' || widgetType === 'bar_horizontal') {
      xField = pickField(preferredX, defaultDimension, columns[0] || '')
      if (rawChartType === 'histogram') {
        yField = pickField(preferredY, xField, defaultMeasure, columns[0] || '')
        aggregation = 'count'
      } else if (rawChartType === 'box') {
        yField = pickField(preferredY, defaultMeasure, columns[0] || '')
        aggregation = 'avg'
      } else {
        yField = pickField(preferredY, defaultMeasure, columns[0] || '')
        aggregation = 'sum'
      }
    } else if (widgetType === 'pie' || widgetType === 'donut' || widgetType === 'treemap' || widgetType === 'funnel' || widgetType === 'waterfall') {
      xField = pickField(preferredX, defaultDimension, columns[0] || '')
      yField = pickField(preferredY, defaultMeasure, columns[0] || '')
      aggregation = 'sum'
    } else if (widgetType === 'radar') {
      xField = pickField(preferredX, defaultDimension, columns[0] || '')
      groupByField = pickField(preferredGroupBy, preferredColor, categoricalColumns[0], xField)
      yField = pickField(preferredY, defaultMeasure, columns[0] || '')
      aggregation = 'avg'
    } else if (widgetType === 'kpi' || widgetType === 'gauge') {
      yField = pickField(preferredY, defaultMeasure, columns[0] || '')
      aggregation = undefined
    } else if (widgetType === 'table' || widgetType === 'text') {
      aggregation = undefined
    }

    const baseLimit = Math.max(25, Math.min(Number(pythonVizConfig.limit || 5000), 100000))
    let chartConfig: Widget['chart_config']

    if (widgetType === 'table') {
      chartConfig = { limit: Math.min(baseLimit, 5000) }
    } else if (widgetType === 'text') {
      const lines = [
        `## ${pythonVizResult.title || 'Python Analytics Insight'}`,
        `- Source: ${sourceLabel}`,
        `- Chart generated: ${pythonVizResult.chart_type}`,
        `- Rows analyzed: ${Number(pythonVizResult.row_count || 0).toLocaleString()}`,
        `- Suggested action: review top trends and anomalies before drill-down.`,
      ]
      chartConfig = { text_content: lines.join('\n') }
    } else if (widgetType === 'kpi' || widgetType === 'gauge') {
      chartConfig = {
        kpi_field: yField || undefined,
        y_field: yField || undefined,
        kpi_label: yField ? `${yField} (${sourceLabel})` : `${sourceLabel} KPI`,
        limit: widgetType === 'gauge' ? Math.min(baseLimit, 1000000) : undefined,
      }
    } else if (widgetType === 'radar') {
      chartConfig = {
        x_field: xField || undefined,
        y_field: yField || undefined,
        group_by: groupByField || undefined,
        aggregation,
        limit: Math.min(baseLimit, 1000),
      }
    } else if (widgetType === 'heatmap') {
      chartConfig = {
        x_field: xField || undefined,
        y_field: yField || undefined,
        group_by: groupByField || undefined,
        aggregation,
        limit: Math.min(baseLimit, 1000),
      }
    } else {
      chartConfig = {
        x_field: xField || undefined,
        y_field: yField || undefined,
        color_field: preferredColor || undefined,
        group_by: (widgetType === 'treemap' || widgetType === 'funnel' || widgetType === 'waterfall')
          ? undefined
          : (groupByField || undefined),
        aggregation,
        limit: widgetType === 'scatter' ? Math.min(baseLimit, 5000) : Math.min(baseLimit, 1000),
      }
    }

    const widget: Widget = {
      id: widgetId,
      type: widgetType,
      title: pythonVizResult.title || `Python ${widgetType.toUpperCase()} Chart`,
      subtitle: `${sourceLabel} • Python Analytics Studio`,
      data_source: {
        type: analyticsConfig.source_type,
        dataset: analyticsConfig.dataset,
        pipeline_id: analyticsConfig.pipeline_id,
        pipeline_node_id: analyticsConfig.pipeline_node_id,
        file_path: analyticsConfig.file_path,
        mlops_workflow_id: analyticsConfig.mlops_workflow_id,
        mlops_run_id: analyticsConfig.mlops_run_id,
        mlops_node_id: analyticsConfig.mlops_node_id,
        mlops_output_mode: analyticsConfig.mlops_output_mode,
        mlops_prediction_start_date: analyticsConfig.mlops_prediction_start_date,
        mlops_prediction_end_date: analyticsConfig.mlops_prediction_end_date,
        sql: analyticsConfig.sql,
      },
      chart_config: chartConfig,
      style: {
        show_legend: true,
        show_grid: true,
        show_labels: false,
      },
    }

    addWidget(widget)
    setSelectedWidgetId(widgetId)

    const mappedNote = pythonWidgetTargetType === 'auto'
      ? (rawChartType !== widgetType ? ` (mapped from ${rawChartType})` : '')
      : ` (target: ${widgetType}, python: ${rawChartType})`
    messageApi.success(`Python visualization added as ${widgetType} widget${mappedNote}`)
  }, [activeDashboard, pythonVizResult, messageApi, analyticsConfig, pythonVizConfig, addWidget, pythonWidgetTargetType])

  const handleWidgetUpdate = useCallback(
    (updates: Partial<Widget>) => {
      if (!selectedWidgetId) return
      updateWidget(selectedWidgetId, updates)
    },
    [selectedWidgetId, updateWidget]
  )

  const handleWidgetRemove = useCallback(() => {
    if (!selectedWidgetId) return
    removeWidget(selectedWidgetId)
    setSelectedWidgetId(null)
  }, [selectedWidgetId, removeWidget])

  const handleSizeChange = useCallback((newW: number, newH: number) => {
    if (!selectedWidgetId || !activeDashboard) return
    const safeW = Math.max(2, Math.min(24, Math.round(newW)))
    const safeH = Math.max(2, Math.min(40, Math.round(newH)))
    const existing = activeDashboard.layout.find(l => l.i === selectedWidgetId)
    const updated = existing
      ? activeDashboard.layout.map(l =>
          l.i === selectedWidgetId ? { ...l, w: safeW, h: safeH } : l
        )
      : [
          ...activeDashboard.layout,
          {
            i: selectedWidgetId,
            x: 0,
            y: activeDashboard.layout.reduce((max, l) => Math.max(max, l.y + l.h), 0),
            w: safeW,
            h: safeH,
            minW: 4,
            minH: 3,
          }
        ]
    latestLayoutRef.current = updated
    updateLayout(updated)
  }, [selectedWidgetId, activeDashboard, updateLayout])

  const handleRefreshData = useCallback(() => {
    if (!selectedWidget) return
    void fetchWidgetData(selectedWidget)
    messageApi.info('Refreshing widget data…')
  }, [selectedWidget, fetchWidgetData, messageApi])

  const handleCanvasMouseMove = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    const now = Date.now()
    if (now - lastRulerUpdateAtRef.current < 50) return
    lastRulerUpdateAtRef.current = now

    const rect = e.currentTarget.getBoundingClientRect()
    const nextX = Math.round(e.clientX - rect.left)
    const nextY = Math.round(e.clientY - rect.top)
    setRulerGuide((prev) => {
      if (prev.visible && Math.abs(prev.x - nextX) < 2 && Math.abs(prev.y - nextY) < 2) {
        return prev
      }
      return {
        x: nextX,
        y: nextY,
        visible: true,
      }
    })
  }, [])

  const handleCanvasMouseLeave = useCallback(() => {
    setRulerGuide((prev) => (prev.visible ? { ...prev, visible: false } : prev))
  }, [])

  // These must be before any early return to satisfy rules-of-hooks
  const layout: LayoutItem[] = activeDashboard?.layout ?? []
  const widgets: Widget[] = activeDashboard?.widgets ?? []
  const isLightDashboardTheme = (activeDashboard?.theme ?? 'dark') === 'light'
  const rulerLineColor = isLightDashboardTheme ? '#dbe4f0' : '#252535'
  const guideLabelBg = isLightDashboardTheme ? '#ffffffd9' : '#0f172acc'
  const guideLabelBorder = isLightDashboardTheme ? '#cbd5e1' : '#6366f155'
  const guideLabelText = isLightDashboardTheme ? '#334155' : '#cbd5e1'
  const analyticsSourceMissing =
    (analyticsConfig.source_type === 'pipeline' && !analyticsConfig.pipeline_id) ||
    (analyticsConfig.source_type === 'file' && !(analyticsConfig.file_path || '').trim()) ||
    (analyticsConfig.source_type === 'mlops' && !analyticsConfig.mlops_workflow_id)
  const pythonVizSourceMissing = analyticsSourceMissing
  const pythonPreviewColumns = Array.isArray(pythonVizResult?.columns)
    ? pythonVizResult.columns.slice(0, 16)
    : []
  const pythonAllColumns = useMemo(() => {
    const base = Array.isArray(pythonFieldColumns) ? pythonFieldColumns : []
    const fromResult = Array.isArray(pythonVizResult?.columns) ? pythonVizResult.columns : []
    return Array.from(new Set([...base, ...fromResult].map((col) => String(col).trim()).filter(Boolean)))
  }, [pythonFieldColumns, pythonVizResult?.columns])
  const pythonMetricColumns = useMemo(() => {
    const base = Array.isArray(pythonFieldMetrics) ? pythonFieldMetrics : []
    const fromProfile = Array.isArray(pythonVizResult?.column_profile?.numeric)
      ? pythonVizResult.column_profile.numeric
      : []
    const merged = [...base, ...fromProfile]
      .map((col) => String(col).trim())
      .filter(Boolean)
      .filter((col, idx, arr) => arr.indexOf(col) === idx)
    if (merged.length > 0) return merged
    return pythonAllColumns
  }, [pythonFieldMetrics, pythonVizResult?.column_profile?.numeric, pythonAllColumns])
  const pythonDimensionColumns = useMemo(() => {
    const base = Array.isArray(pythonFieldDimensions) ? pythonFieldDimensions : []
    const fromDate = Array.isArray(pythonVizResult?.column_profile?.datetime)
      ? pythonVizResult.column_profile.datetime
      : []
    const fromCategory = Array.isArray(pythonVizResult?.column_profile?.categorical)
      ? pythonVizResult.column_profile.categorical
      : []
    const merged = [...base, ...fromDate, ...fromCategory]
      .map((col) => String(col).trim())
      .filter(Boolean)
      .filter((col, idx, arr) => arr.indexOf(col) === idx)
    if (merged.length > 0) return merged
    return pythonAllColumns
  }, [pythonFieldDimensions, pythonVizResult?.column_profile?.datetime, pythonVizResult?.column_profile?.categorical, pythonAllColumns])

  useEffect(() => {
    latestLayoutRef.current = layout
  }, [layout])

  // Stable RGL layout — only recompute when layout array actually changes
  const rglLayouts: RGLLayouts = useMemo(() => {
    const base: RGLLayoutItem[] = layout.map(toRGLItem)
    const cloneBase = () => base.map(item => ({ ...item }))
    return {
      lg:  cloneBase(),
      md:  cloneBase(),
      sm:  cloneBase().map(l => ({ ...l, w: Math.min(l.w, 20), x: Math.min(l.x, 20) })),
      xs:  cloneBase().map(l => ({ ...l, w: Math.min(l.w, 8),  x: 0 })),
      xxs: cloneBase().map(l => ({ ...l, w: Math.min(l.w, 4),  x: 0 })),
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(layout)])

  // ─── loading screen ──────────────────────────────────────────────────────

  if (loading && !activeDashboard) {
    return (
      <div style={{ ...S.root, alignItems: 'center', justifyContent: 'center' }}>
        <Spin size="large" />
        <Text style={{ color: 'var(--app-text-dim)', marginTop: 16, display: 'block' }}>Loading dashboard…</Text>
      </div>
    )
  }

  // ─── render ──────────────────────────────────────────────────────────────

  return (
    <div style={S.root}>
      {contextHolder}

      {/* Resize handle global CSS */}
      <style>{`
        .react-resizable-handle {
          position: absolute !important;
          width: 16px !important;
          height: 16px !important;
          bottom: 3px !important;
          right: 3px !important;
          cursor: se-resize !important;
          opacity: 0;
          transition: opacity 0.2s;
          background: transparent !important;
          border-right: 2px solid #6366f1 !important;
          border-bottom: 2px solid #6366f1 !important;
          border-radius: 0 0 3px 0 !important;
          z-index: 10;
        }
        .react-resizable-handle::after { display: none !important; }
        .react-grid-item:hover .react-resizable-handle,
        .react-grid-item.resizing .react-resizable-handle { opacity: 1; }
        .react-grid-item.react-grid-placeholder {
          background: #6366f120 !important;
          border: 1px dashed #6366f160 !important;
          border-radius: 8px !important;
          opacity: 1 !important;
        }
      `}</style>

      {/* ── Toolbar ─────────────────────────────────────────────────────── */}
      <div style={S.toolbar}>

        {/* Back button */}
        <Tooltip title="Back to Dashboards">
          <Button
            type="text"
            icon={<ArrowLeftOutlined />}
            size="small"
            onClick={() => navigate('/dashboards')}
            style={{ color: 'var(--app-text-muted)', flexShrink: 0 }}
          />
        </Tooltip>

        {/* Dashboard name — inline edit */}
        {editingName ? (
          <Input
            ref={nameInputRef}
            value={nameValue}
            size="small"
            onChange={e => setNameValue(e.target.value)}
            onBlur={handleNameCommit}
            onPressEnter={handleNameCommit}
            style={{
              ...S.inputDark,
              width: 220,
              fontSize: 14,
              fontWeight: 600,
            }}
          />
        ) : (
          <div
            style={{ display: 'flex', alignItems: 'center', gap: 4, cursor: 'pointer', minWidth: 0 }}
            onClick={() => setEditingName(true)}
          >
            <Text
              style={{
                color: 'var(--app-text)',
                fontSize: 14,
                fontWeight: 600,
                maxWidth: 240,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
            >
              {activeDashboard?.name || 'Untitled Dashboard'}
            </Text>
            <EditOutlined style={{ color: 'var(--app-text-faint)', fontSize: 12, flexShrink: 0 }} />
          </div>
        )}

        {/* Dirty indicator */}
        {isDirty && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 5, flexShrink: 0 }}>
            <div
              style={{
                width: 7,
                height: 7,
                borderRadius: '50%',
                background: '#f97316',
                boxShadow: '0 0 6px #f97316aa',
              }}
            />
            <Text style={{ color: '#f97316', fontSize: 12 }}>Unsaved</Text>
          </div>
        )}

        {/* Spacer */}
        <div style={{ flex: 1 }} />

        {/* Add Filter */}
        <Tooltip title="Add a global filter that applies across all widgets on this dashboard">
          <Button
            size="small"
            icon={<FilterOutlined />}
            style={{ background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)', color: 'var(--app-text-muted)' }}
          >
            Add Filter
          </Button>
        </Tooltip>

        <Tooltip title="Auto-build a complete descriptive, diagnostic, forecasting, and prescriptive dashboard">
          <Button
            size="small"
            icon={<SettingOutlined />}
            onClick={openAnalyticsBuilder}
            style={{ background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)', color: 'var(--app-text-muted)' }}
          >
            Analytics Studio
          </Button>
        </Tooltip>

        <Tooltip title="Run pure Python (Pandas + Plotly) analytics on selected datasource in full-screen overlay">
          <Button
            size="small"
            icon={<CodeOutlined />}
            onClick={openPythonAnalyticsOverlay}
            style={{ background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)', color: 'var(--app-text-muted)' }}
          >
            Python Analytics
          </Button>
        </Tooltip>

        {/* Theme toggle */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexShrink: 0 }}>
          <BulbOutlined style={{ color: 'var(--app-text-dim)', fontSize: 13 }} />
          <Tooltip title={`Switch to ${activeDashboard?.theme === 'light' ? 'dark' : 'light'} theme`}>
            <Switch
              size="small"
              checked={activeDashboard?.theme === 'light'}
              onChange={handleThemeToggle}
              checkedChildren="☀"
              unCheckedChildren="🌙"
            />
          </Tooltip>
        </div>

        {/* Preview */}
        <Tooltip title="Open dashboard preview in a new tab">
          <Button
            size="small"
            icon={<EyeOutlined />}
            style={{ background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)', color: 'var(--app-text-muted)' }}
            onClick={() => {
              if (activeDashboard?.id) {
                window.open(`/dashboards/${activeDashboard.id}`, '_blank')
              }
            }}
          >
            Preview
          </Button>
        </Tooltip>

        {/* Save */}
        <Button
          size="small"
          type="primary"
          icon={<SaveOutlined />}
          loading={saving}
          onClick={triggerSave}
          style={{ background: '#6366f1', borderColor: '#6366f1', flexShrink: 0 }}
        >
          Save
        </Button>
      </div>

      {/* ── Body ────────────────────────────────────────────────────────── */}
      <div style={S.body}>

        {/* ── Left Sidebar ──────────────────────────────────────────────── */}
        <div style={S.sidebar}>
          <WidgetPalette />
        </div>

        {/* ── Canvas with Rulers ─────────────────────────────────────────── */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden', position: 'relative' }}>

          {/* Horizontal ruler row */}
          <div style={{ display: 'flex', flexShrink: 0, zIndex: 2 }}>
            {/* Corner */}
            <div style={{ width: 20, height: 18, background: 'var(--app-panel-2)', borderRight: `1px solid ${rulerLineColor}`, borderBottom: `1px solid ${rulerLineColor}`, flexShrink: 0 }} />
            {/* H ruler */}
            <div style={{
              flex: 1, height: 18, background: 'var(--app-panel-2)', borderBottom: `1px solid ${rulerLineColor}`,
              backgroundImage: `repeating-linear-gradient(90deg, ${rulerLineColor} 0, ${rulerLineColor} 1px, transparent 1px, transparent 24px)`,
              position: 'relative', overflow: 'hidden',
            }}>
              {[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20].map(i => (
                <span key={i} style={{ position: 'absolute', left: i * 96 + 2, top: 4, color: 'var(--app-text-faint)', fontSize: 9, userSelect: 'none', lineHeight: 1 }}>
                  {i * 4}
                </span>
              ))}
            </div>
          </div>

          {/* Canvas row with vertical ruler */}
          <div style={{ flex: 1, display: 'flex', overflow: 'hidden' }}>
            {/* V ruler */}
            <div style={{
              width: 20, flexShrink: 0, background: 'var(--app-panel-2)', borderRight: `1px solid ${rulerLineColor}`,
              backgroundImage: `repeating-linear-gradient(180deg, ${rulerLineColor} 0, ${rulerLineColor} 1px, transparent 1px, transparent 24px)`,
              position: 'relative', overflow: 'hidden',
            }}>
              {[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20].map(i => (
                <span key={i} style={{
                  position: 'absolute', top: i * 96 + 3, left: 1,
                  color: 'var(--app-text-faint)', fontSize: 9, userSelect: 'none', lineHeight: 1,
                  writingMode: 'vertical-rl', transform: 'rotate(180deg)',
                }}>
                  {i * 4}
                </span>
              ))}
            </div>

        <div
          style={S.canvas}
          onClick={() => setSelectedWidgetId(null)}
          onMouseMove={selectedWidget ? handleCanvasMouseMove : undefined}
          onMouseLeave={selectedWidget ? handleCanvasMouseLeave : undefined}
        >
          {rulerGuide.visible && (
            <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none', zIndex: 4 }}>
              <div style={{
                position: 'absolute',
                left: 0,
                right: 0,
                top: rulerGuide.y,
                height: 1,
                background: '#6366f155',
              }} />
              <div style={{
                position: 'absolute',
                top: 0,
                bottom: 0,
                left: rulerGuide.x,
                width: 1,
                background: '#6366f155',
              }} />
              <div style={{
                position: 'absolute',
                left: Math.max(4, rulerGuide.x + 8),
                top: Math.max(4, rulerGuide.y + 8),
                background: guideLabelBg,
                border: `1px solid ${guideLabelBorder}`,
                borderRadius: 4,
                color: guideLabelText,
                fontSize: 10,
                lineHeight: 1,
                padding: '4px 6px',
                letterSpacing: '0.02em',
              }}>
                X {rulerGuide.x} • Y {rulerGuide.y}
              </div>
            </div>
          )}

          {/* Empty state */}
          {widgets.length === 0 && (
            <div
              style={{
                position: 'absolute',
                inset: 0,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                pointerEvents: 'none',
              }}
            >
              <div
                style={{
                  border: '2px dashed var(--app-border)',
                  borderRadius: 16,
                  padding: '48px 64px',
                  textAlign: 'center',
                  maxWidth: 420,
                }}
              >
                <div style={{ fontSize: 48, marginBottom: 16 }}>📊</div>
                <Text
                  style={{
                    color: 'var(--app-text-faint)',
                    fontSize: 17,
                    display: 'block',
                    marginBottom: 8,
                    fontWeight: 600,
                  }}
                >
                  Your canvas is empty
                </Text>
                <Text style={{ color: '#1e293b', fontSize: 13, lineHeight: 1.6 }}>
                  Drag a chart type from the left panel to get started,
                  or click any widget to add it instantly.
                </Text>
              </div>
            </div>
          )}

          <div ref={gridContainerRef as React.RefObject<HTMLDivElement>} style={{ padding: 12, minHeight: '100%' }}>
            {gridMounted && gridWidth > 0 && (
              <RGL
                width={gridWidth}
                className="layout"
                layouts={rglLayouts}
                cols={COLS}
                rowHeight={ROW_HEIGHT}
                isDraggable
                isResizable
                margin={MARGIN}
                draggableHandle=".drag-handle"
                useCSSTransforms
                measureBeforeMount={false}
                onDragStop={handleDragOrResizeStop}
                onResizeStop={handleDragOrResizeStop}
                style={{ minHeight: 400 }}
              >
                {widgets.map(w => {
                  const li = layout.find(l => l.i === w.id)
                  const h = li ? li.h : 9
                  const chartH = Math.max(computeBodyHeight(h), 60)
                  const isSelected = selectedWidgetId === w.id
                  const wData = widgetData[w.id]

                  return (
                    <div
                      key={w.id}
                      onClick={e => {
                        e.stopPropagation()
                        setSelectedWidgetId(w.id)
                      }}
                    >
                      <div style={S.widgetCard(isSelected)}>

                      {/* Widget header bar */}
                      <div style={S.widgetHeader}>

                        {/* Drag handle */}
                        <div
                          className="drag-handle"
                          style={{
                            cursor: 'grab',
                            color: 'var(--app-border-strong)',
                            display: 'flex',
                            alignItems: 'center',
                            padding: '0 2px',
                            flexShrink: 0,
                          }}
                        >
                          <HolderOutlined style={{ fontSize: 12 }} />
                        </div>

                        {/* Title */}
                        <Text
                          style={{
                            color: 'var(--app-text-muted)',
                            fontSize: 12,
                            fontWeight: 500,
                            flex: 1,
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                          }}
                        >
                          {w.title}
                        </Text>

                        {/* Config icon */}
                        <Tooltip title="Configure widget">
                          <Button
                            type="text"
                            size="small"
                            icon={<EditOutlined style={{ fontSize: 11 }} />}
                            onClick={e => {
                              e.stopPropagation()
                              setSelectedWidgetId(w.id)
                            }}
                            style={{
                              color: 'var(--app-text-faint)',
                              width: 22,
                              height: 22,
                              minWidth: 0,
                              padding: 0,
                              flexShrink: 0,
                            }}
                          />
                        </Tooltip>

                        {/* Delete icon */}
                        <Tooltip title="Remove widget">
                          <Button
                            type="text"
                            size="small"
                            danger
                            icon={<DeleteOutlined style={{ fontSize: 11 }} />}
                            onClick={e => {
                              e.stopPropagation()
                              removeWidget(w.id)
                              if (selectedWidgetId === w.id) setSelectedWidgetId(null)
                            }}
                            style={{
                              width: 22,
                              height: 22,
                              minWidth: 0,
                              padding: 0,
                              flexShrink: 0,
                            }}
                          />
                        </Tooltip>
                      </div>

                      {/* Widget body */}
                      <div
                        style={{
                          flex: 1,
                          overflow: 'hidden',
                          background: w.style?.bg_color || 'transparent',
                        }}
                      >
                        <ChartWidget
                          widget={w}
                          data={wData?.data ?? []}
                          columns={wData?.columns ?? []}
                          height={chartH}
                          theme={activeDashboard?.theme ?? 'dark'}
                        />
                      </div>
                      </div>
                    </div>
                  )
                })}
              </RGL>
            )}
          </div>{/* end RGL padding wrapper */}
        </div>{/* end S.canvas */}
        </div>{/* end canvas row (v-ruler + canvas) */}
      </div>{/* end canvas+rulers wrapper */}

        {/* ── Right Config Panel ─────────────────────────────────────────── */}
        <div style={S.rightPanel(selectedWidget !== null)}>
          {selectedWidget !== null && (
            <ConfigPanel
              widget={selectedWidget}
              layoutItem={layout.find(l => l.i === selectedWidget.id)}
              columns={widgetData[selectedWidget.id]?.columns ?? []}
              onUpdate={handleWidgetUpdate}
              onSizeChange={handleSizeChange}
              onRemove={handleWidgetRemove}
              onClose={() => setSelectedWidgetId(null)}
              onRefresh={handleRefreshData}
            />
          )}
        </div>

      </div>

      {/* Persistent save action so save is always accessible even on smaller screens */}
      <div
        style={{
          position: 'fixed',
          right: selectedWidget ? 340 : 20,
          bottom: 20,
          zIndex: 1200,
          pointerEvents: 'none',
        }}
      >
        <Tooltip title={isDirty ? 'Save dashboard changes' : 'Dashboard is already saved'}>
          <Button
            type="primary"
            icon={<SaveOutlined />}
            loading={saving}
            disabled={!isDirty && !saving}
            onClick={triggerSave}
            style={{
              background: '#6366f1',
              borderColor: '#6366f1',
              boxShadow: '0 10px 28px rgba(99,102,241,0.35)',
              pointerEvents: 'auto',
            }}
          >
            {isDirty ? 'Save Changes' : 'Saved'}
          </Button>
        </Tooltip>
      </div>

      <Modal
        title="Python Analytics Studio (Pandas + Plotly)"
        open={pythonVizOpen}
        onCancel={() => setPythonVizOpen(false)}
        footer={null}
        width="96vw"
        destroyOnClose={false}
        styles={{
          content: {
            height: '96vh',
            padding: 0,
            overflow: 'hidden',
            background: 'var(--app-panel-bg)',
          },
          body: {
            height: '100%',
            padding: 0,
            overflow: 'hidden',
          },
        }}
      >
        <div style={{ display: 'flex', height: '100%' }}>
          <div style={{ width: 360, minWidth: 360, borderRight: '1px solid var(--app-border)', padding: 16, overflowY: 'auto' }}>
            <Text style={{ color: 'var(--app-text)', fontSize: 14, fontWeight: 600, display: 'block', marginBottom: 12 }}>
              Data Source
            </Text>

            <div style={{ marginBottom: 12 }}>
              <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Source Type</Text>
              <Select
                value={analyticsConfig.source_type}
                onChange={(value) => handleAnalyticsSourceTypeChange(value as AnalyticsSourceType)}
                options={[
                  { label: 'Sample Dataset', value: 'sample' },
                  { label: 'ETL Pipeline Output', value: 'pipeline' },
                  { label: 'MLOps Model Output', value: 'mlops' },
                  { label: 'File Path', value: 'file' },
                ]}
                style={{ width: '100%' }}
              />
            </div>

            {analyticsConfig.source_type === 'sample' && (
              <div style={{ marginBottom: 12 }}>
                <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Sample Dataset</Text>
                <Select
                  value={analyticsConfig.dataset || 'sales'}
                  onChange={(value) => updateAnalyticsConfig({ dataset: value })}
                  options={SAMPLE_DATASETS.map((d) => ({ label: `${d.icon} ${d.label}`, value: d.key }))}
                  style={{ width: '100%' }}
                />
              </div>
            )}

            {analyticsConfig.source_type === 'pipeline' && (
              <>
                <div style={{ marginBottom: 12 }}>
                  <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Pipeline</Text>
                  <Select
                    value={analyticsConfig.pipeline_id || undefined}
                    onChange={(value) => updateAnalyticsConfig({ pipeline_id: value, pipeline_node_id: undefined })}
                    placeholder="Select ETL pipeline"
                    options={analyticsPipelineOptions}
                    loading={analyticsPipelinesLoading}
                    notFoundContent={analyticsPipelinesLoading ? 'Loading pipelines...' : 'No pipelines found'}
                    allowClear
                    showSearch
                    optionFilterProp="label"
                    style={{ width: '100%' }}
                  />
                </div>
                <div style={{ marginBottom: 12 }}>
                  <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Pipeline Node (optional)</Text>
                  <Select
                    value={analyticsConfig.pipeline_node_id || undefined}
                    onChange={(value) => updateAnalyticsConfig({ pipeline_node_id: value })}
                    placeholder="Auto-detect latest tabular node"
                    options={analyticsPipelineNodes}
                    loading={analyticsPipelineNodesLoading}
                    notFoundContent={analyticsPipelineNodesLoading ? 'Loading nodes...' : 'No nodes found in this pipeline'}
                    disabled={!analyticsConfig.pipeline_id}
                    allowClear
                    showSearch
                    optionFilterProp="label"
                    style={{ width: '100%' }}
                  />
                </div>
              </>
            )}

            {analyticsConfig.source_type === 'mlops' && (
              <>
                <div style={{ marginBottom: 12 }}>
                  <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>MLOps Workflow</Text>
                  <Select
                    value={analyticsConfig.mlops_workflow_id || undefined}
                    onChange={(value) =>
                      updateAnalyticsConfig({
                        mlops_workflow_id: value,
                        mlops_run_id: undefined,
                        mlops_node_id: undefined,
                      })
                    }
                    placeholder="Select workflow/model"
                    options={analyticsMLOpsWorkflows}
                    loading={analyticsMLOpsWorkflowsLoading}
                    notFoundContent={analyticsMLOpsWorkflowsLoading ? 'Loading workflows...' : 'No MLOps workflows found'}
                    allowClear
                    showSearch
                    optionFilterProp="label"
                    style={{ width: '100%' }}
                  />
                </div>
                <div style={{ marginBottom: 12 }}>
                  <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Run (optional)</Text>
                  <Select
                    value={analyticsConfig.mlops_run_id || undefined}
                    onChange={(value, option) => {
                      const selected = option as MLOpsRunOption | undefined
                      const runWorkflowId = String(selected?.workflowId || '').trim()
                      const currentWorkflowId = String(analyticsConfig.mlops_workflow_id || '').trim()
                      if (runWorkflowId && runWorkflowId !== currentWorkflowId) {
                        updateAnalyticsConfig({
                          mlops_workflow_id: runWorkflowId,
                          mlops_run_id: value,
                          mlops_node_id: undefined,
                        })
                        return
                      }
                      updateAnalyticsConfig({ mlops_run_id: value })
                    }}
                    placeholder="Latest successful run"
                    options={analyticsMLOpsRuns}
                    loading={analyticsMLOpsRunsLoading}
                    notFoundContent={analyticsMLOpsRunsLoading ? 'Loading runs...' : 'No runs found'}
                    disabled={!analyticsConfig.mlops_workflow_id}
                    allowClear
                    showSearch
                    optionFilterProp="label"
                    style={{ width: '100%' }}
                  />
                </div>
                <div style={{ marginBottom: 12 }}>
                  <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Node (optional)</Text>
                  <Select
                    value={analyticsConfig.mlops_node_id || undefined}
                    onChange={(value) => updateAnalyticsConfig({ mlops_node_id: value })}
                    placeholder="Auto-select prediction node"
                    options={analyticsMLOpsNodes}
                    loading={analyticsMLOpsNodesLoading}
                    notFoundContent={analyticsMLOpsNodesLoading ? 'Loading nodes...' : 'No nodes found in workflow'}
                    disabled={!analyticsConfig.mlops_workflow_id}
                    allowClear
                    showSearch
                    optionFilterProp="label"
                    style={{ width: '100%' }}
                  />
                </div>
                <div style={{ marginBottom: 12 }}>
                  <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Output Mode</Text>
                  <Select
                    value={analyticsConfig.mlops_output_mode || 'predictions'}
                    onChange={(value) => updateAnalyticsConfig({ mlops_output_mode: value as 'predictions' | 'metrics' | 'monitor' | 'evaluation' })}
                    options={[
                      { label: 'Predictions', value: 'predictions' },
                      { label: 'Metrics Summary', value: 'metrics' },
                      { label: 'Monitoring Runs', value: 'monitor' },
                      { label: 'Evaluation Output', value: 'evaluation' },
                    ]}
                    style={{ width: '100%' }}
                  />
                </div>
                <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
                  <div style={{ flex: 1 }}>
                    <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Prediction Start</Text>
                    <Input
                      type="date"
                      value={analyticsConfig.mlops_prediction_start_date || ''}
                      onChange={(e) => updateAnalyticsConfig({ mlops_prediction_start_date: e.target.value || undefined })}
                      style={S.inputDark}
                    />
                  </div>
                  <div style={{ flex: 1 }}>
                    <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Prediction End</Text>
                    <Input
                      type="date"
                      value={analyticsConfig.mlops_prediction_end_date || ''}
                      onChange={(e) => updateAnalyticsConfig({ mlops_prediction_end_date: e.target.value || undefined })}
                      style={S.inputDark}
                    />
                  </div>
                </div>
              </>
            )}

            {analyticsConfig.source_type === 'file' && (
              <div style={{ marginBottom: 12 }}>
                <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>File Path</Text>
                <Input
                  value={analyticsConfig.file_path || ''}
                  onChange={(e) => updateAnalyticsConfig({ file_path: e.target.value })}
                  placeholder="/absolute/path/to/file.csv"
                  style={S.inputDark}
                />
              </div>
            )}

            <Divider style={{ margin: '8px 0 12px 0' }} />

            <Text style={{ color: 'var(--app-text)', fontSize: 14, fontWeight: 600, display: 'block', marginBottom: 12 }}>
              Plot Config
            </Text>

            <div style={{ marginBottom: 12, display: 'flex', gap: 8 }}>
              <Button
                size="small"
                loading={pythonFieldSchemaLoading}
                onClick={() => void handleLoadPythonFieldSchema(false)}
                disabled={pythonVizSourceMissing}
              >
                Load Fields
              </Button>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11, display: 'flex', alignItems: 'center' }}>
                {pythonAllColumns.length > 0 ? `${pythonAllColumns.length} fields detected` : 'Select source then load fields'}
              </Text>
            </div>

            <div style={{ marginBottom: 12 }}>
              <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Chart Type</Text>
              <Select
                value={pythonVizConfig.chart_type}
                onChange={(value) => setPythonVizConfig((prev) => ({ ...prev, chart_type: value as PythonAnalyticsConfig['chart_type'] }))}
                options={[
                  { label: 'Auto', value: 'auto' },
                  { label: 'Line', value: 'line' },
                  { label: 'Bar', value: 'bar' },
                  { label: 'Scatter', value: 'scatter' },
                  { label: 'Histogram', value: 'histogram' },
                  { label: 'Box', value: 'box' },
                  { label: 'Heatmap', value: 'heatmap' },
                  { label: 'Table', value: 'table' },
                ]}
                style={{ width: '100%' }}
              />
            </div>

            <div style={{ marginBottom: 12 }}>
              <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Add As React Widget</Text>
              <Select
                value={pythonWidgetTargetType}
                onChange={(value) => setPythonWidgetTargetType(value as PythonWidgetTargetType)}
                options={[
                  { label: 'Auto map from Python chart', value: 'auto' },
                  { label: 'KPI Card', value: 'kpi' },
                  { label: 'Bar Chart', value: 'bar' },
                  { label: 'Horizontal Bar', value: 'bar_horizontal' },
                  { label: 'Line Chart', value: 'line' },
                  { label: 'Area Chart', value: 'area' },
                  { label: 'Pie Chart', value: 'pie' },
                  { label: 'Donut Chart', value: 'donut' },
                  { label: 'Scatter Plot', value: 'scatter' },
                  { label: 'Heatmap', value: 'heatmap' },
                  { label: 'Treemap', value: 'treemap' },
                  { label: 'Funnel', value: 'funnel' },
                  { label: 'Gauge', value: 'gauge' },
                  { label: 'Radar', value: 'radar' },
                  { label: 'Waterfall', value: 'waterfall' },
                  { label: 'Data Table', value: 'table' },
                  { label: 'Text Block', value: 'text' },
                ]}
                style={{ width: '100%' }}
              />
            </div>

            <div style={{ marginBottom: 12 }}>
              <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>X Field (optional)</Text>
              <Select
                value={pythonVizConfig.x_field || undefined}
                onChange={(value) => setPythonVizConfig((prev) => ({ ...prev, x_field: value || '' }))}
                placeholder="date / category field"
                options={pythonDimensionColumns.map((col) => ({ label: col, value: col }))}
                notFoundContent={pythonFieldSchemaLoading ? 'Loading fields...' : 'No fields found'}
                allowClear
                showSearch
                optionFilterProp="label"
                style={{ width: '100%' }}
              />
            </div>
            <div style={{ marginBottom: 12 }}>
              <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Y Field (optional)</Text>
              <Select
                value={pythonVizConfig.y_field || undefined}
                onChange={(value) => setPythonVizConfig((prev) => ({ ...prev, y_field: value || '' }))}
                placeholder="numeric / measure field"
                options={pythonMetricColumns.map((col) => ({ label: col, value: col }))}
                notFoundContent={pythonFieldSchemaLoading ? 'Loading fields...' : 'No fields found'}
                allowClear
                showSearch
                optionFilterProp="label"
                style={{ width: '100%' }}
              />
            </div>
            <div style={{ marginBottom: 12 }}>
              <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Color Field (optional)</Text>
              <Select
                value={pythonVizConfig.color_field || undefined}
                onChange={(value) => setPythonVizConfig((prev) => ({ ...prev, color_field: value || '' }))}
                placeholder="segment / class field"
                options={pythonDimensionColumns.map((col) => ({ label: col, value: col }))}
                notFoundContent={pythonFieldSchemaLoading ? 'Loading fields...' : 'No fields found'}
                allowClear
                showSearch
                optionFilterProp="label"
                style={{ width: '100%' }}
              />
            </div>
            <div style={{ marginBottom: 12 }}>
              <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Group By (optional)</Text>
              <Select
                value={pythonVizConfig.group_by || undefined}
                onChange={(value) => setPythonVizConfig((prev) => ({ ...prev, group_by: value || '' }))}
                placeholder="secondary dimension"
                options={pythonDimensionColumns.map((col) => ({ label: col, value: col }))}
                notFoundContent={pythonFieldSchemaLoading ? 'Loading fields...' : 'No fields found'}
                allowClear
                showSearch
                optionFilterProp="label"
                style={{ width: '100%' }}
              />
            </div>
            <div style={{ marginBottom: 12 }}>
              <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Chart Title (optional)</Text>
              <Input
                value={pythonVizConfig.title || ''}
                onChange={(e) => setPythonVizConfig((prev) => ({ ...prev, title: e.target.value }))}
                placeholder="Python analytics title"
                style={S.inputDark}
              />
            </div>
            <div style={{ marginBottom: 12 }}>
              <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Row Limit</Text>
              <InputNumber
                min={100}
                max={100000}
                value={pythonVizConfig.limit}
                onChange={(value) => setPythonVizConfig((prev) => ({ ...prev, limit: Number(value) || 5000 }))}
                style={{ width: '100%' }}
              />
            </div>
            <div style={{ marginBottom: 12 }}>
              <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Optional SQL Filter</Text>
              <Input.TextArea
                rows={3}
                value={analyticsConfig.sql || ''}
                onChange={(e) => updateAnalyticsConfig({ sql: e.target.value || undefined })}
                placeholder="SELECT * FROM data WHERE ..."
                style={S.inputDark}
              />
            </div>

            <div style={{ display: 'flex', gap: 8 }}>
              <Button
                type="primary"
                icon={<CodeOutlined />}
                loading={pythonVizLoading}
                onClick={handleRunPythonVisualization}
                disabled={pythonVizSourceMissing}
              >
                Run Python Analytics
              </Button>
              <Button onClick={() => setPythonVizOpen(false)}>
                Close
              </Button>
            </div>
          </div>

          <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
            <div style={{ padding: '10px 14px', borderBottom: '1px solid var(--app-border)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 10 }}>
              <Text style={{ color: 'var(--app-text)', fontSize: 13, fontWeight: 600 }}>
                {pythonVizResult?.title || 'Visualization Output'}
              </Text>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                {pythonVizResult && (
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                    Rows: {Number(pythonVizResult.row_count || 0).toLocaleString()} | Sampled: {Number(pythonVizResult.sampled_row_count || 0).toLocaleString()} | Type: {pythonVizResult.chart_type}
                  </Text>
                )}
                <Button
                  size="small"
                  type="primary"
                  onClick={handleAddPythonWidgetToDashboard}
                  disabled={!pythonVizResult || !activeDashboard}
                >
                  Add To Dashboard
                </Button>
              </div>
            </div>

            {pythonVizError && (
              <div style={{ padding: '10px 14px', borderBottom: '1px solid var(--app-border)' }}>
                <Text style={{ color: '#ef4444', fontSize: 12 }}>{pythonVizError}</Text>
              </div>
            )}

            <div style={{ flex: 1, minHeight: 0, overflow: 'auto', background: '#0b1220' }}>
              {pythonVizLoading && (
                <div style={{ padding: 24 }}>
                  <Spin />
                  <Text style={{ color: 'var(--app-text-muted)', marginLeft: 10 }}>Running pandas + plotly analytics...</Text>
                </div>
              )}

              {!pythonVizLoading && pythonVizResult?.figure_html && (
                <iframe
                  title="python-analytics-plotly"
                  srcDoc={pythonVizResult.figure_html}
                  style={{ border: 0, width: '100%', height: '68vh', background: '#ffffff' }}
                />
              )}

              {!pythonVizLoading && !pythonVizResult?.figure_html && (
                <div style={{ padding: 24 }}>
                  <Text style={{ color: 'var(--app-text-subtle)' }}>
                    Configure datasource and click "Run Python Analytics" to render the chart.
                  </Text>
                </div>
              )}

              {pythonVizResult?.preview_rows && pythonVizResult.preview_rows.length > 0 && (
                <div style={{ padding: '10px 14px', background: 'var(--app-panel-bg)' }}>
                  <Text style={{ color: 'var(--app-text-muted)', fontSize: 12, display: 'block', marginBottom: 8 }}>
                    Data Preview ({pythonVizResult.preview_rows.length} rows)
                  </Text>
                  <Table
                    size="small"
                    dataSource={pythonVizResult.preview_rows.map((row, idx) => ({ key: `py-prev-${idx}`, ...row }))}
                    columns={pythonPreviewColumns.map((col) => ({
                      title: col,
                      dataIndex: col,
                      key: col,
                      width: 140,
                      render: (value: unknown) => {
                        if (value === null || value === undefined) return '-'
                        if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return String(value)
                        return JSON.stringify(value)
                      },
                    }))}
                    pagination={{
                      pageSize: 25,
                      pageSizeOptions: ['25', '50', '100'],
                      showSizeChanger: true,
                      hideOnSinglePage: false,
                    }}
                    scroll={{ x: true }}
                  />
                </div>
              )}
            </div>
          </div>
        </div>
      </Modal>

      <Modal
        title="Auto Generate Data Science Dashboard"
        open={analyticsOpen}
        onCancel={() => setAnalyticsOpen(false)}
        onOk={handleGenerateAnalyticsDashboard}
        okText="Generate Dashboard"
        confirmLoading={analyticsGenerating}
        okButtonProps={{ disabled: analyticsSourceMissing }}
        width={860}
        destroyOnClose={false}
      >
        <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 12 }}>
          <div style={{ flex: 1, minWidth: 220 }}>
            <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Source Type</Text>
            <Select
              value={analyticsConfig.source_type}
              onChange={(value) => handleAnalyticsSourceTypeChange(value as AnalyticsSourceType)}
              options={[
                { label: 'Sample Dataset', value: 'sample' },
                { label: 'ETL Pipeline Output', value: 'pipeline' },
                { label: 'MLOps Model Output', value: 'mlops' },
                { label: 'File Path', value: 'file' },
              ]}
              style={{ width: '100%' }}
            />
          </div>

          <div style={{ width: 220 }}>
            <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Forecast Horizon (days)</Text>
            <InputNumber
              min={3}
              max={3650}
              value={analyticsConfig.forecast_horizon}
              onChange={(value) => updateAnalyticsConfig({ forecast_horizon: Number(value) || 30 })}
              style={{ width: '100%' }}
            />
          </div>
        </div>

        {analyticsConfig.source_type === 'sample' && (
          <div style={{ marginBottom: 12 }}>
            <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Sample Dataset</Text>
            <Select
              value={analyticsConfig.dataset || 'sales'}
              onChange={(value) => updateAnalyticsConfig({ dataset: value })}
              options={SAMPLE_DATASETS.map((d) => ({ label: `${d.icon} ${d.label}`, value: d.key }))}
              style={{ width: '100%' }}
            />
          </div>
        )}

        {analyticsConfig.source_type === 'pipeline' && (
          <>
            <div style={{ marginBottom: 12 }}>
              <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Pipeline</Text>
              <Select
                value={analyticsConfig.pipeline_id || undefined}
                onChange={(value) => updateAnalyticsConfig({ pipeline_id: value, pipeline_node_id: undefined })}
                placeholder="Select ETL pipeline"
                options={analyticsPipelineOptions}
                loading={analyticsPipelinesLoading}
                allowClear
                showSearch
                optionFilterProp="label"
                style={{ width: '100%' }}
              />
            </div>
            <div style={{ marginBottom: 12 }}>
              <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Pipeline Node (optional)</Text>
              <Select
                value={analyticsConfig.pipeline_node_id || undefined}
                onChange={(value) => updateAnalyticsConfig({ pipeline_node_id: value })}
                placeholder="Auto-detect latest tabular node"
                options={analyticsPipelineNodes}
                loading={analyticsPipelineNodesLoading}
                disabled={!analyticsConfig.pipeline_id}
                allowClear
                showSearch
                optionFilterProp="label"
                style={{ width: '100%' }}
              />
            </div>
          </>
        )}

        {analyticsConfig.source_type === 'mlops' && (
          <>
            <div style={{ marginBottom: 12 }}>
              <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>MLOps Workflow</Text>
              <Select
                value={analyticsConfig.mlops_workflow_id || undefined}
                onChange={(value) =>
                  updateAnalyticsConfig({
                    mlops_workflow_id: value,
                    mlops_run_id: undefined,
                    mlops_node_id: undefined,
                  })
                }
                placeholder="Select workflow/model"
                options={analyticsMLOpsWorkflows}
                loading={analyticsMLOpsWorkflowsLoading}
                allowClear
                showSearch
                optionFilterProp="label"
                style={{ width: '100%' }}
              />
            </div>
            <div style={{ display: 'flex', gap: 12, marginBottom: 12 }}>
              <div style={{ flex: 1 }}>
                <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Run (optional)</Text>
                <Select
                  value={analyticsConfig.mlops_run_id || undefined}
                  onChange={(value, option) => {
                    const selected = option as MLOpsRunOption | undefined
                    const runWorkflowId = String(selected?.workflowId || '').trim()
                    const currentWorkflowId = String(analyticsConfig.mlops_workflow_id || '').trim()
                    if (runWorkflowId && runWorkflowId !== currentWorkflowId) {
                      updateAnalyticsConfig({
                        mlops_workflow_id: runWorkflowId,
                        mlops_run_id: value,
                        mlops_node_id: undefined,
                      })
                      return
                    }
                    updateAnalyticsConfig({ mlops_run_id: value })
                  }}
                  placeholder="Latest successful run"
                  options={analyticsMLOpsRuns}
                  loading={analyticsMLOpsRunsLoading}
                  disabled={!analyticsConfig.mlops_workflow_id}
                  allowClear
                  showSearch
                  optionFilterProp="label"
                  style={{ width: '100%' }}
                />
              </div>
              <div style={{ flex: 1 }}>
                <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Node (optional)</Text>
                <Select
                  value={analyticsConfig.mlops_node_id || undefined}
                  onChange={(value) => updateAnalyticsConfig({ mlops_node_id: value })}
                  placeholder="Auto-select prediction node"
                  options={analyticsMLOpsNodes}
                  loading={analyticsMLOpsNodesLoading}
                  disabled={!analyticsConfig.mlops_workflow_id}
                  allowClear
                  showSearch
                  optionFilterProp="label"
                  style={{ width: '100%' }}
                />
              </div>
            </div>
            <div style={{ display: 'flex', gap: 12, marginBottom: 12 }}>
              <div style={{ flex: 1 }}>
                <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Output Mode</Text>
                <Select
                  value={analyticsConfig.mlops_output_mode || 'predictions'}
                  onChange={(value) => updateAnalyticsConfig({ mlops_output_mode: value as 'predictions' | 'metrics' | 'monitor' | 'evaluation' })}
                  options={[
                    { label: 'Predictions', value: 'predictions' },
                    { label: 'Metrics Summary', value: 'metrics' },
                    { label: 'Monitoring Runs', value: 'monitor' },
                    { label: 'Evaluation Output', value: 'evaluation' },
                  ]}
                  style={{ width: '100%' }}
                />
              </div>
              <div style={{ flex: 1 }}>
                <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Prediction Start Date</Text>
                <Input
                  type="date"
                  value={analyticsConfig.mlops_prediction_start_date || ''}
                  onChange={(e) => updateAnalyticsConfig({ mlops_prediction_start_date: e.target.value || undefined })}
                  style={S.inputDark}
                />
              </div>
              <div style={{ flex: 1 }}>
                <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Prediction End Date</Text>
                <Input
                  type="date"
                  value={analyticsConfig.mlops_prediction_end_date || ''}
                  onChange={(e) => updateAnalyticsConfig({ mlops_prediction_end_date: e.target.value || undefined })}
                  style={S.inputDark}
                />
              </div>
            </div>
          </>
        )}

        {analyticsConfig.source_type === 'file' && (
          <div style={{ marginBottom: 12 }}>
            <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>File Path</Text>
            <Input
              value={analyticsConfig.file_path || ''}
              onChange={(e) => updateAnalyticsConfig({ file_path: e.target.value })}
              placeholder="/absolute/path/to/file.csv"
              style={S.inputDark}
            />
          </div>
        )}

        <Divider style={{ margin: '8px 0 12px 0' }} />

        <div style={{ marginBottom: 12 }}>
          <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>Optional SQL Filter</Text>
          <Input.TextArea
            rows={3}
            value={analyticsConfig.sql || ''}
            onChange={(e) => updateAnalyticsConfig({ sql: e.target.value || undefined })}
            placeholder="SELECT * FROM data WHERE ..."
            style={S.inputDark}
          />
        </div>

        <div style={{ marginBottom: 6 }}>
          <Text style={{ ...S.formLabel, display: 'block', marginBottom: 6 }}>NLP Prompt (Context-Aware Visualization)</Text>
          <Input.TextArea
            rows={4}
            value={analyticsConfig.nlp_prompt || ''}
            onChange={(e) => updateAnalyticsConfig({ nlp_prompt: e.target.value })}
            placeholder="Example: Show trend and forecast of temperature by date, highlight anomalies and key drivers."
            style={S.inputDark}
          />
        </div>

        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
          Generates a new dashboard and keeps your current dashboard unchanged.
        </Text>
      </Modal>
    </div>
  )
}
