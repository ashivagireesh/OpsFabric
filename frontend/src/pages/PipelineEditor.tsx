import { Component, type ChangeEvent, type ErrorInfo, type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  Button, Input, Space, Tag, Tooltip, Typography, Dropdown,
  notification, Modal, Form, Switch, Select
} from 'antd'
import {
  ArrowLeftOutlined, SaveOutlined, PlayCircleOutlined,
  LoadingOutlined, CheckCircleFilled,
  CloseCircleFilled, EllipsisOutlined, ScheduleOutlined,
  StopOutlined,
  HistoryOutlined, CodeOutlined, CopyOutlined,
  UndoOutlined, RedoOutlined, UploadOutlined, ClearOutlined, CloseOutlined
} from '@ant-design/icons'
import { ReactFlowProvider } from 'reactflow'
import { useWorkflowStore } from '../store'
import type { PipelineCanvasWidgetStyle } from '../store'
import type { ETLNode, ETLEdge } from '../types'
import {
  applyConnectorTypeToEdges,
  resolveConnectorTypeFromEdges,
  WORKFLOW_CONNECTOR_OPTIONS,
  type WorkflowConnectorType,
} from '../constants/workflowConnectors'
import api from '../api/client'
import NodePalette from '../components/workflow/NodePalette'
import WorkflowCanvas from '../components/workflow/WorkflowCanvas'
import ConfigDrawer from '../components/workflow/ConfigDrawer'
import ExecutionPanel from '../components/workflow/ExecutionPanel'
import { getNodeDef } from '../constants/nodeTypes'
import {
  EDITOR_AUTOSAVE_SETTINGS_CHANGED_EVENT,
  getPersistedEditorAutoSaveSettings,
  type EditorAutoSaveSettings,
} from '../utils/editorAutoSaveSettings'

const { Text } = Typography
const CUSTOM_SCHEDULE_VALUE = 'custom'
const DEFAULT_SCHEDULE_CRON = '0 * * * *'
type ExecutionMode = 'batch' | 'incremental' | 'streaming'
const DEFAULT_EXECUTION_MODE: ExecutionMode = 'batch'
const EXECUTION_MODE_OPTIONS = [
  { value: 'batch', label: 'Batch Processing' },
  { value: 'incremental', label: 'Incremental Update' },
  { value: 'streaming', label: 'Streaming Incremental' },
]
const PIPELINE_CANVAS_WIDGET_STYLE_OPTIONS: Array<{ value: PipelineCanvasWidgetStyle; label: string }> = [
  { value: 'default', label: 'Default' },
  { value: 'nin', label: 'nin' },
]
const MAIN_CANVAS_TAB_KEY = 'main'

type CanvasHistorySnapshot = {
  pipelineId: string | null
  nodes: any[]
  edges: any[]
  key: string
}

type WorkflowCanvasTab = {
  key: string
  kind: 'main' | 'business'
  label: string
  nodeId?: string
}

type WorkflowCanvasSnapshot = {
  nodes: any[]
  edges: any[]
  selectedNodeId: string | null
  connectorType: WorkflowConnectorType
  isDirty: boolean
}

type OpenBusinessCanvasTabDetail = {
  pipelineId?: string
  nodeId?: string
}

type EmbeddedNodeRuntimeStatus = 'idle' | 'running' | 'success' | 'error'
type EmbeddedNodeRuntime = {
  status: EmbeddedNodeRuntimeStatus
  rows?: number
  inputSample?: Array<Record<string, unknown>>
  outputSample?: Array<Record<string, unknown>>
}
type PipelineNodeRuntime = {
  status: EmbeddedNodeRuntimeStatus
  rows?: number
  processedRows?: number
  validatedRows?: number
}

function toRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {}
  return value as Record<string, unknown>
}

function parseJsonArraySafe(value: unknown): unknown[] {
  if (Array.isArray(value)) return value
  if (typeof value === 'string') {
    const text = value.trim()
    if (!text) return []
    try {
      const parsed = JSON.parse(text)
      return Array.isArray(parsed) ? parsed : []
    } catch {
      return []
    }
  }
  return []
}

function parseObjectLikeJsonValue(value: unknown): unknown {
  if (value && typeof value === 'object') return value
  if (typeof value !== 'string') return value
  let text = value.trim()
  if (!text) return value
  for (let attempt = 0; attempt < 2; attempt += 1) {
    if (
      (text.startsWith('"') && text.endsWith('"'))
      || (text.startsWith("'") && text.endsWith("'"))
    ) {
      const inner = text.slice(1, -1).trim()
      if (
        (inner.startsWith('{') && inner.endsWith('}'))
        || (inner.startsWith('[') && inner.endsWith(']'))
      ) {
        text = inner
      }
    }
    try {
      const parsed = JSON.parse(text)
      if (typeof parsed === 'string') {
        text = parsed.trim()
        continue
      }
      return parsed
    } catch {
      const normalized = text
        .replace(/\bNone\b/g, 'null')
        .replace(/\bTrue\b/g, 'true')
        .replace(/\bFalse\b/g, 'false')
        .replace(/([{,]\s*)'([^'\\]+?)'\s*:/g, '$1"$2":')
        .replace(/:\s*'([^'\\]*?)'(\s*[,}\]])/g, ': "$1"$2')
        .replace(/,\s*'([^'\\]*?)'(?=\s*[,}\]])/g, ', "$1"')
      try {
        return JSON.parse(normalized)
      } catch {
        return value
      }
    }
  }
  return value
}

function normalizeSampleRowsForCanvas(value: unknown, maxRows = 80): Array<Record<string, unknown>> {
  const out: Array<Record<string, unknown>> = []
  const seen = new Set<string>()
  const addRow = (rowObj: Record<string, unknown>) => {
    if (out.length >= maxRows) return
    try {
      const sig = JSON.stringify(rowObj)
      if (sig && seen.has(sig)) return
      if (sig) seen.add(sig)
    } catch {
      // best-effort dedupe only
    }
    out.push(cloneStructured(rowObj))
  }
  const collect = (candidate: unknown) => {
    if (candidate == null || out.length >= maxRows) return
    const parsed = parseObjectLikeJsonValue(candidate)
    if (Array.isArray(parsed)) {
      parsed.forEach((item) => collect(item))
      return
    }
    if (parsed && typeof parsed === 'object') {
      addRow(parsed as Record<string, unknown>)
      return
    }
  }
  collect(value)
  return out
}

function collectBusinessNodeParentSampleRows(node: ETLNode | undefined): Array<Record<string, unknown>> {
  if (!node?.data) return []
  const cfg = toRecord(node.data.config)
  const candidateSets = [
    node.data.executionSampleInput,
    node.data.executionSampleOutput,
    cfg._preview_rows,
  ]
  const merged: Array<Record<string, unknown>> = []
  candidateSets.forEach((candidate) => {
    const rows = normalizeSampleRowsForCanvas(candidate, 80)
    rows.forEach((rowObj) => {
      if (merged.length < 80) merged.push(rowObj)
    })
  })
  return normalizeSampleRowsForCanvas(merged, 80)
}

function collectUpstreamSampleRowsForNode(
  targetNodeId: string,
  nodes: ETLNode[],
  edges: ETLEdge[],
): Array<Record<string, unknown>> {
  const targetId = String(targetNodeId || '').trim()
  if (!targetId || !Array.isArray(nodes) || !Array.isArray(edges)) return []
  const nodeById = new Map(nodes.map((node) => [String(node?.id || '').trim(), node] as const))
  const sourceIds = Array.from(new Set(
    edges
      .filter((edge) => String(edge?.target || '').trim() === targetId)
      .map((edge) => String(edge?.source || '').trim())
      .filter(Boolean),
  ))
  if (sourceIds.length === 0) return []
  const merged: Array<Record<string, unknown>> = []
  sourceIds.forEach((sourceId) => {
    const sourceNode = nodeById.get(sourceId)
    if (!sourceNode?.data) return
    const sourceCfg = toRecord(sourceNode.data.config)
    const candidateSets = [
      sourceNode.data.executionSampleOutput,
      sourceNode.data.executionSampleInput,
      sourceCfg._preview_rows,
    ]
    candidateSets.forEach((candidate) => {
      const rows = normalizeSampleRowsForCanvas(candidate, 80)
      rows.forEach((rowObj) => {
        if (merged.length < 80) merged.push(rowObj)
      })
    })
  })
  return normalizeSampleRowsForCanvas(merged, 80)
}

function hydrateRootEmbeddedNodesWithParentSamples(
  nodes: ETLNode[],
  edges: ETLEdge[],
  parentRows: Array<Record<string, unknown>>,
): ETLNode[] {
  if (!Array.isArray(nodes) || nodes.length === 0 || !Array.isArray(parentRows) || parentRows.length === 0) {
    return nodes
  }
  const incomingTargets = new Set((Array.isArray(edges) ? edges : []).map((edge) => String(edge?.target || '').trim()))
  return nodes.map((node) => {
    const nodeId = String(node?.id || '').trim()
    if (!nodeId || incomingTargets.has(nodeId)) return node
    const prevData = (node?.data || {}) as any
    const nodeType = String(prevData.nodeType || node?.type || '').trim()
    const existingOutput = normalizeSampleRowsForCanvas(prevData.executionSampleOutput, 5)
    const shouldMirrorParentToOutput = nodeType === 'workflow_input_source'
    if (
      normalizeSampleRowsForCanvas(prevData.executionSampleInput, 5).length > 0
      && (!shouldMirrorParentToOutput || existingOutput.length > 0)
    ) {
      const inputSig = JSON.stringify(normalizeSampleRowsForCanvas(prevData.executionSampleInput, 5))
      const parentSig = JSON.stringify(normalizeSampleRowsForCanvas(parentRows, 5))
      if (inputSig === parentSig && (!shouldMirrorParentToOutput || JSON.stringify(existingOutput) === parentSig)) {
        return node
      }
    }
    return {
      ...node,
      data: {
        ...prevData,
        executionSampleInput: cloneStructured(parentRows),
        executionSampleOutput: shouldMirrorParentToOutput || existingOutput.length === 0
          ? cloneStructured(parentRows)
          : prevData.executionSampleOutput,
      },
    } as ETLNode
  })
}

function normalizeEmbeddedSampleRows(value: unknown, maxRows = 25): Array<Record<string, unknown>> {
  return normalizeSampleRowsForCanvas(value, maxRows)
}

function normalizeEmbeddedRuntimeStatus(raw: unknown): EmbeddedNodeRuntimeStatus {
  const norm = String(raw || '').trim().toLowerCase()
  if (norm === 'running') return 'running'
  if (norm === 'success') return 'success'
  if (norm === 'error' || norm === 'failed') return 'error'
  return 'idle'
}

function parseEmbeddedChildRuntimeRef(
  rawNodeId: unknown,
  parentNodeId: string,
): { childNodeId: string; isPerRowInstance: boolean } | null {
  const nodeId = String(rawNodeId || '').trim()
  const parent = String(parentNodeId || '').trim()
  if (!nodeId || !parent) return null
  const prefix = `${parent}::`
  if (!nodeId.startsWith(prefix)) return null
  const suffix = nodeId.slice(prefix.length)
  if (!suffix) return null
  const parts = suffix.split('::').filter(Boolean)
  if (parts.length === 1) return { childNodeId: parts[0], isPerRowInstance: false }
  if (parts.length === 2 && /^i\d+$/i.test(parts[0])) {
    return { childNodeId: parts[1], isPerRowInstance: true }
  }
  // Nested child workflow levels are intentionally ignored at this top-level child canvas.
  return null
}

function deriveEmbeddedRuntimeFromLogs(
  logs: Array<{ nodeId?: string; status?: string; rows?: number; input_sample?: unknown; output_sample?: unknown; sample_input?: unknown; sample_output?: unknown }> | undefined,
  parentNodeId: string,
): Map<string, EmbeddedNodeRuntime> {
  const out = new Map<string, EmbeddedNodeRuntime>()
  const list = Array.isArray(logs) ? logs : []
  for (const log of list) {
    const ref = parseEmbeddedChildRuntimeRef(log?.nodeId, parentNodeId)
    if (!ref) continue
    const nextStatus = normalizeEmbeddedRuntimeStatus(log?.status)
    const nextRows = Number.isFinite(Number(log?.rows)) ? Number(log?.rows) : undefined
    const nextInputSample = normalizeEmbeddedSampleRows((log as any)?.input_sample ?? (log as any)?.sample_input)
    const nextOutputSample = normalizeEmbeddedSampleRows((log as any)?.output_sample ?? (log as any)?.sample_output)
    const prev = out.get(ref.childNodeId)
    if (!prev) {
      out.set(ref.childNodeId, {
        status: nextStatus,
        rows: nextRows,
        inputSample: nextInputSample.length > 0 ? nextInputSample : undefined,
        outputSample: nextOutputSample.length > 0 ? nextOutputSample : undefined,
      })
      continue
    }
    // In per-row child invocation, success->running is expected for later row instances.
    // For non per-row, keep terminal state if a stale running event arrives later.
    const mergedStatus: EmbeddedNodeRuntimeStatus = (
      prev.status === 'error'
        ? 'error'
        : nextStatus === 'error'
          ? 'error'
          : (prev.status === 'success' && nextStatus === 'running' && !ref.isPerRowInstance)
            ? 'success'
            : nextStatus
    )
    out.set(ref.childNodeId, {
      status: mergedStatus,
      rows: nextRows ?? prev.rows,
      inputSample: nextInputSample.length > 0 ? nextInputSample : prev.inputSample,
      outputSample: nextOutputSample.length > 0 ? nextOutputSample : prev.outputSample,
    })
  }
  return out
}

function applyEmbeddedRuntimeToNodes(nodes: ETLNode[], runtimeByNodeId: Map<string, EmbeddedNodeRuntime>): ETLNode[] {
  if (!Array.isArray(nodes) || runtimeByNodeId.size === 0) return nodes
  return nodes.map((node) => {
    const runtime = runtimeByNodeId.get(String(node?.id || '').trim())
    if (!runtime) return node
    const prevData = (node?.data || {}) as any
    return {
      ...node,
      data: {
        ...prevData,
        status: runtime.status,
        executionRows: runtime.rows ?? prevData.executionRows,
        executionSampleInput: runtime.inputSample ?? prevData.executionSampleInput,
        executionSampleOutput: runtime.outputSample ?? prevData.executionSampleOutput,
      },
    } as ETLNode
  })
}

function derivePipelineRuntimeFromLogs(
  logs: Array<{ nodeId?: string; status?: string; rows?: number; processed_rows?: number; validated_rows?: number }> | undefined,
): Map<string, PipelineNodeRuntime> {
  const out = new Map<string, PipelineNodeRuntime>()
  const list = Array.isArray(logs) ? logs : []
  for (const log of list) {
    const nodeId = String(log?.nodeId || '').trim()
    if (!nodeId) continue
    if (nodeId === '__system__' || nodeId === 'system') continue
    // Embedded child workflow runtime IDs are handled separately.
    if (nodeId.includes('::')) continue
    const status = normalizeEmbeddedRuntimeStatus(log?.status)
    const rows = Number.isFinite(Number(log?.rows)) ? Number(log?.rows) : undefined
    const processedRows = Number.isFinite(Number(log?.processed_rows))
      ? Number(log?.processed_rows)
      : undefined
    const validatedRows = Number.isFinite(Number(log?.validated_rows))
      ? Number(log?.validated_rows)
      : undefined
    out.set(nodeId, {
      status,
      rows,
      processedRows,
      validatedRows,
    })
  }
  return out
}

function applyPipelineRuntimeToNodes(
  nodes: ETLNode[],
  runtimeByNodeId: Map<string, PipelineNodeRuntime>,
): ETLNode[] {
  if (!Array.isArray(nodes) || runtimeByNodeId.size === 0) return nodes
  return nodes.map((node) => {
    const runtime = runtimeByNodeId.get(String(node?.id || '').trim())
    if (!runtime) return node
    const prevData = (node?.data || {}) as any
    return {
      ...node,
      data: {
        ...prevData,
        status: runtime.status,
        executionRows: runtime.rows ?? prevData.executionRows,
        executionProcessedRows: runtime.processedRows ?? prevData.executionProcessedRows,
        executionValidatedRows: runtime.validatedRows ?? prevData.executionValidatedRows,
      },
    } as ETLNode
  })
}

function normalizeEmbeddedNodes(rawNodes: unknown): ETLNode[] {
  const list = parseJsonArraySafe(rawNodes)
  const out: ETLNode[] = []
  list.forEach((item, idx) => {
    const row = toRecord(item)
    const id = String(row.id || `bw_node_${idx + 1}`).trim()
    if (!id) return
    const rawData = toRecord(row.data)
    const nodeType = String(rawData.nodeType || row.nodeType || row.type || '').trim()
    if (!nodeType || nodeType === 'etlNode') return
    const def = getNodeDef(nodeType)
    if (!def) return
    const label = String(rawData.label || row.label || def.label || nodeType).trim() || nodeType
    const rawConfig = rawData.config
    const config = toRecord(rawConfig || row.config)
    const rawPos = toRecord(row.position)
    const x = Number(rawPos.x)
    const y = Number(rawPos.y)
    out.push({
      id,
      type: 'etlNode',
      position: {
        x: Number.isFinite(x) ? x : 120 + (idx % 4) * 260,
        y: Number.isFinite(y) ? y : 100 + Math.floor(idx / 4) * 160,
      },
      data: {
        nodeType,
        label,
        definition: def,
        config,
        status: 'idle',
      },
    } as ETLNode)
  })
  return out
}

function normalizeEmbeddedEdges(rawEdges: unknown, nodes: ETLNode[], connectorType: WorkflowConnectorType): ETLEdge[] {
  const list = parseJsonArraySafe(rawEdges)
  const nodeIds = new Set(nodes.map((n) => String(n.id || '').trim()))
  const base: ETLEdge[] = []
  const seen = new Set<string>()
  list.forEach((item, idx) => {
    const row = toRecord(item)
    const source = String(row.source || '').trim()
    const target = String(row.target || '').trim()
    if (!source || !target) return
    if (!nodeIds.has(source) || !nodeIds.has(target)) return
    const sourceHandle = String(row.sourceHandle || 'output').trim() || 'output'
    const targetHandle = String(row.targetHandle || 'input').trim() || 'input'
    const uniq = `${source}|${sourceHandle}|${target}|${targetHandle}`
    if (seen.has(uniq)) return
    seen.add(uniq)
    base.push({
      id: String(row.id || `bw_edge_${idx + 1}_${source}_${target}`).trim() || `bw_edge_${idx + 1}_${source}_${target}`,
      source,
      target,
      sourceHandle,
      targetHandle,
      type: connectorType,
      reconnectable: true,
      updatable: true,
      animated: true,
      interactionWidth: 44,
      style: { stroke: '#6366f1', strokeWidth: 2 },
    } as ETLEdge)
  })
  return applyConnectorTypeToEdges(base, connectorType) as ETLEdge[]
}

function serializeEmbeddedNodes(nodes: any[]): Array<Record<string, unknown>> {
  return (Array.isArray(nodes) ? nodes : []).map((node) => {
    const cfg = toRecord(node?.data?.config)
    return {
      id: String(node?.id || ''),
      position: {
        x: Number(node?.position?.x || 0),
        y: Number(node?.position?.y || 0),
      },
      data: {
        nodeType: String(node?.data?.nodeType || ''),
        label: String(node?.data?.label || ''),
        config: cfg,
      },
    }
  }).filter((node) => String(node.id || '').trim().length > 0)
}

function serializeEmbeddedEdges(edges: any[]): Array<Record<string, unknown>> {
  return (Array.isArray(edges) ? edges : []).map((edge) => ({
    id: String(edge?.id || ''),
    source: String(edge?.source || ''),
    target: String(edge?.target || ''),
    sourceHandle: edge?.sourceHandle ? String(edge.sourceHandle) : undefined,
    targetHandle: edge?.targetHandle ? String(edge.targetHandle) : undefined,
  })).filter((edge) => String(edge.source || '').trim() && String(edge.target || '').trim())
}

type ConfigDrawerErrorBoundaryProps = {
  resetKey: string | null
  children: ReactNode
}

type ConfigDrawerErrorBoundaryState = {
  hasError: boolean
  message: string
}

class ConfigDrawerErrorBoundary extends Component<ConfigDrawerErrorBoundaryProps, ConfigDrawerErrorBoundaryState> {
  state: ConfigDrawerErrorBoundaryState = {
    hasError: false,
    message: '',
  }

  static getDerivedStateFromError(error: unknown): ConfigDrawerErrorBoundaryState {
    return {
      hasError: true,
      message: String((error as any)?.message || error || 'Unknown render error'),
    }
  }

  componentDidCatch(error: unknown, info: ErrorInfo): void {
    console.error('ConfigDrawer render failed:', error, info)
  }

  componentDidUpdate(prevProps: ConfigDrawerErrorBoundaryProps): void {
    if (this.state.hasError && prevProps.resetKey !== this.props.resetKey) {
      this.setState({ hasError: false, message: '' })
    }
  }

  render(): ReactNode {
    if (!this.state.hasError) return this.props.children
    return (
      <div
        style={{
          width: 360,
          borderLeft: '1px solid var(--app-border-strong)',
          background: 'var(--app-panel-bg)',
          color: 'var(--app-text)',
          padding: 16,
          display: 'flex',
          flexDirection: 'column',
          gap: 8,
        }}
      >
        <Text style={{ color: '#ef4444', fontWeight: 700 }}>Configuration panel crashed</Text>
        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
          {this.state.message || 'Unknown error'}
        </Text>
        <Button size="small" onClick={() => this.setState({ hasError: false, message: '' })}>
          Retry
        </Button>
      </div>
    )
  }
}

function cloneStructured<T>(value: T): T {
  try {
    if (typeof structuredClone === 'function') {
      return structuredClone(value)
    }
  } catch {
    // fallback below
  }
  return JSON.parse(JSON.stringify(value)) as T
}

function normalizeNodeForHistory(node: any): any {
  if (!node || typeof node !== 'object') return node
  const cleaned: any = { ...node }
  delete cleaned.selected
  delete cleaned.dragging
  delete cleaned.resizing
  delete cleaned.positionAbsolute
  delete cleaned.zIndex
  if (cleaned.data && typeof cleaned.data === 'object') {
    cleaned.data = { ...cleaned.data }
    delete cleaned.data.status
    delete cleaned.data.executionRows
    delete cleaned.data.executionProcessedRows
    delete cleaned.data.executionValidatedRows
    delete cleaned.data.executionSampleInput
    delete cleaned.data.executionSampleOutput
    delete cleaned.data.executionError
    delete cleaned.data.executionStartedAt
    delete cleaned.data.executionFinishedAt
    delete cleaned.data.executionDurationMs
    if (cleaned.data.config && typeof cleaned.data.config === 'object') {
      const cfg: Record<string, unknown> = { ...(cleaned.data.config as Record<string, unknown>) }
      const dropExact = new Set([
        '_preview_rows',
        '_preview_columns',
        '_detected_columns',
        '_row_count',
      ])
      Object.keys(cfg).forEach((key) => {
        const name = String(key || '')
        if (!name) return
        if (dropExact.has(name)) {
          delete cfg[name]
          return
        }
        if (
          name.startsWith('_preview_')
          || name.startsWith('__preview_')
          || name.startsWith('_detected_')
          || name.startsWith('__detected_')
        ) {
          delete cfg[name]
        }
      })
      cleaned.data.config = cfg
    }
  }
  return cleaned
}

function normalizeEdgeForHistory(edge: any): any {
  if (!edge || typeof edge !== 'object') return edge
  const cleaned: any = { ...edge }
  delete cleaned.selected
  return cleaned
}

function createCanvasHistorySnapshot(
  pipelineId: string | null,
  nodes: any[],
  edges: any[],
): CanvasHistorySnapshot {
  const safeNodes = cloneStructured((Array.isArray(nodes) ? nodes : []).map(normalizeNodeForHistory))
  const safeEdges = cloneStructured((Array.isArray(edges) ? edges : []).map(normalizeEdgeForHistory))
  const compactNodeKey = safeNodes
    .map((node: any) => {
      const id = String(node?.id || '')
      const pos = node?.position || {}
      const x = Number(pos?.x || 0)
      const y = Number(pos?.y || 0)
      const nodeType = String(node?.data?.nodeType || '')
      let configKey = ''
      try {
        configKey = JSON.stringify(node?.data?.config || {})
      } catch {
        configKey = ''
      }
      if (configKey.length > 4000) configKey = configKey.slice(0, 4000)
      return `${id}@${Math.round(x)}:${Math.round(y)}:${nodeType}:${configKey}`
    })
    .join('~')
  const compactEdgeKey = safeEdges
    .map((edge: any) => `${String(edge?.source || '')}:${String(edge?.sourceHandle || '')}->${String(edge?.target || '')}:${String(edge?.targetHandle || '')}`)
    .join('~')
  return {
    pipelineId,
    nodes: safeNodes,
    edges: safeEdges,
    key: `${pipelineId || ''}|${compactNodeKey}|${compactEdgeKey}`,
  }
}

function clearNodeRuntimeState(nodes: any[] | undefined): any[] {
  if (!Array.isArray(nodes)) return []
  return nodes.map((node: any) => ({
    ...node,
    data: {
      ...(node?.data || {}),
      status: 'idle',
      executionRows: undefined,
      executionProcessedRows: undefined,
      executionValidatedRows: undefined,
      executionSampleInput: undefined,
      executionSampleOutput: undefined,
      executionError: undefined,
      executionStartedAt: undefined,
      executionFinishedAt: undefined,
      executionDurationMs: undefined,
    },
  }))
}

const SCHEDULE_PRESETS = [
  { value: 'every_1_min', label: 'Every 1 minute', cron: '* * * * *' },
  { value: 'every_5_min', label: 'Every 5 minutes', cron: '*/5 * * * *' },
  { value: 'every_15_min', label: 'Every 15 minutes', cron: '*/15 * * * *' },
  { value: 'hourly', label: 'Every hour', cron: '0 * * * *' },
  { value: 'daily_9am', label: 'Daily at 9:00 AM', cron: '0 9 * * *' },
  { value: 'weekly_mon_9am', label: 'Weekly on Monday 9:00 AM', cron: '0 9 * * 1' },
  { value: CUSTOM_SCHEDULE_VALUE, label: 'Custom Cron', cron: '' },
]

function normalizeCronExpr(cron: string): string {
  return cron.trim().split(/\s+/).join(' ')
}

function detectSchedulePreset(cron?: string): string {
  const normalized = normalizeCronExpr(cron || '')
  const matched = SCHEDULE_PRESETS.find(
    (preset) => preset.value !== CUSTOM_SCHEDULE_VALUE && preset.cron === normalized,
  )
  return matched?.value || CUSTOM_SCHEDULE_VALUE
}

function toBoundedInt(value: unknown, fallback: number, min: number, max: number): number {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(min, Math.min(max, Math.floor(parsed)))
}

function parseNodeEnabledValue(value: unknown): boolean {
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return value !== 0
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase()
    if (['false', '0', 'no', 'off', 'disabled', 'disable'].includes(normalized)) return false
    if (['true', '1', 'yes', 'on', 'enabled', 'enable'].includes(normalized)) return true
  }
  return true
}

function extractExecutionRuntimeFromNodes(nodes: any[] | undefined): {
  executionMode: ExecutionMode
  batchSize: number
  incrementalField: string
  streamingIntervalSeconds: number
  streamingMaxBatches: number
} {
  const fallback = {
    executionMode: DEFAULT_EXECUTION_MODE,
    batchSize: 5000,
    incrementalField: '',
    streamingIntervalSeconds: 5,
    streamingMaxBatches: 10,
  }
  if (!Array.isArray(nodes)) return fallback
  const trigger = nodes.find((node) => {
    const nodeType = String(node?.data?.nodeType || node?.type || '')
    return ['manual_trigger', 'schedule_trigger', 'webhook_trigger'].includes(nodeType)
  })
  if (!trigger) return fallback
  const cfg = (trigger?.data?.config && typeof trigger.data.config === 'object')
    ? trigger.data.config
    : {}
  const modeRaw = String((cfg as any).execution_mode || fallback.executionMode).trim().toLowerCase()
  const mode: ExecutionMode = (['batch', 'incremental', 'streaming'].includes(modeRaw)
    ? modeRaw
    : fallback.executionMode) as ExecutionMode
  return {
    executionMode: mode,
    batchSize: toBoundedInt((cfg as any).batch_size, fallback.batchSize, 1, 1_000_000),
    incrementalField: String((cfg as any).incremental_field || '').trim(),
    streamingIntervalSeconds: toBoundedInt((cfg as any).streaming_interval_seconds, fallback.streamingIntervalSeconds, 1, 3600),
    streamingMaxBatches: toBoundedInt((cfg as any).streaming_max_batches, fallback.streamingMaxBatches, 1, 10_000),
  }
}

function applyExecutionRuntimeToTriggerNodes(nodes: any[] | undefined, runtime: {
  executionMode: ExecutionMode
  batchSize: number
  incrementalField: string
  streamingIntervalSeconds: number
  streamingMaxBatches: number
}) {
  if (!Array.isArray(nodes)) return []
  return nodes.map((node) => {
    const nodeType = String(node?.data?.nodeType || node?.type || '')
    if (!['manual_trigger', 'schedule_trigger', 'webhook_trigger'].includes(nodeType)) {
      return node
    }
    const prevCfg = (node?.data?.config && typeof node.data.config === 'object')
      ? node.data.config
      : {}
    return {
      ...node,
      data: {
        ...(node.data || {}),
        config: {
          ...prevCfg,
          execution_mode: runtime.executionMode,
          batch_size: runtime.batchSize,
          incremental_field: runtime.incrementalField,
          streaming_interval_seconds: runtime.streamingIntervalSeconds,
          streaming_max_batches: runtime.streamingMaxBatches,
        },
      },
    }
  })
}

export default function PipelineEditor() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const {
    nodes, edges,
    pipeline, loadPipeline, savePipeline, executePipeline,
    abortExecution, executionAbortRequested, resumeExecutionForPipeline,
    isDirty, isExecuting, selectedNodeId, setSelectedNode,
    resetCanvas, executionLogs, connectorType, setConnectorType,
    canvasWidgetStyle, setCanvasWidgetStyle, duplicateNode,
  } = useWorkflowStore()

  const [saving, setSaving] = useState(false)
  const [editorAutoSaveSettings, setEditorAutoSaveSettings] = useState<EditorAutoSaveSettings>(() => (
    getPersistedEditorAutoSaveSettings()
  ))
  const [scheduleModal, setScheduleModal] = useState(false)
  const [pipelineName, setPipelineName] = useState('')
  const [form] = Form.useForm()
  const selectedSchedulePreset = Form.useWatch('schedulePreset', form)
  const selectedExecutionMode = Form.useWatch('executionMode', form)
  const [canUndo, setCanUndo] = useState(false)
  const [canRedo, setCanRedo] = useState(false)
  const [historyReady, setHistoryReady] = useState(false)
  const historyRef = useRef<CanvasHistorySnapshot[]>([])
  const historyIndexRef = useRef<number>(-1)
  const applyingHistoryRef = useRef(false)
  const activeHistoryPipelineRef = useRef<string | null>(null)
  const importFileInputRef = useRef<HTMLInputElement | null>(null)
  const [canvasTabs, setCanvasTabs] = useState<WorkflowCanvasTab[]>([
    { key: MAIN_CANVAS_TAB_KEY, kind: 'main', label: 'Main Canvas' },
  ])
  const [activeCanvasTabKey, setActiveCanvasTabKey] = useState<string>(MAIN_CANVAS_TAB_KEY)
  const canvasSnapshotsRef = useRef<Record<string, WorkflowCanvasSnapshot>>({})
  const activeCanvasTabKeyRef = useRef<string>(MAIN_CANVAS_TAB_KEY)

  useEffect(() => {
    const syncEditorAutoSaveSettings = () => {
      setEditorAutoSaveSettings(getPersistedEditorAutoSaveSettings())
    }
    window.addEventListener('storage', syncEditorAutoSaveSettings)
    window.addEventListener(EDITOR_AUTOSAVE_SETTINGS_CHANGED_EVENT, syncEditorAutoSaveSettings as EventListener)
    return () => {
      window.removeEventListener('storage', syncEditorAutoSaveSettings)
      window.removeEventListener(EDITOR_AUTOSAVE_SETTINGS_CHANGED_EVENT, syncEditorAutoSaveSettings as EventListener)
    }
  }, [])

  const captureActiveCanvasSnapshot = useCallback((): WorkflowCanvasSnapshot => {
    const state = useWorkflowStore.getState()
    return {
      nodes: cloneStructured(Array.isArray(state.nodes) ? state.nodes : []),
      edges: cloneStructured(Array.isArray(state.edges) ? state.edges : []),
      selectedNodeId: state.selectedNodeId ? String(state.selectedNodeId) : null,
      connectorType: (state.connectorType || 'smooth') as WorkflowConnectorType,
      isDirty: Boolean(state.isDirty),
    }
  }, [])

  const applyCanvasSnapshot = useCallback((snapshot: WorkflowCanvasSnapshot) => {
    const nextSelected = snapshot.selectedNodeId ? String(snapshot.selectedNodeId) : null
    useWorkflowStore.setState((state) => ({
      ...state,
      nodes: cloneStructured(Array.isArray(snapshot.nodes) ? snapshot.nodes : []),
      edges: cloneStructured(Array.isArray(snapshot.edges) ? snapshot.edges : []),
      connectorType: (snapshot.connectorType || state.connectorType || 'smooth') as WorkflowConnectorType,
      selectedNodeId: nextSelected,
      isDirty: Boolean(snapshot.isDirty),
    }))
    setSelectedNode(nextSelected)
  }, [setSelectedNode])

  const ensureMainSnapshot = useCallback(() => {
    if (canvasSnapshotsRef.current[MAIN_CANVAS_TAB_KEY]) return
    canvasSnapshotsRef.current[MAIN_CANVAS_TAB_KEY] = captureActiveCanvasSnapshot()
  }, [captureActiveCanvasSnapshot])

  const buildBusinessCanvasSnapshotFromMain = useCallback((nodeId: string): { snapshot: WorkflowCanvasSnapshot; label: string } => {
    ensureMainSnapshot()
    const mainSnapshot = canvasSnapshotsRef.current[MAIN_CANVAS_TAB_KEY] || captureActiveCanvasSnapshot()
    const mainNodes = Array.isArray(mainSnapshot.nodes) ? mainSnapshot.nodes : []
    const hit = mainNodes.find((node: any) => String(node?.id || '') === String(nodeId || ''))
    if (!hit) {
      throw new Error('Business workflow node not found in main canvas.')
    }
    const nodeType = String(hit?.data?.nodeType || hit?.type || '')
    if (nodeType !== 'business_workflow') {
      throw new Error('Selected node is not a Business Logic Workflow node.')
    }
    const config = toRecord(hit?.data?.config)
    const embeddedNodes = normalizeEmbeddedNodes(config.embedded_workflow_nodes)
    const inferredConnector = resolveConnectorTypeFromEdges(
      Array.isArray(config.embedded_workflow_edges) ? (config.embedded_workflow_edges as any[]) : []
    )
    const nextConnector = (inferredConnector || mainSnapshot.connectorType || 'smooth') as WorkflowConnectorType
    const embeddedEdges = normalizeEmbeddedEdges(config.embedded_workflow_edges, embeddedNodes, nextConnector)
    const runtimeByChildNode = deriveEmbeddedRuntimeFromLogs(executionLogs as any[], String(nodeId))
    const runtimeNodes = applyEmbeddedRuntimeToNodes(embeddedNodes, runtimeByChildNode)
    const parentInputSamples = normalizeSampleRowsForCanvas([
      ...collectBusinessNodeParentSampleRows(hit as ETLNode),
      ...collectUpstreamSampleRowsForNode(String(nodeId), mainNodes as ETLNode[], (mainSnapshot.edges || []) as ETLEdge[]),
    ], 80)
    const hydratedNodes = hydrateRootEmbeddedNodesWithParentSamples(runtimeNodes, embeddedEdges, parentInputSamples)
    return {
      snapshot: {
        nodes: hydratedNodes,
        edges: embeddedEdges,
        selectedNodeId: null,
        connectorType: nextConnector,
        isDirty: false,
      },
      label: String(hit?.data?.label || 'Business Workflow'),
    }
  }, [captureActiveCanvasSnapshot, ensureMainSnapshot, executionLogs])

  useEffect(() => {
    const businessTabs = canvasTabs.filter((tab) => tab.kind === 'business' && Boolean(tab.nodeId))

    let activeNodesPatch: ETLNode[] | null = null
    const statusRowsChanged = (prevNodes: ETLNode[], nextNodes: ETLNode[]): boolean => {
      if (nextNodes.length !== prevNodes.length) return true
      for (let idx = 0; idx < nextNodes.length; idx += 1) {
        const prevStatus = String((prevNodes[idx] as any)?.data?.status || 'idle')
        const nextStatus = String((nextNodes[idx] as any)?.data?.status || 'idle')
        const prevRows = (prevNodes[idx] as any)?.data?.executionRows
        const nextRows = (nextNodes[idx] as any)?.data?.executionRows
        const prevProcessed = (prevNodes[idx] as any)?.data?.executionProcessedRows
        const nextProcessed = (nextNodes[idx] as any)?.data?.executionProcessedRows
        const prevValidated = (prevNodes[idx] as any)?.data?.executionValidatedRows
        const nextValidated = (nextNodes[idx] as any)?.data?.executionValidatedRows
        const prevSampleIn = (prevNodes[idx] as any)?.data?.executionSampleInput
        const nextSampleIn = (nextNodes[idx] as any)?.data?.executionSampleInput
        const prevSampleOut = (prevNodes[idx] as any)?.data?.executionSampleOutput
        const nextSampleOut = (nextNodes[idx] as any)?.data?.executionSampleOutput
        if (
          prevStatus !== nextStatus
          || prevRows !== nextRows
          || prevProcessed !== nextProcessed
          || prevValidated !== nextValidated
          || prevSampleIn !== nextSampleIn
          || prevSampleOut !== nextSampleOut
        ) {
          return true
        }
      }
      return false
    }

    let mainSnapshot = canvasSnapshotsRef.current[MAIN_CANVAS_TAB_KEY]
    if (activeCanvasTabKeyRef.current === MAIN_CANVAS_TAB_KEY) {
      const liveMainSnapshot = captureActiveCanvasSnapshot()
      canvasSnapshotsRef.current[MAIN_CANVAS_TAB_KEY] = liveMainSnapshot
      mainSnapshot = liveMainSnapshot
    }
    if (mainSnapshot && Array.isArray(mainSnapshot.nodes) && mainSnapshot.nodes.length > 0) {
      const runtimeByMainNode = derivePipelineRuntimeFromLogs(executionLogs as any[])
      if (runtimeByMainNode.size > 0) {
        const nextMainNodes = applyPipelineRuntimeToNodes(mainSnapshot.nodes as ETLNode[], runtimeByMainNode)
        if (statusRowsChanged(mainSnapshot.nodes as ETLNode[], nextMainNodes)) {
          canvasSnapshotsRef.current[MAIN_CANVAS_TAB_KEY] = {
            ...mainSnapshot,
            nodes: cloneStructured(nextMainNodes),
          }
          if (activeCanvasTabKeyRef.current === MAIN_CANVAS_TAB_KEY) {
            activeNodesPatch = nextMainNodes
          }
        }
      }
    }

    if (businessTabs.length === 0) {
      if (activeNodesPatch) {
        useWorkflowStore.setState((state) => ({
          ...state,
          nodes: cloneStructured(activeNodesPatch as ETLNode[]),
        }))
      }
      return
    }

    businessTabs.forEach((tab) => {
      const tabKey = String(tab.key || '').trim()
      const parentNodeId = String(tab.nodeId || '').trim()
      if (!tabKey || !parentNodeId) return
      const snapshot = canvasSnapshotsRef.current[tabKey]
      if (!snapshot || !Array.isArray(snapshot.nodes) || snapshot.nodes.length === 0) return
      const latestMainSnapshot = canvasSnapshotsRef.current[MAIN_CANVAS_TAB_KEY]
      const parentNode = (Array.isArray(latestMainSnapshot?.nodes) ? latestMainSnapshot.nodes : [])
        .find((node: any) => String(node?.id || '') === parentNodeId) as ETLNode | undefined
      const parentInputSamples = collectBusinessNodeParentSampleRows(parentNode)
      const parentHydratedNodes = hydrateRootEmbeddedNodesWithParentSamples(
        snapshot.nodes as ETLNode[],
        Array.isArray(snapshot.edges) ? snapshot.edges : [],
        parentInputSamples,
      )
      const runtimeByChildNode = deriveEmbeddedRuntimeFromLogs(executionLogs as any[], parentNodeId)
      const nextNodes = runtimeByChildNode.size > 0
        ? applyEmbeddedRuntimeToNodes(parentHydratedNodes, runtimeByChildNode)
        : parentHydratedNodes

      if (!statusRowsChanged(snapshot.nodes as ETLNode[], nextNodes)) return

      canvasSnapshotsRef.current[tabKey] = {
        ...snapshot,
        nodes: cloneStructured(nextNodes),
      }

      if (activeCanvasTabKeyRef.current === tabKey) {
        activeNodesPatch = nextNodes
      }
    })

    if (activeNodesPatch) {
      useWorkflowStore.setState((state) => ({
        ...state,
        nodes: cloneStructured(activeNodesPatch as ETLNode[]),
      }))
    }
  }, [canvasTabs, executionLogs])

  const switchCanvasTab = useCallback((nextTabKey: string) => {
    const target = String(nextTabKey || '').trim()
    if (!target || target === activeCanvasTabKeyRef.current) return
    canvasSnapshotsRef.current[activeCanvasTabKeyRef.current] = captureActiveCanvasSnapshot()
    const nextSnapshot = canvasSnapshotsRef.current[target]
    if (!nextSnapshot) return
    applyCanvasSnapshot(nextSnapshot)
    activeCanvasTabKeyRef.current = target
    setActiveCanvasTabKey(target)
  }, [applyCanvasSnapshot, captureActiveCanvasSnapshot])

  const openBusinessCanvasTab = useCallback((nodeIdRaw: string) => {
    const nodeId = String(nodeIdRaw || '').trim()
    if (!nodeId) return
    const tabKey = `business:${nodeId}`
    canvasSnapshotsRef.current[activeCanvasTabKeyRef.current] = captureActiveCanvasSnapshot()
    try {
      if (!canvasSnapshotsRef.current[tabKey]) {
        const built = buildBusinessCanvasSnapshotFromMain(nodeId)
        canvasSnapshotsRef.current[tabKey] = built.snapshot
        setCanvasTabs((prev) => {
          const exists = prev.some((tab) => tab.key === tabKey)
          if (exists) {
            return prev.map((tab) => (tab.key === tabKey ? { ...tab, label: built.label } : tab))
          }
          return [...prev, { key: tabKey, kind: 'business', label: built.label, nodeId }]
        })
      } else {
        const mainSnapshot = canvasSnapshotsRef.current[MAIN_CANVAS_TAB_KEY] || captureActiveCanvasSnapshot()
        const parentNode = (Array.isArray(mainSnapshot.nodes) ? mainSnapshot.nodes : [])
          .find((node: any) => String(node?.id || '') === nodeId) as ETLNode | undefined
        const parentInputSamples = collectBusinessNodeParentSampleRows(parentNode)
        const existingSnapshot = canvasSnapshotsRef.current[tabKey]
        canvasSnapshotsRef.current[tabKey] = {
          ...existingSnapshot,
          nodes: hydrateRootEmbeddedNodesWithParentSamples(
            (existingSnapshot?.nodes || []) as ETLNode[],
            (existingSnapshot?.edges || []) as ETLEdge[],
            parentInputSamples,
          ),
        }
        setCanvasTabs((prev) => prev.map((tab) => (tab.key === tabKey ? { ...tab, nodeId } : tab)))
      }
      switchCanvasTab(tabKey)
    } catch (error: any) {
      notification.error({
        message: 'Unable to open child canvas',
        description: String(error?.message || 'Invalid business workflow node configuration'),
        placement: 'bottomRight',
        duration: 3,
      })
    }
  }, [buildBusinessCanvasSnapshotFromMain, captureActiveCanvasSnapshot, switchCanvasTab])

  const closeCanvasTab = useCallback((tabKeyRaw: string) => {
    const tabKey = String(tabKeyRaw || '').trim()
    if (!tabKey || tabKey === MAIN_CANVAS_TAB_KEY) return
    if (isExecuting) {
      notification.warning({
        message: 'Abort running pipeline before closing canvas tab.',
        placement: 'bottomRight',
        duration: 2.5,
      })
      return
    }
    delete canvasSnapshotsRef.current[tabKey]
    setCanvasTabs((prev) => prev.filter((tab) => tab.key !== tabKey))
    if (activeCanvasTabKeyRef.current === tabKey) {
      const fallback = canvasSnapshotsRef.current[MAIN_CANVAS_TAB_KEY] || captureActiveCanvasSnapshot()
      applyCanvasSnapshot(fallback)
      activeCanvasTabKeyRef.current = MAIN_CANVAS_TAB_KEY
      setActiveCanvasTabKey(MAIN_CANVAS_TAB_KEY)
    }
  }, [applyCanvasSnapshot, captureActiveCanvasSnapshot, isExecuting])

  const resetRuntimeStateAcrossCanvases = useCallback(() => {
    const currentSnapshots = canvasSnapshotsRef.current || {}
    const nextSnapshots: Record<string, WorkflowCanvasSnapshot> = {}
    Object.entries(currentSnapshots).forEach(([tabKey, snapshot]) => {
      if (!snapshot || !Array.isArray(snapshot.nodes)) {
        return
      }
      nextSnapshots[tabKey] = {
        ...snapshot,
        nodes: clearNodeRuntimeState(snapshot.nodes),
      }
    })
    canvasSnapshotsRef.current = nextSnapshots
    useWorkflowStore.setState((state) => ({
      ...state,
      nodes: clearNodeRuntimeState(state.nodes),
      executionLogs: [],
    }))
  }, [])

  const saveActiveCanvas = useCallback(async (options?: { silent?: boolean }) => {
    const silent = Boolean(options?.silent)
    if (activeCanvasTabKeyRef.current === MAIN_CANVAS_TAB_KEY) {
      setSaving(true)
      try {
        await savePipeline()
        if (!silent) {
          notification.success({ message: 'Saved!', placement: 'bottomRight', duration: 2 })
        }
      } finally {
        setSaving(false)
      }
      return
    }
    const activeTab = canvasTabs.find((tab) => tab.key === activeCanvasTabKeyRef.current)
    if (!activeTab || activeTab.kind !== 'business' || !activeTab.nodeId) return
    if (!id) return
    setSaving(true)
    try {
      canvasSnapshotsRef.current[activeCanvasTabKeyRef.current] = captureActiveCanvasSnapshot()
      ensureMainSnapshot()
      const mainSnapshot = canvasSnapshotsRef.current[MAIN_CANVAS_TAB_KEY]
      const childSnapshot = canvasSnapshotsRef.current[activeCanvasTabKeyRef.current]
      if (!mainSnapshot || !childSnapshot) return
      const updatedMainNodes = (Array.isArray(mainSnapshot.nodes) ? mainSnapshot.nodes : []).map((node: any) => {
        if (String(node?.id || '') !== String(activeTab.nodeId)) return node
        const data = toRecord(node?.data)
        const cfg = toRecord(data.config)
        return {
          ...node,
          data: {
            ...data,
            config: {
              ...cfg,
              workflow_mode: 'embedded',
              embedded_workflow_enabled: true,
              embedded_workflow_nodes: serializeEmbeddedNodes(childSnapshot.nodes),
              embedded_workflow_edges: serializeEmbeddedEdges(childSnapshot.edges),
            },
          },
        }
      })
      await api.updatePipeline(String(id), {
        nodes: updatedMainNodes,
        edges: Array.isArray(mainSnapshot.edges) ? mainSnapshot.edges : [],
      })
      canvasSnapshotsRef.current[MAIN_CANVAS_TAB_KEY] = {
        ...mainSnapshot,
        nodes: cloneStructured(updatedMainNodes),
        isDirty: false,
      }
      canvasSnapshotsRef.current[activeCanvasTabKeyRef.current] = {
        ...childSnapshot,
        isDirty: false,
      }
      useWorkflowStore.setState((state) => ({ ...state, isDirty: false }))
      if (!silent) {
        notification.success({ message: 'Child workflow saved to Business Logic node', placement: 'bottomRight', duration: 2.5 })
      }
    } finally {
      setSaving(false)
    }
  }, [canvasTabs, captureActiveCanvasSnapshot, ensureMainSnapshot, id, savePipeline])

  const selectedNodeIds = useMemo(() => {
    const explicitlySelected = nodes
      .filter((node) => Boolean((node as any)?.selected))
      .map((node) => String(node.id))
    if (explicitlySelected.length > 0) return explicitlySelected
    return selectedNodeId ? [selectedNodeId] : []
  }, [nodes, selectedNodeId])
  const actionTargetNodeIds = selectedNodeIds
  const actionTargetCount = actionTargetNodeIds.length
  const activeCanvasTab = useMemo(
    () => canvasTabs.find((tab) => tab.key === activeCanvasTabKey) || { key: MAIN_CANVAS_TAB_KEY, kind: 'main', label: 'Main Canvas' } as WorkflowCanvasTab,
    [activeCanvasTabKey, canvasTabs]
  )
  const isChildCanvasTabActive = activeCanvasTab.kind === 'business'
  const isAnyNodeDragging = useMemo(
    () => nodes.some((node) => Boolean((node as any)?.dragging) || Boolean((node as any)?.resizing)),
    [nodes]
  )
  const actionTargetStats = useMemo(() => {
    const targetSet = new Set(actionTargetNodeIds)
    let enabledCount = 0
    let disabledCount = 0
    nodes.forEach((node) => {
      if (!targetSet.has(String(node.id))) return
      const raw = (
        node?.data?.config && typeof node.data.config === 'object'
          ? (node.data.config as Record<string, unknown>).node_enabled
          : undefined
      )
      if (parseNodeEnabledValue(raw)) enabledCount += 1
      else disabledCount += 1
    })
    return { enabledCount, disabledCount }
  }, [actionTargetNodeIds, nodes])

  const setNodeEnabledForSelection = useCallback((enabled: boolean) => {
    if (isExecuting || actionTargetNodeIds.length === 0) return
    const targetSet = new Set(actionTargetNodeIds)
    let changedCount = 0
    useWorkflowStore.setState((state) => ({
      ...state,
      nodes: state.nodes.map((node) => {
        if (!targetSet.has(String(node.id))) return node
        const currentConfig = (
          node.data?.config && typeof node.data.config === 'object'
            ? (node.data.config as Record<string, unknown>)
            : {}
        )
        const currentEnabled = parseNodeEnabledValue(currentConfig.node_enabled)
        if (currentEnabled === enabled) return node
        changedCount += 1
        return {
          ...node,
          data: {
            ...node.data,
            config: {
              ...currentConfig,
              node_enabled: enabled,
            },
          },
        }
      }),
      isDirty: changedCount > 0 ? true : state.isDirty,
    }))
    if (changedCount > 0) {
      notification.success({
        message: `${enabled ? 'Enabled' : 'Disabled'} ${changedCount} node${changedCount > 1 ? 's' : ''}`,
        placement: 'bottomRight',
        duration: 2,
      })
    }
  }, [actionTargetNodeIds, isExecuting])

  const updateHistoryFlags = useCallback(() => {
    const idx = historyIndexRef.current
    const len = historyRef.current.length
    setCanUndo(idx > 0)
    setCanRedo(idx >= 0 && idx < len - 1)
  }, [])

  const applyHistorySnapshot = useCallback((snapshot: CanvasHistorySnapshot) => {
    applyingHistoryRef.current = true
    const nextNodes = cloneStructured(snapshot.nodes)
    const nextEdges = cloneStructured(snapshot.edges)
    const currentSelectedNodeId = useWorkflowStore.getState().selectedNodeId
    const nextSelectedNodeId = nextNodes.some((node: any) => String(node?.id) === String(currentSelectedNodeId))
      ? currentSelectedNodeId
      : null
    useWorkflowStore.setState((state) => ({
      ...state,
      nodes: nextNodes,
      edges: nextEdges,
      selectedNodeId: nextSelectedNodeId,
      isDirty: true,
    }))
    window.setTimeout(() => {
      applyingHistoryRef.current = false
    }, 0)
  }, [])

  const pushHistorySnapshot = useCallback((snapshot: CanvasHistorySnapshot) => {
    const current = historyRef.current[historyIndexRef.current]
    if (current?.key === snapshot.key) {
      updateHistoryFlags()
      return
    }
    const base = historyRef.current.slice(0, historyIndexRef.current + 1)
    base.push(snapshot)
    const maxSnapshots = 60
    if (base.length > maxSnapshots) {
      base.splice(0, base.length - maxSnapshots)
    }
    historyRef.current = base
    historyIndexRef.current = base.length - 1
    updateHistoryFlags()
  }, [updateHistoryFlags])

  const handleUndo = useCallback(() => {
    if (activeCanvasTabKeyRef.current !== MAIN_CANVAS_TAB_KEY) return
    if (isExecuting || !historyReady) return
    const nextIndex = historyIndexRef.current - 1
    if (nextIndex < 0) return
    historyIndexRef.current = nextIndex
    const snapshot = historyRef.current[nextIndex]
    const routePipelineId = id ? String(id) : null
    if (snapshot && (!routePipelineId || snapshot.pipelineId === routePipelineId)) {
      applyHistorySnapshot(snapshot)
    }
    updateHistoryFlags()
  }, [applyHistorySnapshot, historyReady, id, isExecuting, updateHistoryFlags])

  const handleRedo = useCallback(() => {
    if (activeCanvasTabKeyRef.current !== MAIN_CANVAS_TAB_KEY) return
    if (isExecuting || !historyReady) return
    const nextIndex = historyIndexRef.current + 1
    if (nextIndex >= historyRef.current.length) return
    historyIndexRef.current = nextIndex
    const snapshot = historyRef.current[nextIndex]
    const routePipelineId = id ? String(id) : null
    if (snapshot && (!routePipelineId || snapshot.pipelineId === routePipelineId)) {
      applyHistorySnapshot(snapshot)
    }
    updateHistoryFlags()
  }, [applyHistorySnapshot, historyReady, id, isExecuting, updateHistoryFlags])

  useEffect(() => {
    let cancelled = false
    setHistoryReady(false)
    historyRef.current = []
    historyIndexRef.current = -1
    activeHistoryPipelineRef.current = null
    canvasSnapshotsRef.current = {}
    setCanvasTabs([{ key: MAIN_CANVAS_TAB_KEY, kind: 'main', label: 'Main Canvas' }])
    setActiveCanvasTabKey(MAIN_CANVAS_TAB_KEY)
    activeCanvasTabKeyRef.current = MAIN_CANVAS_TAB_KEY
    updateHistoryFlags()

    if (id) {
      const routePipelineId = String(id)
      const currentState = useWorkflowStore.getState()
      const currentPipelineId = currentState.pipeline?.id ? String(currentState.pipeline.id) : ''
      const preserveActiveExecutionView =
        currentState.isExecuting
        && currentPipelineId === routePipelineId
        && Boolean(currentState.executionId)

      if (preserveActiveExecutionView) {
        const loadedName = currentState.pipeline?.name || 'Untitled Pipeline'
        setPipelineName(loadedName)
        const initialSnapshot = createCanvasHistorySnapshot(
          routePipelineId,
          currentState.nodes || [],
          currentState.edges || [],
        )
        historyRef.current = [initialSnapshot]
        historyIndexRef.current = 0
        activeHistoryPipelineRef.current = routePipelineId
        canvasSnapshotsRef.current[MAIN_CANVAS_TAB_KEY] = {
          nodes: cloneStructured(currentState.nodes || []),
          edges: cloneStructured(currentState.edges || []),
          selectedNodeId: currentState.selectedNodeId ? String(currentState.selectedNodeId) : null,
          connectorType: (currentState.connectorType || 'smooth') as WorkflowConnectorType,
          isDirty: Boolean(currentState.isDirty),
        }
        updateHistoryFlags()
        setHistoryReady(true)
        void resumeExecutionForPipeline(routePipelineId)
      } else {
        resetCanvas()
        loadPipeline(id).then(() => {
          if (cancelled) return
          const state = useWorkflowStore.getState()
          const loadedName = state.pipeline?.name || 'Untitled Pipeline'
          setPipelineName(loadedName)
          const loadedPipelineId = state.pipeline?.id ? String(state.pipeline.id) : null
          if (loadedPipelineId && loadedPipelineId === routePipelineId) {
            const initialSnapshot = createCanvasHistorySnapshot(
              loadedPipelineId,
              state.nodes || [],
              state.edges || [],
            )
            historyRef.current = [initialSnapshot]
            historyIndexRef.current = 0
            activeHistoryPipelineRef.current = loadedPipelineId
            canvasSnapshotsRef.current[MAIN_CANVAS_TAB_KEY] = {
              nodes: cloneStructured(state.nodes || []),
              edges: cloneStructured(state.edges || []),
              selectedNodeId: state.selectedNodeId ? String(state.selectedNodeId) : null,
              connectorType: (state.connectorType || 'smooth') as WorkflowConnectorType,
              isDirty: Boolean(state.isDirty),
            }
            updateHistoryFlags()
            setHistoryReady(true)
          } else {
            setHistoryReady(false)
          }
          void resumeExecutionForPipeline(routePipelineId)
        }).catch(() => {
          if (cancelled) return
          setHistoryReady(false)
        })
      }
    }

    return () => {
      cancelled = true
    }
  }, [id, loadPipeline, resetCanvas, resumeExecutionForPipeline, updateHistoryFlags])

  useEffect(() => {
    if (pipeline?.name) setPipelineName(pipeline.name)
  }, [pipeline?.name])

  useEffect(() => {
    activeCanvasTabKeyRef.current = activeCanvasTabKey
  }, [activeCanvasTabKey])

  useEffect(() => {
    const handler = (event: Event) => {
      const customEvent = event as CustomEvent<OpenBusinessCanvasTabDetail>
      const detail = customEvent?.detail || {}
      const detailPipelineId = String(detail.pipelineId || '').trim()
      const detailNodeId = String(detail.nodeId || '').trim()
      const routePipelineId = String(id || '').trim()
      if (!detailNodeId) return
      if (detailPipelineId && routePipelineId && detailPipelineId !== routePipelineId) return
      openBusinessCanvasTab(detailNodeId)
    }
    window.addEventListener('pipeline:open-business-canvas-tab', handler as EventListener)
    return () => {
      window.removeEventListener('pipeline:open-business-canvas-tab', handler as EventListener)
    }
  }, [id, openBusinessCanvasTab])

  useEffect(() => {
    if (!scheduleModal) return
    const cron = normalizeCronExpr(pipeline?.schedule_cron || DEFAULT_SCHEDULE_CRON)
    const runtime = extractExecutionRuntimeFromNodes(nodes)
    form.setFieldsValue({
      enabled: pipeline?.schedule_enabled ?? false,
      schedulePreset: detectSchedulePreset(cron),
      cron,
      executionMode: runtime.executionMode,
      batchSize: runtime.batchSize,
      incrementalField: runtime.incrementalField,
      streamingIntervalSeconds: runtime.streamingIntervalSeconds,
      streamingMaxBatches: runtime.streamingMaxBatches,
    })
  }, [scheduleModal, pipeline?.schedule_enabled, pipeline?.schedule_cron, nodes, form])

  useEffect(() => {
    if (!scheduleModal) return
    const selected = SCHEDULE_PRESETS.find((preset) => preset.value === selectedSchedulePreset)
    if (selected && selected.value !== CUSTOM_SCHEDULE_VALUE) {
      form.setFieldValue('cron', selected.cron)
    }
  }, [scheduleModal, selectedSchedulePreset, form])

  useEffect(() => {
    if (!historyReady || isExecuting || applyingHistoryRef.current) return
    if (activeCanvasTabKey !== MAIN_CANVAS_TAB_KEY) return
    if (isAnyNodeDragging) return
    const routePipelineId = id ? String(id) : null
    const pipelineId = pipeline?.id ? String(pipeline.id) : null
    if (!routePipelineId || pipelineId !== routePipelineId) return
    const timer = window.setTimeout(() => {
      const snapshot = createCanvasHistorySnapshot(pipelineId, nodes, edges)
      if (activeHistoryPipelineRef.current !== pipelineId) {
        activeHistoryPipelineRef.current = pipelineId
        historyRef.current = [snapshot]
        historyIndexRef.current = 0
        updateHistoryFlags()
        return
      }
      pushHistorySnapshot(snapshot)
    }, 120)
    return () => window.clearTimeout(timer)
  }, [activeCanvasTabKey, edges, historyReady, id, isExecuting, isAnyNodeDragging, nodes, pipeline?.id, pushHistorySnapshot, updateHistoryFlags])

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null
      const active = document.activeElement as HTMLElement | null
      const tag = String(target?.tagName || '').toUpperCase()
      const withMod = event.metaKey || event.ctrlKey
      const key = event.key.toLowerCase()

      if (withMod && key === 's') {
        event.preventDefault()
        if (!isExecuting && !saving) {
          void saveActiveCanvas()
        }
        return
      }

      const isMonacoActive =
        !!active?.closest('.monaco-editor')
        || active?.classList?.contains('inputarea')
        || !!target?.closest('.monaco-editor')
        || !!target?.closest('.monaco-editor *')
      const isTyping =
        !!target?.isContentEditable
        || tag === 'INPUT'
        || tag === 'TEXTAREA'
        || tag === 'SELECT'
        || isMonacoActive
      if (isTyping) return
      if (!withMod) return
      if (key === 'z') {
        event.preventDefault()
        if (event.shiftKey) {
          handleRedo()
        } else {
          handleUndo()
        }
      } else if (key === 'y') {
        event.preventDefault()
        handleRedo()
      } else if (key === 'd') {
        event.preventDefault()
        if (!isExecuting && selectedNodeId) {
          duplicateNode(selectedNodeId)
        }
      } else if (key === 'enter' && !event.shiftKey) {
        event.preventDefault()
        if (event.repeat) return
        if (!isExecuting) {
          if (activeCanvasTabKeyRef.current !== MAIN_CANVAS_TAB_KEY) {
            switchCanvasTab(MAIN_CANVAS_TAB_KEY)
          }
          resetRuntimeStateAcrossCanvases()
          void executePipeline()
        }
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => {
      window.removeEventListener('keydown', onKeyDown)
    }
  }, [duplicateNode, executePipeline, handleUndo, handleRedo, isExecuting, resetRuntimeStateAcrossCanvases, saveActiveCanvas, saving, selectedNodeId, switchCanvasTab])

  // Auto-save on dirty (main + child canvas)
  useEffect(() => {
    if (!editorAutoSaveSettings.enabled) return
    if (isExecuting || saving) return
    if (!isDirty) return
    const timer = setTimeout(async () => {
      await saveActiveCanvas({ silent: true })
    }, editorAutoSaveSettings.intervalMs)
    return () => clearTimeout(timer)
  }, [
    activeCanvasTabKey,
    editorAutoSaveSettings.enabled,
    editorAutoSaveSettings.intervalMs,
    isDirty,
    isExecuting,
    saveActiveCanvas,
    saving,
  ])

  const handleSave = async () => {
    await saveActiveCanvas()
  }

  const handleRun = async () => {
    if (isExecuting) return
    if (activeCanvasTabKeyRef.current !== MAIN_CANVAS_TAB_KEY) {
      switchCanvasTab(MAIN_CANVAS_TAB_KEY)
    }
    resetRuntimeStateAcrossCanvases()
    await executePipeline()
  }

  const handleAbort = async () => {
    if (!isExecuting) return
    await abortExecution()
  }

  const handleResetCanvasTop = useCallback(() => {
    if (isExecuting) return
    if (activeCanvasTabKeyRef.current !== MAIN_CANVAS_TAB_KEY) {
      switchCanvasTab(MAIN_CANVAS_TAB_KEY)
    }
    Modal.confirm({
      title: 'Reset pipeline to default state?',
      content: 'This will discard unsaved changes and restore the last saved pipeline state in non-run mode.',
      okText: 'Reset',
      okButtonProps: { danger: true },
      cancelText: 'Cancel',
      onOk: async () => {
        const routePipelineId = String(id || '').trim()
        const isLocalOnly = routePipelineId.startsWith('p_')

        if (!routePipelineId || isLocalOnly) {
          const state = useWorkflowStore.getState()
          const runtimeClearedNodes = clearNodeRuntimeState(state.nodes)
          useWorkflowStore.setState((state) => ({
            ...state,
            nodes: runtimeClearedNodes,
            selectedNodeId: null,
            isExecuting: false,
            executionId: null,
            executionAbortRequested: false,
            executionLogs: [],
            showLogs: false,
            isDirty: false,
          }))
          setSelectedNode(null)
          const localSnapshot = createCanvasHistorySnapshot(
            routePipelineId || null,
            runtimeClearedNodes,
            state.edges || [],
          )
          historyRef.current = [localSnapshot]
          historyIndexRef.current = 0
          activeHistoryPipelineRef.current = routePipelineId || null
          canvasSnapshotsRef.current = {
            [MAIN_CANVAS_TAB_KEY]: {
              nodes: cloneStructured(runtimeClearedNodes),
              edges: cloneStructured(state.edges || []),
              selectedNodeId: null,
              connectorType: (state.connectorType || 'smooth') as WorkflowConnectorType,
              isDirty: false,
            },
          }
          setCanvasTabs([{ key: MAIN_CANVAS_TAB_KEY, kind: 'main', label: 'Main Canvas' }])
          setActiveCanvasTabKey(MAIN_CANVAS_TAB_KEY)
          activeCanvasTabKeyRef.current = MAIN_CANVAS_TAB_KEY
          setHistoryReady(Boolean(routePipelineId))
          updateHistoryFlags()
          notification.success({
            message: 'Pipeline reset to initial non-run state.',
            placement: 'bottomRight',
            duration: 2,
          })
          return
        }

        await loadPipeline(routePipelineId)
        const state = useWorkflowStore.getState()
        const loadedPipelineId = String(state.pipeline?.id || '').trim()
        if (!loadedPipelineId || loadedPipelineId !== routePipelineId) {
          notification.error({
            message: 'Reset failed',
            description: 'Could not restore saved pipeline state.',
            placement: 'bottomRight',
            duration: 3,
          })
          return
        }
        const runtimeClearedNodes = clearNodeRuntimeState(state.nodes)
        useWorkflowStore.setState((s) => ({
          ...s,
          nodes: runtimeClearedNodes,
          selectedNodeId: null,
          isExecuting: false,
          executionId: null,
          executionAbortRequested: false,
          executionLogs: [],
          showLogs: false,
          isDirty: false,
        }))
        setSelectedNode(null)
        const baselineSnapshot = createCanvasHistorySnapshot(
          loadedPipelineId,
          runtimeClearedNodes,
          state.edges || [],
        )
        historyRef.current = [baselineSnapshot]
        historyIndexRef.current = 0
        activeHistoryPipelineRef.current = loadedPipelineId
        canvasSnapshotsRef.current = {
          [MAIN_CANVAS_TAB_KEY]: {
            nodes: cloneStructured(runtimeClearedNodes),
            edges: cloneStructured(state.edges || []),
            selectedNodeId: null,
            connectorType: (state.connectorType || 'smooth') as WorkflowConnectorType,
            isDirty: false,
          },
        }
        setCanvasTabs([{ key: MAIN_CANVAS_TAB_KEY, kind: 'main', label: 'Main Canvas' }])
        setActiveCanvasTabKey(MAIN_CANVAS_TAB_KEY)
        activeCanvasTabKeyRef.current = MAIN_CANVAS_TAB_KEY
        setHistoryReady(true)
        updateHistoryFlags()
        notification.success({
          message: 'Pipeline reset to saved default state.',
          placement: 'bottomRight',
          duration: 2,
        })
      },
    })
  }, [id, isExecuting, loadPipeline, setSelectedNode, switchCanvasTab, updateHistoryFlags])

  const applyImportedGraph = useCallback((payload: any, fallbackName: string) => {
    const source = payload?.pipeline && typeof payload.pipeline === 'object' ? payload.pipeline : payload
    const rawNodes = Array.isArray(source?.nodes) ? source.nodes : []
    const rawEdges = Array.isArray(source?.edges) ? source.edges : []
    if (!rawNodes.length && !rawEdges.length) {
      throw new Error('Import JSON must include `nodes` and `edges` arrays.')
    }

    const normalizedNodes = rawNodes.map((rawNode: any, index: number) => {
      const nodeType = String(rawNode?.data?.nodeType || '').trim()
      const definition = getNodeDef(nodeType)
      const rawConfig = (
        rawNode?.data?.config && typeof rawNode.data.config === 'object'
          ? rawNode.data.config
          : {}
      )
      const nodeId = String(rawNode?.id || '').trim()
      if (!nodeId) {
        throw new Error(`Imported node at index ${index + 1} is missing id.`)
      }
      if (!nodeType) {
        throw new Error(`Imported node \`${nodeId}\` is missing data.nodeType.`)
      }
      return {
        ...rawNode,
        id: nodeId,
        type: 'etlNode',
        data: {
          ...(rawNode?.data || {}),
          nodeType,
          label: String(rawNode?.data?.label || definition?.label || `Node ${index + 1}`),
          definition: definition || rawNode?.data?.definition,
          config: rawConfig,
          status: 'idle',
          executionRows: undefined,
          executionProcessedRows: undefined,
          executionValidatedRows: undefined,
          executionSampleInput: undefined,
          executionSampleOutput: undefined,
          executionError: undefined,
        },
        selected: false,
        dragging: false,
      }
    })

    const nodeIds = new Set<string>()
    normalizedNodes.forEach((node: any) => {
      const nodeId = String(node?.id || '').trim()
      if (!nodeId) return
      if (nodeIds.has(nodeId)) {
        throw new Error(`Duplicate node id found in import: \`${nodeId}\``)
      }
      nodeIds.add(nodeId)
    })

    const normalizedEdges = rawEdges.map((rawEdge: any, index: number) => {
      const sourceId = String(rawEdge?.source || '').trim()
      const targetId = String(rawEdge?.target || '').trim()
      if (!sourceId || !targetId) {
        throw new Error(`Imported edge at index ${index + 1} is missing source/target.`)
      }
      if (!nodeIds.has(sourceId) || !nodeIds.has(targetId)) {
        throw new Error(`Imported edge \`${rawEdge?.id || `${sourceId}->${targetId}`}\` references unknown node.`)
      }
      const edgeId = String(rawEdge?.id || `${sourceId}-${targetId}-${index + 1}`).trim()
      return {
        ...rawEdge,
        id: edgeId,
        source: sourceId,
        target: targetId,
        reconnectable: rawEdge?.reconnectable ?? true,
        updatable: rawEdge?.updatable ?? true,
        interactionWidth: Number(rawEdge?.interactionWidth || 44),
      }
    })

    const importedConnectorType = resolveConnectorTypeFromEdges(normalizedEdges)
    const importedEdges = applyConnectorTypeToEdges(normalizedEdges, importedConnectorType)
    const importedName = String(source?.name || payload?.name || '').trim()

    useWorkflowStore.setState((state) => ({
      ...state,
      nodes: normalizedNodes,
      edges: importedEdges,
      connectorType: importedConnectorType,
      selectedNodeId: null,
      isDirty: true,
    }))
    canvasSnapshotsRef.current = {
      [MAIN_CANVAS_TAB_KEY]: {
        nodes: cloneStructured(normalizedNodes),
        edges: cloneStructured(importedEdges),
        connectorType: importedConnectorType,
        selectedNodeId: null,
        isDirty: true,
      },
    }
    setCanvasTabs([{ key: MAIN_CANVAS_TAB_KEY, kind: 'main', label: 'Main Canvas' }])
    setActiveCanvasTabKey(MAIN_CANVAS_TAB_KEY)
    activeCanvasTabKeyRef.current = MAIN_CANVAS_TAB_KEY

    if (importedName) {
      setPipelineName(importedName)
    } else if (!String(fallbackName || '').trim()) {
      setPipelineName('Imported Pipeline')
    }
  }, [])

  const handleImportFileChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    event.target.value = ''
    if (!file) return
    if (isExecuting) {
      notification.warning({
        message: 'Cannot import while pipeline is executing.',
        placement: 'bottomRight',
        duration: 3,
      })
      return
    }

    const reader = new FileReader()
    reader.onload = () => {
      try {
        const rawText = String(reader.result || '').trim()
        if (!rawText) {
          throw new Error('Selected file is empty.')
        }
        const parsed = JSON.parse(rawText)
        Modal.confirm({
          title: 'Import Pipeline JSON',
          content: 'This will replace current canvas nodes and edges.',
          okText: 'Import',
          cancelText: 'Cancel',
          okButtonProps: { danger: true },
          onOk: () => {
            try {
              applyImportedGraph(parsed, pipelineName)
              notification.success({
                message: 'Pipeline imported successfully.',
                placement: 'bottomRight',
                duration: 2,
              })
            } catch (error: any) {
              notification.error({
                message: 'Import failed',
                description: String(error?.message || 'Invalid pipeline JSON'),
                placement: 'bottomRight',
                duration: 4,
              })
            }
          },
        })
      } catch (error: any) {
        notification.error({
          message: 'Import failed',
          description: String(error?.message || 'Invalid JSON file'),
          placement: 'bottomRight',
          duration: 4,
        })
      }
    }
    reader.onerror = () => {
      notification.error({
        message: 'Import failed',
        description: 'Unable to read selected JSON file.',
        placement: 'bottomRight',
        duration: 4,
      })
    }
    reader.readAsText(file)
  }, [applyImportedGraph, isExecuting, pipelineName])

  const triggerImportPicker = useCallback(() => {
    if (activeCanvasTabKey !== MAIN_CANVAS_TAB_KEY) {
      notification.info({
        message: 'Switch to Main Canvas to import pipeline JSON.',
        placement: 'bottomRight',
        duration: 2,
      })
      return
    }
    if (isExecuting) return
    importFileInputRef.current?.click()
  }, [activeCanvasTabKey, isExecuting])

  const hasError = executionLogs.some(l => l.status === 'error')
  const hasSuccess = !isExecuting && executionLogs.length > 0 && !hasError

  const moreMenuItems = [
    { key: 'schedule', icon: <ScheduleOutlined />, label: 'Configure Schedule' },
    { key: 'history', icon: <HistoryOutlined />, label: 'Execution History' },
    { key: 'import', icon: <UploadOutlined />, label: 'Import from JSON' },
    { key: 'export', icon: <CodeOutlined />, label: 'Export as JSON' },
    { type: 'divider' as const },
    { key: 'delete', icon: <CloseCircleFilled />, label: 'Delete Pipeline', danger: true },
  ]

  return (
    <ReactFlowProvider>
      <div style={{ height: '100vh', background: 'var(--app-shell-bg)', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        {/* ── TOP TOOLBAR ───────────────────────────────────────────────────── */}
        <div style={{
          background: 'var(--app-panel-bg)',
          borderBottom: '1px solid var(--app-border)',
          padding: '0 16px',
          height: 52,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'flex-start',
          flexShrink: 0,
          zIndex: 10,
        }}>
          {/* Left: back + name */}
          <Space size={12}>
            <Tooltip title="Back to pipelines">
              <Button
                type="text"
                icon={<ArrowLeftOutlined />}
                style={{ color: 'var(--app-text-subtle)' }}
                onClick={() => navigate('/pipelines')}
              />
            </Tooltip>
            <div style={{ width: 1, height: 20, background: 'var(--app-border)' }} />
            <Input
              value={pipelineName}
              onChange={e => setPipelineName(e.target.value)}
              onBlur={async () => {
                if (pipeline?.id && pipelineName !== pipeline?.name) {
                  await api.updatePipeline(pipeline.id, { name: pipelineName })
                  notification.success({ message: 'Renamed!', placement: 'bottomRight', duration: 2 })
                }
              }}
              style={{
                background: 'transparent',
                border: '1px solid transparent',
                color: 'var(--app-text)',
                fontWeight: 600,
                fontSize: 14,
                padding: '2px 8px',
                width: 280,
                borderRadius: 6,
              }}
              onMouseEnter={e => (e.currentTarget.style.borderColor = 'var(--app-border-strong)')}
              onMouseLeave={e => (e.currentTarget.style.borderColor = 'transparent')}
              onFocus={e => (e.currentTarget.style.borderColor = '#6366f1')}
            />

          </Space>

          {/* Left-aligned core actions */}
          <Space size={8} style={{ marginLeft: 12 }}>
            <Tooltip title={isExecuting ? (executionAbortRequested ? 'Aborting…' : 'Abort') : 'Execute (Ctrl/Cmd + Enter) — runs from Main Canvas'}>
              <Button
                icon={
                  isExecuting
                    ? (executionAbortRequested ? <LoadingOutlined spin /> : <StopOutlined />)
                    : <PlayCircleOutlined />
                }
                onClick={isExecuting ? handleAbort : handleRun}
                disabled={executionAbortRequested}
                aria-label={isExecuting ? (executionAbortRequested ? 'Aborting' : 'Abort') : 'Execute'}
                style={{
                  background: 'var(--app-card-bg)',
                  border: isExecuting ? '1px solid rgba(239,68,68,0.45)' : '1px solid rgba(34,197,94,0.55)',
                  color: isExecuting ? '#ef4444' : '#22c55e',
                  boxShadow: isExecuting ? 'none' : '0 0 0 1px rgba(34,197,94,0.12) inset',
                }}
              />
            </Tooltip>

            <Tooltip title={isChildCanvasTabActive ? 'Save Child Workflow (Ctrl/Cmd + S)' : 'Save (Ctrl/Cmd + S)'}>
              <Button
                icon={<SaveOutlined />}
                loading={saving}
                onClick={handleSave}
                aria-label="Save"
                style={{
                  background: 'var(--app-card-bg)',
                  border: saving
                    ? '1px solid rgba(99,102,241,0.55)'
                    : isDirty
                      ? '1px solid rgba(245,158,11,0.65)'
                      : '1px solid rgba(34,197,94,0.55)',
                  color: saving ? '#6366f1' : isDirty ? '#f59e0b' : '#22c55e',
                  boxShadow: saving
                    ? '0 0 0 1px rgba(99,102,241,0.12) inset'
                    : isDirty
                      ? '0 0 0 1px rgba(245,158,11,0.14) inset'
                      : '0 0 0 1px rgba(34,197,94,0.12) inset',
                }}
              />
            </Tooltip>

            <Tooltip title="Duplicate selected node (Ctrl/Cmd + D)">
              <Button
                icon={<CopyOutlined />}
                onClick={() => selectedNodeId && duplicateNode(selectedNodeId)}
                disabled={!selectedNodeId || isExecuting}
                style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)' }}
              />
            </Tooltip>
            <Tooltip title="Undo (Ctrl/Cmd + Z)">
              <Button
                icon={<UndoOutlined />}
                onClick={handleUndo}
                disabled={!canUndo || isExecuting || isChildCanvasTabActive}
                style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)' }}
              />
            </Tooltip>
            <Tooltip title="Redo (Ctrl/Cmd + Shift + Z)">
              <Button
                icon={<RedoOutlined />}
                onClick={handleRedo}
                disabled={!canRedo || isExecuting || isChildCanvasTabActive}
                style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)' }}
              />
            </Tooltip>
            <Tooltip title="Reset Canvas">
              <Button
                icon={<ClearOutlined />}
                onClick={handleResetCanvasTop}
                disabled={isExecuting}
                style={{ background: 'var(--app-card-bg)', border: '1px solid rgba(239,68,68,0.45)', color: '#ef4444' }}
              />
            </Tooltip>
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
            <Tooltip title="Canvas widget style">
              <Select<PipelineCanvasWidgetStyle>
                value={canvasWidgetStyle}
                onChange={(value) => setCanvasWidgetStyle(value)}
                options={PIPELINE_CANVAS_WIDGET_STYLE_OPTIONS}
                size="small"
                style={{ width: 122 }}
                dropdownStyle={{ background: 'var(--app-card-bg)' }}
              />
            </Tooltip>

            {actionTargetCount > 0 && (
              <Space
                size={6}
                style={{
                  padding: '2px 8px',
                  borderRadius: 8,
                  border: '1px solid var(--app-border-strong)',
                  background: 'var(--app-card-bg)',
                }}
              >
                <Tag style={{ marginInlineEnd: 0, borderRadius: 6, background: 'var(--app-panel-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)' }}>
                  {actionTargetCount} selected
                </Tag>
                <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Enabled</Text>
                <Tooltip title={actionTargetStats.disabledCount === 0 ? 'Disable selected nodes' : 'Enable selected nodes'}>
                  <Switch
                    size="small"
                    checked={actionTargetStats.disabledCount === 0}
                    disabled={isExecuting}
                    onChange={(checked) => setNodeEnabledForSelection(checked)}
                  />
                </Tooltip>
              </Space>
            )}

            {/* Execution status badge */}
            {hasError && (
              <Tag
                icon={<CloseCircleFilled />}
                style={{ background: '#ef444415', border: '1px solid #ef444430', color: '#ef4444', borderRadius: 6 }}
              >
                Failed
              </Tag>
            )}
            {hasSuccess && (
              <Tag
                icon={<CheckCircleFilled />}
                style={{ background: '#22c55e15', border: '1px solid #22c55e30', color: '#22c55e', borderRadius: 6 }}
              >
                {executionLogs.reduce((s, l) => s + (l.rows || 0), 0).toLocaleString()} rows
              </Tag>
            )}

            {isDirty && !saving && (
              <Tag style={{ background: '#f59e0b15', border: '1px solid #f59e0b30', color: '#f59e0b', fontSize: 11, borderRadius: 4 }}>
                Unsaved
              </Tag>
            )}
            {saving && <LoadingOutlined style={{ color: '#6366f1', fontSize: 12 }} spin />}
            {!isDirty && !saving && (
              <Tag style={{ background: '#22c55e10', border: '1px solid #22c55e20', color: '#22c55e', fontSize: 11, borderRadius: 4 }}>
                Saved
              </Tag>
            )}
          </Space>

          {/* Right-side status/meta actions */}
          <Space size={8} style={{ marginLeft: 'auto' }}>
            <Dropdown
              menu={{
                items: moreMenuItems,
                onClick: ({ key }) => {
                  if (key === 'schedule') {
                    if (isChildCanvasTabActive) {
                      notification.info({
                        message: 'Switch to Main Canvas to configure schedule.',
                        placement: 'bottomRight',
                        duration: 2,
                      })
                    } else {
                      setScheduleModal(true)
                    }
                  }
                  else if (key === 'history') navigate('/executions')
                  else if (key === 'import') triggerImportPicker()
                  else if (key === 'export') {
                    const state = useWorkflowStore.getState()
                    const blob = new Blob([JSON.stringify({ nodes: state.nodes, edges: state.edges }, null, 2)], { type: 'application/json' })
                    const url = URL.createObjectURL(blob)
                    const a = document.createElement('a')
                    a.href = url
                    a.download = `${pipelineName}.json`
                    a.click()
                  }
                },
                style: { background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)' }
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

        <div
          style={{
            height: 40,
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            padding: '0 12px',
            background: 'var(--app-panel-bg)',
            borderBottom: '1px solid var(--app-border)',
            flexShrink: 0,
          }}
        >
          {canvasTabs.map((tab) => {
            const active = tab.key === activeCanvasTabKey
            return (
              <div
                key={tab.key}
                role="button"
                tabIndex={0}
                onClick={() => switchCanvasTab(tab.key)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault()
                    switchCanvasTab(tab.key)
                  }
                }}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                  height: 28,
                  padding: '0 10px',
                  borderRadius: 8,
                  border: active ? '1px solid #6366f1aa' : '1px solid var(--app-border-strong)',
                  background: active ? '#6366f11a' : 'var(--app-card-bg)',
                  color: active ? '#c4b5fd' : 'var(--app-text-muted)',
                  cursor: 'pointer',
                  userSelect: 'none',
                  maxWidth: 280,
                }}
              >
                <span style={{ fontSize: 12, fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {tab.kind === 'main' ? tab.label : `${tab.label}`}
                </span>
                {tab.kind === 'business' ? (
                  <Button
                    type="text"
                    size="small"
                    icon={<CloseOutlined />}
                    onClick={(event) => {
                      event.stopPropagation()
                      closeCanvasTab(tab.key)
                    }}
                    style={{
                      minWidth: 16,
                      width: 16,
                      height: 16,
                      padding: 0,
                      color: active ? '#ddd6fe' : 'var(--app-text-subtle)',
                    }}
                  />
                ) : null}
              </div>
            )
          })}
          {isChildCanvasTabActive ? (
            <Tag style={{ marginInlineStart: 4, marginInlineEnd: 0, borderRadius: 6, background: '#6366f112', border: '1px solid #6366f155', color: '#a5b4fc' }}>
              Child Workflow Canvas
            </Tag>
          ) : null}
        </div>

        {/* ── MAIN EDITOR AREA ──────────────────────────────────────────────── */}
        <div style={{ flex: 1, display: 'flex', overflow: 'hidden', position: 'relative' }}>
          {/* Left: Node Palette */}
          <NodePalette />

          {/* Center: Canvas + Execution Panel */}
          <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
            <WorkflowCanvas />
            <ExecutionPanel />
          </div>

          {/* Right: Config Drawer (inline, not overlay) */}
          <ConfigDrawerErrorBoundary resetKey={selectedNodeId ? String(selectedNodeId) : null}>
            <ConfigDrawer
              open={!!selectedNodeId}
              onClose={() => setSelectedNode(null)}
            />
          </ConfigDrawerErrorBoundary>
        </div>
      </div>

      <input
        ref={importFileInputRef}
        type="file"
        accept=".json,application/json"
        style={{ display: 'none' }}
        onChange={handleImportFileChange}
      />

      {/* Schedule Modal */}
      <Modal
        title={<span style={{ color: 'var(--app-text)' }}>⏰ Schedule Pipeline</span>}
        open={scheduleModal}
        onOk={async () => {
          if (!pipeline?.id) return
          try {
            const values = await form.validateFields()
            const enabled = Boolean(values.enabled)
            const selected = SCHEDULE_PRESETS.find((preset) => preset.value === values.schedulePreset)
            const cron = selected && selected.value !== CUSTOM_SCHEDULE_VALUE
              ? selected.cron
              : normalizeCronExpr(String(values.cron || ''))
            const runtimeModeRaw = String(values.executionMode || DEFAULT_EXECUTION_MODE).toLowerCase()
            const runtimeMode: ExecutionMode = (['batch', 'incremental', 'streaming'].includes(runtimeModeRaw)
              ? runtimeModeRaw
              : DEFAULT_EXECUTION_MODE) as ExecutionMode
            const runtime = {
              executionMode: runtimeMode,
              batchSize: toBoundedInt(values.batchSize, 5000, 1, 1_000_000),
              incrementalField: String(values.incrementalField || '').trim(),
              streamingIntervalSeconds: toBoundedInt(values.streamingIntervalSeconds, 5, 1, 3600),
              streamingMaxBatches: toBoundedInt(values.streamingMaxBatches, 10, 1, 10_000),
            }
            const patchedNodes = applyExecutionRuntimeToTriggerNodes(nodes, runtime)

            await api.updatePipeline(pipeline.id, {
              schedule_enabled: enabled,
              schedule_cron: cron,
              nodes: patchedNodes,
            })
            useWorkflowStore.setState((state) => ({
              pipeline: state.pipeline
                ? {
                  ...state.pipeline,
                  schedule_enabled: enabled,
                  schedule_cron: cron,
                  execution_mode: runtime.executionMode,
                  batch_size: runtime.batchSize,
                  incremental_field: runtime.incrementalField,
                  streaming_interval_seconds: runtime.streamingIntervalSeconds,
                  streaming_max_batches: runtime.streamingMaxBatches,
                }
                : state.pipeline,
              nodes: patchedNodes,
            }))
            notification.success({
              message: enabled ? `Schedule saved: ${cron}` : 'Schedule disabled',
              placement: 'bottomRight',
            })
            setScheduleModal(false)
          } catch (error: any) {
            const detail = error?.response?.data?.detail
            const message = typeof detail === 'string'
              ? detail
              : error?.message || 'Failed to save schedule'
            notification.error({ message, placement: 'bottomRight' })
          }
        }}
        onCancel={() => setScheduleModal(false)}
        okText="Save Schedule"
        okButtonProps={{ style: { background: 'linear-gradient(135deg, #6366f1, #a855f7)', border: 'none' } }}
        styles={{ content: { background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)' }, header: { background: 'var(--app-card-bg)', borderBottom: '1px solid var(--app-border-strong)' }, footer: { background: 'var(--app-card-bg)', borderTop: '1px solid var(--app-border-strong)' }, mask: { backdropFilter: 'blur(4px)' } }}
      >
        <Form
          form={form}
          layout="vertical"
          style={{ marginTop: 16 }}
          initialValues={{
            enabled: pipeline?.schedule_enabled ?? false,
            schedulePreset: detectSchedulePreset(pipeline?.schedule_cron || DEFAULT_SCHEDULE_CRON),
            cron: pipeline?.schedule_cron || DEFAULT_SCHEDULE_CRON,
            executionMode: extractExecutionRuntimeFromNodes(nodes).executionMode,
            batchSize: extractExecutionRuntimeFromNodes(nodes).batchSize,
            incrementalField: extractExecutionRuntimeFromNodes(nodes).incrementalField,
            streamingIntervalSeconds: extractExecutionRuntimeFromNodes(nodes).streamingIntervalSeconds,
            streamingMaxBatches: extractExecutionRuntimeFromNodes(nodes).streamingMaxBatches,
          }}
        >
          <Form.Item
            name="enabled"
            valuePropName="checked"
            label={<span style={{ color: 'var(--app-text-muted)' }}>Enable Schedule</span>}
          >
            <Switch />
          </Form.Item>
          <Form.Item
            name="schedulePreset"
            label={<span style={{ color: 'var(--app-text-muted)' }}>Schedule Pattern</span>}
            rules={[{ required: true, message: 'Select a schedule pattern' }]}
          >
            <Select
              options={SCHEDULE_PRESETS.map((preset) => ({
                value: preset.value,
                label: preset.label,
              }))}
              style={{ width: '100%' }}
              dropdownStyle={{ background: 'var(--app-card-bg)' }}
              placeholder="Select schedule"
            />
          </Form.Item>
          {selectedSchedulePreset === CUSTOM_SCHEDULE_VALUE && (
            <Form.Item
              name="cron"
              label={<span style={{ color: 'var(--app-text-muted)' }}>Custom Cron Expression</span>}
              rules={[
                { required: true, message: 'Cron expression is required' },
                {
                  validator: async (_, value) => {
                    const normalized = normalizeCronExpr(String(value || ''))
                    if (!normalized) {
                      throw new Error('Cron expression is required')
                    }
                    if (normalized.split(' ').length !== 5) {
                      throw new Error('Cron expression must have exactly 5 fields')
                    }
                  },
                },
              ]}
            >
              <Input
                placeholder="0 * * * *"
                style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)', fontFamily: 'monospace' }}
              />
            </Form.Item>
          )}
          {selectedSchedulePreset !== CUSTOM_SCHEDULE_VALUE && (
            <Form.Item name="cron" hidden>
              <Input />
            </Form.Item>
          )}
          <div style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '12px 16px' }}>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
              Scheduled executions run in the backend scheduler even when this page is closed.
            </Text>
          </div>
          <div style={{ height: 12 }} />
          <div style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '12px 16px' }}>
            <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 600 }}>
              ETL Runtime Mode
            </Text>
            <Form.Item
              style={{ marginTop: 10, marginBottom: 10 }}
              name="executionMode"
              label={<span style={{ color: 'var(--app-text-muted)' }}>Execution Mode</span>}
              rules={[{ required: true, message: 'Select execution mode' }]}
            >
              <Select
                options={EXECUTION_MODE_OPTIONS}
                style={{ width: '100%' }}
                dropdownStyle={{ background: 'var(--app-card-bg)' }}
              />
            </Form.Item>
            <Form.Item
              style={{ marginBottom: 10 }}
              name="batchSize"
              label={<span style={{ color: 'var(--app-text-muted)' }}>Batch Size (rows/chunk)</span>}
              rules={[{ required: true, message: 'Batch size is required' }]}
            >
              <Select
                options={[
                  { value: 1000, label: '1,000' },
                  { value: 5000, label: '5,000' },
                  { value: 10000, label: '10,000' },
                  { value: 50000, label: '50,000' },
                ]}
                dropdownStyle={{ background: 'var(--app-card-bg)' }}
              />
            </Form.Item>
            {(selectedExecutionMode === 'incremental' || selectedExecutionMode === 'streaming') && (
              <Form.Item
                style={{ marginBottom: 10 }}
                name="incrementalField"
                label={<span style={{ color: 'var(--app-text-muted)' }}>Incremental Field (watermark)</span>}
                rules={[{ required: true, message: 'Incremental field is required for incremental/streaming mode' }]}
              >
                <Input
                  placeholder="updated_at, id, timestamp"
                  style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }}
                />
              </Form.Item>
            )}
            {selectedExecutionMode === 'streaming' && (
              <Space style={{ width: '100%' }} direction="vertical" size={0}>
                <Form.Item
                  style={{ marginBottom: 10 }}
                  name="streamingIntervalSeconds"
                  label={<span style={{ color: 'var(--app-text-muted)' }}>Streaming Interval (seconds)</span>}
                  rules={[{ required: true, message: 'Interval is required' }]}
                >
                  <Select
                    options={[
                      { value: 1, label: '1 sec' },
                      { value: 2, label: '2 sec' },
                      { value: 5, label: '5 sec' },
                      { value: 10, label: '10 sec' },
                      { value: 30, label: '30 sec' },
                    ]}
                    dropdownStyle={{ background: 'var(--app-card-bg)' }}
                  />
                </Form.Item>
                <Form.Item
                  style={{ marginBottom: 0 }}
                  name="streamingMaxBatches"
                  label={<span style={{ color: 'var(--app-text-muted)' }}>Streaming Max Batches / Run</span>}
                  rules={[{ required: true, message: 'Max batches is required' }]}
                >
                  <Select
                    options={[
                      { value: 5, label: '5 batches' },
                      { value: 10, label: '10 batches' },
                      { value: 20, label: '20 batches' },
                      { value: 50, label: '50 batches' },
                    ]}
                    dropdownStyle={{ background: 'var(--app-card-bg)' }}
                  />
                </Form.Item>
              </Space>
            )}
          </div>
        </Form>
      </Modal>
    </ReactFlowProvider>
  )
}
