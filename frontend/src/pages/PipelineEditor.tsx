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
  UndoOutlined, RedoOutlined, UploadOutlined
} from '@ant-design/icons'
import { ReactFlowProvider } from 'reactflow'
import { useWorkflowStore } from '../store'
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

type CanvasHistorySnapshot = {
  pipelineId: string | null
  nodes: any[]
  edges: any[]
  key: string
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
  return {
    pipelineId,
    nodes: safeNodes,
    edges: safeEdges,
    key: JSON.stringify({ pipelineId, nodes: safeNodes, edges: safeEdges }),
  }
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
    resetCanvas, executionLogs, connectorType, setConnectorType, duplicateNode,
  } = useWorkflowStore()

  const [saving, setSaving] = useState(false)
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

  const selectedNodeIds = useMemo(() => {
    const explicitlySelected = nodes
      .filter((node) => Boolean((node as any)?.selected))
      .map((node) => String(node.id))
    if (explicitlySelected.length > 0) return explicitlySelected
    return selectedNodeId ? [selectedNodeId] : []
  }, [nodes, selectedNodeId])
  const actionTargetNodeIds = selectedNodeIds
  const actionTargetCount = actionTargetNodeIds.length
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
    const maxSnapshots = 120
    if (base.length > maxSnapshots) {
      base.splice(0, base.length - maxSnapshots)
    }
    historyRef.current = base
    historyIndexRef.current = base.length - 1
    updateHistoryFlags()
  }, [updateHistoryFlags])

  const handleUndo = useCallback(() => {
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
    const routePipelineId = id ? String(id) : null
    const pipelineId = pipeline?.id ? String(pipeline.id) : null
    if (!routePipelineId || pipelineId !== routePipelineId) return
    const snapshot = createCanvasHistorySnapshot(pipelineId, nodes, edges)
    if (activeHistoryPipelineRef.current !== pipelineId) {
      activeHistoryPipelineRef.current = pipelineId
      historyRef.current = [snapshot]
      historyIndexRef.current = 0
      updateHistoryFlags()
      return
    }
    pushHistorySnapshot(snapshot)
  }, [edges, historyReady, id, isExecuting, nodes, pipeline?.id, pushHistorySnapshot, updateHistoryFlags])

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
          setSaving(true)
          void savePipeline()
            .then(() => {
              notification.success({ message: 'Saved!', placement: 'bottomRight', duration: 2 })
            })
            .finally(() => {
              setSaving(false)
            })
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
        if (!isExecuting) {
          void executePipeline()
        }
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => {
      window.removeEventListener('keydown', onKeyDown)
    }
  }, [duplicateNode, executePipeline, handleUndo, handleRedo, isExecuting, savePipeline, saving, selectedNodeId])

  // Auto-save on dirty
  useEffect(() => {
    if (!isDirty) return
    const timer = setTimeout(async () => {
      await savePipeline()
    }, 2500)
    return () => clearTimeout(timer)
  }, [isDirty])

  const handleSave = async () => {
    setSaving(true)
    try {
      await savePipeline()
      notification.success({ message: 'Saved!', placement: 'bottomRight', duration: 2 })
    } finally {
      setSaving(false)
    }
  }

  const handleRun = async () => {
    if (isExecuting) return
    await executePipeline()
  }

  const handleAbort = async () => {
    if (!isExecuting) return
    await abortExecution()
  }

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
    if (isExecuting) return
    importFileInputRef.current?.click()
  }, [isExecuting])

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
            <Tooltip title={isExecuting ? (executionAbortRequested ? 'Aborting…' : 'Abort') : 'Execute (Ctrl/Cmd + Enter)'}>
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

            <Tooltip title="Save (Ctrl/Cmd + S)">
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
                disabled={!canUndo || isExecuting}
                style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)' }}
              />
            </Tooltip>
            <Tooltip title="Redo (Ctrl/Cmd + Shift + Z)">
              <Button
                icon={<RedoOutlined />}
                onClick={handleRedo}
                disabled={!canRedo || isExecuting}
                style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)' }}
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
          </Space>

          {/* Right-side status/meta actions */}
          <Space size={8} style={{ marginLeft: 'auto' }}>
            <Dropdown
              menu={{
                items: moreMenuItems,
                onClick: ({ key }) => {
                  if (key === 'schedule') setScheduleModal(true)
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
