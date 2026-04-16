import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  Button, Input, Space, Tag, Tooltip, Typography, Dropdown,
  notification, Modal, Form, Switch, Select
} from 'antd'
import {
  ArrowLeftOutlined, SaveOutlined, PlayCircleOutlined,
  SettingOutlined, LoadingOutlined, CheckCircleFilled,
  CloseCircleFilled, EllipsisOutlined, ScheduleOutlined,
  ShareAltOutlined, HistoryOutlined, CodeOutlined
} from '@ant-design/icons'
import { ReactFlowProvider } from 'reactflow'
import { useWorkflowStore } from '../store'
import {
  WORKFLOW_CONNECTOR_OPTIONS,
  type WorkflowConnectorType,
} from '../constants/workflowConnectors'
import api from '../api/client'
import NodePalette from '../components/workflow/NodePalette'
import WorkflowCanvas from '../components/workflow/WorkflowCanvas'
import ConfigDrawer from '../components/workflow/ConfigDrawer'
import ExecutionPanel from '../components/workflow/ExecutionPanel'

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
    nodes,
    pipeline, loadPipeline, savePipeline, executePipeline,
    isDirty, isExecuting, selectedNodeId, setSelectedNode,
    resetCanvas, executionLogs, connectorType, setConnectorType,
  } = useWorkflowStore()

  const [saving, setSaving] = useState(false)
  const [scheduleModal, setScheduleModal] = useState(false)
  const [pipelineName, setPipelineName] = useState('')
  const [form] = Form.useForm()
  const selectedSchedulePreset = Form.useWatch('schedulePreset', form)
  const selectedExecutionMode = Form.useWatch('executionMode', form)

  useEffect(() => {
    if (id) {
      resetCanvas()
      loadPipeline(id).then(() => {
        const state = useWorkflowStore.getState()
        setPipelineName(state.pipeline?.name || 'Untitled Pipeline')
      })
    }
  }, [id])

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

  const hasError = executionLogs.some(l => l.status === 'error')
  const hasSuccess = !isExecuting && executionLogs.length > 0 && !hasError

  const moreMenuItems = [
    { key: 'schedule', icon: <ScheduleOutlined />, label: 'Configure Schedule' },
    { key: 'history', icon: <HistoryOutlined />, label: 'Execution History' },
    { key: 'export', icon: <CodeOutlined />, label: 'Export as JSON' },
    { type: 'divider' as const },
    { key: 'delete', icon: <CloseCircleFilled />, label: 'Delete Pipeline', danger: true },
  ]

  return (
    <ReactFlowProvider>
      <div style={{ height: '100vh', background: '#0a0a10', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        {/* ── TOP TOOLBAR ───────────────────────────────────────────────────── */}
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

            {/* Save/dirty indicator */}
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

          {/* Right: action buttons */}
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
                background: isExecuting
                  ? 'rgba(99,102,241,0.5)'
                  : 'linear-gradient(135deg, #6366f1, #a855f7)',
                border: 'none',
                minWidth: 90,
              }}
            >
              {isExecuting ? 'Running…' : 'Execute'}
            </Button>

            <Dropdown
              menu={{
                items: moreMenuItems,
                onClick: ({ key }) => {
                  if (key === 'schedule') setScheduleModal(true)
                  else if (key === 'history') navigate('/executions')
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
          <ConfigDrawer
            open={!!selectedNodeId}
            onClose={() => setSelectedNode(null)}
          />
        </div>
      </div>

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
