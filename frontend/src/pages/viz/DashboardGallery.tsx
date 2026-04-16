import { useState, useEffect, useMemo, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Button,
  Input,
  Select,
  InputNumber,
  Tag,
  Modal,
  Form,
  Avatar,
  Tooltip,
  Badge,
  Skeleton,
  Empty,
  Dropdown,
  message,
  Popconfirm,
  Typography,
  Space,
  Row,
  Col,
} from 'antd'
import {
  SearchOutlined,
  PlusOutlined,
  AppstoreOutlined,
  UnorderedListOutlined,
  EditOutlined,
  EyeOutlined,
  DeleteOutlined,
  CopyOutlined,
  BarChartOutlined,
  GlobalOutlined,
  ClockCircleOutlined,
  TagOutlined,
  UserOutlined,
  DownOutlined,
  RightOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import axios from 'axios'
import { useVizStore, Dashboard } from '../../store/vizStore'

dayjs.extend(relativeTime)

const { Title, Text, Paragraph } = Typography
const VIZ_API_BASE = import.meta.env.DEV ? 'http://localhost:8001' : ''
const vizHttp = axios.create({ baseURL: VIZ_API_BASE, timeout: 45000 })
const PYTHON_DASHBOARD_TAG = '__python_analytics__'

type AnalyticsSourceType = 'sample' | 'pipeline' | 'file' | 'mlops'

interface PipelineOption {
  value: string
  label: string
}

interface MLOpsWorkflowOption {
  value: string
  label: string
}

interface AnalyticsAutoConfig {
  source_type: AnalyticsSourceType
  dataset?: string
  pipeline_id?: string
  file_path?: string
  mlops_workflow_id?: string
  mlops_output_mode?: 'predictions' | 'metrics' | 'monitor' | 'evaluation'
  mlops_prediction_start_date?: string
  mlops_prediction_end_date?: string
  sql?: string
  nlp_prompt?: string
  forecast_horizon: number
}

const SAMPLE_DATASETS = [
  { value: 'sales', label: 'Sales' },
  { value: 'web_analytics', label: 'Web Analytics' },
  { value: 'financials', label: 'Financials' },
  { value: 'employees', label: 'Employees' },
  { value: 'supply_chain', label: 'Supply Chain' },
  { value: 'customer', label: 'Customer' },
]

// ─── Constants ──────────────────────────────────────────────────────────────

const TAG_COLORS: string[] = [
  '#6366f1', '#8b5cf6', '#ec4899', '#f43f5e',
  '#f97316', '#eab308', '#22c55e', '#06b6d4',
  '#0ea5e9', '#14b8a6', '#a3e635', '#fb923c',
]

const GRADIENT_PRESETS: string[] = [
  'linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%)',
  'linear-gradient(135deg, #0ea5e9 0%, #06b6d4 100%)',
  'linear-gradient(135deg, #ec4899 0%, #f43f5e 100%)',
  'linear-gradient(135deg, #f97316 0%, #eab308 100%)',
  'linear-gradient(135deg, #22c55e 0%, #14b8a6 100%)',
  'linear-gradient(135deg, #8b5cf6 0%, #ec4899 100%)',
]

function getTagColor(tag: string): string {
  let hash = 0
  for (let i = 0; i < tag.length; i++) hash = tag.charCodeAt(i) + ((hash << 5) - hash)
  return TAG_COLORS[Math.abs(hash) % TAG_COLORS.length]
}

function getGradient(id: string): string {
  let hash = 0
  for (let i = 0; i < id.length; i++) hash = id.charCodeAt(i) + ((hash << 5) - hash)
  return GRADIENT_PRESETS[Math.abs(hash) % GRADIENT_PRESETS.length]
}

// ─── Skeleton Card ────────────────────────────────────────────────────────────

function SkeletonCard() {
  return (
    <div style={{
      background: 'var(--app-panel-bg)',
      border: '1px solid var(--app-border)',
      borderRadius: 12,
      overflow: 'hidden',
    }}>
      <Skeleton.Image active style={{ width: '100%', height: 140, borderRadius: 0 }} />
      <div style={{ padding: '14px 16px 16px' }}>
        <Skeleton active paragraph={{ rows: 2 }} title={{ width: '60%' }} />
        <Skeleton.Button active size="small" style={{ marginTop: 8, width: 80 }} />
      </div>
    </div>
  )
}

// ─── Dashboard Card ───────────────────────────────────────────────────────────

interface CardProps {
  dashboard: Dashboard
  viewMode: 'grid' | 'list'
  onEdit: (id: string) => void
  onView: (id: string) => void
  onDelete: (id: string) => void
  onDuplicate: (id: string) => void
}

function DashboardCard({ dashboard, viewMode, onEdit, onView, onDelete, onDuplicate }: CardProps) {
  const gradient = getGradient(dashboard.id)
  const updatedAgo = dashboard.updated_at
    ? dayjs(dashboard.updated_at).fromNow()
    : 'Never updated'

  const widgetCount = dashboard.widget_count ?? dashboard.widgets?.length ?? 0
  const ownerInitial = (dashboard.owner || 'U').charAt(0).toUpperCase()

  const cardStyle: React.CSSProperties = viewMode === 'list'
    ? {
        background: 'var(--app-panel-bg)',
        border: '1px solid var(--app-border)',
        borderRadius: 12,
        display: 'flex',
        flexDirection: 'row',
        overflow: 'hidden',
        transition: 'border-color 0.2s, box-shadow 0.2s',
        cursor: 'pointer',
      }
    : {
        background: 'var(--app-panel-bg)',
        border: '1px solid var(--app-border)',
        borderRadius: 12,
        overflow: 'hidden',
        transition: 'border-color 0.2s, box-shadow 0.2s',
        cursor: 'pointer',
        display: 'flex',
        flexDirection: 'column',
      }

  return (
    <div
      style={cardStyle}
      onMouseEnter={e => {
        const el = e.currentTarget as HTMLDivElement
        el.style.borderColor = '#6366f1'
        el.style.boxShadow = '0 0 0 1px #6366f130, 0 8px 32px #6366f115'
      }}
      onMouseLeave={e => {
        const el = e.currentTarget as HTMLDivElement
        el.style.borderColor = 'var(--app-border)'
        el.style.boxShadow = 'none'
      }}
    >
      {/* Thumbnail */}
      <div
        style={{
          background: gradient,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          flexShrink: 0,
          ...(viewMode === 'list'
            ? { width: 140, minHeight: 100 }
            : { height: 140, width: '100%' }),
          position: 'relative',
        }}
        onClick={() => onView(dashboard.id)}
      >
        <BarChartOutlined style={{ fontSize: 40, color: 'rgba(255,255,255,0.35)' }} />
        {dashboard.is_public && (
          <div style={{
            position: 'absolute',
            top: 10,
            left: 10,
            background: '#22c55e22',
            border: '1px solid #22c55e60',
            borderRadius: 6,
            padding: '2px 8px',
            display: 'flex',
            alignItems: 'center',
            gap: 4,
          }}>
            <GlobalOutlined style={{ color: '#22c55e', fontSize: 11 }} />
            <span style={{ color: '#22c55e', fontSize: 11, fontWeight: 600 }}>Public</span>
          </div>
        )}
        <div style={{
          position: 'absolute',
          top: 10,
          right: 10,
          background: 'var(--app-shell-overlay)',
          borderRadius: 6,
          padding: '2px 8px',
          fontSize: 11,
          color: 'var(--app-text-muted)',
          backdropFilter: 'blur(4px)',
        }}>
          {widgetCount} widget{widgetCount !== 1 ? 's' : ''}
        </div>
      </div>

      {/* Content */}
      <div
        style={{ flex: 1, padding: '14px 16px 14px', display: 'flex', flexDirection: 'column', gap: 8, minWidth: 0 }}
        onClick={() => onView(dashboard.id)}
      >
        {/* Name + description */}
        <div>
          <Text
            strong
            style={{ color: 'var(--app-text)', fontSize: 15, display: 'block', marginBottom: 4, lineHeight: '1.3' }}
            ellipsis
          >
            {dashboard.name}
          </Text>
          {dashboard.description && (
            <Paragraph
              ellipsis={{ rows: 2 }}
              style={{ color: 'var(--app-text-subtle)', fontSize: 12, margin: 0, lineHeight: '1.5' }}
            >
              {dashboard.description}
            </Paragraph>
          )}
        </div>

        {/* Tags */}
        {dashboard.tags && dashboard.tags.length > 0 && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
            {dashboard.tags.slice(0, 4).map(tag => (
              <Tag
                key={tag}
                style={{
                  background: getTagColor(tag) + '22',
                  border: `1px solid ${getTagColor(tag)}55`,
                  color: getTagColor(tag),
                  borderRadius: 6,
                  fontSize: 11,
                  padding: '0 7px',
                  margin: 0,
                  lineHeight: '20px',
                }}
              >
                {tag}
              </Tag>
            ))}
            {dashboard.tags.length > 4 && (
              <Tag style={{ background: 'var(--app-border)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-subtle)', borderRadius: 6, fontSize: 11, margin: 0 }}>
                +{dashboard.tags.length - 4}
              </Tag>
            )}
          </div>
        )}

        {/* Owner + timestamp */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 'auto' }}>
          <Avatar
            size={22}
            style={{ background: '#6366f133', color: '#6366f1', fontSize: 11, flexShrink: 0, fontWeight: 700 }}
          >
            {ownerInitial}
          </Avatar>
          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, flex: 1, minWidth: 0 }} ellipsis>
            {dashboard.owner}
          </Text>
          <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexShrink: 0 }}>
            <ClockCircleOutlined style={{ color: 'var(--app-text-dim)', fontSize: 11 }} />
            <Text style={{ color: 'var(--app-text-dim)', fontSize: 11 }}>{updatedAgo}</Text>
          </div>
        </div>
      </div>

      {/* Action bar */}
      <div
        style={{
          borderTop: '1px solid var(--app-border)',
          padding: '8px 12px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'flex-end',
          gap: 4,
          background: '#0a0a12',
          flexShrink: 0,
          ...(viewMode === 'list' ? { flexDirection: 'column', borderTop: 'none', borderLeft: '1px solid var(--app-border)', padding: '12px 12px', justifyContent: 'center', width: 120 } : {}),
        }}
        onClick={e => e.stopPropagation()}
      >
        <Tooltip title="Edit dashboard">
          <Button
            type="primary"
            size="small"
            icon={<EditOutlined />}
            onClick={() => onEdit(dashboard.id)}
            style={{
              background: '#6366f1',
              borderColor: '#6366f1',
              fontSize: 12,
              height: 28,
              ...(viewMode === 'list' ? { width: '100%' } : {}),
            }}
          >
            {viewMode === 'list' ? 'Edit' : 'Edit'}
          </Button>
        </Tooltip>
        <Tooltip title="View dashboard">
          <Button
            size="small"
            icon={<EyeOutlined />}
            onClick={() => onView(dashboard.id)}
            style={{
              background: 'transparent',
              borderColor: 'var(--app-border-strong)',
              color: 'var(--app-text-muted)',
              fontSize: 12,
              height: 28,
              ...(viewMode === 'list' ? { width: '100%' } : {}),
            }}
          >
            View
          </Button>
        </Tooltip>
        <Tooltip title="Duplicate">
          <Button
            size="small"
            icon={<CopyOutlined />}
            onClick={() => onDuplicate(dashboard.id)}
            style={{ background: 'transparent', border: 'none', color: 'var(--app-text-subtle)', height: 28, width: 28, padding: 0, minWidth: 'unset' }}
          />
        </Tooltip>
        <Popconfirm
          title="Delete dashboard"
          description="This action cannot be undone."
          onConfirm={() => onDelete(dashboard.id)}
          okText="Delete"
          cancelText="Cancel"
          okButtonProps={{ danger: true }}
          overlayStyle={{ maxWidth: 240 }}
        >
          <Tooltip title="Delete">
            <Button
              size="small"
              icon={<DeleteOutlined />}
              danger
              style={{ background: 'transparent', border: 'none', color: 'var(--app-text-subtle)', height: 28, width: 28, padding: 0, minWidth: 'unset' }}
            />
          </Tooltip>
        </Popconfirm>
      </div>
    </div>
  )
}

// ─── New Dashboard Modal ───────────────────────────────────────────────────────

interface NewDashboardModalProps {
  open: boolean
  onClose: () => void
  onCreate: (name: string, description: string) => Promise<void>
}

function NewDashboardModal({ open, onClose, onCreate }: NewDashboardModalProps) {
  const [form] = Form.useForm()
  const [loading, setLoading] = useState(false)

  const handleOk = async () => {
    try {
      const values = await form.validateFields()
      setLoading(true)
      await onCreate(values.name, values.description || '')
      form.resetFields()
    } catch {
      // validation error - do nothing
    } finally {
      setLoading(false)
    }
  }

  const handleCancel = () => {
    form.resetFields()
    onClose()
  }

  return (
    <Modal
      title={
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div style={{
            width: 32, height: 32, borderRadius: 8,
            background: 'linear-gradient(135deg, #6366f1, #8b5cf6)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <BarChartOutlined style={{ color: '#fff', fontSize: 16 }} />
          </div>
          <span style={{ color: 'var(--app-text)', fontSize: 16 }}>New Dashboard</span>
        </div>
      }
      open={open}
      onOk={handleOk}
      onCancel={handleCancel}
      confirmLoading={loading}
      okText="Create & Edit"
      okButtonProps={{ style: { background: '#6366f1', borderColor: '#6366f1' } }}
      styles={{
        content: { background: 'var(--app-panel-bg)', border: '1px solid var(--app-border)' },
        header: { background: 'var(--app-panel-bg)', borderBottom: '1px solid var(--app-border)', padding: '16px 24px 14px' },
        footer: { background: 'var(--app-panel-bg)', borderTop: '1px solid var(--app-border)' },
        mask: { backdropFilter: 'blur(4px)' },
      }}
    >
      <Form
        form={form}
        layout="vertical"
        style={{ marginTop: 8 }}
        requiredMark={false}
      >
        <Form.Item
          name="name"
          label={<Text style={{ color: 'var(--app-text-muted)', fontSize: 13 }}>Dashboard Name</Text>}
          rules={[
            { required: true, message: 'Please enter a name' },
            { min: 2, message: 'Name must be at least 2 characters' },
            { max: 80, message: 'Name must be at most 80 characters' },
          ]}
        >
          <Input
            placeholder="e.g. Sales Overview Q1 2025"
            autoFocus
            size="large"
            style={{ background: '#0a0a12', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)', borderRadius: 8 }}
          />
        </Form.Item>
        <Form.Item
          name="description"
          label={<Text style={{ color: 'var(--app-text-muted)', fontSize: 13 }}>Description <Text style={{ color: 'var(--app-text-dim)', fontSize: 12 }}>(optional)</Text></Text>}
        >
          <Input.TextArea
            placeholder="Brief description of what this dashboard tracks..."
            rows={3}
            style={{ background: '#0a0a12', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)', borderRadius: 8, resize: 'none' }}
          />
        </Form.Item>
      </Form>
    </Modal>
  )
}

// ─── Main Component ───────────────────────────────────────────────────────────

export default function DashboardGallery() {
  const navigate = useNavigate()
  const { dashboards, loading, fetchDashboards, createDashboard, deleteDashboard, duplicateDashboard } = useVizStore()

  const [searchText, setSearchText] = useState('')
  const [selectedTags, setSelectedTags] = useState<string[]>([])
  const [viewMode, setViewMode] = useState<'grid' | 'list'>('grid')
  const [showNewModal, setShowNewModal] = useState(false)
  const [analyticsSectionCollapsed, setAnalyticsSectionCollapsed] = useState(true)
  const [analyticsGenerating, setAnalyticsGenerating] = useState(false)
  const [analyticsPipelinesLoading, setAnalyticsPipelinesLoading] = useState(false)
  const [analyticsMLOpsLoading, setAnalyticsMLOpsLoading] = useState(false)
  const [analyticsPipelineOptions, setAnalyticsPipelineOptions] = useState<PipelineOption[]>([])
  const [analyticsMLOpsWorkflowOptions, setAnalyticsMLOpsWorkflowOptions] = useState<MLOpsWorkflowOption[]>([])
  const [analyticsConfig, setAnalyticsConfig] = useState<AnalyticsAutoConfig>({
    source_type: 'sample',
    dataset: 'sales',
    mlops_output_mode: 'predictions',
    forecast_horizon: 30,
    nlp_prompt: '',
  })
  const [messageApi, contextHolder] = message.useMessage()

  useEffect(() => {
    fetchDashboards()
  }, [fetchDashboards])

  useEffect(() => {
    let mounted = true
    const loadPipelineOptions = async () => {
      setAnalyticsPipelinesLoading(true)
      try {
        const response = await vizHttp.get('/api/pipelines')
        if (!mounted) return
        const options = Array.isArray(response.data)
          ? response.data.map((item: any) => ({
              value: String(item.id),
              label: String(item.name || item.id),
            }))
          : []
        setAnalyticsPipelineOptions(options)
      } catch {
        if (mounted) setAnalyticsPipelineOptions([])
      } finally {
        if (mounted) setAnalyticsPipelinesLoading(false)
      }
    }
    const loadMLOpsOptions = async () => {
      setAnalyticsMLOpsLoading(true)
      try {
        const response = await vizHttp.get('/api/mlops/workflows')
        if (!mounted) return
        const options = Array.isArray(response.data)
          ? response.data.map((item: any) => ({
              value: String(item.id),
              label: String(item.name || item.id),
            }))
          : []
        setAnalyticsMLOpsWorkflowOptions(options)
      } catch {
        if (mounted) setAnalyticsMLOpsWorkflowOptions([])
      } finally {
        if (mounted) setAnalyticsMLOpsLoading(false)
      }
    }

    void loadPipelineOptions()
    void loadMLOpsOptions()
    return () => { mounted = false }
  }, [])

  // Derive all unique tags across all dashboards
  const allTags = useMemo(() => {
    const tagSet = new Set<string>()
    dashboards.forEach(d => d.tags?.forEach(t => tagSet.add(t)))
    return Array.from(tagSet).sort()
  }, [dashboards])

  // Filter dashboards
  const filteredDashboards = useMemo(() => {
    const query = searchText.toLowerCase().trim()
    return dashboards.filter(d => {
      const matchesSearch =
        !query ||
        d.name.toLowerCase().includes(query) ||
        (d.description || '').toLowerCase().includes(query) ||
        d.tags?.some(t => t.toLowerCase().includes(query))
      const matchesTags =
        selectedTags.length === 0 ||
        selectedTags.every(t => d.tags?.includes(t))
      return matchesSearch && matchesTags
    })
  }, [dashboards, searchText, selectedTags])

  const toggleTag = useCallback((tag: string) => {
    setSelectedTags(prev =>
      prev.includes(tag) ? prev.filter(t => t !== tag) : [...prev, tag]
    )
  }, [])

  const handleEdit = useCallback((id: string) => {
    const row = dashboards.find((dashboard) => dashboard.id === id)
    const isPythonDashboard = Boolean(row?.tags?.some((tag) => String(tag).trim() === PYTHON_DASHBOARD_TAG))
    if (isPythonDashboard) {
      navigate(`/python-analytics?dashboardId=${encodeURIComponent(id)}`)
      return
    }
    navigate(`/dashboards/${id}/edit`)
  }, [dashboards, navigate])

  const handleView = useCallback((id: string) => {
    const row = dashboards.find((dashboard) => dashboard.id === id)
    const isPythonDashboard = Boolean(row?.tags?.some((tag) => String(tag).trim() === PYTHON_DASHBOARD_TAG))
    if (isPythonDashboard) {
      navigate(`/python-analytics?dashboardId=${encodeURIComponent(id)}`)
      return
    }
    navigate(`/dashboards/${id}`)
  }, [dashboards, navigate])

  const handleDelete = useCallback(async (id: string) => {
    try {
      await deleteDashboard(id)
      messageApi.success('Dashboard deleted')
    } catch {
      messageApi.error('Failed to delete dashboard')
    }
  }, [deleteDashboard, messageApi])

  const handleDuplicate = useCallback(async (id: string) => {
    try {
      await duplicateDashboard(id)
      messageApi.success('Dashboard duplicated')
    } catch {
      messageApi.error('Failed to duplicate dashboard')
    }
  }, [duplicateDashboard, messageApi])

  const handleCreate = useCallback(async (name: string, description: string) => {
    try {
      const newDashboard = await createDashboard(name, description)
      setShowNewModal(false)
      messageApi.success(`"${name}" created!`)
      navigate(`/dashboards/${newDashboard.id}/edit`)
    } catch {
      messageApi.error('Failed to create dashboard')
    }
  }, [createDashboard, navigate, messageApi])

  const updateAnalyticsConfig = useCallback((patch: Partial<AnalyticsAutoConfig>) => {
    setAnalyticsConfig(prev => ({ ...prev, ...patch }))
  }, [])

  const handleAnalyticsSourceTypeChange = useCallback((nextType: AnalyticsSourceType) => {
    if (nextType === 'sample') {
      updateAnalyticsConfig({
        source_type: nextType,
        dataset: analyticsConfig.dataset || 'sales',
        pipeline_id: undefined,
        file_path: undefined,
        mlops_workflow_id: undefined,
      })
      return
    }
    if (nextType === 'pipeline') {
      updateAnalyticsConfig({
        source_type: nextType,
        dataset: undefined,
        file_path: undefined,
        mlops_workflow_id: undefined,
      })
      return
    }
    if (nextType === 'mlops') {
      updateAnalyticsConfig({
        source_type: nextType,
        dataset: undefined,
        pipeline_id: undefined,
        file_path: undefined,
        mlops_output_mode: analyticsConfig.mlops_output_mode || 'predictions',
      })
      return
    }
    updateAnalyticsConfig({
      source_type: nextType,
      dataset: undefined,
      pipeline_id: undefined,
      mlops_workflow_id: undefined,
    })
  }, [analyticsConfig.dataset, analyticsConfig.mlops_output_mode, updateAnalyticsConfig])

  const handleGenerateAnalyticsDashboard = useCallback(async () => {
    if (!analyticsConfig.nlp_prompt || !analyticsConfig.nlp_prompt.trim()) {
      messageApi.warning('Enter an NLP prompt to generate dashboard')
      return
    }
    if (analyticsConfig.source_type === 'pipeline' && !analyticsConfig.pipeline_id) {
      messageApi.warning('Select a pipeline data source')
      return
    }
    if (analyticsConfig.source_type === 'file' && !analyticsConfig.file_path) {
      messageApi.warning('Provide a file path')
      return
    }
    if (analyticsConfig.source_type === 'mlops' && !analyticsConfig.mlops_workflow_id) {
      messageApi.warning('Select an MLOps workflow data source')
      return
    }

    setAnalyticsGenerating(true)
    try {
      const payload: Record<string, unknown> = {
        source_type: analyticsConfig.source_type,
        dataset: analyticsConfig.dataset,
        pipeline_id: analyticsConfig.pipeline_id,
        file_path: analyticsConfig.file_path,
        mlops_workflow_id: analyticsConfig.mlops_workflow_id,
        mlops_output_mode: analyticsConfig.mlops_output_mode,
        mlops_prediction_start_date: analyticsConfig.mlops_prediction_start_date,
        mlops_prediction_end_date: analyticsConfig.mlops_prediction_end_date,
        sql: analyticsConfig.sql,
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

      const summary = typeof response.data?.summary === 'string'
        ? response.data.summary
        : 'Auto-generated from NLP prompt'
      const sourceLabel = analyticsConfig.source_type === 'sample'
        ? (analyticsConfig.dataset || 'sample')
        : analyticsConfig.source_type
      const generatedName = `AI Dashboard · ${sourceLabel}`
      const generatedDescription = `Generated via GPT-5.3-Codex prompt. ${summary}`

      const createResponse = await vizHttp.post('/api/dashboards', {
        name: generatedName,
        description: generatedDescription,
        theme: 'dark',
        tags: ['ai', 'nlp', 'auto-generated'],
      })
      const newDashboardId = String(createResponse.data?.id || '').trim()
      if (!newDashboardId) {
        throw new Error('Failed to create dashboard')
      }

      await vizHttp.put(`/api/dashboards/${newDashboardId}`, {
        name: generatedName,
        description: generatedDescription,
        widgets: generatedWidgets,
        layout: generatedLayout,
        theme: 'dark',
        global_filters: [],
        tags: ['ai', 'nlp', 'auto-generated'],
        is_public: false,
      })

      await fetchDashboards()
      navigate(`/dashboards/${newDashboardId}/edit`)
      messageApi.success(`AI dashboard created with ${generatedWidgets.length} widgets`)
      if (nlpFallbackReason) {
        messageApi.warning(`NLP LLM fallback active: ${nlpFallbackReason}. Set valid OPENAI_API_KEY in backend/.env`)
      }
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      messageApi.error(typeof detail === 'string' ? detail : 'Failed to generate AI dashboard')
    } finally {
      setAnalyticsGenerating(false)
    }
  }, [analyticsConfig, fetchDashboards, messageApi, navigate])

  const colSpan = viewMode === 'list'
    ? { xs: 24, sm: 24, md: 24, lg: 24, xl: 24, xxl: 24 }
    : { xs: 24, sm: 12, md: 12, lg: 8, xl: 6, xxl: 6 }

  return (
    <div style={{ minHeight: '100vh', background: 'var(--app-shell-bg)', padding: '28px 32px 48px' }}>
      {contextHolder}

      {/* ── Header ── */}
      <div style={{ marginBottom: 28 }}>
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap' }}>
          <div>
            <Title level={3} style={{ color: 'var(--app-text)', margin: 0, fontWeight: 700, letterSpacing: '-0.5px' }}>
              Dashboards
            </Title>
            <Text style={{ color: 'var(--app-text-dim)', fontSize: 14, marginTop: 4, display: 'block' }}>
              {dashboards.length > 0
                ? `${dashboards.length} dashboard${dashboards.length !== 1 ? 's' : ''} • ${allTags.length} tag${allTags.length !== 1 ? 's' : ''}`
                : 'No dashboards yet'}
            </Text>
          </div>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            size="large"
            onClick={() => setShowNewModal(true)}
            style={{
              background: 'linear-gradient(135deg, #6366f1, #8b5cf6)',
              border: 'none',
              borderRadius: 10,
              fontWeight: 600,
              height: 40,
              paddingLeft: 20,
              paddingRight: 20,
              boxShadow: '0 4px 14px #6366f130',
            }}
          >
            New Dashboard
          </Button>
        </div>

        {/* Search + tag filters + view toggle */}
        <div style={{ marginTop: 20, display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div style={{ display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
            <Input
              prefix={<SearchOutlined style={{ color: 'var(--app-text-dim)' }} />}
              placeholder="Search by name, description or tag..."
              value={searchText}
              onChange={e => setSearchText(e.target.value)}
              allowClear
              style={{
                flex: '1 1 280px',
                maxWidth: 440,
                background: 'var(--app-panel-bg)',
                border: '1px solid var(--app-border-strong)',
                borderRadius: 10,
                color: 'var(--app-text)',
                height: 38,
              }}
            />
            <div style={{ display: 'flex', gap: 4, marginLeft: 'auto' }}>
              <Tooltip title="Grid view">
                <Button
                  icon={<AppstoreOutlined />}
                  onClick={() => setViewMode('grid')}
                  style={{
                    background: viewMode === 'grid' ? '#6366f122' : 'transparent',
                    border: viewMode === 'grid' ? '1px solid #6366f155' : '1px solid var(--app-border-strong)',
                    color: viewMode === 'grid' ? '#6366f1' : 'var(--app-text-subtle)',
                    borderRadius: 8,
                    height: 38,
                    width: 38,
                  }}
                />
              </Tooltip>
              <Tooltip title="List view">
                <Button
                  icon={<UnorderedListOutlined />}
                  onClick={() => setViewMode('list')}
                  style={{
                    background: viewMode === 'list' ? '#6366f122' : 'transparent',
                    border: viewMode === 'list' ? '1px solid #6366f155' : '1px solid var(--app-border-strong)',
                    color: viewMode === 'list' ? '#6366f1' : 'var(--app-text-subtle)',
                    borderRadius: 8,
                    height: 38,
                    width: 38,
                  }}
                />
              </Tooltip>
            </div>
          </div>

          {/* Tag pills */}
          {allTags.length > 0 && (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, alignItems: 'center' }}>
              <TagOutlined style={{ color: 'var(--app-text-dim)', fontSize: 13 }} />
              {allTags.map(tag => {
                const active = selectedTags.includes(tag)
                const color = getTagColor(tag)
                return (
                  <button
                    key={tag}
                    onClick={() => toggleTag(tag)}
                    style={{
                      background: active ? color + '30' : 'var(--app-border)',
                      border: `1px solid ${active ? color + '80' : 'var(--app-border-strong)'}`,
                      borderRadius: 20,
                      color: active ? color : 'var(--app-text-subtle)',
                      fontSize: 12,
                      padding: '3px 12px',
                      cursor: 'pointer',
                      fontWeight: active ? 600 : 400,
                      transition: 'all 0.15s',
                      outline: 'none',
                    }}
                  >
                    {tag}
                  </button>
                )
              })}
              {selectedTags.length > 0 && (
                <button
                  onClick={() => setSelectedTags([])}
                  style={{
                    background: 'transparent',
                    border: 'none',
                    color: 'var(--app-text-dim)',
                    fontSize: 12,
                    cursor: 'pointer',
                    padding: '3px 8px',
                    outline: 'none',
                    textDecoration: 'underline',
                  }}
                >
                  Clear
                </button>
              )}
            </div>
          )}
        </div>
      </div>

      {/* ── AI Dashboard Builder ── */}
      <div
        style={{
          marginBottom: 22,
          background: 'var(--app-panel-bg)',
          border: '1px solid var(--app-border)',
          borderRadius: 12,
          padding: '16px 16px 14px',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
          <div>
            <Title level={5} style={{ margin: 0, color: 'var(--app-text)' }}>
              AI Dashboard Builder
            </Title>
            <Text style={{ color: 'var(--app-text-dim)', fontSize: 12 }}>
              Prompt-based dashboard generation on selected datasource using GPT-5.3-Codex (with fallback if API key is not configured).
            </Text>
          </div>
          <Space size={8}>
            <Button
              icon={analyticsSectionCollapsed ? <RightOutlined /> : <DownOutlined />}
              onClick={() => setAnalyticsSectionCollapsed((prev) => !prev)}
              style={{ borderRadius: 9 }}
            >
              {analyticsSectionCollapsed ? 'Expand' : 'Collapse'}
            </Button>
            <Button
              type="primary"
              icon={<BarChartOutlined />}
              loading={analyticsGenerating}
              onClick={() => { void handleGenerateAnalyticsDashboard() }}
              style={{ background: 'linear-gradient(135deg, #6366f1, #8b5cf6)', border: 'none', borderRadius: 9, fontWeight: 600 }}
            >
              Generate Dashboard
            </Button>
          </Space>
        </div>

        {!analyticsSectionCollapsed && (
          <>
            <div style={{ display: 'flex', gap: 12, marginTop: 14, flexWrap: 'wrap' }}>
              <div style={{ minWidth: 220, flex: '1 1 220px' }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Datasource Type</Text>
                <Select
                  value={analyticsConfig.source_type}
                  onChange={(value) => handleAnalyticsSourceTypeChange(value as AnalyticsSourceType)}
                  options={[
                    { value: 'sample', label: 'Sample Dataset' },
                    { value: 'pipeline', label: 'ETL Pipeline Output' },
                    { value: 'file', label: 'File' },
                    { value: 'mlops', label: 'MLOps Output' },
                  ]}
                  style={{ width: '100%' }}
                />
              </div>

              {analyticsConfig.source_type === 'sample' && (
                <div style={{ minWidth: 220, flex: '1 1 220px' }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Dataset</Text>
                  <Select
                    value={analyticsConfig.dataset || 'sales'}
                    onChange={(value) => updateAnalyticsConfig({ dataset: value })}
                    options={SAMPLE_DATASETS}
                    style={{ width: '100%' }}
                  />
                </div>
              )}

              {analyticsConfig.source_type === 'pipeline' && (
                <div style={{ minWidth: 260, flex: '1 1 260px' }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Pipeline</Text>
                  <Select
                    value={analyticsConfig.pipeline_id || undefined}
                    onChange={(value) => updateAnalyticsConfig({ pipeline_id: value })}
                    options={analyticsPipelineOptions}
                    loading={analyticsPipelinesLoading}
                    placeholder="Select pipeline"
                    showSearch
                    optionFilterProp="label"
                    style={{ width: '100%' }}
                  />
                </div>
              )}

              {analyticsConfig.source_type === 'file' && (
                <div style={{ minWidth: 280, flex: '1 1 280px' }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>File Path</Text>
                  <Input
                    value={analyticsConfig.file_path || ''}
                    onChange={(e) => updateAnalyticsConfig({ file_path: e.target.value })}
                    placeholder="/absolute/path/to/file.csv"
                    style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }}
                  />
                </div>
              )}

              {analyticsConfig.source_type === 'mlops' && (
                <>
                  <div style={{ minWidth: 260, flex: '1 1 260px' }}>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>MLOps Workflow</Text>
                    <Select
                      value={analyticsConfig.mlops_workflow_id || undefined}
                      onChange={(value) => updateAnalyticsConfig({ mlops_workflow_id: value })}
                      options={analyticsMLOpsWorkflowOptions}
                      loading={analyticsMLOpsLoading}
                      placeholder="Select workflow"
                      showSearch
                      optionFilterProp="label"
                      style={{ width: '100%' }}
                    />
                  </div>
                  <div style={{ minWidth: 220, flex: '1 1 220px' }}>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>MLOps Output Mode</Text>
                    <Select
                      value={analyticsConfig.mlops_output_mode || 'predictions'}
                      onChange={(value) => updateAnalyticsConfig({ mlops_output_mode: value as 'predictions' | 'metrics' | 'monitor' | 'evaluation' })}
                      options={[
                        { value: 'predictions', label: 'Predictions' },
                        { value: 'metrics', label: 'Metrics Summary' },
                        { value: 'monitor', label: 'Monitoring Runs' },
                        { value: 'evaluation', label: 'Evaluation Output' },
                      ]}
                      style={{ width: '100%' }}
                    />
                  </div>
                </>
              )}
            </div>

            <div style={{ marginTop: 12 }}>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>NLP Prompt</Text>
              <Input.TextArea
                rows={3}
                value={analyticsConfig.nlp_prompt || ''}
                onChange={(e) => updateAnalyticsConfig({ nlp_prompt: e.target.value })}
                placeholder="Example: Create a dashboard for revenue trend, anomalies, and top drivers by region."
                style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)', resize: 'none' }}
              />
            </div>

            <div style={{ display: 'flex', gap: 12, marginTop: 12, flexWrap: 'wrap' }}>
              <div style={{ minWidth: 190, flex: '0 0 190px' }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Forecast Horizon</Text>
                <InputNumber
                  min={3}
                  max={365}
                  value={analyticsConfig.forecast_horizon}
                  onChange={(value) => updateAnalyticsConfig({ forecast_horizon: Number(value) || 30 })}
                  style={{ width: '100%' }}
                />
              </div>
              <div style={{ minWidth: 320, flex: '1 1 320px' }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 6 }}>Optional SQL Filter</Text>
                <Input
                  value={analyticsConfig.sql || ''}
                  onChange={(e) => updateAnalyticsConfig({ sql: e.target.value || undefined })}
                  placeholder="SELECT * FROM data WHERE ..."
                  style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }}
                />
              </div>
            </div>
          </>
        )}
      </div>

      {/* ── Results count ── */}
      {!loading && searchText && (
        <Text style={{ color: 'var(--app-text-dim)', fontSize: 13, display: 'block', marginBottom: 16 }}>
          {filteredDashboards.length} result{filteredDashboards.length !== 1 ? 's' : ''} for &ldquo;{searchText}&rdquo;
        </Text>
      )}

      {/* ── Loading skeleton ── */}
      {loading && (
        <Row gutter={[16, 16]}>
          {[...Array(6)].map((_, i) => (
            <Col key={i} {...colSpan}>
              <SkeletonCard />
            </Col>
          ))}
        </Row>
      )}

      {/* ── Empty state ── */}
      {!loading && filteredDashboards.length === 0 && (
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          padding: '80px 24px',
          gap: 20,
        }}>
          <div style={{
            width: 96,
            height: 96,
            borderRadius: 24,
            background: 'linear-gradient(135deg, #6366f115, #8b5cf615)',
            border: '1px solid #6366f120',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}>
            <BarChartOutlined style={{ fontSize: 44, color: '#6366f180' }} />
          </div>
          <div style={{ textAlign: 'center' }}>
            <Title level={4} style={{ color: 'var(--app-text)', margin: 0, marginBottom: 8 }}>
              {searchText || selectedTags.length > 0
                ? 'No dashboards match your filters'
                : 'No dashboards yet'}
            </Title>
            <Text style={{ color: 'var(--app-text-dim)', fontSize: 14 }}>
              {searchText || selectedTags.length > 0
                ? 'Try adjusting your search terms or clearing the tag filters.'
                : 'Create your first dashboard to start visualizing your data.'}
            </Text>
          </div>
          {!searchText && selectedTags.length === 0 && (
            <Button
              type="primary"
              icon={<PlusOutlined />}
              size="large"
              onClick={() => setShowNewModal(true)}
              style={{ background: '#6366f1', border: 'none', borderRadius: 10, fontWeight: 600 }}
            >
              Create your first dashboard
            </Button>
          )}
          {(searchText || selectedTags.length > 0) && (
            <Button
              onClick={() => { setSearchText(''); setSelectedTags([]) }}
              style={{ background: 'transparent', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)', borderRadius: 10 }}
            >
              Clear filters
            </Button>
          )}
        </div>
      )}

      {/* ── Dashboard grid / list ── */}
      {!loading && filteredDashboards.length > 0 && (
        <Row gutter={[16, 16]}>
          {filteredDashboards.map(dashboard => (
            <Col key={dashboard.id} {...colSpan}>
              <DashboardCard
                dashboard={dashboard}
                viewMode={viewMode}
                onEdit={handleEdit}
                onView={handleView}
                onDelete={handleDelete}
                onDuplicate={handleDuplicate}
              />
            </Col>
          ))}
        </Row>
      )}

      {/* ── New Dashboard Modal ── */}
      <NewDashboardModal
        open={showNewModal}
        onClose={() => setShowNewModal(false)}
        onCreate={handleCreate}
      />
    </div>
  )
}
