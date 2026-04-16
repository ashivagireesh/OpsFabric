import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Card, Button, Input, Tag, Space, Typography, Modal, Form, Select,
  Dropdown, Badge, Empty, Spin, Row, Col, Tooltip, notification
} from 'antd'
import {
  PlusOutlined, SearchOutlined, EditOutlined, DeleteOutlined,
  CopyOutlined, PlayCircleOutlined, MoreOutlined, ApiOutlined,
  CheckCircleFilled, CloseCircleFilled, ClockCircleFilled,
  LoadingOutlined, ThunderboltOutlined
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import { usePipelineStore } from '../store'
import type { Pipeline } from '../types'

dayjs.extend(relativeTime)

const { Title, Text } = Typography

const statusColors: Record<string, string> = {
  active: '#22c55e',
  inactive: 'var(--app-text-muted)',
  draft: '#f59e0b',
}

const execStatusIcon: Record<string, JSX.Element> = {
  success: <CheckCircleFilled style={{ color: '#22c55e' }} />,
  failed: <CloseCircleFilled style={{ color: '#ef4444' }} />,
  running: <LoadingOutlined style={{ color: '#6366f1' }} spin />,
  pending: <ClockCircleFilled style={{ color: '#f59e0b' }} />,
}

export default function PipelineList() {
  const navigate = useNavigate()
  const { pipelines, loading, fetchPipelines, createPipeline, deletePipeline, duplicatePipeline } = usePipelineStore()
  const [search, setSearch] = useState('')
  const [modalOpen, setModalOpen] = useState(false)
  const [form] = Form.useForm()

  useEffect(() => { fetchPipelines() }, [])

  const filtered = pipelines.filter(p =>
    p.name.toLowerCase().includes(search.toLowerCase()) ||
    p.description?.toLowerCase().includes(search.toLowerCase()) ||
    p.tags?.some(t => t.toLowerCase().includes(search.toLowerCase()))
  )

  const handleCreate = async () => {
    try {
      const values = await form.validateFields()
      if (!values.name?.trim()) return
      const pipeline = await createPipeline(values.name.trim(), values.description?.trim())
      notification.success({ message: 'Pipeline created!', description: `"${pipeline.name}" is ready to build.`, placement: 'bottomRight' })
      setModalOpen(false)
      form.resetFields()
      navigate(`/pipelines/${pipeline.id}/edit`)
    } catch (err: any) {
      // Ant Design throws on failed validation — that's expected, just don't crash
      if (err?.errorFields) return
      notification.error({ message: 'Failed to create pipeline', description: String(err), placement: 'bottomRight' })
    }
  }

  const handleDelete = (id: string, name: string) => {
    Modal.confirm({
      title: `Delete "${name}"?`,
      content: 'This will permanently delete the pipeline and all its execution history.',
      okText: 'Delete',
      okButtonProps: { danger: true },
      onOk: async () => {
        await deletePipeline(id)
        notification.success({ message: 'Deleted', description: `"${name}" was removed.`, placement: 'bottomRight' })
      },
    })
  }

  return (
    <div style={{ padding: '24px' }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <Title level={4} style={{ color: 'var(--app-text)', margin: 0 }}>Pipelines</Title>
          <Text style={{ color: 'var(--app-text-subtle)' }}>{pipelines.length} pipelines total</Text>
        </div>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => setModalOpen(true)}
          style={{ background: 'linear-gradient(135deg, #6366f1, #a855f7)', border: 'none' }}
        >
          New Pipeline
        </Button>
      </div>

      {/* Search */}
      <Input
        placeholder="Search pipelines by name, description, or tag..."
        prefix={<SearchOutlined style={{ color: 'var(--app-text-subtle)' }} />}
        value={search}
        onChange={e => setSearch(e.target.value)}
        style={{
          background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)',
          color: 'var(--app-text)', borderRadius: 8, marginBottom: 20,
          maxWidth: 480,
        }}
        allowClear
      />

      {/* Pipeline Grid */}
      {loading ? (
        <div style={{ textAlign: 'center', padding: 80 }}>
          <Spin size="large" />
        </div>
      ) : filtered.length === 0 ? (
        <Empty
          description={<Text style={{ color: 'var(--app-text-subtle)' }}>No pipelines found</Text>}
          style={{ padding: 80 }}
        >
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setModalOpen(true)}
            style={{ background: 'linear-gradient(135deg, #6366f1, #a855f7)', border: 'none' }}
          >
            Create Pipeline
          </Button>
        </Empty>
      ) : (
        <Row gutter={[16, 16]}>
          {filtered.map(pipeline => (
            <Col xs={24} sm={12} xl={8} key={pipeline.id}>
              <PipelineCard
                pipeline={pipeline}
                onEdit={() => navigate(`/pipelines/${pipeline.id}/edit`)}
                onDelete={() => handleDelete(pipeline.id, pipeline.name)}
                onDuplicate={async () => {
                  await duplicatePipeline(pipeline.id)
                  notification.success({ message: 'Duplicated!', description: 'A copy has been added to your pipelines.', placement: 'bottomRight' })
                }}
              />
            </Col>
          ))}
        </Row>
      )}

      {/* Create Modal */}
      <Modal
        title={<span style={{ color: 'var(--app-text)' }}>New Pipeline</span>}
        open={modalOpen}
        onOk={handleCreate}
        onCancel={() => { setModalOpen(false); form.resetFields() }}
        okText="Create & Open Editor"
        okButtonProps={{ style: { background: 'linear-gradient(135deg, #6366f1, #a855f7)', border: 'none' } }}
        styles={{ content: { background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)' }, header: { background: 'var(--app-card-bg)', borderBottom: '1px solid var(--app-border-strong)' }, footer: { background: 'var(--app-card-bg)', borderTop: '1px solid var(--app-border-strong)' }, mask: { backdropFilter: 'blur(4px)' } }}
      >
        <Form form={form} layout="vertical" style={{ marginTop: 16 }}>
          <Form.Item name="name" label={<span style={{ color: 'var(--app-text-muted)' }}>Pipeline Name</span>}
            rules={[{ required: true, message: 'Pipeline name is required' }]}
          >
            <Input placeholder="e.g. Customer Data Sync" style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }} />
          </Form.Item>
          <Form.Item name="description" label={<span style={{ color: 'var(--app-text-muted)' }}>Description (optional)</span>}>
            <Input.TextArea
              placeholder="What does this pipeline do?"
              rows={3}
              style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)', resize: 'none' }}
            />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}

// ─── Pipeline Card ────────────────────────────────────────────────────────────

function PipelineCard({ pipeline, onEdit, onDelete, onDuplicate }: {
  pipeline: Pipeline
  onEdit: () => void
  onDelete: () => void
  onDuplicate: () => void
}) {
  const lastExec = pipeline.last_execution

  const menuItems = [
    { key: 'edit', icon: <EditOutlined />, label: 'Open Editor' },
    { key: 'duplicate', icon: <CopyOutlined />, label: 'Duplicate' },
    { type: 'divider' as const },
    { key: 'delete', icon: <DeleteOutlined />, label: 'Delete', danger: true },
  ]

  return (
    <Card
      hoverable
      style={{
        background: 'var(--app-card-bg)',
        border: '1px solid var(--app-border-strong)',
        borderRadius: 12,
        cursor: 'pointer',
        transition: 'all 0.2s',
      }}
      bodyStyle={{ padding: '20px' }}
      onClick={onEdit}
      onMouseEnter={e => (e.currentTarget.style.borderColor = '#6366f1')}
      onMouseLeave={e => (e.currentTarget.style.borderColor = 'var(--app-border-strong)')}
    >
      {/* Card Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 12 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div style={{
            width: 36, height: 36,
            background: `${statusColors[pipeline.status] || '#6366f1'}15`,
            border: `1px solid ${statusColors[pipeline.status] || '#6366f1'}30`,
            borderRadius: 9,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            color: statusColors[pipeline.status] || '#6366f1',
            fontSize: 16,
          }}>
            <ApiOutlined />
          </div>
          <div>
            <div style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 14, lineHeight: 1.3 }}>
              {pipeline.name}
            </div>
            <Tag
              style={{
                background: `${statusColors[pipeline.status]}18`,
                border: `1px solid ${statusColors[pipeline.status]}40`,
                color: statusColors[pipeline.status],
                borderRadius: 4, fontSize: 10, padding: '0 6px', marginTop: 2,
              }}
            >
              {pipeline.status.toUpperCase()}
            </Tag>
          </div>
        </div>
        <Dropdown
          menu={{
            items: menuItems,
            onClick: ({ key, domEvent }) => {
              domEvent.stopPropagation()
              if (key === 'edit') onEdit()
              else if (key === 'duplicate') onDuplicate()
              else if (key === 'delete') onDelete()
            },
            style: { background: '#22222f', border: '1px solid var(--app-border-strong)' }
          }}
          trigger={['click']}
        >
          <Button
            type="text"
            icon={<MoreOutlined />}
            style={{ color: 'var(--app-text-subtle)' }}
            onClick={e => e.stopPropagation()}
          />
        </Dropdown>
      </div>

      {/* Description */}
      {pipeline.description && (
        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 12 }}>
          {pipeline.description}
        </Text>
      )}

      {/* Tags */}
      {pipeline.tags?.length > 0 && (
        <Space wrap style={{ marginBottom: 12 }}>
          {pipeline.tags.slice(0, 3).map(tag => (
            <Tag key={tag} style={{ background: 'var(--app-border)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)', borderRadius: 4, fontSize: 11 }}>
              {tag}
            </Tag>
          ))}
        </Space>
      )}

      {/* Footer */}
      <div style={{ borderTop: '1px solid var(--app-border)', paddingTop: 12, marginTop: 4 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Space size={12}>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
              {(pipeline.nodeCount || 0)} nodes
            </Text>
            {lastExec && (
              <Space size={4}>
                {execStatusIcon[lastExec.status]}
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  {lastExec.rows_processed?.toLocaleString()} rows
                </Text>
              </Space>
            )}
          </Space>
          <Text style={{ color: 'var(--app-text-dim)', fontSize: 11 }}>
            {dayjs(pipeline.updated_at).fromNow()}
          </Text>
        </div>
      </div>
    </Card>
  )
}
