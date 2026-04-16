import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Badge,
  Button,
  Card,
  Col,
  Dropdown,
  Empty,
  Form,
  Input,
  Modal,
  Row,
  Space,
  Spin,
  Tag,
  Typography,
  notification,
} from 'antd'
import {
  CopyOutlined,
  DeleteOutlined,
  EditOutlined,
  ExperimentOutlined,
  MoreOutlined,
  PlusOutlined,
  SearchOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import { useMLOpsCatalogStore } from '../../store/mlopsStore'
import type { MLOpsWorkflow } from '../../types'

dayjs.extend(relativeTime)

const { Title, Text } = Typography

const statusColors: Record<string, string> = {
  active: '#22c55e',
  inactive: 'var(--app-text-muted)',
  draft: '#f59e0b',
}

export default function MLOpsList() {
  const navigate = useNavigate()
  const { workflows, loading, fetchWorkflows, createWorkflow, deleteWorkflow, duplicateWorkflow } = useMLOpsCatalogStore()
  const [search, setSearch] = useState('')
  const [modalOpen, setModalOpen] = useState(false)
  const [form] = Form.useForm()

  useEffect(() => { void fetchWorkflows() }, [fetchWorkflows])

  const filtered = workflows.filter((wf) => (
    wf.name.toLowerCase().includes(search.toLowerCase()) ||
    wf.description?.toLowerCase().includes(search.toLowerCase()) ||
    wf.tags?.some((tag) => tag.toLowerCase().includes(search.toLowerCase()))
  ))

  const handleCreate = async () => {
    try {
      const values = await form.validateFields()
      if (!values.name?.trim()) return
      const workflow = await createWorkflow(values.name.trim(), values.description?.trim())
      notification.success({
        message: 'MLOps workflow created',
        description: `"${workflow.name}" is ready for feature engineering and training.`,
        placement: 'bottomRight',
      })
      setModalOpen(false)
      form.resetFields()
      navigate(`/mlops/${workflow.id}/edit`)
    } catch (err: any) {
      if (err?.errorFields) return
      notification.error({
        message: 'Failed to create MLOps workflow',
        description: String(err),
        placement: 'bottomRight',
      })
    }
  }

  const handleDelete = (id: string, name: string) => {
    Modal.confirm({
      title: `Delete "${name}"?`,
      content: 'This will remove workflow design and run history.',
      okText: 'Delete',
      okButtonProps: { danger: true },
      onOk: async () => {
        await deleteWorkflow(id)
        notification.success({
          message: 'Deleted',
          description: `"${name}" was removed.`,
          placement: 'bottomRight',
        })
      },
    })
  }

  return (
    <div style={{ padding: '24px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <Title level={4} style={{ color: 'var(--app-text)', margin: 0 }}>MLOps Studio</Title>
          <Text style={{ color: 'var(--app-text-subtle)' }}>{workflows.length} workflows for analytics, prediction and forecasting</Text>
        </div>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => setModalOpen(true)}
          style={{ background: 'linear-gradient(135deg, #22c55e, #16a34a)', border: 'none' }}
        >
          New MLOps Workflow
        </Button>
      </div>

      <Input
        placeholder="Search by workflow name, description, or tag..."
        prefix={<SearchOutlined style={{ color: 'var(--app-text-subtle)' }} />}
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        style={{
          background: 'var(--app-card-bg)',
          border: '1px solid var(--app-border-strong)',
          color: 'var(--app-text)',
          borderRadius: 8,
          marginBottom: 20,
          maxWidth: 520,
        }}
        allowClear
      />

      {loading ? (
        <div style={{ textAlign: 'center', padding: 80 }}>
          <Spin size="large" />
        </div>
      ) : filtered.length === 0 ? (
        <Empty
          description={<Text style={{ color: 'var(--app-text-subtle)' }}>No MLOps workflows found</Text>}
          style={{ padding: 80 }}
        >
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setModalOpen(true)}
            style={{ background: 'linear-gradient(135deg, #22c55e, #16a34a)', border: 'none' }}
          >
            Create MLOps Workflow
          </Button>
        </Empty>
      ) : (
        <Row gutter={[16, 16]}>
          {filtered.map((workflow) => (
            <Col xs={24} sm={12} xl={8} key={workflow.id}>
              <MLOpsWorkflowCard
                workflow={workflow}
                onEdit={() => navigate(`/mlops/${workflow.id}/edit`)}
                onDelete={() => handleDelete(workflow.id, workflow.name)}
                onDuplicate={async () => {
                  await duplicateWorkflow(workflow.id)
                  notification.success({
                    message: 'Duplicated',
                    description: 'A copy has been added to MLOps workflows.',
                    placement: 'bottomRight',
                  })
                }}
              />
            </Col>
          ))}
        </Row>
      )}

      <Modal
        title={<span style={{ color: 'var(--app-text)' }}>New MLOps Workflow</span>}
        open={modalOpen}
        onOk={handleCreate}
        onCancel={() => { setModalOpen(false); form.resetFields() }}
        okText="Create & Open Studio"
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
            name="name"
            label={<span style={{ color: 'var(--app-text-muted)' }}>Workflow Name</span>}
            rules={[{ required: true, message: 'Workflow name is required' }]}
          >
            <Input
              placeholder="e.g. Demand Forecasting Pipeline"
              style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }}
            />
          </Form.Item>
          <Form.Item
            name="description"
            label={<span style={{ color: 'var(--app-text-muted)' }}>Description (optional)</span>}
          >
            <Input.TextArea
              placeholder="Describe business objective and model use case"
              rows={3}
              style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)', resize: 'none' }}
            />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}

function MLOpsWorkflowCard({ workflow, onEdit, onDelete, onDuplicate }: {
  workflow: MLOpsWorkflow
  onEdit: () => void
  onDelete: () => void
  onDuplicate: () => void
}) {
  const menuItems = [
    { key: 'edit', icon: <EditOutlined />, label: 'Open Studio' },
    { key: 'duplicate', icon: <CopyOutlined />, label: 'Duplicate' },
    { type: 'divider' as const },
    { key: 'delete', icon: <DeleteOutlined />, label: 'Delete', danger: true },
  ]

  const lastRun = workflow.last_run

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
      onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#22c55e' }}
      onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'var(--app-border-strong)' }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 12 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div style={{
            width: 36,
            height: 36,
            background: `${statusColors[workflow.status] || '#22c55e'}15`,
            border: `1px solid ${statusColors[workflow.status] || '#22c55e'}30`,
            borderRadius: 9,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: statusColors[workflow.status] || '#22c55e',
            fontSize: 16,
          }}>
            <ExperimentOutlined />
          </div>
          <div>
            <div style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 14, lineHeight: 1.3 }}>
              {workflow.name}
            </div>
            <Tag
              style={{
                background: `${statusColors[workflow.status]}18`,
                border: `1px solid ${statusColors[workflow.status]}40`,
                color: statusColors[workflow.status],
                borderRadius: 4,
                fontSize: 10,
                padding: '0 6px',
                marginTop: 2,
              }}
            >
              {workflow.status.toUpperCase()}
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
            style: { background: '#22222f', border: '1px solid var(--app-border-strong)' },
          }}
          trigger={['click']}
        >
          <Button
            type="text"
            icon={<MoreOutlined />}
            style={{ color: 'var(--app-text-subtle)' }}
            onClick={(e) => e.stopPropagation()}
          />
        </Dropdown>
      </div>

      {workflow.description && (
        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', marginBottom: 12 }}>
          {workflow.description}
        </Text>
      )}

      {workflow.tags?.length > 0 && (
        <Space wrap style={{ marginBottom: 12 }}>
          {workflow.tags.slice(0, 3).map((tag) => (
            <Tag key={tag} style={{ background: 'var(--app-border)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)', borderRadius: 4, fontSize: 11 }}>
              {tag}
            </Tag>
          ))}
        </Space>
      )}

      <div style={{ borderTop: '1px solid var(--app-border)', paddingTop: 12, marginTop: 4 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Space size={12}>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
              {(workflow.nodeCount || 0)} blocks
            </Text>
            {lastRun && (
              <Badge
                status={lastRun.status === 'success' ? 'success' : lastRun.status === 'failed' ? 'error' : 'processing'}
                text={
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                    {lastRun.model_version || lastRun.status}
                  </Text>
                }
              />
            )}
          </Space>
          <Text style={{ color: 'var(--app-text-dim)', fontSize: 11 }}>
            {dayjs(workflow.updated_at).fromNow()}
          </Text>
        </div>
      </div>
    </Card>
  )
}
