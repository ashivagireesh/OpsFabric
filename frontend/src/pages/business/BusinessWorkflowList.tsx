import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
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
  BranchesOutlined,
  CopyOutlined,
  DeleteOutlined,
  EditOutlined,
  MoreOutlined,
  PlusOutlined,
  SearchOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import { useBusinessCatalogStore } from '../../store/businessStore'
import type { BusinessWorkflow } from '../../types'

dayjs.extend(relativeTime)

const { Title, Text } = Typography

const statusColors: Record<string, string> = {
  active: '#22c55e',
  inactive: 'var(--app-text-muted)',
  draft: '#f59e0b',
}

export default function BusinessWorkflowList() {
  const navigate = useNavigate()
  const { workflows, loading, fetchWorkflows, createWorkflow, deleteWorkflow, duplicateWorkflow } = useBusinessCatalogStore()
  const [search, setSearch] = useState('')
  const [modalOpen, setModalOpen] = useState(false)
  const [form] = Form.useForm()

  useEffect(() => { void fetchWorkflows() }, [fetchWorkflows])

  const filtered = workflows.filter((wf) => (
    wf.name.toLowerCase().includes(search.toLowerCase())
    || wf.description?.toLowerCase().includes(search.toLowerCase())
    || wf.tags?.some((tag) => tag.toLowerCase().includes(search.toLowerCase()))
  ))

  const handleCreate = async () => {
    try {
      const values = await form.validateFields()
      if (!values.name?.trim()) return
      const workflow = await createWorkflow(values.name.trim(), values.description?.trim())
      notification.success({
        message: 'Business workflow created',
        description: `"${workflow.name}" is ready for prompt-driven business automation.`,
        placement: 'bottomRight',
      })
      setModalOpen(false)
      form.resetFields()
      navigate(`/business/${workflow.id}/edit`)
    } catch (err: any) {
      if (err?.errorFields) return
      notification.error({
        message: 'Failed to create workflow',
        description: String(err),
        placement: 'bottomRight',
      })
    }
  }

  const handleDelete = (id: string, name: string) => {
    Modal.confirm({
      title: `Delete "${name}"?`,
      content: 'This will remove workflow design and execution history.',
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
          <Title level={4} style={{ color: 'var(--app-text)', margin: 0 }}>Business Logic Workflows</Title>
          <Text style={{ color: 'var(--app-text-subtle)' }}>{workflows.length} workflows for prompt decisions, analytics, mail and WhatsApp actions</Text>
        </div>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => setModalOpen(true)}
          style={{ background: 'linear-gradient(135deg, #f59e0b, #d97706)', border: 'none' }}
        >
          New Business Workflow
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
          description={<Text style={{ color: 'var(--app-text-subtle)' }}>No business workflows found</Text>}
          style={{ padding: 80 }}
        >
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setModalOpen(true)}
            style={{ background: 'linear-gradient(135deg, #f59e0b, #d97706)', border: 'none' }}
          >
            Create Business Workflow
          </Button>
        </Empty>
      ) : (
        <Row gutter={[16, 16]}>
          {filtered.map((workflow) => (
            <Col xs={24} sm={12} xl={8} key={workflow.id}>
              <BusinessWorkflowCard
                workflow={workflow}
                onEdit={() => navigate(`/business/${workflow.id}/edit`)}
                onDelete={() => handleDelete(workflow.id, workflow.name)}
                onDuplicate={async () => {
                  await duplicateWorkflow(workflow.id)
                  notification.success({
                    message: 'Duplicated',
                    description: 'A copy has been added to Business Logic workflows.',
                    placement: 'bottomRight',
                  })
                }}
              />
            </Col>
          ))}
        </Row>
      )}

      <Modal
        title={<span style={{ color: 'var(--app-text)' }}>New Business Workflow</span>}
        open={modalOpen}
        onOk={handleCreate}
        onCancel={() => { setModalOpen(false); form.resetFields() }}
        okText="Create & Open Editor"
        okButtonProps={{ style: { background: 'linear-gradient(135deg, #f59e0b, #d97706)', border: 'none' } }}
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
              placeholder="e.g. Opportunity Qualification and Follow-up"
              style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }}
            />
          </Form.Item>
          <Form.Item
            name="description"
            label={<span style={{ color: 'var(--app-text-muted)' }}>Description (optional)</span>}
          >
            <Input.TextArea
              placeholder="Describe the business objective and expected automated actions"
              rows={3}
              style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)', resize: 'none' }}
            />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}

function BusinessWorkflowCard({ workflow, onEdit, onDelete, onDuplicate }: {
  workflow: BusinessWorkflow
  onEdit: () => void
  onDelete: () => void
  onDuplicate: () => void
}) {
  const menuItems = [
    { key: 'edit', icon: <EditOutlined />, label: 'Open Editor' },
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
      onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#f59e0b' }}
      onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'var(--app-border-strong)' }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 12 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div style={{
            width: 36,
            height: 36,
            background: `${statusColors[workflow.status] || '#f59e0b'}15`,
            border: `1px solid ${statusColors[workflow.status] || '#f59e0b'}30`,
            borderRadius: 9,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: statusColors[workflow.status] || '#f59e0b',
            fontSize: 16,
          }}>
            <BranchesOutlined />
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
              if (key === 'duplicate') onDuplicate()
              if (key === 'delete') onDelete()
            },
          }}
          trigger={['click']}
        >
          <Button
            type="text"
            icon={<MoreOutlined />}
            size="small"
            onClick={(e) => e.stopPropagation()}
            style={{ color: 'var(--app-text-subtle)' }}
          />
        </Dropdown>
      </div>

      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block', minHeight: 36 }}>
        {workflow.description || 'No description'}
      </Text>

      <div style={{ marginTop: 14, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Tag style={{ background: '#f59e0b15', border: '1px solid #f59e0b35', color: '#fbbf24', borderRadius: 5, margin: 0 }}>
          {workflow.nodeCount || 0} nodes
        </Tag>
        <Text style={{ color: 'var(--app-text-faint)', fontSize: 11 }}>
          Updated {workflow.updated_at ? dayjs(workflow.updated_at).fromNow() : 'recently'}
        </Text>
      </div>

      {lastRun && (
        <div style={{ marginTop: 12, paddingTop: 10, borderTop: '1px solid var(--app-border)' }}>
          <Text style={{ color: 'var(--app-text-faint)', fontSize: 11 }}>
            Last run: {lastRun.status.toUpperCase()}
            {lastRun.model_name ? ` · ${lastRun.model_name}` : ''}
          </Text>
        </div>
      )}
    </Card>
  )
}
