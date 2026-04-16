import { useEffect, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import {
  Button,
  Input,
  Space,
  Tag,
  Tooltip,
  Dropdown,
  notification,
  Modal,
  Select,
} from 'antd'
import {
  ArrowLeftOutlined,
  SaveOutlined,
  PlayCircleOutlined,
  LoadingOutlined,
  CheckCircleFilled,
  CloseCircleFilled,
  EllipsisOutlined,
  BranchesOutlined,
} from '@ant-design/icons'
import { ReactFlowProvider } from 'reactflow'
import api from '../../api/client'
import { useBusinessWorkflowStore } from '../../store/businessStore'
import {
  WORKFLOW_CONNECTOR_OPTIONS,
  type WorkflowConnectorType,
} from '../../constants/workflowConnectors'
import BusinessNodePalette from '../../components/business/BusinessNodePalette'
import BusinessWorkflowCanvas from '../../components/business/BusinessWorkflowCanvas'
import BusinessConfigDrawer from '../../components/business/BusinessConfigDrawer'
import BusinessExecutionPanel from '../../components/business/BusinessExecutionPanel'

export default function BusinessWorkflowEditor() {
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
  } = useBusinessWorkflowStore()

  const [saving, setSaving] = useState(false)
  const [workflowName, setWorkflowName] = useState('')

  useEffect(() => {
    if (!id) return
    resetCanvas()
    void loadWorkflow(id).then(() => {
      const state = useBusinessWorkflowStore.getState()
      setWorkflowName(state.workflow?.name || 'Untitled Business Workflow')
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
    { key: 'runs', icon: <BranchesOutlined />, label: 'View Runs (coming soon)' },
    { type: 'divider' as const },
    { key: 'delete', icon: <CloseCircleFilled />, label: 'Delete Workflow', danger: true },
  ]

  return (
    <ReactFlowProvider>
      <div style={{ height: '100vh', background: '#0a0a10', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        <div
          style={{
            background: 'var(--app-panel-bg)',
            borderBottom: '1px solid var(--app-border)',
            padding: '0 16px',
            height: 52,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            flexShrink: 0,
            zIndex: 10,
          }}
        >
          <Space size={12}>
            <Tooltip title="Back to business workflows">
              <Button
                type="text"
                icon={<ArrowLeftOutlined />}
                style={{ color: 'var(--app-text-subtle)' }}
                onClick={() => navigate('/business')}
              />
            </Tooltip>
            <div style={{ width: 1, height: 20, background: 'var(--app-border)' }} />
            <Input
              value={workflowName}
              onChange={(e) => setWorkflowName(e.target.value)}
              onBlur={async () => {
                if (workflow?.id && workflowName !== workflow?.name) {
                  await api.updateBusinessWorkflow(workflow.id, { name: workflowName })
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
              onFocus={(e) => { e.currentTarget.style.borderColor = '#f59e0b' }}
            />

            {isDirty && !saving && (
              <Tag style={{ background: '#f59e0b15', border: '1px solid #f59e0b30', color: '#f59e0b', fontSize: 11, borderRadius: 4 }}>
                Unsaved
              </Tag>
            )}
            {saving && <LoadingOutlined style={{ color: '#f59e0b', fontSize: 12 }} spin />}
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
                background: isExecuting ? 'rgba(245,158,11,0.45)' : 'linear-gradient(135deg, #f59e0b, #d97706)',
                border: 'none',
                minWidth: 130,
              }}
            >
              {isExecuting ? 'Running…' : 'Run Workflow'}
            </Button>

            <Dropdown
              menu={{
                items: moreMenuItems,
                onClick: ({ key }) => {
                  if (key === 'delete') {
                    Modal.confirm({
                      title: 'Delete this workflow?',
                      content: 'This action cannot be undone.',
                      okText: 'Delete',
                      okButtonProps: { danger: true },
                      onOk: async () => {
                        if (!workflow?.id) return
                        await api.deleteBusinessWorkflow(workflow.id)
                        notification.success({ message: 'Workflow deleted', placement: 'bottomRight' })
                        navigate('/business')
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
          <BusinessNodePalette />
          <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
            <BusinessWorkflowCanvas />
            <BusinessExecutionPanel />
          </div>
          <BusinessConfigDrawer open={!!selectedNodeId} onClose={() => setSelectedNode(null)} />
        </div>
      </div>
    </ReactFlowProvider>
  )
}
