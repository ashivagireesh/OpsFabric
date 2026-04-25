import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Drawer,
  Empty,
  Form,
  Input,
  InputNumber,
  Modal,
  Select,
  Space,
  Switch,
  Table,
  Tabs,
  Typography,
} from 'antd'
import { CloseOutlined, DeleteOutlined } from '@ant-design/icons'
import { useBusinessWorkflowStore } from '../../store/businessStore'
import api from '../../api/client'
import { clearDrawerInteraction, markDrawerInteraction } from '../../utils/drawerAutoHide'
import type { ConfigField, ETLNodeData } from '../../types'

const { Text } = Typography

interface BusinessConfigDrawerProps {
  open: boolean
  onClose: () => void
}

const SMTP_PROVIDER_PRESETS: Record<string, { smtp_host: string; smtp_port: number; smtp_security: string }> = {
  gmail: { smtp_host: 'smtp.gmail.com', smtp_port: 587, smtp_security: 'tls' },
  outlook: { smtp_host: 'smtp.office365.com', smtp_port: 587, smtp_security: 'tls' },
}

export default function BusinessConfigDrawer({ open, onClose }: BusinessConfigDrawerProps) {
  const {
    nodes,
    selectedNodeId,
    updateNodeConfig,
    updateNodeLabel,
    removeNode,
  } = useBusinessWorkflowStore()
  const [form] = Form.useForm()
  const [activeTab, setActiveTab] = useState('config')
  const [fullOutputOpen, setFullOutputOpen] = useState(false)

  const [pipelineOptions, setPipelineOptions] = useState<{ value: string; label: string }[]>([])
  const [pipelineNodes, setPipelineNodes] = useState<{ value: string; label: string }[]>([])
  const [pipelinesLoading, setPipelinesLoading] = useState(false)
  const [pipelineNodesLoading, setPipelineNodesLoading] = useState(false)

  const [mlopsWorkflowOptions, setMLOpsWorkflowOptions] = useState<{ value: string; label: string }[]>([])
  const [mlopsRunOptions, setMLOpsRunOptions] = useState<{ value: string; label: string }[]>([])
  const [mlopsNodeOptions, setMLOpsNodeOptions] = useState<{ value: string; label: string }[]>([])
  const [mlopsLoading, setMLOpsLoading] = useState(false)
  const [mlopsRunsLoading, setMLOpsRunsLoading] = useState(false)
  const [mlopsNodesLoading, setMLOpsNodesLoading] = useState(false)

  const [modelOptions, setModelOptions] = useState<{ value: string; label: string }[]>([])
  const [modelsLoading, setModelsLoading] = useState(false)
  const [dashboardOptions, setDashboardOptions] = useState<{ value: string; label: string }[]>([])
  const [dashboardsLoading, setDashboardsLoading] = useState(false)
  const touchDrawerActivity = useCallback(() => {
    markDrawerInteraction('business')
  }, [])

  const node = nodes.find((n) => n.id === selectedNodeId)
  const data: ETLNodeData | undefined = node?.data

  useEffect(() => {
    if (!open) {
      clearDrawerInteraction('business')
      return
    }
    touchDrawerActivity()
  }, [open, selectedNodeId, touchDrawerActivity])

  useEffect(() => {
    if (!data) return
    form.resetFields()
    form.setFieldsValue({ _label: data.label, ...data.config })
    setActiveTab('config')
  }, [data, form, selectedNodeId])

  const selectedPipelineId = String(data?.config?.pipeline_id || '')
  const selectedMLOpsWorkflowId = String(data?.config?.mlops_workflow_id || '')

  const isETLSourceNode = data?.nodeType === 'business_etl_source'
  const isMLOpsSourceNode = data?.nodeType === 'business_mlops_source'
  const isPromptNode = data?.nodeType === 'business_llm_prompt'
  const isMailWriterNode = data?.nodeType === 'business_mail_writer'

  useEffect(() => {
    if (!open || !isETLSourceNode) return
    let active = true
    setPipelinesLoading(true)

    const loadPipelines = async () => {
      try {
        const rows = await api.listPipelines()
        if (!active) return
        const options = (Array.isArray(rows) ? rows : [])
          .filter((row: any) => row?.id && row?.name)
          .map((row: any) => ({ value: String(row.id), label: String(row.name) }))
        setPipelineOptions(options)
      } catch {
        if (active) setPipelineOptions([])
      } finally {
        if (active) setPipelinesLoading(false)
      }
    }

    void loadPipelines()
    return () => { active = false }
  }, [open, isETLSourceNode])

  useEffect(() => {
    if (!open || !isETLSourceNode || !selectedPipelineId) {
      setPipelineNodes([])
      return
    }
    let active = true
    setPipelineNodesLoading(true)

    const loadPipelineNodes = async () => {
      try {
        const pipeline = await api.getPipeline(selectedPipelineId)
        if (!active) return
        const options = (Array.isArray(pipeline?.nodes) ? pipeline.nodes : [])
          .filter((n: any) => n?.id)
          .map((n: any) => {
            const nodeType = String(n?.data?.nodeType || 'node')
            const label = String(n?.data?.label || nodeType)
            return { value: String(n.id), label: `${label} [${nodeType}]` }
          })
        setPipelineNodes(options)
      } catch {
        if (active) setPipelineNodes([])
      } finally {
        if (active) setPipelineNodesLoading(false)
      }
    }

    void loadPipelineNodes()
    return () => { active = false }
  }, [open, isETLSourceNode, selectedPipelineId])

  useEffect(() => {
    if (!open || !isMLOpsSourceNode) return
    let active = true
    setMLOpsLoading(true)

    const loadMLOpsWorkflows = async () => {
      try {
        const workflows = await api.listMLOpsWorkflows()
        if (!active) return
        const options = (Array.isArray(workflows) ? workflows : [])
          .filter((wf: any) => wf?.id && wf?.name)
          .map((wf: any) => ({ value: String(wf.id), label: String(wf.name) }))
        setMLOpsWorkflowOptions(options)
      } catch {
        if (active) setMLOpsWorkflowOptions([])
      } finally {
        if (active) setMLOpsLoading(false)
      }
    }

    void loadMLOpsWorkflows()
    return () => { active = false }
  }, [open, isMLOpsSourceNode])

  useEffect(() => {
    if (!open || !isMLOpsSourceNode || !selectedMLOpsWorkflowId) {
      setMLOpsRunOptions([])
      setMLOpsNodeOptions([])
      return
    }

    let active = true
    setMLOpsRunsLoading(true)
    setMLOpsNodesLoading(true)

    const loadRuns = async () => {
      try {
        const runs = await api.listMLOpsRuns(selectedMLOpsWorkflowId)
        if (!active) return
        const options = (Array.isArray(runs) ? runs : [])
          .filter((run: any) => run?.id)
          .map((run: any) => {
            const status = String(run.status || 'unknown').toUpperCase()
            const at = run.started_at ? new Date(run.started_at).toLocaleString() : 'no-time'
            return {
              value: String(run.id),
              label: `${String(run.id).slice(0, 8)} · ${status} · ${at}`,
            }
          })
        setMLOpsRunOptions(options)
      } catch {
        if (active) setMLOpsRunOptions([])
      } finally {
        if (active) setMLOpsRunsLoading(false)
      }
    }

    const loadNodes = async () => {
      try {
        const workflow = await api.getMLOpsWorkflow(selectedMLOpsWorkflowId)
        if (!active) return
        const options = (Array.isArray(workflow?.nodes) ? workflow.nodes : [])
          .filter((n: any) => n?.id)
          .map((n: any) => {
            const nodeType = String(n?.data?.nodeType || 'node')
            const label = String(n?.data?.label || nodeType)
            return { value: String(n.id), label: `${label} [${nodeType}]` }
          })
        setMLOpsNodeOptions(options)
      } catch {
        if (active) setMLOpsNodeOptions([])
      } finally {
        if (active) setMLOpsNodesLoading(false)
      }
    }

    void loadRuns()
    void loadNodes()

    return () => { active = false }
  }, [open, isMLOpsSourceNode, selectedMLOpsWorkflowId])

  useEffect(() => {
    if (!open || !isPromptNode) return
    let active = true
    setModelsLoading(true)

    const loadModels = async () => {
      try {
        const rows = await api.listOllamaModels()
        if (!active) return
        const options = (Array.isArray(rows) ? rows : [])
          .filter((m: any) => m?.value)
          .map((m: any) => {
            const provider = m?.provider ? ` · ${String(m.provider)}` : ''
            const available = m?.available === false ? ' (offline)' : ''
            return {
              value: String(m.value),
              label: `${String(m.label || m.value)}${provider}${available}`,
            }
          })
        setModelOptions(options)
      } catch {
        if (active) {
          setModelOptions([
            { value: 'gpt-oss20b', label: 'GPT-OSS20B (Local Ollama)' },
            { value: 'gpt-oss20b-cloud', label: 'GPT-OSS20B Cloud' },
          ])
        }
      } finally {
        if (active) setModelsLoading(false)
      }
    }

    void loadModels()
    return () => { active = false }
  }, [open, isPromptNode])

  useEffect(() => {
    if (!open || !isMailWriterNode) return
    let active = true
    setDashboardsLoading(true)

    const loadDashboards = async () => {
      try {
        const rows = await api.listDashboards()
        if (!active) return
        const options = (Array.isArray(rows) ? rows : [])
          .filter((row: any) => row?.id && row?.name)
          .map((row: any) => ({ value: String(row.id), label: String(row.name) }))
        setDashboardOptions(options)
      } catch {
        if (active) setDashboardOptions([])
      } finally {
        if (active) setDashboardsLoading(false)
      }
    }

    void loadDashboards()
    return () => { active = false }
  }, [open, isMailWriterNode])

  const sampleColumns = useMemo(() => {
    const rows = (data?.executionSampleOutput || []) as Record<string, unknown>[]
    if (!rows.length) return []
    return Object.keys(rows[0]).slice(0, 8).map((key) => ({
      title: key,
      dataIndex: key,
      key,
      render: (value: unknown) => (value === null || value === undefined ? '' : String(value)),
      ellipsis: true,
    }))
  }, [data?.executionSampleOutput])

  const promptOutputText = useMemo(() => {
    if (!isPromptNode) return ''
    const rows = (data?.executionSampleOutput || []) as Record<string, unknown>[]
    const first = rows[0] || {}
    const raw = first.response
    if (typeof raw === 'string') return raw
    if (raw === null || raw === undefined) return ''
    return String(raw)
  }, [data?.executionSampleOutput, isPromptNode])

  const promptOutputMeta = useMemo(() => {
    if (!isPromptNode) return ''
    const rows = (data?.executionSampleOutput || []) as Record<string, unknown>[]
    const first = rows[0] || {}
    const model = typeof first.model === 'string' ? first.model : ''
    const provider = typeof first.provider === 'string' ? first.provider : ''
    const parts = [model, provider].filter(Boolean)
    return parts.join(' · ')
  }, [data?.executionSampleOutput, isPromptNode])

  const promptOutputFormat = useMemo(() => {
    if (!isPromptNode) return 'text'
    const rows = (data?.executionSampleOutput || []) as Record<string, unknown>[]
    const first = rows[0] || {}
    const raw = first.output_format ?? data?.config?.output_format ?? 'text'
    return String(raw || 'text').toLowerCase()
  }, [data?.config?.output_format, data?.executionSampleOutput, isPromptNode])

  const promptEmailPreview = useMemo(() => {
    const empty = { subject: '', body: '' }
    if (!isPromptNode) return empty
    const text = String(promptOutputText || '').trim()
    if (!text) return empty

    const subjectMatch = text.match(/subject:\s*(.+)/i)
    const bodyMatch = text.match(/body:\s*([\s\S]*)$/i)
    const subject = (subjectMatch?.[1] || '').trim()
    if (bodyMatch?.[1]) {
      return { subject, body: bodyMatch[1].trim() }
    }

    const sections = text.split(/\n\s*\n/)
    if (sections.length > 1) {
      return { subject, body: sections.slice(1).join('\n\n').trim() }
    }

    return { subject, body: text }
  }, [isPromptNode, promptOutputText])

  const promptOutputAvailable = isPromptNode && Boolean(String(promptOutputText || '').trim())

  useEffect(() => {
    if (!open) setFullOutputOpen(false)
  }, [open, selectedNodeId])

  if (!data || !data.definition) return null

  const handleFieldChange = (name: string, value: unknown) => {
    if (!selectedNodeId) return

    if (isETLSourceNode && name === 'pipeline_id') {
      updateNodeConfig(selectedNodeId, {
        pipeline_id: value,
        pipeline_node_id: undefined,
      })
      return
    }

    if (isMLOpsSourceNode && name === 'mlops_workflow_id') {
      updateNodeConfig(selectedNodeId, {
        mlops_workflow_id: value,
        mlops_run_id: undefined,
        mlops_node_id: undefined,
      })
      return
    }

    if (isMailWriterNode && name === 'smtp_provider') {
      const provider = String(value || 'custom').toLowerCase()
      const preset = SMTP_PROVIDER_PRESETS[provider]
      if (!preset) {
        updateNodeConfig(selectedNodeId, { smtp_provider: provider })
        return
      }
      updateNodeConfig(selectedNodeId, {
        smtp_provider: provider,
        smtp_host: preset.smtp_host,
        smtp_port: preset.smtp_port,
        smtp_security: preset.smtp_security,
      })
      return
    }

    updateNodeConfig(selectedNodeId, { [name]: value })
  }

  const handleDelete = () => {
    if (!selectedNodeId) return
    removeNode(selectedNodeId)
    onClose()
  }

  const commonInputStyle = {
    background: 'var(--app-input-bg)',
    border: '1px solid var(--app-border-strong)',
    color: 'var(--app-text)',
  }

  const renderField = (field: ConfigField) => {
    const value = data.config[field.name]

    switch (field.type) {
      case 'password':
        return (
          <Input.Password
            placeholder={field.placeholder}
            defaultValue={(value as string) ?? (field.defaultValue as string) ?? ''}
            onChange={(e) => handleFieldChange(field.name, e.target.value)}
            style={commonInputStyle}
          />
        )
      case 'text':
        if (field.name.toLowerCase().includes('password')) {
          return (
            <Input.Password
              placeholder={field.placeholder}
              defaultValue={(value as string) ?? (field.defaultValue as string) ?? ''}
              onChange={(e) => handleFieldChange(field.name, e.target.value)}
              style={commonInputStyle}
            />
          )
        }
        return (
          <Input
            placeholder={field.placeholder}
            defaultValue={(value as string) ?? (field.defaultValue as string) ?? ''}
            onChange={(e) => handleFieldChange(field.name, e.target.value)}
            style={commonInputStyle}
          />
        )
      case 'textarea':
        return (
          <Input.TextArea
            placeholder={field.placeholder}
            rows={4}
            defaultValue={(value as string) ?? (field.defaultValue as string) ?? ''}
            onChange={(e) => handleFieldChange(field.name, e.target.value)}
            style={{ ...commonInputStyle, resize: 'vertical' }}
          />
        )
      case 'number':
        return (
          <InputNumber
            defaultValue={(value as number) ?? (field.defaultValue as number) ?? 0}
            onChange={(v) => handleFieldChange(field.name, v)}
            style={{ ...commonInputStyle, width: '100%' }}
          />
        )
      case 'toggle':
        return (
          <Switch
            checked={Boolean(value ?? field.defaultValue ?? false)}
            onChange={(checked) => handleFieldChange(field.name, checked)}
          />
        )
      case 'select': {
        if (isETLSourceNode && field.name === 'pipeline_id') {
          return (
            <Select
              value={(value as string) || undefined}
              onChange={(v) => handleFieldChange(field.name, v)}
              options={pipelineOptions}
              placeholder="Select ETL pipeline"
              loading={pipelinesLoading}
              showSearch
              optionFilterProp="label"
              allowClear
              style={{ width: '100%' }}
            />
          )
        }

        if (isETLSourceNode && field.name === 'pipeline_node_id') {
          return (
            <Select
              value={(value as string) || undefined}
              onChange={(v) => handleFieldChange(field.name, v)}
              options={pipelineNodes}
              placeholder="Auto-detect latest output"
              loading={pipelineNodesLoading}
              disabled={!selectedPipelineId}
              showSearch
              optionFilterProp="label"
              allowClear
              style={{ width: '100%' }}
            />
          )
        }

        if (isMLOpsSourceNode && field.name === 'mlops_workflow_id') {
          return (
            <Select
              value={(value as string) || undefined}
              onChange={(v) => handleFieldChange(field.name, v)}
              options={mlopsWorkflowOptions}
              placeholder="Select MLOps workflow"
              loading={mlopsLoading}
              showSearch
              optionFilterProp="label"
              allowClear
              style={{ width: '100%' }}
            />
          )
        }

        if (isMLOpsSourceNode && field.name === 'mlops_run_id') {
          return (
            <Select
              value={(value as string) || undefined}
              onChange={(v) => handleFieldChange(field.name, v)}
              options={mlopsRunOptions}
              placeholder="Use latest successful run"
              loading={mlopsRunsLoading}
              disabled={!selectedMLOpsWorkflowId}
              showSearch
              optionFilterProp="label"
              allowClear
              style={{ width: '100%' }}
            />
          )
        }

        if (isMLOpsSourceNode && field.name === 'mlops_node_id') {
          return (
            <Select
              value={(value as string) || undefined}
              onChange={(v) => handleFieldChange(field.name, v)}
              options={mlopsNodeOptions}
              placeholder="Best prediction node auto-selected"
              loading={mlopsNodesLoading}
              disabled={!selectedMLOpsWorkflowId}
              showSearch
              optionFilterProp="label"
              allowClear
              style={{ width: '100%' }}
            />
          )
        }

        if (isPromptNode && field.name === 'model') {
          return (
            <Select
              value={(value as string) || undefined}
              onChange={(v) => handleFieldChange(field.name, v)}
              options={modelOptions}
              loading={modelsLoading}
              placeholder="Select GPT-OSS20B local/Ollama or cloud model"
              showSearch
              optionFilterProp="label"
              style={{ width: '100%' }}
            />
          )
        }

        if (isMailWriterNode && field.name === 'dashboard_id') {
          return (
            <Select
              value={(value as string) || undefined}
              onChange={(v) => handleFieldChange(field.name, v)}
              options={dashboardOptions}
              placeholder="Select dashboard"
              loading={dashboardsLoading}
              showSearch
              optionFilterProp="label"
              allowClear
              style={{ width: '100%' }}
            />
          )
        }

        return (
          <Select
            value={(value as string) ?? (field.defaultValue as string) ?? undefined}
            onChange={(v) => handleFieldChange(field.name, v)}
            options={field.options || []}
            placeholder={field.placeholder}
            style={{ width: '100%' }}
          />
        )
      }
      default:
        return (
          <Input
            placeholder={field.placeholder}
            defaultValue={(value as string) ?? (field.defaultValue as string) ?? ''}
            onChange={(e) => handleFieldChange(field.name, e.target.value)}
            style={commonInputStyle}
          />
        )
    }
  }

  const renderPromptOutput = (fullScreen = false) => {
    const outputHeight = fullScreen ? 'calc(100vh - 180px)' : '320px'
    if (promptOutputFormat === 'email') {
      return (
        <div
          style={{
            border: '1px solid var(--app-border-strong)',
            borderRadius: fullScreen ? 10 : 8,
            background: 'var(--app-input-bg)',
            overflow: 'hidden',
          }}
        >
          <div style={{ padding: '10px 12px', borderBottom: '1px solid var(--app-border)' }}>
            <Text strong style={{ color: 'var(--app-text)' }}>
              Subject: {promptEmailPreview.subject || '(no subject)'}
            </Text>
          </div>
          <pre
            style={{
              margin: 0,
              padding: 12,
              minHeight: fullScreen ? outputHeight : undefined,
              maxHeight: fullScreen ? outputHeight : 320,
              overflow: 'auto',
              whiteSpace: 'pre-wrap',
              color: 'var(--app-text)',
              fontFamily: 'ui-sans-serif, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif',
            }}
          >
            {promptEmailPreview.body || 'No email body generated.'}
          </pre>
        </div>
      )
    }

    if (promptOutputFormat === 'html') {
      return (
        <iframe
          title={fullScreen ? 'Prompt HTML Preview Full Screen' : 'Prompt HTML Preview'}
          srcDoc={promptOutputText || '<html><body><p>No HTML response generated.</p></body></html>'}
          style={{
            width: '100%',
            minHeight: fullScreen ? outputHeight : 260,
            height: fullScreen ? outputHeight : undefined,
            border: '1px solid var(--app-border-strong)',
            borderRadius: fullScreen ? 10 : 8,
            background: 'white',
          }}
        />
      )
    }

    return (
      <pre
        style={{
          margin: 0,
          padding: 12,
          minHeight: fullScreen ? outputHeight : undefined,
          maxHeight: fullScreen ? outputHeight : 320,
          overflow: 'auto',
          whiteSpace: 'pre-wrap',
          background: 'var(--app-input-bg)',
          border: '1px solid var(--app-border-strong)',
          borderRadius: fullScreen ? 10 : 8,
          color: 'var(--app-text)',
          fontFamily: promptOutputFormat === 'code'
            ? 'ui-monospace, SFMono-Regular, Menlo, monospace'
            : 'ui-sans-serif, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif',
        }}
      >
        {promptOutputText || 'No response text found in output sample.'}
      </pre>
    )
  }

  return (
    <Drawer
      width={420}
      open={open}
      onClose={onClose}
      closeIcon={<CloseOutlined style={{ color: 'var(--app-text-subtle)' }} />}
      title={
        <Space direction="vertical" size={0}>
          <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>{data.definition.label}</Text>
          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>{data.definition.description}</Text>
        </Space>
      }
      styles={{
        header: { background: 'var(--app-panel-bg)', borderBottom: '1px solid var(--app-border)' },
        body: { background: 'var(--app-panel-bg)', padding: 0 },
      }}
    >
      <div
        onMouseDownCapture={touchDrawerActivity}
        onTouchStartCapture={touchDrawerActivity}
        onKeyDownCapture={touchDrawerActivity}
        onWheelCapture={touchDrawerActivity}
      >
      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        items={[
          {
            key: 'config',
            label: 'Config',
            children: (
              <div style={{ padding: 16 }}>
                <Form form={form} layout="vertical">
                  <Form.Item label={<span style={{ color: 'var(--app-text-muted)' }}>Node Label</span>} name="_label">
                    <Input
                      value={data.label}
                      onChange={(e) => selectedNodeId && updateNodeLabel(selectedNodeId, e.target.value)}
                      style={commonInputStyle}
                    />
                  </Form.Item>

                  {data.definition.configFields.length === 0 ? (
                    <Alert
                      type="info"
                      message="No configuration required"
                      showIcon
                      style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)' }}
                    />
                  ) : (
                    data.definition.configFields.map((field) => (
                      <Form.Item
                        key={field.name}
                        label={<span style={{ color: 'var(--app-text-muted)' }}>{field.label}</span>}
                        required={field.required}
                        tooltip={field.description}
                      >
                        {renderField(field)}
                        {field.description && (
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>{field.description}</Text>
                        )}
                      </Form.Item>
                    ))
                  )}

                  <div
                    style={{
                      marginTop: 20,
                      paddingTop: 14,
                      borderTop: '1px solid var(--app-border)',
                      display: 'grid',
                      gap: 6,
                    }}
                  >
                    <Text style={{ color: '#ef4444', fontSize: 11, fontWeight: 700 }}>Danger Zone</Text>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                      Delete this node from pipeline canvas.
                    </Text>
                    <Button
                      danger
                      icon={<DeleteOutlined />}
                      onClick={handleDelete}
                    >
                      Delete Node
                    </Button>
                  </div>
                </Form>
              </div>
            ),
          },
          {
            key: 'sample',
            label: 'Sample Output',
            children: (
              <div style={{ padding: 16 }}>
                {Array.isArray(data.executionSampleOutput) && data.executionSampleOutput.length > 0 ? (
                  <>
                    {isPromptNode && (
                      <div style={{ marginBottom: 12 }}>
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, display: 'block' }}>
                            Generated Prompt Content{promptOutputMeta ? ` (${promptOutputMeta})` : ''} · {promptOutputFormat.toUpperCase()}
                          </Text>
                          {promptOutputAvailable && (
                            <Button size="small" onClick={() => setFullOutputOpen(true)}>
                              Full Screen
                            </Button>
                          )}
                        </div>
                        {renderPromptOutput(false)}
                      </div>
                    )}
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                      Latest run sample output ({data.executionSampleOutput.length} rows)
                    </Text>
                    <Table
                      style={{ marginTop: 10 }}
                      size="small"
                      pagination={{ pageSize: 5, hideOnSinglePage: true }}
                      columns={sampleColumns}
                      dataSource={(data.executionSampleOutput || []).slice(0, 30).map((row, idx) => ({ key: idx, ...row }))}
                      scroll={{ x: true }}
                    />
                  </>
                ) : (
                  <Empty
                    description={<Text style={{ color: 'var(--app-text-subtle)' }}>Run the workflow to view output sample</Text>}
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                  />
                )}
              </div>
            ),
          },
        ]}
      />
      <Modal
        open={fullOutputOpen}
        onCancel={() => setFullOutputOpen(false)}
        footer={null}
        width="100vw"
        style={{ top: 0, maxWidth: '100vw', paddingBottom: 0 }}
        title={`Output Preview${promptOutputMeta ? ` · ${promptOutputMeta}` : ''} · ${promptOutputFormat.toUpperCase()}`}
        styles={{
          content: { height: '100vh', borderRadius: 0, background: 'var(--app-panel-bg)' },
          header: { borderBottom: '1px solid var(--app-border)', background: 'var(--app-panel-bg)' },
          body: { padding: 16, background: 'var(--app-panel-bg)', overflow: 'auto' },
        }}
        destroyOnClose
      >
        {renderPromptOutput(true)}
      </Modal>
      </div>
    </Drawer>
  )
}
