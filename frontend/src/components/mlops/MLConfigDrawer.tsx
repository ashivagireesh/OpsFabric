import { useCallback, useEffect, useState } from 'react'
import {
  Alert,
  Button,
  Divider,
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
  Tag,
  Tooltip,
  Typography,
} from 'antd'
import { CloseOutlined, DeleteOutlined, DownloadOutlined, InfoCircleOutlined } from '@ant-design/icons'
import Editor from '@monaco-editor/react'
import { useMLOpsWorkflowStore } from '../../store/mlopsStore'
import api from '../../api/client'
import { clearDrawerInteraction, markDrawerInteraction } from '../../utils/drawerAutoHide'
import type { ConfigField, ETLNodeData } from '../../types'

const { Text } = Typography

interface MLConfigDrawerProps {
  open: boolean
  onClose: () => void
}

export default function MLConfigDrawer({ open, onClose }: MLConfigDrawerProps) {
  const {
    nodes,
    selectedNodeId,
    updateNodeConfig,
    updateNodeLabel,
    removeNode,
    workflow,
  } = useMLOpsWorkflowStore()
  const [form] = Form.useForm()
  const [activeTab, setActiveTab] = useState('config')
  const [pipelineOptions, setPipelineOptions] = useState<{ value: string; label: string }[]>([])
  const [pipelineNodes, setPipelineNodes] = useState<{ value: string; label: string }[]>([])
  const [pipelinesLoading, setPipelinesLoading] = useState(false)
  const [pipelineNodesLoading, setPipelineNodesLoading] = useState(false)
  const [featureProfileLoading, setFeatureProfileLoading] = useState(false)
  const [featureProfile, setFeatureProfile] = useState<any>(null)
  const [featureOpColumn, setFeatureOpColumn] = useState('')
  const [featureOpType, setFeatureOpType] = useState('')
  const [featureOpParamsText, setFeatureOpParamsText] = useState('{}')
  const [featureOpError, setFeatureOpError] = useState('')
  const touchDrawerActivity = useCallback(() => {
    markDrawerInteraction('mlops')
  }, [])

  const node = nodes.find((n) => n.id === selectedNodeId)
  const data: ETLNodeData | undefined = node?.data

  useEffect(() => {
    if (!open) {
      clearDrawerInteraction('mlops')
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

  const isPipelineOutputNode = data?.nodeType === 'ml_pipeline_output_source'
  const isFeatureNode = data?.nodeType === 'ml_feature_engineering'
  const isFeatureOverlay = isFeatureNode
  const selectedPipelineId = String(data?.config?.pipeline_id || '')

  useEffect(() => {
    if (!open || !isPipelineOutputNode) return
    let active = true
    setPipelinesLoading(true)

    const loadPipelines = async () => {
      try {
        const rows = await api.listPipelines()
        if (!active) return
        const options = (Array.isArray(rows) ? rows : [])
          .filter((row: any) => row?.id && row?.name)
          .map((row: any) => {
            const status = row?.last_execution?.status
            const suffix = status === 'success' ? '' : (status ? ` (${status})` : ' (no runs)')
            return { value: String(row.id), label: `${String(row.name)}${suffix}` }
          })
        setPipelineOptions(options)
      } catch {
        if (active) setPipelineOptions([])
      } finally {
        if (active) setPipelinesLoading(false)
      }
    }

    void loadPipelines()
    return () => { active = false }
  }, [isPipelineOutputNode, open])

  useEffect(() => {
    if (!open || !isPipelineOutputNode) return
    if (!selectedPipelineId) {
      setPipelineNodes([])
      return
    }
    let active = true
    setPipelineNodesLoading(true)

    const loadPipelineNodes = async () => {
      try {
        const pipeline = await api.getPipeline(selectedPipelineId)
        if (!active) return
        const nodeOptions = (Array.isArray(pipeline?.nodes) ? pipeline.nodes : [])
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
    return () => { active = false }
  }, [isPipelineOutputNode, open, selectedPipelineId])

  const loadFeatureProfile = async () => {
    if (!open || !isFeatureNode || !workflow?.id || !selectedNodeId) return
    setFeatureProfileLoading(true)
    try {
      const resp = await api.getMLOpsFeatureProfile(workflow.id, {
        node_id: selectedNodeId,
        sample_size: 3000,
      })
      setFeatureProfile(resp)
      const firstOp = Array.isArray(resp?.available_operations) && resp.available_operations.length > 0
        ? String(resp.available_operations[0].value || '')
        : ''
      if (firstOp && !featureOpType) setFeatureOpType(firstOp)
      const firstCol = Array.isArray(resp?.profile?.columns) && resp.profile.columns.length > 0
        ? String(resp.profile.columns[0].name || '')
        : ''
      if (firstCol && !featureOpColumn) setFeatureOpColumn(firstCol)
    } finally {
      setFeatureProfileLoading(false)
    }
  }

  useEffect(() => {
    if (!open || !isFeatureNode) return
    void loadFeatureProfile()
  }, [open, isFeatureNode, workflow?.id, selectedNodeId])

  useEffect(() => {
    if (!isFeatureNode) {
      setFeatureProfile(null)
      setFeatureOpColumn('')
      setFeatureOpType('')
      setFeatureOpParamsText('{}')
      setFeatureOpError('')
    }
  }, [isFeatureNode])

  if (!data || !data.definition) return null

  const { definition } = data

  const handleFieldChange = (name: string, value: unknown) => {
    if (!selectedNodeId) return
    if (isPipelineOutputNode && name === 'pipeline_id') {
      updateNodeConfig(selectedNodeId, {
        pipeline_id: value,
        pipeline_node_id: undefined,
        node_id: undefined,
      })
      return
    }
    updateNodeConfig(selectedNodeId, { [name]: value })
  }

  const handleLabelChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!selectedNodeId) return
    updateNodeLabel(selectedNodeId, e.target.value)
  }

  const handleDelete = () => {
    if (!selectedNodeId) return
    removeNode(selectedNodeId)
    onClose()
  }

  const featureOperations = Array.isArray(data.config?.feature_operations)
    ? (data.config.feature_operations as Array<Record<string, unknown>>)
    : []
  const profileColumns = Array.isArray(featureProfile?.profile?.columns)
    ? featureProfile.profile.columns
    : []
  const featureColumnOptions = profileColumns.map((col: any) => {
    const colName = String(col?.name || '')
    const colType = String(col?.type || 'unknown')
    return {
      value: colName,
      label: colName ? `${colName} (${colType})` : colType,
    }
  })

  const availableFeatureOps = Array.isArray(featureProfile?.available_operations)
    ? featureProfile.available_operations
    : []
  const selectedOpMeta = availableFeatureOps.find((op: any) => String(op?.value || '') === featureOpType)
  const opNeedsColumn = selectedOpMeta ? selectedOpMeta.requires_column !== false : true

  const addFeatureOperation = (op: Record<string, unknown>) => {
    if (!selectedNodeId) return
    const next = [...featureOperations, op]
    updateNodeConfig(selectedNodeId, { feature_operations: next })
  }

  const removeFeatureOperation = (index: number) => {
    if (!selectedNodeId) return
    const next = featureOperations.filter((_, i) => i !== index)
    updateNodeConfig(selectedNodeId, { feature_operations: next })
  }

  const clearFeatureOperations = () => {
    if (!selectedNodeId) return
    updateNodeConfig(selectedNodeId, { feature_operations: [] })
  }

  const applySuggestedFeatureOps = () => {
    if (!selectedNodeId) return
    const recs = Array.isArray(featureProfile?.profile?.recommendations)
      ? featureProfile.profile.recommendations
      : []
    const next: Array<Record<string, unknown>> = []
    recs.slice(0, 20).forEach((rec: any) => {
      const column = String(rec?.column || '')
      const op = Array.isArray(rec?.operations) && rec.operations.length > 0 ? String(rec.operations[0]) : ''
      if (!column || !op) return
      next.push({
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        column,
        operation: op,
        params: {},
      })
    })
    updateNodeConfig(selectedNodeId, { feature_operations: next })
  }

  const handleAddFeatureOpFromForm = () => {
    const opType = featureOpType.trim()
    if (!opType) {
      setFeatureOpError('Select an operation')
      return
    }
    if (opNeedsColumn && !featureOpColumn.trim()) {
      setFeatureOpError('Select a column for this operation')
      return
    }

    let parsedParams: Record<string, unknown> = {}
    const raw = featureOpParamsText.trim()
    if (raw) {
      try {
        const parsed = JSON.parse(raw)
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          parsedParams = parsed as Record<string, unknown>
        } else {
          setFeatureOpError('Params must be a JSON object')
          return
        }
      } catch {
        setFeatureOpError('Params must be valid JSON')
        return
      }
    }

    addFeatureOperation({
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      column: opNeedsColumn ? featureOpColumn.trim() : undefined,
      operation: opType,
      params: parsedParams,
    })
    setFeatureOpError('')
  }

  const commonInputStyle = {
    background: 'var(--app-input-bg)',
    border: '1px solid var(--app-border-strong)',
    color: 'var(--app-text)',
  }

  const renderField = (field: ConfigField) => {
    const value = data.config[field.name]

    switch (field.type) {
      case 'text':
        return (
          <Input
            placeholder={field.placeholder}
            defaultValue={(value as string) ?? (field.defaultValue as string) ?? ''}
            onChange={(e) => handleFieldChange(field.name, e.target.value)}
            style={commonInputStyle}
          />
        )
      case 'password':
        return (
          <Input.Password
            placeholder={field.placeholder || '••••••••'}
            defaultValue={(value as string) ?? ''}
            onChange={(e) => handleFieldChange(field.name, e.target.value)}
            style={commonInputStyle}
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
      case 'select':
        if (isPipelineOutputNode && field.name === 'pipeline_id') {
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
        if (isPipelineOutputNode && (field.name === 'pipeline_node_id' || field.name === 'node_id')) {
          const selectedValue = (data.config.pipeline_node_id as string) || (data.config.node_id as string) || undefined
          return (
            <Select
              value={selectedValue}
              onChange={(v) => handleFieldChange('pipeline_node_id', v)}
              options={pipelineNodes}
              placeholder="Auto-detect latest tabular node"
              loading={pipelineNodesLoading}
              disabled={!selectedPipelineId}
              showSearch
              optionFilterProp="label"
              allowClear
              style={{ width: '100%' }}
            />
          )
        }
        return (
          <Select
            defaultValue={(value as string) ?? (field.defaultValue as string)}
            onChange={(v) => handleFieldChange(field.name, v)}
            options={field.options}
            style={{ width: '100%' }}
          />
        )
      case 'toggle':
        return (
          <Switch
            defaultChecked={(value as boolean) ?? (field.defaultValue as boolean) ?? false}
            onChange={(v) => handleFieldChange(field.name, v)}
            style={{ background: value ? '#22c55e' : undefined }}
          />
        )
      case 'textarea':
        return (
          <Input.TextArea
            placeholder={field.placeholder}
            defaultValue={(value as string) ?? (field.defaultValue as string) ?? ''}
            rows={4}
            onChange={(e) => handleFieldChange(field.name, e.target.value)}
            style={{ ...commonInputStyle, resize: 'vertical', fontFamily: 'monospace', fontSize: 12 }}
          />
        )
      case 'code':
      case 'json':
        return (
          <div style={{ border: '1px solid var(--app-border-strong)', borderRadius: 6, overflow: 'hidden' }}>
            <Editor
              height={field.language === 'python' ? '180px' : field.language === 'sql' ? '100px' : '120px'}
              language={field.language || (field.type === 'json' ? 'json' : 'plaintext')}
              value={(value as string) ?? (field.defaultValue as string) ?? ''}
              onChange={(v) => handleFieldChange(field.name, v || '')}
              theme="vs-dark"
              options={{
                minimap: { enabled: false },
                fontSize: 12,
                lineNumbers: 'off',
                scrollBeyondLastLine: false,
                wordWrap: 'on',
                padding: { top: 8, bottom: 8 },
                renderLineHighlight: 'none',
                overviewRulerLanes: 0,
              }}
            />
          </div>
        )
      default:
        return (
          <Input
            defaultValue={(value as string) ?? ''}
            onChange={(e) => handleFieldChange(field.name, e.target.value)}
            style={commonInputStyle}
          />
        )
    }
  }

  const visibleFields = definition.configFields.filter((field) => !field.name.startsWith('_'))
  const inputSample = data.executionSampleInput || []
  const outputSample = data.executionSampleOutput || []

  const sampleColumns = (rows: Record<string, unknown>[]) => {
    const keys = Array.from(
      new Set(rows.flatMap((row) => Object.keys(row || {})))
    )
    return keys.map((key) => ({
      title: key,
      dataIndex: key,
      key,
      width: 130,
      render: (value: unknown) => {
        if (value === null || value === undefined) return '-'
        if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return String(value)
        return JSON.stringify(value)
      },
    }))
  }

  const toCsvCell = (value: unknown): string => {
    if (value === null || value === undefined) return ''
    const text = typeof value === 'string'
      ? value
      : (typeof value === 'number' || typeof value === 'boolean')
      ? String(value)
      : JSON.stringify(value)
    const escaped = text.replace(/"/g, '""')
    return `"${escaped}"`
  }

  const downloadSampleAsCsv = (rows: Record<string, unknown>[], baseName: string) => {
    if (!Array.isArray(rows) || rows.length === 0) return
    const headers = Array.from(new Set(rows.flatMap((row) => Object.keys(row || {}))))
    const lines: string[] = [headers.map((h) => toCsvCell(h)).join(',')]
    rows.forEach((row) => {
      const line = headers.map((h) => toCsvCell((row || {})[h])).join(',')
      lines.push(line)
    })
    const csv = lines.join('\n')
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
    link.href = url
    link.download = `${baseName}-${timestamp}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  }

  const panelContent = (
    <>
      <div style={{
        background: 'var(--app-card-bg)',
        borderBottom: '1px solid var(--app-border-strong)',
        padding: isFeatureOverlay ? '14px 22px' : '14px 16px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        flexShrink: 0,
      }}>
        <Space>
          <div style={{
            width: 30,
            height: 30,
            background: definition.bgColor,
            border: `1px solid ${definition.color}30`,
            borderRadius: 7,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: 15,
          }}>
            {definition.icon}
          </div>
          <div>
            <Text style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 13 }}>{definition.label}</Text>
            <br />
            <Tag style={{
              background: `${definition.color}15`,
              border: `1px solid ${definition.color}30`,
              color: definition.color,
              borderRadius: 4,
              fontSize: 10,
              padding: '0 5px',
            }}>
              {definition.category}
            </Tag>
          </div>
        </Space>
        <Space>
          <Button type="text" icon={<CloseOutlined />} size="small" style={{ color: 'var(--app-text-subtle)' }} onClick={onClose} />
        </Space>
      </div>

      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        size="small"
        style={{ padding: isFeatureOverlay ? '0 22px' : '0 16px', flexShrink: 0 }}
        tabBarStyle={{ marginBottom: 0, borderBottom: '1px solid var(--app-border)' }}
        items={[
          { key: 'config', label: 'Configuration' },
          { key: 'samples', label: 'Samples' },
          { key: 'info', label: 'Info' },
        ]}
      />

      <div style={{ flex: 1, overflowY: 'auto', padding: isFeatureOverlay ? '20px 22px' : '16px' }}>
        {activeTab === 'config' && (
          <Form layout="vertical" form={form} style={{ maxWidth: isFeatureOverlay ? 1320 : '100%' }}>
            <Form.Item
              label={<Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Node Label</Text>}
              style={{ marginBottom: 14 }}
            >
              <Input
                defaultValue={data.label}
                onChange={handleLabelChange}
                style={commonInputStyle}
              />
            </Form.Item>

            {visibleFields.length > 0 ? (
              <Divider style={{ borderColor: 'var(--app-border)', margin: '4px 0 14px' }}>
                <Text style={{ color: 'var(--app-text-dim)', fontSize: 11 }}>Parameters</Text>
              </Divider>
            ) : null}

            {visibleFields.map((field) => (
              <Form.Item
                key={field.name}
                label={(
                  <Space size={4}>
                    <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>{field.label}</Text>
                    {field.required && <Text style={{ color: '#ef4444', fontSize: 12 }}>*</Text>}
                    {field.description && (
                      <Tooltip title={field.description}>
                        <InfoCircleOutlined style={{ color: 'var(--app-text-dim)', fontSize: 11 }} />
                      </Tooltip>
                    )}
                  </Space>
                )}
                style={{ marginBottom: 14 }}
              >
                {renderField(field)}
              </Form.Item>
            ))}

            {isFeatureNode && (
              <>
                <Divider style={{ borderColor: 'var(--app-border)', margin: '8px 0 14px' }}>
                  <Text style={{ color: 'var(--app-text-dim)', fontSize: 11 }}>Feature Engineering Studio</Text>
                </Divider>

                <div style={{
                  display: 'grid',
                  gap: 12,
                  gridTemplateColumns: isFeatureOverlay ? 'minmax(340px, 440px) minmax(420px, 1fr)' : '1fr',
                  alignItems: 'start',
                }}>
                  <Space direction="vertical" size={10} style={{ width: '100%' }}>
                    <Alert
                      type="info"
                      showIcon
                      message="Define column-level feature operations and run workflow to apply them."
                    />

                    <div style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '10px' }}>
                      <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Add Feature Operation</Text>
                      <Space direction="vertical" size={8} style={{ width: '100%', marginTop: 8 }}>
                        <Select
                          value={featureOpType || undefined}
                          onChange={(v) => {
                            setFeatureOpType(v)
                            setFeatureOpError('')
                          }}
                          options={availableFeatureOps.map((op: any) => ({
                            value: String(op?.value || ''),
                            label: `${String(op?.label || op?.value || '')} [${String(op?.category || 'general')}]`,
                          }))}
                          placeholder="Select operation"
                          showSearch
                          optionFilterProp="label"
                          style={{ width: '100%' }}
                        />
                        {opNeedsColumn && (
                          <Select
                            value={featureOpColumn || undefined}
                            onChange={(v) => {
                              setFeatureOpColumn(v)
                              setFeatureOpError('')
                            }}
                            options={featureColumnOptions}
                            placeholder="Select column"
                            showSearch
                            optionFilterProp="label"
                            style={{ width: '100%' }}
                          />
                        )}
                        <Input.TextArea
                          value={featureOpParamsText}
                          onChange={(e) => {
                            setFeatureOpParamsText(e.target.value)
                            setFeatureOpError('')
                          }}
                          rows={3}
                          style={{ ...commonInputStyle, resize: 'vertical', fontFamily: 'monospace', fontSize: 12 }}
                          placeholder={selectedOpMeta?.params?.length
                            ? `JSON params, e.g. {"${String(selectedOpMeta.params[0])}": "value"}`
                            : 'JSON params (optional), e.g. {}'}
                        />
                        {featureOpError ? (
                          <Alert type="error" showIcon message={featureOpError} />
                        ) : null}
                        <Space wrap>
                          <Button type="primary" onClick={handleAddFeatureOpFromForm}>
                            Add Operation
                          </Button>
                          <Button onClick={applySuggestedFeatureOps} disabled={featureProfileLoading || profileColumns.length === 0}>
                            Apply Suggested
                          </Button>
                          <Button danger onClick={clearFeatureOperations} disabled={featureOperations.length === 0}>
                            Clear All
                          </Button>
                        </Space>
                      </Space>
                    </div>

                    <div style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '10px' }}>
                      <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>
                        Configured Operations ({featureOperations.length})
                      </Text>
                      <Table
                        style={{ marginTop: 8 }}
                        size="small"
                        pagination={false}
                        dataSource={featureOperations.map((op, idx) => ({
                          key: String(op.id || `fop-${idx}`),
                          idx,
                          column: String(op.column || ''),
                          operation: String(op.operation || ''),
                          params: op.params && typeof op.params === 'object' ? JSON.stringify(op.params) : '{}',
                        }))}
                        columns={[
                          {
                            title: '#',
                            dataIndex: 'idx',
                            key: 'idx',
                            width: 36,
                            render: (v: number) => String(v + 1),
                          },
                          {
                            title: 'Column',
                            dataIndex: 'column',
                            key: 'column',
                            width: 110,
                            render: (v: string) => v || <Text type="secondary">n/a</Text>,
                          },
                          {
                            title: 'Operation',
                            dataIndex: 'operation',
                            key: 'operation',
                            width: 120,
                          },
                          {
                            title: 'Params',
                            dataIndex: 'params',
                            key: 'params',
                            render: (v: string) => <Text style={{ fontFamily: 'monospace', fontSize: 11 }}>{v}</Text>,
                          },
                          {
                            title: '',
                            key: 'action',
                            width: 42,
                            render: (_: unknown, record: { idx: number }) => (
                              <Button
                                type="text"
                                danger
                                size="small"
                                icon={<DeleteOutlined />}
                                onClick={() => removeFeatureOperation(record.idx)}
                              />
                            ),
                          },
                        ]}
                        scroll={{ x: true }}
                        locale={{ emptyText: 'No operations configured yet' }}
                      />
                    </div>
                  </Space>

                  <div style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '10px' }}>
                    <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                      <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>
                        Profile: {Number(featureProfile?.profile?.row_count || 0).toLocaleString()} rows, {profileColumns.length} columns, duplicates {Number(featureProfile?.profile?.duplicate_rows || 0).toLocaleString()}
                      </Text>
                      <Button size="small" loading={featureProfileLoading} onClick={() => void loadFeatureProfile()}>
                        Refresh Profile
                      </Button>
                    </Space>
                    <Table
                      style={{ marginTop: 8 }}
                      size="small"
                      pagination={{ pageSize: 12, size: 'small' }}
                      dataSource={profileColumns.map((col: any, idx: number) => ({
                        key: `${String(col?.name || 'col')}-${idx}`,
                        name: String(col?.name || ''),
                        type: String(col?.type || ''),
                        missing_pct: Number(col?.missing_pct || 0),
                        unique_count: Number(col?.unique_count || 0),
                        quality_flags: Array.isArray(col?.quality_flags) ? col.quality_flags : [],
                        suggested_operations: Array.isArray(col?.suggested_operations) ? col.suggested_operations : [],
                      }))}
                      columns={[
                        {
                          title: 'Column',
                          dataIndex: 'name',
                          key: 'name',
                          width: 160,
                          render: (v: string) => <Text style={{ color: 'var(--app-text)' }}>{v || '-'}</Text>,
                        },
                        {
                          title: 'Type',
                          dataIndex: 'type',
                          key: 'type',
                          width: 90,
                          render: (v: string) => (
                            <Tag style={{ marginInlineEnd: 0, background: 'var(--app-border)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-subtle)' }}>
                              {v || 'unknown'}
                            </Tag>
                          ),
                        },
                        {
                          title: 'Missing %',
                          dataIndex: 'missing_pct',
                          key: 'missing_pct',
                          width: 95,
                          render: (v: number) => `${v.toFixed(2)}%`,
                        },
                        {
                          title: 'Unique',
                          dataIndex: 'unique_count',
                          key: 'unique_count',
                          width: 90,
                        },
                        {
                          title: 'Suggested',
                          dataIndex: 'suggested_operations',
                          key: 'suggested_operations',
                          width: 240,
                          render: (vals: string[]) => (
                            <Space size={[4, 4]} wrap>
                              {vals.slice(0, 4).map((op) => (
                                <Tag key={op} style={{ marginInlineEnd: 0, background: '#0f172a', border: '1px solid #334155', color: '#cbd5e1' }}>
                                  {op}
                                </Tag>
                              ))}
                            </Space>
                          ),
                        },
                        {
                          title: 'Flags',
                          dataIndex: 'quality_flags',
                          key: 'quality_flags',
                          width: 200,
                          render: (vals: string[]) => (
                            <Space size={[4, 4]} wrap>
                              {vals.slice(0, 3).map((flag) => (
                                <Tag key={flag} style={{ marginInlineEnd: 0, background: '#3f1d2e', border: '1px solid #7f1d1d', color: '#fecaca' }}>
                                  {flag}
                                </Tag>
                              ))}
                            </Space>
                          ),
                        },
                      ]}
                      scroll={{ x: true }}
                      locale={{ emptyText: featureProfileLoading ? 'Profiling source data…' : 'No column profile available yet' }}
                    />
                  </div>
                </div>
              </>
            )}
          </Form>
        )}

        {activeTab === 'samples' && (
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            {!data.executionSampleInput && !data.executionSampleOutput && (
              <div style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '14px 12px' }}>
                <Empty
                  description={<Text style={{ color: 'var(--app-text-subtle)' }}>Run workflow to preview node input/output samples</Text>}
                  image={Empty.PRESENTED_IMAGE_SIMPLE}
                />
              </div>
            )}

            {data.executionSampleInput && (
              <div style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '10px' }}>
                <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                  <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Input Sample ({inputSample.length} rows)</Text>
                  <Button
                    size="small"
                    icon={<DownloadOutlined />}
                    onClick={() => downloadSampleAsCsv(inputSample, 'mlops-input-sample')}
                    disabled={inputSample.length === 0}
                  >
                    Download CSV
                  </Button>
                </Space>
                <Table
                  style={{ marginTop: 8 }}
                  size="small"
                  pagination={{
                    pageSize: 25,
                    pageSizeOptions: ['25', '50', '100'],
                    showSizeChanger: true,
                    hideOnSinglePage: false,
                  }}
                  columns={sampleColumns(inputSample)}
                  dataSource={inputSample.map((row, idx) => ({ key: `in-${idx}`, ...row }))}
                  scroll={{ x: true }}
                />
              </div>
            )}

            {data.executionSampleOutput && (
              <div style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '10px' }}>
                <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                  <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Output Sample ({outputSample.length} rows)</Text>
                  <Button
                    size="small"
                    icon={<DownloadOutlined />}
                    onClick={() => downloadSampleAsCsv(outputSample, 'mlops-output-sample')}
                    disabled={outputSample.length === 0}
                  >
                    Download CSV
                  </Button>
                </Space>
                <Table
                  style={{ marginTop: 8 }}
                  size="small"
                  pagination={{
                    pageSize: 25,
                    pageSizeOptions: ['25', '50', '100'],
                    showSizeChanger: true,
                    hideOnSinglePage: false,
                  }}
                  columns={sampleColumns(outputSample)}
                  dataSource={outputSample.map((row, idx) => ({ key: `out-${idx}`, ...row }))}
                  scroll={{ x: true }}
                />
              </div>
            )}
          </Space>
        )}

        {activeTab === 'info' && (
          <Space direction="vertical" size={10} style={{ width: '100%' }}>
            <div style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '12px 16px' }}>
              <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Description</Text>
              <br />
              <Text style={{ color: '#d1d5db', fontSize: 13, marginTop: 4, display: 'block' }}>
                {definition.description}
              </Text>
            </div>
            <div style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '12px 16px' }}>
              <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Handles</Text>
              <br />
              <Space style={{ marginTop: 6 }}>
                <Tag style={{ background: 'var(--app-border)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)', fontSize: 11 }}>
                  {definition.inputs} input{definition.inputs !== 1 ? 's' : ''}
                </Tag>
                <Tag style={{ background: 'var(--app-border)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)', fontSize: 11 }}>
                  {definition.outputs} output{definition.outputs !== 1 ? 's' : ''}
                </Tag>
              </Space>
            </div>
            {definition.tags && (
              <div style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '12px 16px' }}>
                <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Tags</Text>
                <br />
                <Space wrap style={{ marginTop: 6 }}>
                  {definition.tags.map((tag) => (
                    <Tag key={tag} style={{ background: 'var(--app-border)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-subtle)', fontSize: 11, borderRadius: 4 }}>
                      {tag}
                    </Tag>
                  ))}
                </Space>
              </div>
            )}
          </Space>
        )}
      </div>

      <div
        style={{
          flexShrink: 0,
          borderTop: '1px solid var(--app-border-strong)',
          background: 'var(--app-card-bg)',
          padding: isFeatureOverlay ? '10px 22px 12px' : '10px 16px 12px',
        }}
      >
        <Space direction="vertical" size={6} style={{ width: '100%' }}>
          <Text style={{ color: '#ef4444', fontSize: 11, fontWeight: 700 }}>Danger Zone</Text>
          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
            Delete this node from pipeline canvas.
          </Text>
          <Button
            danger
            icon={<DeleteOutlined />}
            onClick={handleDelete}
            style={{ width: '100%' }}
          >
            Delete Node
          </Button>
        </Space>
      </div>
    </>
  )

  if (isFeatureOverlay) {
    return (
      <Modal
        open={open}
        onCancel={onClose}
        footer={null}
        closable={false}
        maskClosable
        centered
        width="96vw"
        styles={{
          content: {
            padding: 0,
            borderRadius: 12,
            overflow: 'hidden',
            border: '1px solid var(--app-border-strong)',
            background: 'var(--app-panel-bg)',
            height: '96vh',
            display: 'flex',
            flexDirection: 'column',
          },
          body: {
            padding: 0,
            flex: 1,
            minHeight: 0,
            display: 'flex',
            flexDirection: 'column',
          },
        }}
      >
        {panelContent}
      </Modal>
    )
  }

  return (
    <Drawer
      open={open}
      onClose={onClose}
      mask={false}
      width={360}
      placement="right"
      closable={false}
      styles={{
        body: {
          background: 'var(--app-panel-bg)',
          padding: 0,
          overflow: 'hidden',
          display: 'flex',
          flexDirection: 'column',
        },
        header: { display: 'none' },
        wrapper: { boxShadow: '-4px 0 20px rgba(0,0,0,0.4)' },
      }}
    >
      <div
        onMouseDownCapture={touchDrawerActivity}
        onTouchStartCapture={touchDrawerActivity}
        onKeyDownCapture={touchDrawerActivity}
        onWheelCapture={touchDrawerActivity}
      >
      {panelContent}
      </div>
    </Drawer>
  )
}
