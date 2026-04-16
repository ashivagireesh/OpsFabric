import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Col,
  Empty,
  Input,
  List,
  Row,
  Select,
  Space,
  Spin,
  Statistic,
  Table,
  Typography,
  message,
} from 'antd'
import {
  DatabaseOutlined,
  MessageOutlined,
  RobotOutlined,
  SearchOutlined,
  SendOutlined,
} from '@ant-design/icons'
import axios from 'axios'
import ReactECharts from 'echarts-for-react'

const { Title, Text } = Typography
const { TextArea } = Input

const API_BASE = import.meta.env.DEV ? 'http://localhost:8001' : ''
const http = axios.create({ baseURL: API_BASE, timeout: 60000 })

type SourceType = 'sample' | 'pipeline' | 'file' | 'mlops'

interface SourceConfig {
  source_type: SourceType
  dataset?: string
  pipeline_id?: string
  file_path?: string
  mlops_workflow_id?: string
  mlops_output_mode?: 'predictions' | 'metrics' | 'monitor' | 'evaluation'
  sql?: string
}

interface OptionItem {
  value: string
  label: string
}

interface SchemaResponse {
  row_count: number
  sample_size: number
  columns: string[]
  column_details: Array<{ name: string; type: string }>
  suggested_metrics: string[]
  suggested_dimensions: string[]
  sample_rows: Record<string, unknown>[]
}

interface ChatTurn {
  id: string
  role: 'user' | 'assistant'
  content: string
  meta?: {
    corrected_question?: string
    corrections?: Record<string, string>
    semantic_engine?: string
    response_engine?: {
      used?: boolean
      model?: string
      reason?: string | null
    }
    one_line_highlight?: string
    table_response_columns?: string[]
    table_response_rows?: Record<string, unknown>[]
    selected_fields?: {
      metrics?: string[]
      dimensions?: string[]
      relevant_columns?: string[]
    }
    retrieval?: Array<{ score: number; row: Record<string, unknown> }>
  }
}

type NLPTableRow = Record<string, unknown> & { key: string }

const SAMPLE_DATASETS = [
  { value: 'sales', label: 'Sales' },
  { value: 'web_analytics', label: 'Web Analytics' },
  { value: 'financials', label: 'Financials' },
  { value: 'employees', label: 'Employees' },
  { value: 'supply_chain', label: 'Supply Chain' },
  { value: 'customer', label: 'Customer' },
]

function buildSourcePayload(config: SourceConfig): Record<string, unknown> {
  return {
    source_type: config.source_type,
    dataset: config.dataset,
    pipeline_id: config.pipeline_id,
    file_path: config.file_path,
    mlops_workflow_id: config.mlops_workflow_id,
    mlops_output_mode: config.mlops_output_mode,
    sql: config.sql,
  }
}

export default function NLPChatStudio() {
  const [messageApi, contextHolder] = message.useMessage()

  const [sourceConfig, setSourceConfig] = useState<SourceConfig>({
    source_type: 'sample',
    dataset: 'sales',
    mlops_output_mode: 'predictions',
  })

  const [pipelineOptions, setPipelineOptions] = useState<OptionItem[]>([])
  const [mlopsOptions, setMLOpsOptions] = useState<OptionItem[]>([])
  const [loadingSources, setLoadingSources] = useState(false)

  const [schemaLoading, setSchemaLoading] = useState(false)
  const [schema, setSchema] = useState<SchemaResponse | null>(null)
  const [metricFields, setMetricFields] = useState<string[]>([])
  const [dimensionFields, setDimensionFields] = useState<string[]>([])
  const [domainContext, setDomainContext] = useState('')

  const [question, setQuestion] = useState('')
  const [asking, setAsking] = useState(false)
  const [chatTurns, setChatTurns] = useState<ChatTurn[]>([])

  useEffect(() => {
    let mounted = true
    const load = async () => {
      setLoadingSources(true)
      try {
        const [pipelineResp, mlopsResp] = await Promise.all([
          http.get('/api/pipelines'),
          http.get('/api/mlops/workflows'),
        ])
        if (!mounted) return

        const pOptions = Array.isArray(pipelineResp.data)
          ? pipelineResp.data.map((item: any) => ({
              value: String(item.id),
              label: String(item.name || item.id),
            }))
          : []
        const mOptions = Array.isArray(mlopsResp.data)
          ? mlopsResp.data.map((item: any) => ({
              value: String(item.id),
              label: String(item.name || item.id),
            }))
          : []

        setPipelineOptions(pOptions)
        setMLOpsOptions(mOptions)
      } catch {
        if (mounted) {
          setPipelineOptions([])
          setMLOpsOptions([])
        }
      } finally {
        if (mounted) setLoadingSources(false)
      }
    }

    void load()
    return () => {
      mounted = false
    }
  }, [])

  const handleSourceTypeChange = useCallback((next: SourceType) => {
    setSourceConfig((prev) => ({
      ...prev,
      source_type: next,
      dataset: next === 'sample' ? (prev.dataset || 'sales') : undefined,
      pipeline_id: next === 'pipeline' ? prev.pipeline_id : undefined,
      file_path: next === 'file' ? prev.file_path : undefined,
      mlops_workflow_id: next === 'mlops' ? prev.mlops_workflow_id : undefined,
      mlops_output_mode: next === 'mlops' ? (prev.mlops_output_mode || 'predictions') : undefined,
    }))
    setSchema(null)
    setMetricFields([])
    setDimensionFields([])
  }, [])

  const loadSchema = useCallback(async () => {
    if (sourceConfig.source_type === 'pipeline' && !sourceConfig.pipeline_id) {
      messageApi.warning('Select a pipeline source')
      return
    }
    if (sourceConfig.source_type === 'file' && !(sourceConfig.file_path || '').trim()) {
      messageApi.warning('Provide a file path')
      return
    }
    if (sourceConfig.source_type === 'mlops' && !sourceConfig.mlops_workflow_id) {
      messageApi.warning('Select an MLOps workflow')
      return
    }

    setSchemaLoading(true)
    try {
      const payload = buildSourcePayload(sourceConfig)
      const response = await http.post('/api/nlp-chat/schema', payload)
      const data = response.data as SchemaResponse
      setSchema(data)
      setMetricFields(Array.isArray(data.suggested_metrics) ? data.suggested_metrics.slice(0, 4) : [])
      setDimensionFields(Array.isArray(data.suggested_dimensions) ? data.suggested_dimensions.slice(0, 4) : [])
      messageApi.success(`Datasource connected: ${data.row_count} rows loaded`)
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      messageApi.error(typeof detail === 'string' ? detail : 'Failed to connect datasource')
      setSchema(null)
      setMetricFields([])
      setDimensionFields([])
    } finally {
      setSchemaLoading(false)
    }
  }, [messageApi, sourceConfig])

  const askQuestion = useCallback(async () => {
    const query = (question || '').trim()
    if (!query) {
      messageApi.warning('Type a question first')
      return
    }
    if (!schema) {
      messageApi.warning('Connect datasource and load fields first')
      return
    }

    const userTurn: ChatTurn = {
      id: `${Date.now()}-user`,
      role: 'user',
      content: query,
    }
    setChatTurns((prev) => [...prev, userTurn])
    setQuestion('')
    setAsking(true)

    try {
      const payload = {
        ...buildSourcePayload(sourceConfig),
        question: query,
        metric_fields: metricFields,
        dimension_fields: dimensionFields,
        domain_context: domainContext,
        top_k: 12,
        include_rows: 8,
        semantic_mode: 'auto',
        chat_history: chatTurns.map((t) => ({ role: t.role, content: t.content })),
      }
      const response = await http.post('/api/nlp-chat/ask', payload)
      const data = response.data || {}

      const assistantTurn: ChatTurn = {
        id: `${Date.now()}-assistant`,
        role: 'assistant',
        content: String(data.answer || 'No answer generated.'),
        meta: {
          corrected_question: typeof data.corrected_question === 'string' ? data.corrected_question : undefined,
          corrections: (data.corrections && typeof data.corrections === 'object') ? data.corrections : undefined,
          semantic_engine: typeof data.semantic_engine === 'string' ? data.semantic_engine : undefined,
          response_engine: data.response_engine || data.llm_engine,
          one_line_highlight: typeof data.one_line_highlight === 'string' ? data.one_line_highlight : undefined,
          table_response_columns: Array.isArray(data.table_response_columns)
            ? data.table_response_columns.map((col: unknown) => String(col))
            : undefined,
          table_response_rows: Array.isArray(data.table_response_rows)
            ? data.table_response_rows.filter((row: unknown) => !!row && typeof row === 'object')
                .map((row: any) => row as Record<string, unknown>)
            : undefined,
          selected_fields: (data.selected_fields && typeof data.selected_fields === 'object')
            ? {
                metrics: Array.isArray(data.selected_fields.metrics)
                  ? data.selected_fields.metrics.map((v: unknown) => String(v))
                  : [],
                dimensions: Array.isArray(data.selected_fields.dimensions)
                  ? data.selected_fields.dimensions.map((v: unknown) => String(v))
                  : [],
                relevant_columns: Array.isArray(data.selected_fields.relevant_columns)
                  ? data.selected_fields.relevant_columns.map((v: unknown) => String(v))
                  : [],
              }
            : undefined,
          retrieval: Array.isArray(data.retrieval)
            ? data.retrieval
                .slice(0, 3)
                .map((r: any) => ({
                  score: Number(r?.score || 0),
                  row: (r?.row && typeof r.row === 'object') ? r.row : {},
                }))
            : [],
        },
      }
      setChatTurns((prev) => [...prev, assistantTurn])
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      messageApi.error(typeof detail === 'string' ? detail : 'Failed to get chatbot response')
      setChatTurns((prev) => [
        ...prev,
        {
          id: `${Date.now()}-assistant-error`,
          role: 'assistant',
          content: 'Unable to answer right now. Check datasource configuration and try again.',
        },
      ])
    } finally {
      setAsking(false)
    }
  }, [chatTurns, dimensionFields, domainContext, messageApi, metricFields, question, schema, sourceConfig])

  const columnOptions = useMemo(
    () => (schema?.columns || []).map((col) => ({ value: col, label: col })),
    [schema?.columns],
  )

  const latestAssistantTurn = useMemo(
    () => [...chatTurns].reverse().find((t) => t.role === 'assistant') || null,
    [chatTurns],
  )

  const latestTableRows = useMemo<NLPTableRow[]>(
    () => (latestAssistantTurn?.meta?.table_response_rows || []).map((row, idx) => {
      const base = row as Record<string, unknown>
      return { key: String(base._rank ?? idx), ...base }
    }),
    [latestAssistantTurn],
  )

  const latestTableColumns = useMemo<any[]>(
    () => (
      latestAssistantTurn?.meta?.table_response_columns || []
    ).map((col) => {
      const values = latestTableRows
        .map((row) => row[col])
        .filter((value) => value !== null && value !== undefined)

      const toNumber = (value: unknown): number | null => {
        if (typeof value === 'number') return Number.isFinite(value) ? value : null
        if (typeof value === 'string' && value.trim() !== '') {
          const parsed = Number(value.replace(/,/g, ''))
          return Number.isFinite(parsed) ? parsed : null
        }
        return null
      }

      const numericValues = values.map((value) => toNumber(value)).filter((value): value is number => value !== null)
      const numericLike = values.length > 0 && numericValues.length >= Math.ceil(values.length * 0.6)
      const distinctTexts = Array.from(new Set(values.map((value) => (typeof value === 'object' ? JSON.stringify(value) : String(value)))))
      const canFilter = !numericLike && distinctTexts.length > 1 && distinctTexts.length <= 20

      return {
        title: col,
        dataIndex: col,
        key: col,
        width: col === '_rank' ? 76 : col === '_score' ? 120 : 180,
        ellipsis: true as const,
        sorter: (a: NLPTableRow, b: NLPTableRow) => {
          if (numericLike) {
            const left = toNumber(a[col]) ?? Number.NEGATIVE_INFINITY
            const right = toNumber(b[col]) ?? Number.NEGATIVE_INFINITY
            return left - right
          }
          const left = String(a[col] ?? '').toLowerCase()
          const right = String(b[col] ?? '').toLowerCase()
          return left.localeCompare(right)
        },
        filters: canFilter ? distinctTexts.map((text) => ({ text, value: text })) : undefined,
        onFilter: canFilter
          ? (value: string | number | bigint | boolean, record: NLPTableRow) => String(record[col] ?? '') === String(value)
          : undefined,
        filterSearch: canFilter,
        render: (value: unknown) => {
          if (value === null || value === undefined) return '-'
          if (typeof value === 'number') return Number.isFinite(value) ? value : String(value)
          if (typeof value === 'object') return JSON.stringify(value)
          return String(value)
        },
      }
    }),
    [latestAssistantTurn, latestTableRows],
  )

  const latestChartOption = useMemo(() => {
    const meta = latestAssistantTurn?.meta
    const rawRows = meta?.table_response_rows || []
    const rawCols = meta?.table_response_columns || []
    if (!rawRows.length || !rawCols.length) return null

    const rows = rawRows.slice(0, 12)
    const cols = rawCols

    const isNumericLike = (value: unknown): boolean => {
      if (typeof value === 'number') return Number.isFinite(value)
      if (typeof value === 'string' && value.trim() !== '') {
        const parsed = Number(value)
        return Number.isFinite(parsed)
      }
      return false
    }

    const numericCols = cols.filter((col) => col !== '_rank' && rows.some((row) => isNumericLike(row[col])))
    const preferredMetric = (meta?.selected_fields?.metrics || []).find((field) => numericCols.includes(field))
    const valueCol = preferredMetric || (numericCols.includes('sum') ? 'sum' : numericCols[0])
    if (!valueCol) return null

    const preferredDimension = (meta?.selected_fields?.dimensions || []).find((field) => cols.includes(field))
    const dimensionCandidates = cols.filter((col) => col !== valueCol && col !== '_score')
    const categoryCol = preferredDimension || dimensionCandidates.find((col) => col !== '_rank') || '_rank'

    const labels: string[] = []
    const values: number[] = []
    rows.forEach((row) => {
      const label = String(row[categoryCol] ?? row._rank ?? '').trim()
      const raw = row[valueCol]
      const numericValue = typeof raw === 'number' ? raw : Number(raw)
      if (!label || !Number.isFinite(numericValue)) return
      labels.push(label)
      values.push(Number(numericValue))
    })
    if (!labels.length) return null

    const isTimeLike = /date|time|month|year|day|week|quarter/i.test(categoryCol)
    return {
      grid: { left: 40, right: 20, top: 24, bottom: 50 },
      tooltip: { trigger: 'axis' },
      xAxis: {
        type: 'category',
        data: labels,
        axisLabel: { rotate: labels.length > 6 ? 25 : 0, color: 'var(--app-text-subtle)' },
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: 'var(--app-text-subtle)' },
      },
      series: [
        isTimeLike
          ? {
              type: 'line',
              data: values,
              smooth: true,
              symbol: 'circle',
              lineStyle: { width: 2, color: '#2563eb' },
              itemStyle: { color: '#2563eb' },
              areaStyle: { color: 'rgba(37, 99, 235, 0.18)' },
            }
          : {
              type: 'bar',
              data: values,
              itemStyle: { color: '#2563eb', borderRadius: [4, 4, 0, 0] },
              barMaxWidth: 42,
            },
      ],
    }
  }, [latestAssistantTurn])

  return (
    <div style={{ minHeight: '100vh', background: 'var(--app-shell-bg)', padding: '24px' }}>
      {contextHolder}

      <div style={{ marginBottom: 18 }}>
        <Title level={3} style={{ margin: 0, color: 'var(--app-text)', display: 'flex', alignItems: 'center', gap: 10 }}>
          <MessageOutlined />
          NLP Chat Studio
        </Title>
        <Text style={{ color: 'var(--app-text-subtle)' }}>
          Connect any datasource, define metric/dimension fields, set domain context, and chat with semantic retrieval + typo autocorrection.
        </Text>
      </div>

      <Row gutter={[16, 16]}>
        <Col xs={24} xl={10}>
          <Card
            title={<Space><DatabaseOutlined />Datasource & Domain Setup</Space>}
            style={{ background: 'var(--app-panel-bg)', border: '1px solid var(--app-border)' }}
            bodyStyle={{ paddingBottom: 12 }}
          >
            <Space direction="vertical" size={12} style={{ width: '100%' }}>
              <div>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Datasource Type</Text>
                <Select
                  value={sourceConfig.source_type}
                  onChange={(value) => handleSourceTypeChange(value as SourceType)}
                  options={[
                    { value: 'sample', label: 'Sample Dataset' },
                    { value: 'pipeline', label: 'ETL Pipeline Output' },
                    { value: 'file', label: 'File' },
                    { value: 'mlops', label: 'MLOps Output' },
                  ]}
                  style={{ width: '100%', marginTop: 4 }}
                />
              </div>

              {sourceConfig.source_type === 'sample' && (
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Dataset</Text>
                  <Select
                    value={sourceConfig.dataset || 'sales'}
                    onChange={(value) => setSourceConfig((prev) => ({ ...prev, dataset: value }))}
                    options={SAMPLE_DATASETS}
                    style={{ width: '100%', marginTop: 4 }}
                  />
                </div>
              )}

              {sourceConfig.source_type === 'pipeline' && (
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Pipeline</Text>
                  <Select
                    value={sourceConfig.pipeline_id || undefined}
                    onChange={(value) => setSourceConfig((prev) => ({ ...prev, pipeline_id: value }))}
                    options={pipelineOptions}
                    loading={loadingSources}
                    placeholder="Select pipeline"
                    showSearch
                    optionFilterProp="label"
                    style={{ width: '100%', marginTop: 4 }}
                  />
                </div>
              )}

              {sourceConfig.source_type === 'file' && (
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>File Path</Text>
                  <Input
                    value={sourceConfig.file_path || ''}
                    onChange={(e) => setSourceConfig((prev) => ({ ...prev, file_path: e.target.value }))}
                    placeholder="/absolute/path/to/data.csv"
                    style={{ marginTop: 4 }}
                  />
                </div>
              )}

              {sourceConfig.source_type === 'mlops' && (
                <>
                  <div>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>MLOps Workflow</Text>
                    <Select
                      value={sourceConfig.mlops_workflow_id || undefined}
                      onChange={(value) => setSourceConfig((prev) => ({ ...prev, mlops_workflow_id: value }))}
                      options={mlopsOptions}
                      loading={loadingSources}
                      placeholder="Select workflow"
                      showSearch
                      optionFilterProp="label"
                      style={{ width: '100%', marginTop: 4 }}
                    />
                  </div>
                  <div>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>MLOps Output Mode</Text>
                    <Select
                      value={sourceConfig.mlops_output_mode || 'predictions'}
                      onChange={(value) => setSourceConfig((prev) => ({ ...prev, mlops_output_mode: value as 'predictions' | 'metrics' | 'monitor' | 'evaluation' }))}
                      options={[
                        { value: 'predictions', label: 'Predictions' },
                        { value: 'metrics', label: 'Metrics Summary' },
                        { value: 'monitor', label: 'Monitoring Runs' },
                        { value: 'evaluation', label: 'Evaluation Output' },
                      ]}
                      style={{ width: '100%', marginTop: 4 }}
                    />
                  </div>
                </>
              )}

              <div>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Optional SQL</Text>
                <Input
                  value={sourceConfig.sql || ''}
                  onChange={(e) => setSourceConfig((prev) => ({ ...prev, sql: e.target.value || undefined }))}
                  placeholder="SELECT * FROM data WHERE ..."
                  style={{ marginTop: 4 }}
                />
              </div>

              <Button
                type="primary"
                icon={<SearchOutlined />}
                onClick={() => { void loadSchema() }}
                loading={schemaLoading}
                style={{ background: 'linear-gradient(135deg, #2563eb, #1d4ed8)', border: 'none' }}
              >
                Connect Datasource & Load Fields
              </Button>

              {schema && (
                <>
                  <Row gutter={12}>
                    <Col span={12}><Statistic title="Rows" value={schema.row_count} /></Col>
                    <Col span={12}><Statistic title="Columns" value={schema.columns.length} /></Col>
                  </Row>

                  <div>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Metric Fields</Text>
                    <Select
                      mode="multiple"
                      value={metricFields}
                      onChange={(values) => setMetricFields(values)}
                      options={columnOptions}
                      placeholder="Select metric fields"
                      style={{ width: '100%', marginTop: 4 }}
                    />
                  </div>

                  <div>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Dimension Fields</Text>
                    <Select
                      mode="multiple"
                      value={dimensionFields}
                      onChange={(values) => setDimensionFields(values)}
                      options={columnOptions}
                      placeholder="Select dimension fields"
                      style={{ width: '100%', marginTop: 4 }}
                    />
                  </div>

                  <div>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Domain Understanding Context</Text>
                    <TextArea
                      rows={4}
                      value={domainContext}
                      onChange={(e) => setDomainContext(e.target.value)}
                      placeholder="Describe business domain, KPI semantics, acronyms, and interpretation rules for higher-answer accuracy."
                      style={{ marginTop: 4 }}
                    />
                  </div>

                </>
              )}
            </Space>
          </Card>
        </Col>

        <Col xs={24} xl={14}>
          <Card
            title={<Space><RobotOutlined />NLP Chatbot</Space>}
            style={{ background: 'var(--app-panel-bg)', border: '1px solid var(--app-border)' }}
          >
            {latestAssistantTurn?.meta && (
              <div style={{ marginBottom: 12 }}>
                <Alert
                  showIcon
                  type="success"
                  message={latestAssistantTurn.content}
                  style={{ marginBottom: 8 }}
                />

                {latestAssistantTurn.meta.one_line_highlight && latestAssistantTurn.meta.one_line_highlight !== latestAssistantTurn.content && (
                  <Alert
                    showIcon
                    type="info"
                    message={latestAssistantTurn.meta.one_line_highlight}
                    style={{ marginBottom: 8 }}
                  />
                )}

                {latestChartOption && (
                  <div style={{ marginBottom: 8, border: '1px solid var(--app-border)', borderRadius: 10, background: 'var(--app-card-bg)', padding: 6 }}>
                    <ReactECharts option={latestChartOption} style={{ height: 240, width: '100%' }} opts={{ renderer: 'svg' }} />
                  </div>
                )}

                {latestTableColumns.length > 0 && latestTableRows.length > 0 && (
                  <Table
                    size="small"
                    columns={latestTableColumns}
                    dataSource={latestTableRows}
                    pagination={false}
                    scroll={{ x: 'max-content', y: 220 }}
                    style={{ marginBottom: 8 }}
                  />
                )}
              </div>
            )}

            <div
              style={{
                minHeight: 360,
                maxHeight: 520,
                overflowY: 'auto',
                border: '1px solid var(--app-border)',
                borderRadius: 10,
                padding: 10,
                background: 'var(--app-shell-bg)',
              }}
            >
              {chatTurns.length === 0 ? (
                <Empty
                  image={Empty.PRESENTED_IMAGE_SIMPLE}
                  description="Ask your first question after connecting datasource"
                />
              ) : (
                <List
                  dataSource={chatTurns}
                  renderItem={(turn) => (
                    <List.Item style={{ border: 'none', padding: '8px 0' }}>
                      <div
                        style={{
                          marginLeft: turn.role === 'assistant' ? 0 : 'auto',
                          maxWidth: '88%',
                          background: turn.role === 'assistant' ? 'var(--app-card-bg)' : '#2563eb',
                          color: turn.role === 'assistant' ? 'var(--app-text)' : '#fff',
                          border: turn.role === 'assistant' ? '1px solid var(--app-border)' : '1px solid #1d4ed8',
                          borderRadius: 10,
                          padding: '10px 12px',
                          whiteSpace: 'pre-wrap',
                          lineHeight: 1.45,
                        }}
                      >
                        <div style={{ fontSize: 11, opacity: 0.8, marginBottom: 6, textTransform: 'uppercase', letterSpacing: 0.5 }}>
                          {turn.role}
                        </div>
                        <div>{turn.content}</div>
                      </div>
                    </List.Item>
                  )}
                />
              )}
              {asking && (
                <div style={{ marginTop: 8, textAlign: 'center' }}>
                  <Spin size="small" />
                </div>
              )}
            </div>

            <Space direction="vertical" size={10} style={{ width: '100%', marginTop: 12 }}>
              <TextArea
                rows={3}
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
                placeholder="Ask anything: trend, anomaly, segment drivers, KPI comparison, recommendation..."
                onPressEnter={(e) => {
                  if (!e.shiftKey) {
                    e.preventDefault()
                    void askQuestion()
                  }
                }}
              />

              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 10, flexWrap: 'wrap' }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                  Tip: include metric + dimension in question for best precision.
                </Text>
                <Button
                  type="primary"
                  icon={<SendOutlined />}
                  onClick={() => { void askQuestion() }}
                  loading={asking}
                  disabled={!schema}
                  style={{ background: 'linear-gradient(135deg, #16a34a, #15803d)', border: 'none' }}
                >
                  Ask Chatbot
                </Button>
              </div>
            </Space>
          </Card>
        </Col>
      </Row>
    </div>
  )
}
