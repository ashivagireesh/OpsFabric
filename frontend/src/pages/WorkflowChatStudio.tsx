import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Col,
  Empty,
  Input,
  List,
  Modal,
  Row,
  Select,
  Space,
  Table,
  Tag,
  Typography,
  message,
} from 'antd'
import {
  BranchesOutlined,
  BulbOutlined,
  DeleteOutlined,
  MessageOutlined,
  RocketOutlined,
  SendOutlined,
  UserOutlined,
} from '@ant-design/icons'
import { useNavigate } from 'react-router-dom'
import axios from 'axios'
import ReactECharts from 'echarts-for-react'

const { Title, Text } = Typography
const { TextArea } = Input

const API_BASE = import.meta.env.DEV ? 'http://localhost:8001' : ''
const http = axios.create({ baseURL: API_BASE, timeout: 60000 })

type WorkflowContextType = 'auto' | 'etl' | 'mlops' | 'business' | 'visualization' | 'application'

interface WorkflowStep {
  module: string
  title: string
  detail: string
}

interface WorkflowFlowNode {
  id: string
  label: string
  module: string
  node_type: string
}

interface WorkflowFlowEdge {
  source: string
  target: string
  label?: string
}

interface ChartSuggestion {
  type: string
  title: string
  x_field: string
  y_field: string
  reason: string
}

interface ActionSuggestion {
  label: string
  route: string
  description: string
}

interface WorkflowChatResponse {
  question: string
  context_type: WorkflowContextType
  answer: string
  one_line_recommendation?: string
  steps: WorkflowStep[]
  flow_nodes: WorkflowFlowNode[]
  flow_edges: WorkflowFlowEdge[]
  suggested_charts: ChartSuggestion[]
  suggested_actions: ActionSuggestion[]
  detected_url?: string | null
  engine?: {
    used?: boolean
    provider?: string
    model?: string
    api_kind?: string
    requested_model?: string
    fallback_reason?: string
  }
}

interface WorkflowConfiguredResource {
  module: string
  kind: string
  id: string
  name: string
  route: string
}

interface WorkflowConfigureResponse {
  question: string
  context_type: WorkflowContextType
  summary: string
  detected_url?: string | null
  created_resources: WorkflowConfiguredResource[]
  warnings?: string[]
  steps?: WorkflowStep[]
  engine?: {
    used?: boolean
    provider?: string
    model?: string
    api_kind?: string
    requested_model?: string
    fallback_reason?: string
  }
}

interface WorkflowChatTurn {
  id: string
  role: 'user' | 'assistant'
  content: string
  meta?: WorkflowChatResponse
  configuration?: WorkflowConfigureResponse
}

interface WorkflowHistoryEvent {
  id: string
  role: 'user' | 'assistant'
  contextLabel: string
  summary: string
  occurredAtLabel: string
  hasConfiguration: boolean
}

const CONTEXT_OPTIONS: Array<{ value: WorkflowContextType; label: string }> = [
  { value: 'auto', label: 'Auto Detect' },
  { value: 'etl', label: 'ETL Pipelines' },
  { value: 'mlops', label: 'MLOps' },
  { value: 'business', label: 'Business Logic' },
  { value: 'visualization', label: 'Visualization' },
  { value: 'application', label: 'Full Application' },
]

const QUICK_PROMPTS = [
  'Create complete ETL pipeline from API source and output dashboard charts',
  'Design MLOps workflow with training, evaluation and deployment plus monitoring charts',
  'Build business logic workflow for decision, WhatsApp and mail actions with tracking charts',
  'Create complete end-to-end application workflow from ETL to dashboards and operations',
]

const WORKFLOW_CHAT_STORAGE_KEY = 'framework.workflow_chat.studio.v1'
const MAX_PERSISTED_TURNS = 120
const CHAT_VIEW_OPTIONS: Array<{ label: string; value: number }> = [
  { label: 'Last 20', value: 20 },
  { label: 'Last 40', value: 40 },
  { label: 'Last 80', value: 80 },
  { label: 'All', value: -1 },
]

function isWorkflowContextType(value: unknown): value is WorkflowContextType {
  return ['auto', 'etl', 'mlops', 'business', 'visualization', 'application'].includes(String(value || ''))
}

function normalizeChatTurn(raw: unknown, idx: number): WorkflowChatTurn | null {
  if (!raw || typeof raw !== 'object') return null
  const item = raw as Record<string, unknown>
  const role = String(item.role || '').trim()
  if (role !== 'user' && role !== 'assistant') return null
  const content = String(item.content || '')
  if (!content) return null
  const id = String(item.id || `persisted-${idx}-${Date.now()}`)
  const turn: WorkflowChatTurn = { id, role, content }
  if (item.meta && typeof item.meta === 'object') turn.meta = item.meta as WorkflowChatResponse
  if (item.configuration && typeof item.configuration === 'object') {
    turn.configuration = item.configuration as WorkflowConfigureResponse
  }
  return turn
}

function loadPersistedWorkflowChatState(): {
  contextType: WorkflowContextType
  question: string
  turns: WorkflowChatTurn[]
} {
  const fallback = { contextType: 'auto' as WorkflowContextType, question: '', turns: [] as WorkflowChatTurn[] }
  if (typeof window === 'undefined') return fallback

  try {
    const raw = window.localStorage.getItem(WORKFLOW_CHAT_STORAGE_KEY)
    if (!raw) return fallback
    const parsed = JSON.parse(raw) as Record<string, unknown>
    const contextType = isWorkflowContextType(parsed.contextType) ? parsed.contextType : 'auto'
    const question = String(parsed.question || '')
    const turnsRaw = Array.isArray(parsed.turns) ? parsed.turns : []
    const turns = turnsRaw
      .map((item, idx) => normalizeChatTurn(item, idx))
      .filter((item): item is WorkflowChatTurn => Boolean(item))
      .slice(-MAX_PERSISTED_TURNS)
    return { contextType, question, turns }
  } catch {
    return fallback
  }
}

function moduleColor(moduleName: string): string {
  const key = String(moduleName || '').toLowerCase()
  if (key.includes('etl')) return '#3b82f6'
  if (key.includes('mlops')) return '#22c55e'
  if (key.includes('business')) return '#f59e0b'
  if (key.includes('visual')) return '#a855f7'
  return '#64748b'
}

function buildFlowChartOption(meta?: WorkflowChatResponse | null) {
  if (!meta || !Array.isArray(meta.flow_nodes) || meta.flow_nodes.length === 0) return null

  return {
    tooltip: { trigger: 'item' },
    animationDuration: 500,
    series: [
      {
        type: 'graph',
        layout: 'force',
        roam: true,
        draggable: true,
        symbolSize: 62,
        force: {
          repulsion: 380,
          edgeLength: [130, 210],
        },
        edgeSymbol: ['none', 'arrow'],
        edgeSymbolSize: [4, 10],
        lineStyle: {
          color: '#94a3b8',
          width: 2,
          curveness: 0.18,
        },
        label: {
          show: true,
          color: '#e2e8f0',
          fontSize: 11,
          fontWeight: 600,
          formatter: (p: any) => String(p?.data?.name || ''),
        },
        edgeLabel: {
          show: true,
          color: '#94a3b8',
          fontSize: 10,
          formatter: (p: any) => String(p?.data?.value || ''),
        },
        data: meta.flow_nodes.map((node) => ({
          id: node.id,
          name: node.label,
          value: node.module,
          itemStyle: {
            color: moduleColor(node.module),
            borderColor: '#0f172a',
            borderWidth: 1,
          },
        })),
        links: meta.flow_edges.map((edge) => ({
          source: edge.source,
          target: edge.target,
          value: edge.label || '',
        })),
      },
    ],
  }
}

function turnAccentColor(turn: WorkflowChatTurn): string {
  if (turn.role === 'user') return '#3b82f6'
  const ctx = String(turn.meta?.context_type || '').toLowerCase()
  if (ctx === 'etl') return '#3b82f6'
  if (ctx === 'mlops') return '#22c55e'
  if (ctx === 'business') return '#f59e0b'
  if (ctx === 'visualization') return '#a855f7'
  if (ctx === 'application') return '#06b6d4'
  return '#8b5cf6'
}

const BLUEPRINT_WIDGET_STYLE = {
  background: 'var(--app-input-bg)',
  border: '1px solid var(--app-border-strong)',
  borderRadius: 10,
}

function parseTurnTimestampFromId(turnId: string): number | null {
  const prefix = String(turnId || '').split('-')[0]
  const asNum = Number(prefix)
  if (!Number.isFinite(asNum) || asNum <= 0) return null
  return asNum
}

function formatTurnDateTimeLabel(turnId: string): string {
  const timestamp = parseTurnTimestampFromId(turnId)
  if (!timestamp) return 'Saved'
  try {
    return new Date(timestamp).toLocaleString()
  } catch {
    return 'Saved'
  }
}

function summarizeTurnText(turn: WorkflowChatTurn): string {
  const base = String(
    turn.role === 'assistant'
      ? (turn.meta?.question || turn.content)
      : turn.content,
  ).replace(/\s+/g, ' ').trim()
  if (!base) return 'Conversation event'
  if (base.length <= 96) return base
  return `${base.slice(0, 96).trim()}…`
}

export default function WorkflowChatStudio() {
  const [messageApi, contextHolder] = message.useMessage()
  const navigate = useNavigate()

  const [initialState] = useState(loadPersistedWorkflowChatState)
  const [contextType, setContextType] = useState<WorkflowContextType>(initialState.contextType)
  const [question, setQuestion] = useState(initialState.question)
  const [asking, setAsking] = useState(false)
  const [configuringTurnId, setConfiguringTurnId] = useState<string | null>(null)
  const [turns, setTurns] = useState<WorkflowChatTurn[]>(initialState.turns)
  const [maxVisibleTurns, setMaxVisibleTurns] = useState<number>(40)
  const [overlayTurnId, setOverlayTurnId] = useState<string | null>(null)
  const turnRefs = useRef<Record<string, HTMLDivElement | null>>({})
  const overlayTurn = useMemo(
    () => turns.find((turn) => turn.id === overlayTurnId && turn.role === 'assistant') || null,
    [overlayTurnId, turns],
  )
  const overlayMeta = overlayTurn?.meta
  const overlayFlowChartOption = useMemo(
    () => buildFlowChartOption(overlayMeta),
    [overlayMeta],
  )
  const latestBlueprintTurn = useMemo(
    () => [...turns].reverse().find((turn) => turn.role === 'assistant' && turn.meta) || null,
    [turns],
  )

  const openAssistantOverlay = useCallback((turnId: string) => {
    const turn = turns.find((item) => item.id === turnId && item.role === 'assistant')
    if (!turn?.meta) {
      messageApi.warning('No blueprint available for this response')
      return
    }
    setOverlayTurnId(turnId)
  }, [messageApi, turns])

  const visibleTurns = useMemo(() => {
    if (maxVisibleTurns <= 0) return turns
    if (turns.length <= maxVisibleTurns) return turns
    return turns.slice(-maxVisibleTurns)
  }, [maxVisibleTurns, turns])

  const historyEvents = useMemo<WorkflowHistoryEvent[]>(() => (
    [...turns]
      .reverse()
      .map((turn) => ({
        id: turn.id,
        role: turn.role,
        contextLabel: turn.role === 'assistant'
          ? String(turn.meta?.context_type || 'assistant').toUpperCase()
          : 'USER',
        summary: summarizeTurnText(turn),
        occurredAtLabel: formatTurnDateTimeLabel(turn.id),
        hasConfiguration: Boolean(turn.configuration),
      }))
  ), [turns])

  const jumpToTurn = useCallback((turnId: string, role: 'user' | 'assistant') => {
    setMaxVisibleTurns(-1)
    if (role === 'assistant') {
      openAssistantOverlay(turnId)
    }
    window.setTimeout(() => {
      const el = turnRefs.current[turnId]
      if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' })
    }, 40)
  }, [openAssistantOverlay])

  const clearHistory = useCallback(() => {
    setTurns([])
    setQuestion('')
    setConfiguringTurnId(null)
    setOverlayTurnId(null)
    messageApi.success('Workflow chat history cleared')
  }, [messageApi])

  const deleteTurn = useCallback((turnId: string) => {
    setTurns((prev) => prev.filter((turn) => turn.id !== turnId))
    if (overlayTurnId === turnId) setOverlayTurnId(null)
    if (configuringTurnId === turnId) setConfiguringTurnId(null)
    if (turnRefs.current[turnId]) {
      delete turnRefs.current[turnId]
    }
    messageApi.success('Workflow chat entry deleted')
  }, [configuringTurnId, messageApi, overlayTurnId])

  const askAssistant = useCallback(async () => {
    const userQuestion = (question || '').trim()
    if (!userQuestion) {
      messageApi.warning('Type your workflow request first')
      return
    }

    const userTurn: WorkflowChatTurn = {
      id: `${Date.now()}-user`,
      role: 'user',
      content: userQuestion,
    }

    setTurns((prev) => [...prev, userTurn])
    setQuestion('')
    setAsking(true)

    try {
      const payload = {
        question: userQuestion,
        context_type: contextType,
        chat_history: turns.map((turn) => ({ role: turn.role, content: turn.content })),
      }
      const response = await http.post('/api/workflow-chat/ask', payload)
      const data = response.data as WorkflowChatResponse
      const assistantTurnId = `${Date.now()}-assistant`

      setTurns((prev) => [
        ...prev,
        {
          id: assistantTurnId,
          role: 'assistant',
          content: String(data.answer || 'Workflow guidance generated.'),
          meta: data,
        },
      ])
      setOverlayTurnId(assistantTurnId)
      messageApi.success('Workflow blueprint generated with flow chart')
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      messageApi.error(typeof detail === 'string' ? detail : 'Unable to generate workflow blueprint')
      const assistantTurnId = `${Date.now()}-assistant-error`
      setTurns((prev) => [
        ...prev,
        {
          id: assistantTurnId,
          role: 'assistant',
          content: 'I could not generate the workflow blueprint right now. Please try again.',
        },
      ])
      setOverlayTurnId(assistantTurnId)
    } finally {
      setAsking(false)
    }
  }, [contextType, messageApi, question, turns])

  const applyConfiguration = useCallback(async (turnId: string) => {
    const turn = turns.find((item) => item.id === turnId && item.role === 'assistant')
    if (!turn?.meta) {
      messageApi.warning('Generate a workflow plan first')
      return
    }

    const derivedQuestion = String(turn.meta.question || '').trim()
      || String(
        [...turns].reverse().find((item) => item.role === 'user')?.content || '',
      ).trim()
    if (!derivedQuestion) {
      messageApi.warning('No workflow question found for configuration')
      return
    }

    setConfiguringTurnId(turnId)
    try {
      const response = await http.post('/api/workflow-chat/configure', {
        question: derivedQuestion,
        context_type: turn.meta.context_type || contextType,
        chat_history: turns.map((item) => ({ role: item.role, content: item.content })),
        include_dashboard: true,
      })
      const data = response.data as WorkflowConfigureResponse
      setTurns((prev) => prev.map((item) => (
        item.id === turnId
          ? { ...item, configuration: data }
          : item
      )))
      messageApi.success(data.summary || 'Configuration completed')
    } catch (error: any) {
      const detail = error?.response?.data?.detail
      messageApi.error(typeof detail === 'string' ? detail : 'Failed to apply configuration')
    } finally {
      setConfiguringTurnId(null)
    }
  }, [contextType, messageApi, turns])

  useEffect(() => {
    if (typeof window === 'undefined') return
    try {
      const turnsToPersist = turns.slice(-MAX_PERSISTED_TURNS)
      window.localStorage.setItem(
        WORKFLOW_CHAT_STORAGE_KEY,
        JSON.stringify({
          contextType,
          question,
          turns: turnsToPersist,
          updatedAt: new Date().toISOString(),
        }),
      )
    } catch {
      // Ignore storage failures silently (private mode/quota).
    }
  }, [contextType, question, turns])

  return (
    <div style={{ padding: 20 }}>
      {contextHolder}
      <Space direction="vertical" size={4} style={{ marginBottom: 16 }}>
        <Title level={4} style={{ color: 'var(--app-text)', margin: 0 }}>Workflow Chat Studio</Title>
        <Text style={{ color: 'var(--app-text-subtle)' }}>
          Configure and implement ETL, MLOps, Business Logic and Visualization workflows using chat + flow charts.
        </Text>
      </Space>

      <Row gutter={16}>
        <Col xs={24} lg={8}>
          <Card
            title={<Space><MessageOutlined />Assistant Input</Space>}
            styles={{ body: { display: 'flex', flexDirection: 'column', gap: 12 } }}
          >
            <div>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Planning Context</Text>
              <Select
                style={{ width: '100%', marginTop: 6 }}
                value={contextType}
                onChange={(value) => setContextType(value as WorkflowContextType)}
                options={CONTEXT_OPTIONS}
              />
            </div>

            <div>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Quick Prompts</Text>
              <Space direction="vertical" style={{ width: '100%', marginTop: 8 }}>
                {QUICK_PROMPTS.map((prompt) => (
                  <Button
                    key={prompt}
                    size="small"
                    style={{ textAlign: 'left', whiteSpace: 'normal', height: 'auto' }}
                    onClick={() => setQuestion(prompt)}
                  >
                    {prompt}
                  </Button>
                ))}
              </Space>
            </div>

            <div>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Request</Text>
              <TextArea
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
                placeholder="Example: Configure end-to-end workflow from API to dashboard with WhatsApp notifications"
                autoSize={{ minRows: 5, maxRows: 10 }}
                style={{ marginTop: 6 }}
              />
            </div>

            <Button
              type="primary"
              icon={<SendOutlined />}
              loading={asking}
              onClick={() => void askAssistant()}
            >
              Generate Workflow Plan
            </Button>
          </Card>

          <Card
            style={{ marginTop: 12 }}
            title={<Space><RocketOutlined />Module Shortcuts</Space>}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <Button onClick={() => navigate('/pipelines')} icon={<BranchesOutlined />}>Open Pipelines</Button>
              <Button onClick={() => navigate('/mlops')} icon={<BranchesOutlined />}>Open MLOps Studio</Button>
              <Button onClick={() => navigate('/business')} icon={<BranchesOutlined />}>Open Business Logic</Button>
              <Button onClick={() => navigate('/dashboards')} icon={<BranchesOutlined />}>Open Visualizations</Button>
            </Space>
          </Card>

          <Card
            style={{ marginTop: 12 }}
            title={<Space><BulbOutlined />Historical Events</Space>}
            extra={(
              <Button
                size="small"
                disabled={turns.length === 0}
                onClick={() => clearHistory()}
              >
                Delete All
              </Button>
            )}
            styles={{ body: { paddingTop: 10 } }}
          >
            {historyEvents.length === 0 ? (
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                No events yet. Start with a workflow request.
              </Text>
            ) : (
              <List
                size="small"
                style={{ maxHeight: 320, overflowY: 'auto', paddingRight: 4 }}
                dataSource={historyEvents}
                renderItem={(event) => (
                  <List.Item style={{ padding: 0, border: 'none', marginBottom: 8 }}>
                    <div
                      style={{
                        width: '100%',
                        border: '1px solid var(--app-border-strong)',
                        background: 'var(--app-input-bg)',
                        borderRadius: 8,
                        padding: '8px 10px',
                        cursor: 'pointer',
                        transition: 'border-color 0.2s ease',
                      }}
                      onClick={() => jumpToTurn(event.id, event.role)}
                      onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#6366f1' }}
                      onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'var(--app-border-strong)' }}
                    >
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 }}>
                        <Space size={6} wrap>
                          <Tag color={event.role === 'assistant' ? 'purple' : 'blue'}>{event.contextLabel}</Tag>
                          {event.hasConfiguration && <Tag color="green">Configured</Tag>}
                        </Space>
                        <Button
                          size="small"
                          type="text"
                          danger
                          icon={<DeleteOutlined />}
                          onClick={(clickEvent) => {
                            clickEvent.stopPropagation()
                            deleteTurn(event.id)
                          }}
                        />
                      </div>
                      <Text
                        style={{
                          display: 'block',
                          color: 'var(--app-text)',
                          marginTop: 4,
                          fontSize: 12,
                          lineHeight: 1.35,
                        }}
                      >
                        {event.summary}
                      </Text>
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                        {event.occurredAtLabel}
                      </Text>
                    </div>
                  </List.Item>
                )}
              />
            )}
          </Card>
        </Col>

        <Col xs={24} lg={16}>
          <Card
            title={<Space><BulbOutlined />Chat + Blueprint</Space>}
            styles={{ body: { display: 'flex', flexDirection: 'column', gap: 14 } }}
          >
            <Space
              wrap
              align="center"
              style={{
                justifyContent: 'space-between',
                width: '100%',
                padding: '8px 10px',
                border: '1px solid var(--app-border)',
                borderRadius: 8,
                background: 'var(--app-panel-2)',
              }}
            >
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                Showing {visibleTurns.length} of {turns.length} turns
              </Text>
              <Space wrap>
                <Select
                  size="small"
                  value={maxVisibleTurns}
                  options={CHAT_VIEW_OPTIONS}
                  onChange={(value) => setMaxVisibleTurns(Number(value))}
                  style={{ width: 110 }}
                />
                <Button
                  size="small"
                  disabled={!latestBlueprintTurn}
                  onClick={() => {
                    if (latestBlueprintTurn) openAssistantOverlay(latestBlueprintTurn.id)
                  }}
                >
                  Open Latest
                </Button>
              </Space>
            </Space>

            {turns.length === 0 ? (
              <Empty description="Ask for a workflow implementation plan to generate compact widgets and blueprint overlays" />
            ) : (
              <List
                style={{ maxHeight: '70vh', overflowY: 'auto', paddingRight: 4 }}
                dataSource={visibleTurns}
                renderItem={(turn) => {
                  const isUser = turn.role === 'user'
                  const meta = turn.meta
                  const hasBlueprint = Boolean(!isUser && meta)
                  const accent = turnAccentColor(turn)
                  return (
                    <List.Item style={{ display: 'block', border: 'none', padding: '0 0 14px 0' }}>
                      <div ref={(node) => { turnRefs.current[turn.id] = node }}>
                        <Card
                          hoverable
                          style={{
                            background: 'var(--app-card-bg)',
                            border: '1px solid var(--app-border-strong)',
                            borderRadius: 12,
                            cursor: hasBlueprint ? 'pointer' : 'default',
                            transition: 'all 0.2s',
                          }}
                          bodyStyle={{ padding: 14 }}
                          onClick={() => {
                            if (hasBlueprint) openAssistantOverlay(turn.id)
                          }}
                          onMouseEnter={(e) => { e.currentTarget.style.borderColor = accent }}
                          onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'var(--app-border-strong)' }}
                        >
                          <Space direction="vertical" size={8} style={{ width: '100%' }}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 10 }}>
                              <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                                <div style={{
                                  width: 36,
                                  height: 36,
                                  background: `${accent}18`,
                                  border: `1px solid ${accent}40`,
                                  borderRadius: 9,
                                  display: 'flex',
                                  alignItems: 'center',
                                  justifyContent: 'center',
                                  color: accent,
                                  fontSize: 16,
                                }}>
                                  {isUser ? <UserOutlined /> : <BulbOutlined />}
                                </div>
                                <div>
                                  <Text style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 14 }}>
                                    {isUser ? 'User Request' : 'Blueprint Widget'}
                                  </Text>
                                  <div style={{ marginTop: 2 }}>
                                    <Space size={4} wrap>
                                      <Tag color={isUser ? 'blue' : 'purple'}>{isUser ? 'User' : 'Assistant'}</Tag>
                                      {meta?.context_type && !isUser && <Tag>{meta.context_type}</Tag>}
                                      {meta?.detected_url && !isUser && <Tag color="cyan">URL detected</Tag>}
                                      {meta?.engine?.model && !isUser && (
                                        <Tag color={meta.engine.provider === 'openai' ? 'geekblue' : 'default'}>
                                          {meta.engine.provider === 'openai' ? 'Codex' : 'Fallback'}: {meta.engine.model}
                                        </Tag>
                                      )}
                                      {turn.configuration?.created_resources?.length ? <Tag color="green">Configured</Tag> : null}
                                    </Space>
                                  </div>
                                </div>
                              </div>
                              {hasBlueprint && (
                                <Button
                                  size="small"
                                  onClick={(event) => {
                                    event.stopPropagation()
                                    openAssistantOverlay(turn.id)
                                  }}
                                >
                                  Open Configuration
                                </Button>
                              )}
                            </div>

                            <div
                              style={{
                                color: 'var(--app-text)',
                                fontSize: 13,
                                lineHeight: 1.45,
                                display: '-webkit-box',
                                WebkitLineClamp: 2,
                                WebkitBoxOrient: 'vertical',
                                overflow: 'hidden',
                              }}
                            >
                              {turn.content}
                            </div>

                            {!isUser && meta?.one_line_recommendation && (
                              <Text
                                style={{
                                  color: 'var(--app-text-subtle)',
                                  fontSize: 12,
                                  display: '-webkit-box',
                                  WebkitLineClamp: 1,
                                  WebkitBoxOrient: 'vertical',
                                  overflow: 'hidden',
                                }}
                              >
                                {meta.one_line_recommendation}
                              </Text>
                            )}

                            <div style={{
                              borderTop: '1px solid var(--app-border)',
                              paddingTop: 10,
                              marginTop: 6,
                              display: 'flex',
                              justifyContent: 'space-between',
                              alignItems: 'center',
                              gap: 8,
                            }}>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                                {formatTurnDateTimeLabel(turn.id)}
                              </Text>
                              <Space size={8} wrap>
                                {meta?.steps?.length ? (
                                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                                    {meta.steps.length} steps
                                  </Text>
                                ) : null}
                                {meta?.suggested_charts?.length ? (
                                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                                    {meta.suggested_charts.length} charts
                                  </Text>
                                ) : null}
                                {turn.configuration?.created_resources?.length ? (
                                  <Text style={{ color: '#22c55e', fontSize: 11 }}>
                                    {turn.configuration.created_resources.length} configured
                                  </Text>
                                ) : null}
                                <Text style={{ color: 'var(--app-text-dim)', fontSize: 11 }}>
                                  {isUser ? 'Request' : (hasBlueprint ? 'Click to open' : 'Response')}
                                </Text>
                                <Button
                                  size="small"
                                  type="text"
                                  danger
                                  icon={<DeleteOutlined />}
                                  onClick={(event) => {
                                    event.stopPropagation()
                                    deleteTurn(turn.id)
                                  }}
                                >
                                  Delete
                                </Button>
                              </Space>
                            </div>
                          </Space>
                        </Card>
                      </div>
                    </List.Item>
                  )
                }}
              />
            )}
            {turns.length > visibleTurns.length && (
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                Older turns are hidden. Change view to “All” to display full history.
              </Text>
            )}
          </Card>
        </Col>
      </Row>
      <Modal
        title={(
          <Space wrap>
            <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Blueprint Configuration</Text>
            {overlayMeta?.context_type ? <Tag color="purple">{overlayMeta.context_type}</Tag> : null}
            {overlayMeta?.engine?.model ? (
              <Tag color={overlayMeta.engine.provider === 'openai' ? 'geekblue' : 'default'}>
                {overlayMeta.engine.provider === 'openai' ? 'Codex' : 'Fallback'}: {overlayMeta.engine.model}
              </Tag>
            ) : null}
            {overlayTurn?.configuration?.created_resources?.length ? <Tag color="green">Configured</Tag> : null}
          </Space>
        )}
        open={Boolean(overlayTurn && overlayMeta)}
        onCancel={() => setOverlayTurnId(null)}
        footer={null}
        width="96vw"
        style={{ top: 12 }}
        destroyOnClose
        styles={{
          content: {
            background: 'var(--app-card-bg)',
            border: '1px solid var(--app-border-strong)',
            borderRadius: 12,
          },
          header: {
            background: 'var(--app-card-bg)',
            borderBottom: '1px solid var(--app-border)',
            borderRadius: '12px 12px 0 0',
          },
          body: {
            height: 'calc(100vh - 120px)',
            overflowY: 'auto',
            padding: 14,
            background: 'var(--app-card-bg)',
          },
          mask: { backdropFilter: 'blur(3px)' },
        }}
      >
        {overlayTurn && overlayMeta && (
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            <Card style={BLUEPRINT_WIDGET_STYLE}>
              <Space direction="vertical" size={10} style={{ width: '100%' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
                  <Space size={8} wrap>
                    <Tag color="blue">Assistant Response</Tag>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                      {formatTurnDateTimeLabel(overlayTurn.id)}
                    </Text>
                  </Space>
                  <Space wrap>
                    <Button
                      danger
                      icon={<DeleteOutlined />}
                      onClick={() => deleteTurn(overlayTurn.id)}
                    >
                      Delete Entry
                    </Button>
                    <Button
                      type="primary"
                      icon={<RocketOutlined />}
                      loading={configuringTurnId === overlayTurn.id}
                      onClick={() => void applyConfiguration(overlayTurn.id)}
                    >
                      Apply Configuration
                    </Button>
                  </Space>
                </div>
                <Text style={{ color: 'var(--app-text)' }}>
                  {overlayTurn.content}
                </Text>
                {overlayMeta.engine?.provider !== 'openai' && overlayMeta.engine?.fallback_reason && (
                  <Alert
                    type="warning"
                    showIcon
                    message={`Codex fallback: ${overlayMeta.engine.fallback_reason}`}
                  />
                )}
                {overlayMeta.one_line_recommendation && (
                  <Alert
                    type="info"
                    showIcon
                    message={overlayMeta.one_line_recommendation}
                  />
                )}
              </Space>
            </Card>

            <Row gutter={[10, 10]}>
              {Array.isArray(overlayMeta.steps) && overlayMeta.steps.length > 0 && (
                <Col xs={24} md={12}>
                  <Card size="small" title={`Implementation Steps (${overlayMeta.steps.length})`} style={BLUEPRINT_WIDGET_STYLE}>
                    <List
                      size="small"
                      style={{ maxHeight: 260, overflowY: 'auto' }}
                      dataSource={overlayMeta.steps}
                      renderItem={(step, idx) => (
                        <List.Item>
                          <Space direction="vertical" size={0} style={{ width: '100%' }}>
                            <Text style={{ color: 'var(--app-text)' }}>
                              {idx + 1}. <strong>{step.title}</strong> <Tag>{step.module}</Tag>
                            </Text>
                            <Text style={{ color: 'var(--app-text-subtle)' }}>{step.detail}</Text>
                          </Space>
                        </List.Item>
                      )}
                    />
                  </Card>
                </Col>
              )}

              {overlayFlowChartOption && (
                <Col xs={24} md={12}>
                  <Card size="small" title="Workflow Flow Widget" style={BLUEPRINT_WIDGET_STYLE}>
                    <ReactECharts option={overlayFlowChartOption} style={{ height: 320, width: '100%' }} notMerge lazyUpdate />
                  </Card>
                </Col>
              )}

              {Array.isArray(overlayMeta.suggested_charts) && overlayMeta.suggested_charts.length > 0 && (
                <Col xs={24} lg={15}>
                  <Card size="small" title="Suggested Chart Widgets" style={BLUEPRINT_WIDGET_STYLE}>
                    <Table
                      size="small"
                      pagination={false}
                      rowKey={(row) => `${row.title}-${row.type}`}
                      dataSource={overlayMeta.suggested_charts}
                      columns={[
                        { title: 'Type', dataIndex: 'type', key: 'type', width: 100 },
                        { title: 'Title', dataIndex: 'title', key: 'title', width: 220 },
                        { title: 'X', dataIndex: 'x_field', key: 'x_field', width: 140 },
                        { title: 'Y', dataIndex: 'y_field', key: 'y_field', width: 140 },
                        { title: 'Reason', dataIndex: 'reason', key: 'reason' },
                      ]}
                      scroll={{ x: true, y: 220 }}
                    />
                  </Card>
                </Col>
              )}

              {Array.isArray(overlayMeta.suggested_actions) && overlayMeta.suggested_actions.length > 0 && (
                <Col xs={24} lg={9}>
                  <Card size="small" title="Module Actions" style={BLUEPRINT_WIDGET_STYLE}>
                    <Space direction="vertical" size={8} style={{ width: '100%' }}>
                      {overlayMeta.suggested_actions.map((action) => (
                        <Button
                          key={`${action.label}-${action.route}`}
                          size="small"
                          style={{ textAlign: 'left', width: '100%' }}
                          onClick={() => navigate(action.route)}
                        >
                          {action.label}
                        </Button>
                      ))}
                    </Space>
                  </Card>
                </Col>
              )}

              {overlayTurn.configuration && (
                <Col xs={24}>
                  <Card size="small" title="Applied Configuration Widget" style={BLUEPRINT_WIDGET_STYLE}>
                    <Space direction="vertical" size={10} style={{ width: '100%' }}>
                      <Alert type="success" showIcon message={overlayTurn.configuration.summary} />
                      {Array.isArray(overlayTurn.configuration.warnings) && overlayTurn.configuration.warnings.length > 0 && (
                        <Alert
                          type="warning"
                          showIcon
                          message={overlayTurn.configuration.warnings.join(' ')}
                        />
                      )}
                      <List
                        size="small"
                        dataSource={overlayTurn.configuration.created_resources || []}
                        renderItem={(resource) => (
                          <List.Item
                            actions={[
                              <Button
                                key={`${resource.id}-open`}
                                size="small"
                                onClick={() => navigate(resource.route)}
                              >
                                Open
                              </Button>,
                            ]}
                          >
                            <Space>
                              <Tag>{resource.module}</Tag>
                              <Text style={{ color: 'var(--app-text)' }}>{resource.name}</Text>
                              <Text style={{ color: 'var(--app-text-subtle)' }}>({resource.kind})</Text>
                            </Space>
                          </List.Item>
                        )}
                      />
                    </Space>
                  </Card>
                </Col>
              )}
            </Row>
          </Space>
        )}
      </Modal>
    </div>
  )
}
