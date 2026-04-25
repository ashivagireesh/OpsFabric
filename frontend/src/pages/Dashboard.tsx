import { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Row, Col, Card, Statistic, Table, Tag, Button, Typography, Space,
  Progress, Badge, Avatar, Tooltip
} from 'antd'
import {
  ApiOutlined, ThunderboltOutlined, CheckCircleFilled, CloseCircleFilled,
  ClockCircleFilled, RocketOutlined, PlusOutlined, ArrowUpOutlined,
  LoadingOutlined, PlayCircleOutlined, DatabaseOutlined,
  ExperimentOutlined, BranchesOutlined, BarChartOutlined, CommentOutlined
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import { useStatsStore, usePipelineStore, useExecutionStore } from '../store'
import { parseTimestampMsOrNaN } from '../utils/time'

dayjs.extend(relativeTime)

const { Title, Text } = Typography

const statusConfig = {
  success: { color: '#22c55e', icon: <CheckCircleFilled />, text: 'Success' },
  failed: { color: '#ef4444', icon: <CloseCircleFilled />, text: 'Failed' },
  running: { color: '#6366f1', icon: <LoadingOutlined spin />, text: 'Running' },
  cancelling: { color: '#f59e0b', icon: <LoadingOutlined spin />, text: 'Cancelling' },
  pending: { color: '#f59e0b', icon: <ClockCircleFilled />, text: 'Pending' },
  cancelled: { color: 'var(--app-text-muted)', icon: <CloseCircleFilled />, text: 'Cancelled' },
}

const WIDGET_POLL_INTERVAL_MS = 1500
const LIVE_EXECUTION_STATUSES = new Set(['running', 'cancelling'])
const TERMINAL_EXECUTION_STATUSES = new Set(['success', 'failed', 'error', 'cancelled'])

function isExecutionActivelyRunning(execution: any): boolean {
  const status = String(execution?.status || '').trim().toLowerCase()
  if (!LIVE_EXECUTION_STATUSES.has(status)) return false
  const finishedAt = String(execution?.finished_at || '').trim()
  return finishedAt.length === 0
}

function formatElapsedTimerWithBaseline(startedAt: string | undefined, nowMs: number, baselineMs?: number): string {
  const startedMs = parseTimestampMsOrNaN(String(startedAt || '').trim())
  const hasStarted = Number.isFinite(startedMs)
  const hasBaseline = Number.isFinite(Number(baselineMs))
  if (!hasStarted && !hasBaseline) return '00:00'
  const effectiveStartMs = hasStarted
    ? Number(startedMs)
    : Number(baselineMs)
  const elapsedSec = Math.max(0, Math.floor((nowMs - effectiveStartMs) / 1000))
  const minutes = Math.floor(elapsedSec / 60)
  const seconds = elapsedSec % 60
  return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`
}

function formatDurationTimer(durationSeconds: number | undefined): string {
  if (!Number.isFinite(Number(durationSeconds)) || Number(durationSeconds) <= 0) return '00:00'
  const total = Math.max(0, Math.floor(Number(durationSeconds)))
  const minutes = Math.floor(total / 60)
  const seconds = total % 60
  return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`
}

function deriveDurationSeconds(execution: any): number | undefined {
  const direct = Number(execution?.duration)
  if (Number.isFinite(direct) && direct >= 0) return Math.floor(direct)
  const startedMs = parseTimestampMsOrNaN(String(execution?.started_at || '').trim())
  const finishedMs = parseTimestampMsOrNaN(String(execution?.finished_at || '').trim())
  if (Number.isFinite(startedMs) && Number.isFinite(finishedMs) && finishedMs >= startedMs) {
    return Math.floor((finishedMs - startedMs) / 1000)
  }
  return undefined
}

function getExecutionSortTimeMs(execution: any): number {
  const startedMs = parseTimestampMsOrNaN(String(execution?.started_at || '').trim())
  if (Number.isFinite(startedMs)) return Number(startedMs)
  const finishedMs = parseTimestampMsOrNaN(String(execution?.finished_at || '').trim())
  if (Number.isFinite(finishedMs)) return Number(finishedMs)
  return 0
}

export default function Dashboard() {
  const navigate = useNavigate()
  const { stats, fetchStats } = useStatsStore()
  const { pipelines, fetchPipelines } = usePipelineStore()
  const { executions, fetchExecutions } = useExecutionStore()
  const [nowMs, setNowMs] = useState<number>(() => Date.now())
  const runningBaselineByExecutionIdRef = useRef<Map<string, number>>(new Map())

  useEffect(() => {
    fetchStats()
    fetchPipelines()
    fetchExecutions()
  }, [fetchExecutions, fetchPipelines, fetchStats])

  const latestExecutionByPipeline = useMemo(() => {
    const map = new Map<string, (typeof executions)[number]>()
    executions.forEach((execution) => {
      const pipelineId = String(execution?.pipeline_id || '').trim()
      if (!pipelineId) return
      const existing = map.get(pipelineId)
      if (!existing || getExecutionSortTimeMs(execution) >= getExecutionSortTimeMs(existing)) {
        map.set(pipelineId, execution)
      }
    })
    return map
  }, [executions])

  const hasLiveExecution = useMemo(() => (
    executions.some((execution) => {
      const status = String(execution?.status || '').trim().toLowerCase()
      return LIVE_EXECUTION_STATUSES.has(status)
    })
  ), [executions])

  useEffect(() => {
    const activeIds = new Set<string>()
    const now = Date.now()
    executions.forEach((execution) => {
      if (!isExecutionActivelyRunning(execution)) return
      const executionId = String(execution?.id || '').trim()
      if (!executionId) return
      activeIds.add(executionId)
      if (!runningBaselineByExecutionIdRef.current.has(executionId)) {
        runningBaselineByExecutionIdRef.current.set(executionId, now)
      }
    })
    Array.from(runningBaselineByExecutionIdRef.current.keys()).forEach((executionId) => {
      if (!activeIds.has(executionId)) {
        runningBaselineByExecutionIdRef.current.delete(executionId)
      }
    })
  }, [executions])

  const latestCompletedDurationByPipeline = useMemo(() => {
    const map = new Map<string, number>()
    executions.forEach((execution) => {
      const pipelineId = String(execution?.pipeline_id || '').trim()
      if (!pipelineId || map.has(pipelineId)) return
      const status = String(execution?.status || '').trim().toLowerCase()
      if (!TERMINAL_EXECUTION_STATUSES.has(status)) return
      map.set(pipelineId, deriveDurationSeconds(execution) ?? 0)
    })
    return map
  }, [executions])

  const latestCompletedTimeByPipeline = useMemo(() => {
    const map = new Map<string, number>()
    executions.forEach((execution) => {
      const pipelineId = String(execution?.pipeline_id || '').trim()
      if (!pipelineId) return
      const status = String(execution?.status || '').trim().toLowerCase()
      if (!TERMINAL_EXECUTION_STATUSES.has(status)) return
      const t = getExecutionSortTimeMs(execution)
      const existing = Number(map.get(pipelineId) ?? 0)
      if (t >= existing) map.set(pipelineId, t)
    })
    return map
  }, [executions])

  useEffect(() => {
    if (!hasLiveExecution) return
    const timer = window.setInterval(() => {
      void fetchExecutions()
      void fetchStats()
    }, WIDGET_POLL_INTERVAL_MS)
    return () => window.clearInterval(timer)
  }, [fetchExecutions, fetchStats, hasLiveExecution])

  useEffect(() => {
    if (!hasLiveExecution) return
    const timer = window.setInterval(() => {
      setNowMs(Date.now())
    }, 1000)
    return () => window.clearInterval(timer)
  }, [hasLiveExecution])

  useEffect(() => {
    if (hasLiveExecution) return
    setNowMs(Date.now())
  }, [hasLiveExecution])

  const recentExecutions = executions.slice(0, 8)

  const statCards = [
    {
      title: 'Total Pipelines',
      value: stats?.total_pipelines ?? 0,
      icon: <ApiOutlined />,
      color: '#6366f1',
      suffix: `${stats?.active_pipelines ?? 0} active`,
    },
    {
      title: 'Total Executions',
      value: stats?.total_executions ?? 0,
      icon: <ThunderboltOutlined />,
      color: '#3b82f6',
      suffix: `${stats?.running_executions ?? 0} running`,
    },
    {
      title: 'Success Rate',
      value: stats?.success_rate ?? 0,
      icon: <CheckCircleFilled />,
      color: '#22c55e',
      suffix: '%',
      precision: 1,
    },
    {
      title: 'Rows Processed',
      value: stats?.total_rows_processed ?? 0,
      icon: <DatabaseOutlined />,
      color: '#f59e0b',
      formatter: (v: number) => v >= 1000000 ? `${(v/1000000).toFixed(1)}M` : v >= 1000 ? `${(v/1000).toFixed(0)}K` : String(v),
    },
  ]

  const moduleCards = [
    {
      key: 'pipelines',
      title: 'ETL Pipelines',
      description: 'Build and run extraction + transformation workflows.',
      icon: <ApiOutlined />,
      color: '#3b82f6',
      route: '/pipelines',
      cta: 'Open ETL',
    },
    {
      key: 'mlops',
      title: 'MLOps Studio',
      description: 'Create model training, evaluation and deployment workflows.',
      icon: <ExperimentOutlined />,
      color: '#22c55e',
      route: '/mlops',
      cta: 'Open MLOps',
    },
    {
      key: 'business',
      title: 'Business Logic',
      description: 'Automate decisions and actions with business workflows.',
      icon: <BranchesOutlined />,
      color: '#f59e0b',
      route: '/business',
      cta: 'Open Business',
    },
    {
      key: 'visualizations',
      title: 'Visualizations',
      description: 'Create, edit and share analytics dashboards.',
      icon: <BarChartOutlined />,
      color: '#a855f7',
      route: '/dashboards',
      cta: 'Open Dashboards',
    },
    {
      key: 'workflow-chat',
      title: 'Workflow Chat',
      description: 'Chat-driven workflow planning and auto-configuration.',
      icon: <CommentOutlined />,
      color: '#06b6d4',
      route: '/workflow-chat',
      cta: 'Open Chat',
    },
  ]

  return (
    <div style={{ padding: '24px', maxWidth: 1400 }}>
      {/* Header */}
      <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
        <div>
          <Title level={4} style={{ color: 'var(--app-text)', margin: 0 }}>
            Welcome back 👋
          </Title>
          <Text style={{ color: 'var(--app-text-subtle)' }}>
            Here's what's happening with your data pipelines
          </Text>
        </div>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => navigate('/pipelines')}
          style={{ background: 'linear-gradient(135deg, #6366f1, #a855f7)', border: 'none' }}
        >
          New Pipeline
        </Button>
      </div>

      {/* Stat Cards */}
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        {statCards.map((card, idx) => (
          <Col xs={24} sm={12} lg={6} key={idx}>
            <Card
              style={{
                background: 'var(--app-card-bg)',
                border: '1px solid var(--app-border-strong)',
                borderRadius: 12,
                overflow: 'hidden',
                position: 'relative',
              }}
              bodyStyle={{ padding: '20px 24px' }}
            >
              {/* Color bar */}
              <div style={{
                position: 'absolute', top: 0, left: 0, right: 0, height: 3,
                background: `linear-gradient(90deg, ${card.color}80, ${card.color}20)`,
              }} />
              <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12, letterSpacing: '0.05em', textTransform: 'uppercase' }}>
                    {card.title}
                  </Text>
                  <div style={{ marginTop: 4 }}>
                    <span style={{ fontSize: 28, fontWeight: 700, color: 'var(--app-text)' }}>
                      {card.formatter ? card.formatter(card.value) : card.precision
                        ? card.value.toFixed(card.precision)
                        : card.value.toLocaleString()}
                    </span>
                    {card.suffix && (
                      <span style={{ color: 'var(--app-text-subtle)', fontSize: 12, marginLeft: 4 }}>{card.suffix}</span>
                    )}
                  </div>
                </div>
                <div style={{
                  width: 40, height: 40,
                  background: `${card.color}18`,
                  borderRadius: 10,
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  fontSize: 18, color: card.color,
                }}>
                  {card.icon}
                </div>
              </div>
            </Card>
          </Col>
        ))}
      </Row>

      {/* Module Widgets */}
      <Card
        title={
          <Space>
            <RocketOutlined style={{ color: '#6366f1' }} />
            <span style={{ color: 'var(--app-text)' }}>Platform Modules</span>
          </Space>
        }
        style={{
          background: 'var(--app-card-bg)',
          border: '1px solid var(--app-border-strong)',
          borderRadius: 12,
          marginBottom: 24,
        }}
        headStyle={{ borderBottom: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }}
      >
        <Row gutter={[12, 12]}>
          {moduleCards.map((module) => (
            <Col xs={24} sm={12} lg={8} xl={6} key={module.key}>
              <div
                style={{
                  background: 'var(--app-input-bg)',
                  border: '1px solid var(--app-border-strong)',
                  borderRadius: 10,
                  padding: '12px 14px',
                  minHeight: 132,
                  display: 'flex',
                  flexDirection: 'column',
                  justifyContent: 'space-between',
                  transition: 'border-color 0.2s ease, transform 0.2s ease',
                  cursor: 'pointer',
                }}
                onClick={() => navigate(module.route)}
                onMouseEnter={(e) => {
                  e.currentTarget.style.borderColor = module.color
                  e.currentTarget.style.transform = 'translateY(-1px)'
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.borderColor = 'var(--app-border-strong)'
                  e.currentTarget.style.transform = 'translateY(0px)'
                }}
              >
                <div>
                  <Space size={8} align="center">
                    <Avatar
                      size={30}
                      style={{
                        background: `${module.color}20`,
                        color: module.color,
                        border: `1px solid ${module.color}55`,
                      }}
                      icon={module.icon}
                    />
                    <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>
                      {module.title}
                    </Text>
                  </Space>
                  <Text
                    style={{
                      marginTop: 8,
                      color: 'var(--app-text-subtle)',
                      fontSize: 12,
                      lineHeight: 1.45,
                      display: 'block',
                    }}
                  >
                    {module.description}
                  </Text>
                </div>
                <Button
                  size="small"
                  type="text"
                  onClick={(e) => { e.stopPropagation(); navigate(module.route) }}
                  style={{
                    marginTop: 8,
                    padding: 0,
                    alignSelf: 'flex-start',
                    color: module.color,
                    fontWeight: 600,
                  }}
                >
                  {module.cta} →
                </Button>
              </div>
            </Col>
          ))}
        </Row>
      </Card>

      <Row gutter={[16, 16]}>
        {/* Recent Executions */}
        <Col xs={24} lg={14}>
          <Card
            title={
              <Space>
                <ThunderboltOutlined style={{ color: '#6366f1' }} />
                <span style={{ color: 'var(--app-text)' }}>Recent Executions</span>
              </Space>
            }
            extra={
              <Button type="link" onClick={() => navigate('/executions')} style={{ color: '#6366f1', padding: 0 }}>
                View all →
              </Button>
            }
            style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 12 }}
            headStyle={{ borderBottom: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }}
            bodyStyle={{ padding: 0 }}
          >
            <Table
              dataSource={recentExecutions}
              rowKey="id"
              pagination={false}
              size="small"
              style={{ background: 'transparent' }}
              rowClassName={() => 'exec-row'}
              columns={[
                {
                  title: 'Pipeline',
                  dataIndex: 'pipeline_name',
                  render: (name: string) => (
                    <Text style={{ color: 'var(--app-text)', fontSize: 13 }}>{name}</Text>
                  ),
                },
                {
                  title: 'Status',
                  dataIndex: 'status',
                  width: 100,
                  render: (status: string) => {
                    const cfg = statusConfig[status as keyof typeof statusConfig] || statusConfig.pending
                    return (
                      <Tag
                        icon={cfg.icon}
                        style={{
                          background: `${cfg.color}18`,
                          border: `1px solid ${cfg.color}40`,
                          color: cfg.color,
                          borderRadius: 6,
                          fontSize: 11,
                        }}
                      >
                        {cfg.text}
                      </Tag>
                    )
                  },
                },
                {
                  title: 'Rows',
                  dataIndex: 'rows_processed',
                  width: 90,
                  render: (n: number) => (
                    <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>
                      {n?.toLocaleString() ?? '-'}
                    </Text>
                  ),
                },
                {
                  title: 'Duration',
                  dataIndex: 'duration',
                  width: 80,
                  render: (d: number, row: any) => (
                    <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>
                      {LIVE_EXECUTION_STATUSES.has(String(row?.status || '').trim().toLowerCase())
                        ? formatElapsedTimerWithBaseline(String(row?.started_at || ''), nowMs)
                        : (d ? `${d}s` : '-')}
                    </Text>
                  ),
                },
                {
                  title: 'Time',
                  dataIndex: 'started_at',
                  width: 100,
                  render: (t: string) => (
                    <Tooltip title={dayjs(t).format('YYYY-MM-DD HH:mm:ss')}>
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>{dayjs(t).fromNow()}</Text>
                    </Tooltip>
                  ),
                },
              ]}
            />
          </Card>
        </Col>

        {/* Active Pipelines */}
        <Col xs={24} lg={10}>
          <Card
            title={
              <Space>
                <ApiOutlined style={{ color: '#6366f1' }} />
                <span style={{ color: 'var(--app-text)' }}>Active Pipelines</span>
              </Space>
            }
            extra={
              <Button type="link" onClick={() => navigate('/pipelines')} style={{ color: '#6366f1', padding: 0 }}>
                View all →
              </Button>
            }
            style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 12 }}
            headStyle={{ borderBottom: '1px solid var(--app-border-strong)' }}
          >
            <Space direction="vertical" style={{ width: '100%' }} size={12}>
              {pipelines.filter(p => p.status === 'active').slice(0, 5).map((pipeline) => {
                const lastExec = latestExecutionByPipeline.get(String(pipeline.id)) || pipeline.last_execution
                const lastExecStatus = String(lastExec?.status || '').trim().toLowerCase()
                const runningCandidate = isExecutionActivelyRunning(lastExec)
                const runStartedMs = parseTimestampMsOrNaN(String((lastExec as any)?.started_at || '').trim())
                const completedTimeMs = Number(latestCompletedTimeByPipeline.get(String(pipeline.id)) ?? 0)
                const runningIsStale = Number.isFinite(runStartedMs) && completedTimeMs > 0 && completedTimeMs >= Number(runStartedMs)
                const isLastExecLive = runningCandidate && !runningIsStale
                const runningBaselineMs = runningBaselineByExecutionIdRef.current.get(String((lastExec as any)?.id || ''))
                const runDurationLabel = isLastExecLive
                  ? formatElapsedTimerWithBaseline(String((lastExec as any)?.started_at || ''), nowMs, runningBaselineMs)
                  : '00:00'
                const lastDurationLabel = formatDurationTimer(
                  isLastExecLive
                    ? Number(latestCompletedDurationByPipeline.get(String(pipeline.id)) ?? 0)
                    : Number(deriveDurationSeconds(lastExec) ?? latestCompletedDurationByPipeline.get(String(pipeline.id)) ?? 0),
                )
                const statusCfg = lastExec
                  ? statusConfig[lastExecStatus as keyof typeof statusConfig]
                  : null
                return (
                  <div
                    key={pipeline.id}
                    style={{
                      padding: '12px 16px',
                      background: 'var(--app-input-bg)',
                      borderRadius: 10,
                      border: '1px solid var(--app-border-strong)',
                      cursor: 'pointer',
                      transition: 'border-color 0.2s',
                    }}
                    onClick={() => navigate(`/pipelines/${pipeline.id}/edit`)}
                    onMouseEnter={e => (e.currentTarget.style.borderColor = '#6366f1')}
                    onMouseLeave={e => (e.currentTarget.style.borderColor = 'var(--app-border-strong)')}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <div>
                        <Text style={{ color: 'var(--app-text)', fontWeight: 500, fontSize: 13 }}>
                          {pipeline.name}
                        </Text>
                        <div style={{ marginTop: 2 }}>
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                            {pipeline.nodeCount} nodes · {lastExec
                              ? `${lastExec.rows_processed?.toLocaleString()} rows · run ${runDurationLabel} · last ${lastDurationLabel}`
                              : 'Never run'}
                          </Text>
                        </div>
                      </div>
                      <Space size={8}>
                        {statusCfg && (
                          <div style={{ color: statusCfg.color, fontSize: 14 }}>{statusCfg.icon}</div>
                        )}
                        <Button
                          type="text"
                          icon={<PlayCircleOutlined />}
                          size="small"
                          style={{ color: '#6366f1' }}
                          onClick={(e) => { e.stopPropagation(); navigate(`/pipelines/${pipeline.id}/edit`) }}
                        />
                      </Space>
                    </div>
                  </div>
                )
              })}
              {pipelines.filter(p => p.status === 'active').length === 0 && (
                <div style={{ textAlign: 'center', padding: '20px 0', color: 'var(--app-text-subtle)' }}>
                  No active pipelines yet. <br />
                  <Button type="link" onClick={() => navigate('/pipelines')} style={{ color: '#6366f1' }}>
                    Create one →
                  </Button>
                </div>
              )}
            </Space>
          </Card>

          {/* Success rate card */}
          <Card
            style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 12, marginTop: 16 }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 12 }}>
              <Text style={{ color: 'var(--app-text)', fontWeight: 500 }}>Execution Health</Text>
              <Text style={{ color: '#22c55e', fontWeight: 600 }}>
                {stats?.success_rate ?? 0}%
              </Text>
            </div>
            <Progress
              percent={stats?.success_rate ?? 0}
              strokeColor={{ '0%': '#6366f1', '100%': '#22c55e' }}
              trailColor='var(--app-border)'
              showInfo={false}
              strokeWidth={6}
            />
            <Row gutter={8} style={{ marginTop: 12 }}>
              <Col span={8}>
                <div style={{ textAlign: 'center', padding: '8px', background: '#22c55e18', borderRadius: 8 }}>
                  <div style={{ color: '#22c55e', fontWeight: 600 }}>{stats?.successful_executions ?? 0}</div>
                  <div style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Success</div>
                </div>
              </Col>
              <Col span={8}>
                <div style={{ textAlign: 'center', padding: '8px', background: '#ef444418', borderRadius: 8 }}>
                  <div style={{ color: '#ef4444', fontWeight: 600 }}>{stats?.failed_executions ?? 0}</div>
                  <div style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Failed</div>
                </div>
              </Col>
              <Col span={8}>
                <div style={{ textAlign: 'center', padding: '8px', background: '#6366f118', borderRadius: 8 }}>
                  <div style={{ color: '#6366f1', fontWeight: 600 }}>{stats?.running_executions ?? 0}</div>
                  <div style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Running</div>
                </div>
              </Col>
            </Row>
          </Card>
        </Col>
      </Row>
    </div>
  )
}
