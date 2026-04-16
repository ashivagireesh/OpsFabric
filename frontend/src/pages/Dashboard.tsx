import { useEffect } from 'react'
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

dayjs.extend(relativeTime)

const { Title, Text } = Typography

const statusConfig = {
  success: { color: '#22c55e', icon: <CheckCircleFilled />, text: 'Success' },
  failed: { color: '#ef4444', icon: <CloseCircleFilled />, text: 'Failed' },
  running: { color: '#6366f1', icon: <LoadingOutlined spin />, text: 'Running' },
  pending: { color: '#f59e0b', icon: <ClockCircleFilled />, text: 'Pending' },
  cancelled: { color: 'var(--app-text-muted)', icon: <CloseCircleFilled />, text: 'Cancelled' },
}

export default function Dashboard() {
  const navigate = useNavigate()
  const { stats, fetchStats } = useStatsStore()
  const { pipelines, fetchPipelines } = usePipelineStore()
  const { executions, fetchExecutions } = useExecutionStore()

  useEffect(() => {
    fetchStats()
    fetchPipelines()
    fetchExecutions()
  }, [])

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
                  render: (d: number) => (
                    <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>
                      {d ? `${d}s` : '-'}
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
                const lastExec = pipeline.last_execution
                const statusCfg = lastExec
                  ? statusConfig[lastExec.status as keyof typeof statusConfig]
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
                              ? `${lastExec.rows_processed?.toLocaleString()} rows`
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
