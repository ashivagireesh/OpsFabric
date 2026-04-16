import { useEffect, useState } from 'react'
import {
  Table, Tag, Typography, Space, Button, Input, Select, Modal,
  Drawer, Badge, Tooltip, Card, Row, Col
} from 'antd'
import {
  SearchOutlined, DeleteOutlined, CheckCircleFilled, CloseCircleFilled,
  ClockCircleFilled, LoadingOutlined, EyeOutlined, ReloadOutlined
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../api/client'
import { useExecutionStore } from '../store'
import type { Execution, LogEntry } from '../types'

dayjs.extend(relativeTime)
const { Text, Title } = Typography

const statusConfig: Record<string, { color: string; icon: JSX.Element; text: string }> = {
  success: { color: '#22c55e', icon: <CheckCircleFilled />, text: 'Success' },
  failed: { color: '#ef4444', icon: <CloseCircleFilled />, text: 'Failed' },
  running: { color: '#6366f1', icon: <LoadingOutlined spin />, text: 'Running' },
  pending: { color: '#f59e0b', icon: <ClockCircleFilled />, text: 'Pending' },
  cancelled: { color: 'var(--app-text-muted)', icon: <CloseCircleFilled />, text: 'Cancelled' },
}

export default function ExecutionList() {
  const { executions, loading, fetchExecutions, deleteExecution } = useExecutionStore()
  const [search, setSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState<string>('all')
  const [logsDrawer, setLogsDrawer] = useState<Execution | null>(null)
  const [logsLoading, setLogsLoading] = useState(false)

  useEffect(() => { fetchExecutions() }, [])

  const filtered = executions.filter(e => {
    const matchSearch = e.pipeline_name?.toLowerCase().includes(search.toLowerCase()) ||
      e.id.toLowerCase().includes(search.toLowerCase())
    const matchStatus = statusFilter === 'all' || e.status === statusFilter
    return matchSearch && matchStatus
  })

  const columns = [
    {
      title: 'Pipeline',
      dataIndex: 'pipeline_name',
      render: (name: string, row: Execution) => (
        <div>
          <Text style={{ color: 'var(--app-text)', fontWeight: 500 }}>{name}</Text>
          <br />
          <Text style={{ color: 'var(--app-text-dim)', fontSize: 11, fontFamily: 'monospace' }}>
            {row.id.slice(0, 8)}…
          </Text>
        </div>
      ),
    },
    {
      title: 'Status',
      dataIndex: 'status',
      width: 110,
      render: (status: string) => {
        const cfg = statusConfig[status] || statusConfig.pending
        return (
          <Tag
            icon={cfg.icon}
            style={{
              background: `${cfg.color}18`, border: `1px solid ${cfg.color}40`,
              color: cfg.color, borderRadius: 6, fontSize: 11,
            }}
          >
            {cfg.text}
          </Tag>
        )
      },
    },
    {
      title: 'Rows Processed',
      dataIndex: 'rows_processed',
      width: 130,
      render: (n: number) => (
        <Text style={{ color: 'var(--app-text-muted)', fontVariantNumeric: 'tabular-nums' }}>
          {n?.toLocaleString() ?? '—'}
        </Text>
      ),
    },
    {
      title: 'Duration',
      dataIndex: 'duration',
      width: 90,
      render: (d: number) => {
        if (!d) return <Text style={{ color: 'var(--app-text-dim)' }}>—</Text>
        const s = Math.round(d)
        const mins = Math.floor(s / 60)
        const secs = s % 60
        return (
          <Text style={{ color: 'var(--app-text-muted)' }}>
            {mins > 0 ? `${mins}m ${secs}s` : `${secs}s`}
          </Text>
        )
      },
    },
    {
      title: 'Triggered by',
      dataIndex: 'triggered_by',
      width: 110,
      render: (t: string) => (
        <Tag style={{ background: 'var(--app-border)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)', borderRadius: 4, fontSize: 11 }}>
          {t}
        </Tag>
      ),
    },
    {
      title: 'Started',
      dataIndex: 'started_at',
      width: 130,
      render: (t: string) => (
        <Tooltip title={dayjs(t).format('YYYY-MM-DD HH:mm:ss')}>
          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>{dayjs(t).fromNow()}</Text>
        </Tooltip>
      ),
    },
    {
      title: '',
      width: 80,
      render: (_: unknown, row: Execution) => (
        <Space>
          <Tooltip title="View logs">
            <Button
              type="text"
              icon={<EyeOutlined />}
              size="small"
              style={{ color: '#6366f1' }}
              onClick={() => {
                setLogsDrawer(row)
                setLogsLoading(true)
                void api.getExecution(row.id)
                  .then((full) => {
                    setLogsDrawer((prev) => {
                      if (!prev || prev.id !== row.id) return prev
                      return {
                        ...prev,
                        ...full,
                        pipeline_name: prev.pipeline_name,
                        triggered_by: prev.triggered_by || (full as any)?.triggered_by || 'manual',
                      }
                    })
                  })
                  .finally(() => setLogsLoading(false))
              }}
            />
          </Tooltip>
          <Tooltip title="Delete">
            <Button
              type="text"
              icon={<DeleteOutlined />}
              size="small"
              style={{ color: '#ef4444' }}
              onClick={() => {
                Modal.confirm({
                  title: 'Delete this execution record?',
                  okText: 'Delete',
                  okButtonProps: { danger: true },
                  onOk: () => deleteExecution(row.id),
                  styles: { content: { background: 'var(--app-card-bg)' }, header: { background: 'var(--app-card-bg)' }, footer: { background: 'var(--app-card-bg)' } }
                })
              }}
            />
          </Tooltip>
        </Space>
      ),
    },
  ]

  return (
    <div style={{ padding: '24px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <Title level={4} style={{ color: 'var(--app-text)', margin: 0 }}>Executions</Title>
          <Text style={{ color: 'var(--app-text-subtle)' }}>{executions.length} execution records</Text>
        </div>
        <Button icon={<ReloadOutlined />} onClick={() => fetchExecutions()} style={{ borderColor: 'var(--app-border-strong)', color: 'var(--app-text-muted)', background: 'var(--app-card-bg)' }}>
          Refresh
        </Button>
      </div>

      {/* Filters */}
      <Space style={{ marginBottom: 16 }}>
        <Input
          placeholder="Search by pipeline name or ID..."
          prefix={<SearchOutlined style={{ color: 'var(--app-text-subtle)' }} />}
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)', borderRadius: 8, width: 300 }}
          allowClear
        />
        <Select
          value={statusFilter}
          onChange={setStatusFilter}
          style={{ width: 140 }}
          options={[
            { value: 'all', label: 'All statuses' },
            { value: 'success', label: '✅ Success' },
            { value: 'failed', label: '❌ Failed' },
            { value: 'running', label: '⏳ Running' },
          ]}
        />
      </Space>

      {/* Stats row */}
      <Row gutter={[12, 12]} style={{ marginBottom: 20 }}>
        {Object.entries(statusConfig).filter(([k]) => ['success','failed','running','pending'].includes(k)).map(([key, cfg]) => {
          const count = executions.filter(e => e.status === key).length
          return (
            <Col key={key}>
              <div style={{
                padding: '8px 16px', background: `${cfg.color}10`,
                border: `1px solid ${cfg.color}30`, borderRadius: 8,
                display: 'flex', alignItems: 'center', gap: 8,
              }}>
                <span style={{ color: cfg.color }}>{cfg.icon}</span>
                <span style={{ color: cfg.color, fontWeight: 600 }}>{count}</span>
                <span style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>{cfg.text}</span>
              </div>
            </Col>
          )
        })}
      </Row>

      <Table
        dataSource={filtered}
        columns={columns}
        rowKey="id"
        loading={loading}
        pagination={{ pageSize: 20, showSizeChanger: true }}
        style={{ background: 'transparent' }}
        rowClassName={() => 'exec-row'}
        locale={{ emptyText: <Text style={{ color: 'var(--app-text-subtle)' }}>No executions found</Text> }}
      />

      {/* Logs Drawer */}
      <Drawer
        title={
          <Space>
            <EyeOutlined style={{ color: '#6366f1' }} />
            <span style={{ color: 'var(--app-text)' }}>Execution Logs</span>
            {logsDrawer && (
              <Tag
                style={{
                  background: `${statusConfig[logsDrawer.status]?.color}18`,
                  border: `1px solid ${statusConfig[logsDrawer.status]?.color}40`,
                  color: statusConfig[logsDrawer.status]?.color,
                  borderRadius: 6,
                }}
              >
                {logsDrawer.status}
              </Tag>
            )}
          </Space>
        }
        open={!!logsDrawer}
        onClose={() => setLogsDrawer(null)}
        width={600}
        styles={{ body: { background: 'var(--app-shell-bg)', padding: '16px' }, header: { background: 'var(--app-card-bg)', borderBottom: '1px solid var(--app-border-strong)' }, wrapper: { background: 'var(--app-shell-bg)' } }}
      >
        {logsDrawer && (
          <Space direction="vertical" style={{ width: '100%' }} size={8}>
            {/* Meta */}
            <Card style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8 }} bodyStyle={{ padding: '12px 16px' }}>
              <Row gutter={16}>
                <Col span={12}><Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Pipeline</Text><br /><Text style={{ color: 'var(--app-text)' }}>{logsDrawer.pipeline_name}</Text></Col>
                <Col span={12}><Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Duration</Text><br /><Text style={{ color: 'var(--app-text)' }}>{logsDrawer.duration ? `${logsDrawer.duration}s` : '—'}</Text></Col>
                <Col span={12} style={{ marginTop: 8 }}><Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Rows</Text><br /><Text style={{ color: 'var(--app-text)' }}>{logsDrawer.rows_processed?.toLocaleString()}</Text></Col>
                <Col span={12} style={{ marginTop: 8 }}><Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Triggered by</Text><br /><Text style={{ color: 'var(--app-text)' }}>{logsDrawer.triggered_by}</Text></Col>
              </Row>
            </Card>

            {/* Error */}
            {logsDrawer.error_message && (
              <div style={{ background: '#ef444415', border: '1px solid #ef444440', borderRadius: 8, padding: '12px 16px' }}>
                <Text style={{ color: '#ef4444', fontSize: 12, fontFamily: 'monospace' }}>{logsDrawer.error_message}</Text>
              </div>
            )}

            {/* Log entries */}
            <div style={{ fontFamily: 'monospace', fontSize: 12 }}>
              {logsLoading && (
                <Text style={{ color: 'var(--app-text-dim)', display: 'block', marginBottom: 8 }}>
                  Loading logs...
                </Text>
              )}
              {(logsDrawer.logs || []).map((log: LogEntry, i: number) => (
                <div
                  key={i}
                  style={{
                    padding: '8px 12px', marginBottom: 4,
                    background: log.status === 'error' ? '#ef444410' : 'var(--app-card-bg)',
                    border: `1px solid ${log.status === 'error' ? '#ef444430' : 'var(--app-border-strong)'}`,
                    borderRadius: 6,
                  }}
                >
                  <span style={{ color: 'var(--app-text-dim)', marginRight: 8 }}>
                    {dayjs(log.timestamp).format('HH:mm:ss')}
                  </span>
                  <span style={{ color: log.status === 'error' ? '#ef4444' : log.status === 'success' ? '#22c55e' : '#6366f1', marginRight: 8 }}>
                    {log.status === 'success' ? '✓' : log.status === 'error' ? '✗' : '⟳'}
                  </span>
                  <span style={{ color: 'var(--app-text)' }}>{log.message}</span>
                  {log.rows > 0 && (
                    <span style={{ color: '#6366f1', marginLeft: 8, fontSize: 11 }}>
                      [{log.rows.toLocaleString()} rows]
                    </span>
                  )}
                </div>
              ))}
              {(!logsDrawer.logs || logsDrawer.logs.length === 0) && (
                <Text style={{ color: 'var(--app-text-dim)' }}>No detailed logs available.</Text>
              )}
            </div>
          </Space>
        )}
      </Drawer>
    </div>
  )
}
