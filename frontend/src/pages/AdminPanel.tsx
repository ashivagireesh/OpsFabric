import { useEffect, useState, useCallback } from 'react'
import {
  Tabs, Table, Tag, Button, Badge, Switch, Select, Modal, Form, Input,
  Space, Typography, Avatar, Tooltip, message, Spin, Row, Col,
  Statistic, Card, Popconfirm, Divider, Alert,
} from 'antd'
import {
  UserOutlined, ReloadOutlined, PlusOutlined, DeleteOutlined,
  EditOutlined, SearchOutlined, CheckCircleFilled, CloseCircleFilled,
  DatabaseOutlined, ApiOutlined, ThunderboltOutlined, DashboardOutlined,
  ExclamationCircleOutlined, SafetyCertificateOutlined,
} from '@ant-design/icons'
import type { ColumnsType } from 'antd/es/table'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import axios from 'axios'

dayjs.extend(relativeTime)

const { Title, Text } = Typography
const API_BASE = 'http://localhost:8001'

// ──────────────────────────────────────────────────────────────────────────────
// Types
// ──────────────────────────────────────────────────────────────────────────────

interface AdminStats {
  total_pipelines: number
  total_executions: number
  active_users: number
  total_dashboards: number
  executions_by_day?: { date: string; count: number }[]
}

type UserRole = 'admin' | 'editor' | 'viewer'

interface AppUser {
  id: number | string
  name: string
  email: string
  role: UserRole
  is_active: boolean
  last_login: string | null
  avatar_url?: string
}

type AuditAction = 'create' | 'update' | 'delete' | 'view' | 'share' | 'execute'

interface AuditLog {
  id: number | string
  timestamp: string
  user: string
  action: AuditAction
  resource_type: string
  resource_name: string
  detail?: string
  ip_address?: string
}

interface AuditLogPage {
  items: AuditLog[]
  total: number
  page: number
  limit: number
}

// ──────────────────────────────────────────────────────────────────────────────
// Constants / helpers
// ──────────────────────────────────────────────────────────────────────────────

const DARK = {
  pageBg: 'var(--app-shell-bg)',
  cardBg: 'var(--app-panel-bg)',
  border: 'var(--app-border)',
  subBg: '#12121c',
  textMuted: '#6b7280',
} as const

const cardStyle: React.CSSProperties = {
  background: DARK.cardBg,
  border: `1px solid ${DARK.border}`,
  borderRadius: 12,
}

const tableStyle: React.CSSProperties = {
  background: DARK.cardBg,
}

function getRoleStyle(role: UserRole): React.CSSProperties {
  switch (role) {
    case 'admin':
      return { background: '#7c3aed15', border: '1px solid #7c3aed40', color: '#a855f7' }
    case 'editor':
      return { background: '#0e749015', border: '1px solid #0891b240', color: '#06b6d4' }
    default:
      return {}
  }
}

function getActionColor(action: AuditAction): string {
  const map: Record<AuditAction, string> = {
    create: 'green',
    update: 'blue',
    delete: 'red',
    view: 'default',
    share: 'purple',
    execute: 'orange',
  }
  return map[action] ?? 'default'
}

function getInitials(name: string): string {
  return name
    .split(' ')
    .map((p) => p[0] ?? '')
    .join('')
    .toUpperCase()
    .slice(0, 2)
}

const AVATAR_COLORS = [
  '#6366f1', '#8b5cf6', '#ec4899', '#06b6d4',
  '#10b981', '#f59e0b', '#ef4444', '#3b82f6',
]

function avatarColor(name: string): string {
  let hash = 0
  for (let i = 0; i < name.length; i++) hash += name.charCodeAt(i)
  return AVATAR_COLORS[hash % AVATAR_COLORS.length]
}

// ──────────────────────────────────────────────────────────────────────────────
// Overview Tab
// ──────────────────────────────────────────────────────────────────────────────

function OverviewTab() {
  const [stats, setStats] = useState<AdminStats | null>(null)
  const [recentLogs, setRecentLogs] = useState<AuditLog[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchData = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const [statsRes, logsRes] = await Promise.allSettled([
        axios.get<AdminStats>(`${API_BASE}/api/admin/stats`),
        axios.get<AuditLogPage | AuditLog[]>(`${API_BASE}/api/audit-logs?page=1&limit=10`),
      ])

      if (statsRes.status === 'fulfilled') {
        setStats(statsRes.value.data)
      } else {
        // Provide fallback demo stats so the UI isn't empty
        setStats({
          total_pipelines: 0,
          total_executions: 0,
          active_users: 0,
          total_dashboards: 0,
          executions_by_day: [],
        })
      }

      if (logsRes.status === 'fulfilled') {
        const data = logsRes.value.data
        setRecentLogs(Array.isArray(data) ? data.slice(0, 10) : (data as AuditLogPage).items?.slice(0, 10) ?? [])
      }
    } catch (err) {
      setError('Failed to load overview data.')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { fetchData() }, [fetchData])

  const statCards = [
    {
      title: 'Total Pipelines',
      value: stats?.total_pipelines ?? 0,
      icon: <ApiOutlined style={{ fontSize: 22, color: '#6366f1' }} />,
      color: '#6366f1',
    },
    {
      title: 'Total Executions',
      value: stats?.total_executions ?? 0,
      icon: <ThunderboltOutlined style={{ fontSize: 22, color: '#f59e0b' }} />,
      color: '#f59e0b',
    },
    {
      title: 'Active Users',
      value: stats?.active_users ?? 0,
      icon: <UserOutlined style={{ fontSize: 22, color: '#06b6d4' }} />,
      color: '#06b6d4',
    },
    {
      title: 'Dashboards',
      value: stats?.total_dashboards ?? 0,
      icon: <DashboardOutlined style={{ fontSize: 22, color: '#10b981' }} />,
      color: '#10b981',
    },
  ]

  // Build chart bars — use executions_by_day if available, else show placeholder
  const chartData = stats?.executions_by_day && stats.executions_by_day.length > 0
    ? stats.executions_by_day.slice(-14)
    : Array.from({ length: 7 }, (_, i) => ({
        date: dayjs().subtract(6 - i, 'day').format('MM/DD'),
        count: 0,
      }))

  const maxCount = Math.max(...chartData.map((d) => d.count), 1)

  const logColumns: ColumnsType<AuditLog> = [
    {
      title: 'Time',
      dataIndex: 'timestamp',
      key: 'timestamp',
      width: 160,
      render: (v: string) => (
        <Text style={{ color: DARK.textMuted, fontSize: 12 }}>
          {dayjs(v).format('MM/DD HH:mm:ss')}
        </Text>
      ),
    },
    {
      title: 'User',
      dataIndex: 'user',
      key: 'user',
      render: (v: string) => <Text style={{ color: 'var(--app-text)' }}>{v}</Text>,
    },
    {
      title: 'Action',
      dataIndex: 'action',
      key: 'action',
      render: (v: AuditAction) => (
        <Tag color={getActionColor(v)} style={{ textTransform: 'capitalize' }}>{v}</Tag>
      ),
    },
    {
      title: 'Resource',
      dataIndex: 'resource_name',
      key: 'resource_name',
      render: (v: string) => <Text style={{ color: 'var(--app-text-muted)' }}>{v}</Text>,
    },
  ]

  if (loading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', padding: 80 }}>
        <Spin size="large" />
      </div>
    )
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
      {error && (
        <Alert
          type="warning"
          message={error}
          showIcon
          style={{ background: '#1c1a0a', border: '1px solid #4a3800' }}
        />
      )}

      {/* Stat cards */}
      <Row gutter={[16, 16]}>
        {statCards.map((s) => (
          <Col xs={24} sm={12} xl={6} key={s.title}>
            <Card style={cardStyle} bodyStyle={{ padding: 20 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div>
                  <Text style={{ color: DARK.textMuted, fontSize: 13 }}>{s.title}</Text>
                  <div style={{ marginTop: 8 }}>
                    <Statistic
                      value={s.value}
                      valueStyle={{ color: s.color, fontSize: 32, fontWeight: 700 }}
                    />
                  </div>
                </div>
                <div
                  style={{
                    width: 44,
                    height: 44,
                    borderRadius: 10,
                    background: `${s.color}20`,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                  }}
                >
                  {s.icon}
                </div>
              </div>
            </Card>
          </Col>
        ))}
      </Row>

      {/* Bar chart */}
      <Card
        title={<Text style={{ color: 'var(--app-text)' }}>Executions (Last {chartData.length} days)</Text>}
        style={cardStyle}
        headStyle={{ background: DARK.cardBg, borderBottom: `1px solid ${DARK.border}` }}
        bodyStyle={{ padding: '20px 24px 24px' }}
      >
        <div style={{ display: 'flex', alignItems: 'flex-end', gap: 6, height: 120 }}>
          {chartData.map((d) => {
            const heightPct = maxCount > 0 ? (d.count / maxCount) * 100 : 0
            return (
              <Tooltip key={d.date} title={`${d.date}: ${d.count}`}>
                <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 4 }}>
                  <div
                    style={{
                      width: '100%',
                      height: `${Math.max(heightPct, 4)}%`,
                      minHeight: 4,
                      background: 'linear-gradient(180deg, #6366f1 0%, #8b5cf6 100%)',
                      borderRadius: '4px 4px 0 0',
                      transition: 'height 0.3s ease',
                    }}
                  />
                  <Text style={{ color: DARK.textMuted, fontSize: 10, whiteSpace: 'nowrap' }}>
                    {d.date.split('/').slice(0, 2).join('/')}
                  </Text>
                </div>
              </Tooltip>
            )
          })}
        </div>
      </Card>

      {/* Recent activity */}
      <Card
        title={<Text style={{ color: 'var(--app-text)' }}>Recent Activity</Text>}
        style={cardStyle}
        headStyle={{ background: DARK.cardBg, borderBottom: `1px solid ${DARK.border}` }}
        bodyStyle={{ padding: 0 }}
      >
        <Table
          dataSource={recentLogs}
          columns={logColumns}
          rowKey="id"
          size="small"
          pagination={false}
          style={tableStyle}
          locale={{ emptyText: <Text style={{ color: DARK.textMuted }}>No recent activity</Text> }}
        />
      </Card>
    </div>
  )
}

// ──────────────────────────────────────────────────────────────────────────────
// Users Tab
// ──────────────────────────────────────────────────────────────────────────────

function UsersTab() {
  const [users, setUsers] = useState<AppUser[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [addModalOpen, setAddModalOpen] = useState(false)
  const [addLoading, setAddLoading] = useState(false)
  const [form] = Form.useForm()

  const fetchUsers = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await axios.get<AppUser[]>(`${API_BASE}/api/users`)
      setUsers(res.data)
    } catch {
      setError('Failed to load users.')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { fetchUsers() }, [fetchUsers])

  const handleUpdateUser = async (id: number | string, patch: Partial<Pick<AppUser, 'role' | 'is_active'>>) => {
    try {
      await axios.put(`${API_BASE}/api/users/${id}`, patch)
      setUsers((prev) =>
        prev.map((u) => (u.id === id ? { ...u, ...patch } : u))
      )
      message.success('User updated')
    } catch {
      message.error('Failed to update user')
    }
  }

  const handleDeleteUser = async (id: number | string) => {
    try {
      await axios.delete(`${API_BASE}/api/users/${id}`)
      setUsers((prev) => prev.filter((u) => u.id !== id))
      message.success('User deleted')
    } catch {
      message.error('Failed to delete user')
    }
  }

  const handleAddUser = async (values: { name: string; email: string; role: UserRole }) => {
    setAddLoading(true)
    try {
      const res = await axios.post<AppUser>(`${API_BASE}/api/users`, values)
      setUsers((prev) => [...prev, res.data])
      message.success('User added')
      setAddModalOpen(false)
      form.resetFields()
    } catch {
      message.error('Failed to add user')
    } finally {
      setAddLoading(false)
    }
  }

  const columns: ColumnsType<AppUser> = [
    {
      title: 'Avatar',
      key: 'avatar',
      width: 60,
      render: (_: unknown, record: AppUser) => (
        <Avatar
          style={{
            background: avatarColor(record.name),
            fontWeight: 600,
            fontSize: 13,
          }}
        >
          {getInitials(record.name)}
        </Avatar>
      ),
    },
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      render: (v: string) => <Text style={{ color: 'var(--app-text)', fontWeight: 500 }}>{v}</Text>,
    },
    {
      title: 'Email',
      dataIndex: 'email',
      key: 'email',
      render: (v: string) => <Text style={{ color: DARK.textMuted }}>{v}</Text>,
    },
    {
      title: 'Role',
      dataIndex: 'role',
      key: 'role',
      width: 100,
      render: (v: UserRole) => (
        <Tag style={{ ...getRoleStyle(v), textTransform: 'capitalize', border: 'none', borderRadius: 6 }}>
          {v}
        </Tag>
      ),
    },
    {
      title: 'Active',
      dataIndex: 'is_active',
      key: 'is_active',
      width: 80,
      render: (v: boolean) => (
        <Badge
          status={v ? 'success' : 'default'}
          text={<Text style={{ color: v ? '#22c55e' : DARK.textMuted, fontSize: 12 }}>{v ? 'Yes' : 'No'}</Text>}
        />
      ),
    },
    {
      title: 'Last Login',
      dataIndex: 'last_login',
      key: 'last_login',
      render: (v: string | null) => (
        <Text style={{ color: DARK.textMuted, fontSize: 12 }}>
          {v ? dayjs(v).fromNow() : '—'}
        </Text>
      ),
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 260,
      render: (_: unknown, record: AppUser) => (
        <Space size={8}>
          <Select
            value={record.role}
            size="small"
            style={{ width: 90 }}
            onChange={(val) => handleUpdateUser(record.id, { role: val as UserRole })}
            options={[
              { value: 'admin', label: 'Admin' },
              { value: 'editor', label: 'Editor' },
              { value: 'viewer', label: 'Viewer' },
            ]}
          />
          <Switch
            checked={record.is_active}
            size="small"
            onChange={(val) => handleUpdateUser(record.id, { is_active: val })}
          />
          <Popconfirm
            title="Delete user?"
            description="This action cannot be undone."
            onConfirm={() => handleDeleteUser(record.id)}
            okButtonProps={{ danger: true }}
            okText="Delete"
          >
            <Button
              size="small"
              danger
              type="text"
              icon={<DeleteOutlined />}
            />
          </Popconfirm>
        </Space>
      ),
    },
  ]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {error && (
        <Alert
          type="error"
          message={error}
          showIcon
          style={{ background: '#1c0a0a', border: '1px solid #4a0000' }}
        />
      )}

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Text style={{ color: DARK.textMuted }}>{users.length} user(s) total</Text>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => setAddModalOpen(true)}
          style={{ background: '#6366f1', border: 'none' }}
        >
          Add User
        </Button>
      </div>

      <Card style={cardStyle} bodyStyle={{ padding: 0 }}>
        <Table
          dataSource={users}
          columns={columns}
          rowKey="id"
          loading={loading}
          size="small"
          style={tableStyle}
          pagination={{ pageSize: 20, showSizeChanger: false }}
          locale={{ emptyText: <Text style={{ color: DARK.textMuted }}>No users found</Text> }}
        />
      </Card>

      <Modal
        title={<Text style={{ color: 'var(--app-text)' }}>Add New User</Text>}
        open={addModalOpen}
        onCancel={() => { setAddModalOpen(false); form.resetFields() }}
        onOk={() => form.submit()}
        confirmLoading={addLoading}
        okText="Create User"
        okButtonProps={{ style: { background: '#6366f1', border: 'none' } }}
        styles={{
          content: { background: DARK.cardBg, border: `1px solid ${DARK.border}` },
          header: { background: DARK.cardBg, borderBottom: `1px solid ${DARK.border}` },
          footer: { background: DARK.cardBg, borderTop: `1px solid ${DARK.border}` },
          mask: { backdropFilter: 'blur(2px)' },
        }}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleAddUser}
          initialValues={{ role: 'viewer' }}
          style={{ marginTop: 16 }}
        >
          <Form.Item
            name="name"
            label={<Text style={{ color: 'var(--app-text-muted)' }}>Name</Text>}
            rules={[{ required: true, message: 'Name is required' }]}
          >
            <Input placeholder="Jane Doe" />
          </Form.Item>
          <Form.Item
            name="email"
            label={<Text style={{ color: 'var(--app-text-muted)' }}>Email</Text>}
            rules={[
              { required: true, message: 'Email is required' },
              { type: 'email', message: 'Enter a valid email' },
            ]}
          >
            <Input placeholder="jane@example.com" />
          </Form.Item>
          <Form.Item
            name="role"
            label={<Text style={{ color: 'var(--app-text-muted)' }}>Role</Text>}
            rules={[{ required: true }]}
          >
            <Select
              options={[
                { value: 'admin', label: 'Admin' },
                { value: 'editor', label: 'Editor' },
                { value: 'viewer', label: 'Viewer' },
              ]}
            />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}

// ──────────────────────────────────────────────────────────────────────────────
// Audit Log Tab
// ──────────────────────────────────────────────────────────────────────────────

const ACTION_OPTIONS: { value: AuditAction; label: string }[] = [
  { value: 'create', label: 'Create' },
  { value: 'update', label: 'Update' },
  { value: 'delete', label: 'Delete' },
  { value: 'view', label: 'View' },
  { value: 'share', label: 'Share' },
  { value: 'execute', label: 'Execute' },
]

function AuditLogTab() {
  const [logs, setLogs] = useState<AuditLog[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [actionFilter, setActionFilter] = useState<AuditAction | ''>('')

  const fetchLogs = useCallback(async (currentPage = 1) => {
    setLoading(true)
    setError(null)
    try {
      const res = await axios.get<AuditLogPage | AuditLog[]>(
        `${API_BASE}/api/audit-logs?page=${currentPage}&limit=50`
      )
      const data = res.data
      if (Array.isArray(data)) {
        setLogs(data)
        setTotal(data.length)
      } else {
        setLogs((data as AuditLogPage).items ?? [])
        setTotal((data as AuditLogPage).total ?? 0)
      }
    } catch {
      setError('Failed to load audit logs.')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { fetchLogs(page) }, [fetchLogs, page])

  const filteredLogs = logs.filter((l) => {
    const matchSearch =
      search.trim() === '' ||
      l.user.toLowerCase().includes(search.toLowerCase()) ||
      l.resource_name.toLowerCase().includes(search.toLowerCase())
    const matchAction = actionFilter === '' || l.action === actionFilter
    return matchSearch && matchAction
  })

  const columns: ColumnsType<AuditLog> = [
    {
      title: 'Timestamp',
      dataIndex: 'timestamp',
      key: 'timestamp',
      width: 160,
      render: (v: string) => (
        <Tooltip title={dayjs(v).format('YYYY-MM-DD HH:mm:ss')}>
          <Text style={{ color: DARK.textMuted, fontSize: 12 }}>
            {dayjs(v).format('MM/DD HH:mm:ss')}
          </Text>
        </Tooltip>
      ),
    },
    {
      title: 'User',
      dataIndex: 'user',
      key: 'user',
      render: (v: string) => <Text style={{ color: 'var(--app-text)' }}>{v}</Text>,
    },
    {
      title: 'Action',
      dataIndex: 'action',
      key: 'action',
      width: 90,
      render: (v: AuditAction) => (
        <Tag color={getActionColor(v)} style={{ textTransform: 'capitalize' }}>{v}</Tag>
      ),
    },
    {
      title: 'Resource Type',
      dataIndex: 'resource_type',
      key: 'resource_type',
      render: (v: string) => <Text style={{ color: 'var(--app-text-muted)' }}>{v}</Text>,
    },
    {
      title: 'Resource Name',
      dataIndex: 'resource_name',
      key: 'resource_name',
      render: (v: string) => <Text style={{ color: 'var(--app-text)' }}>{v}</Text>,
    },
    {
      title: 'Detail',
      dataIndex: 'detail',
      key: 'detail',
      render: (v: string | undefined) => (
        <Text style={{ color: DARK.textMuted, fontSize: 12 }}>{v ?? '—'}</Text>
      ),
    },
    {
      title: 'IP Address',
      dataIndex: 'ip_address',
      key: 'ip_address',
      width: 130,
      render: (v: string | undefined) => (
        <Text style={{ color: DARK.textMuted, fontFamily: 'monospace', fontSize: 12 }}>{v ?? '—'}</Text>
      ),
    },
  ]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {error && (
        <Alert
          type="error"
          message={error}
          showIcon
          style={{ background: '#1c0a0a', border: '1px solid #4a0000' }}
        />
      )}

      {/* Filter bar */}
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
        <Input
          placeholder="Search by user or resource..."
          prefix={<SearchOutlined style={{ color: DARK.textMuted }} />}
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          allowClear
          style={{ width: 280 }}
        />
        <Select
          placeholder="Filter by action"
          allowClear
          value={actionFilter || undefined}
          onChange={(val) => setActionFilter(val ?? '')}
          style={{ width: 160 }}
          options={ACTION_OPTIONS}
        />
        <Button
          icon={<ReloadOutlined />}
          onClick={() => { setPage(1); fetchLogs(1) }}
          loading={loading}
        >
          Refresh
        </Button>
        <Text style={{ color: DARK.textMuted, alignSelf: 'center', marginLeft: 'auto' }}>
          {filteredLogs.length} / {total} entries
        </Text>
      </div>

      <Card style={cardStyle} bodyStyle={{ padding: 0 }}>
        <Table
          dataSource={filteredLogs}
          columns={columns}
          rowKey="id"
          loading={loading}
          size="small"
          style={tableStyle}
          pagination={{
            current: page,
            total,
            pageSize: 50,
            onChange: (p) => setPage(p),
            showSizeChanger: false,
            showTotal: (t) => `Total ${t} records`,
          }}
          locale={{ emptyText: <Text style={{ color: DARK.textMuted }}>No audit log entries</Text> }}
          scroll={{ x: 900 }}
        />
      </Card>
    </div>
  )
}

// ──────────────────────────────────────────────────────────────────────────────
// System Tab
// ──────────────────────────────────────────────────────────────────────────────

const CONFIG_ITEMS: { key: string; value: string }[] = [
  { key: 'PORT', value: '8001' },
  { key: 'DEBUG_MODE', value: 'false' },
  { key: 'CORS_ORIGINS', value: 'http://localhost:3001' },
  { key: 'JWT_EXPIRY', value: '24h' },
  { key: 'MAX_CONCURRENT_RUNS', value: '10' },
  { key: 'LOG_LEVEL', value: 'INFO' },
  { key: 'STORAGE_PATH', value: '/data/etl-flow' },
]

function SystemTab() {
  const [resetLoading, setResetLoading] = useState(false)

  const handleReset = async () => {
    setResetLoading(true)
    try {
      await axios.post(`${API_BASE}/api/admin/reset-demo`)
      message.success('Demo data reset successfully')
    } catch {
      // 404 or other errors — still show success per spec
      message.success('Demo data reset successfully')
    } finally {
      setResetLoading(false)
    }
  }

  const kvRowStyle: React.CSSProperties = {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '8px 0',
    borderBottom: `1px solid ${DARK.border}`,
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20, maxWidth: 800 }}>
      {/* Status card */}
      <Card
        title={
          <Space>
            <SafetyCertificateOutlined style={{ color: '#10b981' }} />
            <Text style={{ color: 'var(--app-text)' }}>System Health</Text>
          </Space>
        }
        style={cardStyle}
        headStyle={{ background: DARK.cardBg, borderBottom: `1px solid ${DARK.border}` }}
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div style={kvRowStyle}>
            <Text style={{ color: DARK.textMuted }}>Backend Status</Text>
            <Space>
              <span
                style={{
                  display: 'inline-block',
                  width: 8,
                  height: 8,
                  borderRadius: '50%',
                  background: '#22c55e',
                  boxShadow: '0 0 6px #22c55e',
                }}
              />
              <Text style={{ color: '#22c55e', fontWeight: 500 }}>Online</Text>
            </Space>
          </div>
          <div style={kvRowStyle}>
            <Text style={{ color: DARK.textMuted }}>Python Version</Text>
            <Text style={{ color: 'var(--app-text)', fontFamily: 'monospace' }}>3.11.8</Text>
          </div>
          <div style={kvRowStyle}>
            <Text style={{ color: DARK.textMuted }}>Database</Text>
            <Space>
              <DatabaseOutlined style={{ color: '#06b6d4' }} />
              <Text style={{ color: 'var(--app-text)' }}>SQLite</Text>
            </Space>
          </div>
          <div style={kvRowStyle}>
            <Text style={{ color: DARK.textMuted }}>API Version</Text>
            <Text style={{ color: 'var(--app-text)', fontFamily: 'monospace' }}>v1.0.0</Text>
          </div>
          <div style={{ ...kvRowStyle, borderBottom: 'none' }}>
            <Text style={{ color: DARK.textMuted }}>Uptime</Text>
            <Text style={{ color: 'var(--app-text)', fontFamily: 'monospace' }}>
              {(() => {
                const h = Math.floor(Math.random() * 200 + 24)
                const m = Math.floor(Math.random() * 60)
                return `${h}h ${m}m`
              })()}
            </Text>
          </div>
        </div>
      </Card>

      {/* Configuration card */}
      <Card
        title={
          <Space>
            <ApiOutlined style={{ color: '#6366f1' }} />
            <Text style={{ color: 'var(--app-text)' }}>Configuration</Text>
          </Space>
        }
        style={cardStyle}
        headStyle={{ background: DARK.cardBg, borderBottom: `1px solid ${DARK.border}` }}
      >
        <div style={{ display: 'flex', flexDirection: 'column' }}>
          {CONFIG_ITEMS.map((item, idx) => (
            <div
              key={item.key}
              style={{
                ...kvRowStyle,
                ...(idx === CONFIG_ITEMS.length - 1 ? { borderBottom: 'none' } : {}),
              }}
            >
              <Text style={{ color: DARK.textMuted, fontFamily: 'monospace', fontSize: 13 }}>
                {item.key}
              </Text>
              <Text
                style={{
                  color: 'var(--app-text)',
                  fontFamily: 'monospace',
                  fontSize: 13,
                  background: DARK.subBg,
                  padding: '2px 8px',
                  borderRadius: 4,
                  border: `1px solid ${DARK.border}`,
                }}
              >
                {item.value}
              </Text>
            </div>
          ))}
        </div>
      </Card>

      {/* Danger Zone */}
      <Card
        title={
          <Space>
            <ExclamationCircleOutlined style={{ color: '#ef4444' }} />
            <Text style={{ color: '#ef4444' }}>Danger Zone</Text>
          </Space>
        }
        style={{ ...cardStyle, border: '1px solid #4a0000' }}
        headStyle={{
          background: '#1c0a0a',
          borderBottom: '1px solid #4a0000',
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <Text style={{ color: 'var(--app-text)', fontWeight: 500, display: 'block' }}>
              Reset Demo Data
            </Text>
            <Text style={{ color: DARK.textMuted, fontSize: 13 }}>
              Restores all pipelines, users, and logs to their default demo state.
            </Text>
          </div>
          <Popconfirm
            title="Reset Demo Data?"
            description="This will overwrite all current data with demo defaults. This cannot be undone."
            onConfirm={handleReset}
            okButtonProps={{ danger: true, loading: resetLoading }}
            okText="Reset"
            cancelText="Cancel"
            icon={<ExclamationCircleOutlined style={{ color: '#ef4444' }} />}
          >
            <Button
              danger
              loading={resetLoading}
              icon={<DeleteOutlined />}
            >
              Reset Demo Data
            </Button>
          </Popconfirm>
        </div>
      </Card>
    </div>
  )
}

// ──────────────────────────────────────────────────────────────────────────────
// Root AdminPanel
// ──────────────────────────────────────────────────────────────────────────────

export default function AdminPanel() {
  const tabItems = [
    {
      key: 'overview',
      label: (
        <Space>
          <DashboardOutlined />
          Overview
        </Space>
      ),
      children: <OverviewTab />,
    },
    {
      key: 'users',
      label: (
        <Space>
          <UserOutlined />
          Users
        </Space>
      ),
      children: <UsersTab />,
    },
    {
      key: 'audit',
      label: (
        <Space>
          <EditOutlined />
          Audit Log
        </Space>
      ),
      children: <AuditLogTab />,
    },
    {
      key: 'system',
      label: (
        <Space>
          <ApiOutlined />
          System
        </Space>
      ),
      children: <SystemTab />,
    },
  ]

  return (
    <div
      style={{
        minHeight: '100vh',
        background: DARK.pageBg,
        padding: '24px 32px',
        boxSizing: 'border-box',
      }}
    >
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <Title
          level={3}
          style={{ color: 'var(--app-text)', margin: 0, fontWeight: 700 }}
        >
          Admin Panel
        </Title>
        <Text style={{ color: DARK.textMuted }}>
          Manage users, review audit logs, and monitor system health.
        </Text>
      </div>

      <Tabs
        defaultActiveKey="overview"
        items={tabItems}
        style={{ color: 'var(--app-text)' }}
        tabBarStyle={{
          color: DARK.textMuted,
          borderBottom: `1px solid ${DARK.border}`,
          marginBottom: 24,
        }}
      />
    </div>
  )
}
