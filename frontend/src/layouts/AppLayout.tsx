import { useState } from 'react'
import { Outlet, useNavigate, useLocation } from 'react-router-dom'
import { Layout, Menu, Tooltip, Avatar, Space, Tag } from 'antd'
import {
  DashboardOutlined, ApiOutlined, HistoryOutlined, KeyOutlined,
  SettingOutlined, MenuFoldOutlined, MenuUnfoldOutlined, ThunderboltOutlined,
  RocketOutlined, GithubOutlined, BarChartOutlined, TeamOutlined, ExperimentOutlined,
  MessageOutlined, CodeOutlined, BranchesOutlined, CommentOutlined, BookOutlined,
} from '@ant-design/icons'
import { useThemeStore } from '../store/themeStore'

const { Sider, Content, Header } = Layout

const menuItems = [
  { key: '/dashboard', icon: <DashboardOutlined />, label: 'Dashboard' },
  { key: '/pipelines', icon: <ApiOutlined />, label: 'Pipelines' },
  { key: '/mlops', icon: <ExperimentOutlined />, label: 'MLOps Studio' },
  { key: '/business', icon: <BranchesOutlined />, label: 'Business Logic' },
  { key: '/executions', icon: <HistoryOutlined />, label: 'Executions' },
  { key: '/credentials', icon: <KeyOutlined />, label: 'Credentials' },
  { type: 'divider' as const },
  { key: '/dashboards', icon: <BarChartOutlined />, label: 'Visualizations' },
  { key: '/python-analytics', icon: <CodeOutlined />, label: 'Python Analytics' },
  { key: '/nlp-chat', icon: <MessageOutlined />, label: 'NLP Chat Studio' },
  { key: '/workflow-chat', icon: <CommentOutlined />, label: 'Workflow Chat' },
  { key: '/reference', icon: <BookOutlined />, label: 'Reference' },
  { type: 'divider' as const },
  { key: '/admin', icon: <TeamOutlined />, label: 'Admin' },
  { key: '/settings', icon: <SettingOutlined />, label: 'Settings' },
]

export default function AppLayout() {
  const [collapsed, setCollapsed] = useState(false)
  const mode = useThemeStore((state) => state.mode)
  const navigate = useNavigate()
  const location = useLocation()
  const isDark = mode === 'dark'

  const shellBg = isDark ? 'var(--app-shell-bg)' : '#f5f7fb'
  const panelBg = isDark ? 'var(--app-panel-bg)' : '#ffffff'
  const borderColor = isDark ? 'var(--app-border)' : '#dbe4f0'
  const mutedText = isDark ? '#888' : 'var(--app-text-subtle)'
  const brandTitle = isDark ? '#ffffff' : '#0f172a'
  const collapseIconColor = isDark ? '#666' : 'var(--app-text-muted)'
  const githubColor = isDark ? '#666' : 'var(--app-text-subtle)'

  return (
    <Layout style={{ height: '100vh', background: shellBg }}>
      {/* ── SIDEBAR ──────────────────────────────────────────────── */}
      <Sider
        collapsible
        collapsed={collapsed}
        onCollapse={setCollapsed}
        trigger={null}
        width={220}
        style={{
          background: panelBg,
          borderRight: `1px solid ${borderColor}`,
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        {/* Logo */}
        <div style={{
          padding: collapsed ? '20px 12px' : '20px 20px',
          borderBottom: `1px solid ${borderColor}`,
          display: 'flex',
          alignItems: 'center',
          gap: 10,
          cursor: 'pointer',
        }} onClick={() => navigate('/dashboard')}>
          <div style={{
            width: 32, height: 32,
            background: 'linear-gradient(135deg, #6366f1, #a855f7)',
            borderRadius: 8,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: 16, flexShrink: 0,
          }}>
            <ThunderboltOutlined style={{ color: '#fff' }} />
          </div>
          {!collapsed && (
            <div>
              <div style={{ color: brandTitle, fontWeight: 700, fontSize: 15, lineHeight: 1.2 }}>ETL Flow</div>
              <div style={{ color: '#6366f1', fontSize: 10, letterSpacing: '0.05em' }}>PIPELINE PLATFORM</div>
            </div>
          )}
        </div>

        {/* Navigation */}
        <Menu
          mode="inline"
          theme={isDark ? 'dark' : 'light'}
          selectedKeys={[menuItems.find(m => 'key' in m && location.pathname.startsWith(m.key as string))?.key as string || location.pathname]}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
          style={{ background: 'transparent', border: 'none', marginTop: 8, flex: 1 }}
        />

        {/* Collapse button */}
        <div
          style={{
            padding: '12px 16px',
            borderTop: `1px solid ${borderColor}`,
            cursor: 'pointer',
            color: collapseIconColor,
            display: 'flex',
            alignItems: 'center',
            justifyContent: collapsed ? 'center' : 'flex-end',
            transition: 'color 0.2s',
          }}
          onClick={() => setCollapsed(!collapsed)}
          onMouseEnter={(e) => (e.currentTarget.style.color = '#6366f1')}
          onMouseLeave={(e) => (e.currentTarget.style.color = collapseIconColor)}
        >
          {collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
        </div>
      </Sider>

      {/* ── MAIN AREA ────────────────────────────────────────────── */}
      <Layout style={{ background: shellBg }}>
        {/* Top Header */}
        <Header style={{
          background: panelBg,
          borderBottom: `1px solid ${borderColor}`,
          padding: '0 24px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          height: 52,
        }}>
          <div style={{ color: mutedText, fontSize: 13 }}>
            {menuItems.find(m => 'key' in m && location.pathname.startsWith(m.key as string))?.label || 'ETL Flow'}
          </div>
          <Space size={12}>
            <Tag
              icon={<RocketOutlined />}
              color={isDark ? 'purple' : 'blue'}
              style={{ cursor: 'pointer', fontSize: 11 }}
            >
              v1.0.0
            </Tag>
            <Tooltip title="GitHub">
              <GithubOutlined style={{ color: githubColor, fontSize: 16, cursor: 'pointer' }} />
            </Tooltip>
            <Avatar
              size={28}
              style={{ background: 'linear-gradient(135deg, #6366f1, #a855f7)', cursor: 'pointer', fontSize: 12 }}
            >
              U
            </Avatar>
          </Space>
        </Header>

        {/* Content */}
        <Content style={{
          overflow: 'auto',
          background: shellBg,
          height: 'calc(100vh - 52px)',
        }}>
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  )
}
