import { useEffect, useMemo, useState } from 'react'
import { Card, Typography, Space, Switch, Select, Input, Button, Divider, Tag, Row, Col, Checkbox, Popconfirm, message } from 'antd'
import { SettingOutlined, BellOutlined, SecurityScanOutlined, DatabaseOutlined, ApiOutlined } from '@ant-design/icons'
import { useThemeStore } from '../store/themeStore'
import api from '../api/client'

const { Title, Text } = Typography

type SQLiteUsage = {
  sqlite_enabled: boolean
  db_path: string | null
  db_exists: boolean
  db_size_bytes: number
  db_size_mb: number
  execution_total: number
  execution_running: number
  execution_terminal: number
  mlops_runs_total: number
  mlops_runs_running: number
  business_runs_total: number
  business_runs_running: number
  audit_logs_total: number
  generated_at: string
}

type SQLiteCleanupResult = {
  message: string
  before: SQLiteUsage
  after: SQLiteUsage
  updated: {
    executions: number
    mlops_runs: number
    business_runs: number
    audit_logs: number
  }
  vacuum: {
    attempted: boolean
    ok: boolean
    error: string
  }
  warnings: string[]
}

export default function Settings() {
  const mode = useThemeStore((state) => state.mode)
  const setMode = useThemeStore((state) => state.setMode)
  const [messageApi, contextHolder] = message.useMessage()

  const [sqliteUsage, setSqliteUsage] = useState<SQLiteUsage | null>(null)
  const [sqliteLoading, setSqliteLoading] = useState(false)
  const [cleanupRunning, setCleanupRunning] = useState(false)
  const [cleanupResult, setCleanupResult] = useState<SQLiteCleanupResult | null>(null)
  const [cleanupOptions, setCleanupOptions] = useState({
    clear_execution_runtime_payloads: true,
    clear_mlops_run_payloads: true,
    clear_business_run_payloads: true,
    clear_audit_logs: false,
    vacuum: true,
  })

  const hasAnyCleanupOption = useMemo(
    () => (
      cleanupOptions.clear_execution_runtime_payloads
      || cleanupOptions.clear_mlops_run_payloads
      || cleanupOptions.clear_business_run_payloads
      || cleanupOptions.clear_audit_logs
      || cleanupOptions.vacuum
    ),
    [cleanupOptions],
  )

  const formatBytes = (bytes: number) => {
    if (!Number.isFinite(bytes) || bytes <= 0) return '0 B'
    const units = ['B', 'KB', 'MB', 'GB', 'TB']
    let value = bytes
    let idx = 0
    while (value >= 1024 && idx < units.length - 1) {
      value /= 1024
      idx += 1
    }
    return `${value.toFixed(idx === 0 ? 0 : 2)} ${units[idx]}`
  }

  const loadSqliteUsage = async () => {
    setSqliteLoading(true)
    try {
      const usage = await api.getSqliteUsage()
      setSqliteUsage(usage)
    } catch (err: any) {
      messageApi.error(err?.message || 'Failed to load SQLite usage')
    } finally {
      setSqliteLoading(false)
    }
  }

  const runSqliteCleanup = async () => {
    if (!hasAnyCleanupOption || cleanupRunning) return
    setCleanupRunning(true)
    try {
      const result = await api.cleanupSqliteData(cleanupOptions)
      setCleanupResult(result)
      await loadSqliteUsage()
      if (Array.isArray(result?.warnings) && result.warnings.length > 0) {
        messageApi.warning(result.warnings[0])
      } else {
        messageApi.success('SQLite logs/processed runtime data cleared')
      }
    } catch (err: any) {
      messageApi.error(err?.message || 'SQLite cleanup failed')
    } finally {
      setCleanupRunning(false)
    }
  }

  useEffect(() => {
    void loadSqliteUsage()
  }, [])

  return (
    <div style={{ padding: '24px', maxWidth: 900 }}>
      {contextHolder}
      <div style={{ marginBottom: 24 }}>
        <Title level={4} style={{ color: 'var(--app-text)', margin: 0 }}>Settings</Title>
        <Text style={{ color: 'var(--app-text-subtle)' }}>Configure your ETL Flow platform preferences</Text>
      </div>

      <Space direction="vertical" style={{ width: '100%' }} size={16}>
        {/* General */}
        <SettingsSection icon={<SettingOutlined />} title="General">
          <SettingRow label="Application Theme" description="Choose dark or light mode for the complete product">
            <Select
              value={mode}
              onChange={(value) => setMode(value as 'dark' | 'light')}
              style={{ width: 180 }}
              options={[
                { value: 'dark', label: 'Dark Mode' },
                { value: 'light', label: 'Light Mode' },
              ]}
              dropdownStyle={{ background: 'var(--app-card-bg)' }}
            />
          </SettingRow>
          <Divider style={{ borderColor: 'var(--app-border)', margin: '12px 0' }} />
          <SettingRow label="Default Execution Timeout" description="Maximum time (seconds) a pipeline can run">
            <Input defaultValue="3600" style={{ width: 120, background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }} suffix={<Text style={{ color: 'var(--app-text-subtle)' }}>sec</Text>} />
          </SettingRow>
          <Divider style={{ borderColor: 'var(--app-border)', margin: '12px 0' }} />
          <SettingRow label="Default Row Limit" description="Maximum rows extracted per source node">
            <Input defaultValue="10000" style={{ width: 120, background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }} />
          </SettingRow>
          <Divider style={{ borderColor: 'var(--app-border)', margin: '12px 0' }} />
          <SettingRow label="Parallel Execution" description="Run independent pipeline branches in parallel">
            <Switch defaultChecked style={{ background: '#6366f1' }} />
          </SettingRow>
          <Divider style={{ borderColor: 'var(--app-border)', margin: '12px 0' }} />
          <SettingRow label="Auto-save Editor" description="Automatically save pipeline while editing">
            <Switch defaultChecked style={{ background: '#6366f1' }} />
          </SettingRow>
        </SettingsSection>

        {/* Notifications */}
        <SettingsSection icon={<BellOutlined />} title="Notifications">
          <SettingRow label="Execution Failure Alerts" description="Get notified when a pipeline fails">
            <Switch defaultChecked style={{ background: '#6366f1' }} />
          </SettingRow>
          <Divider style={{ borderColor: 'var(--app-border)', margin: '12px 0' }} />
          <SettingRow label="Execution Success Notifications" description="Get notified on successful completions">
            <Switch style={{ background: '#6366f1' }} />
          </SettingRow>
          <Divider style={{ borderColor: 'var(--app-border)', margin: '12px 0' }} />
          <SettingRow label="Webhook URL" description="POST execution events to this URL">
            <Input placeholder="https://hooks.example.com/notify" style={{ width: 280, background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }} />
          </SettingRow>
        </SettingsSection>

        {/* Database */}
        <SettingsSection icon={<DatabaseOutlined />} title="Storage & Database">
          <SettingRow label="Metadata Database" description="Where pipelines and executions are stored">
            <Space>
              <Tag color="green">SQLite (default)</Tag>
              <Button size="small" style={{ borderColor: 'var(--app-border-strong)', color: 'var(--app-text-muted)', background: 'var(--app-card-bg)' }}>
                Configure PostgreSQL
              </Button>
            </Space>
          </SettingRow>
          <Divider style={{ borderColor: 'var(--app-border)', margin: '12px 0' }} />
          <div style={{ marginBottom: 12 }}>
            <Space style={{ justifyContent: 'space-between', width: '100%', alignItems: 'center' }}>
              <div>
                <Text style={{ color: 'var(--app-text)', fontWeight: 500 }}>SQLite Runtime Cleanup</Text>
                <br />
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                  Clear unwanted logs and processed runtime payloads without deleting pipeline configuration.
                </Text>
              </div>
              <Button
                size="small"
                onClick={() => void loadSqliteUsage()}
                loading={sqliteLoading}
                style={{ borderColor: 'var(--app-border-strong)', color: 'var(--app-text-muted)', background: 'var(--app-card-bg)' }}
              >
                Refresh
              </Button>
            </Space>
          </div>

          <Row gutter={[12, 12]} style={{ marginBottom: 12 }}>
            <Col xs={24} md={12}>
              <Card size="small" style={{ background: 'var(--app-bg-secondary)', border: '1px solid var(--app-border)' }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Database Size</Text>
                <br />
                <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>
                  {formatBytes(Number(sqliteUsage?.db_size_bytes || 0))}
                </Text>
                <br />
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  {sqliteUsage?.db_path || 'SQLite path unavailable'}
                </Text>
              </Card>
            </Col>
            <Col xs={24} md={12}>
              <Card size="small" style={{ background: 'var(--app-bg-secondary)', border: '1px solid var(--app-border)' }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Execution Runtime Rows</Text>
                <br />
                <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>
                  {Number(sqliteUsage?.execution_terminal || 0)}
                </Text>
                <br />
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  Running: {Number(sqliteUsage?.execution_running || 0)}
                </Text>
              </Card>
            </Col>
          </Row>

          <Row gutter={[12, 12]} style={{ marginBottom: 12 }}>
            <Col xs={24} md={8}>
              <Card size="small" style={{ background: 'var(--app-bg-secondary)', border: '1px solid var(--app-border)' }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>MLOps Runs</Text>
                <br />
                <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>
                  {Number(sqliteUsage?.mlops_runs_total || 0)}
                </Text>
              </Card>
            </Col>
            <Col xs={24} md={8}>
              <Card size="small" style={{ background: 'var(--app-bg-secondary)', border: '1px solid var(--app-border)' }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Business Runs</Text>
                <br />
                <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>
                  {Number(sqliteUsage?.business_runs_total || 0)}
                </Text>
              </Card>
            </Col>
            <Col xs={24} md={8}>
              <Card size="small" style={{ background: 'var(--app-bg-secondary)', border: '1px solid var(--app-border)' }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Audit Logs</Text>
                <br />
                <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>
                  {Number(sqliteUsage?.audit_logs_total || 0)}
                </Text>
              </Card>
            </Col>
          </Row>

          <Space direction="vertical" size={8} style={{ width: '100%' }}>
            <Checkbox
              checked={cleanupOptions.clear_execution_runtime_payloads}
              onChange={(e) => setCleanupOptions((prev) => ({ ...prev, clear_execution_runtime_payloads: e.target.checked }))}
            >
              <Text style={{ color: 'var(--app-text)' }}>Clear execution logs and processed node results</Text>
            </Checkbox>
            <Checkbox
              checked={cleanupOptions.clear_mlops_run_payloads}
              onChange={(e) => setCleanupOptions((prev) => ({ ...prev, clear_mlops_run_payloads: e.target.checked }))}
            >
              <Text style={{ color: 'var(--app-text)' }}>Clear MLOps run logs/metrics payloads</Text>
            </Checkbox>
            <Checkbox
              checked={cleanupOptions.clear_business_run_payloads}
              onChange={(e) => setCleanupOptions((prev) => ({ ...prev, clear_business_run_payloads: e.target.checked }))}
            >
              <Text style={{ color: 'var(--app-text)' }}>Clear Business workflow run payloads</Text>
            </Checkbox>
            <Checkbox
              checked={cleanupOptions.clear_audit_logs}
              onChange={(e) => setCleanupOptions((prev) => ({ ...prev, clear_audit_logs: e.target.checked }))}
            >
              <Text style={{ color: 'var(--app-text)' }}>Clear audit logs table</Text>
            </Checkbox>
            <Checkbox
              checked={cleanupOptions.vacuum}
              onChange={(e) => setCleanupOptions((prev) => ({ ...prev, vacuum: e.target.checked }))}
            >
              <Text style={{ color: 'var(--app-text)' }}>Run SQLite VACUUM after cleanup</Text>
            </Checkbox>
          </Space>

          <div style={{ marginTop: 12, display: 'flex', justifyContent: 'space-between', gap: 8, flexWrap: 'wrap' }}>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
              Last refresh: {sqliteUsage?.generated_at ? new Date(sqliteUsage.generated_at).toLocaleString() : 'N/A'}
            </Text>
            <Popconfirm
              title="Clear selected SQLite runtime data?"
              description="This keeps pipeline/workflow configuration intact."
              okText="Clear"
              cancelText="Cancel"
              onConfirm={() => void runSqliteCleanup()}
              disabled={!hasAnyCleanupOption || cleanupRunning}
            >
              <Button
                type="primary"
                danger
                loading={cleanupRunning}
                disabled={!hasAnyCleanupOption || cleanupRunning}
              >
                Clear Selected Data
              </Button>
            </Popconfirm>
          </div>

          {cleanupResult ? (
            <>
              <Divider style={{ borderColor: 'var(--app-border)', margin: '12px 0' }} />
              <Text style={{ color: 'var(--app-text)', fontWeight: 500 }}>Last cleanup result</Text>
              <div style={{ marginTop: 8 }}>
                <Tag color="blue">Executions: {cleanupResult.updated.executions}</Tag>
                <Tag color="purple">MLOps: {cleanupResult.updated.mlops_runs}</Tag>
                <Tag color="cyan">Business: {cleanupResult.updated.business_runs}</Tag>
                <Tag color="magenta">Audit: {cleanupResult.updated.audit_logs}</Tag>
                <Tag color={cleanupResult.vacuum.ok ? 'green' : 'orange'}>
                  VACUUM: {cleanupResult.vacuum.ok ? 'OK' : (cleanupResult.vacuum.attempted ? 'Skipped/Failed' : 'Not Run')}
                </Tag>
              </div>
            </>
          ) : null}

          <Divider style={{ borderColor: 'var(--app-border)', margin: '12px 0' }} />
          <SettingRow label="Execution Log Retention" description="Automatically delete execution logs older than">
            <Select
              defaultValue="30"
              style={{ width: 160 }}
              options={[
                { value: '7', label: '7 days' },
                { value: '30', label: '30 days' },
                { value: '90', label: '90 days' },
                { value: '365', label: '1 year' },
                { value: '0', label: 'Never delete' },
              ]}
            />
          </SettingRow>
        </SettingsSection>

        {/* API */}
        <SettingsSection icon={<ApiOutlined />} title="API & Integrations">
          <SettingRow label="API Key" description="Use this key to access the ETL Flow REST API">
            <Space>
              <Input.Password
                defaultValue="sk-etlflow-xxxxxxxxxxxxx"
                style={{ width: 240, background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }}
                readOnly
              />
              <Button size="small" style={{ borderColor: 'var(--app-border-strong)', color: 'var(--app-text-muted)', background: 'var(--app-card-bg)' }}>
                Regenerate
              </Button>
            </Space>
          </SettingRow>
          <Divider style={{ borderColor: 'var(--app-border)', margin: '12px 0' }} />
          <SettingRow label="CORS Origins" description="Allowed origins for API access">
            <Input defaultValue="*" style={{ width: 280, background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }} />
          </SettingRow>
        </SettingsSection>

        {/* Security */}
        <SettingsSection icon={<SecurityScanOutlined />} title="Security">
          <SettingRow label="Encrypt Credentials at Rest" description="Encrypt stored credentials using AES-256">
            <Switch defaultChecked style={{ background: '#6366f1' }} />
          </SettingRow>
          <Divider style={{ borderColor: 'var(--app-border)', margin: '12px 0' }} />
          <SettingRow label="Python Sandbox" description="Execute Python Script nodes in a sandboxed environment">
            <Switch defaultChecked style={{ background: '#6366f1' }} />
          </SettingRow>
        </SettingsSection>

        <Button
          type="primary"
          style={{ background: 'linear-gradient(135deg, #6366f1, #a855f7)', border: 'none', marginTop: 8 }}
        >
          Save Settings
        </Button>
      </Space>
    </div>
  )
}

function SettingsSection({ icon, title, children }: { icon: React.ReactNode; title: string; children: React.ReactNode }) {
  return (
    <Card
      title={<Space style={{ color: 'var(--app-text)' }}>{icon} {title}</Space>}
      style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 12 }}
      headStyle={{ borderBottom: '1px solid var(--app-border-strong)' }}
    >
      {children}
    </Card>
  )
}

function SettingRow({ label, description, children }: { label: string; description: string; children: React.ReactNode }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 12 }}>
      <div>
        <Text style={{ color: 'var(--app-text)', fontWeight: 500 }}>{label}</Text>
        <br />
        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>{description}</Text>
      </div>
      <div>{children}</div>
    </div>
  )
}
