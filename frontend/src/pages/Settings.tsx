import { Card, Typography, Space, Switch, Select, Input, Button, Divider, Tag, Row, Col } from 'antd'
import { SettingOutlined, BellOutlined, SecurityScanOutlined, DatabaseOutlined, ApiOutlined } from '@ant-design/icons'
import { useThemeStore } from '../store/themeStore'

const { Title, Text } = Typography

export default function Settings() {
  const mode = useThemeStore((state) => state.mode)
  const setMode = useThemeStore((state) => state.setMode)

  return (
    <div style={{ padding: '24px', maxWidth: 900 }}>
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
