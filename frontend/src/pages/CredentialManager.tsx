import { useEffect, useState } from 'react'
import {
  Card, Button, Modal, Form, Input, Select, Typography, Space,
  Row, Col, Tag, Empty, Tooltip, notification
} from 'antd'
import {
  PlusOutlined, DeleteOutlined, KeyOutlined, DatabaseOutlined,
  CloudOutlined, ApiOutlined, ThunderboltOutlined
} from '@ant-design/icons'
import dayjs from 'dayjs'
import { useCredentialStore } from '../store'

const { Title, Text } = Typography

const credentialTypes = [
  { value: 'postgres', label: 'PostgreSQL', icon: '🐘', color: '#336791' },
  { value: 'mysql', label: 'MySQL', icon: '🐬', color: '#4479a1' },
  { value: 'oracle', label: 'Oracle', icon: '🟠', color: '#f97316' },
  { value: 'mongodb', label: 'MongoDB', icon: '🍃', color: '#4db33d' },
  { value: 'redis', label: 'Redis', icon: '🔴', color: '#dc382d' },
  { value: 'elasticsearch', label: 'Elasticsearch', icon: '🔍', color: '#f5a623' },
  { value: 's3', label: 'AWS S3', icon: '☁', color: '#ff9900' },
  { value: 'gcs', label: 'Google Cloud Storage', icon: '☁', color: '#4285f4' },
  { value: 'azure_blob', label: 'Azure Blob Storage', icon: '☁', color: '#0078d4' },
  { value: 'kafka', label: 'Apache Kafka', icon: '🌊', color: '#231f20' },
  { value: 'rabbitmq', label: 'RabbitMQ', icon: '🐰', color: '#ff6600' },
  { value: 'rest_api', label: 'REST API', icon: '🌐', color: '#06b6d4' },
  { value: 'sftp', label: 'FTP / SFTP', icon: '📁', color: '#8b5cf6' },
]

const credentialFields: Record<string, { name: string; label: string; type: string }[]> = {
  postgres: [
    { name: 'host', label: 'Host', type: 'text' },
    { name: 'port', label: 'Port', type: 'text' },
    { name: 'database', label: 'Database', type: 'text' },
    { name: 'user', label: 'Username', type: 'text' },
    { name: 'password', label: 'Password', type: 'password' },
  ],
  mysql: [
    { name: 'host', label: 'Host', type: 'text' },
    { name: 'port', label: 'Port', type: 'text' },
    { name: 'database', label: 'Database', type: 'text' },
    { name: 'user', label: 'Username', type: 'text' },
    { name: 'password', label: 'Password', type: 'password' },
  ],
  oracle: [
    { name: 'host', label: 'Host', type: 'text' },
    { name: 'port', label: 'Port', type: 'text' },
    { name: 'service_name', label: 'Service Name / Database', type: 'text' },
    { name: 'sid', label: 'SID (optional)', type: 'text' },
    { name: 'dsn', label: 'DSN (optional)', type: 'text' },
    { name: 'user', label: 'Username', type: 'text' },
    { name: 'password', label: 'Password', type: 'password' },
  ],
  mongodb: [
    { name: 'connection_string', label: 'Connection String', type: 'text' },
    { name: 'database', label: 'Default Database', type: 'text' },
  ],
  redis: [
    { name: 'host', label: 'Host', type: 'text' },
    { name: 'port', label: 'Port', type: 'text' },
    { name: 'password', label: 'Password', type: 'password' },
  ],
  elasticsearch: [
    { name: 'hosts', label: 'Hosts (comma-separated)', type: 'text' },
    { name: 'username', label: 'Username', type: 'text' },
    { name: 'password', label: 'Password', type: 'password' },
    { name: 'api_key', label: 'API Key (optional)', type: 'password' },
  ],
  s3: [
    { name: 'access_key', label: 'Access Key ID', type: 'text' },
    { name: 'secret_key', label: 'Secret Access Key', type: 'password' },
    { name: 'region', label: 'Region', type: 'text' },
    { name: 'endpoint', label: 'Custom Endpoint (optional)', type: 'text' },
  ],
  kafka: [
    { name: 'bootstrap_servers', label: 'Bootstrap Servers', type: 'text' },
    { name: 'username', label: 'SASL Username (optional)', type: 'text' },
    { name: 'password', label: 'SASL Password (optional)', type: 'password' },
  ],
  rest_api: [
    { name: 'base_url', label: 'Base URL', type: 'text' },
    { name: 'auth_type', label: 'Auth Type', type: 'text' },
    { name: 'token', label: 'Token / API Key', type: 'password' },
  ],
  sftp: [
    { name: 'host', label: 'Host', type: 'text' },
    { name: 'port', label: 'Port', type: 'text' },
    { name: 'username', label: 'Username', type: 'text' },
    { name: 'password', label: 'Password', type: 'password' },
    { name: 'private_key', label: 'Private Key (optional)', type: 'password' },
  ],
}

export default function CredentialManager() {
  const { credentials, fetchCredentials, createCredential, deleteCredential } = useCredentialStore()
  const [modalOpen, setModalOpen] = useState(false)
  const [selectedType, setSelectedType] = useState<string>('')
  const [form] = Form.useForm()

  useEffect(() => { fetchCredentials() }, [])

  const handleCreate = async () => {
    const values = await form.validateFields()
    const { name, type, ...rest } = values
    await createCredential({ name, type, data: rest })
    notification.success({ message: 'Credential saved!', placement: 'bottomRight' })
    setModalOpen(false)
    form.resetFields()
    setSelectedType('')
  }

  const handleDelete = (id: string, name: string) => {
    Modal.confirm({
      title: `Delete "${name}"?`,
      content: 'Pipelines using this credential will need to be updated.',
      okText: 'Delete',
      okButtonProps: { danger: true },
      styles: { content: { background: 'var(--app-card-bg)' }, header: { background: 'var(--app-card-bg)' }, footer: { background: 'var(--app-card-bg)' } },
      onOk: async () => { await deleteCredential(id); notification.success({ message: 'Credential deleted', placement: 'bottomRight' }) },
    })
  }

  const getTypeInfo = (type: string) => credentialTypes.find(t => t.value === type)

  return (
    <div style={{ padding: '24px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <Title level={4} style={{ color: 'var(--app-text)', margin: 0 }}>Credentials</Title>
          <Text style={{ color: 'var(--app-text-subtle)' }}>Manage connection credentials for data sources and destinations</Text>
        </div>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => setModalOpen(true)}
          style={{ background: 'linear-gradient(135deg, #6366f1, #a855f7)', border: 'none' }}
        >
          Add Credential
        </Button>
      </div>

      {credentials.length === 0 ? (
        <Empty
          description={<Text style={{ color: 'var(--app-text-subtle)' }}>No credentials yet</Text>}
          style={{ padding: 80 }}
        >
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setModalOpen(true)}
            style={{ background: 'linear-gradient(135deg, #6366f1, #a855f7)', border: 'none' }}
          >
            Add Credential
          </Button>
        </Empty>
      ) : (
        <Row gutter={[16, 16]}>
          {credentials.map(cred => {
            const typeInfo = getTypeInfo(cred.type)
            return (
              <Col xs={24} sm={12} lg={8} xl={6} key={cred.id}>
                <Card
                  style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 12 }}
                  bodyStyle={{ padding: '16px 20px' }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                      <div style={{
                        width: 36, height: 36,
                        background: `${typeInfo?.color || '#6366f1'}18`,
                        borderRadius: 9,
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                        fontSize: 18,
                      }}>
                        {typeInfo?.icon || '🔑'}
                      </div>
                      <div>
                        <Text style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 14 }}>{cred.name}</Text>
                        <div>
                          <Tag style={{
                            background: `${typeInfo?.color || '#6366f1'}18`,
                            border: `1px solid ${typeInfo?.color || '#6366f1'}30`,
                            color: typeInfo?.color || '#6366f1',
                            borderRadius: 4, fontSize: 10,
                          }}>
                            {typeInfo?.label || cred.type}
                          </Tag>
                        </div>
                      </div>
                    </div>
                    <Tooltip title="Delete">
                      <Button
                        type="text"
                        icon={<DeleteOutlined />}
                        size="small"
                        style={{ color: '#ef4444' }}
                        onClick={() => handleDelete(cred.id, cred.name)}
                      />
                    </Tooltip>
                  </div>
                  <Text style={{ color: 'var(--app-text-dim)', fontSize: 11, display: 'block', marginTop: 10 }}>
                    Added {dayjs(cred.created_at).fromNow()}
                  </Text>
                </Card>
              </Col>
            )
          })}
        </Row>
      )}

      {/* Create Modal */}
      <Modal
        title={<span style={{ color: 'var(--app-text)' }}>Add Credential</span>}
        open={modalOpen}
        onOk={handleCreate}
        onCancel={() => { setModalOpen(false); form.resetFields(); setSelectedType('') }}
        okText="Save Credential"
        okButtonProps={{ style: { background: 'linear-gradient(135deg, #6366f1, #a855f7)', border: 'none' } }}
        width={520}
        styles={{ content: { background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)' }, header: { background: 'var(--app-card-bg)', borderBottom: '1px solid var(--app-border-strong)' }, footer: { background: 'var(--app-card-bg)', borderTop: '1px solid var(--app-border-strong)' }, mask: { backdropFilter: 'blur(4px)' } }}
      >
        <Form form={form} layout="vertical" style={{ marginTop: 16 }}>
          <Form.Item name="name" label={<span style={{ color: 'var(--app-text-muted)' }}>Credential Name</span>}
            rules={[{ required: true }]}>
            <Input placeholder="e.g. Production PostgreSQL" style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }} />
          </Form.Item>
          <Form.Item name="type" label={<span style={{ color: 'var(--app-text-muted)' }}>Type</span>}
            rules={[{ required: true }]}>
            <Select
              placeholder="Select credential type"
              onChange={v => { setSelectedType(v); form.resetFields(['host','port','database','user','username','password','connection_string','access_key','secret_key','region','bootstrap_servers','token','base_url','private_key','api_key','endpoint']) }}
              options={credentialTypes.map(t => ({
                value: t.value,
                label: <Space><span>{t.icon}</span><span>{t.label}</span></Space>
              }))}
            />
          </Form.Item>
          {selectedType && (credentialFields[selectedType] || []).map(f => (
            <Form.Item key={f.name} name={f.name} label={<span style={{ color: 'var(--app-text-muted)' }}>{f.label}</span>}>
              {f.type === 'password'
                ? <Input.Password style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }} />
                : <Input style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }} />
              }
            </Form.Item>
          ))}
        </Form>
      </Modal>
    </div>
  )
}
