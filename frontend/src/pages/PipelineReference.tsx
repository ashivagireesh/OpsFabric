import { useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Alert,
  Anchor,
  Button,
  Card,
  Col,
  Collapse,
  Divider,
  Empty,
  Input,
  Row,
  Segmented,
  Space,
  Statistic,
  Tag,
  Typography,
} from 'antd'
import {
  ApiOutlined,
  BarChartOutlined,
  BranchesOutlined,
  ExperimentOutlined,
  LinkOutlined,
  SearchOutlined,
} from '@ant-design/icons'
import { ALL_NODE_TYPES, NODE_CATEGORIES } from '../constants/nodeTypes'
import { ALL_MLOPS_NODE_TYPES, MLOPS_NODE_CATEGORIES } from '../constants/mlopsNodeTypes'
import { ALL_BUSINESS_NODE_TYPES, BUSINESS_NODE_CATEGORIES } from '../constants/businessNodeTypes'
import type { ConfigField, NodeCategory, NodeTypeDefinition } from '../types'

const { Title, Text, Paragraph } = Typography

type ReferenceModuleKey = 'etl' | 'mlops' | 'business'

type CategoryMeta = { label: string; color: string; icon: string }

type StarterBlueprint = {
  id: string
  title: string
  goal: string
  flow: string[]
}

type ModuleCatalog = {
  key: ReferenceModuleKey
  label: string
  summary: string
  nodes: NodeTypeDefinition[]
  categories: Record<NodeCategory, CategoryMeta>
  workbenchLabel: string
  workbenchPath: string
  blueprints: StarterBlueprint[]
}

const CATEGORY_ORDER: NodeCategory[] = ['trigger', 'source', 'transform', 'flow', 'destination']

const MODULES: ModuleCatalog[] = [
  {
    key: 'etl',
    label: 'ETL Pipeline',
    summary:
      'Design extraction, transformation, enrichment, profile updates and loading pipelines across databases, files, APIs and document stores.',
    nodes: ALL_NODE_TYPES,
    categories: NODE_CATEGORIES as Record<NodeCategory, CategoryMeta>,
    workbenchLabel: 'Pipeline Workbench',
    workbenchPath: '/pipelines',
    blueprints: [
      {
        id: 'etl-api-ops',
        title: 'API to Analytics Table',
        goal: 'Ingest REST payload, normalize fields, aggregate KPIs and persist to Oracle/PostgreSQL.',
        flow: ['Schedule', 'REST API', 'Custom Fields', 'Aggregate / Group By', 'Oracle Destination'],
      },
      {
        id: 'etl-profileing',
        title: 'Customer Profile Incremental Build',
        goal: 'Ingest transactions incrementally, update profile JSON documents and expose for downstream analytics.',
        flow: ['Manual Trigger', 'Oracle Source', 'Custom Fields', 'LMDB/RocksDB Source', 'CSV/DB Destination'],
      },
      {
        id: 'etl-quality',
        title: 'Data Quality and Routing',
        goal: 'Apply validation rules, split pass/fail paths and merge curated outputs.',
        flow: ['Webhook', 'CSV/JSON Source', 'Filter', 'If / Condition', 'Merge', 'REST API Destination'],
      },
    ],
  },
  {
    key: 'mlops',
    label: 'MLOps Studio',
    summary:
      'Build model pipelines from dataset intake to feature engineering, training, evaluation, deployment and batch/real-time scoring.',
    nodes: ALL_MLOPS_NODE_TYPES,
    categories: MLOPS_NODE_CATEGORIES as Record<NodeCategory, CategoryMeta>,
    workbenchLabel: 'MLOps Workbench',
    workbenchPath: '/mlops',
    blueprints: [
      {
        id: 'mlops-train-deploy',
        title: 'Train and Deploy',
        goal: 'Prepare data, train model, evaluate threshold and deploy endpoint with quality gate.',
        flow: ['Manual Run', 'Dataset Source', 'Feature Engineering', 'Model Training', 'Model Evaluation', 'Deploy Endpoint'],
      },
      {
        id: 'mlops-etl-source',
        title: 'Train from ETL Output',
        goal: 'Reuse curated ETL outputs as ML training/scoring source with reproducible pipeline.',
        flow: ['Schedule', 'ETL Pipeline Output', 'Data Staging', 'Train/Test Split', 'Model Training', 'Model Registry'],
      },
      {
        id: 'mlops-batch-scoring',
        title: 'Batch Prediction Publishing',
        goal: 'Run recurring predictions and publish result table for dashboards and business workflows.',
        flow: ['Schedule', 'Dataset Source', 'Model Training', 'Forecasting', 'Batch Scoring'],
      },
    ],
  },
  {
    key: 'business',
    label: 'Business Logic Workflow',
    summary:
      'Orchestrate AI-driven decisions and actions from ETL/MLOps outputs with prompt logic, branching, mail and WhatsApp operations.',
    nodes: ALL_BUSINESS_NODE_TYPES,
    categories: BUSINESS_NODE_CATEGORIES as Record<NodeCategory, CategoryMeta>,
    workbenchLabel: 'Business Workbench',
    workbenchPath: '/business',
    blueprints: [
      {
        id: 'biz-alerting',
        title: 'Decision to Communication',
        goal: 'Use prompt model to decide action, branch with rule gate, then send stakeholder communication.',
        flow: ['Manual Trigger', 'ETL Output Source', 'Prompt Decision Model', 'Decision Gate', 'Mail Writer'],
      },
      {
        id: 'biz-mlops-outcome',
        title: 'Model Outcome Actioning',
        goal: 'Read MLOps predictions, summarize business implications and trigger WhatsApp updates.',
        flow: ['Schedule Trigger', 'MLOps Output Source', 'Business Analytics', 'Prompt Decision Model', 'WhatsApp Sender'],
      },
      {
        id: 'biz-ops-control',
        title: 'Parallel Action Orchestration',
        goal: 'Split and merge actions for different teams while preserving traceability in workflow runs.',
        flow: ['Manual Trigger', 'ETL Output Source', 'Decision Gate', 'Mail Writer / WhatsApp Sender', 'Merge'],
      },
    ],
  },
]

function deriveFieldValue(field: ConfigField): unknown {
  if (field.defaultValue !== undefined) {
    return shortenValue(field.defaultValue)
  }
  if (field.type === 'number') return 1
  if (field.type === 'toggle') return false
  if (field.type === 'select') return field.options?.[0]?.value ?? ''
  if (field.type === 'json') return {}
  if (field.type === 'code') return field.placeholder || `-- ${field.label}`
  if (field.type === 'textarea') return field.placeholder || `${field.label}...`
  if (field.type === 'password') return '***'
  if (field.type === 'file') return '/path/to/file'
  return field.placeholder || `<${field.name}>`
}

function shortenValue(value: unknown): unknown {
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (trimmed.length > 120) return `${trimmed.slice(0, 117)}...`
    return trimmed
  }
  if (Array.isArray(value)) return value.slice(0, 6)
  return value
}

function buildSampleConfig(node: NodeTypeDefinition): Record<string, unknown> {
  const required = node.configFields.filter((field) => Boolean(field.required))
  const optional = node.configFields.filter((field) => !field.required)
  const selectedFields = [...required, ...optional].slice(0, 8)
  const out: Record<string, unknown> = {}
  selectedFields.forEach((field) => {
    out[field.name] = deriveFieldValue(field)
  })
  return out
}

function buildNodeFlow(node: NodeTypeDefinition): string {
  if (node.category === 'trigger') return `${node.label} -> Source -> Transform -> Destination`
  if (node.category === 'source') return `Trigger -> ${node.label} -> Transform -> Destination`
  if (node.category === 'transform') return `Trigger -> Source -> ${node.label} -> Destination`
  if (node.category === 'flow') return `Trigger -> Source -> ${node.label} -> Branch A/B -> Merge`
  return `Trigger -> Source -> Transform -> ${node.label}`
}

function includesSearch(node: NodeTypeDefinition, search: string): boolean {
  if (!search) return true
  const q = search.toLowerCase()
  return [
    node.label,
    node.type,
    node.description,
    ...(node.tags || []),
  ].some((entry) => String(entry || '').toLowerCase().includes(q))
}

export default function PipelineReference() {
  const navigate = useNavigate()
  const [activeModuleKey, setActiveModuleKey] = useState<ReferenceModuleKey>('etl')
  const [searchText, setSearchText] = useState('')

  const activeModule = useMemo(
    () => MODULES.find((moduleItem) => moduleItem.key === activeModuleKey) || MODULES[0],
    [activeModuleKey],
  )

  const filteredNodes = useMemo(
    () => activeModule.nodes.filter((node) => includesSearch(node, searchText.trim())),
    [activeModule, searchText],
  )

  const groupedNodes = useMemo(() => (
    CATEGORY_ORDER
      .map((category) => ({
        category,
        nodes: filteredNodes.filter((node) => node.category === category),
      }))
      .filter((group) => group.nodes.length > 0)
  ), [filteredNodes])

  const anchorItems = useMemo(() => {
    const top = [
      { key: 'overview', href: '#overview', title: 'Overview' },
      { key: 'blueprints', href: '#blueprints', title: 'Pipeline Blueprints' },
    ]
    const categoryItems = groupedNodes.map((group) => {
      const categoryMeta = activeModule.categories[group.category]
      return {
        key: `cat-${group.category}`,
        href: `#cat-${group.category}`,
        title: `${categoryMeta?.icon || ''} ${categoryMeta?.label || group.category} (${group.nodes.length})`,
      }
    })
    return [...top, ...categoryItems]
  }, [groupedNodes, activeModule])

  const totalConfigFields = useMemo(
    () => activeModule.nodes.reduce((sum, node) => sum + node.configFields.length, 0),
    [activeModule],
  )

  return (
    <div style={{ padding: 20, background: 'var(--app-shell-bg)', minHeight: '100%' }}>
      <Row gutter={[16, 16]} align="top">
        <Col xs={24} lg={7} xl={6}>
          <div style={{ position: 'sticky', top: 16 }}>
            <Space direction="vertical" size={12} style={{ width: '100%' }}>
              <Card
                style={{ borderRadius: 14, background: 'var(--app-panel-bg)', borderColor: 'var(--app-border)' }}
                bodyStyle={{ padding: 14 }}
              >
                <Space direction="vertical" size={12} style={{ width: '100%' }}>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Reference Workbench</Text>
                  <Segmented
                    block
                    value={activeModuleKey}
                    onChange={(value) => setActiveModuleKey(value as ReferenceModuleKey)}
                    options={MODULES.map((item) => {
                      let iconNode = <ApiOutlined />
                      if (item.key === 'mlops') iconNode = <ExperimentOutlined />
                      if (item.key === 'business') iconNode = <BranchesOutlined />
                      return {
                        label: (
                          <Space size={6}>
                            {iconNode}
                            <span>{item.label}</span>
                          </Space>
                        ),
                        value: item.key,
                      }
                    })}
                  />
                  <Input
                    allowClear
                    value={searchText}
                    onChange={(event) => setSearchText(event.target.value)}
                    prefix={<SearchOutlined />}
                    placeholder="Search nodes, tags, operations"
                  />
                  <Button
                    type="primary"
                    icon={<LinkOutlined />}
                    block
                    onClick={() => navigate(activeModule.workbenchPath)}
                  >
                    Open {activeModule.workbenchLabel}
                  </Button>
                  <Button
                    block
                    icon={<BarChartOutlined />}
                    onClick={() => navigate('/dashboards')}
                  >
                    Open Visualizations
                  </Button>
                </Space>
              </Card>

              <Card
                title={<span style={{ color: 'var(--app-text)' }}>Section Navigation</span>}
                style={{ borderRadius: 14, background: 'var(--app-panel-bg)', borderColor: 'var(--app-border)' }}
                bodyStyle={{ padding: 12 }}
              >
                <Anchor
                  items={anchorItems}
                  targetOffset={72}
                  style={{ maxHeight: 420, overflow: 'auto', paddingInlineEnd: 8 }}
                />
              </Card>
            </Space>
          </div>
        </Col>

        <Col xs={24} lg={17} xl={18}>
          <Space direction="vertical" size={16} style={{ width: '100%' }}>
            <Card
              id="overview"
              style={{ borderRadius: 16, background: 'var(--app-panel-bg)', borderColor: 'var(--app-border)' }}
            >
              <Space direction="vertical" size={12} style={{ width: '100%' }}>
                <Space align="center" wrap>
                  <Title level={3} style={{ margin: 0, color: 'var(--app-text)' }}>
                    {activeModule.label} Reference
                  </Title>
                  <Tag color="blue">{activeModule.nodes.length} Nodes</Tag>
                  <Tag color="purple">{totalConfigFields} Config Parameters</Tag>
                  {searchText.trim() ? <Tag color="gold">Filtered: {filteredNodes.length}</Tag> : null}
                </Space>
                <Paragraph style={{ margin: 0, color: 'var(--app-text-subtle)' }}>
                  {activeModule.summary}
                </Paragraph>
                <Alert
                  type="info"
                  showIcon
                  message="This reference is generated from live node definitions."
                  description="Each node section includes purpose, operation model, field-level configuration reference, pipeline construction pattern and a ready sample configuration."
                />
                <Row gutter={[12, 12]}>
                  <Col xs={24} sm={8}>
                    <Card size="small" style={{ borderRadius: 10 }}>
                      <Statistic title="Triggers" value={activeModule.nodes.filter((item) => item.category === 'trigger').length} />
                    </Card>
                  </Col>
                  <Col xs={24} sm={8}>
                    <Card size="small" style={{ borderRadius: 10 }}>
                      <Statistic title="Sources + Transforms" value={activeModule.nodes.filter((item) => item.category === 'source' || item.category === 'transform').length} />
                    </Card>
                  </Col>
                  <Col xs={24} sm={8}>
                    <Card size="small" style={{ borderRadius: 10 }}>
                      <Statistic title="Destinations + Flow" value={activeModule.nodes.filter((item) => item.category === 'destination' || item.category === 'flow').length} />
                    </Card>
                  </Col>
                </Row>
              </Space>
            </Card>

            <Card
              id="blueprints"
              title={<span style={{ color: 'var(--app-text)' }}>Pipeline Blueprints</span>}
              style={{ borderRadius: 16, background: 'var(--app-panel-bg)', borderColor: 'var(--app-border)' }}
            >
              <Row gutter={[12, 12]}>
                {activeModule.blueprints.map((blueprint) => (
                  <Col xs={24} xl={8} key={blueprint.id}>
                    <Card
                      size="small"
                      style={{ height: '100%', borderRadius: 12, borderColor: 'var(--app-border)' }}
                      actions={[
                        <Button key="open" type="link" onClick={() => navigate(activeModule.workbenchPath)}>
                          Build in {activeModule.workbenchLabel}
                        </Button>,
                      ]}
                    >
                      <Space direction="vertical" size={8} style={{ width: '100%' }}>
                        <Text strong style={{ color: 'var(--app-text)' }}>{blueprint.title}</Text>
                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>{blueprint.goal}</Text>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                          {blueprint.flow.map((step, idx) => (
                            <Tag key={`${blueprint.id}-${idx}`} color={idx === blueprint.flow.length - 1 ? 'green' : 'blue'}>
                              {step}
                            </Tag>
                          ))}
                        </div>
                      </Space>
                    </Card>
                  </Col>
                ))}
              </Row>
            </Card>

            {groupedNodes.length === 0 ? (
              <Card style={{ borderRadius: 16, background: 'var(--app-panel-bg)', borderColor: 'var(--app-border)' }}>
                <Empty description="No nodes match current search" />
              </Card>
            ) : null}

            {groupedNodes.map((group) => {
              const categoryMeta = activeModule.categories[group.category]
              return (
                <Card
                  key={group.category}
                  id={`cat-${group.category}`}
                  title={(
                    <Space align="center">
                      <Tag color={categoryMeta?.color || 'default'} style={{ marginInlineEnd: 0 }}>
                        {categoryMeta?.icon || '•'}
                      </Tag>
                      <span style={{ color: 'var(--app-text)' }}>
                        {categoryMeta?.label || group.category} ({group.nodes.length})
                      </span>
                    </Space>
                  )}
                  style={{ borderRadius: 16, background: 'var(--app-panel-bg)', borderColor: 'var(--app-border)' }}
                >
                  <Collapse
                    ghost
                    items={group.nodes.map((node) => {
                      const sampleConfig = buildSampleConfig(node)
                      return {
                        key: node.type,
                        label: (
                          <Space align="center" wrap>
                            <span style={{ fontSize: 16 }}>{node.icon}</span>
                            <Text strong style={{ color: 'var(--app-text)' }}>{node.label}</Text>
                            <Tag>{node.type}</Tag>
                            <Tag color="geekblue">In: {node.inputs}</Tag>
                            <Tag color="cyan">Out: {node.outputs}</Tag>
                          </Space>
                        ),
                        children: (
                          <Space direction="vertical" size={10} style={{ width: '100%' }}>
                            <Paragraph style={{ margin: 0, color: 'var(--app-text-subtle)' }}>
                              {node.description}
                            </Paragraph>

                            <Space wrap>
                              {(node.tags || []).map((tag) => (
                                <Tag key={`${node.type}-${tag}`} color="processing">{tag}</Tag>
                              ))}
                            </Space>

                            <Alert
                              showIcon
                              type="success"
                              message="Pipeline Construction Pattern"
                              description={<Text code>{buildNodeFlow(node)}</Text>}
                            />

                            <Divider style={{ margin: '4px 0 2px' }} />
                            <Text strong style={{ color: 'var(--app-text)' }}>Configuration Reference</Text>
                            {node.configFields.length === 0 ? (
                              <Text style={{ color: 'var(--app-text-subtle)' }}>No configuration needed for this node.</Text>
                            ) : (
                              <Space direction="vertical" size={8} style={{ width: '100%' }}>
                                {node.configFields.map((field) => (
                                  <Card
                                    key={`${node.type}-${field.name}`}
                                    size="small"
                                    style={{ borderRadius: 10, borderColor: 'var(--app-border)' }}
                                    bodyStyle={{ padding: 10 }}
                                  >
                                    <Space direction="vertical" size={4} style={{ width: '100%' }}>
                                      <Space wrap>
                                        <Text strong style={{ color: 'var(--app-text)' }}>{field.label}</Text>
                                        <Tag>{field.name}</Tag>
                                        <Tag color="purple">{field.type}</Tag>
                                        {field.required ? <Tag color="red">Required</Tag> : <Tag>Optional</Tag>}
                                      </Space>
                                      {field.description ? (
                                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>{field.description}</Text>
                                      ) : null}
                                      {field.defaultValue !== undefined ? (
                                        <Text code style={{ whiteSpace: 'pre-wrap' }}>
                                          default: {JSON.stringify(shortenValue(field.defaultValue))}
                                        </Text>
                                      ) : null}
                                    </Space>
                                  </Card>
                                ))}
                              </Space>
                            )}

                            <Divider style={{ margin: '4px 0 2px' }} />
                            <Text strong style={{ color: 'var(--app-text)' }}>Sample Configuration</Text>
                            <pre
                              style={{
                                margin: 0,
                                padding: 12,
                                borderRadius: 10,
                                background: 'var(--app-shell-bg-2)',
                                border: '1px solid var(--app-border)',
                                color: 'var(--app-text)',
                                fontSize: 12,
                                overflowX: 'auto',
                              }}
                            >
                              {JSON.stringify(sampleConfig, null, 2)}
                            </pre>

                            <Space wrap>
                              <Button type="primary" icon={<ApiOutlined />} onClick={() => navigate(activeModule.workbenchPath)}>
                                Open {activeModule.workbenchLabel}
                              </Button>
                              <Button icon={<BarChartOutlined />} onClick={() => navigate('/dashboards')}>
                                Open Visualizations
                              </Button>
                            </Space>
                          </Space>
                        ),
                      }
                    })}
                  />
                </Card>
              )
            })}
          </Space>
        </Col>
      </Row>
    </div>
  )
}
