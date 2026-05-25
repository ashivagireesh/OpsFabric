import { useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Form,
  Input,
  InputNumber,
  Modal,
  Select,
  Space,
  Switch,
  Table,
  Tabs,
  Tag,
  Typography,
  notification,
} from 'antd'
import {
  DeleteOutlined,
  FileTextOutlined,
  PlusOutlined,
  SafetyCertificateOutlined,
} from '@ant-design/icons'
import api from '../../api/client'

const { Text, Title } = Typography

type RuleOperator = '>' | '>=' | '<' | '<=' | '=' | '!=' | 'contains' | 'exists'
type ConditionJoin = 'and' | 'or'
type TemplateMappingSource = 'field' | 'custom'

interface RRECondition {
  id: string
  field: string
  operator: RuleOperator
  value: string
}

interface RREConditionGroup {
  id: string
  join: ConditionJoin
  conditions: RRECondition[]
  groups: RREConditionGroup[]
}

interface RRETemplateMapping {
  placeholder: string
  source: TemplateMappingSource
  field: string
  value: string
}

interface RRERule {
  id: string
  name: string
  enabled: boolean
  priority: number
  rootGroup: RREConditionGroup
  templateId: string
  templateName?: string
  templateBody?: string
  templateResponsibility?: string
  templateMappings: RRETemplateMapping[]
}

interface RRETemplate {
  id: string
  name: string
  responsibility: string
  body: string
}

interface RREStudioProps {
  sourceFields?: string[]
  predictionFields?: string[]
  sourceRows?: Array<Record<string, unknown>>
  predictionRows?: Array<Record<string, unknown>>
  rulesConfig?: RRERule[]
  onRulesConfigChange?: (rules: RRERule[]) => void
}

const sampleModelOutput = {
  source: {
    customer_id: 'C-10291',
    segment: 'SME',
    monthly_revenue: 128000,
    active_days: 24,
  },
  predictions: {
    ensemble_prediction: 'approve_with_review',
    risk_score: 0.74,
    confidence: 0.88,
    drift_flag: false,
    recommendation_band: 'review',
  },
  model_outputs: {
    xgboost_score: 0.79,
    random_forest_score: 0.72,
  },
}

const initialTemplates: RRETemplate[] = []

const operatorOptions: { label: string; value: RuleOperator }[] = [
  { label: 'Greater than', value: '>' },
  { label: 'Greater than or equal', value: '>=' },
  { label: 'Less than', value: '<' },
  { label: 'Less than or equal', value: '<=' },
  { label: 'Equals', value: '=' },
  { label: 'Not equals', value: '!=' },
  { label: 'Contains', value: 'contains' },
  { label: 'Exists', value: 'exists' },
]

const joinOptions: { label: string; value: ConditionJoin }[] = [
  { label: 'AND', value: 'and' },
  { label: 'OR', value: 'or' },
]

function newCondition(seed?: Partial<RRECondition>): RRECondition {
  return {
    id: seed?.id || `cond-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    field: seed?.field || 'risk_score',
    operator: seed?.operator || '>=',
    value: seed?.value || '0.7',
  }
}

function newGroup(seed?: Partial<RREConditionGroup>): RREConditionGroup {
  return {
    id: seed?.id || `grp-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    join: seed?.join || 'and',
    conditions: seed?.conditions || [newCondition()],
    groups: seed?.groups || [],
  }
}

const initialRules: RRERule[] = []

function normalizeCondition(raw: any): RRECondition {
  return newCondition({
    id: String(raw?.id || ''),
    field: String(raw?.field || ''),
    operator: operatorOptions.some((item) => item.value === raw?.operator) ? raw.operator : 'exists',
    value: String(raw?.value ?? ''),
  })
}

function normalizeGroup(raw: any): RREConditionGroup {
  const join: ConditionJoin = raw?.join === 'or' ? 'or' : 'and'
  const conditions = Array.isArray(raw?.conditions) ? raw.conditions.map(normalizeCondition) : []
  const groups = Array.isArray(raw?.groups) ? raw.groups.map(normalizeGroup) : []
  return newGroup({
    id: String(raw?.id || ''),
    join,
    conditions,
    groups,
  })
}

function normalizeMappings(raw: any): RRETemplateMapping[] {
  if (!Array.isArray(raw)) return []
  return raw.map((item): RRETemplateMapping => ({
    placeholder: String(item?.placeholder || ''),
    source: item?.source === 'custom' ? 'custom' : 'field',
    field: String(item?.field || ''),
    value: String(item?.value ?? ''),
  })).filter((item) => item.placeholder)
}

function normalizeRulesConfig(raw: unknown): RRERule[] {
  if (!Array.isArray(raw)) return initialRules
  if (raw.length === 0) return []
  return raw.map((item: any, index) => ({
    id: String(item?.id || `rule-${Date.now()}-${index}`),
    name: String(item?.name || `Rule ${index + 1}`),
    enabled: item?.enabled !== false,
    priority: Number.isFinite(Number(item?.priority)) ? Number(item.priority) : index + 1,
    rootGroup: normalizeGroup(item?.rootGroup),
    templateId: String(item?.templateId || ''),
    templateName: String(item?.templateName || item?.template_name || ''),
    templateBody: String(item?.templateBody || item?.template_body || ''),
    templateResponsibility: String(item?.templateResponsibility || item?.template_responsibility || ''),
    templateMappings: normalizeMappings(item?.templateMappings),
  }))
}

function cloneRule(rule: RRERule): RRERule {
  return JSON.parse(JSON.stringify(rule)) as RRERule
}

function countGroupConditions(group: RREConditionGroup): number {
  return group.conditions.length + group.groups.reduce((sum, child) => sum + countGroupConditions(child), 0)
}

function ruleOutputColumn(rule: RRERule): string {
  return String(rule.name || rule.id || '').trim()
}

function templateFromRuleSnapshot(rule: RRERule): RRETemplate | null {
  const id = String(rule.templateId || '').trim()
  const body = String(rule.templateBody || '').trim()
  const name = String(rule.templateName || id || '').trim()
  if (!id || (!body && !name)) return null
  return {
    id,
    name: name || id,
    responsibility: String(rule.templateResponsibility || ''),
    body,
  }
}

function readField(row: Record<string, unknown>, path: string): unknown {
  return path.split('.').reduce<unknown>((current, key) => {
    if (!current || typeof current !== 'object' || Array.isArray(current)) return undefined
    return (current as Record<string, unknown>)[key]
  }, row)
}

function assignPath(target: Record<string, unknown>, path: string, value: unknown) {
  const parts = path.split('.').map((item) => item.trim()).filter(Boolean)
  if (parts.length === 0) return
  let current = target
  parts.slice(0, -1).forEach((part) => {
    const existing = current[part]
    if (!existing || typeof existing !== 'object' || Array.isArray(existing)) current[part] = {}
    current = current[part] as Record<string, unknown>
  })
  current[parts[parts.length - 1]] = value
}

function buildModelOutputFromFields({
  sourceFields = [],
  predictionFields = [],
  sourceRows = [],
  predictionRows = [],
}: RREStudioProps): Record<string, unknown> {
  if (sourceFields.length === 0 && predictionFields.length === 0) return sampleModelOutput
  const sourceRow = sourceRows.find((row) => row && typeof row === 'object') || {}
  const predictionRow = predictionRows.find((row) => row && typeof row === 'object') || {}
  const source: Record<string, unknown> = {}
  const predictions: Record<string, unknown> = {}
  sourceFields.forEach((field) => assignPath(source, field, readField(sourceRow, field) ?? null))
  predictionFields.forEach((field) => assignPath(predictions, field, readField(predictionRow, field) ?? readField(sourceRow, field) ?? null))
  return { source, predictions }
}

function formatScalar(value: unknown): string {
  if (value === null || value === undefined) return ''
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return String(value)
  return JSON.stringify(value)
}

function compareValue(actual: unknown, operator: RuleOperator, expectedRaw: string): boolean {
  if (operator === 'exists') return actual !== undefined && actual !== null && String(actual).trim() !== ''
  const actualText = String(actual ?? '')
  const expectedText = String(expectedRaw ?? '')
  if (operator === 'contains') return actualText.toLowerCase().includes(expectedText.toLowerCase())
  if (operator === '=') return actualText.toLowerCase() === expectedText.toLowerCase()
  if (operator === '!=') return actualText.toLowerCase() !== expectedText.toLowerCase()

  const actualNumber = Number(actual)
  const expectedNumber = Number(expectedRaw)
  if (!Number.isFinite(actualNumber) || !Number.isFinite(expectedNumber)) return false
  if (operator === '>') return actualNumber > expectedNumber
  if (operator === '>=') return actualNumber >= expectedNumber
  if (operator === '<') return actualNumber < expectedNumber
  if (operator === '<=') return actualNumber <= expectedNumber
  return false
}

function evaluateGroup(group: RREConditionGroup, output: Record<string, unknown>): boolean {
  const conditionResults = group.conditions.map((condition) => (
    compareValue(readField(output, condition.field), condition.operator, condition.value)
  ))
  const groupResults = group.groups.map((child) => evaluateGroup(child, output))
  const results = [...conditionResults, ...groupResults]
  if (results.length === 0) return false
  return group.join === 'and' ? results.every(Boolean) : results.some(Boolean)
}

function collectOutputFields(value: unknown, prefix = ''): string[] {
  if (!value || typeof value !== 'object') return []
  if (Array.isArray(value)) {
    const firstObject = value.find((item) => item && typeof item === 'object' && !Array.isArray(item))
    return firstObject ? collectOutputFields(firstObject, prefix ? `${prefix}[]` : '[]') : []
  }
  return Object.entries(value as Record<string, unknown>).flatMap(([key, child]) => {
    const path = prefix ? `${prefix}.${key}` : key
    if (child && typeof child === 'object') {
      return collectOutputFields(child, path)
    }
    return [path]
  })
}

function extractPlaceholders(template: RRETemplate | undefined): string[] {
  const body = template?.body || ''
  return Array.from(new Set(Array.from(body.matchAll(/\{\{\s*([A-Za-z0-9_.-]+)\s*\}\}/g)).map((m) => m[1])))
}

function resolveMappedValue(output: Record<string, unknown>, mapping: RRETemplateMapping): string {
  if (mapping.source === 'custom') return String(mapping.value ?? '')
  const path = String(mapping.field || '').trim()
  if (!path) return ''
  return formatScalar(readField(output, path))
}

function renderTemplate(
  template: RRETemplate | undefined,
  rule: RRERule | undefined,
  output: Record<string, unknown>,
): string {
  if (!template || !rule) return 'No template matched the satisfied rules.'
  const values: Record<string, string> = {
    responsibility: template.responsibility,
  }
  rule.templateMappings.forEach((mapping) => {
    if (!mapping.placeholder) return
    values[mapping.placeholder] = resolveMappedValue(output, mapping)
  })
  return template.body.replace(/\{\{\s*([A-Za-z0-9_.-]+)\s*\}\}/g, (_match, key: string) => (
    values[key] ?? ''
  ))
}

function updateGroupById(group: RREConditionGroup, groupId: string, patcher: (group: RREConditionGroup) => RREConditionGroup): RREConditionGroup {
  if (group.id === groupId) return patcher(group)
  return {
    ...group,
    groups: group.groups.map((child) => updateGroupById(child, groupId, patcher)),
  }
}

function deleteGroupById(group: RREConditionGroup, groupId: string): RREConditionGroup {
  return {
    ...group,
    groups: group.groups
      .filter((child) => child.id !== groupId)
      .map((child) => deleteGroupById(child, groupId)),
  }
}

function upsertMapping(mappings: RRETemplateMapping[], placeholder: string, patch: Partial<RRETemplateMapping>): RRETemplateMapping[] {
  const exists = mappings.some((item) => item.placeholder === placeholder)
  if (exists) return mappings.map((item) => item.placeholder === placeholder ? { ...item, ...patch } : item)
  return [...mappings, { placeholder, source: 'field', field: '', value: '', ...patch }]
}

function RuleConditionGroupEditor({
  group,
  depth,
  fieldOptions,
  onChange,
  onDelete,
}: {
  group: RREConditionGroup
  depth: number
  fieldOptions: { label: string; value: string }[]
  onChange: (group: RREConditionGroup) => void
  onDelete?: () => void
}) {
  const patchGroup = (groupId: string, patcher: (item: RREConditionGroup) => RREConditionGroup) => {
    onChange(updateGroupById(group, groupId, patcher))
  }

  return (
    <div style={{ border: '1px solid var(--app-border)', borderRadius: 8, padding: 8, background: depth ? 'var(--app-card-bg)' : 'var(--app-input-bg)' }}>
      <Space style={{ width: '100%', justifyContent: 'space-between' }} wrap>
        <Space>
          <Text style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 12 }}>{depth ? 'Nested Group' : 'Root Group'}</Text>
          <Select
            size="small"
            value={group.join}
            options={joinOptions}
            onChange={(join) => patchGroup(group.id, (item) => ({ ...item, join }))}
            style={{ width: 92 }}
          />
        </Space>
        <Space>
          <Button size="small" icon={<PlusOutlined />} onClick={() => patchGroup(group.id, (item) => ({ ...item, conditions: [...item.conditions, newCondition()] }))}>
            Condition
          </Button>
          <Button size="small" icon={<PlusOutlined />} onClick={() => patchGroup(group.id, (item) => ({ ...item, groups: [...item.groups, newGroup({ join: 'or' })] }))}>
            Group
          </Button>
          {onDelete ? <Button size="small" danger icon={<DeleteOutlined />} onClick={onDelete} /> : null}
        </Space>
      </Space>

      <Space direction="vertical" size={8} style={{ width: '100%', marginTop: 8 }}>
        {group.conditions.map((condition) => (
          <Space.Compact key={condition.id} style={{ width: '100%' }}>
            <Select
              showSearch
              optionFilterProp="label"
              value={condition.field || undefined}
              options={fieldOptions}
              onChange={(field) => patchGroup(group.id, (item) => ({
                ...item,
                conditions: item.conditions.map((current) => current.id === condition.id ? { ...current, field } : current),
              }))}
              placeholder="Output field"
              style={{ minWidth: 210 }}
            />
            <Select
              value={condition.operator}
              options={operatorOptions}
              onChange={(operator) => patchGroup(group.id, (item) => ({
                ...item,
                conditions: item.conditions.map((current) => current.id === condition.id ? { ...current, operator } : current),
              }))}
              style={{ minWidth: 142 }}
            />
            <Input
              value={condition.value}
              disabled={condition.operator === 'exists'}
              placeholder="Value"
              onChange={(event) => patchGroup(group.id, (item) => ({
                ...item,
                conditions: item.conditions.map((current) => current.id === condition.id ? { ...current, value: event.target.value } : current),
              }))}
            />
            <Button
              danger
              icon={<DeleteOutlined />}
              onClick={() => patchGroup(group.id, (item) => ({
                ...item,
                conditions: item.conditions.filter((current) => current.id !== condition.id),
              }))}
            />
          </Space.Compact>
        ))}

        {group.groups.map((child) => (
          <RuleConditionGroupEditor
            key={child.id}
            group={child}
            depth={depth + 1}
            fieldOptions={fieldOptions}
            onChange={(nextChild) => patchGroup(child.id, () => nextChild)}
            onDelete={() => onChange(deleteGroupById(group, child.id))}
          />
        ))}
      </Space>
    </div>
  )
}

export default function RREStudio(props: RREStudioProps) {
  const [rules, setRulesState] = useState<RRERule[]>(() => normalizeRulesConfig(props.rulesConfig))
  const [ruleConfigDraft, setRuleConfigDraft] = useState<RRERule | null>(null)
  const [templates, setTemplates] = useState<RRETemplate[]>(initialTemplates)
  const [templatesLoading, setTemplatesLoading] = useState(false)
  const [templateSaving, setTemplateSaving] = useState(false)
  const externalModelOutput = useMemo(() => buildModelOutputFromFields(props), [
    props.sourceFields,
    props.predictionFields,
    props.sourceRows,
    props.predictionRows,
  ])
  const externalModelOutputText = useMemo(() => JSON.stringify(externalModelOutput, null, 2), [externalModelOutput])
  const [modelOutputText, setModelOutputText] = useState(externalModelOutputText)
  const [modelOutputEdited, setModelOutputEdited] = useState(false)
  const [templateDraft, setTemplateDraft] = useState<RRETemplate>({
    id: '',
    name: '',
    responsibility: '',
    body: '',
  })
  const externalRulesSignature = useMemo(() => JSON.stringify(props.rulesConfig || null), [props.rulesConfig])

  const parsedOutput = useMemo(() => {
    try {
      const parsed = JSON.parse(modelOutputText)
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        return { row: null, error: 'Ensemble output must be a single JSON object.' }
      }
      return { row: parsed as Record<string, unknown>, error: '' }
    } catch (error) {
      return { row: null, error: error instanceof Error ? error.message : 'Invalid JSON' }
    }
  }, [modelOutputText])

  useEffect(() => {
    if (!modelOutputEdited) setModelOutputText(externalModelOutputText)
  }, [externalModelOutputText, modelOutputEdited])

  useEffect(() => {
    if (props.rulesConfig) setRulesState(normalizeRulesConfig(props.rulesConfig))
  }, [externalRulesSignature])

  useEffect(() => {
    let active = true
    const loadTemplates = async () => {
      setTemplatesLoading(true)
      try {
        const rows = await api.listRRETemplates()
        if (!active) return
        if (Array.isArray(rows) && rows.length > 0) {
          setTemplates(rows.map((row: any) => ({
            id: String(row.id || ''),
            name: String(row.name || 'Untitled Template'),
            responsibility: String(row.responsibility || ''),
            body: String(row.body || ''),
          })).filter((row: RRETemplate) => row.id))
        }
      } finally {
        if (active) setTemplatesLoading(false)
      }
    }
    void loadTemplates()
    return () => { active = false }
  }, [])

  const outputFields = useMemo(() => parsedOutput.row ? collectOutputFields(parsedOutput.row) : [], [parsedOutput.row])
  const sourceFields = outputFields.filter((field) => field.startsWith('source.'))
  const predictionFields = outputFields.filter((field) => field.startsWith('predictions.'))
  const otherOutputFields = outputFields.filter((field) => !field.startsWith('source.') && !field.startsWith('predictions.'))
  const fieldOptions = [
    ...sourceFields.map((field) => ({ label: field.replace(/^source\./, ''), value: field, group: 'Source Fields' })),
    ...predictionFields.map((field) => ({ label: field.replace(/^predictions\./, ''), value: field, group: 'Prediction Fields' })),
    ...otherOutputFields.map((field) => ({ label: field, value: field, group: 'Other Output Fields' })),
  ]
  const allTemplates = useMemo(() => {
    const byId = new Map<string, RRETemplate>()
    templates.forEach((template) => {
      if (template.id) byId.set(template.id, template)
    })
    rules.forEach((rule) => {
      const snapshot = templateFromRuleSnapshot(rule)
      if (snapshot && !byId.has(snapshot.id)) byId.set(snapshot.id, snapshot)
    })
    return Array.from(byId.values())
  }, [rules, templates])

  const templateOptions = allTemplates.map((template) => ({ label: template.name, value: template.id }))

  const enrichRulesWithTemplateSnapshots = (items: RRERule[]): RRERule[] => items.map((rule) => {
    const template = allTemplates.find((item) => item.id === rule.templateId)
    if (!template) return rule
    return {
      ...rule,
      templateName: template.name,
      templateBody: template.body,
      templateResponsibility: template.responsibility,
    }
  })

  const commitRules = (nextRules: RRERule[] | ((current: RRERule[]) => RRERule[])) => {
    setRulesState((current) => {
      const next = enrichRulesWithTemplateSnapshots(typeof nextRules === 'function' ? nextRules(current) : nextRules)
      props.onRulesConfigChange?.(next)
      return next
    })
  }

  const validationErrors = useMemo(() => {
    const errors: string[] = []
    const validateGroup = (ruleName: string, group: RREConditionGroup) => {
      if (group.conditions.length === 0 && group.groups.length === 0) errors.push(`${ruleName}: condition group is empty`)
      group.conditions.forEach((condition) => {
        if (!condition.field.trim()) errors.push(`${ruleName}: condition field is required`)
        if (condition.operator !== 'exists' && !String(condition.value).trim()) errors.push(`${ruleName}: comparison value is required for ${condition.field || 'field'}`)
      })
      group.groups.forEach((child) => validateGroup(ruleName, child))
    }

    rules.forEach((rule) => {
      if (!rule.enabled) return
      const ruleName = rule.name || rule.id
      if (!rule.name.trim()) errors.push(`${rule.id}: rule name is required`)
      if (!allTemplates.some((template) => template.id === rule.templateId)) errors.push(`${ruleName}: template picker is required`)
      validateGroup(ruleName, rule.rootGroup)
    })
    if (parsedOutput.error) errors.push(parsedOutput.error)
    return errors
  }, [parsedOutput.error, rules, allTemplates])

  const evaluation = useMemo(() => {
    if (!parsedOutput.row || validationErrors.length > 0) {
      return { matched: [] as RRERule[], template: undefined as RRETemplate | undefined, explanation: '' }
    }
    const matched = rules
      .filter((rule) => rule.enabled)
      .filter((rule) => evaluateGroup(rule.rootGroup, parsedOutput.row!))
      .sort((a, b) => a.priority - b.priority)
    const selectedRule = matched[0]
    const template = allTemplates.find((item) => item.id === selectedRule?.templateId)
    return {
      matched,
      template,
      explanation: renderTemplate(template, selectedRule, parsedOutput.row),
    }
  }, [parsedOutput.row, rules, allTemplates, validationErrors.length])

  const recommendationOutput = useMemo(() => {
    const out: Record<string, string> = {}
    rules.forEach((rule) => {
      const column = ruleOutputColumn(rule)
      if (!column) return
      const matched = evaluation.matched.some((item) => item.id === rule.id)
      const template = allTemplates.find((item) => item.id === rule.templateId)
      out[column] = matched && parsedOutput.row ? renderTemplate(template, rule, parsedOutput.row) : ''
    })
    return out
  }, [evaluation.matched, parsedOutput.row, rules, allTemplates])

  const addRule = () => {
    const id = `rule-${Date.now()}`
    const nextRule: RRERule = {
        id,
        name: 'New Rule',
        enabled: true,
        priority: rules.length + 1,
        rootGroup: newGroup({ join: 'and', conditions: [newCondition({ field: predictionFields[0] || outputFields[0] || 'predictions.ensemble_prediction', operator: 'exists', value: '' })] }),
        templateId: templates[0]?.id || '',
        templateMappings: [],
    }
    commitRules((current) => [...current, nextRule])
    setRuleConfigDraft(cloneRule(nextRule))
  }

  const updateRule = (id: string, patch: Partial<RRERule>) => {
    commitRules((current) => current.map((rule) => rule.id === id ? { ...rule, ...patch } : rule))
  }

  const deleteRule = (id: string) => {
    commitRules((current) => current.filter((item) => item.id !== id))
    setRuleConfigDraft((current) => current?.id === id ? null : current)
  }

  const saveRuleConfigDraft = () => {
    if (!ruleConfigDraft) return
    const selectedTemplate = allTemplates.find((item) => item.id === ruleConfigDraft.templateId)
    const nextRule = {
      ...ruleConfigDraft,
      templateName: selectedTemplate?.name || ruleConfigDraft.templateName || '',
      templateBody: selectedTemplate?.body || ruleConfigDraft.templateBody || '',
      templateResponsibility: selectedTemplate?.responsibility || ruleConfigDraft.templateResponsibility || '',
    }
    commitRules((current) => current.map((rule) => rule.id === nextRule.id ? cloneRule(nextRule) : rule))
    setRuleConfigDraft(null)
  }

  const draftTemplate = allTemplates.find((item) => item.id === ruleConfigDraft?.templateId)
  const draftPlaceholders = extractPlaceholders(draftTemplate).filter((item) => item !== 'responsibility')

  const saveTemplate = async () => {
    setTemplateSaving(true)
    try {
      const clean = {
        ...templateDraft,
        id: templateDraft.id.trim(),
        name: templateDraft.name.trim() || 'Untitled Template',
        responsibility: templateDraft.responsibility.trim() || 'Unassigned',
      }
      const saved = await api.saveRRETemplate(clean)
      const normalized = {
        id: String(saved.id || clean.id || `rre_tpl_${Date.now()}`),
        name: String(saved.name || clean.name),
        responsibility: String(saved.responsibility || clean.responsibility),
        body: String(saved.body || clean.body || ''),
      }
      setTemplates((current) => {
      const exists = current.some((template) => template.id === clean.id)
        return exists ? current.map((template) => template.id === clean.id ? normalized : template) : [...current, normalized]
      })
      setTemplateDraft(normalized)
      notification.success({ message: 'RRE template saved', placement: 'bottomRight' })
    } catch (error: any) {
      notification.error({
        message: 'Failed to save RRE template',
        description: String(error?.message || error),
        placement: 'bottomRight',
      })
    } finally {
      setTemplateSaving(false)
    }
  }

  const deleteTemplate = async (id: string) => {
    await api.deleteRRETemplate(id)
    setTemplates((current) => current.filter((item) => item.id !== id))
    notification.success({ message: 'RRE template deleted', placement: 'bottomRight' })
  }

  return (
    <Card
      style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8 }}
      bodyStyle={{ padding: 16 }}
    >
      <Tabs
        defaultActiveKey="engine"
        items={[
          {
            key: 'engine',
            label: 'Signal Engine',
            children: (
              <Space direction="vertical" size={14} style={{ width: '100%' }}>
                <div>
                  <Title level={5} style={{ color: 'var(--app-text)', margin: 0 }}>Recommendate Rule Engine</Title>
                  <Text style={{ color: 'var(--app-text-subtle)' }}>
                    Consumes final ensemble output, evaluates nested AND/OR rules, and applies the selected template mapping.
                  </Text>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'minmax(320px, 0.85fr) minmax(520px, 1.4fr)', gap: 14 }}>
                  <Card size="small" title="Ensemble Model Output" style={{ background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)' }}>
                    <Input.TextArea
                      value={modelOutputText}
                      onChange={(event) => {
                        setModelOutputEdited(true)
                        setModelOutputText(event.target.value)
                      }}
                      rows={13}
                      style={{ fontFamily: 'monospace', background: 'var(--app-card-bg)', color: 'var(--app-text)' }}
                    />
                    <div style={{ marginTop: 10 }}>
                      <Text style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 12 }}>Output Fields</Text>
                      <div style={{ marginTop: 6, maxHeight: 180, overflowY: 'auto', display: 'grid', gap: 8 }}>
                        {[
                          ['Source Fields', sourceFields],
                          ['Prediction Fields', predictionFields],
                          ['Other Output Fields', otherOutputFields],
                        ].map(([label, fields]) => (
                          <div key={String(label)}>
                            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>{String(label)}</Text>
                            <div style={{ marginTop: 4 }}>
                              <Space wrap size={6}>
                                {(fields as string[]).length ? (fields as string[]).map((field) => (
                                  <Tag key={field} style={{ marginInlineEnd: 0 }}>{field}</Tag>
                                )) : <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>None</Text>}
                              </Space>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </Card>

                  <Card
                    size="small"
                    title="Rule Configuration Management"
                    extra={<Button size="small" icon={<PlusOutlined />} onClick={addRule}>Rule</Button>}
                    style={{ background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)' }}
                  >
                    <Table
                      size="small"
                      rowKey="id"
                      dataSource={rules}
                      pagination={false}
                      columns={[
                        {
                          title: 'Enabled',
                          dataIndex: 'enabled',
                          width: 90,
                          render: (_value, rule) => (
                            <Switch size="small" checked={rule.enabled} onChange={(enabled) => updateRule(rule.id, { enabled })} />
                          ),
                        },
                        {
                          title: 'Rule Name',
                          dataIndex: 'name',
                          ellipsis: true,
                          render: (value, rule) => (
                            <Space>
                              <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>{String(value || rule.id)}</Text>
                              {evaluation.matched.some((item) => item.id === rule.id) ? <Tag color="green">matched</Tag> : null}
                            </Space>
                          ),
                        },
                        { title: 'Priority', dataIndex: 'priority', width: 90 },
                        {
                          title: 'Template',
                          dataIndex: 'templateId',
                          width: 210,
                          ellipsis: true,
                          render: (templateId) => allTemplates.find((item) => item.id === templateId)?.name || 'Not selected',
                        },
                        {
                          title: 'Conditions',
                          width: 110,
                          render: (_value, rule) => countGroupConditions(rule.rootGroup),
                        },
                        {
                          title: 'Mappings',
                          width: 100,
                          render: (_value, rule) => rule.templateMappings.length,
                        },
                        {
                          title: 'Configuration',
                          width: 190,
                          render: (_value, rule) => (
                            <Space>
                              <Button size="small" onClick={() => setRuleConfigDraft(cloneRule(rule))}>Configure</Button>
                              <Button
                                size="small"
                                danger
                                icon={<DeleteOutlined />}
                                onClick={() => deleteRule(rule.id)}
                              />
                            </Space>
                          ),
                        },
                      ]}
                      locale={{ emptyText: 'No rules configured.' }}
                    />
                  </Card>
                </div>

                {validationErrors.length > 0 ? (
                  <Alert type="warning" showIcon message="Conditional validation failed" description={validationErrors.join(' | ')} />
                ) : (
                  <Alert type="success" showIcon message="Conditional validation passed" description={`${evaluation.matched.length} rule(s) satisfied.`} />
                )}

                <Card size="small" title="Selected Template Output" style={{ background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)' }}>
                  <Space direction="vertical" size={8} style={{ width: '100%' }}>
                    <Space wrap>
                      <Tag icon={<SafetyCertificateOutlined />} color={evaluation.template ? 'blue' : 'default'}>
                        {evaluation.template?.name || 'No Template'}
                      </Tag>
                      {evaluation.template?.responsibility && <Tag icon={<FileTextOutlined />}>{evaluation.template.responsibility}</Tag>}
                    </Space>
                    <Text style={{ color: 'var(--app-text)' }}>{evaluation.explanation || 'Fix validation errors to generate final output.'}</Text>
                  </Space>
                </Card>

                <Card size="small" title="Rule Wise Recommendation Output Columns" style={{ background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)' }}>
                  <Input.TextArea
                    readOnly
                    rows={7}
                    value={JSON.stringify(recommendationOutput, null, 2)}
                    style={{ fontFamily: 'monospace', background: 'var(--app-card-bg)', color: 'var(--app-text)' }}
                  />
                </Card>
              </Space>
            ),
          },
          {
            key: 'templates',
            label: 'Template Responsibility',
            children: (
              <div style={{ display: 'grid', gridTemplateColumns: 'minmax(300px, 420px) 1fr', gap: 14 }}>
                <Card size="small" title="Create / Manage Template" style={{ background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)' }}>
                  <Form layout="vertical">
                    <Form.Item label="Template Id">
                      <Input value={templateDraft.id} onChange={(event) => setTemplateDraft((current) => ({ ...current, id: event.target.value }))} />
                    </Form.Item>
                    <Form.Item label="Template Name">
                      <Input value={templateDraft.name} onChange={(event) => setTemplateDraft((current) => ({ ...current, name: event.target.value }))} />
                    </Form.Item>
                    <Form.Item label="Responsibility">
                      <Input value={templateDraft.responsibility} onChange={(event) => setTemplateDraft((current) => ({ ...current, responsibility: event.target.value }))} />
                    </Form.Item>
                    <Form.Item label="Template Body">
                      <Input.TextArea
                        rows={6}
                        value={templateDraft.body}
                        onChange={(event) => setTemplateDraft((current) => ({ ...current, body: event.target.value }))}
                        placeholder="Use {{prediction}}, {{riskScore}}, {{confidenceValue}}, {{review_reason}}, {{responsibility}}"
                      />
                    </Form.Item>
                    <Button type="primary" loading={templateSaving} onClick={() => void saveTemplate()}>Save Template</Button>
                  </Form>
                </Card>

                <Card size="small" title="Template Responsibility Catalog" style={{ background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)' }}>
                  <Table
                    size="small"
                    rowKey="id"
                    pagination={false}
                    loading={templatesLoading}
                    dataSource={allTemplates}
                    columns={[
                      { title: 'Template', dataIndex: 'name' },
                      { title: 'Responsibility', dataIndex: 'responsibility' },
                      { title: 'Placeholders', render: (_, template) => extractPlaceholders(template).map((item) => <Tag key={item}>{`{{${item}}}`}</Tag>) },
                      { title: 'Body', dataIndex: 'body', ellipsis: true },
                      {
                        title: '',
                        width: 120,
                        render: (_, template) => (
                          <Space>
                            <Button size="small" onClick={() => setTemplateDraft(template)}>Edit</Button>
                            <Button
                              size="small"
                              danger
                              icon={<DeleteOutlined />}
                              onClick={() => void deleteTemplate(template.id)}
                            />
                          </Space>
                        ),
                      },
                    ]}
                  />
                </Card>
              </div>
            ),
          },
        ]}
      />
      <Modal
        open={Boolean(ruleConfigDraft)}
        title="Configure Rule"
        width="100vw"
        onCancel={() => setRuleConfigDraft(null)}
        onOk={saveRuleConfigDraft}
        okText="Save Rule"
        centered={false}
        style={{ top: 0, maxWidth: '100vw', paddingBottom: 0 }}
        styles={{
          content: {
            height: '100vh',
            borderRadius: 0,
            display: 'flex',
            flexDirection: 'column',
          },
          body: {
            flex: 1,
            overflowY: 'auto',
          },
        }}
      >
        {ruleConfigDraft ? (
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            <Space wrap>
              <Switch
                checked={ruleConfigDraft.enabled}
                onChange={(enabled) => setRuleConfigDraft((current) => current ? { ...current, enabled } : current)}
              />
              <Input
                value={ruleConfigDraft.name}
                onChange={(event) => setRuleConfigDraft((current) => current ? { ...current, name: event.target.value } : current)}
                placeholder="Rule name"
                style={{ width: 260 }}
              />
              <Text style={{ color: 'var(--app-text-subtle)' }}>Priority</Text>
              <InputNumber
                min={1}
                value={ruleConfigDraft.priority}
                onChange={(priority) => setRuleConfigDraft((current) => current ? { ...current, priority: Number(priority || 1) } : current)}
                style={{ width: 90 }}
              />
            </Space>

            <RuleConditionGroupEditor
              group={ruleConfigDraft.rootGroup}
              depth={0}
              fieldOptions={fieldOptions}
              onChange={(rootGroup) => setRuleConfigDraft((current) => current ? { ...current, rootGroup } : current)}
            />

            <div style={{ borderTop: '1px solid var(--app-border)', paddingTop: 10 }}>
              <Space style={{ width: '100%', justifyContent: 'space-between' }} wrap>
                <Space>
                  <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>End Rule Template Picker</Text>
                  <Select
                    value={ruleConfigDraft.templateId || undefined}
                    options={templateOptions}
                    onChange={(templateId) => {
                      const selectedTemplate = allTemplates.find((item) => item.id === templateId)
                      setRuleConfigDraft((current) => current ? {
                        ...current,
                        templateId,
                        templateName: selectedTemplate?.name || '',
                        templateBody: selectedTemplate?.body || '',
                        templateResponsibility: selectedTemplate?.responsibility || '',
                      } : current)
                    }}
                    placeholder="Pick template"
                    style={{ width: 280 }}
                  />
                </Space>
                {draftTemplate?.responsibility && <Tag icon={<FileTextOutlined />}>{draftTemplate.responsibility}</Tag>}
              </Space>

              <Table
                size="small"
                rowKey="placeholder"
                pagination={false}
                style={{ marginTop: 8 }}
                dataSource={draftPlaceholders.map((placeholder) => ({
                  ...(ruleConfigDraft.templateMappings.find((item) => item.placeholder === placeholder) || {
                    source: 'field',
                    field: '',
                    value: '',
                  }),
                  placeholder,
                }))}
                columns={[
                  { title: 'Template Placeholder', dataIndex: 'placeholder', width: 220, render: (value) => <Text code>{`{{${value}}}`}</Text> },
                  {
                    title: 'Value Source',
                    dataIndex: 'source',
                    width: 160,
                    render: (_value, row) => (
                      <Select
                        value={(row.source || 'field') as TemplateMappingSource}
                        options={[
                          { label: 'Field', value: 'field' },
                          { label: 'Custom Value', value: 'custom' },
                        ]}
                        onChange={(source: TemplateMappingSource) => setRuleConfigDraft((current) => current ? {
                          ...current,
                          templateMappings: upsertMapping(current.templateMappings, row.placeholder, { source }),
                        } : current)}
                        style={{ width: '100%' }}
                      />
                    ),
                  },
                  {
                    title: 'Map From Ensemble Field / Custom Value',
                    render: (_value, row) => row.source === 'custom' ? (
                      <Input
                        value={row.value || ''}
                        onChange={(event) => setRuleConfigDraft((current) => current ? {
                          ...current,
                          templateMappings: upsertMapping(current.templateMappings, row.placeholder, {
                            source: 'custom',
                            value: event.target.value,
                          }),
                        } : current)}
                        placeholder="Enter custom value"
                      />
                    ) : (
                      <Select
                        allowClear
                        showSearch
                        optionFilterProp="label"
                        value={row.field || undefined}
                        options={fieldOptions}
                        onChange={(field) => setRuleConfigDraft((current) => current ? {
                          ...current,
                          templateMappings: upsertMapping(current.templateMappings, row.placeholder, {
                            source: 'field',
                            field: String(field || ''),
                          }),
                        } : current)}
                        placeholder="Select output field"
                        style={{ width: '100%' }}
                      />
                    ),
                  },
                ]}
                locale={{ emptyText: 'Selected template has no custom field placeholders.' }}
              />
            </div>
          </Space>
        ) : null}
      </Modal>
    </Card>
  )
}
