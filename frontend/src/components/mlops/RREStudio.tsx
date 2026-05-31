import { useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Checkbox,
  Collapse,
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
  CopyOutlined,
  DeleteOutlined,
  FileTextOutlined,
  PlusOutlined,
  SafetyCertificateOutlined,
  SettingOutlined,
} from '@ant-design/icons'
import api from '../../api/client'

const { Text, Title } = Typography

type RuleOperator = '>' | '>=' | '<' | '<=' | '=' | '!=' | 'contains' | 'exists'
type ConditionJoin = 'and' | 'or'
type TemplateMappingSource = 'field' | 'custom' | 'template'
type RRESeverity = 'HIGH' | 'MEDIUM' | 'LOW'
type RREImpactRole = 'driver' | 'outcome' | 'context' | 'identifier'

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

interface RREFeatureDictionaryEntry {
  field: string
  business_name: string
  meaning: string
  unit: string
  direction: string
  impact_role: RREImpactRole
  impact_weight: number
  auto_signal_enabled: boolean
  warning_threshold: string
  critical_threshold: string
  default_recommendation: string
}

interface RRESignalConfig {
  enabled: boolean
  smartWordingEnabled: boolean
  feature: string
  severity: RRESeverity
  valueField: string
  peerValueField: string
  impactSource: TemplateMappingSource
  impactField: string
  impactValue: string
  recommendation: string
  jsonFields: string[]
}

interface RRERule {
  id: string
  name: string
  enabled: boolean
  smartWordingEnabled: boolean
  smartWordingMaxChars: number
  priority: number
  rootGroup: RREConditionGroup
  templateId: string
  templateName?: string
  templateBody?: string
  templateResponsibility?: string
  signalConfig?: RRESignalConfig
  templateMappings: RRETemplateMapping[]
}

interface RRETemplate {
  id: string
  name: string
  responsibility: string
  body: string
}

interface RREClusterConfig {
  id: string
  name: string
  enabled: boolean
  features: string[]
  observation: string
  recommendation: string
  priority: number
  summary_max_chars: number
  feature_filter: 'all_selected' | 'breached_only'
  rank_by: 'balanced_score' | 'intelligent_impact' | 'configured_order' | 'severity' | 'breach_percent' | 'absolute_breach' | 'value'
  feature_limit: number
}

interface RREStudioProps {
  sourceFields?: string[]
  predictionFields?: string[]
  sourceRows?: Array<Record<string, unknown>>
  predictionRows?: Array<Record<string, unknown>>
  rulesConfig?: RRERule[]
  onRulesConfigChange?: (rules: RRERule[]) => void
  featureDictionary?: RREFeatureDictionaryEntry[]
  onFeatureDictionaryChange?: (dictionary: RREFeatureDictionaryEntry[]) => void
  autoSignalJsonFields?: string[]
  onAutoSignalJsonFieldsChange?: (fields: string[]) => void
  clusterConfig?: RREClusterConfig[]
  onClusterConfigChange?: (clusters: RREClusterConfig[]) => void
  clusterJsonFields?: string[]
  onClusterJsonFieldsChange?: (fields: string[]) => void
  includeAutoClusterRecommendation?: boolean
  onIncludeAutoClusterRecommendationChange?: (enabled: boolean) => void
  clusterTransformersEnabled?: boolean
  onClusterTransformersEnabledChange?: (enabled: boolean) => void
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

const severityOptions: { label: string; value: RRESeverity }[] = [
  { label: 'HIGH', value: 'HIGH' },
  { label: 'MEDIUM', value: 'MEDIUM' },
  { label: 'LOW', value: 'LOW' },
]

const directionOptions = [
  { label: 'Higher is risky', value: 'higher_is_risky' },
  { label: 'Lower is risky', value: 'lower_is_risky' },
  { label: 'Deviation is risky', value: 'deviation_is_risky' },
  { label: 'Informational', value: 'informational' },
]

const impactRoleOptions: { label: string; value: RREImpactRole }[] = [
  { label: 'Driver', value: 'driver' },
  { label: 'Outcome', value: 'outcome' },
  { label: 'Context', value: 'context' },
  { label: 'Identifier', value: 'identifier' },
]

const signalJsonFieldOptions = [
  { label: 'Summary', value: 'summary' },
  { label: 'Observation', value: 'observation' },
  { label: 'Recommendation', value: 'recommendation' },
  { label: 'Default Recommendation', value: 'default_recommendation' },
  { label: 'Risk Band', value: 'risk_band' },
  { label: 'Feature', value: 'feature' },
  { label: 'Business Name', value: 'business_name' },
  { label: 'Meaning', value: 'meaning' },
  { label: 'Severity', value: 'severity' },
  { label: 'Value', value: 'value' },
  { label: 'Peer Value', value: 'peer_value' },
  { label: 'Impact', value: 'impact' },
  { label: 'Unit', value: 'unit' },
  { label: 'Direction', value: 'direction' },
]

const defaultSignalJsonFields = ['summary', 'observation', 'recommendation', 'default_recommendation', 'risk_band', 'feature', 'severity', 'value', 'peer_value', 'impact']
const defaultAutoSignalJsonFields = ['business_name', 'severity', 'value', 'threshold', 'threshold_type', 'observation', 'recommendation']
const defaultClusterJsonFields = ['cluster', 'severity', 'features', 'primary_driver', 'secondary_drivers', 'impacted_outcomes', 'context_fields', 'observation', 'recommendation', 'evidence']
const defaultClusterSummaryMaxChars = 240
const clusterJsonFieldOptions = [
  { label: 'Cluster Name', value: 'cluster' },
  { label: 'Severity', value: 'severity' },
  { label: 'Selected Features', value: 'features' },
  { label: 'Primary Driver', value: 'primary_driver' },
  { label: 'Secondary Drivers', value: 'secondary_drivers' },
  { label: 'Impacted Outcomes', value: 'impacted_outcomes' },
  { label: 'Context Fields', value: 'context_fields' },
  { label: 'Observation Sentence', value: 'observation' },
  { label: 'Recommendation Sentence', value: 'recommendation' },
  { label: 'Evidence Details', value: 'evidence' },
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
const defaultRuleSmartWordingMaxChars = 180
const minRuleSmartWordingMaxChars = 40
const maxRuleSmartWordingMaxChars = 2000

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
    source: item?.source === 'custom' || item?.source === 'template' ? item.source : 'field',
    field: String(item?.field || ''),
    value: String(item?.value ?? ''),
  })).filter((item) => item.placeholder)
}

function normalizeSignalConfig(raw: any): RRESignalConfig {
  const jsonFields = Array.isArray(raw?.jsonFields || raw?.json_fields)
    ? (raw?.jsonFields || raw?.json_fields).map((item: unknown) => String(item || '').trim()).filter(Boolean)
    : defaultSignalJsonFields
  return {
    enabled: raw?.enabled !== false,
    smartWordingEnabled: Boolean(raw?.smartWordingEnabled || raw?.smart_wording_enabled || false),
    feature: String(raw?.feature || ''),
    severity: ['HIGH', 'MEDIUM', 'LOW'].includes(String(raw?.severity || '')) ? raw.severity : 'MEDIUM',
    valueField: String(raw?.valueField || raw?.value_field || ''),
    peerValueField: String(raw?.peerValueField || raw?.peer_value_field || ''),
    impactSource: raw?.impactSource === 'custom' || raw?.impact_source === 'custom' ? 'custom' : 'field',
    impactField: String(raw?.impactField || raw?.impact_field || ''),
    impactValue: String(raw?.impactValue || raw?.impact_value || ''),
    recommendation: String(raw?.recommendation || ''),
    jsonFields,
  }
}

function normalizeRuleSmartWordingMaxChars(raw: unknown): number {
  const value = Math.trunc(Number(raw))
  if (!Number.isFinite(value)) return defaultRuleSmartWordingMaxChars
  return Math.max(minRuleSmartWordingMaxChars, Math.min(maxRuleSmartWordingMaxChars, value))
}

function ruleSmartWordingMaxChars(rule: RRERule | undefined): number {
  return normalizeRuleSmartWordingMaxChars(rule?.smartWordingMaxChars)
}

function compactRuleWording(text: string, maxChars = 240): string {
  const limit = normalizeRuleSmartWordingMaxChars(maxChars)
  const cleaned = String(text || '').replace(/\s+/g, ' ').trim()
  if (cleaned.length <= limit) return cleaned
  const meaningMatch = cleaned.match(/^(.*?\.)\s+This indicates\s+(.+?)\.?$/i)
  if (meaningMatch) {
    const summary = `${meaningMatch[1].trim()} Indicates ${meaningMatch[2].trim().replace(/\.$/, '')}.`
    if (summary.length <= limit) return summary
  }
  const first = cleaned.split(/(?<=[.!?])\s+/)[0] || cleaned
  if (first.length <= limit) return first
  const suffix = '.'
  const maxBodyLength = Math.max(1, limit - suffix.length)
  const words = first.split(/\s+/).filter(Boolean)
  let body = ''
  for (const word of words) {
    const next = body ? `${body} ${word}` : word
    if (next.length > maxBodyLength) break
    body = next
  }
  if (!body) body = first.slice(0, maxBodyLength).trimEnd()
  return `${body}${suffix}`
}

function normalizeFeatureDictionary(raw: unknown): RREFeatureDictionaryEntry[] {
  if (!Array.isArray(raw)) return []
  return raw.map((item: any) => ({
    field: String(item?.field || ''),
    business_name: String(item?.business_name || item?.businessName || ''),
    meaning: String(item?.meaning || ''),
    unit: String(item?.unit || ''),
    direction: String(item?.direction || 'higher_is_risky'),
    impact_role: ['driver', 'outcome', 'context', 'identifier'].includes(String(item?.impact_role || item?.impactRole || '').toLowerCase())
      ? String(item?.impact_role || item?.impactRole).toLowerCase() as RREImpactRole
      : inferImpactRole(String(item?.field || '')),
    impact_weight: Math.max(0, Math.min(10, Number(item?.impact_weight ?? item?.impactWeight ?? inferImpactWeight(String(item?.field || ''))) || 0)),
    auto_signal_enabled: Boolean(item?.auto_signal_enabled || item?.autoSignalEnabled || false),
    warning_threshold: String(item?.warning_threshold || item?.warningThreshold || ''),
    critical_threshold: String(item?.critical_threshold || item?.criticalThreshold || ''),
    default_recommendation: String(item?.default_recommendation || item?.defaultRecommendation || ''),
  })).filter((item) => item.field)
}

function inferImpactRole(field: string): RREImpactRole {
  const key = field.toLowerCase()
  if (key.includes('account') || key.includes('customer') || key.includes('agent') || key.includes('entity') || key.endsWith('_id') || key.includes('token')) return 'identifier'
  if (key.includes('total_') || key.includes('overall') || key.includes('aggregate')) return 'outcome'
  if (key.includes('segment') || key.includes('branch') || key.includes('region') || key.includes('category')) return 'context'
  return 'driver'
}

function inferImpactWeight(field: string): number {
  const key = field.toLowerCase()
  if (inferImpactRole(field) === 'identifier') return 0
  if (key.includes('amount') || key.includes('risk') || key.includes('score') || key.includes('prediction')) return 8
  if (key.includes('count') || key.includes('rate') || key.includes('repeat') || key.includes('velocity')) return 7
  if (inferImpactRole(field) === 'outcome') return 6
  if (inferImpactRole(field) === 'context') return 3
  return 5
}

function titleCase(value: string): string {
  return value
    .replace(/^(source|predictions)\./, '')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_\-.]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase())
}

function inferFeatureMetadata(field: string): RREFeatureDictionaryEntry {
  const key = field.toLowerCase()
  let meaning = 'possible business exception or behavior requiring review'
  let unit = ''
  let direction = 'higher_is_risky'
  let defaultRecommendation = `Review ${titleCase(field).toLowerCase()} and validate supporting evidence.`

  if (key.includes('ratio') || key.includes('rate') || key.includes('percent') || key.includes('success')) unit = '%'
  if (key.includes('count')) unit = 'count'
  if (key.includes('amount') || key.includes('amt')) unit = 'amount'
  if (key.includes('score')) unit = 'score'
  if (key.includes('velocity')) unit = 'txn/min'

  if (key.includes('fail')) {
    meaning = 'possible authentication issue, retry pattern, or process friction'
    defaultRecommendation = 'Review failed transactions and validate the authentication process.'
  } else if (key.includes('location') || key.includes('geo') || key.includes('gps')) {
    meaning = 'possible location inconsistency, GPS mismatch, shared device usage, or field misuse'
    defaultRecommendation = 'Verify device, GPS, and agent operating location.'
  } else if (key.includes('velocity') || key.includes('burst')) {
    meaning = 'possible bulk, scripted, or abnormal transaction behavior'
    defaultRecommendation = 'Monitor burst transactions and check for scripted behavior.'
  } else if (key.includes('repeat') || key.includes('customer_concentration')) {
    meaning = 'possible customer concentration, repeated customer usage, or limited customer dependency'
    defaultRecommendation = 'Review repeated customer usage and account concentration.'
  } else if (key.includes('risk')) {
    meaning = 'elevated model risk signal requiring operational review'
    defaultRecommendation = 'Review high-risk cases and validate the rule evidence.'
  } else if (key.includes('confidence')) {
    meaning = 'model certainty signal used to support decision confidence'
    direction = 'informational'
    defaultRecommendation = 'Use confidence level with rule evidence before taking action.'
  } else if (key.includes('drift')) {
    meaning = 'possible model or data distribution drift'
    defaultRecommendation = 'Review recent data distribution and model monitoring signals.'
  }

  return {
    field,
    business_name: titleCase(field),
    meaning,
    unit,
    direction,
    impact_role: inferImpactRole(field),
    impact_weight: inferImpactWeight(field),
    auto_signal_enabled: false,
    warning_threshold: '',
    critical_threshold: '',
    default_recommendation: defaultRecommendation,
  }
}

function parseDictionaryImport(text: string): RREFeatureDictionaryEntry[] {
  const trimmed = text.trim()
  if (!trimmed) return []
  try {
    const parsed = JSON.parse(trimmed)
    if (Array.isArray(parsed)) return normalizeFeatureDictionary(parsed)
    if (parsed && typeof parsed === 'object') {
      return normalizeFeatureDictionary(Object.entries(parsed).map(([field, meta]) => ({ field, ...(meta as Record<string, unknown>) })))
    }
  } catch {
    // Fall through to CSV/TSV parsing.
  }

  const lines = trimmed.split(/\r?\n/).map((line) => line.trim()).filter(Boolean)
  if (lines.length < 2) return []
  const delimiter = lines[0].includes('\t') ? '\t' : ','
  const headers = lines[0].split(delimiter).map((item) => item.trim())
  return normalizeFeatureDictionary(lines.slice(1).map((line) => {
    const cells = line.split(delimiter).map((item) => item.trim())
    return headers.reduce<Record<string, string>>((acc, header, index) => {
      acc[header] = cells[index] || ''
      return acc
    }, {})
  }))
}

function normalizeRulesConfig(raw: unknown): RRERule[] {
  if (!Array.isArray(raw)) return initialRules
  if (raw.length === 0) return []
  return raw.map((item: any, index) => ({
    id: String(item?.id || `rule-${Date.now()}-${index}`),
    name: String(item?.name || `Rule ${index + 1}`),
    enabled: item?.enabled !== false,
    smartWordingEnabled: Boolean(item?.smartWordingEnabled || item?.smart_wording_enabled || item?.signalConfig?.smartWordingEnabled || item?.signal_config?.smart_wording_enabled || false),
    smartWordingMaxChars: normalizeRuleSmartWordingMaxChars(item?.smartWordingMaxChars ?? item?.smart_wording_max_chars ?? item?.smartWordingChars ?? item?.smart_wording_chars),
    priority: Number.isFinite(Number(item?.priority)) ? Number(item.priority) : index + 1,
    rootGroup: normalizeGroup(item?.rootGroup),
    templateId: String(item?.templateId || ''),
    templateName: String(item?.templateName || item?.template_name || ''),
    templateBody: String(item?.templateBody || item?.template_body || ''),
    templateResponsibility: String(item?.templateResponsibility || item?.template_responsibility || ''),
    signalConfig: item?.signalConfig || item?.signal_config ? normalizeSignalConfig(item?.signalConfig || item?.signal_config) : undefined,
    templateMappings: normalizeMappings(item?.templateMappings),
  }))
}

function normalizeClusterConfig(raw: unknown): RREClusterConfig[] {
  if (!Array.isArray(raw)) return []
  return raw.map((item: any, index) => ({
    id: String(item?.id || `cluster-${index + 1}`),
    name: String(item?.name || item?.cluster || `Cluster ${index + 1}`),
    enabled: item?.enabled !== false,
    features: Array.isArray(item?.features) ? item.features.map((field: unknown) => String(field || '').trim()).filter(Boolean) : [],
    observation: String(item?.observation || ''),
    recommendation: String(item?.recommendation || ''),
    priority: Number(item?.priority || index + 1),
    summary_max_chars: Math.max(100, Math.min(2000, Number(item?.summary_max_chars || item?.summaryMaxChars || defaultClusterSummaryMaxChars))),
    feature_filter: (String(item?.feature_filter || item?.featureFilter || 'breached_only') === 'all_selected' ? 'all_selected' : 'breached_only') as RREClusterConfig['feature_filter'],
    rank_by: ['balanced_score', 'intelligent_impact', 'configured_order', 'severity', 'breach_percent', 'absolute_breach', 'value'].includes(String(item?.rank_by || item?.rankBy || 'balanced_score'))
      ? String(item?.rank_by || item?.rankBy || 'balanced_score') as RREClusterConfig['rank_by']
      : 'balanced_score',
    feature_limit: Math.max(0, Math.min(100, Number(item?.feature_limit ?? item?.featureLimit ?? 3) || 0)),
  })).filter((item) => item.name)
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

function signalOutputColumn(rule: RRERule): string {
  const base = ruleOutputColumn(rule)
  return base ? `${base}_Generate_XAI_signal` : ''
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

function severityWord(severity: string): string {
  if (severity === 'HIGH') return 'significantly'
  if (severity === 'LOW') return 'slightly'
  return 'moderately'
}

function riskBand(score: unknown): string {
  const n = Number(score)
  if (!Number.isFinite(n)) return ''
  if (n >= 80 || (n <= 1 && n >= 0.8)) return 'HIGH'
  if (n >= 60 || (n <= 1 && n >= 0.6)) return 'MEDIUM'
  return 'LOW'
}

function readField(row: Record<string, unknown>, path: string): unknown {
  const cleanPath = String(path || '').trim()
  if (!cleanPath) return undefined
  const nested = cleanPath.split('.').reduce<unknown>((current, key) => {
    if (!current || typeof current !== 'object' || Array.isArray(current)) return undefined
    return (current as Record<string, unknown>)[key]
  }, row)
  if (nested !== undefined) return nested
  if (Object.prototype.hasOwnProperty.call(row, cleanPath)) return row[cleanPath]
  for (const prefix of ['source.', 'predictions.']) {
    if (cleanPath.startsWith(prefix)) {
      const flatPath = cleanPath.slice(prefix.length)
      if (Object.prototype.hasOwnProperty.call(row, flatPath)) return row[flatPath]
    }
  }
  return undefined
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

function decodeJsonStrings(value: unknown): unknown {
  if (typeof value === 'string') {
    const text = value.trim()
    if (
      (text.startsWith('{') && text.endsWith('}'))
      || (text.startsWith('[') && text.endsWith(']'))
    ) {
      try {
        return decodeJsonStrings(JSON.parse(text))
      } catch {
        return value
      }
    }
    return value
  }
  if (Array.isArray(value)) return value.map(decodeJsonStrings)
  if (value && typeof value === 'object') {
    return Object.entries(value as Record<string, unknown>).reduce<Record<string, unknown>>((acc, [key, item]) => {
      acc[key] = decodeJsonStrings(item)
      return acc
    }, {})
  }
  return value
}

function JsonValue({ value, depth, expanded }: { value: unknown; depth: number; expanded: boolean }) {
  const [localExpanded, setLocalExpanded] = useState(expanded)
  useEffect(() => setLocalExpanded(expanded), [expanded])

  const isArray = Array.isArray(value)
  const isObject = Boolean(value && typeof value === 'object' && !isArray)
  const entries = isArray
    ? (value as unknown[]).map((item, index) => [String(index), item] as const)
    : isObject
      ? Object.entries(value as Record<string, unknown>)
      : []

  if (!isArray && !isObject) {
    const type = value === null ? 'null' : typeof value
    const color = type === 'string'
      ? '#86efac'
      : type === 'number'
        ? '#93c5fd'
        : type === 'boolean'
          ? '#fbbf24'
          : '#94a3b8'
    const display = type === 'string' ? JSON.stringify(value) : String(value)
    return <span style={{ color }}>{display}</span>
  }

  if (entries.length === 0) {
    return <span style={{ color: '#94a3b8' }}>{isArray ? '[]' : '{}'}</span>
  }

  const opener = isArray ? '[' : '{'
  const closer = isArray ? ']' : '}'
  return (
    <span>
      <button
        type="button"
        onClick={() => setLocalExpanded((current) => !current)}
        style={{
          width: 18,
          height: 18,
          padding: 0,
          marginRight: 4,
          border: '1px solid var(--app-border)',
          borderRadius: 4,
          background: 'var(--app-card-bg)',
          color: 'var(--app-text-subtle)',
          cursor: 'pointer',
          lineHeight: '14px',
        }}
        aria-label={localExpanded ? 'Collapse JSON node' : 'Expand JSON node'}
      >
        {localExpanded ? '-' : '+'}
      </button>
      <span style={{ color: '#cbd5e1' }}>{opener}</span>
      {!localExpanded ? (
        <>
          <span style={{ color: '#94a3b8' }}> {entries.length} {isArray ? 'items' : 'keys'} </span>
          <span style={{ color: '#cbd5e1' }}>{closer}</span>
        </>
      ) : (
        <>
          <div>
            {entries.map(([key, item], index) => (
              <div key={`${depth}_${key}_${index}`} style={{ paddingLeft: Math.min(28, 14 + depth * 2), lineHeight: '20px' }}>
                {!isArray ? (
                  <>
                    <span style={{ color: '#67e8f9' }}>{JSON.stringify(key)}</span>
                    <span style={{ color: '#cbd5e1' }}>: </span>
                  </>
                ) : (
                  <span style={{ color: '#64748b' }}>{key}: </span>
                )}
                <JsonValue value={item} depth={depth + 1} expanded={expanded} />
                {index < entries.length - 1 ? <span style={{ color: '#cbd5e1' }}>,</span> : null}
              </div>
            ))}
          </div>
          <span style={{ color: '#cbd5e1' }}>{closer}</span>
        </>
      )}
    </span>
  )
}

function JsonViewerCard({ title, value }: { title: string; value: unknown }) {
  const [expanded, setExpanded] = useState(true)
  const displayValue = useMemo(() => decodeJsonStrings(value), [value])
  const rawText = useMemo(() => JSON.stringify(displayValue, null, 2), [displayValue])

  return (
    <div style={{ minWidth: 0 }}>
      <Space size={6} wrap style={{ width: '100%', justifyContent: 'space-between', marginBottom: 6 }}>
        <Text style={{ color: 'var(--app-text)', fontWeight: 700, fontSize: 12 }}>{title}</Text>
        <Space size={6}>
          <Button size="small" onClick={() => setExpanded(true)}>Expand</Button>
          <Button size="small" onClick={() => setExpanded(false)}>Collapse</Button>
          <Button
            size="small"
            icon={<CopyOutlined />}
            onClick={() => {
              void navigator.clipboard?.writeText(rawText)
              notification.success({ message: `${title} copied`, placement: 'bottomRight', duration: 1.5 })
            }}
          >
            Copy
          </Button>
        </Space>
      </Space>
      <div
        style={{
          minHeight: 260,
          maxHeight: 420,
          overflow: 'auto',
          border: '1px solid var(--app-border-strong)',
          borderRadius: 8,
          background: '#0f172a',
          padding: 12,
          color: '#cbd5e1',
          fontFamily: 'monospace',
          fontSize: 12,
          whiteSpace: 'pre-wrap',
        }}
      >
        <JsonValue value={displayValue} depth={0} expanded={expanded} />
      </div>
    </div>
  )
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

function uniqueOptions(options: Array<{ label: string; value: string; group?: string }>) {
  const seen = new Set<string>()
  return options.filter((option) => {
    const value = String(option.value || '').trim()
    if (!value || seen.has(value)) return false
    seen.add(value)
    return true
  })
}

function extractPlaceholders(template: RRETemplate | undefined): string[] {
  const body = template?.body || ''
  return Array.from(new Set(Array.from(body.matchAll(/\{\{\s*([A-Za-z0-9_.-]+)\s*\}\}/g)).map((m) => m[1])))
}

function evaluateTemplateMathExpression(expression: string, output: Record<string, unknown>): string {
  const functionNames = new Set(['abs', 'ceil', 'floor', 'max', 'min', 'pow', 'round', 'sqrt'])
  const expressionWithValues = expression.replace(/\b[A-Za-z_][A-Za-z0-9_.]*\b/g, (token) => {
    if (functionNames.has(token)) return token
    const value = readField(output, token)
    const numeric = Number(value)
    return Number.isFinite(numeric) ? String(numeric) : '0'
  })
  const executable = expressionWithValues.replace(/\b(abs|ceil|floor|max|min|pow|round|sqrt)\b/g, 'Math.$1')
  if (!/^[\d\s+\-*/%().,Mathceilfloormaxminpowroundabsqrt]+$/.test(executable)) return ''
  try {
    const result = Function(`"use strict"; return (${executable})`)()
    return result === undefined || result === null ? '' : formatScalar(result)
  } catch {
    return ''
  }
}

function evaluateTemplateMathExpressionStrict(expression: string, output: Record<string, unknown>): string {
  const functionNames = new Set(['abs', 'ceil', 'floor', 'max', 'min', 'pow', 'round', 'sqrt'])
  const identifiers = Array.from(expression.matchAll(/\b[A-Za-z_][A-Za-z0-9_.]*\b/g)).map((match) => match[0])
  const fields = identifiers.filter((token) => !functionNames.has(token))
  if (!fields.length) return /[0-9)]\s*[+\-*/%]\s*[0-9(]/.test(expression) ? evaluateTemplateMathExpression(expression, output) : ''
  if (fields.some((field) => readField(output, field) === undefined)) return ''
  return evaluateTemplateMathExpression(expression, output)
}

function evaluateTemplateCondition(expression: string, output: Record<string, unknown>): boolean {
  const functionNames = new Set(['abs', 'ceil', 'floor', 'max', 'min', 'pow', 'round', 'sqrt'])
  const expressionWithValues = String(expression || '')
    .replace(/\band\b/gi, '&&')
    .replace(/\bor\b/gi, '||')
    .replace(/\b[A-Za-z_][A-Za-z0-9_.]*\b/g, (token) => {
      if (functionNames.has(token)) return token
      const value = readField(output, token)
      const numeric = Number(value)
      return Number.isFinite(numeric) ? String(numeric) : '0'
    })
    .replace(/\b(abs|ceil|floor|max|min|pow|round|sqrt)\b/g, 'Math.$1')
  const executable = expressionWithValues.replace(/(?<![<>=!])=(?![=])/g, '==')
  if (!/^[\d\s+\-*/%().,Mathceilfloormaxminpowroundabsqrt<>=!&|]+$/.test(executable)) return false
  try {
    return Boolean(Function(`"use strict"; return (${executable})`)())
  } catch {
    return false
  }
}

function renderTemplateConditionals(text: string, output: Record<string, unknown>): string {
  const renderControlBlock = (firstCondition: string, body: string) => {
    const clauses: Array<{ condition: string | null; content: string }> = []
    let currentCondition: string | null = firstCondition
    let cursor = 0
    const markerPattern = /\[(elseif)\s+([^\]\n]+)\]|\[(else)\]/gi
    let marker: RegExpExecArray | null
    while ((marker = markerPattern.exec(body)) !== null) {
      clauses.push({ condition: currentCondition, content: body.slice(cursor, marker.index) })
      currentCondition = marker[1]?.toLowerCase() === 'elseif' ? marker[2] : null
      cursor = marker.index + marker[0].length
    }
    clauses.push({ condition: currentCondition, content: body.slice(cursor) })
    const selected = clauses.find((clause) => clause.condition === null || evaluateTemplateCondition(clause.condition, output))
    return selected ? renderPlainTemplateText(selected.content, output) : ''
  }
  let rendered = String(text || '')
  const renderInnermostCluster = (value: string): { value: string; changed: boolean } => {
    const stack: Array<{ index: number; bodyStart: number; condition: string }> = []
    const tokenPattern = /\[\[if\s+([^\]\n]+)\]|\[end\]\]/gi
    let token: RegExpExecArray | null
    while ((token = tokenPattern.exec(value)) !== null) {
      if (/^\[\[if\s/i.test(token[0])) {
        stack.push({ index: token.index, bodyStart: token.index + token[0].length, condition: token[1] || '' })
      } else if (stack.length > 0) {
        const opener = stack.pop()
        if (!opener) continue
        const body = value.slice(opener.bodyStart, token.index)
        const replacement = renderControlBlock(opener.condition, body)
        return {
          value: `${value.slice(0, opener.index)}${replacement}${value.slice(token.index + token[0].length)}`,
          changed: true,
        }
      }
    }
    return { value, changed: false }
  }
  for (let guard = 0; guard < 50; guard += 1) {
    const next = renderInnermostCluster(rendered)
    rendered = next.value
    if (!next.changed) break
  }
  rendered = rendered.replace(/\[if\s+([^\]\n]+)\]([\s\S]*?)\[end\]/gi, (_match, firstCondition: string, body: string) => {
    return renderControlBlock(firstCondition, body)
  })
  return rendered
}

function renderPlainTemplateText(text: string, output: Record<string, unknown>): string {
  const conditionRendered = renderTemplateConditionals(text, output)
  const directValue = readField(output, conditionRendered.trim())
  if (directValue !== undefined) return formatScalar(directValue)
  const expressionAtom = String.raw`(?:[A-Za-z_][A-Za-z0-9_.]*|\d+(?:\.\d+)?)`
  const binaryExpressionPattern = new RegExp(String.raw`\b${expressionAtom}(?:\s*[+\-*/%]\s*${expressionAtom})+\b`, 'g')
  const functionExpressionPattern = /\b(?:round|abs|ceil|floor|min|max|sqrt|pow)\s*\([^()]*[+\-*/%][^()]*\)/g
  let rendered = conditionRendered.replace(/\[([^\][\n]+)\]/g, (match, expression: string) => {
    const fieldValue = readField(output, expression.trim())
    if (fieldValue !== undefined) return formatScalar(fieldValue)
    const evaluated = evaluateTemplateMathExpressionStrict(expression, output)
    return evaluated || expression
  })
  rendered = rendered.replace(functionExpressionPattern, (candidate) => {
    const evaluated = evaluateTemplateMathExpressionStrict(candidate, output)
    return evaluated || candidate
  })
  rendered = rendered.replace(binaryExpressionPattern, (candidate) => {
    const evaluated = evaluateTemplateMathExpressionStrict(candidate, output)
    return evaluated || candidate
  })
  rendered = rendered.replace(/\b[A-Za-z_][A-Za-z0-9_.]*\b/g, (token) => {
    const value = readField(output, token)
    return value === undefined ? token : formatScalar(value)
  })
  return rendered
}

function templateDisplayParts(value: string) {
  const text = String(value || '')
  const parts: Array<{ type: 'text' | 'expression' | 'field' | 'control'; value: string }> = []
  let cursor = 0
  Array.from(text.matchAll(/\[\[if\s+[^\]\n]+\]|\[if\s+[^\]\n]+\]|\[elseif\s+[^\]\n]+\]|\[else\]|\[end\]\]|\[end\]|\[([^\][\n]+)\]/gi)).forEach((match) => {
    const index = match.index ?? 0
    if (index > cursor) parts.push({ type: 'text', value: text.slice(cursor, index) })
    if (/^\[\[?if\s/i.test(match[0]) || /^\[elseif\s/i.test(match[0]) || /^\[(else|end)\]?\]/i.test(match[0])) {
      parts.push({ type: 'control', value: match[0] })
    } else {
      const inner = match[1] || ''
      const isExpression = /[+\-*/%()]|\b(round|abs|ceil|floor|min|max|sqrt|pow)\s*\(/.test(inner)
      parts.push({ type: isExpression ? 'expression' : 'field', value: match[0] })
    }
    cursor = index + match[0].length
  })
  if (cursor < text.length) parts.push({ type: 'text', value: text.slice(cursor) })
  if (!parts.length && text) parts.push({ type: 'text', value: text })
  return parts
}

function templatePartStyle(type: 'text' | 'expression' | 'field' | 'control') {
  if (type === 'control') return { color: '#fbbf24', fontWeight: 700 }
  if (type === 'expression') return { color: '#c4b5fd', fontWeight: 650 }
  if (type === 'field') return { color: '#93c5fd', fontWeight: 650 }
  return { color: 'var(--app-text)' }
}

function renderMappingTemplate(template: string, output: Record<string, unknown>): string {
  const text = String(template || '')
  if (!text.includes('{{')) {
    return renderPlainTemplateText(text, output)
  }
  return text.replace(/\{\{\s*(=)?\s*([^{}]+?)\s*\}\}/g, (_match, isExpression: string, body: string) => {
    const content = String(body || '').trim()
    if (!content) return ''
    if (isExpression) return evaluateTemplateMathExpression(content, output)
    return formatScalar(readField(output, content))
  })
}

function appendTemplateToken(value: string, token: string): string {
  const current = String(value || '').trimEnd()
  return `${current}${current ? ' ' : ''}${token}`
}

function appendExpressionSnippet(value: string, snippet: string): string {
  const current = String(value || '').trimEnd()
  const expressionMatch = current.match(/\{\{\s*=\s*([^{}]*?)\s*\}\}\s*$/)
  if (!expressionMatch || expressionMatch.index === undefined) {
    return appendTemplateToken(current, `{{= ${snippet} }}`)
  }
  const prefix = current.slice(0, expressionMatch.index)
  const expression = String(expressionMatch[1] || '').trimEnd()
  return `${prefix}{{= ${expression}${expression ? ' ' : ''}${snippet} }}`
}

function insertExpressionSnippetAt(value: string, snippet: string, cursor: number): string {
  const text = String(value || '')
  const cursorIndex = Math.max(0, Math.min(Number.isFinite(cursor) ? cursor : text.length, text.length))
  const draft = templateInlineDraftAt(text, cursorIndex)
  if (draft) {
    const before = text.slice(0, cursorIndex).trimEnd()
    const after = text.slice(cursorIndex)
    return `${before}${before.endsWith('=') || before.endsWith('(') ? ' ' : ' '}${snippet}${after}`
  }
  return `${text.slice(0, cursorIndex)}${snippet}${text.slice(cursorIndex)}`
}

function templateInlineDraftAt(value: string, cursor: number): { mode: 'field' | 'expression'; query: string; start: number; cursor: number } | null {
  const text = String(value || '')
  const cursorIndex = Math.max(0, Math.min(Number.isFinite(cursor) ? cursor : text.length, text.length))
  const beforeCursor = text.slice(0, cursorIndex)
  const start = beforeCursor.lastIndexOf('{{')
  if (start < 0) {
    const bracketStart = beforeCursor.lastIndexOf('[')
    if (bracketStart >= 0 && beforeCursor.lastIndexOf(']') < bracketStart && !beforeCursor.slice(bracketStart + 1).includes('\n')) {
      const expression = beforeCursor.slice(bracketStart + 1)
      const match = expression.match(/([A-Za-z_][A-Za-z0-9_.]*)$/)
      return { mode: 'expression', query: match?.[1] || '', start: bracketStart, cursor: cursorIndex }
    }
    const match = beforeCursor.match(/([A-Za-z_][A-Za-z0-9_.]*)$/)
    const operatorNearCursor = /[+\-*/%(,]\s*[A-Za-z_][A-Za-z0-9_.]*$/.test(beforeCursor)
    if (!match && !operatorNearCursor) return null
    return { mode: 'expression', query: match?.[1] || '', start: cursorIndex - (match?.[1]?.length || 0), cursor: cursorIndex }
  }
  if (beforeCursor.lastIndexOf('}}') > start) return null
  const nextClose = text.indexOf('}}', start + 2)
  if (nextClose >= 0 && nextClose < cursorIndex) return null
  const body = text.slice(start + 2, cursorIndex)
  if (body.includes('\n')) return null
  const trimmedStart = body.trimStart()
  if (trimmedStart.startsWith('=')) {
    const expression = trimmedStart.slice(1)
    const match = expression.match(/([A-Za-z_][A-Za-z0-9_.]*)$/)
    return { mode: 'expression', query: match?.[1] || '', start, cursor: cursorIndex }
  }
  const match = body.match(/([A-Za-z_][A-Za-z0-9_.]*)$/)
  return { mode: 'field', query: match?.[1] || '', start, cursor: cursorIndex }
}

function templateInlineDraft(value: string): { mode: 'field' | 'expression'; query: string; start: number; cursor: number } | null {
  return templateInlineDraftAt(value, String(value || '').length)
}

function replaceTemplateInlineDraftAt(value: string, field: string, cursor: number): string {
  const text = String(value || '')
  const draft = templateInlineDraftAt(text, cursor)
  if (!draft) return `${text.slice(0, cursor)}${field}${text.slice(cursor)}`
  const braceMode = text.slice(draft.start, draft.start + 2) === '{{'
  const bracketMode = text.slice(draft.start, draft.start + 1) === '['
  const prefix = text.slice(0, draft.start)
  const body = text.slice(draft.start + (braceMode ? 2 : bracketMode ? 1 : 0), draft.cursor)
  const suffix = text.slice(draft.cursor)
  if (!braceMode && !bracketMode) {
    const replaced = draft.query
      ? body.replace(new RegExp(`${draft.query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`), field)
      : field
    return `${prefix}${replaced}${suffix}`
  }
  if (bracketMode) {
    const replacedBody = draft.query
      ? body.replace(new RegExp(`${draft.query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`), field)
      : `${body}${field}`
    return `${prefix}[${replacedBody}${suffix}`
  }
  if (draft.mode === 'expression') {
    const replacedBody = draft.query
      ? body.replace(new RegExp(`${draft.query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`), field)
      : `${body}${body.trim().endsWith('=') ? ' ' : ''}${field}`
    return `${prefix}{{${replacedBody}${suffix}`
  }
  return `${prefix}{{${field}}}${suffix}`
}

function replaceTemplateInlineDraft(value: string, field: string): string {
  return replaceTemplateInlineDraftAt(value, field, String(value || '').length)
}

function replaceTemplateExpressionFunctionDraftAt(value: string, fn: string, cursor: number): string {
  const text = String(value || '')
  const draft = templateInlineDraftAt(text, cursor)
  if (!draft || draft.mode !== 'expression') return insertExpressionSnippetAt(text, `${fn}()`, cursor)
  const braceMode = text.slice(draft.start, draft.start + 2) === '{{'
  const bracketMode = text.slice(draft.start, draft.start + 1) === '['
  const prefix = text.slice(0, draft.start)
  const body = text.slice(draft.start + (braceMode ? 2 : bracketMode ? 1 : 0), draft.cursor)
  const suffix = text.slice(draft.cursor)
  const snippet = `${fn}()`
  const replacedBody = draft.query
    ? body.replace(new RegExp(`${draft.query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`), snippet)
    : `${body}${body.trim().endsWith('=') ? ' ' : ''}${snippet}`
  return `${prefix}${braceMode ? '{{' : bracketMode ? '[' : ''}${replacedBody}${suffix}`
}

function checkTemplateSyntax(value: string, fieldOptions: Array<{ label: string; value: string }>): { errors: string[]; warnings: string[] } {
  const text = String(value || '')
  const errors: string[] = []
  const warnings: string[] = []
  const fieldSet = new Set(fieldOptions.map((option) => String(option.value || '').trim()).filter(Boolean))
  const knownFunctions = new Set(['round', 'abs', 'ceil', 'floor', 'min', 'max', 'sqrt', 'pow'])

  const openCount = (text.match(/\{\{/g) || []).length
  const closeCount = (text.match(/\}\}/g) || []).length
  if (openCount > closeCount) errors.push('Missing closing }}.')
  if (closeCount > openCount) errors.push('Extra closing }}.')
  const squareOpenCount = (text.match(/\[/g) || []).length
  const squareCloseCount = (text.match(/\]/g) || []).length
  if (squareOpenCount > squareCloseCount) errors.push('Missing closing ] for expression.')
  if (squareCloseCount > squareOpenCount) errors.push('Extra closing ].')

  if (!text.includes('{{')) {
    const trimmed = text.trim()
    if (!trimmed) return { errors, warnings }
    Array.from(trimmed.matchAll(/\[([^\][\n]*)\]/g)).forEach((match) => {
      const expression = String(match[1] || '').trim()
      if (/^(if|elseif)\s+/i.test(expression) || /^(else|end)$/i.test(expression)) return
      if (!expression) errors.push('Expression block [] is empty.')
      const leftParens = (expression.match(/\(/g) || []).length
      const rightParens = (expression.match(/\)/g) || []).length
      if (leftParens !== rightParens) errors.push('Expression parentheses are not balanced.')
    })
    const expressionLike = /[+\-*/%()]|\b(round|abs|ceil|floor|min|max|sqrt|pow)\s*\(/.test(trimmed)
    const directFieldLike = /^[A-Za-z_][A-Za-z0-9_.]*$/.test(trimmed)
    if (expressionLike) {
      const leftParens = (trimmed.match(/\(/g) || []).length
      const rightParens = (trimmed.match(/\)/g) || []).length
      if (leftParens !== rightParens) errors.push('Expression parentheses are not balanced.')
      Array.from(trimmed.matchAll(/\b([A-Za-z_][A-Za-z0-9_]*)\s*\(/g)).forEach((fnMatch) => {
        const fn = String(fnMatch[1] || '')
        if (!knownFunctions.has(fn)) warnings.push(`Unknown function ${fn}().`)
      })
      Array.from(trimmed.matchAll(/\b[A-Za-z_][A-Za-z0-9_.]*\b/g)).forEach((tokenMatch) => {
        const token = String(tokenMatch[0] || '')
        if (knownFunctions.has(token) || fieldSet.has(token)) return
        const before = trimmed.slice(Math.max(0, (tokenMatch.index || 0) - 3), tokenMatch.index || 0)
        const after = trimmed.slice((tokenMatch.index || 0) + token.length, (tokenMatch.index || 0) + token.length + 3)
        if (/[+\-*/%(,]\s*$/.test(before) || /^\s*[+\-*/%),]/.test(after)) warnings.push(`Field ${token} is not in current output fields.`)
      })
    } else if (directFieldLike && !fieldSet.has(trimmed)) {
      warnings.push(`Field ${trimmed} is not in current output fields.`)
    }
    return {
      errors: Array.from(new Set(errors)),
      warnings: Array.from(new Set(warnings)).slice(0, 4),
    }
  }

  Array.from(text.matchAll(/\{\{\s*([^{}]*?)\s*\}\}/g)).forEach((match) => {
    const body = String(match[1] || '').trim()
    if (!body) {
      errors.push('Empty template token.')
      return
    }
    if (body.startsWith('=')) {
      const expression = body.slice(1).trim()
      if (!expression) errors.push('Expression is empty after =.')
      const leftParens = (expression.match(/\(/g) || []).length
      const rightParens = (expression.match(/\)/g) || []).length
      if (leftParens !== rightParens) errors.push('Expression parentheses are not balanced.')
      Array.from(expression.matchAll(/\b([A-Za-z_][A-Za-z0-9_]*)\s*\(/g)).forEach((fnMatch) => {
        const fn = String(fnMatch[1] || '')
        if (!knownFunctions.has(fn)) warnings.push(`Unknown function ${fn}().`)
      })
      Array.from(expression.matchAll(/\b[A-Za-z_][A-Za-z0-9_.]*\b/g)).forEach((tokenMatch) => {
        const token = String(tokenMatch[0] || '')
        if (knownFunctions.has(token)) return
        if (!fieldSet.has(token)) warnings.push(`Field ${token} is not in current output fields.`)
      })
    } else if (!fieldSet.has(body)) {
      warnings.push(`Field ${body} is not in current output fields.`)
    }
  })

  return {
    errors: Array.from(new Set(errors)),
    warnings: Array.from(new Set(warnings)).slice(0, 4),
  }
}

function autoCorrectTemplateSyntax(value: string): string {
  let text = String(value || '')
  text = text.replace(/\{=\s*([^{}]+?)\}/g, '{{= $1 }}')
  text = text.replace(/\{([A-Za-z_][A-Za-z0-9_.]*)\}/g, '{{$1}}')
  text = text.replace(/\{\{\s*=\s*([^{}]*?)$/g, '{{= $1 }}')
  text = text.replace(/\{\{\s*([^{}]*?)$/g, '{{$1}}')
  const openCount = (text.match(/\{\{/g) || []).length
  const closeCount = (text.match(/\}\}/g) || []).length
  if (openCount > closeCount) text = `${text}${'}}'.repeat(openCount - closeCount)}`
  if (closeCount > openCount) {
    let excess = closeCount - openCount
    text = text.replace(/\}\}/g, (match) => {
      if (excess <= 0) return match
      excess -= 1
      return ''
    })
  }
  return text
}

function textareaCaretPosition(textarea: HTMLTextAreaElement): { left: number; top: number } {
  const style = window.getComputedStyle(textarea)
  const mirror = document.createElement('div')
  const properties = [
    'boxSizing',
    'width',
    'fontFamily',
    'fontSize',
    'fontWeight',
    'fontStyle',
    'letterSpacing',
    'textTransform',
    'wordSpacing',
    'textIndent',
    'lineHeight',
    'paddingTop',
    'paddingRight',
    'paddingBottom',
    'paddingLeft',
    'borderTopWidth',
    'borderRightWidth',
    'borderBottomWidth',
    'borderLeftWidth',
  ]
  properties.forEach((property) => {
    mirror.style.setProperty(property, style.getPropertyValue(property))
  })
  mirror.style.position = 'absolute'
  mirror.style.visibility = 'hidden'
  mirror.style.whiteSpace = 'pre-wrap'
  mirror.style.overflowWrap = 'break-word'
  mirror.style.top = '0'
  mirror.style.left = '-9999px'
  mirror.textContent = textarea.value.slice(0, textarea.selectionStart || 0)
  const marker = document.createElement('span')
  marker.textContent = textarea.value.slice(textarea.selectionStart || 0, (textarea.selectionStart || 0) + 1) || '.'
  mirror.appendChild(marker)
  document.body.appendChild(mirror)
  const lineHeight = Number.parseFloat(style.lineHeight) || 18
  const left = marker.offsetLeft - textarea.scrollLeft
  const top = marker.offsetTop - textarea.scrollTop + lineHeight + 4
  document.body.removeChild(mirror)
  return { left: Math.max(6, left), top: Math.max(6, top) }
}

function resolveMappedValue(output: Record<string, unknown>, mapping: RRETemplateMapping): string {
  if (mapping.source === 'custom') return String(mapping.value ?? '')
  if (mapping.source === 'template') return renderMappingTemplate(String(mapping.value ?? ''), output)
  const path = String(mapping.field || '').trim()
  if (!path) return ''
  return formatScalar(readField(output, path))
}

function renderTemplate(
  template: RRETemplate | undefined,
  rule: RRERule | undefined,
  output: Record<string, unknown>,
  dictionary: RREFeatureDictionaryEntry[] = [],
): string {
  if (!template || !rule) return 'No template matched the satisfied rules.'
  const values: Record<string, string> = {
    responsibility: template.responsibility,
  }
  const signal = buildNarrative(rule, output, dictionary)
  Object.assign(values, signal.placeholders)
  const smartWordingMaxChars = ruleSmartWordingMaxChars(rule)
  rule.templateMappings.forEach((mapping) => {
    if (!mapping.placeholder) return
    const mappedValue = resolveMappedValue(output, mapping)
    values[mapping.placeholder] = rule.smartWordingEnabled ? compactRuleWording(mappedValue, smartWordingMaxChars) : mappedValue
  })
  const rendered = template.body.replace(/\{\{\s*(=)?\s*([^{}]+?)\s*\}\}/g, (_match, isExpression: string, body: string) => {
    const key = String(body || '').trim()
    if (!key) return ''
    if (isExpression) return evaluateTemplateMathExpression(key, output)
    return values[key] ?? formatScalar(readField(output, key))
  })
  return rule.smartWordingEnabled ? compactRuleWording(rendered, smartWordingMaxChars) : rendered
}

function buildNarrative(rule: RRERule, output: Record<string, unknown>, dictionary: RREFeatureDictionaryEntry[]) {
  const cfg = rule.signalConfig
  const feature = String(cfg?.feature || '').trim()
  const meta = dictionary.find((item) => item.field === feature)
  const businessName = meta?.business_name || feature || rule.name
  const severity = cfg?.severity || 'MEDIUM'
  const value = cfg?.valueField ? readField(output, cfg.valueField) : undefined
  const peerValue = cfg?.peerValueField ? readField(output, cfg.peerValueField) : undefined
  const impact = cfg?.impactSource === 'custom'
    ? cfg.impactValue
    : (cfg?.impactField ? formatScalar(readField(output, cfg.impactField)) : '')
  const defaultRecommendation = meta?.default_recommendation || ''
  let recommendation = cfg?.recommendation || defaultRecommendation
  const smartWordingMaxChars = ruleSmartWordingMaxChars(rule)
  const risk = riskBand(readField(output, 'predictions.risk_score') ?? readField(output, 'risk_score') ?? readField(output, 'prediction_score'))
  const summary = risk ? `${rule.name} classified this record as ${risk} risk.` : `${rule.name} matched this record.`
  let observation = ''
  if (feature && peerValue !== undefined && peerValue !== null && String(peerValue).trim() !== '') {
    observation = `${businessName.charAt(0).toUpperCase()}${businessName.slice(1)} is ${severityWord(severity)} higher than peer behavior, with observed value ${formatScalar(value)} compared to peer average ${formatScalar(peerValue)}.`
  } else if (feature) {
    observation = `${businessName.charAt(0).toUpperCase()}${businessName.slice(1)} is ${severityWord(severity)} abnormal, with observed value ${formatScalar(value)}.`
  }
  if (meta?.meaning) observation = `${observation} This indicates ${meta.meaning}.`
  if (rule.smartWordingEnabled || cfg?.smartWordingEnabled) {
    observation = compactRuleWording(observation, smartWordingMaxChars)
    recommendation = compactRuleWording(recommendation, smartWordingMaxChars)
  }
  const signalRecommendations = recommendation ? `- ${recommendation}` : ''
  const signalOutput = [
    summary,
    observation ? `Observation: ${observation}` : '',
    impact ? `Impact: ${impact}` : '',
    recommendation ? `Recommendation: ${recommendation}` : '',
  ].filter(Boolean).join('\n')
  return {
    observation,
    recommendation,
    json: {
      summary,
      observation,
      recommendation,
      default_recommendation: defaultRecommendation,
      risk_band: risk,
      feature,
      business_name: businessName,
      meaning: meta?.meaning || '',
      severity,
      value: formatScalar(value),
      peer_value: formatScalar(peerValue),
      impact: String(impact || ''),
      unit: meta?.unit || '',
      direction: meta?.direction || '',
    },
    placeholders: {
      signal_output: signalOutput,
      summary,
      signal_observations: observation ? `- ${observation}` : '',
      signal_recommendations: signalRecommendations,
      signal_default_recommendation: defaultRecommendation,
      top_signal: businessName,
      risk_band: risk,
      signal_feature: feature,
      signal_business_name: businessName,
      signal_meaning: meta?.meaning || '',
      signal_unit: meta?.unit || '',
      signal_direction: meta?.direction || '',
      signal_severity: severity,
      signal_value: formatScalar(value),
      signal_peer_value: formatScalar(peerValue),
      signal_impact: String(impact || ''),
      signal_action: recommendation,
    },
  }
}

function buildSignalJsonOutput(rule: RRERule, output: Record<string, unknown>, dictionary: RREFeatureDictionaryEntry[]): string {
  const cfg = normalizeSignalConfig(rule.signalConfig)
  if (cfg.enabled === false) return ''
  const narrative = buildNarrative(rule, output, dictionary)
  const fields = cfg.jsonFields.length > 0 ? cfg.jsonFields : defaultSignalJsonFields
  const selected = fields.reduce<Record<string, string>>((acc, field) => {
    const key = String(field || '').trim()
    if (!key) return acc
    acc[key] = String((narrative.json as Record<string, unknown>)[key] ?? '')
    return acc
  }, {})
  return JSON.stringify(selected)
}

function evaluateAutoSignal(entry: RREFeatureDictionaryEntry, output: Record<string, unknown>) {
  if (!entry.auto_signal_enabled) return null
  const value = readField(output, entry.field)
  const num = Number(value)
  if (!Number.isFinite(num)) return null
  const warning = Number(entry.warning_threshold)
  const critical = Number(entry.critical_threshold)
  const hasWarning = Number.isFinite(warning)
  const hasCritical = Number.isFinite(critical)
  if (!hasWarning && !hasCritical) return null
  const direction = String(entry.direction || 'higher_is_risky')
  const criticalHit = hasCritical && (
    direction === 'lower_is_risky' ? num <= critical : num >= critical
  )
  const warningHit = hasWarning && (
    direction === 'lower_is_risky' ? num <= warning : num >= warning
  )
  if (!criticalHit && !warningHit) return null
  const severity = criticalHit ? 'HIGH' : 'MEDIUM'
  const threshold = criticalHit ? entry.critical_threshold : entry.warning_threshold
  const thresholdNum = Number(threshold)
  const breachAmount = Number.isFinite(thresholdNum)
    ? Math.abs(num - thresholdNum)
    : 0
  const breachPercent = Number.isFinite(thresholdNum) && thresholdNum !== 0
    ? (breachAmount / Math.abs(thresholdNum)) * 100
    : 0
  const businessName = entry.business_name || titleCase(entry.field)
  const relation = direction === 'lower_is_risky' ? 'below' : 'above'
  const observation = `${businessName} is ${severity === 'HIGH' ? 'significantly' : 'moderately'} ${relation} ${criticalHit ? 'critical' : 'warning'} threshold ${threshold}, with observed value ${formatScalar(value)}.`
  return {
    feature: entry.field,
    business_name: businessName,
    severity,
    value: formatScalar(value),
    threshold: String(threshold || ''),
    threshold_type: criticalHit ? 'critical' : 'warning',
    breach_amount: Number.isFinite(breachAmount) ? formatScalar(breachAmount) : '',
    breach_percent: Number.isFinite(breachPercent) ? formatScalar(breachPercent) : '',
    impact_role: entry.impact_role || 'driver',
    impact_weight: formatScalar(entry.impact_weight ?? 5),
    direction,
    unit: entry.unit || '',
    meaning: entry.meaning || '',
    observation: entry.meaning ? `${observation} This indicates ${entry.meaning}.` : observation,
    recommendation: entry.default_recommendation || '',
  }
}

function buildConfiguredClusterSignal(entry: RREFeatureDictionaryEntry, output: Record<string, unknown>) {
  const value = readField(output, entry.field)
  if (value === undefined || value === null || String(value).trim() === '') return null
  const autoSignal = evaluateAutoSignal(entry, output)
  if (autoSignal) return autoSignal
  const businessName = entry.business_name || titleCase(entry.field)
  return {
    feature: entry.field,
    business_name: businessName,
    severity: 'CONFIGURED',
    value: formatScalar(value),
    threshold: '',
    threshold_type: '',
    breach_amount: '',
    breach_percent: '',
    impact_role: entry.impact_role || 'driver',
    impact_weight: formatScalar(entry.impact_weight ?? 5),
    direction: entry.direction || '',
    unit: entry.unit || '',
    meaning: entry.meaning || '',
    observation: `${businessName} is included in this configured cluster with observed value ${formatScalar(value)}.`,
    recommendation: entry.default_recommendation || '',
  }
}

function signalSeverityRank(signal: Record<string, string>): number {
  const severity = String(signal.severity || '').toUpperCase()
  if (severity === 'HIGH') return 3
  if (severity === 'MEDIUM') return 2
  if (severity === 'LOW') return 1
  return 0
}

function signalNumericValue(signal: Record<string, string>, key: string): number {
  const value = Number(String(signal[key] || '').replace(/,/g, ''))
  return Number.isFinite(value) ? value : 0
}

function signalBalancedScore(signal: Record<string, string>): number {
  const severity = signalSeverityRank(signal)
  const severityWeight = severity === 3 ? 1000 : severity === 2 ? 500 : severity === 1 ? 100 : 0
  return severityWeight + signalNumericValue(signal, 'breach_percent')
}

function signalImpactRole(signal: Record<string, string>): RREImpactRole {
  const role = String(signal.impact_role || '').toLowerCase()
  return ['driver', 'outcome', 'context', 'identifier'].includes(role) ? role as RREImpactRole : 'driver'
}

function signalImpactRoleWeight(signal: Record<string, string>): number {
  const role = signalImpactRole(signal)
  if (role === 'driver') return 300
  if (role === 'outcome') return 100
  if (role === 'context') return 20
  return 0
}

function signalIntelligentImpactScore(signal: Record<string, string>): number {
  return signalBalancedScore(signal) + signalImpactRoleWeight(signal) + (signalNumericValue(signal, 'impact_weight') * 25)
}

function rankClusterSignals(cluster: RREClusterConfig, signals: Array<Record<string, string>>): Array<Record<string, string>> {
  const featureOrder = new Map(cluster.features.map((field, index) => [field, index]))
  const filtered = (cluster.feature_filter || 'breached_only') === 'all_selected'
    ? signals
    : signals.filter((signal) => signalSeverityRank(signal) > 0)
  const rankBy = cluster.rank_by || 'balanced_score'
  const ranked = [...filtered].sort((a, b) => {
    if (rankBy === 'configured_order') {
      return (featureOrder.get(String(a.feature || '')) ?? 999999) - (featureOrder.get(String(b.feature || '')) ?? 999999)
    }
    if (rankBy === 'balanced_score') {
      const scoreDelta = signalBalancedScore(b) - signalBalancedScore(a)
      if (scoreDelta !== 0) return scoreDelta
    }
    if (rankBy === 'intelligent_impact') {
      const scoreDelta = signalIntelligentImpactScore(b) - signalIntelligentImpactScore(a)
      if (scoreDelta !== 0) return scoreDelta
    }
    const severityDelta = signalSeverityRank(b) - signalSeverityRank(a)
    if (rankBy === 'severity' && severityDelta !== 0) return severityDelta
    if (rankBy === 'breach_percent' && severityDelta !== 0) return severityDelta
    const metricKey = rankBy === 'breach_percent'
      ? 'breach_percent'
      : rankBy === 'absolute_breach'
        ? 'breach_amount'
        : rankBy === 'value'
          ? 'value'
          : ''
    if (metricKey) {
      const metricDelta = signalNumericValue(b, metricKey) - signalNumericValue(a, metricKey)
      if (metricDelta !== 0) return metricDelta
    }
    if (severityDelta !== 0) return severityDelta
    return (featureOrder.get(String(a.feature || '')) ?? 999999) - (featureOrder.get(String(b.feature || '')) ?? 999999)
  })
  const limit = Math.max(0, Math.trunc(Number(cluster.feature_limit || 0)))
  return limit > 0 ? ranked.slice(0, limit) : ranked
}

function buildAutoSignalJsonOutput(output: Record<string, unknown>, dictionary: RREFeatureDictionaryEntry[]): string {
  const signals = dictionary.map((entry) => evaluateAutoSignal(entry, output)).filter(Boolean)
  if (!signals.length) return ''
  return JSON.stringify({ signals })
}

function buildSelectedAutoSignalJsonOutput(output: Record<string, unknown>, dictionary: RREFeatureDictionaryEntry[], selectedFields: string[]): string {
  const fields = selectedFields.length > 0 ? selectedFields : defaultAutoSignalJsonFields
  const signals = dictionary
    .map((entry) => evaluateAutoSignal(entry, output))
    .filter(Boolean)
    .map((signal) => fields.reduce<Record<string, string>>((acc, field) => {
      const key = String(field || '').trim()
      if (key) acc[key] = String((signal as Record<string, unknown>)[key] ?? '')
      return acc
    }, {}))
  if (!signals.length) return ''
  return JSON.stringify({ signals })
}

function buildAutoSignalPreview(entry: RREFeatureDictionaryEntry, output: Record<string, unknown> | null): Record<string, unknown> {
  if (!output) {
    return {
      status: 'no_sample',
      reason: 'No preview row is available. Use Load Latest Output to preview against actual data.',
      field: entry.field,
    }
  }
  const signal = evaluateAutoSignal(entry, output)
  if (signal) return signal
  const value = entry.field ? readField(output, entry.field) : undefined
  return {
    status: 'not_triggered',
    reason: value === undefined || value === null || String(value).trim() === ''
      ? `No sample value found for ${entry.field || 'selected field'}. Use Load Latest Output or select a field available in preview data.`
      : 'Sample value does not cross the configured warning or critical threshold.',
    field: entry.field,
    sample_value: formatScalar(value),
    direction: entry.direction || 'higher_is_risky',
    warning_threshold: entry.warning_threshold || '',
    critical_threshold: entry.critical_threshold || '',
    auto_signal_enabled: Boolean(entry.auto_signal_enabled),
  }
}

function selectSignalFields(signal: Record<string, string>, selectedFields: string[]): Record<string, string> {
  const fields = selectedFields.length > 0 ? selectedFields : defaultAutoSignalJsonFields
  return fields.reduce<Record<string, string>>((acc, field) => {
    const key = String(field || '').trim()
    if (!key) return acc
    acc[key] = String(signal[key] ?? '')
    return acc
  }, {})
}

function clusterFactorSentence(signal: Record<string, string>): string {
  const name = signal.business_name || signal.feature || 'factor'
  const value = signal.value ? `${signal.value}${signal.unit ? ` ${signal.unit}` : ''}` : ''
  const thresholdType = String(signal.threshold_type || '').trim()
  const threshold = String(signal.threshold || '').trim()
  const direction = String(signal.direction || '').trim()
  if (thresholdType && threshold) {
    const relation = direction === 'lower_is_risky' ? 'below' : 'above'
    return `${name} is ${value || 'present'}, ${relation} the ${thresholdType} threshold of ${threshold}${signal.unit ? ` ${signal.unit}` : ''}`
  }
  return `${name}${value ? ` is ${value}` : ' is present'}`
}

function buildClusterEvidence(signals: Array<Record<string, string>>) {
  return signals.map((signal) => ({
    field: signal.feature || '',
    factor: signal.business_name || signal.feature || '',
    value: signal.value || '',
    unit: signal.unit || '',
    severity: signal.severity || '',
    threshold: signal.threshold || '',
    threshold_type: signal.threshold_type || '',
    breach_amount: signal.breach_amount || '',
    breach_percent: signal.breach_percent || '',
    impact_role: signalImpactRole(signal),
    impact_weight: signal.impact_weight || '',
    impact_score: formatScalar(signalIntelligentImpactScore(signal)),
    direction: signal.direction || '',
  }))
}

function classifyClusterSignals(signals: Array<Record<string, string>>) {
  const drivers = signals.filter((signal) => signalImpactRole(signal) === 'driver')
  const outcomes = signals.filter((signal) => signalImpactRole(signal) === 'outcome')
  const context = signals.filter((signal) => ['context', 'identifier'].includes(signalImpactRole(signal)))
  const primary = drivers[0] || signals[0] || null
  return {
    primary_driver: primary ? (primary.business_name || primary.feature || '') : '',
    secondary_drivers: drivers
      .filter((signal) => signal !== primary)
      .map((signal) => signal.business_name || signal.feature)
      .filter(Boolean),
    impacted_outcomes: outcomes
      .map((signal) => signal.business_name || signal.feature)
      .filter(Boolean),
    context_fields: context
      .map((signal) => signal.business_name || signal.feature)
      .filter(Boolean),
  }
}

function buildCumulativeClusterObservation(clusterName: string, signals: Array<Record<string, string>>, scenario = ''): string {
  const classified = classifyClusterSignals(signals)
  if (classified.primary_driver) {
    const primary = signals.find((signal) => (signal.business_name || signal.feature) === classified.primary_driver) || signals[0]
    const primaryRole = signalImpactRole(primary)
    const driverReason = primary
      ? `${classified.primary_driver} is the ${primaryRole === 'outcome' ? 'primary impacted outcome' : 'primary impact driver'} because it crossed ${primary.threshold_type || 'configured'} criteria${primary.breach_percent ? ` by ${primary.breach_percent}%` : ''}`
      : `${classified.primary_driver} is the primary impact driver`
    const evidenceText = signals.length
      ? ` Evidence: ${signals.map(clusterFactorSentence).join('; ')}.`
      : ''
    const impactedOutcomes = classified.impacted_outcomes.filter((item) => item !== classified.primary_driver)
    const outcomeText = impactedOutcomes.length
      ? ` ${impactedOutcomes.join(', ')} ${impactedOutcomes.length === 1 ? 'is' : 'are'} impacted outcome${impactedOutcomes.length === 1 ? '' : 's'}.`
      : ''
    return `${scenario || clusterName}: ${driverReason}.${outcomeText}${evidenceText}`
  }
  const factors = signals.map(clusterFactorSentence)
  const lead = scenario || clusterName
  const severity = signals.some((signal) => signal.severity === 'HIGH')
    ? 'high-risk'
    : signals.some((signal) => signal.severity === 'MEDIUM')
      ? 'warning-level'
      : 'configured'
  return `${lead}: ${factors.join(', and ')}. Together, these criteria form a ${severity} ${clusterName} pattern that should be reviewed as one scenario.`
}

function buildCumulativeClusterRecommendation(clusterName: string, signals: Array<Record<string, string>>, instruction = ''): string {
  const classified = classifyClusterSignals(signals)
  if (classified.primary_driver) {
    const primary = signals.find((signal) => (signal.business_name || signal.feature) === classified.primary_driver) || signals[0]
    const primaryRole = primary ? signalImpactRole(primary) : 'driver'
    const support = classified.secondary_drivers.length
      ? ` Supporting drivers: ${classified.secondary_drivers.join(', ')}.`
      : ''
    const impactedOutcomes = classified.impacted_outcomes.filter((item) => item !== classified.primary_driver)
    const outcome = impactedOutcomes.length
      ? ` Check downstream impact on ${impactedOutcomes.join(', ')}.`
      : ''
    const absoluteEvidence = signals
      .slice(0, 3)
      .map(clusterFactorSentence)
      .join('; ')
    const evidence = absoluteEvidence ? ` Evidence to validate: ${absoluteEvidence}.` : ''
    const action = primaryRole === 'outcome'
      ? `Review ${classified.primary_driver} as the breached outcome`
      : `Prioritize ${classified.primary_driver}`
    const base = `${action}; validate source transactions and compare recent customer/entity behavior.${support}${outcome}${evidence}`
    return instruction ? `${instruction}. ${base}` : base
  }
  const critical = signals
    .filter((signal) => signal.severity === 'HIGH')
    .map((signal) => signal.business_name || signal.feature)
    .filter(Boolean)
  const warning = signals
    .filter((signal) => signal.severity === 'MEDIUM')
    .map((signal) => signal.business_name || signal.feature)
    .filter(Boolean)
  const focus = critical.length > 0
    ? `Prioritize ${critical.join(', ')} because ${critical.length === 1 ? 'it has' : 'they have'} crossed critical criteria`
    : warning.length > 0
      ? `Review ${warning.join(', ')} because ${warning.length === 1 ? 'it has' : 'they have'} crossed warning criteria`
      : 'Review the configured factors together'
  const base = `${focus}. Validate the source transactions, compare against recent customer/entity behavior, and decide whether the ${clusterName} pattern is expected or requires action.`
  return instruction ? `${instruction}. ${base}` : base
}

function compactClusterWording(text: string, maxChars = 280): string {
  const cleaned = String(text || '').replace(/\s+/g, ' ').trim()
  if (cleaned.length <= 100) return cleaned
  if (/primary impact driver/i.test(cleaned)) {
    const [driverPart, evidencePart = ''] = cleaned.split(/\s+Evidence:\s+/i)
    const evidence = evidencePart
      .replace(/\.$/, '')
      .split(/\s*;\s*/)
      .map((item) => item.trim())
      .filter(Boolean)
      .slice(0, 2)
      .map((item) => {
        const match = item.match(/^(.+?)\s+is\s+(.+?),\s+(above|below)\s+the\s+(.+?)\s+threshold\s+of\s+(.+)$/i)
        if (!match) return item
        return `${match[1].trim()} ${match[2].trim()} vs ${match[4].trim()} ${match[5].trim()}`
      })
    const summary = evidence.length ? `${driverPart}. Evidence: ${evidence.join('; ')}.` : driverPart
    if (summary.length <= maxChars) return summary
    const firstSentence = driverPart.split(/(?<=[.!?])\s+/)[0] || driverPart
    return evidence.length ? `${firstSentence} Evidence: ${evidence[0]}.` : firstSentence
  }
  const leadMatch = cleaned.match(/^([^:]{1,80}):\s*(.+)$/)
  if (leadMatch) {
    const lead = leadMatch[1].trim()
    const body = leadMatch[2].replace(/\bTogether,.*$/i, '').trim()
    const breaches = Array.from(body.matchAll(/([^,.]+?)\s+is\s+([^,.]+?),\s+(above|below)\s+the\s+([^,.]+?)\s+threshold\s+of\s+([^,.]+)/gi))
      .map((match) => `${match[1].trim()} ${match[3].toLowerCase()} ${match[4].trim()} (${match[2].trim()} vs ${match[5].trim()})`)
    if (breaches.length > 0) {
      const summary = `${lead}: ${breaches.slice(0, 2).join('; ')}. Review as one cluster scenario.`
      return summary.length <= maxChars ? summary : `${lead}: ${breaches.slice(0, 2).map((item) => item.replace(/\s*\([^)]*\)/g, '')).join('; ')}.`
    }
  }
  const priorityMatch = cleaned.match(/^(Prioritize|Review)\s+([^.]*)\.\s*(.*)$/i)
  if (priorityMatch) {
    const action = priorityMatch[1].toLowerCase() === 'prioritize' ? 'Prioritize' : 'Review'
    const focus = priorityMatch[2].replace(/\sbecause\b.*$/i, '').trim()
    const summary = `${action} ${focus}; validate source transactions and recent behavior.`
    return summary.length <= maxChars ? summary : `${action} ${focus}.`
  }
  const sentences = cleaned
    .split(/(?<=[.!?])\s+/)
    .map((item) => item.trim())
    .filter(Boolean)
  const picked: string[] = []
  let size = 0
  sentences.forEach((sentence) => {
    if (picked.length >= 2) return
    if (size + sentence.length > maxChars && picked.length > 0) return
    picked.push(sentence)
    size += sentence.length + 1
  })
  const summary = (picked.length > 0 ? picked.join(' ') : cleaned).trim()
  if (summary.length <= maxChars) return summary
  const words = summary.split(/\s+/).filter(Boolean)
  const compact = words.slice(0, 14).join(' ')
  return compact.length < summary.length ? `${compact}.` : summary
}

function buildLegacyCumulativeClusterObservation(clusterName: string, signals: Array<Record<string, string>>): string {
  const factors = signals.map((signal) => {
    const name = signal.business_name || signal.feature || 'factor'
    const value = signal.value ? `${signal.value}${signal.unit ? ` ${signal.unit}` : ''}` : ''
    const severity = String(signal.severity || '').toUpperCase()
    const severityText = severity === 'HIGH'
      ? 'critical'
      : severity === 'MEDIUM'
        ? 'warning'
        : 'observed'
    return `${name}${value ? ` is ${value}` : ''}${severityText ? ` (${severityText})` : ''}`
  })
  const highCount = signals.filter((signal) => signal.severity === 'HIGH').length
  const mediumCount = signals.filter((signal) => signal.severity === 'MEDIUM').length
  const driver = highCount > 0
    ? `${highCount} critical factor${highCount === 1 ? '' : 's'}`
    : mediumCount > 0
      ? `${mediumCount} warning factor${mediumCount === 1 ? '' : 's'}`
      : 'the configured factor combination'
  return `${clusterName} flagged a combined pattern across ${factors.join(' and ')}. The cluster is driven by ${driver}, so review these factors together rather than as separate exceptions.`
}

function buildLegacyCumulativeClusterRecommendation(clusterName: string, signals: Array<Record<string, string>>): string {
  const recommendations = Array.from(new Set(signals.map((signal) => String(signal.recommendation || '').trim()).filter(Boolean)))
  const factorNames = signals.map((signal) => signal.business_name || signal.feature).filter(Boolean).join(', ')
  const base = `Check whether the combined ${clusterName} pattern is expected for this customer/entity across ${factorNames || 'the selected factors'}. Validate the source transactions, compare with recent behavior, and prioritize the highest-severity factor first.`
  if (!recommendations.length) return base
  return `${base} Suggested follow-up: ${recommendations.join(' ')}`
}

function buildClusterRecommendationOutput(
  output: Record<string, unknown>,
  dictionary: RREFeatureDictionaryEntry[],
  clusters: RREClusterConfig[],
  selectedFields: string[] = defaultClusterJsonFields,
  smartWordingEnabled = false,
): string {
  const outputFields = Array.from(new Set(['cluster', ...(selectedFields.length > 0 ? selectedFields : defaultClusterJsonFields)]))
  const signalByFeature = new Map(
    dictionary
      .map((entry) => buildConfiguredClusterSignal(entry, output))
      .filter(Boolean)
      .map((signal) => [String((signal as Record<string, string>).feature || ''), signal as Record<string, string>])
  )
  if (signalByFeature.size <= 0) return ''
  const matchedClusters = clusters
    .filter((cluster) => cluster.enabled !== false)
    .sort((a, b) => Number(a.priority || 999999) - Number(b.priority || 999999))
    .map((cluster) => {
      const allSignals = cluster.features.map((field) => signalByFeature.get(field)).filter(Boolean) as Array<Record<string, string>>
      const matchedSignals = rankClusterSignals(cluster, allSignals)
      if (!matchedSignals.length) return null
      const severity = matchedSignals.some((signal) => signal.severity === 'HIGH')
        ? 'HIGH'
        : (matchedSignals.some((signal) => signal.severity === 'MEDIUM') ? 'MEDIUM' : 'CONFIGURED')
      const clusterName = cluster.name || 'Cluster'
      const configuredObservation = String(cluster.observation || '').trim()
      const configuredRecommendation = String(cluster.recommendation || '').trim()
      const summaryMaxChars = Math.max(100, Math.min(2000, Number(cluster.summary_max_chars || defaultClusterSummaryMaxChars)))
      const rawObservation = buildCumulativeClusterObservation(clusterName, matchedSignals, configuredObservation)
      const rawRecommendation = buildCumulativeClusterRecommendation(clusterName, matchedSignals, configuredRecommendation)
      const cumulativeObservation = smartWordingEnabled ? compactClusterWording(rawObservation, summaryMaxChars) : rawObservation
      const cumulativeRecommendation = smartWordingEnabled ? compactClusterWording(rawRecommendation, summaryMaxChars) : rawRecommendation
      const classified = classifyClusterSignals(matchedSignals)
      const fullCluster = {
        cluster: clusterName,
        severity,
        features: matchedSignals.map((signal) => signal.feature),
        primary_driver: classified.primary_driver,
        secondary_drivers: classified.secondary_drivers,
        impacted_outcomes: classified.impacted_outcomes,
        context_fields: classified.context_fields,
        observation: cumulativeObservation,
        recommendation: cumulativeRecommendation,
        evidence: buildClusterEvidence(matchedSignals),
      }
      return outputFields.reduce<Record<string, unknown>>((acc, field) => {
        const key = String(field || '').trim()
        if (key) acc[key] = (fullCluster as Record<string, unknown>)[key]
        return acc
      }, {})
    })
    .filter(Boolean)
  if (!matchedClusters.length) return ''
  return JSON.stringify({ clusters: matchedClusters })
}

function safeOutputKey(value: string): string {
  return String(value || 'cluster')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    || 'cluster'
}

function buildClusterRecommendationColumns(
  output: Record<string, unknown>,
  dictionary: RREFeatureDictionaryEntry[],
  clusters: RREClusterConfig[],
  selectedFields: string[] = defaultClusterJsonFields,
  smartWordingEnabled = false,
): Record<string, string> {
  const raw = buildClusterRecommendationOutput(output, dictionary, clusters, selectedFields, smartWordingEnabled)
  if (!raw) return {}
  try {
    const parsed = JSON.parse(raw)
    const rows: any[] = Array.isArray(parsed?.clusters) ? parsed.clusters : []
    return rows.reduce<Record<string, string>>((acc, cluster: any, index: number) => {
      const key = safeOutputKey(cluster?.cluster || `cluster_${index + 1}`)
      acc[`cluster_${key}`] = JSON.stringify(cluster)
      return acc
    }, {})
  } catch {
    return {}
  }
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
  const [featureDictionary, setFeatureDictionaryState] = useState<RREFeatureDictionaryEntry[]>(() => normalizeFeatureDictionary(props.featureDictionary))
  const [clusterConfig, setClusterConfigState] = useState<RREClusterConfig[]>(() => normalizeClusterConfig(props.clusterConfig))
  const [ruleConfigDraft, setRuleConfigDraft] = useState<RRERule | null>(null)
  const [templates, setTemplates] = useState<RRETemplate[]>(initialTemplates)
  const [templatesLoading, setTemplatesLoading] = useState(false)
  const [templateSaving, setTemplateSaving] = useState(false)
  const [dictionaryImportOpen, setDictionaryImportOpen] = useState(false)
  const [dictionaryImportText, setDictionaryImportText] = useState('')
  const [featureConfigDraft, setFeatureConfigDraft] = useState<{ index: number; item: RREFeatureDictionaryEntry } | null>(null)
  const [autoSignalFieldsOpen, setAutoSignalFieldsOpen] = useState(false)
  const [clusterConfigOpen, setClusterConfigOpen] = useState(false)
  const [clusterEditorOpen, setClusterEditorOpen] = useState(false)
  const [clusterJsonFieldsOpen, setClusterJsonFieldsOpen] = useState(false)
  const [clusterDraft, setClusterDraft] = useState<RREClusterConfig | null>(null)
  const [templateCursorByPlaceholder, setTemplateCursorByPlaceholder] = useState<Record<string, number>>({})
  const [templatePickerPositionByPlaceholder, setTemplatePickerPositionByPlaceholder] = useState<Record<string, { left: number; top: number }>>({})
  const [templateScrollByPlaceholder, setTemplateScrollByPlaceholder] = useState<Record<string, { left: number; top: number }>>({})
  const [autoSignalJsonFields, setAutoSignalJsonFieldsState] = useState<string[]>(() => (
    Array.isArray(props.autoSignalJsonFields) && props.autoSignalJsonFields.length > 0 ? props.autoSignalJsonFields : defaultAutoSignalJsonFields
  ))
  const [clusterJsonFields, setClusterJsonFieldsState] = useState<string[]>(() => (
    Array.isArray(props.clusterJsonFields) && props.clusterJsonFields.length > 0 ? props.clusterJsonFields : defaultClusterJsonFields
  ))
  const [includeAutoClusterRecommendation, setIncludeAutoClusterRecommendationState] = useState<boolean>(() => props.includeAutoClusterRecommendation !== false)
  const [clusterTransformersEnabled, setClusterTransformersEnabledState] = useState<boolean>(() => props.clusterTransformersEnabled === true)
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
  const externalDictionarySignature = useMemo(() => JSON.stringify(props.featureDictionary || null), [props.featureDictionary])
  const externalAutoSignalFieldsSignature = useMemo(() => JSON.stringify(props.autoSignalJsonFields || null), [props.autoSignalJsonFields])
  const externalClusterSignature = useMemo(() => JSON.stringify(props.clusterConfig || null), [props.clusterConfig])
  const externalClusterJsonFieldsSignature = useMemo(() => JSON.stringify(props.clusterJsonFields || null), [props.clusterJsonFields])
  const externalIncludeAutoClusterRecommendationSignature = useMemo(() => JSON.stringify(props.includeAutoClusterRecommendation ?? true), [props.includeAutoClusterRecommendation])
  const externalClusterTransformersSignature = useMemo(() => JSON.stringify(props.clusterTransformersEnabled === true), [props.clusterTransformersEnabled])
  const dictionaryLocalSignature = useMemo(() => JSON.stringify(featureDictionary), [featureDictionary])

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
    if (props.featureDictionary) setFeatureDictionaryState(normalizeFeatureDictionary(props.featureDictionary))
  }, [externalDictionarySignature])

  useEffect(() => {
    if (props.autoSignalJsonFields) {
      setAutoSignalJsonFieldsState(props.autoSignalJsonFields.length > 0 ? props.autoSignalJsonFields : defaultAutoSignalJsonFields)
    }
  }, [externalAutoSignalFieldsSignature])

  useEffect(() => {
    if (props.clusterJsonFields) {
      setClusterJsonFieldsState(props.clusterJsonFields.length > 0 ? props.clusterJsonFields : defaultClusterJsonFields)
    }
  }, [externalClusterJsonFieldsSignature])

  useEffect(() => {
    setIncludeAutoClusterRecommendationState(props.includeAutoClusterRecommendation !== false)
  }, [externalIncludeAutoClusterRecommendationSignature])

  useEffect(() => {
    setClusterTransformersEnabledState(props.clusterTransformersEnabled === true)
  }, [externalClusterTransformersSignature])

  useEffect(() => {
    if (props.clusterConfig) setClusterConfigState(normalizeClusterConfig(props.clusterConfig))
  }, [externalClusterSignature])

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

  const outputFields = useMemo(() => Array.from(new Set(parsedOutput.row ? collectOutputFields(parsedOutput.row) : [])), [parsedOutput.row])
  const sourceFields = outputFields.filter((field) => field.startsWith('source.'))
  const predictionFields = outputFields.filter((field) => field.startsWith('predictions.'))
  const otherOutputFields = outputFields.filter((field) => !field.startsWith('source.') && !field.startsWith('predictions.'))
  const fieldOptions = useMemo(() => uniqueOptions([
    ...sourceFields.map((field) => ({ label: `${field.replace(/^source\./, '')} (${field})`, value: field, group: 'Source Fields' })),
    ...predictionFields.map((field) => ({ label: `${field.replace(/^predictions\./, '')} (${field})`, value: field, group: 'Prediction Fields' })),
    ...otherOutputFields.map((field) => ({ label: field, value: field, group: 'Other Output Fields' })),
  ]), [sourceFields, predictionFields, otherOutputFields])
  const signalFeatureOptions = useMemo(() => uniqueOptions([
    ...featureDictionary.map((item) => ({ label: item.business_name ? `${item.business_name} (${item.field})` : item.field, value: item.field })),
    ...fieldOptions,
  ]), [featureDictionary, fieldOptions])
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

  const commitFeatureDictionary = (nextItems: RREFeatureDictionaryEntry[] | ((current: RREFeatureDictionaryEntry[]) => RREFeatureDictionaryEntry[])) => {
    setFeatureDictionaryState((current) => {
      const next = typeof nextItems === 'function' ? nextItems(current) : nextItems
      props.onFeatureDictionaryChange?.(next)
      return next
    })
  }

  const commitAutoSignalJsonFields = (fields: string[]) => {
    const next = fields.map((item) => String(item || '').trim()).filter(Boolean)
    setAutoSignalJsonFieldsState(next)
    props.onAutoSignalJsonFieldsChange?.(next)
  }

  const commitClusterJsonFields = (fields: string[]) => {
    const next = fields.map((item) => String(item || '').trim()).filter(Boolean)
    setClusterJsonFieldsState(next.length > 0 ? next : defaultClusterJsonFields)
    props.onClusterJsonFieldsChange?.(next.length > 0 ? next : defaultClusterJsonFields)
  }

  const commitIncludeAutoClusterRecommendation = (enabled: boolean) => {
    setIncludeAutoClusterRecommendationState(enabled)
    props.onIncludeAutoClusterRecommendationChange?.(enabled)
  }

  const commitClusterTransformersEnabled = (enabled: boolean) => {
    setClusterTransformersEnabledState(enabled)
    props.onClusterTransformersEnabledChange?.(enabled)
  }

  const commitClusterConfig = (nextItems: RREClusterConfig[] | ((current: RREClusterConfig[]) => RREClusterConfig[])) => {
    setClusterConfigState((current) => {
      const next = typeof nextItems === 'function' ? nextItems(current) : nextItems
      props.onClusterConfigChange?.(next)
      return next
    })
  }

  const saveClusterDraft = () => {
    if (!clusterDraft) return
    commitClusterConfig((current) => {
      const exists = current.some((item) => item.id === clusterDraft.id)
      return exists ? current.map((item) => item.id === clusterDraft.id ? clusterDraft : item) : [...current, clusterDraft]
    })
    setClusterDraft(null)
    setClusterEditorOpen(false)
  }

  const openClusterConfig = () => {
    setClusterConfigOpen(true)
  }

  const mergeFeatureDictionary = (incoming: RREFeatureDictionaryEntry[]) => {
    if (!incoming.length) return
    commitFeatureDictionary((current) => {
      const byField = new Map<string, RREFeatureDictionaryEntry>()
      current.forEach((item) => {
        if (item.field) byField.set(item.field, item)
      })
      incoming.forEach((item) => {
        if (!item.field) return
        byField.set(item.field, { ...(byField.get(item.field) || inferFeatureMetadata(item.field)), ...item })
      })
      return Array.from(byField.values())
    })
  }

  const generateMissingFeatureDictionary = () => {
    const existing = new Set(featureDictionary.map((item) => item.field).filter(Boolean))
    const incoming = outputFields.filter((field) => !existing.has(field)).map(inferFeatureMetadata)
    if (!incoming.length) {
      notification.info({ message: 'Feature dictionary already has all output fields.' })
      return
    }
    mergeFeatureDictionary(incoming)
    notification.success({ message: `Added ${incoming.length} feature dictionary rows.` })
  }

  const applyDictionaryImport = () => {
    const imported = parseDictionaryImport(dictionaryImportText)
    if (!imported.length) {
      notification.warning({ message: 'No valid feature dictionary rows found.' })
      return
    }
    mergeFeatureDictionary(imported)
    setDictionaryImportText('')
    setDictionaryImportOpen(false)
    notification.success({ message: `Imported ${imported.length} feature dictionary rows.` })
  }

  const saveFeatureConfigDraft = () => {
    if (!featureConfigDraft) return
    commitFeatureDictionary((current) => current.map((item, index) => (
      index === featureConfigDraft.index ? featureConfigDraft.item : item
    )))
    setFeatureConfigDraft(null)
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
      explanation: renderTemplate(template, selectedRule, parsedOutput.row, featureDictionary),
    }
  }, [parsedOutput.row, rules, allTemplates, validationErrors.length, featureDictionary])

  const recommendationOutput = useMemo(() => {
    const out: Record<string, string> = {}
    rules.forEach((rule) => {
      const column = ruleOutputColumn(rule)
      if (!column) return
      const matched = evaluation.matched.some((item) => item.id === rule.id)
      const template = allTemplates.find((item) => item.id === rule.templateId)
      out[column] = matched && parsedOutput.row ? renderTemplate(template, rule, parsedOutput.row, featureDictionary) : ''
      const signalColumn = signalOutputColumn(rule)
      if (signalColumn) {
        out[signalColumn] = matched && parsedOutput.row && normalizeSignalConfig(rule.signalConfig).enabled
          ? buildSignalJsonOutput(rule, parsedOutput.row, featureDictionary)
          : ''
      }
    })
    if (parsedOutput.row) out.Auto_Generate_XAI_signal = buildSelectedAutoSignalJsonOutput(parsedOutput.row, featureDictionary, autoSignalJsonFields)
    if (parsedOutput.row) {
      if (includeAutoClusterRecommendation) {
        out.Auto_Cluster_Recommendation = buildClusterRecommendationOutput(parsedOutput.row, featureDictionary, clusterConfig, clusterJsonFields, clusterTransformersEnabled)
      }
      Object.assign(out, buildClusterRecommendationColumns(parsedOutput.row, featureDictionary, clusterConfig, clusterJsonFields, clusterTransformersEnabled))
    }
    return out
  }, [evaluation.matched, parsedOutput.row, rules, allTemplates, featureDictionary, autoSignalJsonFields, clusterConfig, clusterJsonFields, includeAutoClusterRecommendation, clusterTransformersEnabled])

  const addRule = () => {
    const id = `rule-${Date.now()}`
    const nextRule: RRERule = {
        id,
        name: 'New Rule',
        enabled: true,
        smartWordingEnabled: false,
        smartWordingMaxChars: defaultRuleSmartWordingMaxChars,
        priority: rules.length + 1,
        rootGroup: newGroup({ join: 'and', conditions: [newCondition({ field: predictionFields[0] || outputFields[0] || 'predictions.ensemble_prediction', operator: 'exists', value: '' })] }),
        templateId: templates[0]?.id || '',
        signalConfig: {
          enabled: true,
          smartWordingEnabled: false,
          feature: predictionFields[0]?.replace(/^predictions\./, '') || outputFields[0] || '',
          severity: 'MEDIUM',
          valueField: predictionFields[0] || outputFields[0] || '',
          peerValueField: '',
          impactSource: 'custom',
          impactField: '',
          impactValue: '',
          recommendation: '',
          jsonFields: defaultSignalJsonFields,
        },
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
      smartWordingMaxChars: normalizeRuleSmartWordingMaxChars(ruleConfigDraft.smartWordingMaxChars),
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
                        placeholder="Use {{summary}}, {{signal_observations}}, {{signal_recommendations}}, {{top_signal}}, {{risk_band}}, plus mapped fields like {{prediction}}"
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
          {
            key: 'dictionary',
            label: 'Feature Dictionary',
            children: (
              <Space direction="vertical" size={12} style={{ width: '100%' }}>
              <Card
                size="small"
                title="Feature Dictionary"
                extra={(
                  <Space wrap>
                    <Button size="small" onClick={generateMissingFeatureDictionary}>Generate Missing</Button>
                    <Button size="small" onClick={() => setAutoSignalFieldsOpen(true)}>Auto Signal JSON Fields</Button>
                    <Button size="small" onClick={openClusterConfig}>Cluster Config</Button>
                    <Button size="small" icon={<FileTextOutlined />} onClick={() => setDictionaryImportOpen(true)}>Paste Import</Button>
                    <Button size="small" icon={<PlusOutlined />} onClick={() => commitFeatureDictionary((current) => [...current, inferFeatureMetadata(outputFields[0] || '')])}>Feature</Button>
                  </Space>
                )}
                style={{ background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)' }}
              >
                <Alert
                  type="info"
                  showIcon
                  style={{ marginBottom: 10 }}
                  message="Use Generate Missing to create rows. Enable Auto Signal and set warning/critical thresholds to generate Auto_Generate_XAI_signal without manual rules."
                />
                <Table
                  size="small"
                  rowKey={(_row, index) => `feature_${index}`}
                  pagination={{ pageSize: 12, size: 'small' }}
                  dataSource={featureDictionary}
                  columns={[
                    {
                      title: 'Field',
                      dataIndex: 'field',
                      width: 260,
                      ellipsis: true,
                      render: (field) => <Text style={{ color: 'var(--app-text)', fontFamily: 'monospace', fontSize: 12 }} ellipsis>{String(field || '')}</Text>,
                    },
                    {
                      title: 'Business Name',
                      dataIndex: 'business_name',
                      width: 180,
                      ellipsis: true,
                    },
                    {
                      title: 'Direction',
                      width: 150,
                      render: (_value, row) => directionOptions.find((item) => item.value === row.direction)?.label || row.direction || '',
                    },
                    {
                      title: 'Unit',
                      dataIndex: 'unit',
                      width: 90,
                    },
                    {
                      title: 'Impact Role',
                      width: 120,
                      render: (_value, row) => <Tag color={row.impact_role === 'driver' ? 'blue' : row.impact_role === 'outcome' ? 'purple' : 'default'}>{impactRoleOptions.find((item) => item.value === row.impact_role)?.label || row.impact_role || 'Driver'}</Tag>,
                    },
                    {
                      title: 'Weight',
                      dataIndex: 'impact_weight',
                      width: 80,
                    },
                    {
                      title: 'Auto Signal',
                      width: 110,
                      render: (_value, row) => <Tag color={row.auto_signal_enabled ? 'blue' : 'default'}>{row.auto_signal_enabled ? 'Enabled' : 'Off'}</Tag>,
                    },
                    {
                      title: 'Warning',
                      dataIndex: 'warning_threshold',
                      width: 110,
                    },
                    {
                      title: 'Critical',
                      dataIndex: 'critical_threshold',
                      width: 110,
                    },
                    {
                      title: 'Default Recommendation',
                      dataIndex: 'default_recommendation',
                      ellipsis: true,
                    },
                    {
                      title: 'Action',
                      width: 150,
                      render: (_value, row, index) => (
                        <Space>
                          <Button size="small" onClick={() => setFeatureConfigDraft({ index: Number(index ?? -1), item: { ...row } })}>Configure</Button>
                          <Button size="small" danger icon={<DeleteOutlined />} onClick={() => commitFeatureDictionary((current) => current.filter((_item, idx) => idx !== index))} />
                        </Space>
                      ),
                    },
                  ]}
                  locale={{ emptyText: 'Create feature metadata to generate dynamic XAI observations.' }}
                />
              </Card>
              <Card
                size="small"
                title="Sample Output"
                style={{ background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)' }}
              >
                <div style={{ display: 'grid', gridTemplateColumns: 'minmax(320px, 0.9fr) minmax(420px, 1.1fr)', gap: 12 }}>
                  <JsonViewerCard title="Sample Input Row" value={parsedOutput.row || {}} />
                  <JsonViewerCard title="Recommendation Output" value={recommendationOutput} />
                </div>
              </Card>
              </Space>
            ),
          },
        ]}
      />
      <Modal
        open={dictionaryImportOpen}
        title="Import Feature Dictionary"
        okText="Import"
        onOk={applyDictionaryImport}
        onCancel={() => setDictionaryImportOpen(false)}
        width={820}
      >
        <Space direction="vertical" size={10} style={{ width: '100%' }}>
          <Alert
            type="info"
            showIcon
            message="Paste JSON array/object or CSV with headers: field,business_name,meaning,unit,direction,impact_role,impact_weight,default_recommendation."
          />
          <Input.TextArea
            value={dictionaryImportText}
            onChange={(event) => setDictionaryImportText(event.target.value)}
            rows={14}
            placeholder={`field,business_name,meaning,unit,direction,impact_role,impact_weight,default_recommendation
predictions.risk_score,Risk Score,elevated model risk signal requiring operational review,score,higher_is_risky,driver,8,Review high-risk cases and validate the rule evidence.`}
            style={{ fontFamily: 'monospace' }}
          />
        </Space>
      </Modal>
      <Modal
        open={autoSignalFieldsOpen}
        title="Auto Signal JSON Fields"
        okText="Done"
        onOk={() => setAutoSignalFieldsOpen(false)}
        onCancel={() => setAutoSignalFieldsOpen(false)}
        width={760}
      >
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          <Alert
            type="info"
            showIcon
            message="Select the fields to include inside each Auto_Generate_XAI_signal JSON signal. This is a global setting for all Feature Dictionary auto signals."
          />
          <Checkbox.Group
            value={autoSignalJsonFields}
            options={[
              { label: 'Feature', value: 'feature' },
              { label: 'Business Name', value: 'business_name' },
              { label: 'Severity', value: 'severity' },
              { label: 'Value', value: 'value' },
              { label: 'Threshold', value: 'threshold' },
              { label: 'Threshold Type', value: 'threshold_type' },
              { label: 'Direction', value: 'direction' },
              { label: 'Unit', value: 'unit' },
              { label: 'Meaning', value: 'meaning' },
              { label: 'Observation', value: 'observation' },
              { label: 'Recommendation', value: 'recommendation' },
            ]}
            onChange={(fields) => commitAutoSignalJsonFields(fields.map((item) => String(item)))}
            style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(170px, 1fr))', gap: '10px 14px' }}
          />
          <Input.TextArea
            readOnly
            rows={5}
            value={JSON.stringify({ Auto_Generate_XAI_signal: { selected_fields: autoSignalJsonFields } }, null, 2)}
            style={{ fontFamily: 'monospace', background: 'var(--app-card-bg)', color: 'var(--app-text)' }}
          />
        </Space>
      </Modal>
      <Modal
        open={clusterConfigOpen}
        title="Auto Signal Cluster Config"
        footer={null}
        onCancel={() => {
          setClusterConfigOpen(false)
          setClusterDraft(null)
          setClusterEditorOpen(false)
        }}
        width="100vw"
        style={{ top: 0, margin: 0, paddingBottom: 0, maxWidth: '100vw' }}
        styles={{
          content: {
            height: '100vh',
            borderRadius: 0,
            display: 'flex',
            flexDirection: 'column',
            background: 'var(--app-panel-bg)',
          },
          body: {
            flex: 1,
            minHeight: 0,
            overflow: 'auto',
          },
        }}
      >
        <Space direction="vertical" size={10} style={{ width: '100%', minHeight: 'calc(100vh - 130px)' }}>
          <Space style={{ width: '100%', justifyContent: 'space-between' }} wrap>
            <Space size={8} wrap>
              <Tag color="blue" style={{ marginInlineEnd: 0 }}>clusters: {clusterConfig.length}</Tag>
              <Tag style={{ marginInlineEnd: 0 }}>json fields: {clusterJsonFields.length}</Tag>
              <Tag color={includeAutoClusterRecommendation ? 'green' : 'default'} style={{ marginInlineEnd: 0 }}>
                Auto_Cluster_Recommendation: {includeAutoClusterRecommendation ? 'on' : 'off'}
              </Tag>
              <Tag color={clusterTransformersEnabled ? 'purple' : 'default'} style={{ marginInlineEnd: 0 }}>
                smart wording: {clusterTransformersEnabled ? 'on' : 'off'}
              </Tag>
            </Space>
            <Space size={8} wrap>
              <Space size={6}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Aggregate column</Text>
                <Switch size="small" checked={includeAutoClusterRecommendation} onChange={commitIncludeAutoClusterRecommendation} />
              </Space>
              <Space size={6}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Smart wording</Text>
                <Switch size="small" checked={clusterTransformersEnabled} onChange={commitClusterTransformersEnabled} />
              </Space>
              <Button size="small" icon={<SettingOutlined />} onClick={() => setClusterJsonFieldsOpen(true)}>
                JSON Output Fields
              </Button>
              <Button size="small" icon={<PlusOutlined />} onClick={() => {
                setClusterDraft({
                  id: `cluster-${Date.now()}`,
                  name: 'Transaction Risk Cluster',
                  enabled: true,
                  features: [],
                  observation: '',
                  recommendation: '',
                  priority: clusterConfig.length + 1,
                  summary_max_chars: defaultClusterSummaryMaxChars,
                  feature_filter: 'breached_only',
                  rank_by: 'intelligent_impact',
                  feature_limit: 3,
                })
                setClusterEditorOpen(true)
              }}>
                New Cluster
              </Button>
            </Space>
          </Space>
          <Table
            size="small"
            rowKey="id"
            pagination={{ pageSize: 8, size: 'small' }}
            dataSource={clusterConfig}
            columns={[
              { title: 'Cluster', dataIndex: 'name', width: 220, ellipsis: true },
              { title: 'Enabled', width: 90, render: (_value, row) => <Tag color={row.enabled ? 'blue' : 'default'}>{row.enabled ? 'Enabled' : 'Off'}</Tag> },
              { title: 'Features', width: 220, render: (_value, row) => <Text ellipsis>{row.features.join(', ')}</Text> },
              { title: 'Ranking', width: 150, render: (_value, row) => <Text ellipsis>{`${row.rank_by || 'balanced_score'} / ${row.feature_filter || 'breached_only'}`}</Text> },
              { title: 'Limit', dataIndex: 'feature_limit', width: 80 },
              { title: 'Observation', dataIndex: 'observation', width: 320, ellipsis: true },
              { title: 'Recommendation', dataIndex: 'recommendation', width: 320, ellipsis: true },
              { title: 'Summary Chars', dataIndex: 'summary_max_chars', width: 110 },
              { title: 'Priority', dataIndex: 'priority', width: 80 },
              {
                title: 'Action',
                width: 145,
                render: (_value, row) => (
                  <Space>
                    <Button size="small" onClick={() => {
                      setClusterDraft({ ...row })
                      setClusterEditorOpen(true)
                    }}>Configure</Button>
                    <Button size="small" danger icon={<DeleteOutlined />} onClick={() => commitClusterConfig((current) => current.filter((item) => item.id !== row.id))} />
                  </Space>
                ),
              },
            ]}
            locale={{ emptyText: 'Create clusters to group related auto signals into high-level recommendations.' }}
            scroll={{ x: 1240, y: 'calc(100vh - 245px)' }}
          />
        </Space>
      </Modal>
      <Modal
        open={clusterEditorOpen}
        title={clusterDraft ? 'Configure Cluster' : 'New Cluster'}
        okText="Save Cluster"
        onOk={saveClusterDraft}
        onCancel={() => {
          setClusterEditorOpen(false)
          setClusterDraft(null)
        }}
        width={760}
        centered
        zIndex={2600}
        styles={{
          content: {
            borderRadius: 8,
            background: 'var(--app-panel-bg)',
            border: '1px solid var(--app-border-strong)',
          },
          body: {
            maxHeight: '72vh',
            overflowY: 'auto',
          },
        }}
      >
        {clusterDraft ? (
          <Form layout="vertical" size="small">
            <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) minmax(260px, 0.75fr)', gap: 12, alignItems: 'start' }}>
              <div>
                <Form.Item label="Cluster Name">
                  <Input value={clusterDraft.name} onChange={(event) => setClusterDraft((current) => current ? { ...current, name: event.target.value } : current)} />
                </Form.Item>
                <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 8 }}>
                  <Text style={{ color: 'var(--app-text)' }}>Enabled</Text>
                  <Switch checked={clusterDraft.enabled} onChange={(enabled) => setClusterDraft((current) => current ? { ...current, enabled } : current)} />
                </Space>
                <Form.Item label="Priority">
                  <InputNumber min={1} value={clusterDraft.priority} onChange={(priority) => setClusterDraft((current) => current ? { ...current, priority: Number(priority || 1) } : current)} style={{ width: '100%' }} />
                </Form.Item>
                <Form.Item label="Smart Wording Character Limit">
                  <InputNumber
                    min={100}
                    max={2000}
                    step={20}
                    value={clusterDraft.summary_max_chars || defaultClusterSummaryMaxChars}
                    onChange={(summaryMaxChars) => setClusterDraft((current) => current ? {
                      ...current,
                      summary_max_chars: Math.max(100, Math.min(2000, Number(summaryMaxChars || defaultClusterSummaryMaxChars))),
                    } : current)}
                    style={{ width: '100%' }}
                  />
                </Form.Item>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 130px', gap: 8 }}>
                  <Form.Item label="Feature Inclusion">
                    <Select
                      value={clusterDraft.feature_filter || 'breached_only'}
                      options={[
                        { value: 'breached_only', label: 'Breached only' },
                        { value: 'all_selected', label: 'All selected' },
                      ]}
                      onChange={(feature_filter) => setClusterDraft((current) => current ? { ...current, feature_filter } : current)}
                    />
                  </Form.Item>
                  <Form.Item label="Rank Features By">
                    <Select
                      value={clusterDraft.rank_by || 'balanced_score'}
                      options={[
        { value: 'balanced_score', label: 'Balanced Score' },
        { value: 'intelligent_impact', label: 'Intelligent Impact' },
                        { value: 'severity', label: 'Severity' },
                        { value: 'breach_percent', label: 'Breach %' },
                        { value: 'absolute_breach', label: 'Breach amount' },
                        { value: 'value', label: 'Observed value' },
                        { value: 'configured_order', label: 'Selected order' },
                      ]}
                      onChange={(rank_by) => setClusterDraft((current) => current ? { ...current, rank_by } : current)}
                    />
                  </Form.Item>
                  <Form.Item label="Max Features">
                    <InputNumber
                      min={0}
                      max={100}
                      value={clusterDraft.feature_limit ?? 3}
                      onChange={(featureLimit) => setClusterDraft((current) => current ? {
                        ...current,
                        feature_limit: Math.max(0, Math.min(100, Number(featureLimit || 0))),
                      } : current)}
                      style={{ width: '100%' }}
                    />
                  </Form.Item>
                </div>
                <Form.Item label="Features">
                  <Select
                    mode="multiple"
                    showSearch
                    optionFilterProp="label"
                    value={clusterDraft.features}
                    options={featureDictionary.map((item) => ({ label: item.business_name ? `${item.business_name} (${item.field})` : item.field, value: item.field }))}
                    onChange={(features) => setClusterDraft((current) => current ? { ...current, features: features.map((item) => String(item)) } : current)}
                  />
                </Form.Item>
                <Form.Item label="Cluster Observation">
                  <Input.TextArea rows={3} value={clusterDraft.observation} onChange={(event) => setClusterDraft((current) => current ? { ...current, observation: event.target.value } : current)} />
                </Form.Item>
                <Form.Item label="Cluster Recommendation">
                  <Input.TextArea rows={3} value={clusterDraft.recommendation} onChange={(event) => setClusterDraft((current) => current ? { ...current, recommendation: event.target.value } : current)} />
                </Form.Item>
              </div>
              <div style={{ display: 'grid', gap: 10 }}>
                <Card size="small" title="Cluster JSON Fields" style={{ background: 'var(--app-card-bg)', borderColor: 'var(--app-border-strong)' }}>
                  <Checkbox.Group
                    value={clusterJsonFields}
                    options={clusterJsonFieldOptions}
                    onChange={(fields) => commitClusterJsonFields(fields.map((item) => String(item)))}
                    style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 8 }}
                  />
                </Card>
                {parsedOutput.row ? (
                  <Input.TextArea
                    readOnly
                    rows={8}
                    value={JSON.stringify({
                      ...(includeAutoClusterRecommendation ? {
                        Auto_Cluster_Recommendation: buildClusterRecommendationOutput(parsedOutput.row, featureDictionary, [clusterDraft], clusterJsonFields, clusterTransformersEnabled),
                      } : {}),
                      ...buildClusterRecommendationColumns(parsedOutput.row, featureDictionary, [clusterDraft], clusterJsonFields, clusterTransformersEnabled),
                    }, null, 2)}
                    style={{ fontFamily: 'monospace', background: 'var(--app-input-bg)', color: 'var(--app-text)' }}
                  />
                ) : null}
              </div>
            </div>
          </Form>
        ) : (
          <Text style={{ color: 'var(--app-text-subtle)' }}>Select a cluster or create a new one.</Text>
        )}
      </Modal>
      <Modal
        open={clusterJsonFieldsOpen}
        title="Cluster JSON Output Fields"
        okText="Done"
        onOk={() => setClusterJsonFieldsOpen(false)}
        onCancel={() => setClusterJsonFieldsOpen(false)}
        width={640}
        centered
      >
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          <Alert
            type="info"
            showIcon
            message="Choose whether to emit the aggregate Auto_Cluster_Recommendation column, and select the high-level fields for enabled cluster JSON outputs."
          />
          <Space style={{ width: '100%', justifyContent: 'space-between' }}>
            <Text style={{ color: 'var(--app-text)' }}>Emit Auto_Cluster_Recommendation column</Text>
            <Switch checked={includeAutoClusterRecommendation} onChange={commitIncludeAutoClusterRecommendation} />
          </Space>
          <Space style={{ width: '100%', justifyContent: 'space-between' }}>
            <div>
              <Text style={{ color: 'var(--app-text)' }}>Use transformer smart wording for long cluster text</Text>
              <br />
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                Uses optional backend transformers summarization when installed; otherwise falls back to compact rule-based shortening.
              </Text>
            </div>
            <Switch checked={clusterTransformersEnabled} onChange={commitClusterTransformersEnabled} />
          </Space>
          <Checkbox.Group
            value={clusterJsonFields}
            options={clusterJsonFieldOptions}
            onChange={(fields) => commitClusterJsonFields(fields.map((item) => String(item)))}
            style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(180px, 1fr))', gap: '10px 14px' }}
          />
          <Input.TextArea
            readOnly
            rows={5}
            value={JSON.stringify({
              Auto_Cluster_Recommendation: {
                enabled: includeAutoClusterRecommendation,
                selected_fields: includeAutoClusterRecommendation ? clusterJsonFields : [],
              },
              smart_wording: {
                transformers_enabled: clusterTransformersEnabled,
              },
              cluster_columns: {
                enabled: true,
                selected_fields: clusterJsonFields,
              },
            }, null, 2)}
            style={{ fontFamily: 'monospace', background: 'var(--app-card-bg)', color: 'var(--app-text)' }}
          />
        </Space>
      </Modal>
      <Modal
        open={Boolean(featureConfigDraft)}
        title="Configure Feature Dictionary Record"
        width={980}
        centered
        onCancel={() => setFeatureConfigDraft(null)}
        onOk={saveFeatureConfigDraft}
        okText="Save Feature"
        styles={{
          body: {
            paddingTop: 12,
            maxHeight: '72vh',
            overflowY: 'auto',
          },
        }}
      >
        {featureConfigDraft ? (
          <Form layout="vertical" size="small">
            <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) 300px', gap: 14, alignItems: 'start' }}>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(220px, 1fr))', gap: '8px 12px' }}>
                <Form.Item label="Field" style={{ marginBottom: 8 }}>
                  <Select
                    showSearch
                    allowClear
                    optionFilterProp="label"
                    value={featureConfigDraft.item.field || undefined}
                    options={fieldOptions}
                    onChange={(field) => setFeatureConfigDraft((current) => current ? { ...current, item: { ...current.item, field: String(field || '') } } : current)}
                    style={{ width: '100%' }}
                  />
                </Form.Item>
                <Form.Item label="Business Name" style={{ marginBottom: 8 }}>
                  <Input
                    value={featureConfigDraft.item.business_name}
                    placeholder={titleCase(featureConfigDraft.item.field)}
                    onChange={(event) => setFeatureConfigDraft((current) => current ? { ...current, item: { ...current.item, business_name: event.target.value } } : current)}
                  />
                </Form.Item>
                <Form.Item label="Direction" style={{ marginBottom: 8 }}>
                  <Select
                    value={featureConfigDraft.item.direction || 'higher_is_risky'}
                    options={directionOptions}
                    onChange={(direction) => setFeatureConfigDraft((current) => current ? { ...current, item: { ...current.item, direction } } : current)}
                  />
                </Form.Item>
                <Form.Item label="Unit" style={{ marginBottom: 8 }}>
                  <Input
                    value={featureConfigDraft.item.unit}
                    onChange={(event) => setFeatureConfigDraft((current) => current ? { ...current, item: { ...current.item, unit: event.target.value } } : current)}
                  />
                </Form.Item>
                <Form.Item label="Impact Role" style={{ marginBottom: 8 }}>
                  <Select
                    value={featureConfigDraft.item.impact_role || 'driver'}
                    options={impactRoleOptions}
                    onChange={(impact_role) => setFeatureConfigDraft((current) => current ? { ...current, item: { ...current.item, impact_role } } : current)}
                  />
                </Form.Item>
                <Form.Item label="Impact Weight" style={{ marginBottom: 8 }}>
                  <InputNumber
                    min={0}
                    max={10}
                    step={1}
                    value={featureConfigDraft.item.impact_weight ?? 5}
                    onChange={(impactWeight) => setFeatureConfigDraft((current) => current ? {
                      ...current,
                      item: { ...current.item, impact_weight: Math.max(0, Math.min(10, Number(impactWeight ?? 0))) },
                    } : current)}
                    style={{ width: '100%' }}
                  />
                </Form.Item>
                <Form.Item label="Warning Threshold (MEDIUM)" style={{ marginBottom: 8 }}>
                  <Input
                    value={featureConfigDraft.item.warning_threshold}
                    onChange={(event) => setFeatureConfigDraft((current) => current ? { ...current, item: { ...current.item, warning_threshold: event.target.value } } : current)}
                  />
                </Form.Item>
                <Form.Item label="Critical Threshold (HIGH)" style={{ marginBottom: 8 }}>
                  <Input
                    value={featureConfigDraft.item.critical_threshold}
                    onChange={(event) => setFeatureConfigDraft((current) => current ? { ...current, item: { ...current.item, critical_threshold: event.target.value } } : current)}
                  />
                </Form.Item>
                <Form.Item label="Meaning" style={{ gridColumn: '1 / -1', marginBottom: 8 }}>
                  <Input.TextArea
                    rows={3}
                    value={featureConfigDraft.item.meaning}
                    onChange={(event) => setFeatureConfigDraft((current) => current ? { ...current, item: { ...current.item, meaning: event.target.value } } : current)}
                  />
                </Form.Item>
                <Form.Item label="Default Recommendation" style={{ gridColumn: '1 / -1', marginBottom: 0 }}>
                  <Input.TextArea
                    rows={2}
                    value={featureConfigDraft.item.default_recommendation}
                    onChange={(event) => setFeatureConfigDraft((current) => current ? { ...current, item: { ...current.item, default_recommendation: event.target.value } } : current)}
                  />
                </Form.Item>
              </div>
              <div style={{ border: '1px solid var(--app-border)', borderRadius: 8, padding: 10, background: 'var(--app-card-bg)' }}>
                <Space direction="vertical" size={10} style={{ width: '100%' }}>
                  <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                    <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>Auto Signal</Text>
                    <Switch
                      checked={Boolean(featureConfigDraft.item.auto_signal_enabled)}
                      onChange={(auto_signal_enabled) => setFeatureConfigDraft((current) => current ? { ...current, item: { ...current.item, auto_signal_enabled } } : current)}
                    />
                  </Space>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                    Uses direction and thresholds to populate Auto_Generate_XAI_signal.
                  </Text>
                  {parsedOutput.row ? (
                    <Input.TextArea
                      readOnly
                      rows={9}
                      value={JSON.stringify(buildAutoSignalPreview(featureConfigDraft.item, parsedOutput.row), null, 2)}
                      style={{ fontFamily: 'monospace', background: 'var(--app-input-bg)', color: 'var(--app-text)' }}
                    />
                  ) : (
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Load latest output to preview auto signal.</Text>
                  )}
                </Space>
              </div>
            </div>
          </Form>
        ) : null}
      </Modal>
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
            <Space style={{ width: '100%', justifyContent: 'space-between' }} wrap align="center">
              <Space wrap align="center">
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
              <Space size={8} wrap align="center" style={{ justifyContent: 'flex-end' }}>
                <Switch
                  checked={Boolean(ruleConfigDraft.smartWordingEnabled)}
                  onChange={(smartWordingEnabled) => setRuleConfigDraft((current) => current ? { ...current, smartWordingEnabled } : current)}
                />
                <Text style={{ color: 'var(--app-text)' }}>Smart wording</Text>
                <Text style={{ color: 'var(--app-text-subtle)' }}>Max chars</Text>
                <InputNumber
                  min={minRuleSmartWordingMaxChars}
                  max={maxRuleSmartWordingMaxChars}
                  value={ruleSmartWordingMaxChars(ruleConfigDraft)}
                  disabled={!ruleConfigDraft.smartWordingEnabled}
                  onChange={(smartWordingMaxChars) => setRuleConfigDraft((current) => current ? {
                    ...current,
                    smartWordingMaxChars: normalizeRuleSmartWordingMaxChars(smartWordingMaxChars),
                  } : current)}
                  style={{ width: 110 }}
                />
              </Space>
            </Space>

            <RuleConditionGroupEditor
              group={ruleConfigDraft.rootGroup}
              depth={0}
              fieldOptions={fieldOptions}
              onChange={(rootGroup) => setRuleConfigDraft((current) => current ? { ...current, rootGroup } : current)}
            />

            <Collapse
              size="small"
              bordered
              defaultActiveKey={[]}
              style={{ background: 'var(--app-input-bg)', borderColor: 'var(--app-border-strong)' }}
              items={[
                {
                  key: 'signal-output',
                  label: <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Signal Output</Text>,
                  children: (
                    <>
              <div style={{ display: 'grid', gridTemplateColumns: 'minmax(180px, 240px) repeat(3, minmax(190px, 1fr))', gap: 10, alignItems: 'end' }}>
                <Space style={{ minHeight: 32 }}>
                  <Switch
                    checked={ruleConfigDraft.signalConfig?.enabled !== false}
                    onChange={(enabled) => setRuleConfigDraft((current) => current ? { ...current, signalConfig: { ...normalizeSignalConfig(current.signalConfig), enabled } } : current)}
                  />
                  <Text style={{ color: 'var(--app-text)' }}>Generate XAI signal</Text>
                </Space>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Signal feature</Text>
                  <Select
                    showSearch
                    allowClear
                    optionFilterProp="label"
                    value={ruleConfigDraft.signalConfig?.feature || undefined}
                    options={signalFeatureOptions}
                    onChange={(feature) => setRuleConfigDraft((current) => current ? { ...current, signalConfig: { ...normalizeSignalConfig(current.signalConfig), feature: String(feature || '') } } : current)}
                    placeholder="Select feature"
                    style={{ width: '100%', marginTop: 4 }}
                  />
                </div>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Severity</Text>
                  <Select
                    value={ruleConfigDraft.signalConfig?.severity || 'MEDIUM'}
                    options={severityOptions}
                    onChange={(severity) => setRuleConfigDraft((current) => current ? { ...current, signalConfig: { ...normalizeSignalConfig(current.signalConfig), severity } } : current)}
                    placeholder="Severity"
                    style={{ width: '100%', marginTop: 4 }}
                  />
                </div>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Observed value field</Text>
                  <Select
                    showSearch
                    allowClear
                    optionFilterProp="label"
                    value={ruleConfigDraft.signalConfig?.valueField || undefined}
                    options={fieldOptions}
                    onChange={(valueField) => setRuleConfigDraft((current) => current ? { ...current, signalConfig: { ...normalizeSignalConfig(current.signalConfig), valueField: String(valueField || '') } } : current)}
                    placeholder="Observed value"
                    style={{ width: '100%', marginTop: 4 }}
                  />
                </div>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Peer value field</Text>
                  <Select
                    showSearch
                    allowClear
                    optionFilterProp="label"
                    value={ruleConfigDraft.signalConfig?.peerValueField || undefined}
                    options={fieldOptions}
                    onChange={(peerValueField) => setRuleConfigDraft((current) => current ? { ...current, signalConfig: { ...normalizeSignalConfig(current.signalConfig), peerValueField: String(peerValueField || '') } } : current)}
                    placeholder="Peer value"
                    style={{ width: '100%', marginTop: 4 }}
                  />
                </div>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Impact source</Text>
                  <Select
                    value={ruleConfigDraft.signalConfig?.impactSource || 'custom'}
                    options={[
                      { label: 'Impact Field', value: 'field' },
                      { label: 'Custom Impact', value: 'custom' },
                    ]}
                    onChange={(impactSource: TemplateMappingSource) => setRuleConfigDraft((current) => current ? { ...current, signalConfig: { ...normalizeSignalConfig(current.signalConfig), impactSource } } : current)}
                    style={{ width: '100%', marginTop: 4 }}
                  />
                </div>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>{(ruleConfigDraft.signalConfig?.impactSource || 'custom') === 'field' ? 'Impact field' : 'Impact value'}</Text>
                  {(ruleConfigDraft.signalConfig?.impactSource || 'custom') === 'field' ? (
                  <Select
                    showSearch
                    allowClear
                    optionFilterProp="label"
                    value={ruleConfigDraft.signalConfig?.impactField || undefined}
                    options={fieldOptions}
                    onChange={(impactField) => setRuleConfigDraft((current) => current ? { ...current, signalConfig: { ...normalizeSignalConfig(current.signalConfig), impactField: String(impactField || '') } } : current)}
                    placeholder="Impact field"
                    style={{ width: '100%', marginTop: 4 }}
                  />
                ) : (
                  <Input
                    value={ruleConfigDraft.signalConfig?.impactValue || ''}
                    onChange={(event) => setRuleConfigDraft((current) => current ? { ...current, signalConfig: { ...normalizeSignalConfig(current.signalConfig), impactValue: event.target.value } } : current)}
                    placeholder="Impact value"
                    style={{ marginTop: 4 }}
                  />
                )}
                </div>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Recommendation override</Text>
                  <Input
                    value={ruleConfigDraft.signalConfig?.recommendation || ''}
                    onChange={(event) => setRuleConfigDraft((current) => current ? { ...current, signalConfig: { ...normalizeSignalConfig(current.signalConfig), recommendation: event.target.value } } : current)}
                    placeholder="Optional override"
                    style={{ marginTop: 4 }}
                  />
                </div>
              </div>
              <div style={{ marginTop: 12, border: '1px solid var(--app-border)', borderRadius: 6, padding: 10, background: 'var(--app-card-bg)' }}>
                <Text style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 12 }}>
                  JSON fields for {signalOutputColumn(ruleConfigDraft) || 'rule_Generate_XAI_signal'}
                </Text>
                <Checkbox.Group
                  value={normalizeSignalConfig(ruleConfigDraft.signalConfig).jsonFields}
                  options={signalJsonFieldOptions}
                  onChange={(jsonFields) => setRuleConfigDraft((current) => current ? {
                    ...current,
                    signalConfig: {
                      ...normalizeSignalConfig(current.signalConfig),
                      jsonFields: jsonFields.map((item) => String(item)),
                    },
                  } : current)}
                  style={{ display: 'grid', gridTemplateColumns: 'repeat(5, minmax(150px, 1fr))', gap: '8px 12px', marginTop: 8 }}
                />
              </div>
              {parsedOutput.row ? (
                <Input.TextArea
                  readOnly
                  rows={6}
                  value={buildSignalJsonOutput(ruleConfigDraft, parsedOutput.row, featureDictionary)}
                  style={{ marginTop: 10, fontFamily: 'monospace', background: 'var(--app-card-bg)', color: 'var(--app-text)' }}
                />
              ) : null}
                    </>
                  ),
                },
              ]}
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
                          { label: 'Template', value: 'template' },
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
                    title: 'Map From Ensemble Field / Custom Value / Template',
                    render: (_value, row) => {
                      const source = (row.source || 'field') as TemplateMappingSource
                      if (source === 'custom') {
                        return (
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
                        )
                      }
                      if (source === 'template') {
                        const cursor = templateCursorByPlaceholder[row.placeholder] ?? String(row.value || '').length
                        const inlineDraft = templateInlineDraftAt(row.value || '', cursor)
                        const inlineFieldOptions = inlineDraft?.query
                          ? fieldOptions.filter((option) => (
                            String(option.value || '').toLowerCase().includes(inlineDraft.query.toLowerCase())
                            || String(option.label || '').toLowerCase().includes(inlineDraft.query.toLowerCase())
                          ))
                          : fieldOptions
                        const expressionFunctions = ['round', 'abs', 'ceil', 'floor', 'min', 'max', 'sqrt', 'pow']
                        const functionOptions = inlineDraft?.mode === 'expression'
                          ? expressionFunctions
                            .filter((fn) => !inlineDraft.query || fn.toLowerCase().includes(inlineDraft.query.toLowerCase()))
                            .map((fn) => ({ type: 'function' as const, label: `${fn}()`, value: fn }))
                          : []
                        const autocompleteOptions = [
                          ...functionOptions,
                          ...inlineFieldOptions.slice(0, 12).map((option) => ({
                            type: 'field' as const,
                            label: String(option.label || option.value || ''),
                            value: String(option.value || ''),
                          })),
                        ]
                        const syntax = checkTemplateSyntax(row.value || '', fieldOptions)
                        const displayParts = templateDisplayParts(row.value || '')
                        const pickerPosition = templatePickerPositionByPlaceholder[row.placeholder] || { left: 8, top: 42 }
                        const editorScroll = templateScrollByPlaceholder[row.placeholder] || { left: 0, top: 0 }
                        const updateTemplateCaret = (target: HTMLTextAreaElement) => {
                          const nextCursor = target.selectionStart ?? String(row.value || '').length
                          setTemplateCursorByPlaceholder((current) => ({ ...current, [row.placeholder]: nextCursor }))
                          setTemplatePickerPositionByPlaceholder((current) => ({ ...current, [row.placeholder]: textareaCaretPosition(target) }))
                        }
                        return (
                          <Space direction="vertical" size={6} style={{ width: '100%' }}>
                            <div style={{ position: 'relative' }}>
                              <pre
                                aria-hidden
                                style={{
                                  position: 'absolute',
                                  inset: 0,
                                  margin: 0,
                                  padding: '7px 11px',
                                  border: '1px solid transparent',
                                  borderRadius: 6,
                                  fontFamily: 'monospace',
                                  fontSize: 14,
                                  lineHeight: '22px',
                                  whiteSpace: 'pre-wrap',
                                  wordBreak: 'break-word',
                                  overflow: 'hidden',
                                  pointerEvents: 'none',
                                  transform: `translate(${-editorScroll.left}px, ${-editorScroll.top}px)`,
                                }}
                              >
                                {displayParts.length > 0 ? displayParts.map((part, index) => (
                                  <span key={`editor_${part.type}_${index}_${part.value}`} style={templatePartStyle(part.type)}>
                                    {part.value}
                                  </span>
                                )) : (
                                  <span style={{ color: 'transparent' }}>{' '}</span>
                                )}
                              </pre>
                              <Input.TextArea
                                autoSize={{ minRows: 2, maxRows: 5 }}
                                value={row.value || ''}
                                spellCheck={false}
                                autoCorrect="off"
                                autoCapitalize="off"
                                status={syntax.errors.length > 0 ? 'error' : syntax.warnings.length > 0 ? 'warning' : undefined}
                                onChange={(event) => {
                                  const target = event.currentTarget
                                  const nextValue = target.value
                                  updateTemplateCaret(target)
                                  setRuleConfigDraft((current) => current ? {
                                    ...current,
                                    templateMappings: upsertMapping(current.templateMappings, row.placeholder, {
                                      source: 'template',
                                      value: nextValue,
                                    }),
                                  } : current)
                                }}
                                onSelect={(event) => updateTemplateCaret(event.currentTarget)}
                                onKeyUp={(event) => updateTemplateCaret(event.currentTarget)}
                                onClick={(event) => updateTemplateCaret(event.currentTarget)}
                                onScroll={(event) => {
                                  updateTemplateCaret(event.currentTarget)
                                  setTemplateScrollByPlaceholder((current) => ({
                                    ...current,
                                    [row.placeholder]: {
                                      left: event.currentTarget.scrollLeft,
                                      top: event.currentTarget.scrollTop,
                                    },
                                  }))
                                }}
                                placeholder="Type plain text and wrap expressions in [ ], e.g. achieved [total_txn_count * 100]"
                                style={{
                                  position: 'relative',
                                  background: 'transparent',
                                  color: 'transparent',
                                  caretColor: 'var(--app-text)',
                                  fontFamily: 'monospace',
                                  fontSize: 14,
                                  lineHeight: '22px',
                                  WebkitTextFillColor: 'transparent',
                                }}
                              />
                              {inlineDraft && autocompleteOptions.length > 0 ? (
                                <div
                                  style={{
                                    position: 'absolute',
                                    left: Math.min(pickerPosition.left, 720),
                                    top: pickerPosition.top,
                                    width: 320,
                                    maxHeight: 220,
                                    overflowY: 'auto',
                                    zIndex: 20,
                                    border: '1px solid var(--app-border-strong)',
                                    borderRadius: 6,
                                    background: 'var(--app-panel-bg)',
                                    boxShadow: '0 12px 30px rgba(0,0,0,0.35)',
                                    padding: 4,
                                  }}
                                >
                                  {autocompleteOptions.map((option) => (
                                    <button
                                      key={`${option.type}_${option.value}`}
                                      type="button"
                                      onMouseDown={(event) => {
                                        event.preventDefault()
                                        const nextValue = option.type === 'function'
                                          ? replaceTemplateExpressionFunctionDraftAt(row.value || '', option.value, cursor)
                                          : replaceTemplateInlineDraftAt(row.value || '', option.value, cursor)
                                        setRuleConfigDraft((current) => current ? {
                                          ...current,
                                          templateMappings: upsertMapping(current.templateMappings, row.placeholder, {
                                            source: 'template',
                                            value: nextValue,
                                          }),
                                        } : current)
                                        setTemplateCursorByPlaceholder((current) => ({ ...current, [row.placeholder]: nextValue.length }))
                                      }}
                                      style={{
                                        display: 'flex',
                                        width: '100%',
                                        alignItems: 'center',
                                        gap: 8,
                                        border: 0,
                                        borderRadius: 4,
                                        background: 'transparent',
                                        color: 'var(--app-text)',
                                        padding: '6px 8px',
                                        cursor: 'pointer',
                                        textAlign: 'left',
                                      }}
                                    >
                                      <Tag color={option.type === 'function' ? 'purple' : 'blue'} style={{ marginInlineEnd: 0, minWidth: 54, textAlign: 'center' }}>
                                        {option.type === 'function' ? 'fn' : 'field'}
                                      </Tag>
                                      <Text ellipsis style={{ color: 'var(--app-text)', flex: 1 }}>{option.label}</Text>
                                    </button>
                                  ))}
                                </div>
                              ) : null}
                            </div>
                            {row.value ? (
                              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, alignItems: 'center' }}>
                                {displayParts.map((part, index) => (
                                  <span
                                    key={`${part.type}_${index}_${part.value}`}
                                    style={{
                                      border: `1px solid ${
                                        part.type === 'control' ? '#f59e0b' : part.type === 'expression' ? '#7c3aed' : part.type === 'field' ? '#2563eb' : 'var(--app-border)'
                                      }`,
                                      background: part.type === 'control'
                                        ? 'rgba(245, 158, 11, 0.14)'
                                        : part.type === 'expression'
                                          ? 'rgba(124, 58, 237, 0.16)'
                                          : part.type === 'field'
                                            ? 'rgba(37, 99, 235, 0.14)'
                                            : 'var(--app-card-bg)',
                                      color: part.type === 'control'
                                        ? '#fbbf24'
                                        : part.type === 'expression'
                                          ? '#c4b5fd'
                                          : part.type === 'field'
                                            ? '#93c5fd'
                                            : 'var(--app-text-subtle)',
                                      borderRadius: 4,
                                      padding: '2px 6px',
                                      fontFamily: part.type === 'expression' || part.type === 'field' || part.type === 'control' ? 'monospace' : undefined,
                                      fontSize: 12,
                                    }}
                                  >
                                    {part.value || ' '}
                                  </span>
                                ))}
                              </div>
                            ) : null}
                            {syntax.errors.length > 0 || syntax.warnings.length > 0 ? (
                              <Alert
                                type={syntax.errors.length > 0 ? 'error' : 'warning'}
                                showIcon
                                message={
                                  <Space size={8} wrap style={{ width: '100%', justifyContent: 'space-between' }}>
                                    <Text style={{ color: 'inherit' }}>
                                      {[...syntax.errors, ...syntax.warnings].join(' ')}
                                    </Text>
                                    <Button
                                      size="small"
                                      onClick={() => {
                                        const nextValue = autoCorrectTemplateSyntax(row.value || '')
                                        setRuleConfigDraft((current) => current ? {
                                          ...current,
                                          templateMappings: upsertMapping(current.templateMappings, row.placeholder, {
                                            source: 'template',
                                            value: nextValue,
                                          }),
                                        } : current)
                                        setTemplateCursorByPlaceholder((current) => ({ ...current, [row.placeholder]: nextValue.length }))
                                      }}
                                    >
                                      Auto Correct
                                    </Button>
                                  </Space>
                                }
                              />
                            ) : (
                              row.value ? <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Expression OK</Text> : null
                            )}
                            <Space size={4} wrap>
                              {['+', '-', '*', '/', '%', '(', ')'].map((snippet) => (
                                <Button
                                  key={snippet}
                                  size="small"
                                  onClick={() => setRuleConfigDraft((current) => current ? {
                                    ...current,
                                    templateMappings: upsertMapping(current.templateMappings, row.placeholder, {
                                      source: 'template',
                                      value: insertExpressionSnippetAt(row.value || '', snippet, cursor),
                                    }),
                                  } : current)}
                                >
                                  {snippet}
                                </Button>
                              ))}
                              {['round', 'abs', 'min', 'max'].map((fn) => (
                                <Button
                                  key={fn}
                                  size="small"
                                  onClick={() => setRuleConfigDraft((current) => current ? {
                                    ...current,
                                    templateMappings: upsertMapping(current.templateMappings, row.placeholder, {
                                      source: 'template',
                                      value: insertExpressionSnippetAt(row.value || '', `${fn}()`, cursor),
                                    }),
                                  } : current)}
                                >
                                  {fn}
                                </Button>
                              ))}
                            </Space>
                          </Space>
                        )
                      }
                      return (
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
                      )
                    },
                  },
                ]}
                locale={{ emptyText: 'Selected template has no custom field placeholders.' }}
              />
              <Collapse
                size="small"
                ghost
                style={{ marginTop: 8 }}
                items={[
                  {
                    key: 'template-expression-reference',
                    label: <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Expression Examples</Text>,
                    children: (
                      <Space direction="vertical" size={6} style={{ width: '100%' }}>
                        <Alert
                          type="info"
                          showIcon
                          message="Use these examples in the template body or in a placeholder row with Value Source = Template."
                        />
                        <Input.TextArea
                          readOnly
                          autoSize={{ minRows: 6, maxRows: 9 }}
                          value={[
                            'Direct field only: total_txn_count',
                            'Text + expression: Target set is 100, achieved [total_txn_count * 100]',
                            'Percentage: [round(risk_score * 100)]%',
                            'Difference: Gap is [target_count - total_txn_count]',
                            'Conditional: [[if total_txn_count >= 100]Target reached[elseif total_txn_count > 0]In progress[else]No progress[end]]',
                            'Plain text stays unchanged; expression blocks in [ ] are evaluated.',
                          ].join('\n')}
                          style={{ fontFamily: 'monospace', background: 'var(--app-card-bg)', color: 'var(--app-text)' }}
                        />
                      </Space>
                    ),
                  },
                ]}
              />
            </div>
          </Space>
        ) : null}
      </Modal>
    </Card>
  )
}
