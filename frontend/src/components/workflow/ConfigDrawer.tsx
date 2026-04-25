import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  Drawer, Form, Input, Select, Switch, InputNumber,
  Button, Typography, Space, Tabs, Divider, Tag, Tooltip, Table, notification, Modal, Popover, AutoComplete
} from 'antd'
import { CloseOutlined, CopyOutlined, DeleteOutlined, InfoCircleOutlined } from '@ant-design/icons'
import Editor, { type Monaco } from '@monaco-editor/react'
import { useWorkflowStore } from '../../store'
import { SourceFilePicker, DestinationPathPicker } from './FilePicker'
import ConditionBuilderModal from './ConditionBuilderModal'
import FieldBuilderModal from './FieldBuilderModal'
import {
  CUSTOM_FIELD_EXAMPLE_REPOSITORY,
  CUSTOM_FIELD_TIPS_TEXT,
  EXPRESSION_FUNCTION_SNIPPETS,
} from './CustomFieldStudioConstants'
import {
  BRE_CLAUSE_OPERATOR_OPTIONS,
  DEFAULT_EXPRESSION_FUNCTION_NAMES,
} from './expressionDefaults'
import {
  buildUiFieldBuilderExpression,
  createDefaultUiFieldBuilderState,
  parseUiFieldBuilderFromCurrent,
  type UiFieldBuilderState,
} from './uiFieldBuilderUtils'
import api from '../../api/client'
import { clearDrawerInteraction, markDrawerInteraction } from '../../utils/drawerAutoHide'
import type { ConfigField, ETLNode, ETLNodeData } from '../../types'

const { Text } = Typography

// File-type node types and which field maps to file/path
const FILE_SOURCE_TYPES = ['csv_source', 'json_source', 'excel_source', 'xml_source', 'parquet_source']
const FILE_DEST_TYPES   = ['csv_destination', 'json_destination', 'excel_destination']
const DB_SOURCE_TYPES = [
  'postgres_source',
  'mysql_source',
  'oracle_source',
  'mongodb_source',
  'redis_source',
  'elasticsearch_source',
]
const CUSTOM_SCHEDULE_PRESET = 'custom'
const SCHEDULE_PRESET_TO_CRON: Record<string, string> = {
  every_1_min: '* * * * *',
  every_5_min: '*/5 * * * *',
  every_15_min: '*/15 * * * *',
  hourly: '0 * * * *',
  daily_9am: '0 9 * * *',
  weekly_mon_9am: '0 9 * * 1',
}
const DEFAULT_SCHEDULE_PRESET = 'hourly'
const DEFAULT_SCHEDULE_CRON = SCHEDULE_PRESET_TO_CRON[DEFAULT_SCHEDULE_PRESET]

function getFileType(nodeType: string): string {
  if (nodeType.includes('csv'))    return 'csv'
  if (nodeType.includes('excel'))  return 'excel'
  if (nodeType.includes('json'))   return 'json'
  if (nodeType.includes('xml'))    return 'xml'
  if (nodeType.includes('parquet')) return 'parquet'
  return 'all'
}

interface ConfigDrawerProps {
  open: boolean
  onClose: () => void
}

type CustomFieldMode = 'value' | 'json'
type SingleValueOutputMode = 'plain_text' | 'json'
type CustomProfileProcessingMode = 'batch' | 'incremental' | 'incremental_batch'
type CustomProfileComputeStrategy = 'single' | 'parallel_by_profile_key'
type CustomProfileComputeExecutor = 'thread' | 'process'
type CustomExpressionEngine = 'auto' | 'python' | 'polars'
type CustomProfileStorage = 'lmdb' | 'rocksdb' | 'redis' | 'oracle'
type CustomProfileOracleWriteStrategy = 'single' | 'parallel_key'
type CustomEditorColorProfile =
  | 'high_contrast'
  | 'soft'
  | 'js_like'
  | 'monokai'
  | 'dracula'
  | 'solarized_dark'
  | 'github_dark'
  | 'github_light'
type CustomEditorFontPreset = 'jetbrains_mono' | 'fira_code' | 'consolas'
type CustomBeautifyStyle = 'json' | 'js_view'

interface CustomFieldSpec {
  id: string
  name: string
  mode: CustomFieldMode
  singleValueOutput: SingleValueOutputMode
  expression: string
  jsonTemplate: string
  enabled: boolean
}

type CustomFieldBeautifyBackup = {
  expression: string
  jsonTemplate: string
}

type UiExpressionEditorTab = 'expression' | 'ui_expression'

type UiExpressionBuilderRow = {
  id: string
  key: string
  expression: string
  valueType: 'expression' | 'json'
}

type SnippetPlaceholder = {
  index: number
  token: string
  kind: 'text' | 'choice'
  defaultValue: string
  options: string[]
}

type UiExpressionParameterMode = 'field' | 'values' | 'variable' | 'literal' | 'raw'

type UiExpressionVariableSpec = {
  id: string
  name: string
  value: string
}

type UiGroupAggregateMetric = {
  id: string
  outputName: string
  path: string
  agg: string
}

type UiConditionClauseRightMode = 'literal' | 'field' | 'values' | 'variable' | 'raw'
type UiConditionLogicalJoin = 'and' | 'or'

type UiConditionCluster = {
  id: string
  joinWithPrev: UiConditionLogicalJoin
}

type UiConditionClause = {
  id: string
  clusterId: string
  joinWithPrev: UiConditionLogicalJoin
  leftField: string
  operator: string
  rightMode: UiConditionClauseRightMode
  rightValue: string
}

type UiConditionBuilderState = {
  clusters: UiConditionCluster[]
  clauses: UiConditionClause[]
}

type BreClauseOperator = '==' | '!=' | '>' | '>=' | '<' | '<=' | 'raw'
type BreClauseJoin = 'and' | 'or'
type BreClauseOperandKind = 'field' | 'literal' | 'raw'

type BreClause = {
  id: string
  joinWithPrev: BreClauseJoin
  leftKind: BreClauseOperandKind
  leftValue: string
  operator: BreClauseOperator
  rightKind: BreClauseOperandKind
  rightValue: string
}

type BreModel = {
  clauses: BreClause[]
  thenExpr: string
  elseExpr: string
  parseError: string | null
}

interface OracleColumnMappingSpec {
  id: string
  source: string
  destination: string
  enabled: boolean
}

interface LmdbStudioDraft {
  env_path: string
  db_name: string
  key_prefix: string
  start_key: string
  end_key: string
  key_contains: string
  value_contains: string
  value_format: 'auto' | 'json' | 'text' | 'base64'
  flatten_json_values: boolean
  expand_profile_documents: boolean
  limit: number
}

interface JsonTagMappingItem {
  path: string
  kind: 'string' | 'number' | 'boolean' | 'null' | 'object' | 'array'
  sample: string
  fieldExpr: string
  valuesExpr: string
  controlTag: string
}

interface CustomFieldExampleItem {
  id: string
  title: string
  category: string
  mode: CustomFieldMode
  name: string
  description: string
  expression?: string
  jsonTemplate?: string
  tags?: string[]
}

interface ProfileMonitorNodeSummary {
  node_id: string
  entity_count: number
  meta_count: number
  sample_entity_keys: string[]
  sample_documents: Array<{ entity_key: string; profile: Record<string, unknown> }>
}

interface ProfileMonitorResponse {
  pipeline_id: string
  node_id?: string | null
  limit: number
  total_nodes: number
  total_entities: number
  total_meta_entries: number
  available_node_ids: string[]
  nodes: ProfileMonitorNodeSummary[]
  generated_at: string
}

interface CustomFieldValidationResult {
  ok: boolean
  validation_source?: string
  input_rows: number
  output_rows: number
  errors: string[]
  warnings: string[]
  sample_input: Array<Record<string, unknown>>
  sample_output: Array<Record<string, unknown>>
}

const MAX_PATH_DEPTH = 5
const MAX_PATH_OPTIONS = 400
const MAX_ARRAY_INDEX_OPTIONS = 3
const MAX_OBJECT_KEYS_PER_LEVEL = 40
const MAX_JSON_TAG_DEPTH = 8
const MAX_JSON_TAG_MAPPINGS = 320

type ExpressionCompletionEntry = {
  label: string
  insertText: string
  kind: 'function' | 'field' | 'path' | 'keyword'
  detail?: string
  documentation?: string
  command?: { id: string; title?: string }
}

const EXPR_LANGUAGE_ID = 'etl-expr'
const JSON_TEMPLATE_LANGUAGE_ID = 'etl-json-template'
const EXPR_VALIDATION_OWNER = 'etl-expr-validator'
const JSON_TEMPLATE_VALIDATION_OWNER = 'etl-json-template-validator'
const EXPR_INLINE_ERROR_CLASS = 'opsfabric-expr-inline-error'
const EXPR_INLINE_WARNING_CLASS = 'opsfabric-expr-inline-warning'
const EXPR_RANGE_ERROR_CLASS = 'opsfabric-expr-range-error'
const EXPR_RANGE_WARNING_CLASS = 'opsfabric-expr-range-warning'
const EXPR_GLYPH_ERROR_CLASS = 'opsfabric-expr-glyph-error'
const EXPR_GLYPH_WARNING_CLASS = 'opsfabric-expr-glyph-warning'
const EXPR_FIELD_SINGLE_QUOTED_PATTERN = /'(?=[^']*[A-Z])(?:[^'\\]|\\.)*'/
const EXPR_LITERAL_SINGLE_QUOTED_PATTERN = /'(?:min|max|sum|mean|avg|count|count_non_null|distinct|distinct_count|value_counts|first|last|row_count|customer|servicename|datewise|timeseries|daily|weekly|monthly|yearly|eq|not|contains|not_contains|like|not_like|in|not_in|startswith|not_startswith|endswith|not_endswith|regex|not_regex)'/
const CUSTOM_EDITOR_THEME_IDS = {
  high_contrast: 'opsfabric-custom-field-dark-hc',
  soft: 'opsfabric-custom-field-dark-soft',
  js_like: 'opsfabric-custom-field-dark-js',
  monokai: 'opsfabric-custom-field-monokai',
  dracula: 'opsfabric-custom-field-dracula',
  solarized_dark: 'opsfabric-custom-field-solarized-dark',
  github_dark: 'opsfabric-custom-field-github-dark',
  github_light: 'opsfabric-custom-field-github-light',
} as const

const CUSTOM_EDITOR_COLOR_PROFILE_OPTIONS: Array<{ value: CustomEditorColorProfile; label: string }> = [
  { value: 'high_contrast', label: 'High Contrast' },
  { value: 'soft', label: 'Soft' },
  { value: 'js_like', label: 'JS-like' },
  { value: 'monokai', label: 'Monokai' },
  { value: 'dracula', label: 'Dracula' },
  { value: 'solarized_dark', label: 'Solarized Dark' },
  { value: 'github_dark', label: 'GitHub Dark' },
  { value: 'github_light', label: 'GitHub Light' },
]

const CUSTOM_EDITOR_FONT_FAMILY_OPTIONS: Array<{ value: CustomEditorFontPreset; label: string }> = [
  { value: 'jetbrains_mono', label: 'JetBrains Mono' },
  { value: 'fira_code', label: 'Fira Code' },
  { value: 'consolas', label: 'Consolas' },
]
const CUSTOM_EDITOR_BEAUTIFY_STYLE_OPTIONS: Array<{ value: CustomBeautifyStyle; label: string }> = [
  { value: 'json', label: 'Beautify: JSON' },
  { value: 'js_view', label: 'Beautify: JS View' },
]
const CUSTOM_EDITOR_PREFS_STORAGE_KEY = 'opsfabric.custom_editor_prefs.v1'

type CustomEditorPrefs = {
  colorProfile: CustomEditorColorProfile
  fontPreset: CustomEditorFontPreset
  beautifyStyle: CustomBeautifyStyle
  fontSize: number
  lineHeight: number
  wordWrap: boolean
  ligatures: boolean
}

const DEFAULT_CUSTOM_EDITOR_PREFS: CustomEditorPrefs = {
  colorProfile: 'high_contrast',
  fontPreset: 'jetbrains_mono',
  beautifyStyle: 'json',
  fontSize: 13,
  lineHeight: 22,
  wordWrap: true,
  ligatures: false,
}

function loadCustomEditorPrefs(): CustomEditorPrefs {
  if (typeof window === 'undefined' || !window.localStorage) {
    return { ...DEFAULT_CUSTOM_EDITOR_PREFS }
  }
  try {
    const raw = window.localStorage.getItem(CUSTOM_EDITOR_PREFS_STORAGE_KEY)
    if (!raw) return { ...DEFAULT_CUSTOM_EDITOR_PREFS }
    const parsed = JSON.parse(raw) as Partial<CustomEditorPrefs>
    const colorProfile = parsed?.colorProfile
    const fontPreset = parsed?.fontPreset
    const beautifyStyle = parsed?.beautifyStyle
    const allowedColorProfiles = new Set<CustomEditorColorProfile>([
      'high_contrast',
      'soft',
      'js_like',
      'monokai',
      'dracula',
      'solarized_dark',
      'github_dark',
      'github_light',
    ])
    const allowedFontPresets = new Set<CustomEditorFontPreset>(['jetbrains_mono', 'fira_code', 'consolas'])
    const allowedBeautifyStyles = new Set<CustomBeautifyStyle>(['json', 'js_view'])
    return {
      colorProfile: allowedColorProfiles.has(colorProfile as CustomEditorColorProfile)
        ? (colorProfile as CustomEditorColorProfile)
        : DEFAULT_CUSTOM_EDITOR_PREFS.colorProfile,
      fontPreset: allowedFontPresets.has(fontPreset as CustomEditorFontPreset)
        ? (fontPreset as CustomEditorFontPreset)
        : DEFAULT_CUSTOM_EDITOR_PREFS.fontPreset,
      beautifyStyle: allowedBeautifyStyles.has(beautifyStyle as CustomBeautifyStyle)
        ? (beautifyStyle as CustomBeautifyStyle)
        : DEFAULT_CUSTOM_EDITOR_PREFS.beautifyStyle,
      fontSize: Number.isFinite(Number(parsed?.fontSize))
        ? Math.max(11, Math.min(20, Math.floor(Number(parsed?.fontSize))))
        : DEFAULT_CUSTOM_EDITOR_PREFS.fontSize,
      lineHeight: Number.isFinite(Number(parsed?.lineHeight))
        ? Math.max(18, Math.min(28, Math.floor(Number(parsed?.lineHeight))))
        : DEFAULT_CUSTOM_EDITOR_PREFS.lineHeight,
      wordWrap: typeof parsed?.wordWrap === 'boolean' ? parsed.wordWrap : DEFAULT_CUSTOM_EDITOR_PREFS.wordWrap,
      ligatures: typeof parsed?.ligatures === 'boolean' ? parsed.ligatures : DEFAULT_CUSTOM_EDITOR_PREFS.ligatures,
    }
  } catch {
    return { ...DEFAULT_CUSTOM_EDITOR_PREFS }
  }
}

function resolveEditorFontFamily(preset: CustomEditorFontPreset): string {
  if (preset === 'fira_code') return '"Fira Code", "JetBrains Mono", "Cascadia Mono", "SF Mono", Menlo, Monaco, Consolas, "Courier New", monospace'
  if (preset === 'consolas') return 'Consolas, "Cascadia Mono", "SF Mono", Menlo, Monaco, "Courier New", monospace'
  return '"JetBrains Mono", "Fira Code", "Cascadia Mono", "SF Mono", Menlo, Monaco, Consolas, "Courier New", monospace'
}

let exprLanguageRegistered = false
let jsonTemplateLanguageRegistered = false
let customEditorLastMonaco: Monaco | null = null
let exprCompletionEntries: ExpressionCompletionEntry[] = []
let exprFunctionSignatureMap = new Map<string, { name: string; label: string; parameters: string[]; documentation: string }>()
const EXPR_ALLOWED_AGG_VALUES = new Set([
  'sum',
  'mean',
  'avg',
  'min',
  'max',
  'count',
  'count_non_null',
  'distinct',
  'distinct_count',
  'value_counts',
  'first',
  'last',
  'row_count',
  'std',
  'variance',
])
const EXPR_ALLOWED_CONDITION_OPERATORS = new Set([
  'eq',
  '==',
  '!=',
  'not',
  'contains',
  'not_contains',
  'like',
  'not_like',
  'in',
  'not_in',
  'startswith',
  'not_startswith',
  'endswith',
  'not_endswith',
  'regex',
  'not_regex',
])

const EXPR_ALLOWED_AGG_VALUES_OPTIONS = Array.from(EXPR_ALLOWED_AGG_VALUES).sort((a, b) => a.localeCompare(b))
const EXPR_ALLOWED_CONDITION_OPERATORS_OPTIONS = Array.from(EXPR_ALLOWED_CONDITION_OPERATORS).sort((a, b) => a.localeCompare(b))
type ExpressionValidationContext = {
  allowedFunctionNames: Set<string>
  allowedFieldNames: Set<string>
  allowedFieldNamesLower: Set<string>
  allowedPathNames: Set<string>
  allowedPathNamesLower: Set<string>
}

let exprValidationContext: ExpressionValidationContext = {
  allowedFunctionNames: new Set(DEFAULT_EXPRESSION_FUNCTION_NAMES),
  allowedFieldNames: new Set(),
  allowedFieldNamesLower: new Set(),
  allowedPathNames: new Set(),
  allowedPathNamesLower: new Set(),
}
const editorValidationDecorations = new WeakMap<any, string[]>()
const modelValidationIssues = new WeakMap<any, ExpressionValidationIssue[]>()
const modelSuggestIntent = new WeakMap<any, 'issue_click'>()

function buildExpressionValidationContext(entries: ExpressionCompletionEntry[]): ExpressionValidationContext {
  const allowedFunctionNames = new Set(DEFAULT_EXPRESSION_FUNCTION_NAMES)
  const allowedFieldNames = new Set<string>()
  const allowedFieldNamesLower = new Set<string>()
  const allowedPathNames = new Set<string>()
  const allowedPathNamesLower = new Set<string>()
  ;(entries || []).forEach((entry) => {
    if (!entry) return
    if (entry.kind === 'function') {
      const candidates = [entry.insertText, entry.label]
      candidates.forEach((candidate) => {
        const m = String(candidate || '').match(/\b([A-Za-z_]\w*)\s*\(/)
        if (m?.[1]) {
          allowedFunctionNames.add(String(m[1]).toLowerCase())
        }
      })
      return
    }
    if (entry.kind === 'field') {
      const fieldName = String(entry.label || '').trim()
      if (!fieldName) return
      allowedFieldNames.add(fieldName)
      allowedFieldNamesLower.add(fieldName.toLowerCase())
      allowedPathNames.add(fieldName)
      allowedPathNamesLower.add(fieldName.toLowerCase())
      return
    }
    if (entry.kind === 'path') {
      const pathName = String(entry.label || '').trim()
      if (!pathName) return
      allowedPathNames.add(pathName)
      allowedPathNamesLower.add(pathName.toLowerCase())
      const root = extractPathRoot(pathName)
      if (root) {
        allowedFieldNames.add(root)
        allowedFieldNamesLower.add(root.toLowerCase())
      }
    }
  })
  return {
    allowedFunctionNames,
    allowedFieldNames,
    allowedFieldNamesLower,
    allowedPathNames,
    allowedPathNamesLower,
  }
}

function getExpressionValidationContext(): ExpressionValidationContext {
  return exprValidationContext
}

function setExpressionCompletionEntries(entries: ExpressionCompletionEntry[]): void {
  exprCompletionEntries = entries
  exprValidationContext = buildExpressionValidationContext(entries)
  exprFunctionSignatureMap = buildFunctionSignatureMap(entries)
}

function ensureCustomEditorTheme(monaco: Monaco): void {
  customEditorLastMonaco = monaco

  monaco.editor.defineTheme(CUSTOM_EDITOR_THEME_IDS.high_contrast, {
    base: 'vs-dark',
    inherit: true,
    rules: [
      { token: 'entity.name.function', foreground: '00E5FF', fontStyle: 'bold' },
      { token: 'field.function', foreground: '4DA3FF', fontStyle: 'bold' },
      { token: 'parameter.name', foreground: '9CDCFE', fontStyle: 'bold' },
      { token: 'variable.field', foreground: 'FFEA00', fontStyle: 'bold' },
      { token: 'string.literal.value', foreground: 'FF4D9A', fontStyle: 'bold' },
      { token: 'string.expression', foreground: 'D1FAE5' },
      { token: 'string.expression.quote', foreground: '34D399', fontStyle: 'bold' },
      { token: 'key', foreground: 'C084FC', fontStyle: 'bold' },
      { token: 'number', foreground: 'FB7185', fontStyle: 'bold' },
      { token: 'operator', foreground: 'F472B6', fontStyle: 'bold' },
      { token: 'delimiter.bracket', foreground: 'F8FAFC', fontStyle: 'bold' },
      { token: 'string', foreground: 'A7F3D0' },
    ],
    colors: {
      'editor.background': '#0b1020',
      'editor.foreground': '#e2e8f0',
      'editorCursor.foreground': '#f8fafc',
      'editorLineNumber.foreground': '#64748b',
      'editorLineNumber.activeForeground': '#cbd5e1',
      'editorBracketMatch.background': '#1d4ed833',
      'editorBracketMatch.border': '#38bdf8',
      'editorError.foreground': '#ef4444',
      'editorWarning.foreground': '#f59e0b',
    },
  })
  monaco.editor.defineTheme(CUSTOM_EDITOR_THEME_IDS.soft, {
    base: 'vs-dark',
    inherit: true,
    rules: [
      { token: 'entity.name.function', foreground: '67E8F9', fontStyle: 'bold' },
      { token: 'field.function', foreground: '93C5FD', fontStyle: 'bold' },
      { token: 'parameter.name', foreground: '9CDCFE', fontStyle: 'bold' },
      { token: 'variable.field', foreground: 'FDE68A', fontStyle: 'bold' },
      { token: 'string.literal.value', foreground: 'F9A8D4', fontStyle: 'bold' },
      { token: 'string.expression', foreground: 'D1FAE5' },
      { token: 'string.expression.quote', foreground: '6EE7B7' },
      { token: 'key', foreground: 'DDD6FE', fontStyle: 'bold' },
      { token: 'number', foreground: 'FBCFE8' },
      { token: 'operator', foreground: 'C4B5FD' },
      { token: 'delimiter.bracket', foreground: 'E2E8F0', fontStyle: 'bold' },
      { token: 'string', foreground: 'A7F3D0' },
    ],
    colors: {
      'editor.background': '#111827',
      'editor.foreground': '#e5e7eb',
      'editorCursor.foreground': '#f8fafc',
      'editorLineNumber.foreground': '#6b7280',
      'editorLineNumber.activeForeground': '#d1d5db',
      'editorBracketMatch.background': '#33415533',
      'editorBracketMatch.border': '#93c5fd',
      'editorError.foreground': '#ef4444',
      'editorWarning.foreground': '#f59e0b',
    },
  })
  monaco.editor.defineTheme(CUSTOM_EDITOR_THEME_IDS.js_like, {
    base: 'vs-dark',
    inherit: true,
    rules: [
      { token: 'entity.name.function', foreground: 'DCDCAA', fontStyle: 'bold' },
      { token: 'field.function', foreground: '4FC1FF', fontStyle: 'bold' },
      { token: 'parameter.name', foreground: '9CDCFE', fontStyle: 'bold' },
      { token: 'variable.field', foreground: '4FC1FF', fontStyle: 'bold' },
      { token: 'string.literal.value', foreground: 'CE9178', fontStyle: 'bold' },
      { token: 'string.expression', foreground: 'd4d4d4' },
      { token: 'string.expression.quote', foreground: 'CE9178' },
      { token: 'key', foreground: '9CDCFE', fontStyle: 'bold' },
      { token: 'number', foreground: 'B5CEA8' },
      { token: 'operator', foreground: 'D4D4D4' },
      { token: 'delimiter.bracket', foreground: 'FFD700', fontStyle: 'bold' },
      { token: 'string', foreground: 'CE9178' },
    ],
    colors: {
      'editor.background': '#1e1e1e',
      'editor.foreground': '#d4d4d4',
      'editorCursor.foreground': '#f8fafc',
      'editorLineNumber.foreground': '#858585',
      'editorLineNumber.activeForeground': '#c6c6c6',
      'editorBracketMatch.background': '#264f7840',
      'editorBracketMatch.border': '#4FC1FF',
      'editorError.foreground': '#f44747',
      'editorWarning.foreground': '#cca700',
    },
  })
  monaco.editor.defineTheme(CUSTOM_EDITOR_THEME_IDS.monokai, {
    base: 'vs-dark',
    inherit: true,
    rules: [
      { token: 'entity.name.function', foreground: 'A6E22E', fontStyle: 'bold' },
      { token: 'field.function', foreground: '66D9EF', fontStyle: 'bold' },
      { token: 'parameter.name', foreground: 'FD971F', fontStyle: 'bold' },
      { token: 'variable.field', foreground: 'E6DB74', fontStyle: 'bold' },
      { token: 'string.literal.value', foreground: 'E6DB74', fontStyle: 'bold' },
      { token: 'string.expression', foreground: 'F8F8F2' },
      { token: 'string.expression.quote', foreground: 'E6DB74' },
      { token: 'key', foreground: 'F92672', fontStyle: 'bold' },
      { token: 'number', foreground: 'AE81FF', fontStyle: 'bold' },
      { token: 'operator', foreground: 'F92672', fontStyle: 'bold' },
      { token: 'delimiter.bracket', foreground: 'F8F8F2', fontStyle: 'bold' },
      { token: 'string', foreground: 'E6DB74' },
    ],
    colors: {
      'editor.background': '#272822',
      'editor.foreground': '#F8F8F2',
      'editorCursor.foreground': '#F8F8F0',
      'editorLineNumber.foreground': '#75715E',
      'editorLineNumber.activeForeground': '#F8F8F2',
      'editorBracketMatch.background': '#3E3D32',
      'editorBracketMatch.border': '#A6E22E',
      'editorError.foreground': '#F92672',
      'editorWarning.foreground': '#FD971F',
    },
  })
  monaco.editor.defineTheme(CUSTOM_EDITOR_THEME_IDS.dracula, {
    base: 'vs-dark',
    inherit: true,
    rules: [
      { token: 'entity.name.function', foreground: '50FA7B', fontStyle: 'bold' },
      { token: 'field.function', foreground: '8BE9FD', fontStyle: 'bold' },
      { token: 'parameter.name', foreground: 'FFB86C', fontStyle: 'bold' },
      { token: 'variable.field', foreground: 'F1FA8C', fontStyle: 'bold' },
      { token: 'string.literal.value', foreground: 'F1FA8C', fontStyle: 'bold' },
      { token: 'string.expression', foreground: 'F8F8F2' },
      { token: 'string.expression.quote', foreground: 'F1FA8C' },
      { token: 'key', foreground: 'FF79C6', fontStyle: 'bold' },
      { token: 'number', foreground: 'BD93F9', fontStyle: 'bold' },
      { token: 'operator', foreground: 'FF79C6', fontStyle: 'bold' },
      { token: 'delimiter.bracket', foreground: 'F8F8F2', fontStyle: 'bold' },
      { token: 'string', foreground: 'F1FA8C' },
    ],
    colors: {
      'editor.background': '#282A36',
      'editor.foreground': '#F8F8F2',
      'editorCursor.foreground': '#F8F8F2',
      'editorLineNumber.foreground': '#6272A4',
      'editorLineNumber.activeForeground': '#F8F8F2',
      'editorBracketMatch.background': '#44475A66',
      'editorBracketMatch.border': '#8BE9FD',
      'editorError.foreground': '#FF5555',
      'editorWarning.foreground': '#FFB86C',
    },
  })
  monaco.editor.defineTheme(CUSTOM_EDITOR_THEME_IDS.solarized_dark, {
    base: 'vs-dark',
    inherit: true,
    rules: [
      { token: 'entity.name.function', foreground: 'B58900', fontStyle: 'bold' },
      { token: 'field.function', foreground: '268BD2', fontStyle: 'bold' },
      { token: 'parameter.name', foreground: '2AA198', fontStyle: 'bold' },
      { token: 'variable.field', foreground: 'CB4B16', fontStyle: 'bold' },
      { token: 'string.literal.value', foreground: '859900', fontStyle: 'bold' },
      { token: 'string.expression', foreground: '93A1A1' },
      { token: 'string.expression.quote', foreground: '859900' },
      { token: 'key', foreground: 'D33682', fontStyle: 'bold' },
      { token: 'number', foreground: '6C71C4', fontStyle: 'bold' },
      { token: 'operator', foreground: '839496', fontStyle: 'bold' },
      { token: 'delimiter.bracket', foreground: '93A1A1', fontStyle: 'bold' },
      { token: 'string', foreground: '859900' },
    ],
    colors: {
      'editor.background': '#002B36',
      'editor.foreground': '#93A1A1',
      'editorCursor.foreground': '#93A1A1',
      'editorLineNumber.foreground': '#586E75',
      'editorLineNumber.activeForeground': '#EEE8D5',
      'editorBracketMatch.background': '#073642',
      'editorBracketMatch.border': '#2AA198',
      'editorError.foreground': '#DC322F',
      'editorWarning.foreground': '#B58900',
    },
  })
  monaco.editor.defineTheme(CUSTOM_EDITOR_THEME_IDS.github_dark, {
    base: 'vs-dark',
    inherit: true,
    rules: [
      { token: 'entity.name.function', foreground: 'D2A8FF', fontStyle: 'bold' },
      { token: 'field.function', foreground: '79C0FF', fontStyle: 'bold' },
      { token: 'parameter.name', foreground: 'A5D6FF', fontStyle: 'bold' },
      { token: 'variable.field', foreground: 'FFA657', fontStyle: 'bold' },
      { token: 'string.literal.value', foreground: 'A5D6FF', fontStyle: 'bold' },
      { token: 'string.expression', foreground: 'C9D1D9' },
      { token: 'string.expression.quote', foreground: 'A5D6FF' },
      { token: 'key', foreground: 'FF7B72', fontStyle: 'bold' },
      { token: 'number', foreground: '79C0FF', fontStyle: 'bold' },
      { token: 'operator', foreground: 'FF7B72', fontStyle: 'bold' },
      { token: 'delimiter.bracket', foreground: 'C9D1D9', fontStyle: 'bold' },
      { token: 'string', foreground: 'A5D6FF' },
    ],
    colors: {
      'editor.background': '#0D1117',
      'editor.foreground': '#C9D1D9',
      'editorCursor.foreground': '#C9D1D9',
      'editorLineNumber.foreground': '#6E7681',
      'editorLineNumber.activeForeground': '#C9D1D9',
      'editorBracketMatch.background': '#30363D66',
      'editorBracketMatch.border': '#58A6FF',
      'editorError.foreground': '#F85149',
      'editorWarning.foreground': '#D29922',
    },
  })
  monaco.editor.defineTheme(CUSTOM_EDITOR_THEME_IDS.github_light, {
    base: 'vs',
    inherit: true,
    rules: [
      { token: 'entity.name.function', foreground: '8250DF', fontStyle: 'bold' },
      { token: 'field.function', foreground: '0550AE', fontStyle: 'bold' },
      { token: 'parameter.name', foreground: '0A3069', fontStyle: 'bold' },
      { token: 'variable.field', foreground: '953800', fontStyle: 'bold' },
      { token: 'string.literal.value', foreground: '0A3069', fontStyle: 'bold' },
      { token: 'string.expression', foreground: '24292F' },
      { token: 'string.expression.quote', foreground: '0A3069' },
      { token: 'key', foreground: 'CF222E', fontStyle: 'bold' },
      { token: 'number', foreground: '0550AE', fontStyle: 'bold' },
      { token: 'operator', foreground: 'CF222E', fontStyle: 'bold' },
      { token: 'delimiter.bracket', foreground: '24292F', fontStyle: 'bold' },
      { token: 'string', foreground: '0A3069' },
    ],
    colors: {
      'editor.background': '#FFFFFF',
      'editor.foreground': '#24292F',
      'editorCursor.foreground': '#24292F',
      'editorLineNumber.foreground': '#8C959F',
      'editorLineNumber.activeForeground': '#24292F',
      'editorBracketMatch.background': '#DDF4FF',
      'editorBracketMatch.border': '#0969DA',
      'editorError.foreground': '#CF222E',
      'editorWarning.foreground': '#9A6700',
    },
  })
}

function extractSuggestionFromIssueMessage(message: string): string | null {
  const text = String(message || '')
  if (!text) return null
  const match = text.match(/Did you mean "([^"]+)"/)
  const suggestion = String(match?.[1] || '').trim()
  return suggestion || null
}

function findIssueQuickFixAtOffset(
  model: any,
  offset: number
): { replacement: string; startOffset: number; endOffset: number } | null {
  const issues = modelValidationIssues.get(model) || []
  const cursorOffset = Math.max(0, Number(offset || 0))
  let best: { replacement: string; startOffset: number; endOffset: number } | null = null
  let bestSpan = Number.POSITIVE_INFINITY

  issues.forEach((issue) => {
    if (!issue) return
    const start = Math.max(0, Number(issue.startOffset || 0))
    const end = Math.max(start + 1, Number(issue.endOffset || 0))
    if (cursorOffset < start || cursorOffset > end) return
    const replacement = extractSuggestionFromIssueMessage(issue.message)
    if (!replacement) return
    const span = end - start
    if (span < bestSpan) {
      bestSpan = span
      best = { replacement, startOffset: start, endOffset: end }
    }
  })

  return best
}

type ExpressionCompletionMode = 'default' | 'field_path' | 'profile_path'

function isEscapedAt(source: string, index: number): boolean {
  let backslashCount = 0
  for (let i = index - 1; i >= 0; i -= 1) {
    if (source[i] !== '\\') break
    backslashCount += 1
  }
  return backslashCount % 2 === 1
}

function findActiveQuotedLiteralRange(
  source: string,
  cursorOffset: number
): { startOffset: number; endOffset: number } | null {
  const text = String(source || '')
  const cursor = Math.max(0, Math.min(Number(cursorOffset || 0), text.length))
  if (!text || cursor <= 0) return null

  let openQuoteOffset = -1
  let quoteChar: "'" | '"' | null = null

  for (let idx = cursor - 1; idx >= 0; idx -= 1) {
    const ch = text[idx]
    if ((ch === "'" || ch === '"') && !isEscapedAt(text, idx)) {
      openQuoteOffset = idx
      quoteChar = ch
      break
    }
  }
  if (openQuoteOffset < 0 || !quoteChar) return null

  for (let idx = openQuoteOffset + 1; idx < cursor; idx += 1) {
    if (text[idx] === quoteChar && !isEscapedAt(text, idx)) {
      return null
    }
  }

  let closeQuoteOffset = text.length
  for (let idx = cursor; idx < text.length; idx += 1) {
    const ch = text[idx]
    if (ch === '\n' || ch === '\r') break
    if (ch === quoteChar && !isEscapedAt(text, idx)) {
      closeQuoteOffset = idx
      break
    }
  }

  return {
    startOffset: openQuoteOffset + 1,
    endOffset: closeQuoteOffset,
  }
}

function detectExpressionCompletionMode(
  source: string,
  cursorOffset: number,
  leftContextOffset = cursorOffset
): ExpressionCompletionMode {
  const fnContext = findActiveFunctionCallContext(source, cursorOffset)
  const fnName = String(fnContext?.name || '').trim().toLowerCase()
  const argIndex = Math.max(0, Number(fnContext?.activeParameter || 0))
  const contextOffset = Math.max(0, Math.min(Number(leftContextOffset || 0), source.length))
  const leftContext = source.slice(Math.max(0, contextOffset - 180), contextOffset)
  const isPathAssignment = /\bpath\s*=\s*$/i.test(leftContext)

  if (isPathAssignment) return 'field_path'
  if ((fnName === 'field' || fnName === 'values' || fnName === 'group_aggregate') && argIndex === 0) {
    return 'field_path'
  }
  if (
    (fnName === 'prev' && argIndex === 0)
    || ((fnName === 'inc' || fnName === 'map_inc' || fnName === 'append_unique' || fnName === 'rolling_update') && argIndex === 0)
  ) {
    return 'profile_path'
  }
  return 'default'
}

function buildExpressionCompletionSuggestions(monaco: Monaco, model: any, position: any) {
  const source = String(model?.getValue?.() || '')
  const cursorOffset = Number(model?.getOffsetAt?.(position) || 0)
  const suggestIntent = modelSuggestIntent.get(model)
  if (suggestIntent === 'issue_click') {
    modelSuggestIntent.delete(model)
  }
  const issueFix = findIssueQuickFixAtOffset(model, cursorOffset)
  if (suggestIntent === 'issue_click' && issueFix) {
    const completionKind = (monaco as any)?.languages?.CompletionItemKind || {}
    const start = model.getPositionAt(issueFix.startOffset)
    const end = model.getPositionAt(issueFix.endOffset)
    return {
      suggestions: [
        {
          label: `Replace with "${issueFix.replacement}"`,
          kind: completionKind.Text ?? completionKind.Variable ?? 18,
          insertText: issueFix.replacement,
          detail: 'Suggested fix from validation',
          range: {
            startLineNumber: start.lineNumber,
            endLineNumber: end.lineNumber,
            startColumn: start.column,
            endColumn: end.column,
          },
        },
      ],
    }
  }
  const word = model.getWordUntilPosition(position)
  const defaultRange = {
    startLineNumber: position.lineNumber,
    endLineNumber: position.lineNumber,
    startColumn: word.startColumn,
    endColumn: word.endColumn,
  }
  const contextMode = detectExpressionCompletionMode(source, cursorOffset)
  let mode: ExpressionCompletionMode = contextMode
  let range = defaultRange
  let prefixRaw = String(word.word || '')
  const literalRange = findActiveQuotedLiteralRange(source, cursorOffset)
  if (literalRange) {
    const detectedMode = detectExpressionCompletionMode(source, cursorOffset, literalRange.startOffset)
    if (detectedMode !== 'default') {
      mode = detectedMode
      const start = model.getPositionAt(literalRange.startOffset)
      const end = model.getPositionAt(Math.max(literalRange.startOffset, literalRange.endOffset))
      range = {
        startLineNumber: start.lineNumber,
        endLineNumber: end.lineNumber,
        startColumn: start.column,
        endColumn: end.column,
      }
      prefixRaw = source.slice(literalRange.startOffset, cursorOffset)
    }
  }

  const useRawPathInsert = mode !== 'default'
  const prefix = prefixRaw.toLowerCase()
  const toSearchKey = (label: string): string => {
    if (useRawPathInsert) return String(label || '').toLowerCase()
    const text = String(label || '')
    const open = text.indexOf('(')
    return (open > 0 ? text.slice(0, open) : text).toLowerCase()
  }
  const rankEntry = (entry: ExpressionCompletionEntry): number => {
    if (!prefix) return 100
    const key = toSearchKey(entry.label)
    if (key === prefix) return 0
    if (key.startsWith(prefix)) return 1
    if (key.includes(prefix)) return 2
    const distance = levenshteinDistance(prefix, key)
    return 10 + distance
  }

  let items = exprCompletionEntries
  if (useRawPathInsert) {
    items = exprCompletionEntries.filter((entry) => entry.kind === 'field' || entry.kind === 'path')
    const deduped = new Map<string, ExpressionCompletionEntry>()
    items.forEach((entry) => {
      const key = String(entry.label || '').trim().toLowerCase()
      if (!key) return
      const existing = deduped.get(key)
      if (!existing || (entry.kind === 'path' && existing.kind !== 'path')) {
        deduped.set(key, entry)
      }
    })
    items = Array.from(deduped.values())
  }

  if (prefix) {
    const strict = items.filter((entry) => {
      const key = toSearchKey(entry.label)
      return key.startsWith(prefix) || key.includes(prefix)
    })
    if (strict.length > 0) {
      items = strict
    } else {
      // No direct match: show nearest recommendations so user can pick a fix.
      items = [...items]
        .sort((a, b) => rankEntry(a) - rankEntry(b))
        .slice(0, 12)
    }
  }
  items = [...items].sort((a, b) => rankEntry(a) - rankEntry(b))
  const completionKind = (monaco as any)?.languages?.CompletionItemKind || {}
  const snippetInsertRule = (monaco as any)?.languages?.CompletionItemInsertTextRule?.InsertAsSnippet

  const suggestions = items.map((entry) => ({
    label: entry.label,
    kind:
      entry.kind === 'function'
        ? (completionKind.Function ?? completionKind.Method ?? completionKind.Keyword ?? 17)
        : entry.kind === 'field' || entry.kind === 'path'
          ? (completionKind.Field ?? completionKind.Variable ?? completionKind.Text ?? 18)
          : (completionKind.Keyword ?? completionKind.Text ?? 18),
    insertText: useRawPathInsert ? entry.label : entry.insertText,
    insertTextRules: !useRawPathInsert && entry.insertText.includes('${') && snippetInsertRule !== undefined
      ? snippetInsertRule
      : undefined,
    detail: entry.detail,
    documentation: entry.documentation,
    command: entry.command,
    range,
  }))
  return { suggestions }
}

function normalizeSnippetTemplateText(snippet: string): string {
  return String(snippet || '')
    .replace(/\$\{\d+:([^}]+)\}/g, '$1')
    .replace(/\$\{\d+\|([^}]+)\|\}/g, (_all, choices) => String(choices || '').split(',')[0] || '')
    .replace(/\$\d+/g, '')
}

function escapeSnippetChoiceValue(value: string): string {
  return String(value || '')
    .replace(/\\/g, '\\\\')
    .replace(/\|/g, '\\|')
    .replace(/,/g, '\\,')
    .replace(/\}/g, '\\}')
}

function unescapeSnippetChoiceValue(value: string): string {
  return String(value || '')
    .replace(/\\,/g, ',')
    .replace(/\\\|/g, '|')
    .replace(/\\}/g, '}')
    .replace(/\\\\/g, '\\')
}

function splitSnippetChoiceValues(raw: string): string[] {
  const text = String(raw || '')
  if (!text) return []
  const out: string[] = []
  let current = ''
  let escaped = false
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i]
    if (escaped) {
      current += ch
      escaped = false
      continue
    }
    if (ch === '\\') {
      escaped = true
      continue
    }
    if (ch === ',') {
      out.push(unescapeSnippetChoiceValue(current))
      current = ''
      continue
    }
    current += ch
  }
  out.push(unescapeSnippetChoiceValue(current))
  return out.map((item) => String(item || '').trim()).filter(Boolean)
}

function parseSnippetPlaceholders(snippet: string): SnippetPlaceholder[] {
  const text = String(snippet || '')
  if (!text) return []
  const out: SnippetPlaceholder[] = []
  const seen = new Set<number>()
  const regex = /\$\{(\d+):([^}]+)\}|\$\{(\d+)\|([^}]*)\|\}/g
  let match = regex.exec(text)
  while (match) {
    const textIndexRaw = match[1]
    const textDefaultRaw = match[2]
    const choiceIndexRaw = match[3]
    const choiceRaw = match[4]
    if (textIndexRaw) {
      const idx = Number(textIndexRaw)
      if (Number.isFinite(idx) && !seen.has(idx)) {
        seen.add(idx)
        const defaultValue = String(textDefaultRaw || '')
        out.push({
          index: idx,
          token: String(match[0] || ''),
          kind: 'text',
          defaultValue,
          options: defaultValue ? [defaultValue] : [],
        })
      }
    } else if (choiceIndexRaw) {
      const idx = Number(choiceIndexRaw)
      if (Number.isFinite(idx) && !seen.has(idx)) {
        seen.add(idx)
        const options = splitSnippetChoiceValues(String(choiceRaw || ''))
        out.push({
          index: idx,
          token: String(match[0] || ''),
          kind: 'choice',
          defaultValue: String(options[0] || ''),
          options,
        })
      }
    }
    match = regex.exec(text)
  }
  return out.sort((a, b) => a.index - b.index)
}

function buildExpressionFromSnippet(snippet: string, valuesByIndex: Record<number, string>): string {
  const text = String(snippet || '')
  if (!text) return ''
  const resolved = text
    .replace(/\$\{(\d+):([^}]+)\}/g, (_full, indexRaw, fallbackRaw) => {
      const index = Number(indexRaw)
      const candidate = String(valuesByIndex[index] ?? fallbackRaw ?? '')
      return candidate
    })
    .replace(/\$\{(\d+)\|([^}]*)\|\}/g, (_full, indexRaw, choicesRaw) => {
      const index = Number(indexRaw)
      const choices = splitSnippetChoiceValues(String(choicesRaw || ''))
      const fallback = String(choices[0] || '')
      const candidate = String(valuesByIndex[index] ?? fallback)
      return candidate
    })
    .replace(/\$\d+/g, '')
  return resolved
}

function normalizeExpressionValue(text: string): string {
  const raw = String(text || '').trim()
  if (!raw) return ''
  if (raw.startsWith('=')) return raw
  return `=${raw}`
}

function createBreClause(seed?: Partial<BreClause>): BreClause {
  return {
    id: String(seed?.id || `bre_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`),
    joinWithPrev: (seed?.joinWithPrev || 'and') as BreClauseJoin,
    leftKind: (seed?.leftKind || 'field') as BreClauseOperandKind,
    leftValue: String(seed?.leftValue || ''),
    operator: (seed?.operator || '==') as BreClauseOperator,
    rightKind: (seed?.rightKind || 'literal') as BreClauseOperandKind,
    rightValue: String(seed?.rightValue || ''),
  }
}

function parseTopLevelFunctionCallSource(source: string): { name: string; args: string[] } | null {
  const text = String(source || '').trim()
  if (!text) return null
  const fnMatch = text.match(/^([A-Za-z_]\w*)\s*\(/)
  if (!fnMatch?.[1]) return null
  const name = String(fnMatch[1]).trim()
  const openParenOffset = text.indexOf('(')
  if (openParenOffset < 0) return null
  const closeParenOffset = findMatchingParenOffset(text, openParenOffset)
  if (closeParenOffset <= openParenOffset) return null
  if (text.slice(closeParenOffset + 1).trim()) return null
  const argsSource = text.slice(openParenOffset + 1, closeParenOffset)
  const args = splitTopLevelArgsWithOffsets(argsSource).map((segment) => String(segment.text || '').trim())
  return { name, args }
}

function splitBreConditionClauses(source: string): Array<{ joinWithPrev: BreClauseJoin; text: string }> {
  const text = String(source || '')
  if (!text.trim()) return []
  const out: Array<{ joinWithPrev: BreClauseJoin; text: string }> = []
  const joins: BreClauseJoin[] = []
  let start = 0
  let depthRound = 0
  let depthSquare = 0
  let depthCurly = 0
  let quote: "'" | '"' | null = null
  let escaped = false
  const lower = text.toLowerCase()
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i]
    if (quote) {
      if (escaped) {
        escaped = false
        continue
      }
      if (ch === '\\') {
        escaped = true
        continue
      }
      if (ch === quote) quote = null
      continue
    }
    if (ch === "'" || ch === '"') {
      quote = ch
      continue
    }
    if (ch === '(') depthRound += 1
    else if (ch === ')') depthRound = Math.max(0, depthRound - 1)
    else if (ch === '[') depthSquare += 1
    else if (ch === ']') depthSquare = Math.max(0, depthSquare - 1)
    else if (ch === '{') depthCurly += 1
    else if (ch === '}') depthCurly = Math.max(0, depthCurly - 1)
    if (depthRound !== 0 || depthSquare !== 0 || depthCurly !== 0) continue
    if (lower.startsWith(' and ', i)) {
      out.push({ joinWithPrev: 'and', text: text.slice(start, i).trim() })
      joins.push('and')
      start = i + 5
      i += 4
      continue
    }
    if (lower.startsWith(' or ', i)) {
      out.push({ joinWithPrev: 'and', text: text.slice(start, i).trim() })
      joins.push('or')
      start = i + 4
      i += 3
      continue
    }
  }
  out.push({ joinWithPrev: 'and', text: text.slice(start).trim() })
  return out
    .map((item, idx) => ({
      joinWithPrev: idx === 0 ? 'and' : joins[idx - 1] || 'and',
      text: item.text,
    }))
    .filter((item) => item.text)
}

function findTopLevelComparator(source: string): { operator: BreClauseOperator; index: number } | null {
  const text = String(source || '')
  const operators: BreClauseOperator[] = ['>=', '<=', '!=', '==', '>', '<']
  let depthRound = 0
  let depthSquare = 0
  let depthCurly = 0
  let quote: "'" | '"' | null = null
  let escaped = false
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i]
    if (quote) {
      if (escaped) {
        escaped = false
        continue
      }
      if (ch === '\\') {
        escaped = true
        continue
      }
      if (ch === quote) quote = null
      continue
    }
    if (ch === "'" || ch === '"') {
      quote = ch
      continue
    }
    if (ch === '(') depthRound += 1
    else if (ch === ')') depthRound = Math.max(0, depthRound - 1)
    else if (ch === '[') depthSquare += 1
    else if (ch === ']') depthSquare = Math.max(0, depthSquare - 1)
    else if (ch === '{') depthCurly += 1
    else if (ch === '}') depthCurly = Math.max(0, depthCurly - 1)
    if (depthRound !== 0 || depthSquare !== 0 || depthCurly !== 0) continue

    const rest = text.slice(i)
    const hit = operators.find((op) => rest.startsWith(op))
    if (hit) return { operator: hit, index: i }
  }
  return null
}

function parseBreOperand(text: string): { kind: BreClauseOperandKind; value: string } {
  const raw = String(text || '').trim()
  if (!raw) return { kind: 'literal', value: '' }
  const fieldMatch = raw.match(/^field\s*\(\s*'((?:[^'\\]|\\.)*)'\s*\)$/i)
  if (fieldMatch?.[1] !== undefined) {
    return { kind: 'field', value: decodeSingleQuotedLiteral(fieldMatch[1]) }
  }
  if (/^-?\d+(\.\d+)?$/.test(raw) || /^(true|false|null)$/i.test(raw)) {
    return { kind: 'literal', value: raw }
  }
  const singleQuoted = raw.match(/^'((?:[^'\\]|\\.)*)'$/)
  if (singleQuoted?.[1] !== undefined) {
    return { kind: 'literal', value: decodeSingleQuotedLiteral(singleQuoted[1]) }
  }
  const doubleQuoted = raw.match(/^"((?:[^"\\]|\\.)*)"$/)
  if (doubleQuoted?.[1] !== undefined) {
    return { kind: 'literal', value: String(doubleQuoted[1]).replace(/\\"/g, '"').replace(/\\\\/g, '\\') }
  }
  return { kind: 'raw', value: raw }
}

function parseBreClauseText(text: string, joinWithPrev: BreClauseJoin): BreClause {
  const raw = String(text || '').trim()
  if (!raw) return createBreClause({ joinWithPrev, operator: 'raw', leftKind: 'raw', leftValue: '' })
  const cmp = findTopLevelComparator(raw)
  if (!cmp) {
    return createBreClause({
      joinWithPrev,
      operator: 'raw',
      leftKind: 'raw',
      leftValue: raw,
      rightKind: 'raw',
      rightValue: '',
    })
  }
  const leftText = raw.slice(0, cmp.index).trim()
  const rightText = raw.slice(cmp.index + cmp.operator.length).trim()
  const left = parseBreOperand(leftText)
  const right = parseBreOperand(rightText)
  return createBreClause({
    joinWithPrev,
    operator: cmp.operator,
    leftKind: left.kind,
    leftValue: left.value,
    rightKind: right.kind,
    rightValue: right.value,
  })
}

function parseBreModelFromExpression(expression: string): BreModel {
  const text = String(expression || '').trim()
  if (!text) {
    return {
      clauses: [createBreClause()],
      thenExpr: "'YES'",
      elseExpr: "'NO'",
      parseError: 'Expression is empty',
    }
  }
  const body = text.startsWith('=') ? text.slice(1).trim() : text
  const call = parseTopLevelFunctionCallSource(body)
  if (!call || call.name.toLowerCase() !== 'if_' || call.args.length < 3) {
    return {
      clauses: [createBreClause({ operator: 'raw', leftKind: 'raw', leftValue: body })],
      thenExpr: "'YES'",
      elseExpr: "'NO'",
      parseError: 'BRE parser supports if_(condition, then, else) expressions.',
    }
  }
  const conditionText = String(call.args[0] || '').trim()
  const thenExpr = String(call.args[1] || '').trim() || "'YES'"
  const elseExpr = String(call.args[2] || '').trim() || "'NO'"
  const conditionClauses = splitBreConditionClauses(conditionText)
  const clauses = conditionClauses.length > 0
    ? conditionClauses.map((item) => parseBreClauseText(item.text, item.joinWithPrev))
    : [createBreClause()]
  return {
    clauses,
    thenExpr,
    elseExpr,
    parseError: null,
  }
}

function renderBreLiteral(raw: string): string {
  const text = String(raw || '').trim()
  if (!text) return "''"
  if (/^-?\d+(\.\d+)?$/.test(text) || /^(true|false|null)$/i.test(text)) {
    return text.toLowerCase()
  }
  if ((text.startsWith("'") && text.endsWith("'")) || (text.startsWith('"') && text.endsWith('"'))) {
    return text
  }
  return `'${escapeExprPath(text)}'`
}

function renderBreOperand(kind: BreClauseOperandKind, value: string): string {
  const text = String(value || '').trim()
  if (kind === 'field') {
    return `field('${escapeExprPath(text)}')`
  }
  if (kind === 'literal') {
    return renderBreLiteral(text)
  }
  return text || "''"
}

function buildBreConditionExpression(clause: BreClause): string {
  if (!clause) return 'true'
  if (clause.operator === 'raw') {
    return String(clause.leftValue || '').trim() || 'true'
  }
  const left = renderBreOperand(clause.leftKind, clause.leftValue)
  const right = renderBreOperand(clause.rightKind, clause.rightValue)
  return `${left} ${clause.operator} ${right}`
}

function buildExpressionFromBreModel(model: BreModel): string {
  const clauses = Array.isArray(model?.clauses) ? model.clauses : []
  const condition = (
    clauses.length > 0
      ? clauses
        .map((clause, idx) => {
          const text = buildBreConditionExpression(clause)
          if (idx === 0) return text
          const join = clause.joinWithPrev === 'or' ? 'or' : 'and'
          return `${join} ${text}`
        })
        .join(' ')
      : 'true'
  )
  const thenExpr = String(model?.thenExpr || '').trim() || "'YES'"
  const elseExpr = String(model?.elseExpr || '').trim() || "'NO'"
  return `=if_(${condition}, ${thenExpr}, ${elseExpr})`
}

function parseJsonTemplateExpressionMap(jsonTemplate: string): Record<string, string> {
  const obj = parseJsonTemplateObject(jsonTemplate)
  return Object.entries(obj).reduce((acc, [key, value]) => {
    const keyName = String(key || '').trim()
    if (!keyName) return acc
    if (typeof value === 'string') {
      acc[keyName] = value
      return acc
    }
    try {
      acc[keyName] = JSON.stringify(value, null, 2)
    } catch {
      acc[keyName] = String(value ?? '')
    }
    return acc
  }, {} as Record<string, string>)
}

function convertTemplateLiteralToJsonQuoted(source: string): string {
  let out = ''
  let i = 0
  while (i < source.length) {
    const ch = source[i]
    if (ch !== '`') {
      out += ch
      i += 1
      continue
    }
    i += 1
    let content = ''
    let escaped = false
    while (i < source.length) {
      const curr = source[i]
      if (escaped) {
        if (curr === '`' || curr === '\\') content += curr
        else content += `\\${curr}`
        escaped = false
        i += 1
        continue
      }
      if (curr === '\\') {
        escaped = true
        i += 1
        continue
      }
      if (curr === '`') {
        i += 1
        break
      }
      content += curr
      i += 1
    }
    out += JSON.stringify(content)
  }
  return out
}

function escapeNewlinesInsideQuotedStrings(source: string): string {
  let out = ''
  let quote: '"' | "'" | null = null
  let escaped = false
  for (let i = 0; i < source.length; i += 1) {
    const ch = source[i]
    if (!quote) {
      if (ch === '"' || ch === "'") {
        quote = ch
        out += ch
        continue
      }
      out += ch
      continue
    }
    if (escaped) {
      out += ch
      escaped = false
      continue
    }
    if (ch === '\\') {
      out += ch
      escaped = true
      continue
    }
    if (ch === quote) {
      out += ch
      quote = null
      continue
    }
    if (ch === '\r') {
      if (source[i + 1] === '\n') i += 1
      out += '\\n'
      continue
    }
    if (ch === '\n') {
      out += '\\n'
      continue
    }
    out += ch
  }
  return out
}

function normalizeJsLikeTemplateTextToJson(text: string): string {
  let normalized = String(text || '')
  normalized = convertTemplateLiteralToJsonQuoted(normalized)
  normalized = escapeNewlinesInsideQuotedStrings(normalized)
  normalized = normalized.replace(/([{,]\s*)([A-Za-z_$][\w$]*)(\s*:)/g, '$1"$2"$3')
  normalized = normalized.replace(/,(\s*[}\]])/g, '$1')
  return normalized
}

function parseJsonTemplateLikeValue(jsonTemplate: string): unknown {
  const text = String(jsonTemplate || '').trim()
  if (!text) return {}
  try {
    return JSON.parse(text)
  } catch {
    try {
      const normalized = normalizeJsLikeTemplateTextToJson(text)
      return JSON.parse(normalized)
    } catch {
      return null
    }
  }
}

function normalizeJsonTemplateForStorage(jsonTemplate: string): string {
  const parsed = parseJsonTemplateLikeValue(jsonTemplate)
  if (parsed === null) return String(jsonTemplate || '').trim()
  return JSON.stringify(parsed, null, 2)
}

function parseJsonTemplateObject(jsonTemplate: string): Record<string, unknown> {
  const parsed = parseJsonTemplateLikeValue(jsonTemplate)
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {}
  return parsed as Record<string, unknown>
}

function parseJsonTemplateRows(jsonTemplate: string): UiExpressionBuilderRow[] {
  const obj = parseJsonTemplateObject(jsonTemplate)
  return Object.entries(obj).map(([key, value], idx) => {
    const keyName = String(key || '').trim()
    const isExpression = typeof value === 'string' && String(value || '').trim().startsWith('=')
    const valueType: UiExpressionBuilderRow['valueType'] = isExpression ? 'expression' : 'json'
    let textValue = ''
    if (typeof value === 'string') {
      textValue = value
    } else {
      try {
        textValue = JSON.stringify(value, null, 2)
      } catch {
        textValue = String(value ?? '')
      }
    }
    return {
      id: `ui_sync_${idx}_${keyName || 'key'}`,
      key: keyName,
      expression: normalizeExpressionValue(String(textValue || '')),
      valueType,
    }
  }).filter((row) => row.key)
}

function parseUiBuilderRowValue(row: UiExpressionBuilderRow): unknown {
  const text = String(row.expression || '').trim()
  if ((row.valueType || 'expression') === 'expression') {
    return normalizeExpressionValue(text)
  }
  if (!text) return {}
  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}

function extractSnippetFunctionName(snippet: string): string {
  const text = String(snippet || '').trim()
  if (!text) return ''
  const match = text.match(/^([A-Za-z_]\w*)\s*\(/)
  return String(match?.[1] || '').trim()
}

function stripEnclosingQuotes(token: string): string {
  const text = String(token || '').trim()
  if (!text) return ''
  if (text.length >= 2) {
    const first = text[0]
    const last = text[text.length - 1]
    if ((first === "'" && last === "'") || (first === '"' && last === '"')) {
      return text
        .slice(1, -1)
        .replace(/\\\\/g, '\\')
        .replace(/\\'/g, "'")
        .replace(/\\"/g, '"')
    }
  }
  return text
}

function inferUiExpressionModeAndValue(token: string): { mode: UiExpressionParameterMode; value: string } {
  const text = String(token || '').trim()
  if (!text) {
    return { mode: 'literal', value: '' }
  }
  const fieldMatch = text.match(/^field\(\s*(['"])([\s\S]*)\1\s*\)$/i)
  if (fieldMatch) {
    return { mode: 'field', value: stripEnclosingQuotes(fieldMatch[1] + fieldMatch[2] + fieldMatch[1]) }
  }
  const valuesMatch = text.match(/^values\(\s*(['"])([\s\S]*)\1\s*\)$/i)
  if (valuesMatch) {
    return { mode: 'values', value: stripEnclosingQuotes(valuesMatch[1] + valuesMatch[2] + valuesMatch[1]) }
  }
  if (/^-?\d+(\.\d+)?$/.test(text) || /^(true|false|null)$/i.test(text) || /^(['"]).*\1$/.test(text)) {
    return { mode: 'literal', value: text }
  }
  return { mode: 'raw', value: text }
}

function parseNamedObjLiteralArg(args: string[], name: string): string {
  const key = String(name || '').trim().toLowerCase()
  if (!key) return ''
  for (const arg of args || []) {
    const text = String(arg || '').trim()
    if (!text) continue
    const equalIndex = text.indexOf('=')
    if (equalIndex <= 0) continue
    const lhs = String(text.slice(0, equalIndex) || '').trim().toLowerCase()
    if (lhs !== key) continue
    return stripEnclosingQuotes(text.slice(equalIndex + 1))
  }
  return ''
}

function parseGroupAggregateMetricsFromArg(metricsArg: string): UiGroupAggregateMetric[] {
  const raw = String(metricsArg || '').trim()
  if (!raw) return []
  let metricsSource = raw
  const metricsCall = parseTopLevelFunctionCallSource(raw)
  if (metricsCall && metricsCall.name.toLowerCase() === 'obj') {
    metricsSource = String(metricsCall.args.join(', ') || '').trim()
  }
  const segments = splitTopLevelArgsWithOffsets(metricsSource)
    .map((segment) => String(segment.text || '').trim())
    .filter(Boolean)
  const parsed: UiGroupAggregateMetric[] = []
  segments.forEach((segment, idx) => {
    const equalIndex = segment.indexOf('=')
    if (equalIndex <= 0) return
    const outputName = normalizeUiGroupAggregateOutputName(segment.slice(0, equalIndex))
    const rhs = String(segment.slice(equalIndex + 1) || '').trim()
    const rhsCall = parseTopLevelFunctionCallSource(rhs)
    if (!rhsCall || rhsCall.name.toLowerCase() !== 'obj') return
    const path = parseNamedObjLiteralArg(rhsCall.args, 'path')
    const agg = parseNamedObjLiteralArg(rhsCall.args, 'agg').toLowerCase() || 'sum'
    parsed.push(
      createUiGroupAggregateMetric({
        outputName: outputName || `metric_${idx + 1}`,
        path,
        agg: EXPR_ALLOWED_AGG_VALUES.has(agg) ? agg : 'sum',
      })
    )
  })
  return parsed
}

function isFieldLikePlaceholder(placeholder: SnippetPlaceholder): boolean {
  const text = String(placeholder.defaultValue || '').toLowerCase()
  if (!text) return false
  return /(field|column|path|key_field|condition_field|value_field)/.test(text)
}

function isAggregateLikePlaceholder(placeholder: SnippetPlaceholder): boolean {
  const text = String(placeholder.defaultValue || '').toLowerCase()
  if (!text) return false
  return /\b(agg|aggregate)\b/.test(text)
}

function isOperatorLikePlaceholder(placeholder: SnippetPlaceholder): boolean {
  const text = String(placeholder.defaultValue || '').toLowerCase()
  if (!text) return false
  return /\bop\b|operator/.test(text)
}

function isConditionLikePlaceholder(placeholder: SnippetPlaceholder): boolean {
  const text = String(placeholder.defaultValue || '').toLowerCase()
  if (!text) return false
  return /\bcond\b|\bcondition\b/.test(text)
}

function isBooleanLikePlaceholder(placeholder: SnippetPlaceholder): boolean {
  const text = String(placeholder.defaultValue || '').toLowerCase()
  if (!text) return false
  return /\b(include_null|case_sensitive|enabled|flag|bool|boolean)\b/.test(text)
}

function createUiConditionCluster(seed?: Partial<UiConditionCluster>): UiConditionCluster {
  const rand = Math.random().toString(36).slice(2, 8)
  const now = Date.now().toString(36)
  return {
    id: String(seed?.id || `cluster_${now}_${rand}`),
    joinWithPrev: seed?.joinWithPrev === 'or' ? 'or' : 'and',
  }
}

function createUiConditionClause(clusterId: string, seed?: Partial<UiConditionClause>): UiConditionClause {
  const rand = Math.random().toString(36).slice(2, 8)
  const now = Date.now().toString(36)
  const mode = seed?.rightMode
  const rightMode: UiConditionClauseRightMode = (
    mode === 'field' || mode === 'values' || mode === 'variable' || mode === 'literal' || mode === 'raw'
      ? mode
      : 'literal'
  )
  return {
    id: String(seed?.id || `clause_${now}_${rand}`),
    clusterId: String(seed?.clusterId || clusterId || 'cluster_1'),
    joinWithPrev: seed?.joinWithPrev === 'or' ? 'or' : 'and',
    leftField: String(seed?.leftField || ''),
    operator: String(seed?.operator || '=='),
    rightMode,
    rightValue: String(seed?.rightValue || ''),
  }
}

function createDefaultUiConditionBuilderState(seed?: Partial<UiConditionBuilderState>): UiConditionBuilderState {
  const seedClusters = Array.isArray(seed?.clusters) ? seed.clusters : []
  const clusters: UiConditionCluster[] = []
  const clusterIdSet = new Set<string>()
  seedClusters.forEach((cluster) => {
    const normalized = createUiConditionCluster(cluster)
    const id = String(normalized.id || '').trim()
    if (!id || clusterIdSet.has(id)) return
    clusters.push({ ...normalized, id })
    clusterIdSet.add(id)
  })
  if (clusters.length === 0) {
    const fallback = createUiConditionCluster({ id: 'cluster_1', joinWithPrev: 'and' })
    clusters.push(fallback)
    clusterIdSet.add(fallback.id)
  }

  const seedClauses = Array.isArray(seed?.clauses) ? seed.clauses : []
  const clauses: UiConditionClause[] = []
  const clauseIdSet = new Set<string>()
  seedClauses.forEach((clause) => {
    const clusterId = clusterIdSet.has(String(clause.clusterId || ''))
      ? String(clause.clusterId)
      : clusters[0].id
    const normalized = createUiConditionClause(clusterId, clause)
    const id = String(normalized.id || '').trim()
    if (!id || clauseIdSet.has(id)) return
    clauses.push({ ...normalized, id })
    clauseIdSet.add(id)
  })
  if (clauses.length === 0) {
    clauses.push(createUiConditionClause(clusters[0].id, { id: 'clause_1', joinWithPrev: 'and' }))
  }

  const referencedClusters = new Set(clauses.map((clause) => clause.clusterId))
  const normalizedClusters = clusters.filter((cluster) => referencedClusters.has(cluster.id))
  if (normalizedClusters.length === 0) {
    const fallback = createUiConditionCluster({ id: 'cluster_1', joinWithPrev: 'and' })
    return {
      clusters: [fallback],
      clauses: clauses.map((clause, index) => createUiConditionClause(fallback.id, {
        ...clause,
        id: index === 0 ? 'clause_1' : clause.id,
        clusterId: fallback.id,
      })),
    }
  }

  return {
    clusters: normalizedClusters,
    clauses,
  }
}

function createUiExpressionVariableSpec(seed?: Partial<UiExpressionVariableSpec>): UiExpressionVariableSpec {
  const rand = Math.random().toString(36).slice(2, 9)
  const now = Date.now().toString(36)
  const name = String(seed?.name || '').trim()
  return {
    id: String(seed?.id || `uivar_${now}_${rand}`),
    name: name || `var_${rand.slice(0, 4)}`,
    value: String(seed?.value || ''),
  }
}

function normalizeUiGroupAggregateOutputName(name: string): string {
  const normalized = String(name || '').trim().replace(/\s+/g, '_').replace(/[^A-Za-z0-9_]/g, '_')
  return normalized || 'metric'
}

function createUiGroupAggregateMetric(seed?: Partial<UiGroupAggregateMetric>): UiGroupAggregateMetric {
  const rand = Math.random().toString(36).slice(2, 9)
  const now = Date.now().toString(36)
  return {
    id: String(seed?.id || `uimetric_${now}_${rand}`),
    outputName: normalizeUiGroupAggregateOutputName(String(seed?.outputName || 'metric')),
    path: String(seed?.path || ''),
    agg: String(seed?.agg || 'sum'),
  }
}

function defaultUiExpressionParameterMode(placeholder: SnippetPlaceholder): UiExpressionParameterMode {
  if (placeholder.kind === 'choice') return 'raw'
  if (isFieldLikePlaceholder(placeholder)) return 'field'
  return 'literal'
}

function toExpressionLiteralToken(value: string): string {
  const raw = String(value || '').trim()
  if (!raw) return "''"
  if (/^-?\d+(\.\d+)?$/.test(raw)) return raw
  if (/^(true|false|null)$/i.test(raw)) return raw.toLowerCase()
  if ((raw.startsWith("'") && raw.endsWith("'")) || (raw.startsWith('"') && raw.endsWith('"'))) {
    return raw
  }
  return `'${raw.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`
}

function resolveUiExpressionParameterToken(
  mode: UiExpressionParameterMode,
  rawValue: string,
  variables: UiExpressionVariableSpec[]
): string {
  const value = String(rawValue || '').trim()
  if (mode === 'field') {
    return value ? `field('${value.replace(/'/g, "\\'")}')` : "field('')"
  }
  if (mode === 'values') {
    return value ? `values('${value.replace(/'/g, "\\'")}')` : "values('')"
  }
  if (mode === 'variable') {
    const variableName = value
    const variable = (variables || []).find((item) => String(item.name || '').trim() === variableName)
    const variableValue = String(variable?.value || '').trim()
    return variableValue || "''"
  }
  if (mode === 'raw') {
    return value || "''"
  }
  return toExpressionLiteralToken(value)
}

function enrichFunctionSnippetWithFieldChoices(snippet: string, fieldNames: string[]): string {
  const fields = uniqueFieldNames(fieldNames || []).slice(0, 40)
  if (fields.length === 0) return String(snippet || '')
  return String(snippet || '').replace(/\$\{(\d+):([^}]+)\}/g, (_full, indexRaw, placeholderRaw) => {
    const index = String(indexRaw || '')
    const placeholder = String(placeholderRaw || '')
    const normalized = placeholder.toLowerCase()
    if (!/(field|column|path)/.test(normalized)) {
      return `\${${index}:${placeholder}}`
    }
    const options = uniqueFieldNames([placeholder, ...fields]).slice(0, 40)
    const choiceList = options.map(escapeSnippetChoiceValue).join(',')
    if (!choiceList) return `\${${index}:${placeholder}}`
    return `\${${index}|${choiceList}|}`
  })
}

function parseFunctionLabelSignature(label: string): { name: string; parameters: string[] } | null {
  const text = String(label || '').trim()
  if (!text) return null
  const open = text.indexOf('(')
  const close = text.lastIndexOf(')')
  if (open <= 0 || close <= open) return null
  const name = text.slice(0, open).trim()
  if (!/^[A-Za-z_]\w*$/.test(name)) return null
  const rawParams = text.slice(open + 1, close).trim()
  const parameters = rawParams
    ? rawParams.split(',').map((p) => p.trim()).filter(Boolean)
    : []
  return { name, parameters }
}

function buildFunctionSignatureMap(entries: ExpressionCompletionEntry[]): Map<string, { name: string; label: string; parameters: string[]; documentation: string }> {
  const map = new Map<string, { name: string; label: string; parameters: string[]; documentation: string }>()
  ;(entries || []).forEach((entry) => {
    if (!entry || entry.kind !== 'function') return
    const parsed = parseFunctionLabelSignature(entry.label)
    if (!parsed?.name) return
    const key = parsed.name.toLowerCase()
    if (map.has(key)) return
    const template = normalizeSnippetTemplateText(entry.insertText)
    map.set(key, {
      name: parsed.name,
      label: `${parsed.name}(${parsed.parameters.join(', ')})`,
      parameters: parsed.parameters.length > 0 ? parsed.parameters : ['arg1'],
      documentation: `Template: \`${template}\`\n\nUse Tab to move across parameters.`,
    })
  })
  return map
}

function findActiveFunctionCallContext(source: string, offset: number): { name: string; activeParameter: number } | null {
  const stack: Array<{ name: string | null; activeParameter: number }> = []
  let quote: '"' | "'" | null = null
  let escaped = false
  for (let i = 0; i < Math.min(source.length, Math.max(0, offset)); i += 1) {
    const ch = source[i]
    if (quote) {
      if (escaped) {
        escaped = false
        continue
      }
      if (ch === '\\') {
        escaped = true
        continue
      }
      if (ch === quote) {
        quote = null
      }
      continue
    }
    if (ch === '"' || ch === "'") {
      quote = ch
      continue
    }
    if (ch === '(') {
      let j = i - 1
      while (j >= 0 && /\s/.test(source[j])) j -= 1
      let end = j
      while (j >= 0 && /[A-Za-z0-9_]/.test(source[j])) j -= 1
      const candidate = end >= 0 ? source.slice(j + 1, end + 1).trim() : ''
      const name = /^[A-Za-z_]\w*$/.test(candidate) ? candidate : null
      stack.push({ name, activeParameter: 0 })
      continue
    }
    if (ch === ',') {
      if (stack.length > 0) {
        stack[stack.length - 1].activeParameter += 1
      }
      continue
    }
    if (ch === ')') {
      stack.pop()
      continue
    }
  }
  for (let i = stack.length - 1; i >= 0; i -= 1) {
    const frame = stack[i]
    if (frame?.name) {
      return { name: frame.name, activeParameter: Math.max(0, frame.activeParameter) }
    }
  }
  return null
}

function attachExpressionAutoSuggest(editor: any): void {
  const applyAutoCorrectEdits = (
    rangeStartOffset: number,
    rangeEndOffset: number,
    replacementText: string,
    cursorOffsetAfter: number
  ): void => {
    const model = editor.getModel?.()
    if (!model) return
    const safeStart = Math.max(0, Math.min(Number(rangeStartOffset || 0), Number(rangeEndOffset || 0)))
    const safeEnd = Math.max(safeStart, Math.max(Number(rangeStartOffset || 0), Number(rangeEndOffset || 0)))
    const startPos = model.getPositionAt(safeStart)
    const endPos = model.getPositionAt(safeEnd)
    const targetOffset = Math.max(0, Number(cursorOffsetAfter || 0))
    editor.pushUndoStop?.()
    editor.executeEdits('opsfabric-autocorrect', [
      {
        range: {
          startLineNumber: startPos.lineNumber,
          startColumn: startPos.column,
          endLineNumber: endPos.lineNumber,
          endColumn: endPos.column,
        },
        text: String(replacementText || ''),
        forceMoveMarkers: true,
      },
    ])
    editor.setPosition?.(model.getPositionAt(targetOffset))
    editor.pushUndoStop?.()
  }

  const autoCorrectFunctionNameAtCursor = (): boolean => {
    const model = editor.getModel?.()
    const position = editor.getPosition?.()
    if (!model || !position) return false
    const source = String(model.getValue?.() || '')
    const cursorOffset = Number(model.getOffsetAt?.(position) || 0)
    if (cursorOffset <= 0 || source[cursorOffset - 1] !== '(') return false

    let idx = cursorOffset - 2
    while (idx >= 0 && /\s/.test(source[idx])) idx -= 1
    const end = idx
    while (idx >= 0 && /[A-Za-z0-9_]/.test(source[idx])) idx -= 1
    const start = idx + 1
    if (end < start) return false

    const currentName = String(source.slice(start, end + 1) || '').trim()
    if (!/^[A-Za-z_]\w*$/.test(currentName)) return false

    const validationContext = getExpressionValidationContext()
    const functionCandidates = Array.from(validationContext.allowedFunctionNames || [])
    const currentLower = currentName.toLowerCase()
    if (validationContext.allowedFunctionNames.has(currentLower)) return false

    const suggestion = findClosestSuggestion(currentLower, functionCandidates)
    if (!suggestion || suggestion.toLowerCase() === currentLower) return false

    const nextCursorOffset = cursorOffset + (suggestion.length - currentName.length)
    applyAutoCorrectEdits(start, end + 1, suggestion, nextCursorOffset)
    return true
  }

  const autoCorrectFieldLiteralAtCursor = (typedChar: string): boolean => {
    if (typedChar !== "'" && typedChar !== '"') return false
    const model = editor.getModel?.()
    const position = editor.getPosition?.()
    if (!model || !position) return false
    const source = String(model.getValue?.() || '')
    const cursorOffset = Number(model.getOffsetAt?.(position) || 0)
    if (cursorOffset <= 1 || source[cursorOffset - 1] !== typedChar) return false

    let openQuoteOffset = -1
    let escaped = false
    for (let idx = cursorOffset - 2; idx >= 0; idx -= 1) {
      const ch = source[idx]
      if (escaped) {
        escaped = false
        continue
      }
      if (ch === '\\') {
        escaped = true
        continue
      }
      if (ch === typedChar) {
        openQuoteOffset = idx
        break
      }
      if (ch === '\n') break
    }
    if (openQuoteOffset < 0) return false

    const rawLiteral = String(source.slice(openQuoteOffset + 1, cursorOffset - 1) || '')
    const fieldName = rawLiteral.trim()
    if (!fieldName) return false

    const leftContext = source.slice(Math.max(0, openQuoteOffset - 180), openQuoteOffset)
    const isFieldContext = /\b(field|values|group_aggregate)\s*\(\s*$/i.test(leftContext)
      || /\bpath\s*=\s*$/i.test(leftContext)
    if (!isFieldContext) return false

    const validationContext = getExpressionValidationContext()
    if (isAllowedFieldName(fieldName, validationContext)) return false
    const fieldCandidates = Array.from(validationContext.allowedFieldNames || [])
    const suggestion = findClosestSuggestion(fieldName, fieldCandidates)
    if (!suggestion || suggestion === fieldName) return false

    const literalStart = openQuoteOffset + 1
    const literalEnd = cursorOffset - 1
    const nextCursorOffset = cursorOffset + (suggestion.length - fieldName.length)
    applyAutoCorrectEdits(literalStart, literalEnd, suggestion, nextCursorOffset)
    return true
  }

  const typedDisposable = editor.onDidType((typedText: string) => {
    const text = String(typedText || '')
    if (!text) return
    const lastChar = text[text.length - 1]
    if (lastChar === '(') {
      const corrected = autoCorrectFunctionNameAtCursor()
      if (corrected) {
        editor.trigger('keyboard', 'editor.action.triggerParameterHints', {})
      }
    } else if (lastChar === "'" || lastChar === '"') {
      autoCorrectFieldLiteralAtCursor(lastChar)
    }
    if (!/[A-Za-z0-9_.'"(]/.test(lastChar)) return
    editor.trigger('keyboard', 'editor.action.triggerSuggest', {})
  })

  editor.onDidDispose(() => {
    typedDisposable.dispose()
  })
}

function ensureExpressionLanguage(monaco: Monaco): void {
  ensureCustomEditorTheme(monaco)
  if (!exprLanguageRegistered) {
    monaco.languages.register({ id: EXPR_LANGUAGE_ID })
    monaco.languages.setLanguageConfiguration(EXPR_LANGUAGE_ID, {
      brackets: [['(', ')'], ['[', ']'], ['{', '}']],
      autoClosingPairs: [
        { open: '(', close: ')' },
        { open: '[', close: ']' },
        { open: '{', close: '}' },
        { open: "'", close: "'" },
        { open: '"', close: '"' },
      ],
      surroundingPairs: [
        { open: '(', close: ')' },
        { open: '[', close: ']' },
        { open: '{', close: '}' },
        { open: "'", close: "'" },
        { open: '"', close: '"' },
      ],
    })
    monaco.languages.setMonarchTokensProvider(EXPR_LANGUAGE_ID, {
      tokenizer: {
        root: [
          [/\s+/, 'white'],
          [/\b[A-Za-z_][\w]*(?=\s*=)/, 'parameter.name'],
          [/\b(field|values|prev|path)\b(?=\s*\()/, 'field.function'],
          [/\b[A-Za-z_][\w]*(?=\s*\()/, 'entity.name.function'],
          [EXPR_LITERAL_SINGLE_QUOTED_PATTERN, 'string.literal.value'],
          [EXPR_FIELD_SINGLE_QUOTED_PATTERN, 'variable.field'],
          [/'(?:[^'\\]|\\.)*'/, 'variable.field'],
          [/"(?:[^"\\]|\\.)*"/, 'string'],
          [/-?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?/, 'number'],
          [/[+\-*/%<>=!&|?:.,]+/, 'operator'],
          [/[{}[\]()]/, 'delimiter.bracket'],
          [/\b(true|false|null)\b/, 'keyword'],
          [/[A-Za-z_][\w.]*/, 'identifier'],
        ],
      },
    })
    monaco.languages.registerCompletionItemProvider(EXPR_LANGUAGE_ID, {
      triggerCharacters: ['.', '(', "'", '"', '_'],
      provideCompletionItems(model: any, position: any) {
        return buildExpressionCompletionSuggestions(monaco, model, position)
      },
    })
    monaco.languages.registerSignatureHelpProvider(EXPR_LANGUAGE_ID, {
      signatureHelpTriggerCharacters: ['(', ','],
      signatureHelpRetriggerCharacters: [','],
      provideSignatureHelp(model: any, position: any) {
        const source = String(model?.getValue?.() || '')
        const offset = Number(model?.getOffsetAt?.(position) || 0)
        const context = findActiveFunctionCallContext(source, offset)
        if (!context?.name) {
          return { value: { signatures: [], activeSignature: 0, activeParameter: 0 }, dispose: () => {} }
        }
        const signature = exprFunctionSignatureMap.get(String(context.name).toLowerCase())
        if (!signature) {
          return { value: { signatures: [], activeSignature: 0, activeParameter: 0 }, dispose: () => {} }
        }
        return {
          value: {
            signatures: [
              {
                label: signature.label,
                documentation: { value: signature.documentation },
                parameters: signature.parameters.map((param) => ({ label: param })),
              },
            ],
            activeSignature: 0,
            activeParameter: Math.min(context.activeParameter, Math.max(0, signature.parameters.length - 1)),
          },
          dispose: () => {},
        }
      },
    })
    exprLanguageRegistered = true
  }
}

function ensureJsonTemplateLanguage(monaco: Monaco): void {
  ensureCustomEditorTheme(monaco)
  if (jsonTemplateLanguageRegistered) return
  monaco.languages.register({ id: JSON_TEMPLATE_LANGUAGE_ID })
  monaco.languages.setLanguageConfiguration(JSON_TEMPLATE_LANGUAGE_ID, {
    brackets: [['{', '}'], ['[', ']'], ['(', ')']],
    autoClosingPairs: [
      { open: '{', close: '}' },
      { open: '[', close: ']' },
      { open: '(', close: ')' },
      { open: '"', close: '"' },
      { open: "'", close: "'" },
    ],
    surroundingPairs: [
      { open: '{', close: '}' },
      { open: '[', close: ']' },
      { open: '(', close: ')' },
      { open: '"', close: '"' },
      { open: "'", close: "'" },
    ],
  })
  monaco.languages.setMonarchTokensProvider(JSON_TEMPLATE_LANGUAGE_ID, {
    tokenizer: {
      root: [
        [/\s+/, 'white'],
        [/[{}[\]()]/, 'delimiter.bracket'],
        [/"(?:[^"\\]|\\.)*"(?=\s*:)/, 'key'],
        [/"=/, { token: 'string.expression.quote', next: '@exprstring' }],
        [/"/, { token: 'string', next: '@string' }],
        [/-?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?/, 'number'],
        [/\b(true|false|null)\b/, 'keyword'],
        [/[,:]/, 'delimiter'],
      ],
      string: [
        [/[^\\"]+/, 'string'],
        [/\\./, 'string.escape'],
        [/"/, { token: 'string', next: '@pop' }],
      ],
      exprstring: [
        [/\s+/, 'white'],
        [/\b[A-Za-z_][\w]*(?=\s*=)/, 'parameter.name'],
        [/\b(field|values|prev|path)\b(?=\s*\()/, 'field.function'],
        [/\b[A-Za-z_][\w]*(?=\s*\()/, 'entity.name.function'],
        [EXPR_LITERAL_SINGLE_QUOTED_PATTERN, 'string.literal.value'],
        [EXPR_FIELD_SINGLE_QUOTED_PATTERN, 'variable.field'],
        [/'(?:[^'\\]|\\.)*'/, 'variable.field'],
        [/"(?:[^"\\]|\\.)*"/, 'string'],
        [/-?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?/, 'number'],
        [/[+\-*/%<>=!&|?:.,]+/, 'operator'],
        [/[{}[\]()]/, 'delimiter.bracket'],
        [/\b[A-Za-z_][\w.]*\b/, 'string.expression'],
        [/\\./, 'string.escape'],
        [/"/, { token: 'string.expression.quote', next: '@pop' }],
        [/./, 'string.expression'],
      ],
    },
  })
  monaco.languages.registerCompletionItemProvider(JSON_TEMPLATE_LANGUAGE_ID, {
    triggerCharacters: ['.', '(', "'", '"', '_'],
    provideCompletionItems(model: any, position: any) {
      return buildExpressionCompletionSuggestions(monaco, model, position)
    },
  })
  monaco.languages.registerSignatureHelpProvider(JSON_TEMPLATE_LANGUAGE_ID, {
    signatureHelpTriggerCharacters: ['(', ','],
    signatureHelpRetriggerCharacters: [','],
    provideSignatureHelp(model: any, position: any) {
      const source = String(model?.getValue?.() || '')
      const offset = Number(model?.getOffsetAt?.(position) || 0)
      const context = findActiveFunctionCallContext(source, offset)
      if (!context?.name) {
        return { value: { signatures: [], activeSignature: 0, activeParameter: 0 }, dispose: () => {} }
      }
      const signature = exprFunctionSignatureMap.get(String(context.name).toLowerCase())
      if (!signature) {
        return { value: { signatures: [], activeSignature: 0, activeParameter: 0 }, dispose: () => {} }
      }
      return {
        value: {
          signatures: [
            {
              label: signature.label,
              documentation: { value: signature.documentation },
              parameters: signature.parameters.map((param) => ({ label: param })),
            },
          ],
          activeSignature: 0,
          activeParameter: Math.min(context.activeParameter, Math.max(0, signature.parameters.length - 1)),
        },
        dispose: () => {},
      }
    },
  })
  jsonTemplateLanguageRegistered = true
}

type ExpressionValidationIssue = {
  startOffset: number
  endOffset: number
  message: string
  severity: 'error' | 'warning'
}

function decodeJsonStringContent(raw: string): string {
  try {
    return JSON.parse(`"${raw}"`)
  } catch {
    return raw
  }
}

function tryExtractJsonParseErrorOffset(message: string): number | null {
  const text = String(message || '')
  const m = text.match(/position\s+(\d+)/i)
  if (!m) return null
  const value = Number(m[1])
  if (!Number.isFinite(value) || value < 0) return null
  return Math.floor(value)
}

function validateJsonTemplateSyntax(text: string): ExpressionValidationIssue[] {
  const source = String(text || '')
  const trimmed = source.trim()
  if (!trimmed) return []
  if (parseJsonTemplateLikeValue(source) !== null) return []
  try {
    JSON.parse(source)
    return []
  } catch (err: any) {
    const rawMessage = String(err?.message || 'Invalid JSON syntax')
    const pos = tryExtractJsonParseErrorOffset(rawMessage)
    const startOffset = Math.max(0, Math.min(source.length, pos ?? 0))
    const endOffset = Math.max(startOffset + 1, Math.min(source.length, startOffset + 1))
    return [
      {
        startOffset,
        endOffset,
        message: rawMessage,
        severity: 'error',
      },
    ]
  }
}

function extractExpressionStringRanges(text: string): Array<{ value: string; startOffset: number; endOffset: number }> {
  const out: Array<{ value: string; startOffset: number; endOffset: number }> = []
  let inString = false
  let quote: '"' | "'" | '`' | null = null
  let escaped = false
  let stringStart = -1
  let buffer = ''

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i]
    if (!inString) {
      if (ch === '"' || ch === "'" || ch === '`') {
        inString = true
        quote = ch
        escaped = false
        stringStart = i
        buffer = ''
      }
      continue
    }
    if (escaped) {
      buffer += ch
      escaped = false
      continue
    }
    if (ch === '\\') {
      buffer += ch
      escaped = true
      continue
    }
    if (ch === quote) {
      const decoded = quote === '"' ? decodeJsonStringContent(buffer) : buffer
      if (decoded.startsWith('=')) {
        out.push({
          value: decoded,
          startOffset: stringStart + 1,
          endOffset: i,
        })
      }
      inString = false
      quote = null
      escaped = false
      buffer = ''
      stringStart = -1
      continue
    }
    buffer += ch
  }

  return out
}

function collapseExpressionWhitespace(raw: string): string {
  const source = String(raw || '').trim()
  if (!source) return ''
  let out = ''
  let quote: '"' | "'" | null = null
  let escaped = false
  let pendingSpace = false

  const appendSpaceIfNeeded = (nextChar: string) => {
    if (!pendingSpace) return
    pendingSpace = false
    if (!out) return
    const last = out[out.length - 1]
    if (!last || /\s/.test(last)) return
    if (/[([{.]/.test(last)) return
    if (/[)\]},.:]/.test(nextChar)) return
    out += ' '
  }

  for (let i = 0; i < source.length; i += 1) {
    const ch = source[i]
    if (quote) {
      out += ch
      if (escaped) {
        escaped = false
        continue
      }
      if (ch === '\\') {
        escaped = true
        continue
      }
      if (ch === quote) quote = null
      continue
    }
    if (ch === '"' || ch === "'") {
      appendSpaceIfNeeded(ch)
      quote = ch
      out += ch
      continue
    }
    if (/\s/.test(ch)) {
      pendingSpace = true
      continue
    }
    appendSpaceIfNeeded(ch)
    out += ch
  }
  return out.trim()
}

function findNextNonWhitespaceChar(source: string, start: number): string {
  for (let i = Math.max(0, start); i < source.length; i += 1) {
    const ch = source[i]
    if (!/\s/.test(ch)) return ch
  }
  return ''
}

function getLastNonWhitespaceChar(source: string): string {
  for (let i = source.length - 1; i >= 0; i -= 1) {
    const ch = source[i]
    if (!/\s/.test(ch)) return ch
  }
  return ''
}

function formatStructuredLiteral(source: string): string {
  const text = collapseExpressionWhitespace(source)
  if (!text) return ''

  let out = ''
  let quote: '"' | "'" | null = null
  let escaped = false
  let depthCurly = 0
  let depthSquare = 0
  let depthRound = 0
  const indentUnit = '  '

  const blockDepth = () => Math.max(0, depthCurly + depthSquare)
  const appendIndent = () => {
    out += indentUnit.repeat(blockDepth())
  }
  const appendNewline = () => {
    out = out.replace(/[ \t]+$/g, '')
    if (!out.endsWith('\n')) out += '\n'
    appendIndent()
  }
  const appendTokenSpaceIfNeeded = () => {
    if (!out) return
    const last = out[out.length - 1]
    if (last === ' ' || last === '\n') return
    out += ' '
  }

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i]
    if (quote) {
      out += ch
      if (escaped) {
        escaped = false
        continue
      }
      if (ch === '\\') {
        escaped = true
        continue
      }
      if (ch === quote) quote = null
      continue
    }

    if (ch === '"' || ch === "'") {
      quote = ch
      out += ch
      continue
    }

    if (/\s/.test(ch)) {
      appendTokenSpaceIfNeeded()
      continue
    }

    if (ch === '{' || ch === '[') {
      out += ch
      if (ch === '{') depthCurly += 1
      else depthSquare += 1
      const next = findNextNonWhitespaceChar(text, i + 1)
      const isEmptyBlock = (ch === '{' && next === '}') || (ch === '[' && next === ']')
      if (!isEmptyBlock && next) appendNewline()
      continue
    }

    if (ch === '}') {
      depthCurly = Math.max(0, depthCurly - 1)
      if (getLastNonWhitespaceChar(out) !== '{') appendNewline()
      out += ch
      continue
    }

    if (ch === ']') {
      depthSquare = Math.max(0, depthSquare - 1)
      if (getLastNonWhitespaceChar(out) !== '[') appendNewline()
      out += ch
      continue
    }

    if (ch === '(') {
      depthRound += 1
      out += ch
      continue
    }

    if (ch === ')') {
      depthRound = Math.max(0, depthRound - 1)
      out += ch
      continue
    }

    if (ch === ',') {
      out += ','
      if (depthRound === 0) appendNewline()
      else out += ' '
      continue
    }

    if (ch === ':') {
      out = out.replace(/[ \t]+$/g, '')
      out += ': '
      continue
    }

    out += ch
  }

  return String(out || '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim()
}

function indentMultilineExpression(text: string, depth: number, indentUnit = '  '): string {
  const prefix = indentUnit.repeat(Math.max(0, depth))
  return String(text || '')
    .split('\n')
    .map((line) => `${prefix}${line}`)
    .join('\n')
}

function shouldMultilineFunctionCall(name: string, args: string[], inlineCandidate: string): boolean {
  const fn = String(name || '').trim().toLowerCase()
  const forceMultiline = new Set([
    'if_',
    'group_aggregate',
    'rolling_update',
    'agg_if',
    'count_if',
    'sum_if',
    'mean_if',
    'min_if',
    'max_if',
    'distinct_if',
    'distinct_count_if',
    'count_non_null_if',
  ])
  if (forceMultiline.has(fn)) return true
  if (args.length >= 4) return true
  if (inlineCandidate.length > 96) return true
  return args.some((arg) => {
    const text = String(arg || '')
    if (text.includes('\n')) return true
    if (text.length > 44) return true
    return false
  })
}

function formatExpressionBody(source: string): string {
  const text = collapseExpressionWhitespace(source)
  if (!text) return ''
  const call = parseTopLevelFunctionCallSource(text)
  if (!call) return text

  const formattedArgs = call.args.map((arg) => formatExpressionBody(String(arg || '')))
  const inline = `${call.name}(${formattedArgs.join(', ')})`
  if (!formattedArgs.length || !shouldMultilineFunctionCall(call.name, formattedArgs, inline)) {
    return inline
  }

  const argsBlock = formattedArgs
    .map((arg) => indentMultilineExpression(arg, 1))
    .join(',\n')
  return `${call.name}(\n${argsBlock}\n)`
}

function beautifyExpressionText(rawExpression: string): string {
  const source = String(rawExpression || '').trim()
  if (!source) return ''
  const hasEqualsPrefix = source.startsWith('=')
  const body = hasEqualsPrefix ? source.slice(1).trim() : source
  if (!body) return hasEqualsPrefix ? '=' : ''

  // Support users who paste JSON object templates into Expression mode.
  // If it is valid JSON and not a normal "=expr", apply JSON+expression beautify.
  if (!hasEqualsPrefix && /^[\[{]/.test(body)) {
    const parsed = parseJsonTemplateLikeValue(body)
    if (parsed !== null) {
      const beautified = beautifyJsonTemplateNode(parsed)
      return JSON.stringify(beautified, null, 2)
    }
    // Not valid JSON/JS-like template; continue with regular expression formatting.
  }

  const parsedTopLevelCall = parseTopLevelFunctionCallSource(body)
  const formattedBody = (
    parsedTopLevelCall
      ? formatExpressionBody(body)
      : /^[\[{]/.test(body)
        ? formatStructuredLiteral(body)
        : collapseExpressionWhitespace(body)
  )
  const normalized = String(formattedBody || body)
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim()
  return hasEqualsPrefix ? `=${normalized}` : normalized
}

function beautifyExpressionForJsonString(rawExpression: string): string {
  const formatted = beautifyExpressionText(rawExpression)
  const hasEqualsPrefix = formatted.startsWith('=')
  const body = hasEqualsPrefix ? formatted.slice(1) : formatted
  const compact = collapseExpressionWhitespace(body)
  return hasEqualsPrefix ? `=${compact}` : compact
}

function toJsPropertyKey(key: string): string {
  const normalized = String(key || '')
  return JSON.stringify(normalized)
}

function toJsViewTemplateText(
  value: unknown,
  depth = 0,
  options?: {
    beautifyExpressions?: boolean
  }
): string {
  const indentUnit = '  '
  const indent = indentUnit.repeat(Math.max(0, depth))
  const nextIndent = indentUnit.repeat(Math.max(0, depth + 1))
  const beautifyExpressions = options?.beautifyExpressions !== false

  if (typeof value === 'string') {
    const text = String(value || '')
    if (text.trim().startsWith('=')) {
      const expressionText = beautifyExpressions ? beautifyExpressionText(text) : text
      const escaped = expressionText
        .replace(/\\/g, '\\\\')
        .replace(/"/g, '\\"')
      return `"${escaped}"`
    }
    return JSON.stringify(text)
  }
  if (typeof value === 'number' || typeof value === 'boolean' || value === null) {
    return JSON.stringify(value)
  }
  if (Array.isArray(value)) {
    if (value.length === 0) return '[]'
    const rows = value.map((item) => `${nextIndent}${toJsViewTemplateText(item, depth + 1, options)}`)
    return `[\n${rows.join(',\n')}\n${indent}]`
  }
  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
    if (entries.length === 0) return '{}'
    const rows = entries.map(([key, child]) =>
      `${nextIndent}${toJsPropertyKey(key)}: ${toJsViewTemplateText(child, depth + 1, options)}`
    )
    return `{\n${rows.join(',\n')}\n${indent}}`
  }
  return JSON.stringify(value)
}

function containsExpressionWithMultilineText(value: unknown): boolean {
  if (typeof value === 'string') {
    const text = String(value || '')
    return text.trim().startsWith('=') && /[\r\n]/.test(text)
  }
  if (Array.isArray(value)) {
    return value.some((item) => containsExpressionWithMultilineText(item))
  }
  if (value && typeof value === 'object') {
    return Object.values(value as Record<string, unknown>).some((child) =>
      containsExpressionWithMultilineText(child)
    )
  }
  return false
}

function normalizeStoredJsonTemplateForEditor(jsonTemplate: string): string {
  const source = String(jsonTemplate || '')
  if (!source.includes('\\n')) return source
  const parsed = parseJsonTemplateLikeValue(source)
  if (parsed === null) return source
  if (!containsExpressionWithMultilineText(parsed)) return source
  return toJsViewTemplateText(parsed, 0, { beautifyExpressions: false })
}

function beautifyJsonTemplateNode(value: unknown): unknown {
  if (typeof value === 'string') {
    const text = String(value || '').trim()
    if (!text.startsWith('=')) return value
    return beautifyExpressionForJsonString(text)
  }
  if (Array.isArray(value)) {
    return value.map((item) => beautifyJsonTemplateNode(item))
  }
  if (value && typeof value === 'object') {
    const obj = value as Record<string, unknown>
    const next: Record<string, unknown> = {}
    Object.entries(obj).forEach(([key, child]) => {
      next[key] = beautifyJsonTemplateNode(child)
    })
    return next
  }
  return value
}

function beautifyJsonTemplateText(raw: string, style: CustomBeautifyStyle = 'json'): string {
  const source = String(raw || '').trim()
  if (!source) return style === 'js_view' ? '{\n}' : '{\n}'
  const parsed = parseJsonTemplateLikeValue(source)
  if (parsed === null) throw new Error('Invalid JSON/JS template syntax')
  const beautified = beautifyJsonTemplateNode(parsed)
  if (style === 'js_view') {
    return toJsViewTemplateText(beautified, 0)
  }
  return JSON.stringify(beautified, null, 2)
}

type ExpressionFunctionCallRef = {
  name: string
  nameStart: number
  openParenOffset: number
  closeParenOffset: number
  argsSource: string
}

type ParsedArgSegment = {
  text: string
  startOffset: number
  endOffset: number
}

function decodeSingleQuotedLiteral(raw: string): string {
  return String(raw || '')
    .replace(/\\\\/g, '\\')
    .replace(/\\'/g, "'")
}

function buildQuoteMask(source: string): boolean[] {
  const mask = Array(source.length).fill(false)
  let quote: '"' | "'" | null = null
  let escaped = false
  for (let i = 0; i < source.length; i += 1) {
    const ch = source[i]
    if (quote) {
      mask[i] = true
      if (escaped) {
        escaped = false
        continue
      }
      if (ch === '\\') {
        escaped = true
        continue
      }
      if (ch === quote) {
        quote = null
      }
      continue
    }
    if (ch === '"' || ch === "'") {
      quote = ch
      mask[i] = true
    }
  }
  return mask
}

function findMatchingParenOffset(source: string, openOffset: number): number {
  let depth = 0
  let quote: '"' | "'" | null = null
  let escaped = false
  for (let i = openOffset; i < source.length; i += 1) {
    const ch = source[i]
    if (quote) {
      if (escaped) {
        escaped = false
        continue
      }
      if (ch === '\\') {
        escaped = true
        continue
      }
      if (ch === quote) {
        quote = null
      }
      continue
    }
    if (ch === '"' || ch === "'") {
      quote = ch
      continue
    }
    if (ch === '(') {
      depth += 1
      continue
    }
    if (ch === ')') {
      depth -= 1
      if (depth === 0) return i
    }
  }
  return -1
}

function collectExpressionFunctionCalls(source: string): ExpressionFunctionCallRef[] {
  const refs: ExpressionFunctionCallRef[] = []
  if (!source) return refs
  const quoteMask = buildQuoteMask(source)
  const matcher = /\b([A-Za-z_]\w*)\s*\(/g
  let match = matcher.exec(source)
  while (match) {
    const name = String(match[1] || '')
    const nameStart = Number(match.index || 0)
    if (!quoteMask[nameStart]) {
      const openParenOffset = source.indexOf('(', nameStart + name.length)
      if (openParenOffset >= 0 && !quoteMask[openParenOffset]) {
        const closeParenOffset = findMatchingParenOffset(source, openParenOffset)
        if (closeParenOffset > openParenOffset) {
          refs.push({
            name,
            nameStart,
            openParenOffset,
            closeParenOffset,
            argsSource: source.slice(openParenOffset + 1, closeParenOffset),
          })
        }
      }
    }
    match = matcher.exec(source)
  }
  return refs
}

function splitTopLevelArgsWithOffsets(argsSource: string): ParsedArgSegment[] {
  const segments: ParsedArgSegment[] = []
  let start = 0
  let depthRound = 0
  let depthSquare = 0
  let depthCurly = 0
  let quote: '"' | "'" | null = null
  let escaped = false

  for (let i = 0; i < argsSource.length; i += 1) {
    const ch = argsSource[i]
    if (quote) {
      if (escaped) {
        escaped = false
        continue
      }
      if (ch === '\\') {
        escaped = true
        continue
      }
      if (ch === quote) {
        quote = null
      }
      continue
    }

    if (ch === '"' || ch === "'") {
      quote = ch
      continue
    }
    if (ch === '(') depthRound += 1
    else if (ch === ')') depthRound = Math.max(0, depthRound - 1)
    else if (ch === '[') depthSquare += 1
    else if (ch === ']') depthSquare = Math.max(0, depthSquare - 1)
    else if (ch === '{') depthCurly += 1
    else if (ch === '}') depthCurly = Math.max(0, depthCurly - 1)
    else if (ch === ',' && depthRound === 0 && depthSquare === 0 && depthCurly === 0) {
      segments.push({
        text: argsSource.slice(start, i),
        startOffset: start,
        endOffset: i,
      })
      start = i + 1
    }
  }

  segments.push({
    text: argsSource.slice(start),
    startOffset: start,
    endOffset: argsSource.length,
  })
  return segments
}

function extractQuotedLiteralFromSegment(segment: ParsedArgSegment): { value: string; startOffset: number; endOffset: number } | null {
  const text = segment.text
  if (!text) return null
  const singleQuoteOffset = text.indexOf("'")
  const doubleQuoteOffset = text.indexOf('"')
  const firstQuote = (
    singleQuoteOffset >= 0 && doubleQuoteOffset >= 0
      ? Math.min(singleQuoteOffset, doubleQuoteOffset)
      : singleQuoteOffset >= 0
        ? singleQuoteOffset
        : doubleQuoteOffset
  )
  if (firstQuote < 0 || text.slice(0, firstQuote).trim()) return null
  const quoteChar = text[firstQuote]
  let escaped = false
  for (let i = firstQuote + 1; i < text.length; i += 1) {
    const ch = text[i]
    if (escaped) {
      escaped = false
      continue
    }
    if (ch === '\\') {
      escaped = true
      continue
    }
    if (ch === quoteChar) {
      if (text.slice(i + 1).trim()) return null
      const raw = text.slice(firstQuote + 1, i)
      return {
        value: String(raw || '')
          .replace(/\\\\/g, '\\')
          .replace(/\\'/g, "'")
          .replace(/\\"/g, '"'),
        startOffset: segment.startOffset + firstQuote + 1,
        endOffset: segment.startOffset + i,
      }
    }
  }
  return null
}

function isAllowedFieldName(fieldName: string, validationContext: ExpressionValidationContext): boolean {
  const normalized = String(fieldName || '').trim()
  if (!normalized) return true
  if (!validationContext.allowedFieldNames.size && !validationContext.allowedFieldNamesLower.size) return true
  if (validationContext.allowedFieldNames.has(normalized)) return true
  return validationContext.allowedFieldNamesLower.has(normalized.toLowerCase())
}

function levenshteinDistance(a: string, b: string): number {
  const x = String(a || '')
  const y = String(b || '')
  const n = x.length
  const m = y.length
  if (n === 0) return m
  if (m === 0) return n
  const dp: number[][] = Array.from({ length: n + 1 }, () => Array(m + 1).fill(0))
  for (let i = 0; i <= n; i += 1) dp[i][0] = i
  for (let j = 0; j <= m; j += 1) dp[0][j] = j
  for (let i = 1; i <= n; i += 1) {
    for (let j = 1; j <= m; j += 1) {
      const cost = x[i - 1] === y[j - 1] ? 0 : 1
      dp[i][j] = Math.min(
        dp[i - 1][j] + 1,
        dp[i][j - 1] + 1,
        dp[i - 1][j - 1] + cost,
      )
    }
  }
  return dp[n][m]
}

function findClosestSuggestion(value: string, candidates: string[]): string | null {
  const source = String(value || '').trim().toLowerCase()
  if (!source) return null
  const normalizedCandidates = uniqueFieldNames(candidates || []).filter(Boolean)
  if (!normalizedCandidates.length) return null

  const exact = normalizedCandidates.find((candidate) => candidate.toLowerCase() === source)
  if (exact) return exact

  const startsWithMatch = normalizedCandidates
    .filter((candidate) => candidate.toLowerCase().startsWith(source))
    .sort((a, b) => a.length - b.length)[0]
  if (startsWithMatch) return startsWithMatch

  let bestCandidate: string | null = null
  let bestScore = Number.POSITIVE_INFINITY
  normalizedCandidates.forEach((candidate) => {
    const score = levenshteinDistance(source, candidate.toLowerCase())
    if (score < bestScore) {
      bestScore = score
      bestCandidate = candidate
    }
  })

  if (!bestCandidate) return null
  const threshold = Math.max(2, Math.floor(source.length * 0.4))
  return bestScore <= threshold ? bestCandidate : null
}

function isAllowedPathName(pathName: string, validationContext: ExpressionValidationContext): boolean {
  const normalized = String(pathName || '').trim()
  if (!normalized) return true
  if (!validationContext.allowedPathNames.size && !validationContext.allowedPathNamesLower.size) return true
  if (validationContext.allowedPathNames.has(normalized)) return true
  return validationContext.allowedPathNamesLower.has(normalized.toLowerCase())
}

function extractPathRoot(pathName: string): string {
  const normalized = String(pathName || '').trim()
  if (!normalized) return ''
  const rootMatch = normalized.match(/^[^.[\]]+/)
  return String(rootMatch?.[0] || '').trim()
}

function withSuggestion(message: string, suggestion: string | null): string {
  const base = String(message || '')
  const hint = String(suggestion || '').trim()
  if (!hint) return base
  return `${base}. Did you mean "${hint}"?`
}

function validateExpressionSemantics(
  source: string,
  baseOffset: number,
  validationContext: ExpressionValidationContext
): ExpressionValidationIssue[] {
  const issues: ExpressionValidationIssue[] = []
  const seen = new Set<string>()
  const quoteMask = buildQuoteMask(source)
  const functionRefs = collectExpressionFunctionCalls(source)
  const conditionFunctions = new Set([
    'count_if',
    'sum_if',
    'mean_if',
    'min_if',
    'max_if',
    'distinct_if',
    'distinct_count_if',
    'count_non_null_if',
  ])

  const addIssue = (issue: ExpressionValidationIssue) => {
    const key = `${issue.startOffset}:${issue.endOffset}:${issue.message}`
    if (seen.has(key)) return
    seen.add(key)
    issues.push(issue)
  }

  // Validate callable names even if the expression is partially malformed.
  // This catches typos like feild(), innc(), group_aggrigate() early.
  const callMatcher = /\b([A-Za-z_]\w*)\s*\(/g
  let callMatch = callMatcher.exec(source)
  while (callMatch) {
    const fnName = String(callMatch[1] || '')
    const fnStart = Number(callMatch.index || 0)
    if (fnName && !quoteMask[fnStart]) {
      const fnLower = fnName.toLowerCase()
      if (!validationContext.allowedFunctionNames.has(fnLower)) {
        const fnSuggestion = findClosestSuggestion(fnName, Array.from(validationContext.allowedFunctionNames))
        addIssue({
          startOffset: baseOffset + fnStart,
          endOffset: baseOffset + fnStart + fnName.length,
          message: withSuggestion(`Function "${fnName}" is not available`, fnSuggestion),
          severity: 'error',
        })
      }
    }
    callMatch = callMatcher.exec(source)
  }

  const markUnknownField = (fieldName: string, startOffset: number, endOffset: number, contextLabel: string) => {
    if (isAllowedFieldName(fieldName, validationContext)) return
    const fieldSuggestion = findClosestSuggestion(fieldName, Array.from(validationContext.allowedFieldNames))
    addIssue({
      startOffset: baseOffset + startOffset,
      endOffset: baseOffset + Math.max(startOffset + 1, endOffset),
      message: withSuggestion(`Field "${fieldName}" is not available (${contextLabel})`, fieldSuggestion),
      severity: 'error',
    })
  }

  const validatePathRootReference = (
    literalValue: string,
    literalStartOffset: number,
    contextLabel: string
  ) => {
    const pathText = String(literalValue || '').trim()
    if (!pathText) return
    const rootMatch = pathText.match(/^[^.[\]]+/)
    const root = String(rootMatch?.[0] || '').trim()
    if (!root) return
    const rootStart = literalStartOffset
    const rootEnd = literalStartOffset + root.length
    markUnknownField(root, rootStart, rootEnd, `${contextLabel} path root`)
  }

  const validateFullPathReference = (
    literalValue: string,
    literalStartOffset: number,
    literalEndOffset: number,
    contextLabel: string
  ) => {
    const pathText = String(literalValue || '').trim()
    if (!pathText) return
    if (isAllowedPathName(pathText, validationContext)) return

    const root = extractPathRoot(pathText)
    if (!root) return

    if (!isAllowedFieldName(root, validationContext)) {
      const rootStart = literalStartOffset
      const rootEnd = literalStartOffset + root.length
      markUnknownField(root, rootStart, rootEnd, `${contextLabel} path root`)
      return
    }

    const sameRootCandidates = Array.from(validationContext.allowedPathNames).filter((candidate) => {
      const cRoot = extractPathRoot(candidate)
      return cRoot.toLowerCase() === root.toLowerCase()
    })

    if (sameRootCandidates.length === 0) return
    const suggestion = findClosestSuggestion(pathText, sameRootCandidates)
    addIssue({
      startOffset: baseOffset + literalStartOffset,
      endOffset: baseOffset + Math.max(literalStartOffset + 1, literalEndOffset),
      message: withSuggestion(`Path "${pathText}" is not available (${contextLabel})`, suggestion),
      severity: 'error',
    })
  }

  functionRefs.forEach((ref) => {
    const fnNameLower = ref.name.toLowerCase()
    if (!validationContext.allowedFunctionNames.has(fnNameLower)) {
      const fnSuggestion = findClosestSuggestion(ref.name, Array.from(validationContext.allowedFunctionNames))
      addIssue({
        startOffset: baseOffset + ref.nameStart,
        endOffset: baseOffset + ref.nameStart + ref.name.length,
        message: withSuggestion(`Function "${ref.name}" is not available`, fnSuggestion),
        severity: 'error',
      })
    }

    const args = splitTopLevelArgsWithOffsets(ref.argsSource)
    const fieldArgIndexes: number[] = []
    if (fnNameLower === 'field' || fnNameLower === 'values' || fnNameLower === 'prev') {
      fieldArgIndexes.push(0)
    } else if (fnNameLower === 'group_aggregate') {
      fieldArgIndexes.push(0)
    } else if (conditionFunctions.has(fnNameLower)) {
      fieldArgIndexes.push(0, 1)
    } else if (fnNameLower === 'agg_if') {
      fieldArgIndexes.push(0, 1)
    }

    fieldArgIndexes.forEach((argIdx) => {
      const segment = args[argIdx]
      if (!segment) return
      const literal = extractQuotedLiteralFromSegment(segment)
      if (!literal) return
      if (fnNameLower === 'prev' && /[.[\]]/.test(literal.value)) {
        validateFullPathReference(
          literal.value,
          ref.openParenOffset + 1 + literal.startOffset,
          ref.openParenOffset + 1 + literal.endOffset,
          `${ref.name}()`
        )
        validatePathRootReference(
          literal.value,
          ref.openParenOffset + 1 + literal.startOffset,
          `${ref.name}()`
        )
        return
      }
      markUnknownField(
        literal.value,
        ref.openParenOffset + 1 + literal.startOffset,
        ref.openParenOffset + 1 + literal.endOffset,
        `${ref.name}()`
      )
    })

    // Validate path-root for profile path helpers where first argument is a dotted path.
    if (['inc', 'map_inc', 'append_unique', 'rolling_update'].includes(fnNameLower)) {
      const firstArg = args[0]
      const firstLiteral = firstArg ? extractQuotedLiteralFromSegment(firstArg) : null
      if (firstLiteral && /[.[\]]/.test(firstLiteral.value)) {
        validateFullPathReference(
          firstLiteral.value,
          ref.openParenOffset + 1 + firstLiteral.startOffset,
          ref.openParenOffset + 1 + firstLiteral.endOffset,
          `${ref.name}()`
        )
        validatePathRootReference(
          firstLiteral.value,
          ref.openParenOffset + 1 + firstLiteral.startOffset,
          `${ref.name}()`
        )
      }
    }

    if (conditionFunctions.has(fnNameLower)) {
      const opArg = args[3]
      const opLiteral = opArg ? extractQuotedLiteralFromSegment(opArg) : null
      if (opLiteral) {
        const opLower = opLiteral.value.trim().toLowerCase()
        if (opLower && !EXPR_ALLOWED_CONDITION_OPERATORS.has(opLower)) {
          const opSuggestion = findClosestSuggestion(opLiteral.value, Array.from(EXPR_ALLOWED_CONDITION_OPERATORS))
          addIssue({
            startOffset: baseOffset + ref.openParenOffset + 1 + opLiteral.startOffset,
            endOffset: baseOffset + ref.openParenOffset + 1 + opLiteral.endOffset,
            message: withSuggestion(`Operator "${opLiteral.value}" is not supported`, opSuggestion),
            severity: 'error',
          })
        }
      }
    }
    if (fnNameLower === 'agg_if') {
      const aggArg = args[3]
      const aggLiteral = aggArg ? extractQuotedLiteralFromSegment(aggArg) : null
      if (aggLiteral) {
        const aggLower = aggLiteral.value.trim().toLowerCase()
        if (aggLower && !EXPR_ALLOWED_AGG_VALUES.has(aggLower)) {
          const aggSuggestion = findClosestSuggestion(aggLiteral.value, Array.from(EXPR_ALLOWED_AGG_VALUES))
          addIssue({
            startOffset: baseOffset + ref.openParenOffset + 1 + aggLiteral.startOffset,
            endOffset: baseOffset + ref.openParenOffset + 1 + aggLiteral.endOffset,
            message: withSuggestion(`Aggregate "${aggLiteral.value}" is not supported`, aggSuggestion),
            severity: 'error',
          })
        }
      }
      const opArg = args[4]
      const opLiteral = opArg ? extractQuotedLiteralFromSegment(opArg) : null
      if (opLiteral) {
        const opLower = opLiteral.value.trim().toLowerCase()
        if (opLower && !EXPR_ALLOWED_CONDITION_OPERATORS.has(opLower)) {
          const opSuggestion = findClosestSuggestion(opLiteral.value, Array.from(EXPR_ALLOWED_CONDITION_OPERATORS))
          addIssue({
            startOffset: baseOffset + ref.openParenOffset + 1 + opLiteral.startOffset,
            endOffset: baseOffset + ref.openParenOffset + 1 + opLiteral.endOffset,
            message: withSuggestion(`Operator "${opLiteral.value}" is not supported`, opSuggestion),
            severity: 'error',
          })
        }
      }
    }
    if (fnNameLower === 'agg' || fnNameLower === 'running_all') {
      const aggArg = args[1]
      const aggLiteral = aggArg ? extractQuotedLiteralFromSegment(aggArg) : null
      if (aggLiteral) {
        const aggLower = aggLiteral.value.trim().toLowerCase()
        if (aggLower && !EXPR_ALLOWED_AGG_VALUES.has(aggLower)) {
          const aggSuggestion = findClosestSuggestion(aggLiteral.value, Array.from(EXPR_ALLOWED_AGG_VALUES))
          addIssue({
            startOffset: baseOffset + ref.openParenOffset + 1 + aggLiteral.startOffset,
            endOffset: baseOffset + ref.openParenOffset + 1 + aggLiteral.endOffset,
            message: withSuggestion(`Aggregate "${aggLiteral.value}" is not supported`, aggSuggestion),
            severity: 'error',
          })
        }
      }
    }
    if (fnNameLower === 'rolling' || fnNameLower === 'rolling_all') {
      const aggArg = args[2]
      const aggLiteral = aggArg ? extractQuotedLiteralFromSegment(aggArg) : null
      if (aggLiteral) {
        const aggLower = aggLiteral.value.trim().toLowerCase()
        if (aggLower && !EXPR_ALLOWED_AGG_VALUES.has(aggLower)) {
          const aggSuggestion = findClosestSuggestion(aggLiteral.value, Array.from(EXPR_ALLOWED_AGG_VALUES))
          addIssue({
            startOffset: baseOffset + ref.openParenOffset + 1 + aggLiteral.startOffset,
            endOffset: baseOffset + ref.openParenOffset + 1 + aggLiteral.endOffset,
            message: withSuggestion(`Aggregate "${aggLiteral.value}" is not supported`, aggSuggestion),
            severity: 'error',
          })
        }
      }
    }
  })

  if (validationContext.allowedFieldNames.size || validationContext.allowedFieldNamesLower.size) {
    const pathMatcher = /\bpath\s*=\s*'((?:[^'\\]|\\.)*)'/g
    let pathMatch = pathMatcher.exec(source)
    while (pathMatch) {
      const raw = String(pathMatch[1] || '')
      const full = String(pathMatch[0] || '')
      const quoteOffset = full.indexOf("'")
      if (quoteOffset >= 0) {
        markUnknownField(
          decodeSingleQuotedLiteral(raw),
          pathMatch.index + quoteOffset + 1,
          pathMatch.index + quoteOffset + 1 + raw.length,
          'obj(path=...)'
        )
      }
      pathMatch = pathMatcher.exec(source)
    }
  }

  const aggMatcher = /\bagg\s*=\s*'((?:[^'\\]|\\.)*)'/g
  let aggMatch = aggMatcher.exec(source)
  while (aggMatch) {
    const raw = decodeSingleQuotedLiteral(String(aggMatch[1] || ''))
    const full = String(aggMatch[0] || '')
    const quoteOffset = full.indexOf("'")
    const aggLower = raw.trim().toLowerCase()
    if (aggLower && !EXPR_ALLOWED_AGG_VALUES.has(aggLower) && quoteOffset >= 0) {
      const aggSuggestion = findClosestSuggestion(raw, Array.from(EXPR_ALLOWED_AGG_VALUES))
      addIssue({
        startOffset: baseOffset + aggMatch.index + quoteOffset + 1,
        endOffset: baseOffset + aggMatch.index + quoteOffset + 1 + String(aggMatch[1] || '').length,
        message: withSuggestion(`Aggregate "${raw}" is not supported`, aggSuggestion),
        severity: 'error',
      })
    }
    aggMatch = aggMatcher.exec(source)
  }

  return issues
}

function validateExpressionSyntax(
  expression: string,
  baseOffset = 0,
  options?: { requireEqualsPrefix?: boolean; validationContext?: ExpressionValidationContext }
): ExpressionValidationIssue[] {
  const issues: ExpressionValidationIssue[] = []
  const source = String(expression || '')
  const trimmed = source.trim()
  const requireEqualsPrefix = options?.requireEqualsPrefix ?? true
  const validationContext = options?.validationContext || getExpressionValidationContext()

  if (!trimmed) return issues
  if (requireEqualsPrefix && !trimmed.startsWith('=')) {
    issues.push({
      startOffset: baseOffset,
      endOffset: baseOffset + Math.max(1, Math.min(source.length, 2)),
      message: 'Expression should start with "="',
      severity: 'warning',
    })
  }
  if (trimmed === '=') {
    issues.push({
      startOffset: baseOffset,
      endOffset: baseOffset + Math.max(1, source.length),
      message: 'Expression is empty',
      severity: 'error',
    })
    return issues
  }

  const stack: Array<{ ch: string; offset: number }> = []
  let quote: '"' | "'" | null = null
  let escaped = false

  for (let i = 0; i < source.length; i += 1) {
    const ch = source[i]
    if (quote) {
      if (escaped) {
        escaped = false
        continue
      }
      if (ch === '\\') {
        escaped = true
        continue
      }
      if (ch === quote) {
        quote = null
      }
      continue
    }

    if (ch === '"' || ch === "'") {
      quote = ch
      continue
    }
    if (ch === '(' || ch === '[' || ch === '{') {
      stack.push({ ch, offset: i })
      continue
    }
    if (ch === ')' || ch === ']' || ch === '}') {
      const expectedOpen = ch === ')' ? '(' : ch === ']' ? '[' : '{'
      const top = stack[stack.length - 1]
      if (!top || top.ch !== expectedOpen) {
        issues.push({
          startOffset: baseOffset + i,
          endOffset: baseOffset + i + 1,
          message: `Unexpected closing "${ch}". Check bracket order or remove this "${ch}".`,
          severity: 'error',
        })
      } else {
        stack.pop()
      }
    }
  }

  if (quote) {
    issues.push({
      startOffset: baseOffset + source.length - 1,
      endOffset: baseOffset + source.length,
      message: `Unclosed quoted string. Add closing ${quote}.`,
      severity: 'error',
    })
  }

  while (stack.length > 0) {
    const unclosed = stack.pop()!
    const expectedClose = unclosed.ch === '(' ? ')' : unclosed.ch === '[' ? ']' : '}'
    issues.push({
      startOffset: baseOffset + unclosed.offset,
      endOffset: baseOffset + unclosed.offset + 1,
      message: `Missing closing "${expectedClose}". Add "${expectedClose}" to complete this block.`,
      severity: 'error',
    })
  }

  issues.push(...validateExpressionSemantics(source, baseOffset, validationContext))

  return issues
}

function toMonacoMarkers(
  monaco: Monaco,
  model: any,
  issues: ExpressionValidationIssue[],
) {
  const markerSeverity =
    (monaco as any)?.MarkerSeverity
    || (monaco as any)?.editor?.MarkerSeverity
    || {}
  const markerErrorSeverity = markerSeverity?.Error ?? 8
  const markerWarningSeverity = markerSeverity?.Warning ?? 4

  return issues.map((issue) => {
    const safeStart = Math.max(0, issue.startOffset)
    const safeEnd = Math.max(safeStart + 1, issue.endOffset)
    const start = model.getPositionAt(safeStart)
    const end = model.getPositionAt(safeEnd)
    return {
      startLineNumber: start.lineNumber,
      startColumn: start.column,
      endLineNumber: end.lineNumber,
      endColumn: end.column,
      message: issue.message,
      severity: issue.severity === 'error'
        ? markerErrorSeverity
        : markerWarningSeverity,
    }
  })
}

function toMonacoDecorations(
  monaco: Monaco,
  model: any,
  issues: ExpressionValidationIssue[],
) {
  const minimapInlinePosition = (monaco as any)?.editor?.MinimapPosition?.Inline
  const overviewRightLane = (monaco as any)?.editor?.OverviewRulerLane?.Right
  return issues.map((issue) => {
    const safeStart = Math.max(0, issue.startOffset)
    const safeEnd = Math.max(safeStart + 1, issue.endOffset)
    const start = model.getPositionAt(safeStart)
    const end = model.getPositionAt(safeEnd)
    const severityLabel = issue.severity === 'error' ? 'Error' : 'Warning'
    return {
      range: new monaco.Range(start.lineNumber, start.column, end.lineNumber, end.column),
      options: {
        className: issue.severity === 'error' ? EXPR_RANGE_ERROR_CLASS : EXPR_RANGE_WARNING_CLASS,
        inlineClassName: issue.severity === 'error' ? EXPR_INLINE_ERROR_CLASS : EXPR_INLINE_WARNING_CLASS,
        inlineClassNameAffectsLetterSpacing: true,
        glyphMarginClassName: issue.severity === 'error' ? EXPR_GLYPH_ERROR_CLASS : EXPR_GLYPH_WARNING_CLASS,
        glyphMarginHoverMessage: [{ value: `${severityLabel}: ${issue.message}` }],
        hoverMessage: [{ value: `${severityLabel}: ${issue.message}` }],
        ...(minimapInlinePosition !== undefined
          ? {
            minimap: {
              color: issue.severity === 'error' ? '#ef4444' : '#f59e0b',
              position: minimapInlinePosition,
            },
          }
          : {}),
        ...(overviewRightLane !== undefined
          ? {
            overviewRuler: {
              color: issue.severity === 'error' ? '#ef4444' : '#f59e0b',
              position: overviewRightLane,
            },
          }
          : {}),
      },
    }
  })
}

function applyValidationDecorations(
  editor: any,
  monaco: Monaco,
  model: any,
  issues: ExpressionValidationIssue[],
): void {
  const prevDecorationIds = editorValidationDecorations.get(editor) || []
  const nextDecorationIds = editor.deltaDecorations(
    prevDecorationIds,
    toMonacoDecorations(monaco, model, issues),
  )
  editorValidationDecorations.set(editor, nextDecorationIds)
}

function clearValidationDecorations(editor: any): void {
  const prevDecorationIds = editorValidationDecorations.get(editor) || []
  if (prevDecorationIds.length > 0) {
    editor.deltaDecorations(prevDecorationIds, [])
  }
  editorValidationDecorations.delete(editor)
}

function attachExpressionValidation(editor: any, monaco: Monaco): void {
  const validate = () => {
    const model = editor.getModel()
    if (!model) return
    const text = String(model.getValue() || '')
    const issues = validateExpressionSyntax(text, 0, { requireEqualsPrefix: true })
    modelValidationIssues.set(model, issues)
    monaco.editor.setModelMarkers(model, EXPR_VALIDATION_OWNER, toMonacoMarkers(monaco, model, issues))
    applyValidationDecorations(editor, monaco, model, issues)
  }
  validate()
  editor.focus()
  editor.onDidChangeModelContent(validate)
  const mouseDisposable = editor.onMouseDown((event: any) => {
    const model = editor.getModel()
    const pos = event?.target?.position
    if (!model || !pos) return
    const offset = Number(model.getOffsetAt(pos) || 0)
    const quickFix = findIssueQuickFixAtOffset(model, offset)
    if (!quickFix) return
    modelSuggestIntent.set(model, 'issue_click')
    editor.setPosition(pos)
    editor.trigger('mouse', 'editor.action.triggerSuggest', {})
  })
  editor.onDidDispose(() => {
    mouseDisposable.dispose()
    clearValidationDecorations(editor)
    const model = editor.getModel()
    if (!model) return
    modelValidationIssues.delete(model)
    modelSuggestIntent.delete(model)
    monaco.editor.setModelMarkers(model, EXPR_VALIDATION_OWNER, [])
  })
}

function attachJsonTemplateExpressionValidation(editor: any, monaco: Monaco): void {
  const validate = () => {
    const model = editor.getModel()
    if (!model) return
    const text = String(model.getValue() || '')
    const jsonIssues = validateJsonTemplateSyntax(text)
    const ranges = extractExpressionStringRanges(text)
    const expressionIssues = ranges.flatMap((entry) =>
      validateExpressionSyntax(entry.value, entry.startOffset, { requireEqualsPrefix: false })
    )
    const issues = [...jsonIssues, ...expressionIssues]
    modelValidationIssues.set(model, issues)
    monaco.editor.setModelMarkers(model, JSON_TEMPLATE_VALIDATION_OWNER, toMonacoMarkers(monaco, model, issues))
    applyValidationDecorations(editor, monaco, model, issues)
  }
  validate()
  editor.focus()
  editor.onDidChangeModelContent(validate)
  const mouseDisposable = editor.onMouseDown((event: any) => {
    const model = editor.getModel()
    const pos = event?.target?.position
    if (!model || !pos) return
    const offset = Number(model.getOffsetAt(pos) || 0)
    const quickFix = findIssueQuickFixAtOffset(model, offset)
    if (!quickFix) return
    modelSuggestIntent.set(model, 'issue_click')
    editor.setPosition(pos)
    editor.trigger('mouse', 'editor.action.triggerSuggest', {})
  })
  editor.onDidDispose(() => {
    mouseDisposable.dispose()
    clearValidationDecorations(editor)
    const model = editor.getModel()
    if (!model) return
    modelValidationIssues.delete(model)
    modelSuggestIntent.delete(model)
    monaco.editor.setModelMarkers(model, JSON_TEMPLATE_VALIDATION_OWNER, [])
  })
}

function createCustomFieldSpec(seed?: Partial<CustomFieldSpec>): CustomFieldSpec {
  return {
    id: seed?.id || `cf_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    name: seed?.name || '',
    mode: seed?.mode || 'value',
    singleValueOutput: seed?.singleValueOutput || 'json',
    expression: seed?.expression || '',
    jsonTemplate: seed?.jsonTemplate || '{\n  "value": "=field(\'id\')"\n}',
    enabled: seed?.enabled ?? true,
  }
}

function cloneCustomFieldSpecs(items: CustomFieldSpec[]): CustomFieldSpec[] {
  return (items || []).map((item) => ({
    ...item,
  }))
}

function createOracleColumnMappingSpec(seed?: Partial<OracleColumnMappingSpec>): OracleColumnMappingSpec {
  return {
    id: seed?.id || `orm_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    source: String(seed?.source || '').trim(),
    destination: String(seed?.destination || '').trim(),
    enabled: seed?.enabled ?? true,
  }
}

function createLmdbStudioDraft(seed?: Partial<LmdbStudioDraft>): LmdbStudioDraft {
  const valueFormatRaw = String(seed?.value_format || 'auto').trim().toLowerCase()
  const valueFormat: 'auto' | 'json' | 'text' | 'base64' = (
    valueFormatRaw === 'json'
      ? 'json'
      : valueFormatRaw === 'text'
        ? 'text'
        : valueFormatRaw === 'base64'
          ? 'base64'
          : 'auto'
  )
  const rawLimit = Number(seed?.limit ?? 1000)
  // limit = 0 means no cap (read all matching LMDB records).
  const limit = Number.isFinite(rawLimit) ? Math.max(0, Math.floor(rawLimit)) : 1000
  return {
    env_path: String(seed?.env_path || '').trim(),
    db_name: String(seed?.db_name || '').trim(),
    key_prefix: String(seed?.key_prefix || '').trim(),
    start_key: String(seed?.start_key || '').trim(),
    end_key: String(seed?.end_key || '').trim(),
    key_contains: String(seed?.key_contains || '').trim(),
    value_contains: String(seed?.value_contains || '').trim(),
    value_format: valueFormat,
    flatten_json_values: seed?.flatten_json_values ?? true,
    expand_profile_documents: parseBoolLike(seed?.expand_profile_documents, true),
    limit,
  }
}

function parseBoolLike(value: unknown, defaultValue = false): boolean {
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return value !== 0
  if (typeof value === 'string') {
    const norm = value.trim().toLowerCase()
    if (['1', 'true', 'yes', 'y', 'on'].includes(norm)) return true
    if (['0', 'false', 'no', 'n', 'off'].includes(norm)) return false
  }
  return defaultValue
}

function normalizeSingleValueOutput(value: unknown): SingleValueOutputMode {
  const text = String(value || '').trim().toLowerCase()
  if (['plain_text', 'text', 'plain', 'string', 'str'].includes(text)) return 'plain_text'
  return 'json'
}

function toPreviewCellText(value: unknown): string {
  if (value === null || value === undefined) return ''
  if (typeof value === 'string') return value
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

function toPreviewJsonText(value: unknown): string {
  try {
    return JSON.stringify(value ?? null, null, 2)
  } catch {
    return JSON.stringify(String(value ?? ''), null, 2)
  }
}

function detectJsonValueKind(value: unknown): JsonTagMappingItem['kind'] {
  if (value === null || value === undefined) return 'null'
  if (Array.isArray(value)) return 'array'
  const t = typeof value
  if (t === 'string') return 'string'
  if (t === 'number') return 'number'
  if (t === 'boolean') return 'boolean'
  if (t === 'object') return 'object'
  return 'string'
}

function toJsonTagSample(value: unknown): string {
  if (value === null || value === undefined) return 'null'
  if (typeof value === 'string') {
    const trimmed = value.trim()
    return trimmed.length > 72 ? `${trimmed.slice(0, 69)}...` : trimmed
  }
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  if (Array.isArray(value)) return `Array(${value.length})`
  if (typeof value === 'object') {
    const keys = Object.keys(value as Record<string, unknown>)
    return `Object(${keys.length})`
  }
  return String(value)
}

function escapeExprPath(path: string): string {
  return String(path || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'")
}

function buildJsonTagMapping(path: string, value: unknown): JsonTagMappingItem {
  const safePath = escapeExprPath(path)
  return {
    path,
    kind: detectJsonValueKind(value),
    sample: toJsonTagSample(value),
    fieldExpr: `=field('${safePath}')`,
    valuesExpr: `=values('${safePath}')`,
    controlTag: `{{${path}}}`,
  }
}

function collectJsonTagMappings(
  value: unknown,
  basePath: string,
  out: JsonTagMappingItem[],
  seen: Set<string>,
  depth: number
): void {
  if (depth > MAX_JSON_TAG_DEPTH || out.length >= MAX_JSON_TAG_MAPPINGS) return

  if (basePath) {
    const dedupeKey = `${basePath}|${detectJsonValueKind(value)}`
    if (!seen.has(dedupeKey)) {
      seen.add(dedupeKey)
      out.push(buildJsonTagMapping(basePath, value))
      if (out.length >= MAX_JSON_TAG_MAPPINGS) return
    }
  }

  if (Array.isArray(value)) {
    if (!basePath) return
    const wildcardPath = `${basePath}[]`
    if (!seen.has(`${wildcardPath}|array`)) {
      seen.add(`${wildcardPath}|array`)
      out.push(buildJsonTagMapping(wildcardPath, value))
      if (out.length >= MAX_JSON_TAG_MAPPINGS) return
    }
    const sampleItems = value.slice(0, MAX_ARRAY_INDEX_OPTIONS)
    sampleItems.forEach((item, idx) => {
      const idxPath = `${basePath}[${idx}]`
      collectJsonTagMappings(item, idxPath, out, seen, depth + 1)
    })
    const exemplar = value.find((item) => item !== null && item !== undefined)
    if (exemplar !== undefined) {
      collectJsonTagMappings(exemplar, wildcardPath, out, seen, depth + 1)
    }
    return
  }

  if (value && typeof value === 'object') {
    Object.entries(value as Record<string, unknown>)
      .slice(0, MAX_OBJECT_KEYS_PER_LEVEL)
      .forEach(([key, child]) => {
        const nextPath = basePath ? `${basePath}.${key}` : key
        collectJsonTagMappings(child, nextPath, out, seen, depth + 1)
      })
  }
}

function buildJsonTagMappingsFromParsed(value: unknown): JsonTagMappingItem[] {
  const out: JsonTagMappingItem[] = []
  const seen = new Set<string>()
  if (value === null || value === undefined) return out
  if (Array.isArray(value)) {
    value.slice(0, MAX_ARRAY_INDEX_OPTIONS).forEach((item, idx) => {
      collectJsonTagMappings(item, `[${idx}]`, out, seen, 0)
    })
    const exemplar = value.find((item) => item !== null && item !== undefined)
    if (exemplar !== undefined) {
      collectJsonTagMappings(exemplar, '[]', out, seen, 0)
    }
    return out.slice(0, MAX_JSON_TAG_MAPPINGS)
  }
  if (value && typeof value === 'object') {
    Object.entries(value as Record<string, unknown>)
      .slice(0, MAX_OBJECT_KEYS_PER_LEVEL)
      .forEach(([key, child]) => {
        collectJsonTagMappings(child, key, out, seen, 0)
      })
    return out.slice(0, MAX_JSON_TAG_MAPPINGS)
  }
  collectJsonTagMappings(value, 'value', out, seen, 0)
  return out.slice(0, MAX_JSON_TAG_MAPPINGS)
}

function tryParseSortableNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value !== 'string') return null
  const trimmed = value.trim()
  if (!trimmed) return null
  if (!/^-?\d+(\.\d+)?$/.test(trimmed)) return null
  const parsed = Number(trimmed)
  return Number.isFinite(parsed) ? parsed : null
}

function applyRocksdbIncrementalTuningDefaults(
  setFlushRows: (value: number) => void,
  setFlushInterval: (value: number) => void,
  setDisableMinRows: (value: number) => void,
  setEntityLimit: (value: number) => void,
  setDisableRatio: (value: number) => void,
): void {
  setFlushRows(50000)
  setFlushInterval(10)
  setDisableMinRows(5000)
  setEntityLimit(10000)
  setDisableRatio(0.35)
}

function parseOracleColumnMappings(value: unknown): OracleColumnMappingSpec[] {
  let raw: unknown = value
  if (typeof raw === 'string') {
    const text = raw.trim()
    if (!text) return []
    try {
      raw = JSON.parse(text)
    } catch {
      return []
    }
  }

  if (!Array.isArray(raw)) return []

  return raw
    .map((item) => {
      if (!item || typeof item !== 'object') return null
      const rec = item as Record<string, unknown>
      const source = String(rec.source ?? rec.from ?? '').trim()
      const destination = String(rec.destination ?? rec.to ?? '').trim()
      const enabled = parseBoolLike(rec.enabled, true)
      if (!source && !destination) return null
      return createOracleColumnMappingSpec({
        id: String(rec.id || ''),
        source,
        destination,
        enabled,
      })
    })
    .filter((item): item is OracleColumnMappingSpec => !!item)
}

function serializeOracleColumnMappings(items: OracleColumnMappingSpec[]): Array<Record<string, unknown>> {
  return items
    .map((item) => ({
      source: String(item.source || '').trim(),
      destination: String(item.destination || '').trim(),
      enabled: Boolean(item.enabled),
    }))
    .filter((item) => item.source || item.destination)
}

function parseCustomFieldSpecs(value: unknown): CustomFieldSpec[] {
  let raw = value
  if (typeof raw === 'string') {
    const text = raw.trim()
    if (!text) return []
    try {
      raw = JSON.parse(text)
    } catch {
      return []
    }
  }
  if (!Array.isArray(raw)) return []

  const out: CustomFieldSpec[] = []
  raw.forEach((item) => {
    if (!item || typeof item !== 'object') return
    const rec = item as Record<string, unknown>
    const modeRaw = String(rec.mode || rec.kind || rec.type || 'value').trim().toLowerCase()
    const mode: CustomFieldMode = modeRaw === 'json' ? 'json' : 'value'
    const rawJsonTemplate = String(rec.jsonTemplate || rec.json_template || rec.template || '')
    out.push(
      createCustomFieldSpec({
        id: String(rec.id || ''),
        name: String(rec.name || rec.field || ''),
        mode,
        singleValueOutput: normalizeSingleValueOutput(
          rec.singleValueOutput ?? rec.single_value_output ?? rec.value_output ?? rec.output_format
        ),
        expression: String(rec.expression || rec.expr || ''),
        jsonTemplate: mode === 'json' ? normalizeStoredJsonTemplateForEditor(rawJsonTemplate) : rawJsonTemplate,
        enabled: parseBoolLike(rec.enabled, true),
      })
    )
  })
  return out
}

function serializeCustomFieldSpecs(items: CustomFieldSpec[]): Array<Record<string, unknown>> {
  return items
    .map((item) => ({
      id: String(item.id || '').trim(),
      name: String(item.name || '').trim(),
      mode: item.mode,
      single_value_output: item.mode === 'value' ? item.singleValueOutput : 'json',
      expression: item.mode === 'value' ? String(item.expression || '').trim() : '',
      json_template: item.mode === 'json' ? normalizeJsonTemplateForStorage(String(item.jsonTemplate || '').trim()) : '',
      enabled: Boolean(item.enabled),
    }))
    .filter((item) => item.name)
}

function uniqueFieldNames(values: string[]): string[] {
  const out: string[] = []
  const seen = new Set<string>()
  values.forEach((raw) => {
    const name = String(raw || '').trim()
    if (!name) return
    const key = name.toLowerCase()
    if (seen.has(key)) return
    seen.add(key)
    out.push(name)
  })
  return out
}

function collectJsonTemplateKeyPaths(
  value: unknown,
  basePath: string,
  out: Set<string>,
  depth: number
): void {
  if (depth > MAX_PATH_DEPTH || out.size >= MAX_PATH_OPTIONS) return

  if (Array.isArray(value)) {
    if (!basePath) return
    out.add(basePath)
    const wildcardPath = `${basePath}[]`
    out.add(wildcardPath)
    const sampleItems = value.slice(0, MAX_ARRAY_INDEX_OPTIONS)
    sampleItems.forEach((item, idx) => {
      const idxPath = `${basePath}[${idx}]`
      out.add(idxPath)
      collectJsonTemplateKeyPaths(item, idxPath, out, depth + 1)
    })
    const exemplar = value.find((item) => item !== null && item !== undefined)
    if (exemplar !== undefined) {
      collectJsonTemplateKeyPaths(exemplar, wildcardPath, out, depth + 1)
    }
    return
  }

  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>).slice(0, MAX_OBJECT_KEYS_PER_LEVEL)
    entries.forEach(([key, child]) => {
      const nextPath = basePath ? `${basePath}.${key}` : key
      out.add(nextPath)
      collectJsonTemplateKeyPaths(child, nextPath, out, depth + 1)
    })
  }
}

function extractJsonTemplateKeyPaths(jsonTemplate: string): string[] {
  const text = String(jsonTemplate || '').trim()
  if (!text) return []
  try {
    const parsed = parseJsonTemplateLikeValue(text)
    if (!parsed) throw new Error('Template parse failed')
    const out = new Set<string>()
    collectJsonTemplateKeyPaths(parsed, '', out, 0)
    return uniqueFieldNames(Array.from(out))
  } catch {
    // Best-effort fallback while user is still typing malformed JSON.
    const out = new Set<string>()
    const keyMatcher = /(?:"([^"\\]+)"|([A-Za-z_$][\w$]*))\s*:/g
    let keyMatch = keyMatcher.exec(text)
    while (keyMatch) {
      const key = String(keyMatch[1] || keyMatch[2] || '').trim()
      if (key) out.add(key)
      keyMatch = keyMatcher.exec(text)
    }
    return uniqueFieldNames(Array.from(out))
  }
}

function buildCustomProfilePathSuggestions(specs: CustomFieldSpec[]): string[] {
  const out = new Set<string>()
  ;(specs || []).forEach((spec) => {
    const rootName = String(spec.name || '').trim()
    if (!rootName) return
    out.add(rootName)
    if (spec.mode !== 'json') return
    const keyPaths = extractJsonTemplateKeyPaths(spec.jsonTemplate)
    keyPaths.forEach((path) => {
      const normalized = String(path || '').trim()
      if (!normalized) return
      out.add(normalized)
      out.add(`${rootName}.${normalized}`)
    })
  })
  return uniqueFieldNames(Array.from(out))
}

const INTERNAL_FIELD_ROOT_PREFIXES = ['_lmdb_', '__lmdb_', '_profile_', '_detected_', '_preview_']
const INTERNAL_FIELD_ROOT_EXACT = new Set([
  '_lmdb_entity_meta',
  '_lmdb_profile_source',
  '_lmdb_value_kind',
  '__lmdb_preview_row_id',
  '__lmdb_preview_row_index',
  '_profile_changed_fields',
  '_row_count',
  'lmdb_key',
  'lmdb_entity_key',
])
const LMDB_GLOBAL_ALL_COLUMNS = '__ALL__'
const LMDB_EMPTY_FILTER_TOKEN = '__LMDB_EMPTY__'

function isInternalSystemField(fieldName: string): boolean {
  const text = String(fieldName || '').trim()
  if (!text) return false
  const rootMatch = text.match(/^[^.[\]]+/)
  const root = String(rootMatch?.[0] || text).trim()
  if (!root) return false
  if (INTERNAL_FIELD_ROOT_EXACT.has(root)) return true
  return INTERNAL_FIELD_ROOT_PREFIXES.some((prefix) => root.startsWith(prefix))
}

function filterUserFacingFieldNames(values: string[]): string[] {
  return uniqueFieldNames(values).filter((name) => !isInternalSystemField(name))
}

function parseFieldList(value: unknown): string[] {
  if (Array.isArray(value)) {
    return uniqueFieldNames(value.map((v) => String(v)))
  }
  if (typeof value === 'string') {
    return uniqueFieldNames(value.split(/[,\n]/).map((p) => p.trim()))
  }
  return []
}

function parseStringList(value: unknown): string[] {
  if (Array.isArray(value)) {
    const out: string[] = []
    const seen = new Set<string>()
    value.forEach((item) => {
      const text = String(item || '').trim()
      if (!text || seen.has(text)) return
      seen.add(text)
      out.push(text)
    })
    return out
  }
  if (typeof value === 'string') {
    const out: string[] = []
    const seen = new Set<string>()
    value.split(/[,\n]/).forEach((item) => {
      const text = String(item || '').trim()
      if (!text || seen.has(text)) return
      seen.add(text)
      out.push(text)
    })
    return out
  }
  return []
}

function parseDetectedJsonPaths(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  const out: string[] = []
  value.forEach((item) => {
    if (typeof item === 'string') out.push(item)
    else if (item && typeof item === 'object') {
      const v = String((item as any).value || '').trim()
      if (v) out.push(v)
    }
  })
  return uniqueFieldNames(out.filter((p) => p && p !== '$'))
}

function collectFieldPathsFromValue(value: unknown, basePath: string, out: Set<string>, depth: number): void {
  if (depth > MAX_PATH_DEPTH || out.size >= MAX_PATH_OPTIONS) return

  if (Array.isArray(value)) {
    if (basePath) out.add(basePath)
    const wildcardPath = basePath ? `${basePath}[]` : ''
    if (wildcardPath) out.add(wildcardPath)

    const sampleItems = value.slice(0, MAX_ARRAY_INDEX_OPTIONS)
    sampleItems.forEach((item, idx) => {
      const idxPath = basePath ? `${basePath}[${idx}]` : `[${idx}]`
      if (out.size < MAX_PATH_OPTIONS) out.add(idxPath)
      collectFieldPathsFromValue(item, idxPath, out, depth + 1)
    })

    if (wildcardPath) {
      const exemplar = value.find((item) => item !== null && item !== undefined)
      if (exemplar !== undefined) {
        collectFieldPathsFromValue(exemplar, wildcardPath, out, depth + 1)
      }
    }
    return
  }

  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>).slice(0, MAX_OBJECT_KEYS_PER_LEVEL)
    entries.forEach(([key, child]) => {
      const nextPath = basePath ? `${basePath}.${key}` : key
      if (out.size < MAX_PATH_OPTIONS) out.add(nextPath)
      collectFieldPathsFromValue(child, nextPath, out, depth + 1)
    })
  }
}

function extractSampleFieldPaths(rows: unknown): string[] {
  if (!Array.isArray(rows) || rows.length === 0) return []
  const out = new Set<string>()
  rows.slice(0, 8).forEach((row) => {
    collectFieldPathsFromValue(row, '', out, 0)
  })
  return uniqueFieldNames(Array.from(out))
}

function extractDirectNodeFields(node: ETLNode | undefined): string[] {
  if (!node?.data) return []
  const cfg = (node.data.config && typeof node.data.config === 'object'
    ? node.data.config
    : {}) as Record<string, unknown>

  const fields: string[] = []
  fields.push(...parseFieldList(cfg._detected_columns))
  fields.push(...parseDetectedJsonPaths(cfg._json_paths))
  fields.push(...extractSampleFieldPaths(cfg._preview_rows))
  fields.push(...extractSampleFieldPaths(node.data.executionSampleInput))
  fields.push(...extractSampleFieldPaths(node.data.executionSampleOutput))

  return filterUserFacingFieldNames(fields)
}

function parseRenameMappings(value: unknown): Array<{ from: string; to: string }> {
  if (typeof value !== 'string') return []
  return value
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const idx = line.indexOf(':')
      if (idx === -1) return null
      const from = line.slice(0, idx).trim()
      const to = line.slice(idx + 1).trim()
      if (!from || !to) return null
      return { from, to }
    })
    .filter((x): x is { from: string; to: string } => !!x)
}

function normalizeCronExpr(value: unknown): string {
  return String(value || '').trim().split(/\s+/).join(' ')
}

function detectSchedulePresetFromCron(value: unknown): string {
  const normalized = normalizeCronExpr(value)
  if (!normalized) return DEFAULT_SCHEDULE_PRESET
  const hit = Object.entries(SCHEDULE_PRESET_TO_CRON).find(([, cron]) => cron === normalized)
  return hit ? hit[0] : CUSTOM_SCHEDULE_PRESET
}

function inferNodeOutputFields(
  nodeId: string,
  nodesById: Map<string, ETLNode>,
  incomingByTarget: Map<string, string[]>,
  memo: Map<string, string[]>,
  stack: Set<string>
): string[] {
  if (memo.has(nodeId)) return memo.get(nodeId) || []
  if (stack.has(nodeId)) return []
  stack.add(nodeId)

  const node = nodesById.get(nodeId)
  const nodeType = String(node?.data?.nodeType || '')
  const cfg = (node?.data?.config && typeof node?.data?.config === 'object'
    ? node.data.config
    : {}) as Record<string, unknown>
  const directFields = extractDirectNodeFields(node)
  const upstreamIds = incomingByTarget.get(nodeId) || []
  const upstreamFields = uniqueFieldNames(
    upstreamIds.flatMap((srcId) => inferNodeOutputFields(srcId, nodesById, incomingByTarget, memo, stack))
  )
  const passthroughFields = upstreamFields.length > 0 ? upstreamFields : directFields

  let result: string[] = passthroughFields
  if (nodeType === 'map_transform') {
    const custom = parseCustomFieldSpecs(cfg.custom_fields)
    const primaryKeyField = String(cfg.custom_primary_key_field || cfg.custom_group_by_field || '').trim()
    if (custom.length > 0) {
      const customNames = uniqueFieldNames([
        ...custom.map((item) => String(item.name || '').trim()).filter(Boolean),
        ...(primaryKeyField ? [primaryKeyField] : []),
      ])
      const includeSource = Boolean(cfg.custom_include_source_fields ?? true)
      result = includeSource ? uniqueFieldNames([...passthroughFields, ...customNames]) : customNames
    } else {
      const selected = parseFieldList(cfg.fields)
      result = selected.length > 0 ? selected : passthroughFields
    }
  } else if (nodeType === 'rename_transform') {
    const mappings = parseRenameMappings(cfg.mappings)
    if (mappings.length > 0) {
      const mapped = (passthroughFields.length > 0 ? passthroughFields : mappings.map((m) => m.from)).map((f) => {
        const hit = mappings.find((m) => m.from === f)
        return hit ? hit.to : f
      })
      result = uniqueFieldNames([...mapped, ...mappings.map((m) => m.to)])
    }
  } else if (nodeType === 'aggregate_transform') {
    const groupBy = parseFieldList(cfg.group_by)
    const aggField = String(cfg.agg_field || '').trim()
    const out = uniqueFieldNames([...groupBy, ...(aggField ? [aggField] : [])])
    result = out.length > 0 ? out : passthroughFields
  }

  result = filterUserFacingFieldNames(result)
  memo.set(nodeId, result)
  stack.delete(nodeId)
  return result
}

function buildIncomingByTarget(edges: Array<{ source: string; target: string }>): Map<string, string[]> {
  const incomingByTarget = new Map<string, string[]>()
  edges.forEach((edge) => {
    const curr = incomingByTarget.get(edge.target) || []
    curr.push(edge.source)
    incomingByTarget.set(edge.target, curr)
  })
  return incomingByTarget
}

function inferUpstreamInputFields(targetNodeId: string, nodes: ETLNode[], edges: Array<{ source: string; target: string }>): string[] {
  const nodesById = new Map(nodes.map((n) => [n.id, n] as const))
  const incomingByTarget = buildIncomingByTarget(edges)

  const incomingIds = uniqueFieldNames((incomingByTarget.get(targetNodeId) || []).map((id) => id))
  if (incomingIds.length === 0) return []

  const memo = new Map<string, string[]>()
  const fields = uniqueFieldNames(
    incomingIds.flatMap((srcId) =>
      inferNodeOutputFields(srcId, nodesById, incomingByTarget, memo, new Set<string>())
    )
  )
  return filterUserFacingFieldNames(fields)
}

function inferUpstreamSources(
  targetNodeId: string,
  nodes: ETLNode[],
  edges: Array<{ source: string; target: string }>
): Array<{ id: string; label: string; fields: string[] }> {
  const nodesById = new Map(nodes.map((n) => [n.id, n] as const))
  const incomingByTarget = buildIncomingByTarget(edges)
  const incomingIds = uniqueFieldNames((incomingByTarget.get(targetNodeId) || []).map((id) => id))
  if (incomingIds.length === 0) return []

  const memo = new Map<string, string[]>()
  return incomingIds.map((srcId) => {
    const srcNode = nodesById.get(srcId)
    const label = String(srcNode?.data?.label || srcNode?.data?.definition?.label || srcId)
    const fields = inferNodeOutputFields(srcId, nodesById, incomingByTarget, memo, new Set<string>())
    return { id: srcId, label, fields: uniqueFieldNames(fields) }
  })
}

function extractPreviewRowsFromNode(node: ETLNode | undefined): Array<Record<string, unknown>> {
  if (!node?.data) return []
  const cfg = (node.data.config && typeof node.data.config === 'object'
    ? node.data.config
    : {}) as Record<string, unknown>

  const candidateSets = [
    cfg._preview_rows,
    node.data.executionSampleOutput,
    node.data.executionSampleInput,
  ]

  const out: Array<Record<string, unknown>> = []
  candidateSets.forEach((candidate) => {
    if (!Array.isArray(candidate)) return
    candidate.forEach((row) => {
      if (!row || typeof row !== 'object' || Array.isArray(row)) return
      out.push(row as Record<string, unknown>)
    })
  })
  return out
}

function inferUpstreamPreviewRows(
  targetNodeId: string,
  nodes: ETLNode[],
  edges: Array<{ source: string; target: string }>,
  maxRows = 30
): Array<Record<string, unknown>> {
  const nodesById = new Map(nodes.map((n) => [n.id, n] as const))
  const incomingByTarget = buildIncomingByTarget(edges)
  const queue = [...(incomingByTarget.get(targetNodeId) || [])]
  const visited = new Set<string>()
  const out: Array<Record<string, unknown>> = []

  while (queue.length > 0 && out.length < maxRows) {
    const nodeId = String(queue.shift() || '')
    if (!nodeId || visited.has(nodeId)) continue
    visited.add(nodeId)

    const node = nodesById.get(nodeId)
    const rows = extractPreviewRowsFromNode(node)
    if (rows.length > 0) {
      out.push(...rows.slice(0, Math.max(0, maxRows - out.length)))
      if (out.length >= maxRows) break
    }

    const upstream = incomingByTarget.get(nodeId) || []
    upstream.forEach((srcId) => {
      if (!visited.has(srcId)) queue.push(srcId)
    })
  }

  return out.slice(0, maxRows)
}

function splitPathSegments(path: string): string[] {
  const text = String(path || '').trim()
  if (!text) return []
  const out: string[] = []
  let current = ''
  let bracket = false
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i]
    if (!bracket && ch === '.') {
      if (current.trim()) out.push(current.trim())
      current = ''
      continue
    }
    if (ch === '[') {
      if (current.trim()) out.push(current.trim())
      current = ''
      bracket = true
      continue
    }
    if (ch === ']') {
      const token = current.trim()
      if (token) {
        const normalized = token.replace(/^['"]|['"]$/g, '').trim()
        out.push(normalized || '*')
      } else {
        out.push('*')
      }
      current = ''
      bracket = false
      continue
    }
    current += ch
  }
  if (current.trim()) out.push(current.trim())
  return out
}

function readPathValue(row: Record<string, unknown>, path: string): { found: boolean; value: unknown } {
  const segments = splitPathSegments(path)
  if (segments.length === 0) return { found: false, value: undefined }
  let current: unknown = row
  for (const seg of segments) {
    if (current === null || current === undefined) return { found: false, value: undefined }
    if (Array.isArray(current)) {
      if (seg === '*') {
        if (current.length === 0) return { found: false, value: undefined }
        current = current[0]
        continue
      }
      const idx = Number(seg)
      if (!Number.isInteger(idx) || idx < 0 || idx >= current.length) {
        return { found: false, value: undefined }
      }
      current = current[idx]
      continue
    }
    if (typeof current === 'object') {
      const obj = current as Record<string, unknown>
      if (!(seg in obj)) return { found: false, value: undefined }
      current = obj[seg]
      continue
    }
    return { found: false, value: undefined }
  }
  return { found: true, value: current }
}

function hasMeaningfulValue(value: unknown): boolean {
  if (value === null || value === undefined) return false
  if (typeof value === 'string') return value.trim().length > 0
  if (Array.isArray(value)) return value.length > 0
  if (typeof value === 'object') return Object.keys(value as Record<string, unknown>).length > 0
  return true
}

function filterRowsByPrimaryKey(
  rows: Array<Record<string, unknown>>,
  primaryKeyPath: string,
): Array<Record<string, unknown>> {
  const path = String(primaryKeyPath || '').trim()
  if (!path) return rows
  return rows.filter((row) => {
    const hit = readPathValue(row, path)
    if (!hit.found) return false
    return hasMeaningfulValue(hit.value)
  })
}

export default function ConfigDrawer({ open, onClose }: ConfigDrawerProps) {
  const {
    nodes,
    edges,
    selectedNodeId,
    pipeline,
    isExecuting,
    updateNodeConfig,
    updateNodeConfigSilent,
    updateNodeLabel,
    removeNode,
    duplicateNode,
  } = useWorkflowStore()
  const [form] = Form.useForm()
  const [activeTab, setActiveTab] = useState('config')
  const [jsonPathDetectLoading, setJsonPathDetectLoading] = useState(false)
  const [jsonPathDetectError, setJsonPathDetectError] = useState<string | null>(null)
  const [sourceFieldDetectLoading, setSourceFieldDetectLoading] = useState(false)
  const [sourceFieldDetectError, setSourceFieldDetectError] = useState<string | null>(null)
  const [customFieldStudioOpen, setCustomFieldStudioOpen] = useState(false)
  const [customFieldDraft, setCustomFieldDraft] = useState<CustomFieldSpec[]>([])
  const [customFieldBeautifyUndoById, setCustomFieldBeautifyUndoById] = useState<Record<string, CustomFieldBeautifyBackup>>({})
  const [customIncludeSourceDraft, setCustomIncludeSourceDraft] = useState(true)
  const [customExpressionEngineDraft, setCustomExpressionEngineDraft] = useState<CustomExpressionEngine>('auto')
  const [customPrimaryKeyFieldDraft, setCustomPrimaryKeyFieldDraft] = useState('')
  const [customProfileEnabledDraft, setCustomProfileEnabledDraft] = useState(false)
  const [customProfileStorageDraft, setCustomProfileStorageDraft] = useState<CustomProfileStorage>('lmdb')
  const [customProfileOracleHostDraft, setCustomProfileOracleHostDraft] = useState('')
  const [customProfileOraclePortDraft, setCustomProfileOraclePortDraft] = useState(1521)
  const [customProfileOracleServiceNameDraft, setCustomProfileOracleServiceNameDraft] = useState('')
  const [customProfileOracleSidDraft, setCustomProfileOracleSidDraft] = useState('')
  const [customProfileOracleUserDraft, setCustomProfileOracleUserDraft] = useState('')
  const [customProfileOraclePasswordDraft, setCustomProfileOraclePasswordDraft] = useState('')
  const [customProfileOracleDsnDraft, setCustomProfileOracleDsnDraft] = useState('')
  const [customProfileOracleTableDraft, setCustomProfileOracleTableDraft] = useState('ETL_PROFILE_STATE')
  const [customProfileOracleWriteStrategyDraft, setCustomProfileOracleWriteStrategyDraft] = useState<CustomProfileOracleWriteStrategy>('parallel_key')
  const [customProfileOracleParallelWorkersDraft, setCustomProfileOracleParallelWorkersDraft] = useState(4)
  const [customProfileOracleParallelMinTokensDraft, setCustomProfileOracleParallelMinTokensDraft] = useState(2000)
  const [customProfileOracleMergeBatchSizeDraft, setCustomProfileOracleMergeBatchSizeDraft] = useState(500)
  const [customProfileOracleQueueEnabledDraft, setCustomProfileOracleQueueEnabledDraft] = useState(true)
  const [customProfileOracleQueueWaitOnForceFlushDraft, setCustomProfileOracleQueueWaitOnForceFlushDraft] = useState(true)
  const [customProfileProcessingModeDraft, setCustomProfileProcessingModeDraft] = useState<CustomProfileProcessingMode>('batch')
  const [customProfileComputeStrategyDraft, setCustomProfileComputeStrategyDraft] = useState<CustomProfileComputeStrategy>('single')
  const [customProfileComputeExecutorDraft, setCustomProfileComputeExecutorDraft] = useState<CustomProfileComputeExecutor>('thread')
  const [customProfileComputeWorkersDraft, setCustomProfileComputeWorkersDraft] = useState(4)
  const [customProfileComputeMinRowsDraft, setCustomProfileComputeMinRowsDraft] = useState(20000)
  const [customProfileComputeGlobalPrefetchMaxTokensDraft, setCustomProfileComputeGlobalPrefetchMaxTokensDraft] = useState(40000)
  const [customProfileBackfillCandidatePrefetchChunkSizeDraft, setCustomProfileBackfillCandidatePrefetchChunkSizeDraft] = useState(500)
  const [customProfileLivePersistDraft, setCustomProfileLivePersistDraft] = useState(false)
  const [customProfileFlushEveryRowsDraft, setCustomProfileFlushEveryRowsDraft] = useState(20000)
  const [customProfileFlushIntervalSecondsDraft, setCustomProfileFlushIntervalSecondsDraft] = useState(2)
  const [customProfileAppendUniqueDisableMinRowsDraft, setCustomProfileAppendUniqueDisableMinRowsDraft] = useState(5000)
  const [customProfileAppendUniqueEntityLimitDraft, setCustomProfileAppendUniqueEntityLimitDraft] = useState(10000)
  const [customProfileAppendUniqueDisableRatioDraft, setCustomProfileAppendUniqueDisableRatioDraft] = useState(0.35)
  const [customProfileEmitModeDraft, setCustomProfileEmitModeDraft] = useState<'changed_only' | 'all_entities'>('changed_only')
  const [customProfileRequiredFieldsDraft, setCustomProfileRequiredFieldsDraft] = useState<string[]>([])
  const [customProfileEventTimeFieldDraft, setCustomProfileEventTimeFieldDraft] = useState('')
  const [customProfileWindowDaysDraft, setCustomProfileWindowDaysDraft] = useState('1,7,30')
  const [customProfileRetentionDaysDraft, setCustomProfileRetentionDaysDraft] = useState(45)
  const [customProfileIncludeChangeFieldsDraft, setCustomProfileIncludeChangeFieldsDraft] = useState(false)
  const [profileMonitorLoading, setProfileMonitorLoading] = useState(false)
  const [profileMonitorClearing, setProfileMonitorClearing] = useState(false)
  const [profileMonitorData, setProfileMonitorData] = useState<ProfileMonitorResponse | null>(null)
  const touchDrawerActivity = useCallback(() => {
    markDrawerInteraction('etl')
  }, [])

  useEffect(() => {
    if (!open) {
      clearDrawerInteraction('etl')
      return
    }
    touchDrawerActivity()
  }, [open, selectedNodeId, touchDrawerActivity])
  const [profileMonitorError, setProfileMonitorError] = useState<string | null>(null)
  const [profileDataModalOpen, setProfileDataModalOpen] = useState(false)
  const [selectedProfileEntityKey, setSelectedProfileEntityKey] = useState<string>('')
  const [expandedEditorFieldId, setExpandedEditorFieldId] = useState<string | null>(null)
  const [expandedEditorMode, setExpandedEditorMode] = useState<CustomFieldMode>('value')
  const [expandedEditorTab, setExpandedEditorTab] = useState<UiExpressionEditorTab>('expression')
  const [uiExpressionTemplateLabel, setUiExpressionTemplateLabel] = useState<string>(EXPRESSION_FUNCTION_SNIPPETS[0]?.label || '')
  const [uiExpressionValuesByIndex, setUiExpressionValuesByIndex] = useState<Record<number, string>>({})
  const [uiExpressionParamModeByIndex, setUiExpressionParamModeByIndex] = useState<Record<number, UiExpressionParameterMode>>({})
  const [uiExpressionVariables, setUiExpressionVariables] = useState<UiExpressionVariableSpec[]>([])
  const [uiExpressionBuilderTouched, setUiExpressionBuilderTouched] = useState(false)
  const [uiGroupAggregateKeyField, setUiGroupAggregateKeyField] = useState('')
  const [uiGroupAggregateKeyName, setUiGroupAggregateKeyName] = useState('group')
  const [uiGroupAggregateMetrics, setUiGroupAggregateMetrics] = useState<UiGroupAggregateMetric[]>([
    createUiGroupAggregateMetric({ outputName: 'metric', path: '', agg: 'sum' }),
  ])
  const [uiExpressionJsonKey, setUiExpressionJsonKey] = useState<string>('value')
  const [uiExpressionNewJsonKeyDraft, setUiExpressionNewJsonKeyDraft] = useState<string>('')
  const [uiConditionBuilderByIndex, setUiConditionBuilderByIndex] = useState<Record<number, UiConditionBuilderState>>({})
  const [uiConditionUndoByIndex, setUiConditionUndoByIndex] = useState<Record<number, UiConditionBuilderState[]>>({})
  const [uiConditionRedoByIndex, setUiConditionRedoByIndex] = useState<Record<number, UiConditionBuilderState[]>>({})
  const [uiConditionBuilderModalIndex, setUiConditionBuilderModalIndex] = useState<number | null>(null)
  const [uiFieldBuilderByIndex, setUiFieldBuilderByIndex] = useState<Record<number, UiFieldBuilderState>>({})
  const [uiFieldBuilderModalIndex, setUiFieldBuilderModalIndex] = useState<number | null>(null)
  const [uiBreJsonKey, setUiBreJsonKey] = useState<string>('value')
  const [uiBreModel, setUiBreModel] = useState<BreModel>(() => parseBreModelFromExpression("=if_(true, 'YES', 'NO')"))
  const [uiExpressionRows, setUiExpressionRows] = useState<UiExpressionBuilderRow[]>([])
  const uiBreLastAppliedExpressionRef = useRef<string>('')
  const uiConditionBuilderByIndexRef = useRef<Record<number, UiConditionBuilderState>>({})
  const uiConditionUndoByIndexRef = useRef<Record<number, UiConditionBuilderState[]>>({})
  const uiConditionRedoByIndexRef = useRef<Record<number, UiConditionBuilderState[]>>({})
  const [validationLoading, setValidationLoading] = useState(false)
  const [validationResult, setValidationResult] = useState<CustomFieldValidationResult | null>(null)
  const [validationModalOpen, setValidationModalOpen] = useState(false)
  const [validationSourceDraft, setValidationSourceDraft] = useState<'lmdb' | 'rocksdb' | 'sample'>('sample')
  const [validationLmdbEnvPathDraft, setValidationLmdbEnvPathDraft] = useState('')
  const [validationLmdbDbNameDraft, setValidationLmdbDbNameDraft] = useState('')
  const [validationLmdbKeyPrefixDraft, setValidationLmdbKeyPrefixDraft] = useState('')
  const [validationLmdbStartKeyDraft, setValidationLmdbStartKeyDraft] = useState('')
  const [validationLmdbEndKeyDraft, setValidationLmdbEndKeyDraft] = useState('')
  const [validationLmdbKeyContainsDraft, setValidationLmdbKeyContainsDraft] = useState('')
  const [validationLmdbLimitDraft, setValidationLmdbLimitDraft] = useState(50)
  const [activeExpressionFieldId, setActiveExpressionFieldId] = useState<string | null>(null)
  const [customEditorColorProfile, setCustomEditorColorProfile] = useState<CustomEditorColorProfile>(() => loadCustomEditorPrefs().colorProfile)
  const [customEditorFontPreset, setCustomEditorFontPreset] = useState<CustomEditorFontPreset>(() => loadCustomEditorPrefs().fontPreset)
  const [customEditorBeautifyStyle, setCustomEditorBeautifyStyle] = useState<CustomBeautifyStyle>(() => loadCustomEditorPrefs().beautifyStyle)
  const [customEditorFontSize, setCustomEditorFontSize] = useState(() => loadCustomEditorPrefs().fontSize)
  const [customEditorLineHeight, setCustomEditorLineHeight] = useState(() => loadCustomEditorPrefs().lineHeight)
  const [customEditorWordWrap, setCustomEditorWordWrap] = useState(() => loadCustomEditorPrefs().wordWrap)
  const [customEditorLigatures, setCustomEditorLigatures] = useState(() => loadCustomEditorPrefs().ligatures)
  const [collapsedCustomFieldIds, setCollapsedCustomFieldIds] = useState<string[]>([])
  const [exampleRepoCategory, setExampleRepoCategory] = useState<string>('all')
  const [exampleRepoSearch, setExampleRepoSearch] = useState('')
  const [oracleStudioOpen, setOracleStudioOpen] = useState(false)
  const [oracleMappingDraft, setOracleMappingDraft] = useState<OracleColumnMappingSpec[]>([])
  const [oracleOnlyMappedDraft, setOracleOnlyMappedDraft] = useState(false)
  const [oraclePreSqlDraft, setOraclePreSqlDraft] = useState('')
  const [oraclePostSqlDraft, setOraclePostSqlDraft] = useState('')
  const [oracleTableDraft, setOracleTableDraft] = useState('')
  const [oracleIfExistsDraft, setOracleIfExistsDraft] = useState<'append' | 'replace' | 'fail'>('append')
  const [oracleOperationDraft, setOracleOperationDraft] = useState<'insert' | 'update' | 'upsert'>('insert')
  const [oracleKeyColumnsDraft, setOracleKeyColumnsDraft] = useState<string[]>([])
  const [lmdbStudioOpen, setLmdbStudioOpen] = useState(false)
  const [lmdbStudioDraft, setLmdbStudioDraft] = useState<LmdbStudioDraft>(createLmdbStudioDraft())
  const [lmdbStudioLoading, setLmdbStudioLoading] = useState(false)
  const [lmdbPathBrowseLoading, setLmdbPathBrowseLoading] = useState(false)
  const [lmdbDeleteLoading, setLmdbDeleteLoading] = useState(false)
  const [lmdbStudioError, setLmdbStudioError] = useState<string | null>(null)
  const [lmdbPathBrowseError, setLmdbPathBrowseError] = useState<string | null>(null)
  const [lmdbDiscoveredPaths, setLmdbDiscoveredPaths] = useState<string[]>([])
  const [lmdbStudioPreviewRows, setLmdbStudioPreviewRows] = useState<Array<Record<string, unknown>>>([])
  const [lmdbStudioColumns, setLmdbStudioColumns] = useState<string[]>([])
  const [lmdbStudioRowCount, setLmdbStudioRowCount] = useState<number>(0)
  const [lmdbPreviewView, setLmdbPreviewView] = useState<'table' | 'json' | 'summary'>('table')
  const [lmdbSummaryRemote, setLmdbSummaryRemote] = useState<Record<string, unknown> | null>(null)
  const [lmdbSummaryLoading, setLmdbSummaryLoading] = useState<boolean>(false)
  const [lmdbSummaryError, setLmdbSummaryError] = useState<string | null>(null)
  const [lmdbPreviewDisplayLimit, setLmdbPreviewDisplayLimit] = useState<number>(200)
  const [lmdbTablePageSize, setLmdbTablePageSize] = useState<number>(25)
  const [lmdbTableCurrentPage, setLmdbTableCurrentPage] = useState<number>(1)
  const [lmdbPreviewHasMore, setLmdbPreviewHasMore] = useState<boolean>(false)
  const [lmdbVisibleColumns, setLmdbVisibleColumns] = useState<string[]>([])
  const [lmdbGlobalFilterColumn, setLmdbGlobalFilterColumn] = useState<string>(LMDB_GLOBAL_ALL_COLUMNS)
  const [lmdbGlobalFilterValues, setLmdbGlobalFilterValues] = useState<string[]>([])
  const [lmdbColumnFilterValues, setLmdbColumnFilterValues] = useState<Record<string, string[]>>({})
  const [lmdbJsonEditorOpen, setLmdbJsonEditorOpen] = useState(false)
  const [lmdbJsonEditorMode, setLmdbJsonEditorMode] = useState<'cell' | 'row'>('cell')
  const [lmdbJsonEditorTitle, setLmdbJsonEditorTitle] = useState('')
  const [lmdbJsonEditorText, setLmdbJsonEditorText] = useState('')
  const [lmdbJsonEditorOriginalText, setLmdbJsonEditorOriginalText] = useState('')
  const [lmdbJsonEditorRowIndex, setLmdbJsonEditorRowIndex] = useState<number | null>(null)
  const [lmdbJsonEditorField, setLmdbJsonEditorField] = useState('')
  const [lmdbJsonEditorError, setLmdbJsonEditorError] = useState<string | null>(null)
  const [lmdbJsonTagFilter, setLmdbJsonTagFilter] = useState('')

  const customEditorThemeId = CUSTOM_EDITOR_THEME_IDS[customEditorColorProfile]
  const customEditorFontFamily = resolveEditorFontFamily(customEditorFontPreset)
  useEffect(() => {
    try {
      const monacoAny = customEditorLastMonaco as any
      monacoAny?.editor?.setTheme?.(customEditorThemeId)
    } catch {
      // no-op
    }
  }, [customEditorThemeId])

  useEffect(() => {
    const remeasure = () => {
      try {
        const monacoAny = customEditorLastMonaco as any
        monacoAny?.editor?.remeasureFonts?.()
      } catch {
        // no-op
      }
    }

    remeasure()

    if (typeof document === 'undefined') return
    const docAny = document as any
    const fontFaceSet = docAny.fonts
    if (!fontFaceSet) return

    const familyName = (
      customEditorFontPreset === 'fira_code'
        ? '"Fira Code"'
        : customEditorFontPreset === 'consolas'
          ? 'Consolas'
          : '"JetBrains Mono"'
    )

    try {
      Promise.resolve(fontFaceSet.load(`13px ${familyName}`)).then(() => remeasure()).catch(() => undefined)
      Promise.resolve(fontFaceSet.ready).then(() => remeasure()).catch(() => undefined)
    } catch {
      // no-op
    }
  }, [customEditorFontPreset, customEditorFontFamily, customEditorFontSize])

  useEffect(() => {
    uiConditionBuilderByIndexRef.current = uiConditionBuilderByIndex
  }, [uiConditionBuilderByIndex])

  useEffect(() => {
    uiConditionUndoByIndexRef.current = uiConditionUndoByIndex
  }, [uiConditionUndoByIndex])

  useEffect(() => {
    uiConditionRedoByIndexRef.current = uiConditionRedoByIndex
  }, [uiConditionRedoByIndex])

  useEffect(() => {
    if (typeof window === 'undefined' || !window.localStorage) return
    try {
      const prefs: CustomEditorPrefs = {
        colorProfile: customEditorColorProfile,
        fontPreset: customEditorFontPreset,
        beautifyStyle: customEditorBeautifyStyle,
        fontSize: customEditorFontSize,
        lineHeight: customEditorLineHeight,
        wordWrap: customEditorWordWrap,
        ligatures: customEditorLigatures,
      }
      window.localStorage.setItem(CUSTOM_EDITOR_PREFS_STORAGE_KEY, JSON.stringify(prefs))
    } catch {
      // Ignore storage write failures.
    }
  }, [
    customEditorColorProfile,
    customEditorFontPreset,
    customEditorBeautifyStyle,
    customEditorFontSize,
    customEditorLineHeight,
    customEditorWordWrap,
    customEditorLigatures,
  ])

  const node = nodes.find(n => n.id === selectedNodeId)
  const data: ETLNodeData | undefined = node?.data

  useEffect(() => {
    if (data) {
      const initialConfig = (data.config && typeof data.config === 'object' ? data.config : {}) as Record<string, unknown>
      form.resetFields()
      form.setFieldsValue({ _label: data.label, ...initialConfig })
      setActiveTab('config')
    }
  }, [selectedNodeId])

  useEffect(() => {
    setJsonPathDetectError(null)
    setSourceFieldDetectError(null)
    setCustomFieldStudioOpen(false)
    setOracleStudioOpen(false)
    setLmdbStudioOpen(false)
    setLmdbStudioError(null)
    setLmdbPathBrowseError(null)
    setLmdbDiscoveredPaths([])
    setLmdbPathBrowseLoading(false)
    setLmdbDeleteLoading(false)
    setLmdbStudioPreviewRows([])
    setLmdbStudioColumns([])
    setLmdbStudioRowCount(0)
    setLmdbSummaryRemote(null)
    setLmdbSummaryLoading(false)
    setLmdbSummaryError(null)
    setLmdbPreviewView('table')
    setLmdbPreviewDisplayLimit(200)
    setLmdbTablePageSize(25)
    setLmdbTableCurrentPage(1)
    setLmdbPreviewHasMore(false)
    setLmdbVisibleColumns([])
    setLmdbGlobalFilterColumn(LMDB_GLOBAL_ALL_COLUMNS)
    setLmdbGlobalFilterValues([])
    setLmdbColumnFilterValues({})
    setLmdbJsonEditorOpen(false)
    setLmdbJsonEditorError(null)
    setLmdbJsonTagFilter('')
  }, [selectedNodeId])

  useEffect(() => {
    if (!selectedNodeId || !data) return
    if (data.nodeType !== 'schedule_trigger') return

    const cfg = (data.config && typeof data.config === 'object'
      ? data.config
      : {}) as Record<string, unknown>

    const patch: Record<string, unknown> = {}
    const hasEnabled = Object.prototype.hasOwnProperty.call(cfg, 'enabled')
    if (!hasEnabled) {
      patch.enabled = true
    }

    const rawCron = normalizeCronExpr(cfg.cron || DEFAULT_SCHEDULE_CRON)
    const presetFromConfig = String(cfg.schedule_preset || '').trim()
    const inferredPreset = detectSchedulePresetFromCron(rawCron)
    const normalizedPreset = presetFromConfig || inferredPreset

    if (presetFromConfig !== normalizedPreset) {
      patch.schedule_preset = normalizedPreset
    }

    if (!cfg.cron || normalizeCronExpr(cfg.cron) !== rawCron) {
      patch.cron = rawCron
    }

    if (
      normalizedPreset !== CUSTOM_SCHEDULE_PRESET
      && SCHEDULE_PRESET_TO_CRON[normalizedPreset]
      && rawCron !== SCHEDULE_PRESET_TO_CRON[normalizedPreset]
    ) {
      patch.cron = SCHEDULE_PRESET_TO_CRON[normalizedPreset]
    }

    if (Object.keys(patch).length > 0) {
      updateNodeConfigSilent(selectedNodeId, patch)
    }
  }, [selectedNodeId, data?.nodeType])

  const definition = data?.definition
  const nodeType = data?.nodeType || ''
  const isFileSource = FILE_SOURCE_TYPES.includes(nodeType)
  const isFileDest   = FILE_DEST_TYPES.includes(nodeType)
  const isDatabaseSource = DB_SOURCE_TYPES.includes(nodeType)
  const isRocksdbSource = nodeType === 'rocksdb_source'
  const isLmdbSource = nodeType === 'lmdb_source' || isRocksdbSource
  const kvSourceType = isRocksdbSource ? 'rocksdb_source' : 'lmdb_source'
  const kvSourceLabel = isRocksdbSource ? 'RocksDB' : 'LMDB'
  const kvSourceLabelUpper = isRocksdbSource ? 'ROCKSDB' : 'LMDB'
  const kvSourceRootVar = isRocksdbSource ? '${ROCKSDB_ROOT}' : '${LMDB_ROOT}'
  const kvPathPlaceholder = isRocksdbSource ? '/data/profile_store.rocksdb' : '/data/profile_store.lmdb'
  const fileType     = getFileType(nodeType)
  const isApiJsonNode = nodeType === 'rest_api_source' || nodeType === 'graphql_source'
  const nodeConfig = (data?.config && typeof data.config === 'object' ? data.config : {}) as Record<string, unknown>
  const nodeEnabled = parseBoolLike(nodeConfig.node_enabled, true)
  const configuredCustomFields = parseCustomFieldSpecs(nodeConfig.custom_fields)
  const customFieldConfiguredCount = configuredCustomFields.length
  const customFieldEnabledConfiguredCount = configuredCustomFields.filter((item) => item.enabled).length
  const customFieldDraftCount = customFieldDraft.length
  const customFieldDraftEnabledCount = customFieldDraft.filter((item) => item.enabled).length
  const customFieldDraftCollapsedCount = collapsedCustomFieldIds.length
  const configuredOracleMappings = parseOracleColumnMappings(nodeConfig.oracle_column_mappings)
  const oracleMappingConfiguredCount = configuredOracleMappings.length
  const oracleOnlyMappedConfigured = parseBoolLike(nodeConfig.oracle_only_mapped_columns, false)
  const oracleConfiguredPreSql = String(nodeConfig.oracle_pre_sql || '')
  const oracleConfiguredPostSql = String(nodeConfig.oracle_post_sql || '')
  const oracleConfiguredTable = String(nodeConfig.table || '').trim()
  const oracleConfiguredIfExistsRaw = String(nodeConfig.if_exists || 'append').trim().toLowerCase()
  const oracleConfiguredIfExists: 'append' | 'replace' | 'fail' = (
    oracleConfiguredIfExistsRaw === 'replace'
      ? 'replace'
      : oracleConfiguredIfExistsRaw === 'fail'
        ? 'fail'
        : 'append'
  )
  const oracleConfiguredOperationRaw = String(nodeConfig.oracle_operation || 'insert').trim().toLowerCase()
  const oracleConfiguredOperation: 'insert' | 'update' | 'upsert' = (
    oracleConfiguredOperationRaw === 'update'
      ? 'update'
      : oracleConfiguredOperationRaw === 'upsert'
        ? 'upsert'
        : 'insert'
  )
  const oracleConfiguredKeyColumns = parseFieldList(nodeConfig.oracle_key_columns)
  const lmdbConfigured = useMemo(
    () => createLmdbStudioDraft({
      env_path: String(nodeConfig.env_path || nodeConfig.file_path || ''),
      db_name: String(nodeConfig.db_name || ''),
      key_prefix: String(nodeConfig.key_prefix || ''),
      start_key: String(nodeConfig.start_key || ''),
      end_key: String(nodeConfig.end_key || ''),
      key_contains: String(nodeConfig.key_contains || ''),
      value_contains: String(nodeConfig.value_contains || ''),
      value_format: String(nodeConfig.value_format || 'auto') as 'auto' | 'json' | 'text' | 'base64',
      flatten_json_values: parseBoolLike(nodeConfig.flatten_json_values, true),
      expand_profile_documents: parseBoolLike(nodeConfig.expand_profile_documents, true),
      limit: Number(nodeConfig.limit ?? 1000),
    }),
    [nodeConfig]
  )
  const lmdbPreviewKeys = useMemo(
    () => uniqueFieldNames(
      (lmdbStudioPreviewRows || [])
        .map((row) => String((row as Record<string, unknown>)?.lmdb_key || '').trim())
        .filter(Boolean)
    ),
    [lmdbStudioPreviewRows]
  )
  const lmdbPreviewDisplayRows = useMemo(
    () => (lmdbStudioPreviewRows || []),
    [lmdbStudioPreviewRows]
  )
  const lmdbAllPreviewColumns = useMemo(
    () => uniqueFieldNames([
      ...lmdbStudioColumns,
      ...lmdbPreviewDisplayRows.flatMap((row) => Object.keys(row || {})),
    ]).slice(0, 100),
    [lmdbStudioColumns, lmdbPreviewDisplayRows]
  )
  useEffect(() => {
    if (lmdbAllPreviewColumns.length === 0) {
      setLmdbVisibleColumns([])
      return
    }
    setLmdbVisibleColumns((prev) => {
      if (!Array.isArray(prev) || prev.length === 0) return lmdbAllPreviewColumns
      const kept = prev.filter((name) => lmdbAllPreviewColumns.includes(name))
      return kept.length > 0 ? kept : lmdbAllPreviewColumns
    })
  }, [lmdbAllPreviewColumns])
  useEffect(() => {
    setLmdbColumnFilterValues((prev) => {
      const next: Record<string, string[]> = {}
      Object.entries(prev || {}).forEach(([name, values]) => {
        if (!lmdbAllPreviewColumns.includes(name)) return
        const normalized = uniqueFieldNames((values || []).map((v) => String(v || '').trim()).filter(Boolean))
        if (normalized.length > 0) next[name] = normalized
      })
      return next
    })
    if (
      lmdbGlobalFilterColumn !== LMDB_GLOBAL_ALL_COLUMNS
      && !lmdbAllPreviewColumns.includes(lmdbGlobalFilterColumn)
    ) {
      setLmdbGlobalFilterColumn(LMDB_GLOBAL_ALL_COLUMNS)
      setLmdbGlobalFilterValues([])
    }
  }, [lmdbAllPreviewColumns, lmdbGlobalFilterColumn])
  useEffect(() => {
    setLmdbGlobalFilterValues([])
  }, [lmdbGlobalFilterColumn])

  const lmdbPreviewRowsLimited = useMemo(() => {
    const safeLimit = Math.max(1, Math.min(
      Number.isFinite(Number(lmdbPreviewDisplayLimit)) ? Math.floor(Number(lmdbPreviewDisplayLimit)) : 200,
      2000
    ))
    return (lmdbStudioPreviewRows || []).slice(0, safeLimit)
  }, [lmdbStudioPreviewRows, lmdbPreviewDisplayLimit])
  const lmdbPreviewTableData = useMemo(
    () => {
      const pageOffset = Math.max(0, (Number(lmdbTableCurrentPage || 1) - 1) * Number(lmdbTablePageSize || 25))
      return (lmdbStudioPreviewRows || []).map((row, idx) => ({
      ...row,
      __lmdb_preview_row_id: `lmdb_preview_${pageOffset + idx}`,
      __lmdb_preview_row_index: pageOffset + idx,
      }))
    },
    [lmdbStudioPreviewRows, lmdbTableCurrentPage, lmdbTablePageSize]
  )
  const lmdbGlobalFilterColumnOptions = useMemo(
    () => ([
      { value: LMDB_GLOBAL_ALL_COLUMNS, label: 'All Visible Columns' },
      ...lmdbVisibleColumns.map((name) => ({ value: name, label: name })),
    ]),
    [lmdbVisibleColumns]
  )
  const lmdbGlobalFilterValueOptions = useMemo(
    () => {
      const targetColumns = lmdbGlobalFilterColumn === LMDB_GLOBAL_ALL_COLUMNS
        ? lmdbVisibleColumns
        : (lmdbGlobalFilterColumn ? [lmdbGlobalFilterColumn] : [])
      if (targetColumns.length === 0) return []
      const values: string[] = []
      const seen = new Set<string>()
      for (const row of lmdbPreviewTableData) {
        for (const name of targetColumns) {
          const text = toPreviewCellText((row as Record<string, unknown>)?.[name]).trim()
          const token = text || LMDB_EMPTY_FILTER_TOKEN
          if (seen.has(token)) continue
          seen.add(token)
          values.push(text)
          if (values.length >= 500) break
        }
        if (values.length >= 500) break
      }
      return values.map((value) => ({
        value: value || LMDB_EMPTY_FILTER_TOKEN,
        label: value || '(blank)',
      }))
    },
    [lmdbPreviewTableData, lmdbVisibleColumns, lmdbGlobalFilterColumn]
  )
  const lmdbPreviewTableDataGlobalFiltered = useMemo(
    () => {
      const queries = uniqueFieldNames(
        (lmdbGlobalFilterValues || []).map((value) => String(value || '').trim()).filter(Boolean)
      )
      if (queries.length === 0) return lmdbPreviewTableData
      const targetColumns = lmdbGlobalFilterColumn === LMDB_GLOBAL_ALL_COLUMNS
        ? lmdbVisibleColumns
        : (lmdbGlobalFilterColumn ? [lmdbGlobalFilterColumn] : [])
      if (targetColumns.length === 0) return lmdbPreviewTableData
      return lmdbPreviewTableData.filter((row) => targetColumns.some((name) => {
        const text = toPreviewCellText((row as Record<string, unknown>)?.[name]).trim()
        const textLower = text.toLowerCase()
        return queries.some((queryRaw) => {
          if (queryRaw === LMDB_EMPTY_FILTER_TOKEN) return text === ''
          return textLower.includes(queryRaw.toLowerCase())
        })
      }))
    },
    [lmdbPreviewTableData, lmdbGlobalFilterColumn, lmdbGlobalFilterValues, lmdbVisibleColumns]
  )
  const lmdbSummaryFallback = useMemo(() => {
    const rows = lmdbPreviewTableDataGlobalFiltered
    const keyCounts: Record<string, number> = {}
    const entitySet = new Set<string>()
    const nodeStatsByKey: Record<string, { processed: number; validated: number; outputRows: number }> = {}
    let profileRows = 0
    const toNonNegativeInt = (value: unknown): number => {
      const parsed = Number(value)
      if (!Number.isFinite(parsed) || parsed < 0) return 0
      return Math.floor(parsed)
    }

    rows.forEach((row) => {
      const rec = row as Record<string, unknown>
      const key = String(rec.lmdb_key || '').trim()
      if (key) {
        keyCounts[key] = (keyCounts[key] || 0) + 1
      }
      const entityKey = String(rec.lmdb_entity_key || '').trim()
      const profileSource = String(rec._lmdb_profile_source || '').trim().toLowerCase()
      const isProfileRow = Boolean(entityKey) || profileSource === 'documents' || profileSource === 'document' || profileSource === 'profile'
      if (isProfileRow) {
        profileRows += 1
      }
      if (entityKey) {
        entitySet.add(entityKey)
      }

      const nodeStats = rec._lmdb_node_stats
      if (nodeStats && typeof nodeStats === 'object' && !Array.isArray(nodeStats)) {
        const statRec = nodeStats as Record<string, unknown>
        const statKey = key || entityKey || '__lmdb_default__'
        const existing = nodeStatsByKey[statKey] || { processed: 0, validated: 0, outputRows: 0 }
        existing.processed = Math.max(
          existing.processed,
          toNonNegativeInt(statRec.custom_fields_incremental_processed_rows),
        )
        existing.validated = Math.max(
          existing.validated,
          toNonNegativeInt(statRec.custom_fields_incremental_validated_rows),
        )
        existing.outputRows = Math.max(
          existing.outputRows,
          toNonNegativeInt(statRec.custom_fields_incremental_output_rows),
        )
        nodeStatsByKey[statKey] = existing
      }
    })

    const incrementalProcessedRows = Object.values(nodeStatsByKey)
      .reduce((sum, item) => sum + toNonNegativeInt(item.processed), 0)
    const incrementalValidatedRows = Object.values(nodeStatsByKey)
      .reduce((sum, item) => sum + toNonNegativeInt(item.validated), 0)
    const incrementalOutputRows = Object.values(nodeStatsByKey)
      .reduce((sum, item) => sum + toNonNegativeInt(item.outputRows), 0)

    return {
      totalRows: Math.max(Number(lmdbStudioRowCount || 0), rows.length),
      scannedEntries: Math.max(Number(lmdbStudioRowCount || 0), rows.length),
      detectedColumns: lmdbStudioColumns.length,
      uniqueKeys: Object.keys(keyCounts).length,
      profileRows,
      nonProfileRows: Math.max(0, rows.length - profileRows),
      uniqueEntities: entitySet.size,
      profileLikeKeys: Object.keys(keyCounts).filter((key) => key.toLowerCase().includes('profile')).length,
      txnLatencySamples: 0,
      txnLatencyAvgMs: null as number | null,
      txnLatencyMinMs: null as number | null,
      txnLatencyMaxMs: null as number | null,
      processingLatencyRps: null as number | null,
      scanElapsedSeconds: null as number | null,
      scanCapped: false,
      scanLimit: 0,
      customFieldsIncrementalProcessedRows: incrementalProcessedRows,
      customFieldsIncrementalValidatedRows: incrementalValidatedRows,
      customFieldsIncrementalOutputRows: incrementalOutputRows,
    }
  }, [lmdbPreviewTableDataGlobalFiltered, lmdbStudioRowCount, lmdbStudioColumns])
  const lmdbSummaryStats = useMemo(() => {
    const fallback = lmdbSummaryFallback
    const remote = lmdbSummaryRemote as Record<string, unknown> | null
    const toNumber = (value: unknown, fallbackValue: number): number => {
      const parsed = Number(value)
      return Number.isFinite(parsed) ? parsed : fallbackValue
    }
    if (!remote) return fallback

    return {
      totalRows: toNumber(remote.total_rows, fallback.totalRows),
      scannedEntries: toNumber(remote.scanned_entries, fallback.totalRows),
      detectedColumns: fallback.detectedColumns,
      uniqueKeys: toNumber(remote.unique_keys, fallback.uniqueKeys),
      profileRows: toNumber(remote.profile_rows, fallback.profileRows),
      nonProfileRows: toNumber(remote.non_profile_rows, fallback.nonProfileRows),
      uniqueEntities: toNumber(remote.unique_entities, fallback.uniqueEntities),
      profileLikeKeys: toNumber(remote.profile_like_keys, fallback.profileLikeKeys),
      txnLatencySamples: toNumber(remote.txn_latency_samples, fallback.txnLatencySamples),
      txnLatencyAvgMs: remote.txn_latency_avg_ms === null || remote.txn_latency_avg_ms === undefined
        ? fallback.txnLatencyAvgMs
        : toNumber(remote.txn_latency_avg_ms, 0),
      txnLatencyMinMs: remote.txn_latency_min_ms === null || remote.txn_latency_min_ms === undefined
        ? fallback.txnLatencyMinMs
        : toNumber(remote.txn_latency_min_ms, 0),
      txnLatencyMaxMs: remote.txn_latency_max_ms === null || remote.txn_latency_max_ms === undefined
        ? fallback.txnLatencyMaxMs
        : toNumber(remote.txn_latency_max_ms, 0),
      processingLatencyRps: remote.processing_latency_rps === null || remote.processing_latency_rps === undefined
        ? (
          remote.records_processed_per_second === null || remote.records_processed_per_second === undefined
            ? fallback.processingLatencyRps
            : toNumber(remote.records_processed_per_second, 0)
        )
        : toNumber(remote.processing_latency_rps, 0),
      scanElapsedSeconds: remote.scan_elapsed_seconds === null || remote.scan_elapsed_seconds === undefined
        ? fallback.scanElapsedSeconds
        : toNumber(remote.scan_elapsed_seconds, 0),
      scanCapped: Boolean(remote.scan_capped),
      scanLimit: toNumber(remote.scan_limit, fallback.scanLimit),
      customFieldsIncrementalProcessedRows: toNumber(
        remote.custom_fields_incremental_processed_rows,
        fallback.customFieldsIncrementalProcessedRows,
      ),
      customFieldsIncrementalValidatedRows: toNumber(
        remote.custom_fields_incremental_validated_rows,
        fallback.customFieldsIncrementalValidatedRows,
      ),
      customFieldsIncrementalOutputRows: toNumber(
        remote.custom_fields_incremental_output_rows,
        fallback.customFieldsIncrementalOutputRows,
      ),
    }
  }, [lmdbSummaryFallback, lmdbSummaryRemote])
  const lmdbPreviewTableColumns = useMemo(() => {
    const visibleSet = new Set(lmdbVisibleColumns)
    const columns: any[] = lmdbAllPreviewColumns
      .filter((name) => visibleSet.has(name))
      .map((name) => {
        const optionValues: string[] = []
        const seen = new Set<string>()
        for (const row of lmdbPreviewTableDataGlobalFiltered) {
          const text = toPreviewCellText((row as Record<string, unknown>)?.[name]).trim()
          const token = text || LMDB_EMPTY_FILTER_TOKEN
          if (seen.has(token)) continue
          seen.add(token)
          optionValues.push(text)
          if (optionValues.length >= 300) break
        }
        const selectedValues = lmdbColumnFilterValues[name] || []
        return {
          title: name,
          dataIndex: name,
          key: name,
          ellipsis: true,
          filters: optionValues.map((value) => ({
            text: value || '(blank)',
            value: value || LMDB_EMPTY_FILTER_TOKEN,
          })),
          filterSearch: true,
          filteredValue: selectedValues.length > 0 ? selectedValues : null,
          onFilter: (filterValue: string | number | boolean, record: Record<string, unknown>) => {
            const text = toPreviewCellText(record?.[name]).trim()
            if (String(filterValue) === LMDB_EMPTY_FILTER_TOKEN) return text === ''
            return text === String(filterValue)
          },
          sorter: (a: Record<string, unknown>, b: Record<string, unknown>) => {
            const aNum = tryParseSortableNumber(a?.[name])
            const bNum = tryParseSortableNumber(b?.[name])
            if (aNum !== null && bNum !== null) return aNum - bNum
            return toPreviewCellText(a?.[name]).localeCompare(toPreviewCellText(b?.[name]), undefined, {
              numeric: true,
              sensitivity: 'base',
            })
          },
          render: (value: unknown, record: Record<string, unknown>) => {
            const text = toPreviewCellText(value)
            const short = text.length > 180 ? `${text.slice(0, 177)}...` : text
            return (
              <span
                style={{ fontFamily: 'monospace', fontSize: 12, cursor: 'pointer' }}
                onClick={(event) => {
                  event.stopPropagation()
                  const rowIndexRaw = record?.__lmdb_preview_row_index
                  const rowIndex = Number.isFinite(Number(rowIndexRaw)) ? Number(rowIndexRaw) : null
                  openLmdbCellJsonEditor(rowIndex, String(name), value)
                }}
              >
                {short || ' '}
              </span>
            )
          },
        }
      })

    columns.push({
      title: 'Actions',
      key: '__lmdb_actions',
      fixed: 'right' as const,
      width: 110,
      render: (_: unknown, record: Record<string, unknown>) => {
        const rowIndexRaw = record?.__lmdb_preview_row_index
        const rowIndex = Number.isFinite(Number(rowIndexRaw)) ? Number(rowIndexRaw) : null
        const rowPayload: Record<string, unknown> = {}
        Object.keys(record || {}).forEach((key) => {
          if (!key.startsWith('__lmdb_preview_')) {
            rowPayload[key] = record[key]
          }
        })
        return (
          <Button
            size="small"
            onClick={(event) => {
              event.stopPropagation()
              openLmdbRowJsonEditor(rowIndex, rowPayload)
            }}
          >
            Row JSON
          </Button>
        )
      },
    })
    return columns
  }, [lmdbAllPreviewColumns, lmdbVisibleColumns, lmdbPreviewTableDataGlobalFiltered, lmdbColumnFilterValues])
  const lmdbJsonEditorParsedValue = useMemo(() => {
    const raw = String(lmdbJsonEditorText || '').trim()
    if (!raw) return null
    try {
      return JSON.parse(raw)
    } catch {
      return null
    }
  }, [lmdbJsonEditorText])
  const lmdbJsonEditorHasParseError = useMemo(() => {
    const raw = String(lmdbJsonEditorText || '').trim()
    if (!raw) return false
    try {
      JSON.parse(raw)
      return false
    } catch {
      return true
    }
  }, [lmdbJsonEditorText])
  const lmdbJsonEditorPathMappings = useMemo(
    () => buildJsonTagMappingsFromParsed(lmdbJsonEditorParsedValue),
    [lmdbJsonEditorParsedValue]
  )
  const lmdbJsonEditorFilteredPathMappings = useMemo(() => {
    const query = String(lmdbJsonTagFilter || '').trim().toLowerCase()
    if (!query) return lmdbJsonEditorPathMappings
    return lmdbJsonEditorPathMappings.filter((item) => (
      item.path.toLowerCase().includes(query)
      || item.kind.toLowerCase().includes(query)
      || item.sample.toLowerCase().includes(query)
    ))
  }, [lmdbJsonEditorPathMappings, lmdbJsonTagFilter])
  const lmdbEnvPathPresetOptions = useMemo(
    () => uniqueFieldNames([
      String(lmdbStudioDraft.env_path || '').trim(),
      String(lmdbConfigured.env_path || '').trim(),
      String(validationLmdbEnvPathDraft || '').trim(),
      ...lmdbDiscoveredPaths,
      `${kvSourceRootVar}/${isRocksdbSource ? 'profile_store.rocksdb' : 'profile_store.lmdb'}`,
      `${kvSourceRootVar}/${isRocksdbSource ? 'demo_profiles.rocksdb' : 'demo_profiles.lmdb'}`,
      isRocksdbSource ? '~/data/profile_store.rocksdb' : '~/data/profile_store.lmdb',
      isRocksdbSource ? '/data/profile_store.rocksdb' : '/data/profile_store.lmdb',
    ].filter(Boolean))
      .map((value) => ({ value, label: value })),
    [
      lmdbStudioDraft.env_path,
      lmdbConfigured.env_path,
      validationLmdbEnvPathDraft,
      lmdbDiscoveredPaths,
      kvSourceRootVar,
      isRocksdbSource,
    ]
  )
  const lmdbDbNamePresetOptions = useMemo(
    () => {
      const values = uniqueFieldNames([
        String(lmdbStudioDraft.db_name || '').trim(),
        String(lmdbConfigured.db_name || '').trim(),
        'profiles',
        'state',
        'events',
      ].filter(Boolean))
      return [
        { value: '', label: '(default DB)' },
        ...values.map((value) => ({ value, label: value })),
      ]
    },
    [lmdbStudioDraft.db_name, lmdbConfigured.db_name]
  )
  const lmdbKeyPrefixPresetOptions = useMemo(
    () => {
      const prefixes: string[] = []
      lmdbPreviewKeys.forEach((key) => {
        const parts = String(key).split(':').filter(Boolean)
        const maxDepth = Math.min(4, Math.max(0, parts.length - 1))
        for (let idx = 1; idx <= maxDepth; idx += 1) {
          prefixes.push(`${parts.slice(0, idx).join(':')}:`)
        }
      })
      const values = uniqueFieldNames([
        String(lmdbStudioDraft.key_prefix || '').trim(),
        String(lmdbConfigured.key_prefix || '').trim(),
        'agent:',
        'pipeline:',
        'profile:',
        'customer:',
        'workflow:',
        ...prefixes,
      ].filter(Boolean))
      return values.slice(0, 160).map((value) => ({ value, label: value }))
    },
    [lmdbStudioDraft.key_prefix, lmdbConfigured.key_prefix, lmdbPreviewKeys]
  )
  const lmdbKeyPresetOptions = useMemo(
    () => lmdbPreviewKeys.slice(0, 300).map((value) => ({ value, label: value })),
    [lmdbPreviewKeys]
  )
  const schedulePresetValue = nodeType === 'schedule_trigger'
    ? String(nodeConfig.schedule_preset || detectSchedulePresetFromCron(nodeConfig.cron || DEFAULT_SCHEDULE_CRON))
    : ''
  const scheduleCronValue = nodeType === 'schedule_trigger'
    ? normalizeCronExpr(nodeConfig.cron || DEFAULT_SCHEDULE_CRON)
    : ''
  const upstreamReferenceFields = useMemo(
    () => (
      nodeType === 'map_transform' && selectedNodeId
        ? inferUpstreamInputFields(selectedNodeId, nodes, edges)
        : []
    ),
    [nodeType, selectedNodeId, nodes, edges]
  )
  const upstreamPreviewReferenceFields = useMemo(
    () => {
      if (nodeType !== 'map_transform' || !selectedNodeId) return []
      const previewRows = inferUpstreamPreviewRows(selectedNodeId, nodes, edges, 300)
      if (!previewRows.length) return []
      return filterUserFacingFieldNames(extractSampleFieldPaths(previewRows))
    },
    [nodeType, selectedNodeId, nodes, edges]
  )
  const currentNodeReferenceFields = useMemo(
    () => (nodeType === 'map_transform' ? extractDirectNodeFields(node) : []),
    [nodeType, node]
  )
  const configuredMapFields = useMemo(
    () => parseFieldList(nodeConfig.fields),
    [nodeConfig.fields]
  )
  const draftMapFields = useMemo(
    () => uniqueFieldNames([
      ...customFieldDraft.map((item) => String(item.name || '').trim()).filter(Boolean),
      ...(String(customPrimaryKeyFieldDraft || '').trim() ? [String(customPrimaryKeyFieldDraft || '').trim()] : []),
    ]),
    [customFieldDraft, customPrimaryKeyFieldDraft]
  )
  const effectiveMapFields = useMemo(() => {
    if (nodeType !== 'map_transform') return configuredMapFields
    if (!customFieldStudioOpen) return configuredMapFields
    // While studio is open, always reflect live draft field/key changes.
    return draftMapFields.length > 0 ? draftMapFields : configuredMapFields
  }, [nodeType, customFieldStudioOpen, draftMapFields, configuredMapFields])
  const mapInputFieldOptions = filterUserFacingFieldNames([
    ...effectiveMapFields,
    ...upstreamReferenceFields,
  ])
  const primaryKeyFieldForFilter = String(
    customFieldStudioOpen
      ? customPrimaryKeyFieldDraft
      : (nodeConfig.custom_primary_key_field || nodeConfig.custom_group_by_field || '')
  ).trim()
  const persistedPrimaryKeyField = String(
    nodeConfig.custom_primary_key_field
    || nodeConfig.custom_group_by_field
    || ''
  ).trim()
  const primaryKeyFieldOptionPool = filterUserFacingFieldNames([
    ...upstreamReferenceFields,
    ...mapInputFieldOptions,
    ...customFieldDraft.map((item) => String(item.name || '').trim()).filter(Boolean),
    ...(persistedPrimaryKeyField ? [persistedPrimaryKeyField] : []),
    ...(primaryKeyFieldForFilter ? [primaryKeyFieldForFilter] : []),
  ])
  const primaryKeyScopedUpstreamFields = useMemo(() => {
    if (nodeType !== 'map_transform' || !selectedNodeId || !primaryKeyFieldForFilter) return []
    const previewRows = inferUpstreamPreviewRows(selectedNodeId, nodes, edges, 200)
    if (!previewRows.length) return []
    const filteredRows = filterRowsByPrimaryKey(previewRows, primaryKeyFieldForFilter)
    if (!filteredRows.length) return []
    return filterUserFacingFieldNames(extractSampleFieldPaths(filteredRows))
  }, [nodeType, selectedNodeId, nodes, edges, primaryKeyFieldForFilter])
  const referenceFieldOptions = primaryKeyFieldOptionPool
  const expressionFieldOptions = filterUserFacingFieldNames([
    ...(primaryKeyScopedUpstreamFields.length > 0 ? primaryKeyScopedUpstreamFields : upstreamReferenceFields),
    ...upstreamPreviewReferenceFields,
    ...currentNodeReferenceFields,
    ...parseFieldList(nodeConfig._detected_columns),
    ...parseDetectedJsonPaths(nodeConfig._json_paths),
    ...(primaryKeyFieldForFilter ? [primaryKeyFieldForFilter] : []),
    ...mapInputFieldOptions,
    ...customFieldDraft.map((item) => String(item.name || '').trim()).filter(Boolean),
  ])
  const customProfilePathOptions = useMemo(
    () => filterUserFacingFieldNames(buildCustomProfilePathSuggestions(customFieldDraft)),
    [customFieldDraft]
  )
  const expressionPathOptions = useMemo(
    () => filterUserFacingFieldNames([...expressionFieldOptions, ...customProfilePathOptions]),
    [expressionFieldOptions, customProfilePathOptions]
  )
  const expressionCompletionEntries = useMemo<ExpressionCompletionEntry[]>(() => {
    const out: ExpressionCompletionEntry[] = []
    EXPRESSION_FUNCTION_SNIPPETS.forEach((fn) => {
      const enrichedSnippet = enrichFunctionSnippetWithFieldChoices(fn.snippet, expressionFieldOptions)
      const normalizedTemplate = normalizeSnippetTemplateText(enrichedSnippet)
      out.push({
        label: fn.label,
        insertText: enrichedSnippet,
        kind: 'function',
        detail: fn.label,
        documentation: `Template: \`${normalizedTemplate}\`\n\nSelect and use Tab to fill parameters.`,
        command: { id: 'editor.action.triggerParameterHints' },
      })
    })
    expressionFieldOptions.forEach((fieldName) => {
      out.push({
        label: fieldName,
        insertText: `field('${fieldName.replace(/'/g, "\\'")}')`,
        kind: 'field',
        detail: 'Source/Custom Field',
      })
    })
    expressionPathOptions.forEach((pathName) => {
      out.push({
        label: pathName,
        insertText: pathName,
        kind: 'path',
        detail: 'Field/Profile Path',
      })
    })
    ;[
      { label: 'null', value: 'null' },
      { label: 'true', value: 'true' },
      { label: 'false', value: 'false' },
    ].forEach((kw) => {
      out.push({ label: kw.label, insertText: kw.value, kind: 'keyword', detail: 'Literal' })
    })
    return out
  }, [expressionFieldOptions, expressionPathOptions])
  // Keep validation/autocomplete context in sync before Monaco editor mount.
  setExpressionCompletionEntries(expressionCompletionEntries)
  const uiExpressionTemplateOptions = useMemo(
    () => EXPRESSION_FUNCTION_SNIPPETS.map((item) => ({
      value: item.label,
      label: item.label,
      snippet: item.snippet,
    })),
    []
  )
  const selectedUiExpressionTemplate = useMemo(
    () => uiExpressionTemplateOptions.find((item) => item.value === uiExpressionTemplateLabel) || uiExpressionTemplateOptions[0],
    [uiExpressionTemplateOptions, uiExpressionTemplateLabel]
  )
  const selectedUiExpressionTemplateSnippet = String(selectedUiExpressionTemplate?.snippet || '')
  const isUiGroupAggregateTemplate = useMemo(
    () => /group_aggregate/i.test(String(selectedUiExpressionTemplate?.value || ''))
      || /group_aggregate/i.test(selectedUiExpressionTemplateSnippet),
    [selectedUiExpressionTemplate?.value, selectedUiExpressionTemplateSnippet]
  )
  const uiExpressionPlaceholders = useMemo(
    () => parseSnippetPlaceholders(selectedUiExpressionTemplateSnippet),
    [selectedUiExpressionTemplateSnippet]
  )
  const uiExpressionResolvedValuesByIndex = useMemo(
    () => (
      (uiExpressionPlaceholders || []).reduce((acc, placeholder) => {
        const inputValue = String(
          uiExpressionValuesByIndex[placeholder.index] ?? placeholder.defaultValue ?? ''
        )
        const mode = (uiExpressionParamModeByIndex[placeholder.index] || defaultUiExpressionParameterMode(placeholder)) as UiExpressionParameterMode
        acc[placeholder.index] = resolveUiExpressionParameterToken(mode, inputValue, uiExpressionVariables)
        return acc
      }, {} as Record<number, string>)
    ),
    [uiExpressionPlaceholders, uiExpressionValuesByIndex, uiExpressionParamModeByIndex, uiExpressionVariables]
  )
  const uiGroupAggregateExpression = useMemo(() => {
    if (!isUiGroupAggregateTemplate) return ''
    const keyField = String(uiGroupAggregateKeyField || '').trim()
    const keyName = String(uiGroupAggregateKeyName || '').trim() || 'group'
    const metrics = (uiGroupAggregateMetrics || [])
      .map((metric) => ({
        outputName: normalizeUiGroupAggregateOutputName(String(metric.outputName || '').trim()),
        path: String(metric.path || '').trim(),
        agg: String(metric.agg || '').trim().toLowerCase(),
      }))
      .filter((metric) => metric.outputName)
    const effectiveMetrics = metrics.length > 0
      ? metrics
      : [{ outputName: 'metric', path: '', agg: 'sum' }]
    const metricFragments = effectiveMetrics.map((metric) => {
      const metricPath = metric.path.replace(/'/g, "\\'")
      const metricAgg = EXPR_ALLOWED_AGG_VALUES.has(metric.agg) ? metric.agg : 'sum'
      return `${metric.outputName}=obj(path='${metricPath}',agg='${metricAgg}')`
    })
    return `=group_aggregate('${keyField.replace(/'/g, "\\'")}', obj(${metricFragments.join(', ')}), '${keyName.replace(/'/g, "\\'")}')`
  }, [isUiGroupAggregateTemplate, uiGroupAggregateKeyField, uiGroupAggregateKeyName, uiGroupAggregateMetrics])
  const uiExpressionPreviewRaw = useMemo(
    () => (
      isUiGroupAggregateTemplate
        ? uiGroupAggregateExpression
        : buildExpressionFromSnippet(selectedUiExpressionTemplateSnippet, uiExpressionResolvedValuesByIndex)
    ),
    [isUiGroupAggregateTemplate, uiGroupAggregateExpression, selectedUiExpressionTemplateSnippet, uiExpressionResolvedValuesByIndex]
  )
  const uiExpressionPreview = useMemo(
    () => normalizeExpressionValue(uiExpressionPreviewRaw),
    [uiExpressionPreviewRaw]
  )
  const uiExpressionJsonPreview = useMemo(
    () => JSON.stringify(
      uiExpressionRows.reduce((acc, row) => {
        const key = String(row.key || '').trim()
        const raw = String(row.expression || '').trim()
        if (!key || !raw) return acc
        acc[key] = parseUiBuilderRowValue(row)
        return acc
      }, {} as Record<string, unknown>),
      null,
      2
    ),
    [uiExpressionRows]
  )
  const uiFieldAutoCompleteOptions = useMemo(
    () => expressionPathOptions.slice(0, 600).map((value) => ({ value })),
    [expressionPathOptions]
  )
  const uiExpressionVariableOptions = useMemo(
    () => (
      (uiExpressionVariables || [])
        .map((item) => String(item.name || '').trim())
        .filter(Boolean)
        .map((value) => ({ value, label: value }))
    ),
    [uiExpressionVariables]
  )
  const customFieldExampleCategoryOptions = useMemo(
    () => {
      const categories = Array.from(
        new Set(CUSTOM_FIELD_EXAMPLE_REPOSITORY.map((item) => item.category))
      ).sort((a, b) => a.localeCompare(b))
      return [
        { value: 'all', label: 'All Categories' },
        ...categories.map((category) => ({ value: category, label: category })),
      ]
    },
    []
  )
  const filteredCustomFieldExamples = useMemo(
    () => {
      const search = exampleRepoSearch.trim().toLowerCase()
      return CUSTOM_FIELD_EXAMPLE_REPOSITORY.filter((item) => {
        if (exampleRepoCategory !== 'all' && item.category !== exampleRepoCategory) return false
        if (!search) return true
        const haystack = [
          item.title,
          item.category,
          item.name,
          item.description,
          item.expression || '',
          item.jsonTemplate || '',
          ...(item.tags || []),
        ]
          .join(' ')
          .toLowerCase()
        return haystack.includes(search)
      })
    },
    [exampleRepoCategory, exampleRepoSearch]
  )
  const configuredPrimaryKeyField = String(
    nodeConfig.custom_primary_key_field || nodeConfig.custom_group_by_field || ''
  ).trim()
  const configuredExpressionEngine = (
    () => {
      const raw = String(nodeConfig.custom_expression_engine || 'auto').trim().toLowerCase()
      if (raw === 'python') return 'python'
      if (raw === 'polars') return 'polars'
      return 'auto'
    }
  )() as CustomExpressionEngine
  const configuredProfileEnabled = Boolean(nodeConfig.custom_profile_enabled ?? false)
  const configuredProfileStorage = (
    () => {
      const storage = String(nodeConfig.custom_profile_storage || 'lmdb').trim().toLowerCase()
      if (storage === 'rocksdb' || storage === 'rocks') return 'rocksdb'
      if (storage === 'redis' || storage === 'redisdb') return 'redis'
      if (storage === 'oracle' || storage === 'oracledb' || storage === 'ora') return 'oracle'
      return 'lmdb'
    }
  )() as CustomProfileStorage
  const configuredProfileProcessingMode = (
    (() => {
      const mode = String(nodeConfig.custom_profile_processing_mode || 'batch').trim().toLowerCase()
      if (mode === 'incremental_batch') return 'incremental_batch'
      if (mode === 'incremental') return 'incremental'
      return 'batch'
    })()
  ) as CustomProfileProcessingMode
  const configuredProfileComputeStrategy = (
    (() => {
      const raw = String(nodeConfig.custom_profile_compute_strategy || 'single').trim().toLowerCase()
      if (
        raw === 'parallel_by_profile_key'
        || raw === 'parallel_profile_key'
        || raw === 'profile_key_parallel'
        || raw === 'parallel_key'
        || raw === 'parallel'
      ) {
        return 'parallel_by_profile_key'
      }
      return 'single'
    })()
  ) as CustomProfileComputeStrategy
  const configuredProfileComputeExecutor = (
    (() => {
      const raw = String(nodeConfig.custom_profile_compute_executor || 'thread').trim().toLowerCase()
      if (
        raw === 'process'
        || raw === 'processes'
        || raw === 'multiprocess'
        || raw === 'multi_process'
        || raw === 'mp'
      ) {
        return 'process'
      }
      return 'thread'
    })()
  ) as CustomProfileComputeExecutor
  const configuredProfileComputeWorkersRaw = Number(nodeConfig.custom_profile_compute_workers ?? 4)
  const configuredProfileComputeWorkers = Number.isFinite(configuredProfileComputeWorkersRaw)
    ? Math.max(2, Math.min(Math.floor(configuredProfileComputeWorkersRaw), 16))
    : 4
  const configuredProfileComputeMinRowsRaw = Number(nodeConfig.custom_profile_compute_min_rows ?? 20000)
  const configuredProfileComputeMinRows = Number.isFinite(configuredProfileComputeMinRowsRaw)
    ? Math.max(1000, Math.min(Math.floor(configuredProfileComputeMinRowsRaw), 5000000))
    : 20000
  const configuredProfileComputeGlobalPrefetchMaxTokensRaw = Number(
    nodeConfig.custom_profile_compute_global_prefetch_max_tokens ?? 40000
  )
  const configuredProfileComputeGlobalPrefetchMaxTokens = Number.isFinite(configuredProfileComputeGlobalPrefetchMaxTokensRaw)
    ? Math.max(1000, Math.min(Math.floor(configuredProfileComputeGlobalPrefetchMaxTokensRaw), 2000000))
    : 40000
  const configuredProfileBackfillCandidatePrefetchChunkSizeRaw = Number(
    nodeConfig.custom_profile_backfill_candidate_prefetch_chunk_size ?? 500
  )
  const configuredProfileBackfillCandidatePrefetchChunkSize = Number.isFinite(configuredProfileBackfillCandidatePrefetchChunkSizeRaw)
    ? Math.max(50, Math.min(Math.floor(configuredProfileBackfillCandidatePrefetchChunkSizeRaw), 900))
    : 500
  const configuredProfileEmitMode = (
    String(nodeConfig.custom_profile_emit_mode || 'changed_only').trim().toLowerCase() === 'all_entities'
      ? 'all_entities'
      : 'changed_only'
  ) as 'changed_only' | 'all_entities'
  const configuredProfileRequiredFields = parseFieldList(nodeConfig.custom_profile_required_fields)
  const configuredProfileEventTimeField = String(nodeConfig.custom_profile_event_time_field || '').trim()
  const configuredProfileWindowDays = String(nodeConfig.custom_profile_window_days || '1,7,30').trim() || '1,7,30'
  const configuredProfileRetentionDays = Number(nodeConfig.custom_profile_retention_days ?? 45)
  const configuredProfileIncludeChangeFields = Boolean(nodeConfig.custom_profile_include_change_fields ?? false)
  const configuredProfileOracleHost = String(nodeConfig.custom_profile_oracle_host || '').trim()
  const configuredProfileOraclePortRaw = Number(nodeConfig.custom_profile_oracle_port ?? 1521)
  const configuredProfileOraclePort = Number.isFinite(configuredProfileOraclePortRaw) && configuredProfileOraclePortRaw > 0
    ? Math.floor(configuredProfileOraclePortRaw)
    : 1521
  const configuredProfileOracleServiceName = String(nodeConfig.custom_profile_oracle_service_name || '').trim()
  const configuredProfileOracleSid = String(nodeConfig.custom_profile_oracle_sid || '').trim()
  const configuredProfileOracleUser = String(nodeConfig.custom_profile_oracle_user || '').trim()
  const configuredProfileOraclePassword = String(nodeConfig.custom_profile_oracle_password || '').trim()
  const configuredProfileOracleDsn = String(nodeConfig.custom_profile_oracle_dsn || '').trim()
  const configuredProfileOracleTable = String(nodeConfig.custom_profile_oracle_table || 'ETL_PROFILE_STATE').trim() || 'ETL_PROFILE_STATE'
  const configuredProfileOracleWriteStrategy = (
    (() => {
      const raw = String(nodeConfig.custom_profile_oracle_write_strategy || 'parallel_key').trim().toLowerCase()
      if (raw === 'parallel' || raw === 'parallel_key' || raw === 'parallel_key_partitioned' || raw === 'key_partitioned') {
        return 'parallel_key'
      }
      return 'single'
    })()
  ) as CustomProfileOracleWriteStrategy
  const configuredProfileOracleParallelWorkersRaw = Number(nodeConfig.custom_profile_oracle_parallel_workers ?? 4)
  const configuredProfileOracleParallelWorkers = Number.isFinite(configuredProfileOracleParallelWorkersRaw)
    ? Math.max(2, Math.min(Math.floor(configuredProfileOracleParallelWorkersRaw), 16))
    : 4
  const configuredProfileOracleParallelMinTokensRaw = Number(nodeConfig.custom_profile_oracle_parallel_min_tokens ?? 2000)
  const configuredProfileOracleParallelMinTokens = Number.isFinite(configuredProfileOracleParallelMinTokensRaw)
    ? Math.max(1, Math.min(Math.floor(configuredProfileOracleParallelMinTokensRaw), 1000000))
    : 2000
  const configuredProfileOracleMergeBatchSizeRaw = Number(nodeConfig.custom_profile_oracle_merge_batch_size ?? 500)
  const configuredProfileOracleMergeBatchSize = Number.isFinite(configuredProfileOracleMergeBatchSizeRaw)
    ? Math.max(50, Math.min(Math.floor(configuredProfileOracleMergeBatchSizeRaw), 2000))
    : 500
  const configuredProfileOracleQueueEnabled = Boolean(nodeConfig.custom_profile_oracle_queue_enabled ?? true)
  const configuredProfileOracleQueueWaitOnForceFlush = Boolean(nodeConfig.custom_profile_oracle_queue_wait_on_force_flush ?? true)
  const configuredProfileLivePersist = Boolean(nodeConfig.custom_profile_live_persist ?? false)
  const hasConfiguredProfileFlushEveryRows = Object.prototype.hasOwnProperty.call(nodeConfig, 'custom_profile_flush_every_rows')
  const hasConfiguredProfileFlushIntervalSeconds = Object.prototype.hasOwnProperty.call(nodeConfig, 'custom_profile_flush_interval_seconds')
  const configuredProfileFlushEveryRows = Number(
    nodeConfig.custom_profile_flush_every_rows
    ?? (
      configuredProfileStorage === 'rocksdb' && configuredProfileProcessingMode === 'incremental'
        ? 50000
        : 20000
    )
  )
  const configuredProfileFlushIntervalSeconds = Number(
    nodeConfig.custom_profile_flush_interval_seconds
    ?? (
      configuredProfileStorage === 'rocksdb' && configuredProfileProcessingMode === 'incremental'
        ? 10
        : 2
    )
  )
  const configuredProfileAppendUniqueDisableMinRows = Number(nodeConfig.custom_profile_append_unique_cache_disable_min_rows ?? 5000)
  const configuredProfileAppendUniqueEntityLimit = Number(nodeConfig.custom_profile_append_unique_cache_entity_limit ?? 10000)
  const configuredProfileAppendUniqueDisableRatio = Number(nodeConfig.custom_profile_append_unique_cache_disable_ratio ?? 0.35)
  const configuredValidationLmdbEnvPath = String(
    nodeConfig.custom_validation_rocksdb_env_path
    || nodeConfig.custom_validation_lmdb_env_path
    || nodeConfig.env_path
    || ''
  ).trim()
  const configuredValidationLmdbDbName = String(nodeConfig.custom_validation_lmdb_db_name || '').trim()
  const configuredValidationLmdbKeyPrefix = String(nodeConfig.custom_validation_lmdb_key_prefix || '').trim()
  const configuredValidationLmdbStartKey = String(nodeConfig.custom_validation_lmdb_start_key || '').trim()
  const configuredValidationLmdbEndKey = String(nodeConfig.custom_validation_lmdb_end_key || '').trim()
  const configuredValidationLmdbKeyContains = String(nodeConfig.custom_validation_lmdb_key_contains || '').trim()
  const configuredValidationLmdbLimit = Number(nodeConfig.custom_validation_lmdb_limit ?? 50)
  const activePipelineId = String(pipeline?.id || '').trim()
  const profileMonitorPreferredKeyField = String(
    customFieldStudioOpen
      ? customPrimaryKeyFieldDraft
      : (nodeConfig.custom_primary_key_field || nodeConfig.custom_group_by_field || '')
  ).trim()
  const activeProfileNodeSummary = useMemo(() => {
    if (!profileMonitorData || !Array.isArray(profileMonitorData.nodes) || profileMonitorData.nodes.length === 0) return null
    const nodesList = profileMonitorData.nodes
    const selectedNode = selectedNodeId
      ? nodesList.find((item) => String(item.node_id) === String(selectedNodeId))
      : null
    const hasPreferredKeyInNode = (nodeSummary: any): boolean => {
      if (!nodeSummary || !profileMonitorPreferredKeyField) return false
      const docs = Array.isArray(nodeSummary.sample_documents) ? nodeSummary.sample_documents : []
      return docs.some((doc: any) => {
        const profile = doc?.profile
        if (!profile || typeof profile !== 'object' || Array.isArray(profile)) return false
        const hit = readPathValue(profile as Record<string, unknown>, profileMonitorPreferredKeyField)
        return hit.found && hasMeaningfulValue(hit.value)
      })
    }

    if (selectedNode && (!profileMonitorPreferredKeyField || hasPreferredKeyInNode(selectedNode))) {
      return selectedNode
    }
    if (profileMonitorPreferredKeyField) {
      const preferredNode = nodesList.find((item) => hasPreferredKeyInNode(item))
      if (preferredNode) return preferredNode
    }
    return selectedNode || nodesList[0] || null
  }, [profileMonitorData, selectedNodeId, profileMonitorPreferredKeyField])
  const activeProfileDocuments = useMemo(
    () => (activeProfileNodeSummary?.sample_documents || []),
    [activeProfileNodeSummary]
  )
  const selectedProfileDocument = useMemo(() => {
    if (!activeProfileDocuments.length) return null
    return (
      activeProfileDocuments.find((doc) => String(doc.entity_key) === String(selectedProfileEntityKey))
      || activeProfileDocuments[0]
      || null
    )
  }, [activeProfileDocuments, selectedProfileEntityKey])
  const expandedEditorField = useMemo(
    () => customFieldDraft.find((item) => item.id === expandedEditorFieldId) || null,
    [customFieldDraft, expandedEditorFieldId]
  )
  const uiExpressionJsonObjectFromEditor = useMemo(
    () => parseJsonTemplateObject(String(expandedEditorField?.jsonTemplate || '')),
    [expandedEditorField?.jsonTemplate]
  )
  const uiExpressionJsonKeyOptions = useMemo(
    () => uniqueFieldNames([
      ...Object.keys(uiExpressionJsonObjectFromEditor || {}),
      ...uiExpressionRows.map((row) => String(row.key || '').trim()).filter(Boolean),
      String(uiExpressionJsonKey || '').trim(),
      String(uiBreJsonKey || '').trim(),
      String(expandedEditorField?.name || '').trim(),
      'value',
    ]).map((value) => ({ value, label: value })),
    [uiExpressionJsonObjectFromEditor, uiExpressionRows, uiExpressionJsonKey, uiBreJsonKey, expandedEditorField?.name]
  )
  const uiBreTargetExpression = useMemo(
    () => {
      if (expandedEditorMode === 'json') {
        const key = String(uiBreJsonKey || '').trim()
        if (key && uiExpressionJsonObjectFromEditor[key] !== undefined) {
          return String(uiExpressionJsonObjectFromEditor[key] || '')
        }
        const firstEntry = Object.entries(uiExpressionJsonObjectFromEditor || {})[0]
        return String(firstEntry?.[1] || '')
      }
      return String(expandedEditorField?.expression || '')
    },
    [expandedEditorMode, uiBreJsonKey, uiExpressionJsonObjectFromEditor, expandedEditorField?.expression]
  )
  const uiBreRenderedExpression = useMemo(
    () => normalizeExpressionValue(buildExpressionFromBreModel(uiBreModel)),
    [uiBreModel]
  )
  useEffect(() => {
    if (!uiExpressionPlaceholders.length) return
    setUiExpressionValuesByIndex((prev) => {
      const next = { ...prev }
      uiExpressionPlaceholders.forEach((placeholder) => {
        if (next[placeholder.index] === undefined) {
          next[placeholder.index] = placeholder.defaultValue
        }
      })
      return next
    })
    setUiExpressionParamModeByIndex((prev) => {
      const next = { ...prev }
      uiExpressionPlaceholders.forEach((placeholder) => {
        if (!next[placeholder.index]) {
          next[placeholder.index] = defaultUiExpressionParameterMode(placeholder)
        }
      })
      return next
    })
  }, [uiExpressionPlaceholders])
  useEffect(() => {
    if (!isUiGroupAggregateTemplate) return
    setUiGroupAggregateKeyField((prev) => (String(prev || '').trim() ? prev : (expressionPathOptions[0] || '')))
    setUiGroupAggregateKeyName((prev) => (String(prev || '').trim() ? prev : 'group'))
    setUiGroupAggregateMetrics((prev) => (
      Array.isArray(prev) && prev.length > 0
        ? prev
        : [createUiGroupAggregateMetric({ outputName: 'metric', path: expressionPathOptions[0] || '', agg: 'sum' })]
    ))
  }, [isUiGroupAggregateTemplate, expressionPathOptions])

  useEffect(() => {
    if (!expandedEditorField) return
    if (!String(uiExpressionJsonKey || '').trim()) {
      setUiExpressionJsonKey(expandedEditorField.name || 'value')
    }
  }, [expandedEditorField, uiExpressionJsonKey])
  useEffect(() => {
    if (!expandedEditorField || expandedEditorMode !== 'json') return
    if (!String(uiBreJsonKey || '').trim()) {
      const firstKey = Object.keys(uiExpressionJsonObjectFromEditor || {})[0] || expandedEditorField.name || 'value'
      setUiBreJsonKey(String(firstKey || 'value'))
    }
  }, [expandedEditorField, expandedEditorMode, uiBreJsonKey, uiExpressionJsonObjectFromEditor])

  useEffect(() => {
    if (!expandedEditorField || expandedEditorMode !== 'json') return
    const nextRows = parseJsonTemplateRows(String(expandedEditorField.jsonTemplate || ''))
    setUiExpressionRows(nextRows)
  }, [expandedEditorField?.id, expandedEditorField?.jsonTemplate, expandedEditorMode])

  useEffect(() => {
    if (!expandedEditorField) return
    const normalizedTargetExpression = normalizeExpressionValue(String(uiBreTargetExpression || '')).trim()
    if (!normalizedTargetExpression) {
      setUiBreModel(parseBreModelFromExpression("=if_(true, 'YES', 'NO')"))
      return
    }
    if (
      uiBreLastAppliedExpressionRef.current
      && normalizedTargetExpression === uiBreLastAppliedExpressionRef.current
    ) {
      uiBreLastAppliedExpressionRef.current = ''
      return
    }
    const parsed = parseBreModelFromExpression(normalizedTargetExpression)
    setUiBreModel(parsed)
  }, [expandedEditorField?.id, expandedEditorMode, uiBreJsonKey, uiBreTargetExpression])
  useEffect(() => {
    if (!expandedEditorField) return
    if (expandedEditorTab !== 'ui_expression') return
    if (!uiExpressionBuilderTouched) return
    const expression = normalizeExpressionValue(uiExpressionPreviewRaw)
    if (!expression) return
    if (expandedEditorMode === 'json') {
      const key = String(uiExpressionJsonKey || '').trim() || 'value'
      const currentObject = parseJsonTemplateObject(String(expandedEditorField.jsonTemplate || ''))
      if (String(currentObject[key] || '').trim() === expression.trim()) return
      currentObject[key] = expression
      updateCustomFieldDraft(expandedEditorField.id, { jsonTemplate: JSON.stringify(currentObject, null, 2) })
      return
    }
    if (String(expandedEditorField.expression || '').trim() === expression.trim()) return
    updateCustomFieldDraft(expandedEditorField.id, { expression })
  }, [
    expandedEditorField,
    expandedEditorTab,
    uiExpressionBuilderTouched,
    uiExpressionPreviewRaw,
    expandedEditorMode,
    uiExpressionJsonKey,
  ])

  const oracleUpstreamFieldOptions = useMemo(
    () => (
      nodeType === 'oracle_destination' && selectedNodeId
        ? inferUpstreamInputFields(selectedNodeId, nodes, edges)
        : []
    ),
    [nodeType, selectedNodeId, nodes, edges]
  )
  const oracleFieldOptions = uniqueFieldNames([
    ...oracleUpstreamFieldOptions,
    ...configuredOracleMappings.flatMap((m) => [m.source, m.destination]).filter(Boolean),
  ])
  const joinSources = nodeType === 'join_transform' && selectedNodeId
    ? inferUpstreamSources(selectedNodeId, nodes, edges)
    : []
  const joinSourceOptions = joinSources.map((src) => ({
    value: src.id,
    label: src.label,
  }))
  const joinLeftSource = String(
    nodeConfig.left_source_node
    || joinSources[0]?.id
    || ''
  )
  const joinRightSource = String(
    nodeConfig.right_source_node
    || joinSources.find((src) => src.id !== joinLeftSource)?.id
    || ''
  )
  const joinLeftFieldOptions = uniqueFieldNames([
    ...parseFieldList(nodeConfig.left_key),
    ...(joinSources.find((src) => src.id === joinLeftSource)?.fields || []),
  ])
  const joinRightFieldOptions = uniqueFieldNames([
    ...parseFieldList(nodeConfig.right_key),
    ...(joinSources.find((src) => src.id === joinRightSource)?.fields || []),
  ])
  const jsonPathOptions = (Array.isArray(nodeConfig._json_paths) ? nodeConfig._json_paths : [])
    .map((item) => {
      if (typeof item === 'string') return { value: item, label: item }
      if (item && typeof item === 'object') {
        const value = String((item as any).value ?? '')
        const label = String((item as any).label ?? value)
        if (!value) return null
        return { value, label }
      }
      return null
    })
    .filter((opt): opt is { value: string; label: string } => !!opt)

  useEffect(() => {
    if (!customFieldStudioOpen || nodeType !== 'map_transform' || !customProfileEnabledDraft) return
    if (!selectedNodeId || !activePipelineId) return
    void refreshProfileMonitor()
  }, [customFieldStudioOpen, customProfileEnabledDraft, nodeType, selectedNodeId, activePipelineId])

  useEffect(() => {
    if (!activeProfileDocuments.length) {
      if (selectedProfileEntityKey) setSelectedProfileEntityKey('')
      return
    }
    const hasSelected = activeProfileDocuments.some(
      (doc) => String(doc.entity_key) === String(selectedProfileEntityKey)
    )
    if (!hasSelected) {
      setSelectedProfileEntityKey(String(activeProfileDocuments[0]?.entity_key || ''))
    }
  }, [activeProfileDocuments, selectedProfileEntityKey])

  useEffect(() => {
    // When primary key changes, force re-selection so monitor follows new key samples.
    setSelectedProfileEntityKey('')
  }, [customPrimaryKeyFieldDraft])

  useEffect(() => {
    if (!lmdbStudioOpen || !isLmdbSource || lmdbPreviewView !== 'summary' || !isExecuting) {
      return
    }
    const envPath = String(lmdbStudioDraft.env_path || '').trim()
    if (!envPath) return

    let cancelled = false
    let timer: number | null = null

    const tick = async () => {
      if (cancelled) return
      const globalFilterValues = uniqueFieldNames(
        (lmdbGlobalFilterValues || []).map((value) => String(value || '').trim()).filter(Boolean),
      )
      const columnFilters: Record<string, string[]> = {}
      Object.entries(lmdbColumnFilterValues || {}).forEach(([name, values]) => {
        const col = String(name || '').trim()
        if (!col || !Array.isArray(values)) return
        const normalized = uniqueFieldNames(values.map((v) => String(v || '').trim()).filter(Boolean))
        if (normalized.length > 0) columnFilters[col] = normalized
      })
      const requestConfig: Record<string, unknown> = {
        ...lmdbDraftToConfig(lmdbStudioDraft),
        global_filter_column: String(lmdbGlobalFilterColumn || LMDB_GLOBAL_ALL_COLUMNS),
        global_filter_values: globalFilterValues,
      }
      if (Object.keys(columnFilters).length > 0) {
        requestConfig.column_filters = columnFilters
      }
      await fetchLmdbSummary(requestConfig)
      if (cancelled) return
      timer = window.setTimeout(tick, 1200)
    }

    void tick()
    return () => {
      cancelled = true
      if (timer !== null) window.clearTimeout(timer)
    }
  }, [
    isExecuting,
    lmdbStudioOpen,
    lmdbPreviewView,
    lmdbStudioDraft,
    lmdbGlobalFilterColumn,
    lmdbGlobalFilterValues,
    lmdbColumnFilterValues,
    nodeType,
  ])

  if (!data || !definition) return null

  const handleFieldChange = (name: string, value: unknown) => {
    updateNodeConfig(selectedNodeId!, { [name]: value })
  }

  const handleLabelChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    updateNodeLabel(selectedNodeId!, e.target.value)
  }

  const handleDelete = () => {
    removeNode(selectedNodeId!)
    onClose()
  }

  const handleDuplicate = () => {
    if (!selectedNodeId) return
    duplicateNode(selectedNodeId)
  }

  const detectJsonPathOptions = async () => {
    if (!selectedNodeId) return
    setJsonPathDetectError(null)
    setJsonPathDetectLoading(true)
    try {
      const response = await api.detectSourceJsonFieldOptions(nodeType, nodeConfig)
      const detectedPaths = Array.isArray(response?.json_paths) ? response.json_paths : []
      const detectedColumns = Array.isArray(response?.columns) ? response.columns : []
      const suggestedPath = typeof response?.suggested_json_path === 'string' ? response.suggested_json_path : ''

      const patch: Record<string, unknown> = {
        _json_paths: detectedPaths,
      }
      if (typeof response?.row_count === 'number') patch._row_count = response.row_count
      if (detectedColumns.length > 0) patch._detected_columns = detectedColumns.join(', ')
      if (Array.isArray(response?.preview) && response.preview.length > 0) patch._preview_rows = response.preview
      updateNodeConfigSilent(selectedNodeId, patch)
      if (suggestedPath && !String(nodeConfig.json_path || '').trim()) {
        updateNodeConfig(selectedNodeId, { json_path: suggestedPath })
      }
      if (detectedPaths.length > 0) {
        notification.success({
          message: 'JSON fields detected',
          description: `${detectedPaths.length} selectable paths found from API response.`,
          placement: 'bottomRight',
          duration: 2,
        })
      } else {
        notification.warning({
          message: 'No JSON paths detected',
          description: 'Check API URL/headers/params/body or run with sample response.',
          placement: 'bottomRight',
          duration: 2.5,
        })
      }
    } catch (err: any) {
      const msg = String(err?.message || 'Failed to detect JSON fields from API source')
      setJsonPathDetectError(msg)
      notification.error({ message: 'Detection failed', description: msg, placement: 'bottomRight' })
    } finally {
      setJsonPathDetectLoading(false)
    }
  }

  const detectSourceFieldOptions = async () => {
    if (!selectedNodeId) return
    setSourceFieldDetectError(null)
    setSourceFieldDetectLoading(true)
    try {
      const detectMaxRows = isLmdbSource ? 5000 : 300
      const response = await api.detectSourceFieldOptions(nodeType, nodeConfig, detectMaxRows)
      const detectedColumns = Array.isArray(response?.columns) ? response.columns : []
      const patch: Record<string, unknown> = {}
      if (detectedColumns.length > 0) {
        patch._detected_columns = detectedColumns.join(', ')
      }
      if (typeof response?.row_count === 'number') {
        patch._row_count = response.row_count
      }
      if (Array.isArray(response?.preview) && response.preview.length > 0) {
        patch._preview_rows = response.preview
      }
      if (Object.keys(patch).length > 0) {
        updateNodeConfigSilent(selectedNodeId, patch)
      }
      if (detectedColumns.length > 0) {
        notification.success({
          message: 'Source fields detected',
          description: `${detectedColumns.length} fields detected from source metadata.`,
          placement: 'bottomRight',
          duration: 2,
        })
      } else {
        notification.warning({
          message: 'No fields detected',
          description: 'Query returned no rows/columns. Check source config and SQL query.',
          placement: 'bottomRight',
          duration: 2.5,
        })
      }
    } catch (err: any) {
      const msg = String(err?.message || 'Failed to detect source fields')
      setSourceFieldDetectError(msg)
      notification.error({ message: 'Detection failed', description: msg, placement: 'bottomRight' })
    } finally {
      setSourceFieldDetectLoading(false)
    }
  }

  const refreshProfileMonitor = async () => {
    if (!activePipelineId || !selectedNodeId) {
      setProfileMonitorData(null)
      setProfileMonitorError('Save pipeline first to enable profile monitoring.')
      return
    }
    const preferredPrimaryKeyField = String(
      customFieldStudioOpen
        ? customPrimaryKeyFieldDraft
        : (nodeConfig.custom_primary_key_field || nodeConfig.custom_group_by_field || '')
    ).trim()
    const monitorNodeId = preferredPrimaryKeyField ? undefined : selectedNodeId
    const monitorLimit = preferredPrimaryKeyField ? 24 : 12
    setProfileMonitorLoading(true)
    setProfileMonitorError(null)
    try {
      const response = await api.getPipelineProfileState(
        activePipelineId,
        monitorNodeId,
        monitorLimit,
        preferredPrimaryKeyField || undefined,
      )
      setProfileMonitorData(response as ProfileMonitorResponse)
      setProfileMonitorError(null)
    } catch (err: any) {
      const msg = String(err?.message || 'Failed to load profile state')
      setProfileMonitorError(msg)
      notification.error({
        message: 'Profile monitor failed',
        description: msg,
        placement: 'bottomRight',
      })
    } finally {
      setProfileMonitorLoading(false)
    }
  }

  const clearProfileMonitor = async () => {
    if (!activePipelineId || !selectedNodeId) {
      notification.warning({
        message: 'Pipeline not ready',
        description: 'Save pipeline first, then clear profile data.',
        placement: 'bottomRight',
      })
      return
    }
    setProfileMonitorClearing(true)
    try {
      const result = await api.clearPipelineProfileState(activePipelineId, selectedNodeId)
      const removedEntities = Number((result as any)?.removed_entities || 0)
      const removedNodes = Number((result as any)?.removed_nodes || 0)
      notification.success({
        message: 'Profile data cleared',
        description: `Removed ${removedEntities} profile document${removedEntities === 1 ? '' : 's'} from ${removedNodes} node${removedNodes === 1 ? '' : 's'}.`,
        placement: 'bottomRight',
      })
      await refreshProfileMonitor()
    } catch (err: any) {
      const msg = String(err?.message || 'Failed to clear profile state')
      notification.error({
        message: 'Clear profile failed',
        description: msg,
        placement: 'bottomRight',
      })
    } finally {
      setProfileMonitorClearing(false)
    }
  }

  const openProfileDataViewer = () => {
    if (!activeProfileDocuments.length) {
      notification.info({
        message: 'No profile data available',
        description: 'Run pipeline and refresh Profile Monitoring.',
        placement: 'bottomRight',
      })
      return
    }
    const defaultKey = String(activeProfileDocuments[0]?.entity_key || '')
    setSelectedProfileEntityKey((prev) => (prev ? prev : defaultKey))
    setProfileDataModalOpen(true)
  }

  const openExpandedEditor = (fieldId: string, mode: CustomFieldMode) => {
    setExpandedEditorFieldId(fieldId)
    setExpandedEditorMode(mode)
    setExpandedEditorTab('expression')
    setUiExpressionTemplateLabel(EXPRESSION_FUNCTION_SNIPPETS[0]?.label || '')
    setUiExpressionValuesByIndex({})
    setUiExpressionParamModeByIndex({})
    setUiExpressionVariables([])
    setUiExpressionBuilderTouched(false)
    setUiGroupAggregateKeyField('')
    setUiGroupAggregateKeyName('group')
    setUiGroupAggregateMetrics([createUiGroupAggregateMetric({ outputName: 'metric', path: '', agg: 'sum' })])
    setUiExpressionJsonKey('value')
    setUiExpressionNewJsonKeyDraft('')
    setUiConditionBuilderByIndex({})
    setUiConditionUndoByIndex({})
    setUiConditionRedoByIndex({})
    setUiConditionBuilderModalIndex(null)
    setUiFieldBuilderByIndex({})
    setUiFieldBuilderModalIndex(null)
    setUiBreJsonKey('value')
    setUiBreModel(parseBreModelFromExpression("=if_(true, 'YES', 'NO')"))
    setUiExpressionRows([])
  }

  const syncBreModelToEditor = (nextModel: BreModel) => {
    if (!expandedEditorField) return
    const expression = normalizeExpressionValue(buildExpressionFromBreModel(nextModel))
    if (!String(expression || '').trim()) return
    uiBreLastAppliedExpressionRef.current = expression.trim()
    if (expandedEditorMode === 'json') {
      const key = String(uiBreJsonKey || '').trim() || 'value'
      const currentObject = parseJsonTemplateObject(String(expandedEditorField.jsonTemplate || ''))
      if (String(currentObject[key] || '').trim() === expression.trim()) return
      currentObject[key] = expression
      updateCustomFieldDraft(expandedEditorField.id, { jsonTemplate: JSON.stringify(currentObject, null, 2) })
      return
    }
    if (String(expandedEditorField.expression || '').trim() === expression.trim()) return
    updateCustomFieldDraft(expandedEditorField.id, { expression })
  }

  const updateUiExpressionPlaceholderValue = (index: number, value: string) => {
    const safeIndex = Number(index)
    if (!Number.isFinite(safeIndex)) return
    setUiExpressionBuilderTouched(true)
    setUiExpressionValuesByIndex((prev) => ({
      ...prev,
      [safeIndex]: String(value || ''),
    }))
  }

  const updateUiExpressionPlaceholderMode = (index: number, mode: UiExpressionParameterMode) => {
    const safeIndex = Number(index)
    if (!Number.isFinite(safeIndex)) return
    setUiExpressionBuilderTouched(true)
    setUiExpressionParamModeByIndex((prev) => ({
      ...prev,
      [safeIndex]: mode,
    }))
  }

  const splitUiConditionSegmentsByLogical = (sourceRaw: string): Array<{ text: string; joinWithPrev: UiConditionLogicalJoin }> => {
    const source = String(sourceRaw || '')
    const segments: Array<{ text: string; joinWithPrev: UiConditionLogicalJoin }> = []
    let start = 0
    let quote: '"' | "'" | null = null
    let escaped = false
    let depthRound = 0
    let depthSquare = 0
    let depthCurly = 0
    let pendingJoin: UiConditionLogicalJoin = 'and'

    const isBoundary = (ch: string): boolean => (!ch || /[\s()[\]{}:,]/.test(ch))

    for (let i = 0; i < source.length; i += 1) {
      const ch = source[i]
      if (quote) {
        if (escaped) {
          escaped = false
          continue
        }
        if (ch === '\\') {
          escaped = true
          continue
        }
        if (ch === quote) {
          quote = null
        }
        continue
      }
      if (ch === '"' || ch === "'") {
        quote = ch
        continue
      }
      if (ch === '(') depthRound += 1
      else if (ch === ')') depthRound = Math.max(0, depthRound - 1)
      else if (ch === '[') depthSquare += 1
      else if (ch === ']') depthSquare = Math.max(0, depthSquare - 1)
      else if (ch === '{') depthCurly += 1
      else if (ch === '}') depthCurly = Math.max(0, depthCurly - 1)

      if (depthRound || depthSquare || depthCurly) continue

      const remaining = source.slice(i).toLowerCase()
      const matchedJoin = remaining.startsWith('and')
        ? 'and'
        : remaining.startsWith('or')
          ? 'or'
          : null
      if (!matchedJoin) continue

      const prevChar = source[i - 1] || ''
      const nextChar = source[i + matchedJoin.length] || ''
      if (!isBoundary(prevChar) || !isBoundary(nextChar)) continue

      const segmentText = source.slice(start, i).trim()
      if (segmentText) {
        segments.push({ text: segmentText, joinWithPrev: pendingJoin })
      }
      pendingJoin = matchedJoin
      start = i + matchedJoin.length
      i = start - 1
    }

    const tail = source.slice(start).trim()
    if (tail) {
      segments.push({ text: tail, joinWithPrev: pendingJoin })
    }

    return segments.length > 0 ? segments : [{ text: source.trim(), joinWithPrev: 'and' }]
  }

  const unwrapUiConditionOuterParens = (valueRaw: string): string => {
    let value = String(valueRaw || '').trim()
    if (!value) return value

    const isWrappedOnce = (text: string): boolean => {
      if (!text.startsWith('(') || !text.endsWith(')')) return false
      let quote: '"' | "'" | null = null
      let escaped = false
      let depth = 0
      for (let i = 0; i < text.length; i += 1) {
        const ch = text[i]
        if (quote) {
          if (escaped) {
            escaped = false
            continue
          }
          if (ch === '\\') {
            escaped = true
            continue
          }
          if (ch === quote) quote = null
          continue
        }
        if (ch === '"' || ch === "'") {
          quote = ch
          continue
        }
        if (ch === '(') depth += 1
        else if (ch === ')') {
          depth -= 1
          if (depth === 0 && i < text.length - 1) return false
        }
      }
      return depth === 0
    }

    while (isWrappedOnce(value)) {
      value = value.slice(1, -1).trim()
    }
    return value
  }

  const parseUiConditionRightOperand = (tokenRaw: string): { rightMode: UiConditionClauseRightMode; rightValue: string } => {
    const token = String(tokenRaw || '').trim()
    if (!token) return { rightMode: 'literal', rightValue: '' }

    const fieldMatch = token.match(/^field\s*\(\s*(['"])(.*?)\1\s*\)$/i)
    if (fieldMatch) return { rightMode: 'field', rightValue: String(fieldMatch[2] || '') }

    const valuesMatch = token.match(/^values\s*\(\s*(['"])(.*?)\1\s*\)$/i)
    if (valuesMatch) return { rightMode: 'values', rightValue: String(valuesMatch[2] || '') }

    const variableMatch = (uiExpressionVariables || []).find((item) => (
      String(item.value || '').trim() && String(item.value || '').trim() === token
    ))
    if (variableMatch) return { rightMode: 'variable', rightValue: String(variableMatch.name || '') }

    const singleQuoted = token.match(/^'(.*)'$/)
    if (singleQuoted) {
      return { rightMode: 'literal', rightValue: String(singleQuoted[1] || '').replace(/\\'/g, "'") }
    }
    const doubleQuoted = token.match(/^"(.*)"$/)
    if (doubleQuoted) {
      return { rightMode: 'literal', rightValue: String(doubleQuoted[1] || '').replace(/\\"/g, '"') }
    }

    if (/^-?\d+(\.\d+)?$/.test(token) || /^(true|false|null)$/i.test(token)) {
      return { rightMode: 'literal', rightValue: token }
    }

    return { rightMode: 'raw', rightValue: token }
  }

  const parseUiConditionClauseFromText = (
    sourceRaw: string,
    clusterId: string,
    joinWithPrev: UiConditionLogicalJoin,
  ): UiConditionClause => {
    const source = unwrapUiConditionOuterParens(String(sourceRaw || '').trim())
    const fallback = createUiConditionClause(clusterId, {
      joinWithPrev,
      operator: 'raw',
      rightMode: 'raw',
      rightValue: source || "field('') == ''",
    })
    if (!source) return fallback

    const fieldPrefix = source.match(/^field\s*\(/i)
    if (!fieldPrefix) return fallback

    const openParen = source.indexOf('(', Number(fieldPrefix.index || 0))
    if (openParen < 0) return fallback
    const closeParen = findMatchingParenOffset(source, openParen)
    if (closeParen <= openParen) return fallback

    const leftCall = source.slice(0, closeParen + 1).trim()
    const leftMatch = leftCall.match(/^field\s*\(\s*(['"])(.*?)\1\s*\)$/i)
    if (!leftMatch) return fallback

    const leftField = String(leftMatch[2] || '')
    const remainder = source.slice(closeParen + 1).trim()
    if (!remainder) return createUiConditionClause(clusterId, {
      joinWithPrev,
      leftField,
      operator: '==',
      rightMode: 'literal',
      rightValue: '',
    })

    const conditionOperators = EXPR_ALLOWED_CONDITION_OPERATORS_OPTIONS
      .map((op) => String(op || '').trim())
      .filter(Boolean)
      .sort((a, b) => b.length - a.length)

    let matchedOperator = ''
    let rightRaw = ''
    for (const operator of conditionOperators) {
      const lowerRemainder = remainder.toLowerCase()
      const lowerOp = operator.toLowerCase()
      if (!lowerRemainder.startsWith(lowerOp)) continue
      const nextChar = remainder[operator.length] || ''
      const requiresBoundary = /^[a-z_]/i.test(operator)
      if (requiresBoundary && nextChar && !/[\s()[\]{}:,]/.test(nextChar)) continue
      matchedOperator = operator
      rightRaw = remainder.slice(operator.length).trim()
      break
    }

    if (!matchedOperator) return fallback

    const parsedRight = parseUiConditionRightOperand(rightRaw)
    return createUiConditionClause(clusterId, {
      joinWithPrev,
      leftField,
      operator: matchedOperator,
      rightMode: parsedRight.rightMode,
      rightValue: parsedRight.rightValue,
    })
  }

  const parseUiConditionBuilderFromRaw = (rawCondition: string): UiConditionBuilderState => {
    const source = String(rawCondition || '').trim().replace(/^=\s*/, '').trim()
    if (!source) return createDefaultUiConditionBuilderState()

    const topLevelSegments = splitUiConditionSegmentsByLogical(source)
    const clusters: UiConditionCluster[] = []
    const clauses: UiConditionClause[] = []

    topLevelSegments.forEach((topSegment, topIndex) => {
      const clusterBody = unwrapUiConditionOuterParens(String(topSegment.text || '').trim())
      if (!clusterBody) return
      const cluster = createUiConditionCluster({ joinWithPrev: topIndex === 0 ? 'and' : topSegment.joinWithPrev })
      clusters.push(cluster)
      const clauseSegments = splitUiConditionSegmentsByLogical(clusterBody)
      clauseSegments.forEach((clauseSegment, clauseIndex) => {
        const clause = parseUiConditionClauseFromText(
          clauseSegment.text,
          cluster.id,
          clauseIndex === 0 ? 'and' : clauseSegment.joinWithPrev,
        )
        clauses.push(clause)
      })
    })

    if (clusters.length === 0 || clauses.length === 0) {
      return createDefaultUiConditionBuilderState({
        clusters: [createUiConditionCluster({ joinWithPrev: 'and' })],
        clauses: [
          parseUiConditionClauseFromText(source, 'cluster_1', 'and'),
        ],
      })
    }

    return createDefaultUiConditionBuilderState({ clusters, clauses })
  }

  const openUiConditionBuilderModal = (index: number, rawCondition: string) => {
    const safeIndex = Number(index)
    if (!Number.isFinite(safeIndex)) return
    const parsed = parseUiConditionBuilderFromRaw(rawCondition)
    setUiConditionBuilderByIndex((prev) => ({
      ...prev,
      [safeIndex]: parsed,
    }))
    setUiConditionUndoByIndex((prev) => ({
      ...prev,
      [safeIndex]: [],
    }))
    setUiConditionRedoByIndex((prev) => ({
      ...prev,
      [safeIndex]: [],
    }))
    setUiConditionBuilderModalIndex(safeIndex)
  }

  const updateUiConditionBuilder = (
    index: number,
    updater: (state: UiConditionBuilderState) => UiConditionBuilderState,
  ) => {
    const safeIndex = Number(index)
    if (!Number.isFinite(safeIndex)) return
    const current = createDefaultUiConditionBuilderState(
      uiConditionBuilderByIndexRef.current[safeIndex] || createDefaultUiConditionBuilderState()
    )
    const next = createDefaultUiConditionBuilderState(updater(current))
    const currentSignature = JSON.stringify(current)
    const nextSignature = JSON.stringify(next)
    if (currentSignature === nextSignature) return
    setUiExpressionBuilderTouched(true)
    setUiConditionBuilderByIndex((prev) => ({
      ...prev,
      [safeIndex]: next,
    }))
    const undoStack = [...(uiConditionUndoByIndexRef.current[safeIndex] || []), current].slice(-60)
    setUiConditionUndoByIndex((prev) => ({
      ...prev,
      [safeIndex]: undoStack,
    }))
    setUiConditionRedoByIndex((prev) => ({
      ...prev,
      [safeIndex]: [],
    }))
  }

  const undoUiConditionBuilder = (index: number) => {
    const safeIndex = Number(index)
    if (!Number.isFinite(safeIndex)) return
    const undoStack = [...(uiConditionUndoByIndexRef.current[safeIndex] || [])]
    if (undoStack.length === 0) return
    const current = createDefaultUiConditionBuilderState(
      uiConditionBuilderByIndexRef.current[safeIndex] || createDefaultUiConditionBuilderState()
    )
    const previous = createDefaultUiConditionBuilderState(undoStack.pop() || createDefaultUiConditionBuilderState())
    setUiExpressionBuilderTouched(true)
    setUiConditionBuilderByIndex((prev) => ({
      ...prev,
      [safeIndex]: previous,
    }))
    setUiConditionUndoByIndex((prev) => ({
      ...prev,
      [safeIndex]: undoStack,
    }))
    const redoStack = [...(uiConditionRedoByIndexRef.current[safeIndex] || []), current].slice(-60)
    setUiConditionRedoByIndex((prev) => ({
      ...prev,
      [safeIndex]: redoStack,
    }))
  }

  const redoUiConditionBuilder = (index: number) => {
    const safeIndex = Number(index)
    if (!Number.isFinite(safeIndex)) return
    const redoStack = [...(uiConditionRedoByIndexRef.current[safeIndex] || [])]
    if (redoStack.length === 0) return
    const current = createDefaultUiConditionBuilderState(
      uiConditionBuilderByIndexRef.current[safeIndex] || createDefaultUiConditionBuilderState()
    )
    const next = createDefaultUiConditionBuilderState(redoStack.pop() || createDefaultUiConditionBuilderState())
    setUiExpressionBuilderTouched(true)
    setUiConditionBuilderByIndex((prev) => ({
      ...prev,
      [safeIndex]: next,
    }))
    const undoStack = [...(uiConditionUndoByIndexRef.current[safeIndex] || []), current].slice(-60)
    setUiConditionUndoByIndex((prev) => ({
      ...prev,
      [safeIndex]: undoStack,
    }))
    setUiConditionRedoByIndex((prev) => ({
      ...prev,
      [safeIndex]: redoStack,
    }))
  }

  const openUiFieldBuilderModal = (index: number, mode: UiExpressionParameterMode, value: string) => {
    const safeIndex = Number(index)
    if (!Number.isFinite(safeIndex)) return
    const nextState = parseUiFieldBuilderFromCurrent(mode, value)
    setUiFieldBuilderByIndex((prev) => ({
      ...prev,
      [safeIndex]: nextState,
    }))
    setUiFieldBuilderModalIndex(safeIndex)
  }

  const updateUiFieldBuilder = (index: number, patch: Partial<UiFieldBuilderState>) => {
    const safeIndex = Number(index)
    if (!Number.isFinite(safeIndex)) return
    setUiFieldBuilderByIndex((prev) => ({
      ...prev,
      [safeIndex]: createDefaultUiFieldBuilderState({
        ...(prev[safeIndex] || createDefaultUiFieldBuilderState()),
        ...patch,
      }),
    }))
  }

  const applyUiFieldBuilder = (index: number) => {
    const safeIndex = Number(index)
    if (!Number.isFinite(safeIndex)) return
    const state = createDefaultUiFieldBuilderState(uiFieldBuilderByIndex[safeIndex] || createDefaultUiFieldBuilderState())
    const expression = buildUiFieldBuilderExpression(state)
    if (state.template === 'field') {
      updateUiExpressionPlaceholderMode(safeIndex, 'field')
      updateUiExpressionPlaceholderValue(safeIndex, state.fieldPath)
    } else if (state.template === 'values') {
      updateUiExpressionPlaceholderMode(safeIndex, 'values')
      updateUiExpressionPlaceholderValue(safeIndex, state.fieldPath)
    } else {
      updateUiExpressionPlaceholderMode(safeIndex, 'raw')
      updateUiExpressionPlaceholderValue(safeIndex, expression)
    }
    setUiFieldBuilderModalIndex(null)
    notification.success({
      message: 'Field expression applied',
      description: `Parameter #${safeIndex} updated.`,
      placement: 'bottomRight',
      duration: 1.8,
    })
  }

  const addUiConditionCluster = (index: number) => {
    updateUiConditionBuilder(index, (state) => {
      const existingIds = new Set((state.clusters || []).map((cluster) => String(cluster.id || '').toLowerCase()))
      let counter = (state.clusters || []).length + 1
      let nextId = `cluster_${counter}`
      while (existingIds.has(nextId.toLowerCase())) {
        counter += 1
        nextId = `cluster_${counter}`
      }
      const nextCluster = createUiConditionCluster({ id: nextId, joinWithPrev: 'and' })
      const nextClause = createUiConditionClause(nextCluster.id, { joinWithPrev: 'and' })
      return {
        ...state,
        clusters: [...state.clusters, nextCluster],
        clauses: [...state.clauses, nextClause],
      }
    })
  }

  const removeUiConditionCluster = (index: number, clusterId: string) => {
    updateUiConditionBuilder(index, (state) => {
      const safeClusterId = String(clusterId || '').trim()
      if (!safeClusterId || state.clusters.length <= 1) return state
      const nextClusters = state.clusters.filter((cluster) => cluster.id !== safeClusterId)
      const nextClauses = state.clauses.filter((clause) => clause.clusterId !== safeClusterId)
      if (nextClauses.length === 0) {
        const fallbackCluster = nextClusters[0] || createUiConditionCluster({ id: 'cluster_1', joinWithPrev: 'and' })
        return {
          clusters: nextClusters.length > 0 ? nextClusters : [fallbackCluster],
          clauses: [createUiConditionClause(fallbackCluster.id, { id: 'clause_1', joinWithPrev: 'and' })],
        }
      }
      return {
        clusters: nextClusters,
        clauses: nextClauses,
      }
    })
  }

  const updateUiConditionClusterJoin = (index: number, clusterId: string, joinWithPrev: UiConditionLogicalJoin) => {
    updateUiConditionBuilder(index, (state) => ({
      ...state,
      clusters: state.clusters.map((cluster) => (
        cluster.id === clusterId
          ? { ...cluster, joinWithPrev: joinWithPrev === 'or' ? 'or' : 'and' }
          : cluster
      )),
    }))
  }

  const addUiConditionClause = (index: number, clusterId: string) => {
    const safeClusterId = String(clusterId || '').trim()
    if (!safeClusterId) return
    updateUiConditionBuilder(index, (state) => ({
      ...state,
      clauses: [...state.clauses, createUiConditionClause(safeClusterId, { joinWithPrev: 'and' })],
    }))
  }

  const removeUiConditionClause = (index: number, clauseId: string) => {
    const safeClauseId = String(clauseId || '').trim()
    if (!safeClauseId) return
    updateUiConditionBuilder(index, (state) => {
      const nextClauses = state.clauses.filter((clause) => clause.id !== safeClauseId)
      if (nextClauses.length === 0) {
        const fallbackCluster = state.clusters[0] || createUiConditionCluster({ id: 'cluster_1', joinWithPrev: 'and' })
        return {
          clusters: state.clusters.length > 0 ? state.clusters : [fallbackCluster],
          clauses: [createUiConditionClause(fallbackCluster.id, { id: 'clause_1', joinWithPrev: 'and' })],
        }
      }
      const referencedClusters = new Set(nextClauses.map((clause) => clause.clusterId))
      const nextClusters = state.clusters.filter((cluster) => referencedClusters.has(cluster.id))
      return {
        clusters: nextClusters.length > 0 ? nextClusters : state.clusters.slice(0, 1),
        clauses: nextClauses,
      }
    })
  }

  const updateUiConditionClause = (index: number, clauseId: string, patch: Partial<UiConditionClause>) => {
    const safeClauseId = String(clauseId || '').trim()
    if (!safeClauseId) return
    updateUiConditionBuilder(index, (state) => ({
      ...state,
      clauses: state.clauses.map((clause) => (
        clause.id === safeClauseId
          ? {
            ...clause,
            ...patch,
            joinWithPrev: patch.joinWithPrev === 'or' ? 'or' : patch.joinWithPrev === 'and' ? 'and' : clause.joinWithPrev,
            rightMode: (
              patch.rightMode === 'field'
              || patch.rightMode === 'values'
              || patch.rightMode === 'variable'
              || patch.rightMode === 'raw'
              || patch.rightMode === 'literal'
                ? patch.rightMode
                : clause.rightMode
            ),
          }
          : clause
      )),
    }))
  }

  const buildUiConditionClauseExpression = (clause: UiConditionClause): string => {
    const leftField = String(clause.leftField || '').trim()
    const leftToken = leftField ? `field('${leftField.replace(/'/g, "\\'")}')` : "field('')"
    const operator = String(clause.operator || '==').trim() || '=='
    if (operator.toLowerCase() === 'raw') {
      return String(clause.rightValue || '').trim() || "field('') == ''"
    }
    const rightMode: UiExpressionParameterMode = clause.rightMode === 'field'
      ? 'field'
      : clause.rightMode === 'values'
        ? 'values'
        : clause.rightMode === 'variable'
          ? 'variable'
          : clause.rightMode === 'raw'
            ? 'raw'
            : 'literal'
    const rightToken = resolveUiExpressionParameterToken(
      rightMode,
      String(clause.rightValue || ''),
      uiExpressionVariables,
    )
    return `${leftToken} ${operator} ${rightToken}`
  }

  const applyUiConditionBuilderToPlaceholder = (index: number) => {
    const safeIndex = Number(index)
    if (!Number.isFinite(safeIndex)) return
    const state = uiConditionBuilderByIndex[safeIndex] || createDefaultUiConditionBuilderState()
    const normalizedState = createDefaultUiConditionBuilderState(state)
    const clusterMap = new Map<string, UiConditionClause[]>()
    normalizedState.clauses.forEach((clause) => {
      const clusterId = String(clause.clusterId || '')
      if (!clusterId) return
      if (!clusterMap.has(clusterId)) clusterMap.set(clusterId, [])
      clusterMap.get(clusterId)?.push(clause)
    })
    const clusterExpressions: Array<{ joinWithPrev: UiConditionLogicalJoin; expression: string }> = []
    normalizedState.clusters.forEach((cluster, clusterIndex) => {
      const clusterClauses = clusterMap.get(cluster.id) || []
      if (clusterClauses.length === 0) return
      const clauseExpression = clusterClauses
        .map((clause, clauseIndex) => {
          const expr = buildUiConditionClauseExpression(clause)
          if (clauseIndex === 0) return expr
          return `${clause.joinWithPrev === 'or' ? 'or' : 'and'} ${expr}`
        })
        .join(' ')
      clusterExpressions.push({
        joinWithPrev: clusterIndex === 0 ? 'and' : (cluster.joinWithPrev === 'or' ? 'or' : 'and'),
        expression: `(${clauseExpression})`,
      })
    })
    const conditionExpression = clusterExpressions.length > 0
      ? clusterExpressions
        .map((item, idx) => (idx === 0 ? item.expression : `${item.joinWithPrev} ${item.expression}`))
        .join(' ')
      : "field('') == ''"
    updateUiExpressionPlaceholderMode(safeIndex, 'raw')
    updateUiExpressionPlaceholderValue(safeIndex, conditionExpression)
    notification.success({
      message: 'Condition applied',
      description: `Parameter #${safeIndex} updated.`,
      placement: 'bottomRight',
      duration: 1.8,
    })
  }

  const createUiExpressionJsonRow = () => {
    const key = String(uiExpressionNewJsonKeyDraft || '').trim()
    if (!key) {
      notification.warning({
        message: 'Key name required',
        description: 'Enter a key name to create a new JSON mapping row.',
        placement: 'bottomRight',
      })
      return
    }
    const exists = (uiExpressionRows || []).some(
      (row) => String(row.key || '').trim().toLowerCase() === key.toLowerCase()
    )
    if (exists) {
      notification.info({
        message: 'Key already exists',
        description: `JSON key "${key}" is already present. You can edit it.`,
        placement: 'bottomRight',
      })
      setUiExpressionJsonKey(key)
      return
    }
    const rowId = `ui_row_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
    setUiExpressionRows((prev) => ([
      ...prev,
      { id: rowId, key, expression: "=field('')", valueType: 'expression' },
    ]))
    setUiExpressionJsonKey(key)
    setUiExpressionNewJsonKeyDraft('')
    notification.success({
      message: 'New key created',
      description: `Created JSON key "${key}". Use Edit to configure expression.`,
      placement: 'bottomRight',
    })
  }

  const addUiExpressionVariable = () => {
    setUiExpressionBuilderTouched(true)
    const existingNames = new Set(
      (uiExpressionVariables || []).map((item) => String(item.name || '').trim().toLowerCase()).filter(Boolean)
    )
    let counter = (uiExpressionVariables || []).length + 1
    let candidate = `var_${counter}`
    while (existingNames.has(candidate.toLowerCase())) {
      counter += 1
      candidate = `var_${counter}`
    }
    setUiExpressionVariables((prev) => [
      ...prev,
      createUiExpressionVariableSpec({ name: candidate, value: "''" }),
    ])
  }

  const updateUiExpressionVariable = (id: string, patch: Partial<UiExpressionVariableSpec>) => {
    setUiExpressionBuilderTouched(true)
    setUiExpressionVariables((prev) => (
      (prev || []).map((item) => (item.id === id ? { ...item, ...patch } : item))
    ))
  }

  const removeUiExpressionVariable = (id: string) => {
    setUiExpressionBuilderTouched(true)
    setUiExpressionVariables((prev) => (prev || []).filter((item) => item.id !== id))
  }

  const addUiGroupAggregateMetric = () => {
    setUiExpressionBuilderTouched(true)
    const defaultPath = expressionPathOptions[0] || ''
    setUiGroupAggregateMetrics((prev) => [
      ...(prev || []),
      createUiGroupAggregateMetric({ outputName: `metric_${(prev || []).length + 1}`, path: defaultPath, agg: 'sum' }),
    ])
  }

  const updateUiGroupAggregateMetric = (id: string, patch: Partial<UiGroupAggregateMetric>) => {
    setUiExpressionBuilderTouched(true)
    setUiGroupAggregateMetrics((prev) => (
      (prev || []).map((metric) => {
        if (metric.id !== id) return metric
        return {
          ...metric,
          ...patch,
          outputName: patch.outputName !== undefined
            ? normalizeUiGroupAggregateOutputName(String(patch.outputName || ''))
            : metric.outputName,
        }
      })
    ))
  }

  const removeUiGroupAggregateMetric = (id: string) => {
    setUiExpressionBuilderTouched(true)
    setUiGroupAggregateMetrics((prev) => {
      const next = (prev || []).filter((metric) => metric.id !== id)
      return next.length > 0 ? next : [createUiGroupAggregateMetric({ outputName: 'metric', path: expressionPathOptions[0] || '', agg: 'sum' })]
    })
  }

  const addUiBreClause = () => {
    setUiBreModel((prev) => {
      const nextModel: BreModel = {
        ...prev,
        clauses: [
          ...(Array.isArray(prev.clauses) ? prev.clauses : []),
          createBreClause({
            joinWithPrev: 'and',
            leftKind: 'field',
            leftValue: expressionPathOptions[0] || '',
            operator: '==',
            rightKind: 'literal',
            rightValue: '',
          }),
        ],
        parseError: null,
      }
      syncBreModelToEditor(nextModel)
      return nextModel
    })
  }

  const updateUiBreClause = (clauseId: string, patch: Partial<BreClause>) => {
    setUiBreModel((prev) => {
      const nextModel: BreModel = {
        ...prev,
        clauses: (prev.clauses || []).map((clause) => (
          clause.id === clauseId
            ? {
              ...clause,
              ...patch,
              joinWithPrev: (patch.joinWithPrev || clause.joinWithPrev || 'and') as BreClauseJoin,
              leftKind: (patch.leftKind || clause.leftKind || 'field') as BreClauseOperandKind,
              operator: (patch.operator || clause.operator || '==') as BreClauseOperator,
              rightKind: (patch.rightKind || clause.rightKind || 'literal') as BreClauseOperandKind,
            }
            : clause
        )),
        parseError: null,
      }
      syncBreModelToEditor(nextModel)
      return nextModel
    })
  }

  const removeUiBreClause = (clauseId: string) => {
    setUiBreModel((prev) => {
      const nextClauses = (prev.clauses || []).filter((clause) => clause.id !== clauseId)
      const nextModel: BreModel = {
        ...prev,
        clauses: nextClauses.length > 0 ? nextClauses : [createBreClause()],
        parseError: null,
      }
      syncBreModelToEditor(nextModel)
      return nextModel
    })
  }

  const updateUiBreThenExpr = (nextThenExpr: string) => {
    setUiBreModel((prev) => {
      const nextModel: BreModel = {
        ...prev,
        thenExpr: String(nextThenExpr || ''),
        parseError: null,
      }
      syncBreModelToEditor(nextModel)
      return nextModel
    })
  }

  const updateUiBreElseExpr = (nextElseExpr: string) => {
    setUiBreModel((prev) => {
      const nextModel: BreModel = {
        ...prev,
        elseExpr: String(nextElseExpr || ''),
        parseError: null,
      }
      syncBreModelToEditor(nextModel)
      return nextModel
    })
  }

  const reloadUiBreFromExpression = () => {
    const parsed = parseBreModelFromExpression(uiBreTargetExpression)
    setUiBreModel(parsed)
    if (parsed.parseError) {
      notification.warning({
        message: 'BRE parsing partial',
        description: parsed.parseError,
        placement: 'bottomRight',
      })
    } else {
      notification.success({
        message: 'BRE synced',
        description: 'UI BRE model loaded from current expression.',
        placement: 'bottomRight',
      })
    }
  }

  const applyUiBreNow = () => {
    if (!expandedEditorField) return
    syncBreModelToEditor(uiBreModel)
    notification.success({
      message: 'BRE expression synced',
      description: 'Expression editor is synchronized from BRE builder.',
      placement: 'bottomRight',
    })
  }

  const applyUiExpressionToEditor = () => {
    if (!expandedEditorField) return
    const expression = normalizeExpressionValue(uiExpressionPreviewRaw)
    if (!expression) return
    if (expandedEditorMode === 'json') {
      const key = String(uiExpressionJsonKey || '').trim() || 'value'
      let parsed: Record<string, unknown> = {}
      try {
        const current = JSON.parse(String(expandedEditorField.jsonTemplate || '{}'))
        if (current && typeof current === 'object' && !Array.isArray(current)) {
          parsed = { ...(current as Record<string, unknown>) }
        }
      } catch {
        parsed = {}
      }
      parsed[key] = expression
      updateCustomFieldDraft(expandedEditorField.id, { jsonTemplate: JSON.stringify(parsed, null, 2) })
      notification.success({
        message: 'Expression applied',
        description: `Added/updated JSON key "${key}" in template.`,
        placement: 'bottomRight',
      })
      return
    }
    updateCustomFieldDraft(expandedEditorField.id, { expression })
    notification.success({
      message: 'Expression applied',
      description: 'UI Expression output copied to Expression editor.',
      placement: 'bottomRight',
    })
  }

  const loadUiExpressionIntoBuilder = (sourceExpression: string): boolean => {
    const normalized = normalizeExpressionValue(String(sourceExpression || '')).trim()
    if (!normalized) {
      return false
    }
    const body = normalized.startsWith('=') ? normalized.slice(1).trim() : normalized
    const call = parseTopLevelFunctionCallSource(body)
    if (!call) {
      return false
    }
    const fnNameLower = String(call.name || '').toLowerCase()
    const matchedTemplate = uiExpressionTemplateOptions.find((item) => (
      extractSnippetFunctionName(String(item.snippet || '')).toLowerCase() === fnNameLower
    ))
    if (!matchedTemplate) {
      return false
    }

    setUiExpressionBuilderTouched(true)
    setUiExpressionTemplateLabel(String(matchedTemplate.value || ''))

    if (fnNameLower === 'group_aggregate') {
      const keyField = stripEnclosingQuotes(String(call.args[0] || ''))
      const metrics = parseGroupAggregateMetricsFromArg(String(call.args[1] || ''))
      const keyName = stripEnclosingQuotes(String(call.args[2] || '')) || 'group'
      setUiGroupAggregateKeyField(keyField)
      setUiGroupAggregateKeyName(keyName)
      setUiGroupAggregateMetrics(
        metrics.length > 0
          ? metrics
          : [createUiGroupAggregateMetric({ outputName: 'metric', path: expressionPathOptions[0] || '', agg: 'sum' })]
      )
      return true
    }

    const placeholders = parseSnippetPlaceholders(String(matchedTemplate.snippet || ''))
    const nextModes: Record<number, UiExpressionParameterMode> = {}
    const nextValues: Record<number, string> = {}
    placeholders.forEach((placeholder) => {
      const argIndex = Math.max(0, Number(placeholder.index) - 1)
      const argValue = String(call.args[argIndex] || '').trim()
      if (!argValue) return
      const inferred = inferUiExpressionModeAndValue(argValue)
      nextModes[placeholder.index] = inferred.mode
      nextValues[placeholder.index] = inferred.value
    })
    setUiExpressionParamModeByIndex(nextModes)
    setUiExpressionValuesByIndex(nextValues)
    return true
  }

  const loadUiExpressionFromEditor = () => {
    if (!expandedEditorField) return
    const activeExpression = (() => {
      if (expandedEditorMode !== 'json') return String(expandedEditorField.expression || '')
      const key = String(uiExpressionJsonKey || '').trim()
      const fromJson = parseJsonTemplateObject(String(expandedEditorField.jsonTemplate || '{}'))
      if (key && typeof fromJson[key] === 'string') return String(fromJson[key] || '')
      const firstStringEntry = Object.values(fromJson).find((value) => typeof value === 'string')
      return String(firstStringEntry || '')
    })()

    const loaded = loadUiExpressionIntoBuilder(activeExpression)
    if (loaded) {
      notification.success({
        message: 'Builder synced',
        description: 'Loaded expression from editor into UI Expression Builder.',
        placement: 'bottomRight',
      })
      return
    }
    notification.warning({
      message: 'Unable to load',
      description: 'UI builder can load supported top-level function expressions only.',
      placement: 'bottomRight',
    })
  }

  const editUiExpressionJsonRowInBuilder = (row: UiExpressionBuilderRow) => {
    const key = String(row.key || '').trim()
    if (key) setUiExpressionJsonKey(key)
    if ((row.valueType || 'expression') !== 'expression') {
      notification.info({
        message: 'JSON row selected',
        description: 'This row stores JSON. Edit its JSON directly in the row editor.',
        placement: 'bottomRight',
      })
      return
    }
    const loaded = loadUiExpressionIntoBuilder(String(row.expression || ''))
    if (!loaded) {
      notification.warning({
        message: 'Unable to load row',
        description: 'Selected row expression is not supported by UI builder parser.',
        placement: 'bottomRight',
      })
    }
  }

  const upsertUiExpressionJsonRow = () => {
    const key = String(uiExpressionJsonKey || '').trim()
    const expression = normalizeExpressionValue(uiExpressionPreviewRaw)
    if (!key) {
      notification.warning({
        message: 'JSON key required',
        description: 'Enter a JSON key before adding expression row.',
        placement: 'bottomRight',
      })
      return
    }
    if (!expression) {
      notification.warning({
        message: 'Expression required',
        description: 'Build an expression before adding JSON row.',
        placement: 'bottomRight',
      })
      return
    }
    const rowId = `ui_row_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
    setUiExpressionRows((prev) => {
      const next = [...prev]
      const existingIdx = next.findIndex((row) => row.key.trim().toLowerCase() === key.toLowerCase())
      if (existingIdx >= 0) {
        next[existingIdx] = { ...next[existingIdx], expression, key, valueType: next[existingIdx].valueType || 'expression' }
      } else {
        next.push({ id: rowId, key, expression, valueType: 'expression' })
      }
      return next
    })
  }

  const updateUiExpressionJsonRow = (rowId: string, patch: Partial<UiExpressionBuilderRow>) => {
    setUiExpressionRows((prev) =>
      prev.map((row) => (row.id === rowId
        ? {
          ...row,
          ...(patch.key !== undefined ? { key: String(patch.key || '') } : {}),
          ...(patch.expression !== undefined ? { expression: String(patch.expression || '') } : {}),
          ...(patch.valueType !== undefined ? { valueType: patch.valueType } : {}),
        }
        : row
      ))
    )
  }

  const removeUiExpressionJsonRow = (rowId: string) => {
    setUiExpressionRows((prev) => prev.filter((row) => row.id !== rowId))
  }

  const applyUiExpressionJsonRowsToEditor = () => {
    if (!expandedEditorField || expandedEditorMode !== 'json') return
    const normalizedRows = uiExpressionRows
      .map((row) => ({
        key: String(row.key || '').trim(),
        raw: String(row.expression || '').trim(),
        value: parseUiBuilderRowValue(row),
      }))
      .filter((row) => row.key && row.raw)
    if (!normalizedRows.length) {
      notification.warning({
        message: 'No rows to apply',
        description: 'Add at least one JSON expression row in UI builder.',
        placement: 'bottomRight',
      })
      return
    }
    const payload: Record<string, unknown> = {}
    normalizedRows.forEach((row) => {
      payload[row.key] = row.value
    })
    updateCustomFieldDraft(expandedEditorField.id, { jsonTemplate: JSON.stringify(payload, null, 2) })
    notification.success({
      message: 'JSON template applied',
      description: `Applied ${normalizedRows.length} UI-built expression row${normalizedRows.length === 1 ? '' : 's'}.`,
      placement: 'bottomRight',
    })
  }

  const loadUiExpressionRowsFromEditor = () => {
    if (!expandedEditorField || expandedEditorMode !== 'json') return
    try {
      const rows = parseJsonTemplateRows(String(expandedEditorField.jsonTemplate || '{}'))
      setUiExpressionRows(rows)
      if (rows[0]) setUiExpressionJsonKey(rows[0].key)
      notification.success({
        message: 'UI rows loaded',
        description: `Loaded ${rows.length} expression row${rows.length === 1 ? '' : 's'} from JSON template.`,
        placement: 'bottomRight',
      })
    } catch (err: any) {
      notification.error({
        message: 'Load failed',
        description: String(err?.message || 'JSON template is invalid'),
        placement: 'bottomRight',
      })
    }
  }

  const runCustomFieldValidation = async () => {
    const normalized = serializeCustomFieldSpecs(customFieldDraft)
    if (normalized.length === 0) {
      notification.warning({
        message: 'No custom fields to validate',
        description: 'Add at least one custom field before running test output validation.',
        placement: 'bottomRight',
      })
      return
    }

    const primaryKeyField = String(customPrimaryKeyFieldDraft || '').trim()
    const profileWindowDays = String(customProfileWindowDaysDraft || '').trim() || '1,7,30'
    const profileRetentionDays = Number.isFinite(customProfileRetentionDaysDraft) && customProfileRetentionDaysDraft > 0
      ? Math.floor(customProfileRetentionDaysDraft)
      : 45
    const requestedValidationSource: 'lmdb' | 'rocksdb' | 'rows' = (
      validationSourceDraft === 'sample'
        ? 'rows'
        : validationSourceDraft === 'rocksdb'
          ? 'rocksdb'
          : 'lmdb'
    )
    const validationLimit = Math.max(1, Math.min(Number(validationLmdbLimitDraft || 30) || 30, 200))
    const lmdbEnvPath = String(validationLmdbEnvPathDraft || '').trim()
    const selectedNode = nodes.find((item) => item.id === selectedNodeId)
    const selectedPreviewRows = extractPreviewRowsFromNode(selectedNode)
    const upstreamPreviewRows = selectedNodeId
      ? inferUpstreamPreviewRows(selectedNodeId, nodes, edges, validationLimit)
      : []
    const sampleRowsRaw = [...selectedPreviewRows, ...upstreamPreviewRows]
    const sampleRows = sampleRowsRaw
      .filter((row): row is Record<string, unknown> => !!row && typeof row === 'object' && !Array.isArray(row))
      .slice(0, validationLimit)
    const validationSource: 'lmdb' | 'rocksdb' | 'rows' = (
      requestedValidationSource !== 'rows' && !lmdbEnvPath
        ? 'rows'
        : requestedValidationSource
    )
    const appendUniqueCacheModeDraft = (
      customProfileStorageDraft === 'rocksdb' && customProfileProcessingModeDraft === 'incremental'
        ? 'persistent'
        : 'auto'
    )
    if (requestedValidationSource !== 'rows' && !lmdbEnvPath) {
      notification.warning({
        message: `${requestedValidationSource === 'rocksdb' ? 'RocksDB' : 'LMDB'} path missing, using sample rows`,
        description: 'Validation switched to Sample Rows mode because validation DB path is empty.',
        placement: 'bottomRight',
      })
    }
    if (validationSource === 'rows' && sampleRows.length === 0) {
      notification.error({
        message: 'No sample rows available',
        description: 'Run source detect/preview or execute upstream nodes to generate sample rows for validation.',
        placement: 'bottomRight',
      })
      return
    }

    const testConfig: Record<string, unknown> = {
      ...nodeConfig,
      custom_fields: normalized,
      custom_include_source_fields: customIncludeSourceDraft,
      custom_expression_engine: customExpressionEngineDraft,
      custom_primary_key_field: primaryKeyField,
      custom_group_by_field: primaryKeyField,
      custom_profile_enabled: customProfileEnabledDraft,
      custom_profile_storage: customProfileStorageDraft,
      custom_profile_oracle_host: String(customProfileOracleHostDraft || '').trim(),
      custom_profile_oracle_port: customProfileOraclePortDraft,
      custom_profile_oracle_service_name: String(customProfileOracleServiceNameDraft || '').trim(),
      custom_profile_oracle_sid: String(customProfileOracleSidDraft || '').trim(),
      custom_profile_oracle_user: String(customProfileOracleUserDraft || '').trim(),
      custom_profile_oracle_password: String(customProfileOraclePasswordDraft || '').trim(),
      custom_profile_oracle_dsn: String(customProfileOracleDsnDraft || '').trim(),
      custom_profile_oracle_table: String(customProfileOracleTableDraft || '').trim() || 'ETL_PROFILE_STATE',
      custom_profile_oracle_write_strategy: customProfileOracleWriteStrategyDraft,
      custom_profile_oracle_parallel_workers: customProfileOracleParallelWorkersDraft,
      custom_profile_oracle_parallel_min_tokens: customProfileOracleParallelMinTokensDraft,
      custom_profile_oracle_merge_batch_size: customProfileOracleMergeBatchSizeDraft,
      custom_profile_oracle_queue_enabled: customProfileOracleQueueEnabledDraft,
      custom_profile_oracle_queue_wait_on_force_flush: customProfileOracleQueueWaitOnForceFlushDraft,
      custom_profile_processing_mode: customProfileProcessingModeDraft,
      custom_profile_compute_strategy: customProfileComputeStrategyDraft,
      custom_profile_compute_executor: customProfileComputeExecutorDraft,
      custom_profile_compute_workers: customProfileComputeWorkersDraft,
      custom_profile_compute_min_rows: customProfileComputeMinRowsDraft,
      custom_profile_compute_global_prefetch_max_tokens: customProfileComputeGlobalPrefetchMaxTokensDraft,
      custom_profile_backfill_candidate_prefetch_chunk_size: customProfileBackfillCandidatePrefetchChunkSizeDraft,
      custom_profile_emit_mode: customProfileEmitModeDraft,
      custom_profile_required_fields: customProfileRequiredFieldsDraft.join(', '),
      custom_profile_event_time_field: customProfileEventTimeFieldDraft,
      custom_profile_window_days: profileWindowDays,
      custom_profile_retention_days: profileRetentionDays,
      custom_profile_include_change_fields: customProfileIncludeChangeFieldsDraft,
      custom_profile_live_persist: customProfileLivePersistDraft,
      custom_profile_flush_every_rows: customProfileFlushEveryRowsDraft,
      custom_profile_flush_interval_seconds: customProfileFlushIntervalSecondsDraft,
      custom_profile_append_unique_cache_mode: appendUniqueCacheModeDraft,
      custom_profile_append_unique_cache_disable_min_rows: customProfileAppendUniqueDisableMinRowsDraft,
      custom_profile_append_unique_cache_entity_limit: customProfileAppendUniqueEntityLimitDraft,
      custom_profile_append_unique_cache_disable_ratio: customProfileAppendUniqueDisableRatioDraft,
      custom_validation_source: validationSource,
      custom_validation_lmdb_env_path: validationLmdbEnvPathDraft,
      custom_validation_lmdb_db_name: validationLmdbDbNameDraft,
      custom_validation_lmdb_key_prefix: validationLmdbKeyPrefixDraft,
      custom_validation_lmdb_start_key: validationLmdbStartKeyDraft,
      custom_validation_lmdb_end_key: validationLmdbEndKeyDraft,
      custom_validation_lmdb_key_contains: validationLmdbKeyContainsDraft,
      custom_validation_lmdb_limit: validationLimit,
      custom_validation_rocksdb_env_path: validationLmdbEnvPathDraft,
    }

    setValidationLoading(true)
    setValidationResult(null)
    try {
      const result = await api.validateCustomFields({
        config: testConfig,
        rows: validationSource === 'rows' ? sampleRows : [],
        max_rows: validationLimit,
        validation_source: validationSource,
        lmdb_config: {
          env_path: lmdbEnvPath,
          db_name: String(validationLmdbDbNameDraft || '').trim(),
          key_prefix: String(validationLmdbKeyPrefixDraft || '').trim(),
          start_key: String(validationLmdbStartKeyDraft || '').trim(),
          end_key: String(validationLmdbEndKeyDraft || '').trim(),
          key_contains: String(validationLmdbKeyContainsDraft || '').trim(),
          limit: validationLimit,
        },
        rocksdb_config: {
          env_path: lmdbEnvPath,
          key_prefix: String(validationLmdbKeyPrefixDraft || '').trim(),
          start_key: String(validationLmdbStartKeyDraft || '').trim(),
          end_key: String(validationLmdbEndKeyDraft || '').trim(),
          key_contains: String(validationLmdbKeyContainsDraft || '').trim(),
          limit: validationLimit,
        },
      })
      setValidationResult(result as CustomFieldValidationResult)
      setValidationModalOpen(true)
      const resultOk = Boolean((result as any)?.ok)
      const resultWarnings = Array.isArray((result as any)?.warnings)
        ? ((result as any).warnings as unknown[]).map((item) => String(item))
        : []
      const responseSource = String((result as any)?.validation_source || validationSource).toUpperCase()
      const baseDesc = `${responseSource} | Input ${Number((result as any)?.input_rows || 0)} row(s), output ${Number((result as any)?.output_rows || 0)} row(s).`
      if (resultOk) {
        if (resultWarnings.length > 0) {
          notification.warning({
            message: 'Validation completed with warnings',
            description: `${baseDesc} ${resultWarnings[0]}`,
            placement: 'bottomRight',
          })
        } else {
          notification.success({
            message: 'Validation completed',
            description: baseDesc,
            placement: 'bottomRight',
            duration: 2,
          })
        }
      } else {
        const firstError = Array.isArray((result as any)?.errors) && (result as any).errors.length > 0
          ? String((result as any).errors[0])
          : 'Validation returned errors.'
        notification.error({
          message: 'Validation completed with errors',
          description: `${baseDesc} ${firstError}`,
          placement: 'bottomRight',
        })
      }
    } catch (err: any) {
      const responseData = err?.response?.data
      const responseErrors = Array.isArray(responseData?.errors)
        ? responseData.errors.map((item: unknown) => String(item))
        : []
      const responseWarnings = Array.isArray(responseData?.warnings)
        ? responseData.warnings.map((item: unknown) => String(item))
        : []
      const fallbackMsg = String(
        responseData?.detail
        || responseData?.message
        || err?.message
        || 'Custom field validation failed',
      )
      setValidationResult({
        ok: false,
        validation_source: String(responseData?.validation_source || validationSource),
        input_rows: Number(responseData?.input_rows || 0),
        output_rows: Number(responseData?.output_rows || 0),
        errors: responseErrors.length > 0 ? responseErrors : [fallbackMsg],
        warnings: responseWarnings,
        sample_input: Array.isArray(responseData?.sample_input) ? responseData.sample_input : [],
        sample_output: Array.isArray(responseData?.sample_output) ? responseData.sample_output : [],
      })
      setValidationModalOpen(true)
      notification.error({
        message: 'Validation failed',
        description: fallbackMsg,
        placement: 'bottomRight',
      })
    } finally {
      setValidationLoading(false)
    }
  }

  const openCustomFieldStudio = () => {
    const initialDraft = (
      configuredCustomFields.length > 0
        ? configuredCustomFields.map((item) => createCustomFieldSpec(item))
        : [createCustomFieldSpec()]
    )
    setCustomFieldDraft(initialDraft)
    setCustomIncludeSourceDraft(Boolean(nodeConfig.custom_include_source_fields ?? true))
    setCustomExpressionEngineDraft(configuredExpressionEngine)
    setCustomPrimaryKeyFieldDraft(configuredPrimaryKeyField)
    setCustomProfileEnabledDraft(configuredProfileEnabled)
    setCustomProfileStorageDraft(configuredProfileStorage)
    setCustomProfileProcessingModeDraft(configuredProfileProcessingMode)
    setCustomProfileEmitModeDraft(configuredProfileEmitMode)
    setCustomProfileRequiredFieldsDraft(configuredProfileRequiredFields)
    setCustomProfileEventTimeFieldDraft(configuredProfileEventTimeField)
    setCustomProfileWindowDaysDraft(configuredProfileWindowDays)
    setCustomProfileRetentionDaysDraft(
      Number.isFinite(configuredProfileRetentionDays) && configuredProfileRetentionDays > 0
        ? Math.floor(configuredProfileRetentionDays)
        : 45
    )
    setCustomProfileIncludeChangeFieldsDraft(configuredProfileIncludeChangeFields)
    setCustomProfileOracleHostDraft(configuredProfileOracleHost)
    setCustomProfileOraclePortDraft(configuredProfileOraclePort)
    setCustomProfileOracleServiceNameDraft(configuredProfileOracleServiceName)
    setCustomProfileOracleSidDraft(configuredProfileOracleSid)
    setCustomProfileOracleUserDraft(configuredProfileOracleUser)
    setCustomProfileOraclePasswordDraft(configuredProfileOraclePassword)
    setCustomProfileOracleDsnDraft(configuredProfileOracleDsn)
    setCustomProfileOracleTableDraft(configuredProfileOracleTable)
    setCustomProfileOracleWriteStrategyDraft(configuredProfileOracleWriteStrategy)
    setCustomProfileOracleParallelWorkersDraft(configuredProfileOracleParallelWorkers)
    setCustomProfileOracleParallelMinTokensDraft(configuredProfileOracleParallelMinTokens)
    setCustomProfileOracleMergeBatchSizeDraft(configuredProfileOracleMergeBatchSize)
    setCustomProfileOracleQueueEnabledDraft(configuredProfileOracleQueueEnabled)
    setCustomProfileOracleQueueWaitOnForceFlushDraft(configuredProfileOracleQueueWaitOnForceFlush)
    setCustomProfileComputeStrategyDraft(configuredProfileComputeStrategy)
    setCustomProfileComputeExecutorDraft(configuredProfileComputeExecutor)
    setCustomProfileComputeWorkersDraft(configuredProfileComputeWorkers)
    setCustomProfileComputeMinRowsDraft(configuredProfileComputeMinRows)
    setCustomProfileComputeGlobalPrefetchMaxTokensDraft(configuredProfileComputeGlobalPrefetchMaxTokens)
    setCustomProfileBackfillCandidatePrefetchChunkSizeDraft(configuredProfileBackfillCandidatePrefetchChunkSize)
    setCustomProfileLivePersistDraft(
      configuredProfileProcessingMode === 'incremental_batch'
        ? true
        : configuredProfileLivePersist
    )
    setCustomProfileFlushEveryRowsDraft(
      configuredProfileProcessingMode === 'incremental_batch'
        ? Math.max(100, Math.min(Number(configuredProfileFlushEveryRows) || 20000, 50000))
        : (configuredProfileStorage === 'rocksdb' && configuredProfileProcessingMode === 'incremental' && !hasConfiguredProfileFlushEveryRows)
          ? 50000
        : Math.max(1, Math.floor(Number(configuredProfileFlushEveryRows) || 20000))
    )
    setCustomProfileFlushIntervalSecondsDraft(
      configuredProfileProcessingMode === 'incremental_batch'
        ? Math.max(0.5, Math.min(Number(configuredProfileFlushIntervalSeconds) || 2, 10))
        : (configuredProfileStorage === 'rocksdb' && configuredProfileProcessingMode === 'incremental' && !hasConfiguredProfileFlushIntervalSeconds)
          ? 10
        : Math.max(0.1, Number(configuredProfileFlushIntervalSeconds) || 2)
    )
    setCustomProfileAppendUniqueDisableMinRowsDraft(
      Number.isFinite(configuredProfileAppendUniqueDisableMinRows) && configuredProfileAppendUniqueDisableMinRows > 0
        ? Math.max(1000, Math.floor(configuredProfileAppendUniqueDisableMinRows))
        : 5000
    )
    setCustomProfileAppendUniqueEntityLimitDraft(
      Number.isFinite(configuredProfileAppendUniqueEntityLimit) && configuredProfileAppendUniqueEntityLimit > 0
        ? Math.max(1000, Math.floor(configuredProfileAppendUniqueEntityLimit))
        : 10000
    )
    setCustomProfileAppendUniqueDisableRatioDraft(
      Number.isFinite(configuredProfileAppendUniqueDisableRatio)
        ? Math.max(0.01, Math.min(Number(configuredProfileAppendUniqueDisableRatio), 1))
        : 0.35
    )
    const configuredValidationSource = String(nodeConfig.custom_validation_source || '')
      .trim()
      .toLowerCase()
    setValidationSourceDraft(
      configuredValidationSource === 'rocksdb'
        ? 'rocksdb'
        : configuredValidationSource === 'lmdb'
          ? 'lmdb'
          : 'sample'
    )
    setValidationLmdbEnvPathDraft(configuredValidationLmdbEnvPath)
    setValidationLmdbDbNameDraft(configuredValidationLmdbDbName)
    setValidationLmdbKeyPrefixDraft(configuredValidationLmdbKeyPrefix)
    setValidationLmdbStartKeyDraft(configuredValidationLmdbStartKey)
    setValidationLmdbEndKeyDraft(configuredValidationLmdbEndKey)
    setValidationLmdbKeyContainsDraft(configuredValidationLmdbKeyContains)
    setValidationLmdbLimitDraft(
      Number.isFinite(configuredValidationLmdbLimit) && configuredValidationLmdbLimit > 0
        ? Math.min(200, Math.floor(configuredValidationLmdbLimit))
        : 50
    )
    setActiveExpressionFieldId(null)
    setExampleRepoCategory('all')
    setExampleRepoSearch('')
    setCustomFieldBeautifyUndoById({})
    setCustomFieldStudioOpen(true)
    setProfileMonitorData(null)
    setProfileMonitorError(null)
    setValidationResult(null)
    setValidationModalOpen(false)
    const collapsedIds = parseStringList(nodeConfig.custom_collapsed_field_ids)
    const draftIdSet = new Set(initialDraft.map((item) => String(item.id || '').trim()).filter(Boolean))
    setCollapsedCustomFieldIds(collapsedIds.filter((id) => draftIdSet.has(id)))
  }

  const updateCustomFieldDraft = (
    id: string,
    patch: Partial<CustomFieldSpec>,
    options?: { preserveBeautifyUndo?: boolean },
  ) => {
    const shouldClearBeautifyUndo = (
      !options?.preserveBeautifyUndo
      && (
        Object.prototype.hasOwnProperty.call(patch, 'expression')
        || Object.prototype.hasOwnProperty.call(patch, 'jsonTemplate')
      )
    )
    if (shouldClearBeautifyUndo) {
      setCustomFieldBeautifyUndoById((prev) => {
        if (!prev[id]) return prev
        const next = { ...prev }
        delete next[id]
        return next
      })
    }
    setCustomFieldDraft((prev) =>
      prev.map((item) => (item.id === id ? { ...item, ...patch } : item))
    )
  }

  const beautifyCustomFieldDraft = (id: string) => {
    const target = customFieldDraft.find((item) => item.id === id)
    if (!target) return
    if (target.mode === 'json') {
      try {
        const formatted = beautifyJsonTemplateText(target.jsonTemplate, customEditorBeautifyStyle)
        if (formatted === target.jsonTemplate) {
          return
        }
        setCustomFieldBeautifyUndoById((prev) => ({
          ...prev,
          [id]: {
            expression: String(target.expression || ''),
            jsonTemplate: String(target.jsonTemplate || ''),
          },
        }))
        updateCustomFieldDraft(id, { jsonTemplate: formatted }, { preserveBeautifyUndo: true })
        notification.success({
          message: 'Beautified',
          description: customEditorBeautifyStyle === 'js_view'
            ? 'Template formatted in JS View style.'
            : 'JSON template formatted successfully.',
          placement: 'bottomRight',
          duration: 1.5,
        })
      } catch (err: any) {
        notification.error({
          message: 'Beautify failed',
          description: String(err?.message || 'JSON template is invalid. Fix syntax and try again.'),
          placement: 'bottomRight',
        })
      }
      return
    }
    const formatted = beautifyExpressionText(target.expression)
    if (formatted === target.expression) {
      return
    }
    setCustomFieldBeautifyUndoById((prev) => ({
      ...prev,
      [id]: {
        expression: String(target.expression || ''),
        jsonTemplate: String(target.jsonTemplate || ''),
      },
    }))
    updateCustomFieldDraft(id, { expression: formatted }, { preserveBeautifyUndo: true })
    notification.success({
      message: 'Beautified',
      description: 'Expression formatted successfully.',
      placement: 'bottomRight',
      duration: 1.5,
    })
  }

  const undoBeautifyCustomFieldDraft = (id: string) => {
    const backup = customFieldBeautifyUndoById[id]
    if (!backup) {
      notification.info({
        message: 'Nothing to revert',
        description: 'No beautify history found for this field.',
        placement: 'bottomRight',
        duration: 1.5,
      })
      return
    }
    updateCustomFieldDraft(id, {
      expression: backup.expression,
      jsonTemplate: backup.jsonTemplate,
    }, { preserveBeautifyUndo: true })
    setCustomFieldBeautifyUndoById((prev) => {
      const next = { ...prev }
      delete next[id]
      return next
    })
    notification.success({
      message: 'Beautify reverted',
      description: 'Field content restored to previous version.',
      placement: 'bottomRight',
      duration: 1.5,
    })
  }

  const addCustomFieldDraft = (seed?: Partial<CustomFieldSpec>) => {
    setCustomFieldDraft((prev) => [...prev, createCustomFieldSpec(seed)])
  }

  const removeCustomFieldDraft = (id: string) => {
    setCustomFieldDraft((prev) => prev.filter((item) => item.id !== id))
    setCollapsedCustomFieldIds((prev) => prev.filter((itemId) => itemId !== id))
    setCustomFieldBeautifyUndoById((prev) => {
      if (!prev[id]) return prev
      const next = { ...prev }
      delete next[id]
      return next
    })
  }

  const setAllCustomFieldsEnabled = (enabled: boolean) => {
    setCustomFieldDraft((prev) => prev.map((item) => ({ ...item, enabled })))
  }

  const expandAllCustomFields = () => {
    setCollapsedCustomFieldIds([])
  }

  const collapseAllCustomFields = () => {
    setCollapsedCustomFieldIds(customFieldDraft.map((item) => item.id))
  }

  const insertIntoActiveExpression = (token: string) => {
    const clean = String(token || '').trim()
    if (!clean) return
    setCustomFieldDraft((prev) => {
      let targetIdx = activeExpressionFieldId
        ? prev.findIndex((item) => item.id === activeExpressionFieldId && item.mode === 'value')
        : -1
      if (targetIdx < 0) {
        targetIdx = prev.findIndex((item) => item.mode === 'value')
      }
      if (targetIdx < 0) {
        notification.warning({
          message: 'No expression field selected',
          description: 'Create a "Single Value Expression" custom field and focus its editor first.',
          placement: 'bottomRight',
          duration: 2.5,
        })
        return prev
      }
      const target = prev[targetIdx]
      const current = String(target.expression || '')
      const nextExpr = current ? `${current} ${clean}` : clean
      const next = [...prev]
      next[targetIdx] = { ...target, expression: nextExpr }
      return next
    })
  }

  const collectEnabledCustomFieldSaveIssues = (items: CustomFieldSpec[]) => {
    type SaveIssue = { fieldId: string; fieldName: string; message: string }
    const issues: SaveIssue[] = []
    const enabledItems = items.filter((item) => Boolean(item.enabled))
    const seenNames = new Map<string, string>()

    enabledItems.forEach((item, idx) => {
      const fieldId = String(item.id || '').trim()
      const rawName = String(item.name || '').trim()
      const fallbackName = `Field #${idx + 1}`
      const fieldName = rawName || fallbackName
      const mode: CustomFieldMode = item.mode === 'json' ? 'json' : 'value'

      if (!rawName) {
        issues.push({
          fieldId,
          fieldName,
          message: 'Field name is required for enabled custom field.',
        })
      } else {
        const key = rawName.toLowerCase()
        const existing = seenNames.get(key)
        if (existing) {
          issues.push({
            fieldId,
            fieldName,
            message: `Duplicate enabled field name "${rawName}" (already used by "${existing}").`,
          })
        } else {
          seenNames.set(key, rawName)
        }
      }

      if (mode === 'value') {
        const expression = String(item.expression || '').trim()
        if (!expression) {
          issues.push({
            fieldId,
            fieldName,
            message: 'Expression is empty.',
          })
          return
        }
        const expressionIssues = validateExpressionSyntax(expression, 0, { requireEqualsPrefix: true })
          .filter((issue) => issue.severity === 'error')
        const seenMessages = new Set<string>()
        expressionIssues.forEach((issue) => {
          const message = String(issue.message || '').trim()
          if (!message || seenMessages.has(message)) return
          seenMessages.add(message)
          issues.push({
            fieldId,
            fieldName,
            message: message,
          })
        })
        return
      }

      const template = String(item.jsonTemplate || '').trim()
      if (!template) {
        issues.push({
          fieldId,
          fieldName,
          message: 'JSON template is empty.',
        })
        return
      }

      const jsonIssues = validateJsonTemplateSyntax(template).filter((issue) => issue.severity === 'error')
      const exprIssues = extractExpressionStringRanges(template)
        .flatMap((entry) => validateExpressionSyntax(entry.value, entry.startOffset, { requireEqualsPrefix: false }))
        .filter((issue) => issue.severity === 'error')
      const combined = [...jsonIssues, ...exprIssues]
      const seenMessages = new Set<string>()
      combined.forEach((issue) => {
        const message = String(issue.message || '').trim()
        if (!message || seenMessages.has(message)) return
        seenMessages.add(message)
        issues.push({
          fieldId,
          fieldName,
          message: message,
        })
      })
    })

    return issues
  }

  const saveCustomFieldStudio = () => {
    const enabledFieldSaveIssues = collectEnabledCustomFieldSaveIssues(customFieldDraft)
    if (enabledFieldSaveIssues.length > 0) {
      const firstIssue = enabledFieldSaveIssues[0]
      if (firstIssue?.fieldId) {
        setCollapsedCustomFieldIds((prev) => prev.filter((id) => id !== firstIssue.fieldId))
      }
      const description = enabledFieldSaveIssues
        .slice(0, 6)
        .map((issue, idx) => `${idx + 1}. ${issue.fieldName}: ${issue.message}`)
        .join('\n')
      notification.error({
        message: 'Save blocked: enabled custom fields have errors',
        description,
        placement: 'bottomRight',
        duration: 5,
      })
      return
    }

    const normalized = serializeCustomFieldSpecs(customFieldDraft)
    const normalizedIds = normalized
      .map((item) => String(item.id || '').trim())
      .filter(Boolean)
    const collapsedIds = collapsedCustomFieldIds.filter((id) => normalizedIds.includes(id))
    const primaryKeyField = String(customPrimaryKeyFieldDraft || '').trim()
    const profileWindowDays = String(customProfileWindowDaysDraft || '').trim() || '1,7,30'
    const profileRetentionDays = Number.isFinite(customProfileRetentionDaysDraft) && customProfileRetentionDaysDraft > 0
      ? Math.floor(customProfileRetentionDaysDraft)
      : 45
    const validationLimit = Math.max(1, Math.min(Number(validationLmdbLimitDraft || 50) || 50, 200))
    const appendUniqueCacheModeDraft = (
      customProfileStorageDraft === 'rocksdb' && customProfileProcessingModeDraft === 'incremental'
        ? 'persistent'
        : 'auto'
    )
    updateNodeConfig(selectedNodeId!, {
      custom_fields: normalized,
      custom_include_source_fields: customIncludeSourceDraft,
      custom_expression_engine: customExpressionEngineDraft,
      custom_primary_key_field: primaryKeyField,
      custom_group_by_field: primaryKeyField,
      custom_profile_enabled: customProfileEnabledDraft,
      custom_profile_storage: customProfileStorageDraft,
      custom_profile_oracle_host: String(customProfileOracleHostDraft || '').trim(),
      custom_profile_oracle_port: customProfileOraclePortDraft,
      custom_profile_oracle_service_name: String(customProfileOracleServiceNameDraft || '').trim(),
      custom_profile_oracle_sid: String(customProfileOracleSidDraft || '').trim(),
      custom_profile_oracle_user: String(customProfileOracleUserDraft || '').trim(),
      custom_profile_oracle_password: String(customProfileOraclePasswordDraft || '').trim(),
      custom_profile_oracle_dsn: String(customProfileOracleDsnDraft || '').trim(),
      custom_profile_oracle_table: String(customProfileOracleTableDraft || '').trim() || 'ETL_PROFILE_STATE',
      custom_profile_oracle_write_strategy: customProfileOracleWriteStrategyDraft,
      custom_profile_oracle_parallel_workers: customProfileOracleParallelWorkersDraft,
      custom_profile_oracle_parallel_min_tokens: customProfileOracleParallelMinTokensDraft,
      custom_profile_oracle_merge_batch_size: customProfileOracleMergeBatchSizeDraft,
      custom_profile_oracle_queue_enabled: customProfileOracleQueueEnabledDraft,
      custom_profile_oracle_queue_wait_on_force_flush: customProfileOracleQueueWaitOnForceFlushDraft,
      custom_profile_processing_mode: customProfileProcessingModeDraft,
      custom_profile_compute_strategy: customProfileComputeStrategyDraft,
      custom_profile_compute_executor: customProfileComputeExecutorDraft,
      custom_profile_compute_workers: customProfileComputeWorkersDraft,
      custom_profile_compute_min_rows: customProfileComputeMinRowsDraft,
      custom_profile_compute_global_prefetch_max_tokens: customProfileComputeGlobalPrefetchMaxTokensDraft,
      custom_profile_backfill_candidate_prefetch_chunk_size: customProfileBackfillCandidatePrefetchChunkSizeDraft,
      custom_profile_emit_mode: customProfileEmitModeDraft,
      custom_profile_required_fields: customProfileRequiredFieldsDraft.join(', '),
      custom_profile_event_time_field: customProfileEventTimeFieldDraft,
      custom_profile_window_days: profileWindowDays,
      custom_profile_retention_days: profileRetentionDays,
      custom_profile_include_change_fields: customProfileIncludeChangeFieldsDraft,
      custom_profile_live_persist: customProfileLivePersistDraft,
      custom_profile_flush_every_rows: customProfileFlushEveryRowsDraft,
      custom_profile_flush_interval_seconds: customProfileFlushIntervalSecondsDraft,
      custom_profile_append_unique_cache_mode: appendUniqueCacheModeDraft,
      custom_profile_append_unique_cache_disable_min_rows: customProfileAppendUniqueDisableMinRowsDraft,
      custom_profile_append_unique_cache_entity_limit: customProfileAppendUniqueEntityLimitDraft,
      custom_profile_append_unique_cache_disable_ratio: customProfileAppendUniqueDisableRatioDraft,
      custom_validation_source: validationSourceDraft,
      custom_validation_lmdb_env_path: String(validationLmdbEnvPathDraft || '').trim(),
      custom_validation_lmdb_db_name: String(validationLmdbDbNameDraft || '').trim(),
      custom_validation_lmdb_key_prefix: String(validationLmdbKeyPrefixDraft || '').trim(),
      custom_validation_lmdb_start_key: String(validationLmdbStartKeyDraft || '').trim(),
      custom_validation_lmdb_end_key: String(validationLmdbEndKeyDraft || '').trim(),
      custom_validation_lmdb_key_contains: String(validationLmdbKeyContainsDraft || '').trim(),
      custom_validation_lmdb_limit: validationLimit,
      custom_validation_rocksdb_env_path: String(validationLmdbEnvPathDraft || '').trim(),
      custom_collapsed_field_ids: collapsedIds,
    })
    if (normalized.length > 0) {
      const outputFields = uniqueFieldNames(
        [
          ...normalized
            .filter((item) => Boolean(item.enabled))
            .map((item) => String(item.name || '').trim())
            .filter(Boolean),
          ...(primaryKeyField ? [primaryKeyField] : []),
        ]
      )
      if (outputFields.length > 0) {
        updateNodeConfig(selectedNodeId!, { fields: outputFields.join(', ') })
      }
    }
    setCustomFieldStudioOpen(false)
    notification.success({
      message: 'Custom fields saved',
      description: `${normalized.length} custom field${normalized.length === 1 ? '' : 's'} configured.`,
      placement: 'bottomRight',
      duration: 2,
    })
  }

  const openOracleStudio = () => {
    setOracleMappingDraft(
      configuredOracleMappings.length > 0
        ? configuredOracleMappings.map((item) => createOracleColumnMappingSpec(item))
        : []
    )
    setOracleOnlyMappedDraft(oracleOnlyMappedConfigured)
    setOraclePreSqlDraft(oracleConfiguredPreSql)
    setOraclePostSqlDraft(oracleConfiguredPostSql)
    setOracleTableDraft(oracleConfiguredTable)
    setOracleIfExistsDraft(oracleConfiguredIfExists)
    setOracleOperationDraft(oracleConfiguredOperation)
    setOracleKeyColumnsDraft(oracleConfiguredKeyColumns)
    setOracleStudioOpen(true)
  }

  const addOracleMappingDraft = (seed?: Partial<OracleColumnMappingSpec>) => {
    setOracleMappingDraft((prev) => [...prev, createOracleColumnMappingSpec(seed)])
  }

  const updateOracleMappingDraft = (id: string, patch: Partial<OracleColumnMappingSpec>) => {
    setOracleMappingDraft((prev) => prev.map((row) => (row.id === id ? { ...row, ...patch } : row)))
  }

  const removeOracleMappingDraft = (id: string) => {
    setOracleMappingDraft((prev) => prev.filter((row) => row.id !== id))
  }

  const autoMapOracleFromSource = () => {
    setOracleMappingDraft((prev) => {
      const bySource = new Set(prev.map((item) => String(item.source || '').trim()).filter(Boolean))
      const next = [...prev]
      oracleUpstreamFieldOptions.forEach((fieldName) => {
        if (bySource.has(fieldName)) return
        next.push(createOracleColumnMappingSpec({ source: fieldName, destination: fieldName, enabled: true }))
      })
      return next
    })
  }

  const saveOracleStudio = () => {
    const tableName = String(oracleTableDraft || '').trim()
    if (!tableName) {
      notification.error({
        message: 'Target table is required',
        description: 'Set Oracle target table in Oracle Destination Studio before saving.',
        placement: 'bottomRight',
      })
      return
    }
    const keyColumns = uniqueFieldNames((oracleKeyColumnsDraft || []).map((v) => String(v || '').trim()).filter(Boolean))
    if ((oracleOperationDraft === 'update' || oracleOperationDraft === 'upsert') && keyColumns.length === 0) {
      notification.error({
        message: 'Key columns are required',
        description: `Choose at least one key column for ${oracleOperationDraft.toUpperCase()} operation.`,
        placement: 'bottomRight',
      })
      return
    }
    const normalized = serializeOracleColumnMappings(oracleMappingDraft)
    updateNodeConfig(selectedNodeId!, {
      table: tableName,
      if_exists: oracleIfExistsDraft,
      oracle_operation: oracleOperationDraft,
      oracle_key_columns: keyColumns,
      oracle_column_mappings: normalized,
      oracle_only_mapped_columns: oracleOnlyMappedDraft,
      oracle_pre_sql: oraclePreSqlDraft,
      oracle_post_sql: oraclePostSqlDraft,
    })
    setOracleStudioOpen(false)
    notification.success({
      message: 'Oracle destination config saved',
      description: `${oracleOperationDraft.toUpperCase()} configured for table ${tableName}. ${normalized.length} mapping${normalized.length === 1 ? '' : 's'} configured.`,
      placement: 'bottomRight',
      duration: 2,
    })
  }

  const lmdbDraftToConfig = (draft: LmdbStudioDraft): Record<string, unknown> => ({
    env_path: String(draft.env_path || '').trim(),
    db_name: String(draft.db_name || '').trim(),
    key_prefix: String(draft.key_prefix || '').trim(),
    start_key: String(draft.start_key || '').trim(),
    end_key: String(draft.end_key || '').trim(),
    key_contains: String(draft.key_contains || '').trim(),
    value_contains: String(draft.value_contains || '').trim(),
    value_format: draft.value_format || 'auto',
    flatten_json_values: Boolean(draft.flatten_json_values),
    expand_profile_documents: Boolean(draft.expand_profile_documents),
    limit: Math.max(0, Number.isFinite(Number(draft.limit)) ? Math.floor(Number(draft.limit)) : 1000),
  })

  const loadLmdbEnvPathOptions = async (
    basePathOverride?: string,
    sourceOverride?: 'lmdb' | 'rocksdb',
  ) => {
    setLmdbPathBrowseLoading(true)
    setLmdbPathBrowseError(null)
    try {
      const sourceType = sourceOverride || (isRocksdbSource ? 'rocksdb' : 'lmdb')
      const sourceLabel = sourceType === 'rocksdb' ? 'RocksDB' : 'LMDB'
      const response = sourceType === 'rocksdb'
        ? await api.detectRocksdbEnvPathOptions({
          base_path: String(basePathOverride || lmdbStudioDraft.env_path || lmdbConfigured.env_path || '').trim(),
          max_depth: 5,
          limit: 800,
        })
        : await api.detectLmdbEnvPathOptions({
        base_path: String(basePathOverride || lmdbStudioDraft.env_path || lmdbConfigured.env_path || '').trim(),
        max_depth: 5,
        limit: 800,
      })
      const paths = Array.isArray(response?.paths)
        ? response.paths.map((item: unknown) => String(item || '').trim()).filter(Boolean)
        : []
      setLmdbDiscoveredPaths(uniqueFieldNames(paths))
      if (paths.length === 0) {
        notification.info({
          message: `No ${sourceLabel} paths found`,
          description: sourceType === 'rocksdb'
            ? 'No folders with RocksDB CURRENT file were discovered in browse roots.'
            : 'No folders with data.mdb were discovered in browse roots.',
          placement: 'bottomRight',
          duration: 2.5,
        })
      } else {
        notification.success({
          message: `${sourceLabel} paths loaded`,
          description: `${paths.length} path(s) discovered.`,
          placement: 'bottomRight',
          duration: 2,
        })
      }
    } catch (err: any) {
      const sourceLabel = sourceOverride === 'rocksdb' ? 'RocksDB' : kvSourceLabel
      const msg = String(err?.message || `Failed to fetch ${sourceLabel} path options`)
      setLmdbPathBrowseError(msg)
      notification.error({
        message: `${sourceLabel} browser failed`,
        description: msg,
        placement: 'bottomRight',
      })
    } finally {
      setLmdbPathBrowseLoading(false)
    }
  }

  const openLmdbStudio = () => {
    const draft = createLmdbStudioDraft(lmdbConfigured)
    setLmdbStudioDraft(draft)
    setLmdbStudioError(null)
    setLmdbPathBrowseError(null)
    setLmdbPreviewView('table')
    setLmdbPreviewDisplayLimit(200)
    setLmdbTablePageSize(25)
    setLmdbVisibleColumns([])
    setLmdbJsonEditorOpen(false)
    setLmdbJsonEditorError(null)
    setLmdbJsonTagFilter('')
    setLmdbStudioPreviewRows([])
    setLmdbStudioColumns([])
    setLmdbStudioRowCount(0)
    setLmdbSummaryRemote(null)
    setLmdbSummaryLoading(false)
    setLmdbSummaryError(null)
    setLmdbStudioOpen(true)
    setLmdbTableCurrentPage(1)
    setLmdbPreviewHasMore(false)
    setLmdbGlobalFilterColumn(LMDB_GLOBAL_ALL_COLUMNS)
    setLmdbGlobalFilterValues([])
    setLmdbColumnFilterValues({})
  }

  const fetchLmdbSummary = async (requestConfig: Record<string, unknown>) => {
    setLmdbSummaryLoading(true)
    setLmdbSummaryError(null)
    try {
      const summaryResponse = isRocksdbSource
        ? await api.getRocksdbSummary(requestConfig, 0)
        : await api.getLmdbSummary(requestConfig, 0)
      if (summaryResponse && typeof summaryResponse === 'object') {
        setLmdbSummaryRemote(summaryResponse as Record<string, unknown>)
      }
    } catch (summaryErr: any) {
      const summaryMsg = String(summaryErr?.message || `Failed to fetch ${kvSourceLabel} summary`)
      setLmdbSummaryRemote(null)
      setLmdbSummaryError(summaryMsg)
    } finally {
      setLmdbSummaryLoading(false)
    }
  }

  const runLmdbPreview = async (options?: {
    draftOverride?: LmdbStudioDraft
    page?: number
    pageSize?: number
    quiet?: boolean
    globalFilterColumnOverride?: string
    globalFilterValuesOverride?: string[]
    columnFiltersOverride?: Record<string, string[]>
  }) => {
    const draft = options?.draftOverride || lmdbStudioDraft
    const page = Math.max(1, Math.floor(Number(options?.page ?? lmdbTableCurrentPage ?? 1)))
    const pageSize = Math.max(5, Math.min(Number.isFinite(Number(options?.pageSize))
      ? Math.floor(Number(options?.pageSize))
      : Number(lmdbTablePageSize || 25), 500))
    const quiet = Boolean(options?.quiet)
    const globalFilterColumn = String(
      options?.globalFilterColumnOverride ?? lmdbGlobalFilterColumn ?? LMDB_GLOBAL_ALL_COLUMNS
    )
    const globalFilterValues = uniqueFieldNames(
      (options?.globalFilterValuesOverride ?? lmdbGlobalFilterValues ?? [])
        .map((value) => String(value || '').trim())
        .filter(Boolean)
    )
    const columnFiltersRaw = options?.columnFiltersOverride ?? lmdbColumnFilterValues
    const columnFilters: Record<string, string[]> = {}
    Object.entries(columnFiltersRaw || {}).forEach(([name, values]) => {
      const col = String(name || '').trim()
      if (!col || !Array.isArray(values)) return
      const normalized = uniqueFieldNames(values.map((v) => String(v || '').trim()).filter(Boolean))
      if (normalized.length > 0) columnFilters[col] = normalized
    })
    const envPath = String(draft.env_path || '').trim()
    const shouldRefreshSummary = Boolean(
      options?.draftOverride
      || options?.globalFilterColumnOverride !== undefined
      || options?.globalFilterValuesOverride !== undefined
      || options?.columnFiltersOverride !== undefined
      || page === 1
      || !lmdbSummaryRemote
    )
    if (!envPath) {
      const msg = `${kvSourceLabel} environment path is required.`
      setLmdbStudioError(msg)
      notification.error({ message: `${kvSourceLabel} query failed`, description: msg, placement: 'bottomRight' })
      return
    }
    setLmdbStudioError(null)
    setLmdbStudioLoading(true)
    try {
      const configuredLimit = Number.isFinite(Number(draft.limit)) ? Math.floor(Number(draft.limit)) : 1000
      const previewFetchLimit = configuredLimit <= 0 ? 2_147_483_647 : Math.max(1, configuredLimit)
      const lmdbSchemaScanRows = Math.max(previewFetchLimit, 5000)
      const requestConfig: Record<string, unknown> = {
        ...lmdbDraftToConfig(draft),
        global_filter_column: globalFilterColumn,
        global_filter_values: globalFilterValues,
      }
      if (Object.keys(columnFilters).length > 0) {
        requestConfig.column_filters = columnFilters
      }
      const response = await api.detectSourceFieldOptions(kvSourceType, requestConfig, lmdbSchemaScanRows, {
        page,
        previewRows: pageSize,
        includeSchemaScan: false,
        schemaScanLimit: 250,
        previewCompact: true,
        previewMaxCellChars: 2000,
        previewMaxCollectionItems: 64,
      })
      const previewRows = Array.isArray(response?.preview) ? response.preview : []
      const columns = Array.isArray(response?.columns) ? response.columns.map((v: unknown) => String(v)) : []
      const hasMore = Boolean(response?.has_more)
      const rowCountRaw = Number(response?.row_count || 0)
      const pageBase = Math.max(0, (page - 1) * pageSize)
      const fallbackRowCount = pageBase + previewRows.length + (hasMore ? 1 : 0)
      const rowCount = Number.isFinite(rowCountRaw) && rowCountRaw > 0 ? rowCountRaw : fallbackRowCount

      setLmdbStudioPreviewRows(previewRows)
      setLmdbStudioColumns((prev) => uniqueFieldNames([...(prev || []), ...columns]))
      setLmdbStudioRowCount(Number.isFinite(rowCount) ? rowCount : previewRows.length)
      setLmdbTableCurrentPage(page)
      setLmdbTablePageSize(pageSize)
      setLmdbPreviewHasMore(hasMore)

      if (shouldRefreshSummary) {
        await fetchLmdbSummary(requestConfig)
      }

      if (selectedNodeId) {
        const patch: Record<string, unknown> = {
          ...lmdbDraftToConfig(draft),
          _detected_columns: columns.join(', '),
          _row_count: Number.isFinite(rowCount) ? rowCount : previewRows.length,
          _preview_page: page,
          _preview_page_size: pageSize,
          _preview_has_more: hasMore,
        }
        if (previewRows.length > 0) patch._preview_rows = previewRows
        updateNodeConfigSilent(selectedNodeId, patch)
      }
      if (!quiet) {
        notification.success({
          message: `${kvSourceLabel} query completed`,
          description: `Page ${page} loaded with ${previewRows.length.toLocaleString()} row(s).`,
          placement: 'bottomRight',
          duration: 2,
        })
      }
    } catch (err: any) {
      const msg = String(err?.message || `Failed to query ${kvSourceLabel} source`)
      setLmdbStudioError(msg)
      notification.error({
        message: `${kvSourceLabel} query failed`,
        description: msg,
        placement: 'bottomRight',
      })
    } finally {
      setLmdbStudioLoading(false)
    }
  }

  const saveLmdbJsonEditor = () => {
    const rowIndex = lmdbJsonEditorRowIndex
    if (rowIndex === null || rowIndex < 0) {
      setLmdbJsonEditorError('Row index is not selected.')
      return
    }

    let parsed: unknown
    try {
      parsed = JSON.parse(String(lmdbJsonEditorText || '').trim() || 'null')
    } catch {
      setLmdbJsonEditorError('Invalid JSON. Correct JSON syntax and retry.')
      return
    }

    if (lmdbJsonEditorMode === 'row' && (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed))) {
      setLmdbJsonEditorError('Row JSON must be an object.')
      return
    }

    setLmdbStudioPreviewRows((prev) => {
      if (!Array.isArray(prev) || rowIndex >= prev.length) return prev
      const next = [...prev]
      if (lmdbJsonEditorMode === 'row') {
        next[rowIndex] = parsed as Record<string, unknown>
      } else {
        const fieldName = String(lmdbJsonEditorField || '').trim()
        if (!fieldName) return prev
        const currentRow = next[rowIndex]
        const baseRow = (currentRow && typeof currentRow === 'object' && !Array.isArray(currentRow))
          ? { ...(currentRow as Record<string, unknown>) }
          : {}
        baseRow[fieldName] = parsed
        next[rowIndex] = baseRow
      }
      return next
    })

    setLmdbJsonEditorError(null)
    setLmdbJsonEditorOpen(false)
    setLmdbJsonTagFilter('')
    notification.success({
      message: 'Preview JSON updated',
      description: lmdbJsonEditorMode === 'row'
        ? 'Row JSON was updated in preview.'
        : `Cell JSON for "${lmdbJsonEditorField}" was updated in preview.`,
      placement: 'bottomRight',
      duration: 1.8,
    })
  }

  function openLmdbCellJsonEditor(
    rowIndex: number | null,
    fieldName: string,
    value: unknown,
  ) {
    const text = toPreviewJsonText(value)
    setLmdbJsonEditorMode('cell')
    setLmdbJsonEditorField(String(fieldName || ''))
    setLmdbJsonEditorRowIndex(rowIndex)
    setLmdbJsonEditorTitle(`Edit Cell JSON — ${fieldName}`)
    setLmdbJsonEditorText(text)
    setLmdbJsonEditorOriginalText(text)
    setLmdbJsonEditorError(null)
    setLmdbJsonTagFilter('')
    setLmdbJsonEditorOpen(true)
  }

  function openLmdbRowJsonEditor(
    rowIndex: number | null,
    rowPayload: Record<string, unknown>,
  ) {
    const text = toPreviewJsonText(rowPayload)
    setLmdbJsonEditorMode('row')
    setLmdbJsonEditorField('')
    setLmdbJsonEditorRowIndex(rowIndex)
    setLmdbJsonEditorTitle('Edit Full Row JSON')
    setLmdbJsonEditorText(text)
    setLmdbJsonEditorOriginalText(text)
    setLmdbJsonEditorError(null)
    setLmdbJsonTagFilter('')
    setLmdbJsonEditorOpen(true)
  }

  function formatLmdbJsonEditor() {
    try {
      const parsed = JSON.parse(String(lmdbJsonEditorText || '').trim() || 'null')
      setLmdbJsonEditorText(JSON.stringify(parsed, null, 2))
      setLmdbJsonEditorError(null)
    } catch {
      setLmdbJsonEditorError('Invalid JSON. Unable to format.')
    }
  }

  function minifyLmdbJsonEditor() {
    try {
      const parsed = JSON.parse(String(lmdbJsonEditorText || '').trim() || 'null')
      setLmdbJsonEditorText(JSON.stringify(parsed))
      setLmdbJsonEditorError(null)
    } catch {
      setLmdbJsonEditorError('Invalid JSON. Unable to minify.')
    }
  }

  function resetLmdbJsonEditor() {
    setLmdbJsonEditorText(lmdbJsonEditorOriginalText)
    setLmdbJsonEditorError(null)
  }

  async function copyLmdbJsonSnippet(snippet: string, label: string) {
    const text = String(snippet || '')
    if (!text) return
    let copied = false
    try {
      if (typeof navigator !== 'undefined' && navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text)
        copied = true
      }
    } catch {
      copied = false
    }
    if (!copied) {
      try {
        if (typeof document !== 'undefined') {
          const el = document.createElement('textarea')
          el.value = text
          el.style.position = 'fixed'
          el.style.left = '-9999px'
          document.body.appendChild(el)
          el.focus()
          el.select()
          copied = document.execCommand('copy')
          document.body.removeChild(el)
        }
      } catch {
        copied = false
      }
    }
    if (copied) {
      notification.success({
        message: 'Copied',
        description: `${label} copied to clipboard.`,
        placement: 'bottomRight',
        duration: 1.5,
      })
    } else {
      notification.error({
        message: 'Copy failed',
        description: 'Clipboard is unavailable in this browser context.',
        placement: 'bottomRight',
      })
    }
  }

  const deleteLmdbData = async (mode: 'filtered' | 'all') => {
    const draft = lmdbStudioDraft
    const envPath = String(draft.env_path || '').trim()
    if (!envPath) {
      const msg = `${kvSourceLabel} environment path is required.`
      setLmdbStudioError(msg)
      notification.error({ message: `${kvSourceLabel} delete failed`, description: msg, placement: 'bottomRight' })
      return
    }
    if (mode === 'filtered') {
      const hasCustomFilter = Boolean(
        String(draft.key_prefix || '').trim()
        || String(draft.start_key || '').trim()
        || String(draft.end_key || '').trim()
        || String(draft.key_contains || '').trim()
      )
      if (!hasCustomFilter) {
        const msg = 'Set at least one filter (prefix/start/end/key contains) for custom delete.'
        setLmdbStudioError(msg)
        notification.error({ message: `${kvSourceLabel} delete failed`, description: msg, placement: 'bottomRight' })
        return
      }
    }

    setLmdbDeleteLoading(true)
    setLmdbStudioError(null)
    try {
      const response = isRocksdbSource
        ? await api.deleteRocksdbData({
          env_path: envPath,
          delete_mode: mode,
          key_prefix: String(draft.key_prefix || '').trim(),
          start_key: String(draft.start_key || '').trim(),
          end_key: String(draft.end_key || '').trim(),
          key_contains: String(draft.key_contains || '').trim(),
          limit: Math.max(0, Number.isFinite(Number(draft.limit)) ? Math.floor(Number(draft.limit)) : 1000),
        })
        : await api.deleteLmdbData({
        env_path: envPath,
        db_name: String(draft.db_name || '').trim(),
        delete_mode: mode,
        key_prefix: String(draft.key_prefix || '').trim(),
        start_key: String(draft.start_key || '').trim(),
        end_key: String(draft.end_key || '').trim(),
        key_contains: String(draft.key_contains || '').trim(),
        limit: Math.max(0, Number.isFinite(Number(draft.limit)) ? Math.floor(Number(draft.limit)) : 1000),
      })

      const deleted = Number(response?.deleted_keys || 0)
      const matched = Number(response?.matched_keys || 0)
      notification.success({
        message: mode === 'all' ? `${kvSourceLabel} cleared` : `${kvSourceLabel} filtered delete completed`,
        description: `${deleted.toLocaleString()} key(s) deleted${mode === 'filtered' ? ` (matched: ${matched.toLocaleString()})` : ''}.`,
        placement: 'bottomRight',
        duration: 3,
      })

      await runLmdbPreview({
        draftOverride: draft,
        page: 1,
      })
    } catch (err: any) {
      const msg = String(err?.message || `Failed to delete ${kvSourceLabel} data`)
      setLmdbStudioError(msg)
      notification.error({
        message: `${kvSourceLabel} delete failed`,
        description: msg,
        placement: 'bottomRight',
      })
    } finally {
      setLmdbDeleteLoading(false)
    }
  }

  const confirmDeleteLmdbData = (mode: 'filtered' | 'all') => {
    const title = mode === 'all'
      ? `Delete All ${kvSourceLabel} Data?`
      : `Delete Filtered ${kvSourceLabel} Data?`
    const content = mode === 'all'
      ? `This will delete all keys from the selected ${kvSourceLabel} database. This cannot be undone.`
      : 'This will delete keys matching current filters (prefix/range/contains). This cannot be undone.'
    Modal.confirm({
      title,
      content,
      centered: true,
      okText: mode === 'all' ? 'Delete All' : 'Delete Matching',
      okButtonProps: { danger: true },
      onOk: async () => {
        await deleteLmdbData(mode)
      },
    })
  }

  const saveLmdbStudio = () => {
    const envPath = String(lmdbStudioDraft.env_path || '').trim()
    if (!envPath) {
      notification.error({
        message: `${kvSourceLabel} path is required`,
        description: `Set ${kvSourceLabel} path directly from backend filesystem.`,
        placement: 'bottomRight',
      })
      return
    }
    if (selectedNodeId) {
      updateNodeConfig(selectedNodeId, lmdbDraftToConfig(lmdbStudioDraft))
    }
    setLmdbStudioOpen(false)
    notification.success({
      message: `${kvSourceLabel} source config saved`,
      description: `${kvSourceLabel} source node is ready for execution.`,
      placement: 'bottomRight',
      duration: 2,
    })
  }

  const commonInputStyle = {
    background: 'var(--app-input-bg)',
    border: '1px solid var(--app-border-strong)',
    color: 'var(--app-text)',
  }

  const renderField = (field: ConfigField) => {
    const val = nodeConfig[field.name]

    // ── File source picker ────────────────────────────────────────────
    if (field.name === 'file_path' && isFileSource) {
      const pickerNodeId = selectedNodeId
      return (
        <SourceFilePicker
          key={`source-file-picker-${pickerNodeId || 'none'}`}
          nodeId={pickerNodeId || undefined}
          value={val as string}
          fileType={fileType as any}
          placeholder={`Click to browse ${fileType.toUpperCase()} file…`}
          onChange={(path, fileInfo) => {
            if (!pickerNodeId) return
            const patch: Record<string, unknown> = { file_path: path }
            if (fileInfo) {
              // Auto-populate columns hint if available
              if (fileInfo.columns.length > 0) {
                patch._detected_columns = fileInfo.columns.join(', ')
                patch._row_count = fileInfo.rows
              }
              if (Array.isArray(fileInfo.preview) && fileInfo.preview.length > 0) {
                patch._preview_rows = fileInfo.preview
              }
              if (nodeType === 'csv_source' && fileInfo.detectedEncoding) {
                patch.encoding = fileInfo.detectedEncoding
              }
              if (Array.isArray(fileInfo.jsonPaths) && fileInfo.jsonPaths.length > 0) {
                patch._json_paths = fileInfo.jsonPaths
                if (fileInfo.suggestedJsonPath) {
                  patch._suggested_json_path = fileInfo.suggestedJsonPath
                  if (nodeType === 'json_source' && !String(nodeConfig.json_path || '').trim()) {
                    patch.json_path = fileInfo.suggestedJsonPath
                  }
                }
              }
            }
            updateNodeConfig(pickerNodeId, patch)
          }}
        />
      )
    }

    // ── File destination path picker ──────────────────────────────────
    if (field.name === 'file_path' && isFileDest) {
      const pickerNodeId = selectedNodeId
      return (
        <DestinationPathPicker
          key={`destination-file-picker-${pickerNodeId || 'none'}`}
          value={val as string}
          fileType={fileType as any}
          placeholder={`/output/result.${fileType}`}
          nodeId={pickerNodeId || undefined}
          onChange={(path) => {
            if (!pickerNodeId) return
            updateNodeConfig(pickerNodeId, { file_path: path })
          }}
        />
      )
    }

    switch (field.type) {
      case 'text':
        if (isLmdbSource && field.name === 'env_path') {
          return (
            <Space direction="vertical" size={8} style={{ width: '100%' }}>
              <Space.Compact style={{ width: '100%' }}>
                <Input
                  placeholder={field.placeholder || kvPathPlaceholder}
                  value={String(val || '')}
                  onChange={(e) => handleFieldChange(field.name, e.target.value)}
                  style={{ ...commonInputStyle, width: '100%' }}
                />
                <Button
                  size="small"
                  loading={lmdbPathBrowseLoading}
                  onClick={() => { void loadLmdbEnvPathOptions(String(val || '')) }}
                >
                  Browse
                </Button>
              </Space.Compact>
              <Select
                size="small"
                showSearch
                allowClear
                placeholder={`Select discovered ${kvSourceLabel} path`}
                options={lmdbEnvPathPresetOptions}
                onChange={(path) => {
                  if (typeof path !== 'string' || !path) return
                  handleFieldChange(field.name, path)
                }}
                style={{ width: '100%' }}
              />
              <Space size={8} wrap>
                <Button
                  size="small"
                  onClick={openLmdbStudio}
                  style={{
                    background: '#14b8a61a',
                    border: '1px solid #14b8a640',
                    color: '#14b8a6',
                  }}
                >
                  Open {kvSourceLabel} Studio
                </Button>
                <Button
                  size="small"
                  loading={sourceFieldDetectLoading}
                  onClick={() => { void detectSourceFieldOptions() }}
                >
                  Detect Fields
                </Button>
              </Space>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                Direct backend path only. Supports absolute path, `~`, and env vars.
              </Text>
              {lmdbPathBrowseError ? (
                <Text style={{ color: '#ef4444', fontSize: 11 }}>
                  {lmdbPathBrowseError}
                </Text>
              ) : null}
            </Space>
          )
        }
        if (nodeType === 'join_transform' && (field.name === 'left_key' || field.name === 'right_key')) {
          const options = (field.name === 'left_key' ? joinLeftFieldOptions : joinRightFieldOptions)
            .map((name) => ({ value: name, label: name }))
          if (options.length > 0) {
            return (
              <Space direction="vertical" size={6} style={{ width: '100%' }}>
                <Select
                  showSearch
                  value={String(val || '') || undefined}
                  onChange={(v) => handleFieldChange(field.name, v)}
                  options={options}
                  placeholder={field.placeholder || 'id'}
                  style={{ width: '100%' }}
                />
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  Keys are suggested from selected source branch schema.
                </Text>
              </Space>
            )
          }
        }
        if (nodeType === 'schedule_trigger' && field.name === 'cron') {
          const isCustomSchedule = schedulePresetValue === CUSTOM_SCHEDULE_PRESET
          const displayedCron = isCustomSchedule
            ? scheduleCronValue
            : (SCHEDULE_PRESET_TO_CRON[schedulePresetValue] || scheduleCronValue || DEFAULT_SCHEDULE_CRON)
          return (
            <Space direction="vertical" size={6} style={{ width: '100%' }}>
              <Input
                placeholder={field.placeholder || '0 * * * *'}
                value={displayedCron}
                onChange={e => handleFieldChange(field.name, e.target.value)}
                disabled={!isCustomSchedule}
                style={commonInputStyle}
              />
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                {isCustomSchedule
                  ? 'Custom cron must have 5 fields: minute hour day month weekday.'
                  : 'Cron is auto-filled from the selected schedule pattern.'}
              </Text>
            </Space>
          )
        }
        if (nodeType === 'map_transform' && field.name === 'fields') {
          const selectedFields = parseFieldList(val)
          return (
            <Space direction="vertical" size={6} style={{ width: '100%' }}>
              <Space style={{ justifyContent: 'space-between', width: '100%' }}>
                <Button
                  onMouseDown={(event) => {
                    event.preventDefault()
                    event.stopPropagation()
                  }}
                  onPointerDown={(event) => {
                    event.preventDefault()
                    event.stopPropagation()
                  }}
                  onClick={(event) => {
                    event.preventDefault()
                    event.stopPropagation()
                    openCustomFieldStudio()
                  }}
                  style={{
                    background: '#6366f11a',
                    border: '1px solid #6366f140',
                    color: '#6366f1',
                  }}
                >
                  Open Custom Fields Studio
                </Button>
                {customFieldConfiguredCount > 0 ? (
                  <Tag
                    style={{
                      marginInlineEnd: 0,
                      background: '#22c55e1a',
                      border: '1px solid #22c55e40',
                      color: '#22c55e',
                    }}
                  >
                    {customFieldEnabledConfiguredCount}/{customFieldConfiguredCount} enabled
                  </Tag>
                ) : null}
              </Space>
              <Select
                mode="tags"
                value={selectedFields}
                onChange={(values) => {
                  const normalized = uniqueFieldNames((values || []).map((v) => String(v)))
                  handleFieldChange(field.name, normalized.join(', '))
                }}
                options={mapInputFieldOptions.map((name) => ({ value: name, label: name }))}
                tokenSeparators={[',']}
                placeholder={field.placeholder || 'id, name, email'}
                style={{ width: '100%' }}
              />
              {mapInputFieldOptions.length > 0 ? (
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  Supports nested JSON paths like `customer.address.city`, `items[]`, `items[0].price`.
                </Text>
              ) : (
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  Connect input/source node and detect schema to get field suggestions.
                </Text>
              )}
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                Quick mode keeps selected source fields. Studio mode adds formula-based custom fields and JSON object/array outputs.
              </Text>
            </Space>
          )
        }
        if (field.name === 'json_path') {
          return (
            <Space direction="vertical" size={6} style={{ width: '100%' }}>
              {jsonPathOptions.length > 0 ? (
                <>
                  <Select
                    value={(val as string) || undefined}
                    onChange={(v) => handleFieldChange(field.name, v)}
                    options={jsonPathOptions}
                    showSearch
                    optionFilterProp="label"
                    placeholder="Select JSON object/array path"
                    allowClear
                    style={{ width: '100%' }}
                  />
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                    Pick from detected object/array paths instead of typing.
                  </Text>
                </>
              ) : (
                <Input
                  placeholder={field.placeholder || 'data.items'}
                  defaultValue={val as string ?? field.defaultValue as string ?? ''}
                  onChange={e => handleFieldChange(field.name, e.target.value)}
                  style={commonInputStyle}
                />
              )}

              {isApiJsonNode && (
                <Button
                  size="small"
                  loading={jsonPathDetectLoading}
                  onClick={() => { void detectJsonPathOptions() }}
                  style={{
                    width: 'fit-content',
                    background: 'var(--app-card-bg)',
                    border: '1px solid var(--app-border-strong)',
                    color: 'var(--app-text-muted)',
                  }}
                >
                  Detect Fields From API
                </Button>
              )}

              {jsonPathDetectError && (
                <Text style={{ color: '#ef4444', fontSize: 11 }}>
                  {jsonPathDetectError}
                </Text>
              )}
            </Space>
          )
        }
        return (
          <Input
            placeholder={field.placeholder}
            defaultValue={val as string ?? field.defaultValue as string ?? ''}
            onChange={e => handleFieldChange(field.name, e.target.value)}
            style={commonInputStyle}
          />
        )
      case 'password':
        return (
          <Input.Password
            placeholder={field.placeholder || '••••••••'}
            defaultValue={val as string ?? ''}
            onChange={e => handleFieldChange(field.name, e.target.value)}
            style={commonInputStyle}
          />
        )
      case 'number':
        return (
          <InputNumber
            defaultValue={val as number ?? field.defaultValue as number ?? 0}
            onChange={v => handleFieldChange(field.name, v)}
            style={{ ...commonInputStyle, width: '100%' }}
          />
        )
      case 'select':
        if (nodeType === 'join_transform' && (field.name === 'left_source_node' || field.name === 'right_source_node')) {
          const options = field.name === 'right_source_node'
            ? joinSourceOptions.filter((opt) => opt.value !== joinLeftSource)
            : joinSourceOptions
          return (
            <Space direction="vertical" size={6} style={{ width: '100%' }}>
              <Select
                value={String(val || '') || undefined}
                onChange={v => handleFieldChange(field.name, v)}
                options={options}
                placeholder="Select upstream source"
                style={{ width: '100%' }}
              />
              {joinSources.length < 2 && (
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  Connect two different source branches to this Join node.
                </Text>
              )}
            </Space>
          )
        }
        if (nodeType === 'schedule_trigger' && field.name === 'schedule_preset') {
          return (
            <Select
              value={schedulePresetValue || DEFAULT_SCHEDULE_PRESET}
              onChange={(preset) => {
                const patch: Record<string, unknown> = { schedule_preset: preset }
                if (preset !== CUSTOM_SCHEDULE_PRESET && SCHEDULE_PRESET_TO_CRON[preset]) {
                  patch.cron = SCHEDULE_PRESET_TO_CRON[preset]
                }
                updateNodeConfig(selectedNodeId!, patch)
              }}
              options={field.options}
              style={{ width: '100%' }}
            />
          )
        }
        return (
          <Select
            defaultValue={val as string ?? field.defaultValue as string}
            onChange={v => handleFieldChange(field.name, v)}
            options={field.options}
            style={{ width: '100%' }}
          />
        )
      case 'toggle':
        return (
          <Switch
            defaultChecked={val as boolean ?? field.defaultValue as boolean ?? false}
            onChange={v => handleFieldChange(field.name, v)}
            style={{ background: val ? '#6366f1' : undefined }}
          />
        )
      case 'textarea':
        return (
          <Input.TextArea
            placeholder={field.placeholder}
            defaultValue={val as string ?? field.defaultValue as string ?? ''}
            rows={4}
            onChange={e => handleFieldChange(field.name, e.target.value)}
            style={{ ...commonInputStyle, resize: 'vertical', fontFamily: 'monospace', fontSize: 12 }}
          />
        )
      case 'code':
      case 'json':
        return (
          <div style={{ border: '1px solid var(--app-border-strong)', borderRadius: 6, overflow: 'hidden' }}>
            <Editor
              height={field.language === 'python' ? '200px' : field.language === 'sql' ? '100px' : '120px'}
              language={field.language || (field.type === 'json' ? 'json' : 'plaintext')}
              value={(val as string) ?? (field.defaultValue as string) ?? ''}
              onChange={v => handleFieldChange(field.name, v || '')}
              theme="vs-dark"
              options={{
                minimap: { enabled: false },
                fontSize: 12,
                lineNumbers: 'off',
                scrollBeyondLastLine: false,
                wordWrap: 'on',
                padding: { top: 8, bottom: 8 },
                renderLineHighlight: 'none',
                overviewRulerLanes: 0,
              }}
            />
          </div>
        )
      default:
        return (
          <Input
            defaultValue={val as string ?? ''}
            onChange={e => handleFieldChange(field.name, e.target.value)}
            style={commonInputStyle}
          />
        )
    }
  }

  // Filter out internal fields (prefixed with _)
  if (!data || !definition) {
    return null
  }

  const visibleFields = definition.configFields.filter((f) => {
    if (f.name.startsWith('_')) return false
    if (nodeType === 'oracle_destination' && (f.name === 'table' || f.name === 'if_exists')) {
      return false
    }
    return true
  })
  const canUseCustomFieldStudio = nodeType === 'map_transform'

  return (
    <>
    <Drawer
      open={open}
      onClose={onClose}
      mask={false}
      width={360}
      placement="right"
      closable={false}
      styles={{
        body: { background: 'var(--app-panel-bg)', padding: 0, overflow: 'hidden', display: 'flex', flexDirection: 'column' },
        header: { display: 'none' },
        wrapper: { boxShadow: '-4px 0 20px rgba(0,0,0,0.4)' },
      }}
    >
      <div
        style={{ height: '100%', display: 'flex', flexDirection: 'column' }}
        onMouseDownCapture={touchDrawerActivity}
        onTouchStartCapture={touchDrawerActivity}
        onKeyDownCapture={touchDrawerActivity}
        onWheelCapture={touchDrawerActivity}
      >
      {/* Header */}
      <div style={{
        background: 'var(--app-card-bg)',
        borderBottom: '1px solid var(--app-border-strong)',
        padding: '14px 16px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        flexShrink: 0,
      }}>
        <Space>
          <div style={{
            width: 30, height: 30,
            background: definition.bgColor,
            border: `1px solid ${definition.color}30`,
            borderRadius: 7,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: 15,
          }}>
            {definition.icon}
          </div>
          <div>
            <Text style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 13 }}>{definition.label}</Text>
            <br />
            <Tag style={{
              background: `${definition.color}15`, border: `1px solid ${definition.color}30`,
              color: definition.color, borderRadius: 4, fontSize: 10, padding: '0 5px',
            }}>
              {definition.category}
            </Tag>
            {(isFileSource || isFileDest) && (
              <Tag style={{
                background: '#6366f115', border: '1px solid #6366f130',
                color: '#6366f1', borderRadius: 4, fontSize: 10, padding: '0 5px', marginLeft: 4
              }}>
                {fileType.toUpperCase()}
              </Tag>
            )}
          </div>
        </Space>
        <Space>
          <Tooltip title="Duplicate node">
            <Button type="text" icon={<CopyOutlined />} size="small" style={{ color: 'var(--app-text-muted)' }} onClick={handleDuplicate} />
          </Tooltip>
          <Button type="text" icon={<CloseOutlined />} size="small" style={{ color: 'var(--app-text-subtle)' }} onClick={onClose} />
        </Space>
      </div>

      {/* Tabs */}
      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        size="small"
        style={{ padding: '0 16px', flexShrink: 0 }}
        tabBarStyle={{ marginBottom: 0, borderBottom: '1px solid var(--app-border)' }}
        items={[
          { key: 'config', label: 'Configuration' },
          { key: 'info',   label: 'Info' },
        ]}
      />

      {/* Body */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '16px' }}>
        {activeTab === 'config' && (
          <Form layout="vertical" form={form}>
            {/* Node label */}
            <Form.Item label={<Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Node Label</Text>} style={{ marginBottom: 14 }}>
              <Input
                defaultValue={data.label}
                onChange={handleLabelChange}
                style={commonInputStyle}
              />
            </Form.Item>

            <div
              style={{
                border: '1px solid var(--app-border-strong)',
                borderRadius: 8,
                background: 'var(--app-shell-bg)',
                padding: '8px 10px',
                marginBottom: 12,
              }}
            >
              <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                <div>
                  <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 600 }}>
                    Node Enabled
                  </Text>
                  <br />
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                    Disabled nodes are skipped during execution and pass upstream rows through.
                  </Text>
                </div>
                <Switch
                  checked={nodeEnabled}
                  onChange={(checked) => {
                    if (!selectedNodeId) return
                    updateNodeConfig(selectedNodeId, { node_enabled: checked })
                  }}
                />
              </Space>
            </div>

            {visibleFields.length > 0 ? (
              <Divider style={{ borderColor: 'var(--app-border)', margin: '4px 0 14px' }}>
                <Text style={{ color: 'var(--app-text-dim)', fontSize: 11 }}>Parameters</Text>
              </Divider>
            ) : null}

            {isDatabaseSource ? (
              <div style={{
                background: '#f973160f',
                border: '1px solid #f9731630',
                borderRadius: 8,
                padding: '8px 12px',
                marginBottom: 12,
              }}>
                <Space direction="vertical" size={6} style={{ width: '100%' }}>
                  <Space style={{ justifyContent: 'space-between', width: '100%' }}>
                    <Text style={{ color: '#f97316', fontSize: 11, fontWeight: 600 }}>
                      Database Source Metadata
                    </Text>
                    <Button
                      size="small"
                      loading={sourceFieldDetectLoading}
                      onClick={() => { void detectSourceFieldOptions() }}
                      style={{
                        background: 'var(--app-card-bg)',
                        border: '1px solid var(--app-border-strong)',
                        color: 'var(--app-text-muted)',
                      }}
                    >
                      Detect Fields From Source
                    </Button>
                  </Space>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                    Uses current connection + query to fetch sample rows and populate selectable fields.
                  </Text>
                  {sourceFieldDetectError && (
                    <Text style={{ color: '#ef4444', fontSize: 11 }}>
                      {sourceFieldDetectError}
                    </Text>
                  )}
                </Space>
              </div>
            ) : null}

            {isLmdbSource ? (
              <div style={{
                background: '#14b8a60f',
                border: '1px solid #14b8a640',
                borderRadius: 8,
                padding: '8px 10px',
                marginBottom: 12,
              }}>
                <Space direction="vertical" size={6} style={{ width: '100%' }}>
                  <Text style={{ color: '#14b8a6', fontSize: 11, fontWeight: 700, whiteSpace: 'nowrap' }}>
                    {kvSourceLabel} Source Studio
                  </Text>
                  <Space size={6} wrap>
                    <Button
                      size="small"
                      onClick={openLmdbStudio}
                      style={{
                        background: '#14b8a61a',
                        border: '1px solid #14b8a640',
                        color: '#14b8a6',
                      }}
                    >
                      Open Full Screen
                    </Button>
                  </Space>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                    Set {kvSourceLabel} backend path, apply filters, and preview query output.
                  </Text>
                  <Space size={6} wrap>
                    <Tag style={{ background: '#0ea5e914', border: '1px solid #0ea5e930', color: '#0ea5e9' }}>
                      decode: {lmdbConfigured.value_format}
                    </Tag>
                    <Tag style={{ background: '#f59e0b14', border: '1px solid #f59e0b30', color: '#f59e0b' }}>
                      limit: {Number(lmdbConfigured.limit || 0) || 0}
                    </Tag>
                    {lmdbConfigured.key_prefix ? (
                      <Tag style={{ background: '#22c55e14', border: '1px solid #22c55e30', color: '#22c55e' }}>
                        prefix filter
                      </Tag>
                    ) : null}
                  </Space>
                </Space>
              </div>
            ) : null}

            {nodeType === 'oracle_destination' ? (
              <div style={{
                background: '#f973160f',
                border: '1px solid #f9731630',
                borderRadius: 8,
                padding: '10px 12px',
                marginBottom: 12,
              }}>
                <Space direction="vertical" size={8} style={{ width: '100%' }}>
                  <Space style={{ justifyContent: 'space-between', width: '100%' }}>
                    <Text style={{ color: '#f97316', fontSize: 11, fontWeight: 700 }}>
                      Oracle Destination Studio
                    </Text>
                    <Button
                      size="small"
                      onClick={openOracleStudio}
                      style={{
                        background: '#f973161a',
                        border: '1px solid #f9731640',
                        color: '#f97316',
                      }}
                    >
                      Open Full Screen
                    </Button>
                  </Space>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                    Configure table, operation (insert/update/upsert), source → destination mapping, and SQL scripts.
                  </Text>
                  <Space size={6} wrap>
                    {oracleConfiguredTable ? (
                      <Tag style={{ background: '#0ea5e914', border: '1px solid #0ea5e930', color: '#0ea5e9' }}>
                        table: {oracleConfiguredTable}
                      </Tag>
                    ) : null}
                    <Tag style={{ background: '#f59e0b14', border: '1px solid #f59e0b30', color: '#f59e0b' }}>
                      op: {oracleConfiguredOperation}
                    </Tag>
                    {(oracleConfiguredOperation === 'update' || oracleConfiguredOperation === 'upsert') ? (
                      <Tag style={{ background: '#ec489914', border: '1px solid #ec489930', color: '#ec4899' }}>
                        keys: {oracleConfiguredKeyColumns.length}
                      </Tag>
                    ) : null}
                    <Tag style={{ background: '#f9731614', border: '1px solid #f9731630', color: '#f97316' }}>
                      {oracleMappingConfiguredCount} mappings
                    </Tag>
                    {oracleOnlyMappedConfigured ? (
                      <Tag style={{ background: '#22c55e14', border: '1px solid #22c55e30', color: '#22c55e' }}>
                        mapped columns only
                      </Tag>
                    ) : null}
                    {String(oracleConfiguredPreSql || '').trim() ? (
                      <Tag style={{ background: '#6366f114', border: '1px solid #6366f130', color: '#6366f1' }}>
                        pre-SQL
                      </Tag>
                    ) : null}
                    {String(oracleConfiguredPostSql || '').trim() ? (
                      <Tag style={{ background: '#8b5cf614', border: '1px solid #8b5cf630', color: '#8b5cf6' }}>
                        post-SQL
                      </Tag>
                    ) : null}
                  </Space>
                </Space>
              </div>
            ) : null}

            {/* Auto-detected schema hint for sources */}
            {(isFileSource || isDatabaseSource || isLmdbSource) && !!nodeConfig._detected_columns ? (
              <div style={{
                background: '#22c55e0a', border: '1px solid #22c55e20',
                borderRadius: 8, padding: '8px 12px', marginBottom: 12,
              }}>
                <Text style={{ color: '#22c55e', fontSize: 11, fontWeight: 600 }}>Detected schema</Text>
                <br />
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11, fontFamily: 'monospace' }}>
                  {String(nodeConfig._detected_columns)}
                </Text>
                {!!nodeConfig._row_count ? (
                  <>
                    <br />
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                      {Number(nodeConfig._row_count).toLocaleString()} total rows
                    </Text>
                  </>
                ) : null}
              </div>
            ) : null}

            {nodeType === 'json_source' && jsonPathOptions.length > 0 ? (
              <div style={{
                background: '#6366f10f',
                border: '1px solid #6366f130',
                borderRadius: 8,
                padding: '8px 12px',
                marginBottom: 12,
              }}>
                <Text style={{ color: '#6366f1', fontSize: 11, fontWeight: 600 }}>
                  JSON object/array path options detected
                </Text>
                <br />
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  Use dropdown in <strong>JSON Path</strong> to select nested object/array quickly.
                </Text>
              </div>
            ) : null}

            {visibleFields.map((field) => (
              <Form.Item
                key={field.name}
                label={
                  <Space size={4}>
                    <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>{field.label}</Text>
                    {field.required && <Text style={{ color: '#ef4444', fontSize: 12 }}>*</Text>}
                    {field.description && (
                      <Tooltip title={field.description}>
                        <InfoCircleOutlined style={{ color: 'var(--app-text-dim)', fontSize: 11 }} />
                      </Tooltip>
                    )}
                  </Space>
                }
                style={{ marginBottom: 14 }}
              >
                {renderField(field)}
              </Form.Item>
            ))}

            {visibleFields.length === 0 && (
              <div style={{ textAlign: 'center', padding: '20px 0' }}>
                <Text style={{ color: 'var(--app-text-dim)', fontSize: 12 }}>No configuration required.</Text>
              </div>
            )}
          </Form>
        )}

        {activeTab === 'info' && (
          <Space direction="vertical" size={10} style={{ width: '100%' }}>
            <div style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '12px 16px' }}>
              <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Description</Text>
              <br />
              <Text style={{ color: '#d1d5db', fontSize: 13, marginTop: 4, display: 'block' }}>{definition.description}</Text>
            </div>
            <div style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '12px 16px' }}>
              <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Handles</Text>
              <br />
              <Space style={{ marginTop: 6 }}>
                <Tag style={{ background: 'var(--app-border)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)', fontSize: 11 }}>
                  {definition.inputs} input{definition.inputs !== 1 ? 's' : ''}
                </Tag>
                <Tag style={{ background: 'var(--app-border)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)', fontSize: 11 }}>
                  {definition.outputs} output{definition.outputs !== 1 ? 's' : ''}
                </Tag>
              </Space>
            </div>
            {definition.tags && (
              <div style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8, padding: '12px 16px' }}>
                <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Tags</Text>
                <br />
                <Space wrap style={{ marginTop: 6 }}>
                  {definition.tags.map(tag => (
                    <Tag key={tag} style={{ background: 'var(--app-border)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-subtle)', fontSize: 11, borderRadius: 4 }}>
                      {tag}
                    </Tag>
                  ))}
                </Space>
              </div>
            )}
            {data.status && data.status !== 'idle' && (
              <div style={{
                background: data.status === 'success' ? '#22c55e08' : data.status === 'error' ? '#ef444408' : '#6366f108',
                border: `1px solid ${data.status === 'success' ? '#22c55e25' : data.status === 'error' ? '#ef444425' : '#6366f125'}`,
                borderRadius: 8, padding: '12px 16px',
              }}>
                <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Last Execution</Text>
                <br />
                <Text style={{
                  color: data.status === 'success' ? '#22c55e' : data.status === 'error' ? '#ef4444' : '#6366f1',
                  fontSize: 13, fontWeight: 500,
                }}>
                  {data.status === 'success'
                    ? `✓ ${(data.executionRows || 0).toLocaleString()} rows processed`
                    : data.status === 'error' ? '✗ Execution failed'
                    : '⟳ Running…'}
                </Text>
              </div>
            )}
          </Space>
        )}
      </div>

      <div
        style={{
          flexShrink: 0,
          borderTop: '1px solid var(--app-border-strong)',
          background: 'var(--app-card-bg)',
          padding: '10px 16px 12px',
        }}
      >
        <Space direction="vertical" size={6} style={{ width: '100%' }}>
          <Text style={{ color: '#ef4444', fontSize: 11, fontWeight: 700 }}>Danger Zone</Text>
          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
            Delete this node from pipeline canvas.
          </Text>
          <Button
            danger
            icon={<DeleteOutlined />}
            onClick={handleDelete}
            style={{ width: '100%' }}
          >
            Delete Node
          </Button>
        </Space>
      </div>
      </div>
    </Drawer>
    {canUseCustomFieldStudio && (
      <Modal
        open={customFieldStudioOpen}
        onCancel={() => setCustomFieldStudioOpen(false)}
        footer={null}
        closable={false}
        maskClosable={false}
        centered
        width="96vw"
        styles={{
          content: {
            padding: 0,
            borderRadius: 12,
            overflow: 'hidden',
            border: '1px solid var(--app-border-strong)',
            background: 'var(--app-panel-bg)',
            height: '96vh',
            display: 'flex',
            flexDirection: 'column',
          },
          body: {
            padding: 0,
            flex: 1,
            minHeight: 0,
            display: 'flex',
            flexDirection: 'column',
          },
        }}
      >
        <div
          style={{
            borderBottom: '1px solid var(--app-border-strong)',
            background: 'var(--app-card-bg)',
            padding: '12px 16px',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: 12,
          }}
        >
          <div>
            <Text style={{ color: 'var(--app-text)', fontWeight: 700, fontSize: 16 }}>
              Custom Fields Studio
            </Text>
            <br />
          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
              Build formula fields and nested JSON outputs.
            </Text>
          </div>
          <Space wrap>
            <Popover
              trigger="click"
              placement="bottomRight"
              content={(
                <div style={{ width: 320, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                  <Select
                    size="small"
                    value={customEditorColorProfile}
                    onChange={(value) => setCustomEditorColorProfile(value as CustomEditorColorProfile)}
                    options={CUSTOM_EDITOR_COLOR_PROFILE_OPTIONS}
                    style={{ width: '100%' }}
                  />
                  <Select
                    size="small"
                    value={customEditorFontPreset}
                    onChange={(value) => setCustomEditorFontPreset(value as CustomEditorFontPreset)}
                    options={CUSTOM_EDITOR_FONT_FAMILY_OPTIONS}
                    style={{ width: '100%' }}
                  />
                  <Select
                    size="small"
                    value={customEditorBeautifyStyle}
                    onChange={(value) => setCustomEditorBeautifyStyle(value as CustomBeautifyStyle)}
                    options={CUSTOM_EDITOR_BEAUTIFY_STYLE_OPTIONS}
                    style={{ width: '100%' }}
                  />
                  <Select
                    size="small"
                    value={customEditorFontSize}
                    onChange={(value) => setCustomEditorFontSize(Number(value || 13))}
                    options={[11, 12, 13, 14, 15, 16, 18, 20].map((size) => ({ value: size, label: `Size ${size}` }))}
                    style={{ width: '100%' }}
                  />
                  <Select
                    size="small"
                    value={customEditorLineHeight}
                    onChange={(value) => setCustomEditorLineHeight(Number(value || 22))}
                    options={[18, 20, 22, 24, 26, 28].map((line) => ({ value: line, label: `Line ${line}` }))}
                    style={{ width: '100%' }}
                  />
                  <Space size={4}>
                    <Switch size="small" checked={customEditorWordWrap} onChange={setCustomEditorWordWrap} />
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Wrap</Text>
                  </Space>
                  <Space size={4}>
                    <Switch size="small" checked={customEditorLigatures} onChange={setCustomEditorLigatures} />
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Ligatures</Text>
                  </Space>
                </div>
              )}
            >
              <Button size="small">Editor Style</Button>
            </Popover>
            <Switch
              checked={customIncludeSourceDraft}
              onChange={setCustomIncludeSourceDraft}
            />
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Include source fields in output</Text>
            <Button
              loading={validationLoading}
              onClick={() => {
                void runCustomFieldValidation()
              }}
              style={{ background: '#0ea5e91a', border: '1px solid #0ea5e940', color: '#0284c7' }}
            >
              Test Output Validation
            </Button>
            <Button
              onClick={() => addCustomFieldDraft()}
              style={{ background: '#6366f11a', border: '1px solid #6366f140', color: '#6366f1' }}
            >
              Add Field
            </Button>
            <Button onClick={() => setCustomFieldStudioOpen(false)}>Close</Button>
            <Button
              type="primary"
              onClick={saveCustomFieldStudio}
              style={{ background: 'linear-gradient(135deg, #6366f1, #8b5cf6)', border: 'none' }}
            >
              Save Custom Fields
            </Button>
          </Space>
        </div>

        <div style={{ display: 'flex', flex: 1, minHeight: 0 }}>
          <div
            style={{
              width: '24%',
              minWidth: 290,
              borderRight: '1px solid var(--app-border-strong)',
              padding: 14,
              overflowY: 'auto',
            }}
          >
            <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Field Reference</Text>
            <br />
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
              Select primary/group field and click any field/function to insert into active expression.
            </Text>
            <br />
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
              Group mode: expressions run once per group. Profile mode: expressions run per incoming row and update one document per entity.
            </Text>
            <div
              style={{
                marginTop: 10,
                background: 'var(--app-card-bg)',
                border: '1px solid var(--app-border-strong)',
                borderRadius: 8,
                padding: 10,
              }}
            >
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                Primary Key / Group By Field
              </Text>
              <Select
                allowClear
                showSearch
                value={customPrimaryKeyFieldDraft || undefined}
                onChange={(value) => setCustomPrimaryKeyFieldDraft(String(value || ''))}
                options={referenceFieldOptions.map((field) => ({ value: field, label: field }))}
                placeholder="Select key field (example: state, agencode)"
                style={{ width: '100%', marginTop: 6 }}
              />
            </div>
            <div
              style={{
                marginTop: 10,
                background: 'var(--app-card-bg)',
                border: '1px solid var(--app-border-strong)',
                borderRadius: 8,
                padding: 10,
              }}
            >
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                Expression Engine
              </Text>
              <Select
                value={customExpressionEngineDraft}
                onChange={(value) => setCustomExpressionEngineDraft(value as CustomExpressionEngine)}
                options={[
                  { value: 'auto', label: 'Auto' },
                  { value: 'python', label: 'Python' },
                  { value: 'polars', label: 'Polars' },
                ]}
                style={{ width: '100%', marginTop: 6 }}
              />
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                Auto selects the safest engine. Polars is best for batch/stateless expressions.
              </Text>
            </div>
            <div
              style={{
                marginTop: 10,
                background: 'var(--app-card-bg)',
                border: '1px solid var(--app-border-strong)',
                borderRadius: 8,
                padding: 10,
              }}
            >
              <Space style={{ justifyContent: 'space-between', width: '100%' }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  Profile Document Mode
                </Text>
                <Switch
                  checked={customProfileEnabledDraft}
                  onChange={setCustomProfileEnabledDraft}
                />
              </Space>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                One JSON profile per entity key with incremental updates.
              </Text>
              {customProfileEnabledDraft ? (
                <Space direction="vertical" size={8} style={{ marginTop: 8, width: '100%' }}>
                  <Select
                    value={customProfileStorageDraft}
                    onChange={(value) => {
                      const nextStorage = value as CustomProfileStorage
                      setCustomProfileStorageDraft(nextStorage)
                      if (nextStorage === 'rocksdb' && customProfileProcessingModeDraft === 'incremental') {
                        applyRocksdbIncrementalTuningDefaults(
                          setCustomProfileFlushEveryRowsDraft,
                          setCustomProfileFlushIntervalSecondsDraft,
                          setCustomProfileAppendUniqueDisableMinRowsDraft,
                          setCustomProfileAppendUniqueEntityLimitDraft,
                          setCustomProfileAppendUniqueDisableRatioDraft,
                        )
                      }
                    }}
                    options={[
                      { value: 'lmdb', label: 'LMDB' },
                      { value: 'rocksdb', label: 'RocksDB' },
                      { value: 'redis', label: 'Redis' },
                      { value: 'oracle', label: 'Oracle' },
                    ]}
                    style={{ width: '100%' }}
                  />
                  <Select
                    value={customProfileProcessingModeDraft}
                    onChange={(value) => {
                      const nextMode = value as CustomProfileProcessingMode
                      setCustomProfileProcessingModeDraft(nextMode)
                      if (nextMode === 'incremental_batch') {
                        setCustomProfileLivePersistDraft(true)
                        setCustomProfileFlushEveryRowsDraft((prev) => {
                          const safe = Number.isFinite(prev) ? prev : 20000
                          return Math.max(100, Math.min(Math.floor(safe || 20000), 50000))
                        })
                        setCustomProfileFlushIntervalSecondsDraft((prev) => {
                          const safe = Number.isFinite(prev) ? prev : 2
                          return Math.max(0.5, Math.min(Number(safe || 2), 10))
                        })
                      } else if (nextMode === 'incremental' && customProfileStorageDraft === 'rocksdb') {
                        applyRocksdbIncrementalTuningDefaults(
                          setCustomProfileFlushEveryRowsDraft,
                          setCustomProfileFlushIntervalSecondsDraft,
                          setCustomProfileAppendUniqueDisableMinRowsDraft,
                          setCustomProfileAppendUniqueEntityLimitDraft,
                          setCustomProfileAppendUniqueDisableRatioDraft,
                        )
                      }
                    }}
                    options={[
                      { value: 'batch', label: 'Batch' },
                      { value: 'incremental', label: 'Incremental' },
                      { value: 'incremental_batch', label: 'Incremental Batch' },
                    ]}
                    style={{ width: '100%' }}
                  />
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                    Batch: existing behavior. Incremental: high-speed row-wise updates. Incremental Batch: row-wise updates with chunked live persist for safer recovery.
                  </Text>
                  <div
                    style={{
                      border: '1px solid var(--app-border-strong)',
                      borderRadius: 8,
                      padding: 8,
                      background: 'var(--app-bg-elevated)',
                    }}
                  >
                    <Space direction="vertical" size={8} style={{ width: '100%' }}>
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                        Compute Strategy
                      </Text>
                      <Select
                        value={customProfileComputeStrategyDraft}
                        onChange={(value) => setCustomProfileComputeStrategyDraft(value as CustomProfileComputeStrategy)}
                        options={[
                          { value: 'single', label: 'Single Compute' },
                          { value: 'parallel_by_profile_key', label: 'Parallel by Profile Key' },
                        ]}
                        style={{ width: '100%' }}
                      />
                      {customProfileComputeStrategyDraft === 'parallel_by_profile_key' ? (
                        <Space direction="vertical" size={8} style={{ width: '100%' }}>
                          <div>
                            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                              Compute Executor
                            </Text>
                            <Select
                              value={customProfileComputeExecutorDraft}
                              onChange={(value) => setCustomProfileComputeExecutorDraft(value as CustomProfileComputeExecutor)}
                              options={[
                                { value: 'thread', label: 'Multi Thread' },
                                { value: 'process', label: 'Multi Process' },
                              ]}
                              style={{ width: '100%', marginTop: 4 }}
                            />
                          </div>
                          <div>
                            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                              Compute Workers (2-16)
                            </Text>
                            <InputNumber
                              min={2}
                              max={16}
                              value={customProfileComputeWorkersDraft}
                              onChange={(value) => {
                                const num = Number(value || 4)
                                setCustomProfileComputeWorkersDraft(Math.max(2, Math.min(Math.floor(num), 16)))
                              }}
                              style={{ width: '100%', marginTop: 4 }}
                            />
                          </div>
                          <div>
                            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                              Parallel Min Rows (1000-5000000)
                            </Text>
                            <InputNumber
                              min={1000}
                              max={5000000}
                              value={customProfileComputeMinRowsDraft}
                              onChange={(value) => {
                                const num = Number(value || 20000)
                                setCustomProfileComputeMinRowsDraft(Math.max(1000, Math.min(Math.floor(num), 5000000)))
                              }}
                              style={{ width: '100%', marginTop: 4 }}
                            />
                          </div>
                          <div>
                            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                              Global Prefetch Max Tokens (1000-2000000)
                            </Text>
                            <InputNumber
                              min={1000}
                              max={2000000}
                              value={customProfileComputeGlobalPrefetchMaxTokensDraft}
                              onChange={(value) => {
                                const num = Number(value || 40000)
                                setCustomProfileComputeGlobalPrefetchMaxTokensDraft(Math.max(1000, Math.min(Math.floor(num), 2000000)))
                              }}
                              style={{ width: '100%', marginTop: 4 }}
                            />
                          </div>
                          <div>
                            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                              Prefetch Chunk Size (50-900)
                            </Text>
                            <InputNumber
                              min={50}
                              max={900}
                              value={customProfileBackfillCandidatePrefetchChunkSizeDraft}
                              onChange={(value) => {
                                const num = Number(value || 500)
                                setCustomProfileBackfillCandidatePrefetchChunkSizeDraft(Math.max(50, Math.min(Math.floor(num), 900)))
                              }}
                              style={{ width: '100%', marginTop: 4 }}
                            />
                          </div>
                        </Space>
                      ) : null}
                    </Space>
                  </div>
                  {customProfileProcessingModeDraft !== 'batch' ? (
                    <div
                      style={{
                        border: '1px solid var(--app-border-strong)',
                        borderRadius: 8,
                        padding: 8,
                        background: 'var(--app-bg-elevated)',
                      }}
                    >
                      <Space direction="vertical" size={8} style={{ width: '100%' }}>
                        {customProfileProcessingModeDraft === 'incremental_batch' ? (
                          <Space style={{ justifyContent: 'space-between', width: '100%' }}>
                            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                              Live Persist
                            </Text>
                            <Switch
                              checked={customProfileLivePersistDraft}
                              onChange={setCustomProfileLivePersistDraft}
                            />
                          </Space>
                        ) : null}
                        <div>
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                            Flush Every Rows (100-50000)
                          </Text>
                          <InputNumber
                            min={100}
                            max={50000}
                            value={customProfileFlushEveryRowsDraft}
                            onChange={(value) => {
                              const num = Number(value || 20000)
                              setCustomProfileFlushEveryRowsDraft(Math.max(100, Math.min(Math.floor(num), 50000)))
                            }}
                            style={{ width: '100%', marginTop: 4 }}
                          />
                        </div>
                        <div>
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                            Flush Interval Seconds (0.5-10.0)
                          </Text>
                          <InputNumber
                            min={0.5}
                            max={10}
                            step={0.1}
                            value={customProfileFlushIntervalSecondsDraft}
                            onChange={(value) => {
                              const num = Number(value || 2)
                              setCustomProfileFlushIntervalSecondsDraft(Math.max(0.5, Math.min(num, 10)))
                            }}
                            style={{ width: '100%', marginTop: 4 }}
                          />
                        </div>
                        {customProfileStorageDraft === 'rocksdb' && customProfileProcessingModeDraft === 'incremental' ? (
                          <>
                            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                              RocksDB Incremental Tuning
                            </Text>
                            <div>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                                Append Unique Disable Min Rows
                              </Text>
                              <InputNumber
                                min={1000}
                                max={10000000}
                                value={customProfileAppendUniqueDisableMinRowsDraft}
                                onChange={(value) => {
                                  const num = Number(value || 5000)
                                  setCustomProfileAppendUniqueDisableMinRowsDraft(Math.max(1000, Math.floor(num)))
                                }}
                                style={{ width: '100%', marginTop: 4 }}
                              />
                            </div>
                            <div>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                                Append Unique Entity Limit
                              </Text>
                              <InputNumber
                                min={1000}
                                max={10000000}
                                value={customProfileAppendUniqueEntityLimitDraft}
                                onChange={(value) => {
                                  const num = Number(value || 10000)
                                  setCustomProfileAppendUniqueEntityLimitDraft(Math.max(1000, Math.floor(num)))
                                }}
                                style={{ width: '100%', marginTop: 4 }}
                              />
                            </div>
                            <div>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                                Append Unique Disable Ratio (0-1)
                              </Text>
                              <InputNumber
                                min={0.01}
                                max={1}
                                step={0.01}
                                value={customProfileAppendUniqueDisableRatioDraft}
                                onChange={(value) => {
                                  const num = Number(value || 0.35)
                                  setCustomProfileAppendUniqueDisableRatioDraft(Math.max(0.01, Math.min(num, 1)))
                                }}
                                style={{ width: '100%', marginTop: 4 }}
                              />
                            </div>
                          </>
                        ) : null}
                      </Space>
                    </div>
                  ) : null}
                  {customProfileStorageDraft === 'oracle' ? (
                    <div
                      style={{
                        border: '1px solid var(--app-border-strong)',
                        borderRadius: 8,
                        padding: 8,
                        background: 'var(--app-bg-elevated)',
                      }}
                    >
                      <Space direction="vertical" size={8} style={{ width: '100%' }}>
                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                          Oracle Profile Store
                        </Text>
                        <Input
                          value={customProfileOracleHostDraft}
                          onChange={(e) => setCustomProfileOracleHostDraft(e.target.value)}
                          placeholder="Host (optional if DSN is set)"
                          style={commonInputStyle}
                        />
                        <InputNumber
                          min={1}
                          max={65535}
                          value={customProfileOraclePortDraft}
                          onChange={(value) => {
                            const num = Number(value || 1521)
                            setCustomProfileOraclePortDraft(Math.max(1, Math.min(Math.floor(num), 65535)))
                          }}
                          style={{ width: '100%' }}
                          placeholder="Port"
                        />
                        <Input
                          value={customProfileOracleServiceNameDraft}
                          onChange={(e) => setCustomProfileOracleServiceNameDraft(e.target.value)}
                          placeholder="Service Name (optional)"
                          style={commonInputStyle}
                        />
                        <Input
                          value={customProfileOracleSidDraft}
                          onChange={(e) => setCustomProfileOracleSidDraft(e.target.value)}
                          placeholder="SID (optional)"
                          style={commonInputStyle}
                        />
                        <Input
                          value={customProfileOracleDsnDraft}
                          onChange={(e) => setCustomProfileOracleDsnDraft(e.target.value)}
                          placeholder="DSN (optional, overrides host/port/service/sid)"
                          style={commonInputStyle}
                        />
                        <Input
                          value={customProfileOracleUserDraft}
                          onChange={(e) => setCustomProfileOracleUserDraft(e.target.value)}
                          placeholder="Oracle User"
                          style={commonInputStyle}
                        />
                        <Input.Password
                          value={customProfileOraclePasswordDraft}
                          onChange={(e) => setCustomProfileOraclePasswordDraft(e.target.value)}
                          placeholder="Oracle Password"
                          style={commonInputStyle}
                        />
                        <Input
                          value={customProfileOracleTableDraft}
                          onChange={(e) => setCustomProfileOracleTableDraft(e.target.value)}
                          placeholder="Profile Table (default ETL_PROFILE_STATE)"
                          style={commonInputStyle}
                        />
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                            Async Queue Persist
                          </Text>
                          <Switch
                            checked={customProfileOracleQueueEnabledDraft}
                            onChange={setCustomProfileOracleQueueEnabledDraft}
                          />
                        </div>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                            Wait On Force Flush
                          </Text>
                          <Switch
                            checked={customProfileOracleQueueWaitOnForceFlushDraft}
                            onChange={setCustomProfileOracleQueueWaitOnForceFlushDraft}
                            disabled={!customProfileOracleQueueEnabledDraft}
                          />
                        </div>
                        <Select
                          value={customProfileOracleWriteStrategyDraft}
                          onChange={(value) => setCustomProfileOracleWriteStrategyDraft(value as CustomProfileOracleWriteStrategy)}
                          options={[
                            { value: 'single', label: 'Single Session (Current)' },
                            { value: 'parallel_key', label: 'Parallel by Profile Key' },
                          ]}
                          style={{ width: '100%' }}
                        />
                        {customProfileOracleWriteStrategyDraft === 'parallel_key' ? (
                          <Space direction="vertical" size={8} style={{ width: '100%' }}>
                            <InputNumber
                              min={2}
                              max={16}
                              value={customProfileOracleParallelWorkersDraft}
                              onChange={(value) => {
                                const num = Number(value || 4)
                                setCustomProfileOracleParallelWorkersDraft(Math.max(2, Math.min(Math.floor(num), 16)))
                              }}
                              style={{ width: '100%' }}
                              placeholder="Parallel Workers (2-16)"
                            />
                            <InputNumber
                              min={1}
                              max={1000000}
                              value={customProfileOracleParallelMinTokensDraft}
                              onChange={(value) => {
                                const num = Number(value || 2000)
                                setCustomProfileOracleParallelMinTokensDraft(Math.max(1, Math.min(Math.floor(num), 1000000)))
                              }}
                              style={{ width: '100%' }}
                              placeholder="Parallel Min Tokens (1-1000000)"
                            />
                            <InputNumber
                              min={50}
                              max={2000}
                              value={customProfileOracleMergeBatchSizeDraft}
                              onChange={(value) => {
                                const num = Number(value || 500)
                                setCustomProfileOracleMergeBatchSizeDraft(Math.max(50, Math.min(Math.floor(num), 2000)))
                              }}
                              style={{ width: '100%' }}
                              placeholder="Oracle Merge Batch Size (50-2000)"
                            />
                          </Space>
                        ) : null}
                      </Space>
                    </div>
                  ) : null}
                  <Select
                    value={customProfileEmitModeDraft}
                    onChange={(value) => setCustomProfileEmitModeDraft(value as 'changed_only' | 'all_entities')}
                    options={[
                      { value: 'changed_only', label: 'Emit changed entities only' },
                      { value: 'all_entities', label: 'Emit all entity profiles' },
                    ]}
                    style={{ width: '100%' }}
                  />
                  <Select
                    allowClear
                    showSearch
                    value={customProfileEventTimeFieldDraft || undefined}
                    onChange={(value) => setCustomProfileEventTimeFieldDraft(String(value || ''))}
                    options={expressionFieldOptions.map((field) => ({ value: field, label: field }))}
                    placeholder="Event time field (optional)"
                    style={{ width: '100%' }}
                  />
                  <Select
                    mode="tags"
                    tokenSeparators={[',']}
                    value={customProfileRequiredFieldsDraft}
                    onChange={(values) => setCustomProfileRequiredFieldsDraft(uniqueFieldNames((values || []).map((v) => String(v))))}
                    options={expressionFieldOptions.map((field) => ({ value: field, label: field }))}
                    placeholder="Required fields for validation"
                    style={{ width: '100%' }}
                  />
                  <Input
                    value={customProfileWindowDaysDraft}
                    onChange={(e) => setCustomProfileWindowDaysDraft(e.target.value)}
                    placeholder="Rolling windows (days) e.g. 1,7,30"
                    style={commonInputStyle}
                  />
                  <InputNumber
                    min={1}
                    value={customProfileRetentionDaysDraft}
                    onChange={(value) => setCustomProfileRetentionDaysDraft(Number(value || 45))}
                    placeholder="Retention days"
                    style={{ width: '100%' }}
                  />
                  <Space style={{ justifyContent: 'space-between', width: '100%' }}>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                      Include changed fields metadata
                    </Text>
                    <Switch
                      checked={customProfileIncludeChangeFieldsDraft}
                      onChange={setCustomProfileIncludeChangeFieldsDraft}
                    />
                  </Space>
                  <Divider style={{ margin: '6px 0', borderColor: 'var(--app-border-strong)' }} />
                  <Space style={{ justifyContent: 'space-between', width: '100%' }}>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                      Profile Monitoring
                    </Text>
                    <Space size={6}>
                      <Button
                        size="small"
                        loading={profileMonitorLoading}
                        onClick={() => {
                          void refreshProfileMonitor()
                        }}
                      >
                        Refresh
                      </Button>
                      <Button
                        size="small"
                        disabled={!activeProfileDocuments.length}
                        onClick={openProfileDataViewer}
                      >
                        View
                      </Button>
                      <Button
                        size="small"
                        danger
                        loading={profileMonitorClearing}
                        onClick={() => {
                          Modal.confirm({
                            title: 'Clear Profile Data',
                            content: 'Clear stored profile documents for this node?',
                            okText: 'Clear',
                            okButtonProps: { danger: true },
                            onOk: async () => {
                              await clearProfileMonitor()
                            },
                          })
                        }}
                      >
                        Clear
                      </Button>
                    </Space>
                  </Space>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                    Pipeline: {activePipelineId || 'Not saved yet'} | Node: {selectedNodeId || 'N/A'}
                  </Text>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                    Profiles: {Number(activeProfileNodeSummary?.entity_count || 0)} | Meta entries: {Number(activeProfileNodeSummary?.meta_count || 0)}
                  </Text>
                  {profileMonitorError ? (
                    <Text style={{ color: '#ef4444', fontSize: 11 }}>
                      {profileMonitorError}
                    </Text>
                  ) : null}
                  {activeProfileNodeSummary?.sample_entity_keys?.length ? (
                    <div
                      style={{
                        maxHeight: 140,
                        overflowY: 'auto',
                        border: '1px solid var(--app-border-strong)',
                        borderRadius: 8,
                        padding: 8,
                        background: 'var(--app-card-bg)',
                      }}
                    >
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                        Sample entity keys
                      </Text>
                      <div style={{ marginTop: 6, display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                        {activeProfileNodeSummary.sample_entity_keys.map((key) => (
                          <Tag key={key} style={{ marginInlineEnd: 0 }}>
                            {String(key)}
                          </Tag>
                        ))}
                      </div>
                    </div>
                  ) : (
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                      No stored profile documents for this node yet.
                    </Text>
                  )}
                </Space>
              ) : null}
            </div>

            <div
              style={{
                marginTop: 10,
                background: 'var(--app-card-bg)',
                border: '1px solid var(--app-border-strong)',
                borderRadius: 8,
                padding: 10,
              }}
            >
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                Validation Source
              </Text>
              <Select
                value={validationSourceDraft}
                onChange={(value) => setValidationSourceDraft(value as 'lmdb' | 'rocksdb' | 'sample')}
                options={[
                  { value: 'lmdb', label: 'LMDB (fast validation)' },
                  { value: 'rocksdb', label: 'RocksDB (fast validation)' },
                  { value: 'sample', label: 'Sample Rows (from preview/upstream)' },
                ]}
                style={{ width: '100%', marginTop: 6 }}
              />
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                LMDB/RocksDB mode uses profile store rows. Sample mode uses current node/upstream preview rows.
              </Text>
              <Space direction="vertical" size={8} style={{ marginTop: 8, width: '100%' }}>
                <Space.Compact style={{ width: '100%' }}>
                  <Input
                    value={validationLmdbEnvPathDraft}
                    onChange={(e) => setValidationLmdbEnvPathDraft(e.target.value)}
                    placeholder={validationSourceDraft === 'rocksdb' ? '/path/to/profile_store.rocksdb' : '/path/to/profile_store.lmdb'}
                    style={{ ...commonInputStyle, width: '100%' }}
                  />
                  <Button
                    loading={lmdbPathBrowseLoading}
                    onClick={() => {
                      void loadLmdbEnvPathOptions(
                        String(validationLmdbEnvPathDraft || ''),
                        validationSourceDraft === 'rocksdb' ? 'rocksdb' : 'lmdb',
                      )
                    }}
                  >
                    Browse
                  </Button>
                </Space.Compact>
                <Select
                  showSearch
                  allowClear
                  placeholder="Select discovered profile DB path"
                  options={lmdbEnvPathPresetOptions}
                  value={validationLmdbEnvPathDraft || undefined}
                  onChange={(value) => setValidationLmdbEnvPathDraft(String(value || ''))}
                  style={{ width: '100%' }}
                />
                {validationSourceDraft === 'lmdb' ? (
                  <Select
                    showSearch
                    allowClear
                    placeholder="DB name (optional)"
                    options={lmdbDbNamePresetOptions}
                    value={validationLmdbDbNameDraft || undefined}
                    onChange={(value) => setValidationLmdbDbNameDraft(String(value || ''))}
                    style={{ width: '100%' }}
                  />
                ) : null}
                <Input
                  value={validationLmdbKeyPrefixDraft}
                  onChange={(e) => setValidationLmdbKeyPrefixDraft(e.target.value)}
                  placeholder="Key prefix filter (optional)"
                  style={commonInputStyle}
                />
                <Space.Compact style={{ width: '100%' }}>
                  <Input
                    value={validationLmdbStartKeyDraft}
                    onChange={(e) => setValidationLmdbStartKeyDraft(e.target.value)}
                    placeholder="Start key (optional)"
                    style={{ ...commonInputStyle, width: '100%' }}
                  />
                  <Input
                    value={validationLmdbEndKeyDraft}
                    onChange={(e) => setValidationLmdbEndKeyDraft(e.target.value)}
                    placeholder="End key (optional)"
                    style={{ ...commonInputStyle, width: '100%' }}
                  />
                </Space.Compact>
                <Space.Compact style={{ width: '100%' }}>
                  <Input
                    value={validationLmdbKeyContainsDraft}
                    onChange={(e) => setValidationLmdbKeyContainsDraft(e.target.value)}
                    placeholder="Key contains (optional)"
                    style={{ ...commonInputStyle, width: '100%' }}
                  />
                  <InputNumber
                    min={1}
                    max={200}
                    value={validationLmdbLimitDraft}
                    onChange={(value) => setValidationLmdbLimitDraft(Number(value || 50))}
                    placeholder="Rows"
                    style={{ width: 110 }}
                  />
                </Space.Compact>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  Validation row sample limit: {Math.max(1, Math.min(Number(validationLmdbLimitDraft || 50) || 50, 200))}
                </Text>
                {lmdbPathBrowseError ? (
                  <Text style={{ color: '#ef4444', fontSize: 11 }}>
                    {lmdbPathBrowseError}
                  </Text>
                ) : null}
              </Space>
            </div>

            <div style={{ marginTop: 10 }}>
              <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Fields</Text>
              <div
                style={{
                  marginTop: 8,
                  display: 'grid',
                  gridTemplateColumns: '1fr',
                  gap: 6,
                }}
              >
                {expressionFieldOptions.length > 0 ? (
                  expressionFieldOptions.map((field) => (
                    <Button
                      key={`ref_${field}`}
                      size="small"
                      onClick={() => insertIntoActiveExpression(`field('${field.replace(/'/g, "\\'")}')`)}
                      style={{
                        textAlign: 'left',
                        justifyContent: 'flex-start',
                        background: 'var(--app-card-bg)',
                        border: '1px solid var(--app-border-strong)',
                        color: 'var(--app-text)',
                      }}
                    >
                      {field}
                    </Button>
                  ))
                ) : (
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                    Connect source/input node to load reference fields.
                  </Text>
                )}
              </div>
            </div>

            <div style={{ marginTop: 12 }}>
              <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Functions</Text>
              <div
                style={{
                  marginTop: 8,
                  display: 'grid',
                  gridTemplateColumns: '1fr',
                  gap: 6,
                }}
              >
                {EXPRESSION_FUNCTION_SNIPPETS.map((fn) => (
                  <Button
                    key={fn.label}
                    size="small"
                    onClick={() => insertIntoActiveExpression(fn.snippet.replace(/\$\{\d+:([^}]+)\}/g, '$1'))}
                    style={{
                      textAlign: 'left',
                      justifyContent: 'flex-start',
                      background: 'var(--app-card-bg)',
                      border: '1px solid var(--app-border-strong)',
                      color: 'var(--app-text)',
                    }}
                  >
                    {fn.label}
                  </Button>
                ))}
              </div>
            </div>
          </div>

          <div style={{ flex: 1, padding: 14, overflowY: 'auto' }}>
            <Space
              align="center"
              wrap
              style={{ justifyContent: 'space-between', width: '100%', marginBottom: 10 }}
            >
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                {customFieldDraftEnabledCount}/{customFieldDraftCount} enabled
                {customFieldDraftCount > 0 ? ` • ${customFieldDraftCollapsedCount} collapsed` : ''}
              </Text>
              <Space size={6} wrap>
                <Button
                  size="small"
                  disabled={customFieldDraftCount === 0}
                  onClick={() => setAllCustomFieldsEnabled(customFieldDraftEnabledCount !== customFieldDraftCount)}
                >
                  {customFieldDraftEnabledCount === customFieldDraftCount ? 'Disable All' : 'Enable All'}
                </Button>
                <Button
                  size="small"
                  disabled={customFieldDraftCount === 0}
                  onClick={() => {
                    if (customFieldDraftCollapsedCount === customFieldDraftCount) {
                      expandAllCustomFields()
                      return
                    }
                    collapseAllCustomFields()
                  }}
                >
                  {customFieldDraftCollapsedCount === customFieldDraftCount ? 'Expand All' : 'Collapse All'}
                </Button>
              </Space>
            </Space>
            {customFieldDraft.length === 0 ? (
              <div style={{ padding: '24px 0' }}>
                <Text style={{ color: 'var(--app-text-subtle)' }}>No custom fields yet. Click Add Field.</Text>
              </div>
            ) : (
              customFieldDraft.map((item, idx) => {
                const isCollapsed = collapsedCustomFieldIds.includes(item.id)
                const preview = (
                  item.mode === 'value'
                    ? String(item.expression || '').trim()
                    : String(item.jsonTemplate || '').trim().replace(/\s+/g, ' ')
                )
                return (
                  <div
                    key={item.id}
                    style={{
                      background: 'var(--app-card-bg)',
                      border: '1px solid var(--app-border-strong)',
                      borderRadius: 10,
                      padding: 12,
                      marginBottom: 12,
                      opacity: item.enabled ? 1 : 0.72,
                    }}
                  >
                    <Space align="center" style={{ justifyContent: 'space-between', width: '100%' }}>
                      <Space size={8} align="center">
                        <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Field #{idx + 1}</Text>
                      </Space>
                      <Space size={6} align="center" wrap>
                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Enable</Text>
                        <Switch
                          size="small"
                          checked={item.enabled}
                          onChange={(value) => updateCustomFieldDraft(item.id, { enabled: value })}
                        />
                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Expand</Text>
                        <Switch
                          size="small"
                          checked={!isCollapsed}
                          onChange={(checked) => {
                            setCollapsedCustomFieldIds((prev) => {
                              if (checked) return prev.filter((itemId) => itemId !== item.id)
                              return prev.includes(item.id) ? prev : [...prev, item.id]
                            })
                          }}
                        />
                        <Button
                          size="small"
                          onClick={() => openExpandedEditor(item.id, item.mode)}
                        >
                          Expand Editor
                        </Button>
                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Beautify</Text>
                        <Switch
                          size="small"
                          checked={Boolean(customFieldBeautifyUndoById[item.id])}
                          checkedChildren="Undo"
                          unCheckedChildren="Beautify"
                          onChange={(checked) => {
                            if (checked) {
                              beautifyCustomFieldDraft(item.id)
                              return
                            }
                            undoBeautifyCustomFieldDraft(item.id)
                          }}
                        />
                        <Button
                          danger
                          type="text"
                          icon={<DeleteOutlined />}
                          onClick={() => removeCustomFieldDraft(item.id)}
                        >
                          Remove
                        </Button>
                      </Space>
                    </Space>
                    {isCollapsed ? (
                      <div style={{ marginTop: 8 }}>
                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                          Name: {item.name || '(unnamed)'} | Mode: {item.mode === 'value' ? 'Single Value Expression' : 'JSON Object/Array Template'}
                          {item.mode === 'value' ? ` | Output: ${item.singleValueOutput === 'plain_text' ? 'Plain Text' : 'JSON'}` : ''}
                        </Text>
                        <br />
                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                          {preview ? preview.slice(0, 180) : 'No expression/template configured yet.'}
                          {preview.length > 180 ? '...' : ''}
                        </Text>
                      </div>
                    ) : (
                      <>
                        <div
                          style={{
                            display: 'grid',
                            gridTemplateColumns: item.mode === 'value' ? '1fr 200px 220px' : '1fr 200px',
                            gap: 10,
                            marginTop: 8,
                          }}
                        >
                          <Input
                            value={item.name}
                            placeholder="Custom field name (e.g. profit_margin)"
                            onChange={(e) => updateCustomFieldDraft(item.id, { name: e.target.value })}
                            style={commonInputStyle}
                          />
                          <Select
                            value={item.mode}
                            options={[
                              { value: 'value', label: 'Single Value Expression' },
                              { value: 'json', label: 'JSON Object/Array Template' },
                            ]}
                            onChange={(value) => updateCustomFieldDraft(item.id, { mode: value as CustomFieldMode })}
                          />
                          {item.mode === 'value' ? (
                            <Select
                              value={item.singleValueOutput}
                              options={[
                                { value: 'json', label: 'Output: JSON' },
                                { value: 'plain_text', label: 'Output: Plain Text' },
                              ]}
                              onChange={(value) => updateCustomFieldDraft(item.id, { singleValueOutput: value as SingleValueOutputMode })}
                            />
                          ) : null}
                        </div>

                        {item.mode === 'value' ? (
                          <div style={{ marginTop: 10, border: '1px solid var(--app-border-strong)', borderRadius: 8, overflow: 'hidden' }}>
                            <Editor
                              height="120px"
                              language={EXPR_LANGUAGE_ID}
                              value={item.expression}
                              beforeMount={(monaco) => ensureExpressionLanguage(monaco)}
                              onMount={(editor, monaco) => {
                                attachExpressionValidation(editor, monaco)
                                attachExpressionAutoSuggest(editor)
                                editor.onDidFocusEditorText(() => {
                                  setActiveExpressionFieldId(item.id)
                                })
                              }}
                              onChange={(value) => updateCustomFieldDraft(item.id, { expression: value || '' })}
                              theme={customEditorThemeId}
                              options={{
                                minimap: { enabled: false },
                                fontFamily: customEditorFontFamily,
                                fontSize: customEditorFontSize,
                                lineHeight: customEditorLineHeight,
                                fontLigatures: customEditorLigatures,
                                scrollBeyondLastLine: false,
                                wordWrap: customEditorWordWrap ? 'on' : 'off',
                                quickSuggestions: { other: true, comments: false, strings: true },
                                suggestOnTriggerCharacters: true,
                                renderValidationDecorations: 'on',
                                glyphMargin: true,
                                matchBrackets: 'always',
                                autoClosingBrackets: 'always',
                                bracketPairColorization: {
                                  enabled: true,
                                  independentColorPoolPerBracketType: true,
                                },
                                guides: {
                                  bracketPairs: true,
                                  bracketPairsHorizontal: true,
                                  highlightActiveBracketPair: true,
                                },
                                padding: { top: 8, bottom: 8 },
                              }}
                            />
                          </div>
                        ) : (
                          <div style={{ marginTop: 10, border: '1px solid var(--app-border-strong)', borderRadius: 8, overflow: 'hidden' }}>
                            <Editor
                              height="200px"
                              language={JSON_TEMPLATE_LANGUAGE_ID}
                              value={item.jsonTemplate}
                              beforeMount={(monaco) => ensureJsonTemplateLanguage(monaco)}
                              onMount={(editor, monaco) => {
                                attachJsonTemplateExpressionValidation(editor, monaco)
                                attachExpressionAutoSuggest(editor)
                              }}
                              onChange={(value) => updateCustomFieldDraft(item.id, { jsonTemplate: value || '' })}
                              theme={customEditorThemeId}
                              options={{
                                minimap: { enabled: false },
                                fontFamily: customEditorFontFamily,
                                fontSize: customEditorFontSize,
                                lineHeight: customEditorLineHeight,
                                fontLigatures: customEditorLigatures,
                                scrollBeyondLastLine: false,
                                wordWrap: customEditorWordWrap ? 'on' : 'off',
                                quickSuggestions: { other: true, comments: false, strings: true },
                                suggestOnTriggerCharacters: true,
                                renderValidationDecorations: 'on',
                                glyphMargin: true,
                                matchBrackets: 'always',
                                autoClosingBrackets: 'always',
                                bracketPairColorization: {
                                  enabled: true,
                                  independentColorPoolPerBracketType: true,
                                },
                                guides: {
                                  bracketPairs: true,
                                  bracketPairsHorizontal: true,
                                  highlightActiveBracketPair: true,
                                },
                                padding: { top: 8, bottom: 8 },
                              }}
                            />
                          </div>
                        )}
                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11, marginTop: 6, display: 'block' }}>
                          Reference source fields as `field('column_name')` and custom fields by name or `field('custom_name')`.
                        </Text>
                        {!item.enabled ? (
                          <Text style={{ color: '#f59e0b', fontSize: 11, marginTop: 4, display: 'block' }}>
                            Disabled fields are saved but excluded from execution and validation output.
                          </Text>
                        ) : null}
                      </>
                    )}
                  </div>
                )
              })
            )}
          </div>

          <div
            style={{
              width: '24%',
              minWidth: 300,
              borderLeft: '1px solid var(--app-border-strong)',
              padding: 14,
              overflowY: 'auto',
            }}
          >
            <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Example Repository</Text>
            <br />
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
              Filter by category, search, then click Add Example to insert ready-to-edit templates.
            </Text>
            <Space direction="vertical" size={8} style={{ marginTop: 10, width: '100%' }}>
              <Select
                value={exampleRepoCategory}
                onChange={setExampleRepoCategory}
                options={customFieldExampleCategoryOptions}
                style={{ width: '100%' }}
              />
              <Input
                value={exampleRepoSearch}
                onChange={(e) => setExampleRepoSearch(e.target.value)}
                placeholder="Search examples (count_if, group_aggregate, rolling...)"
                style={commonInputStyle}
              />
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                {filteredCustomFieldExamples.length} example{filteredCustomFieldExamples.length === 1 ? '' : 's'} shown
              </Text>
            </Space>
            <Space direction="vertical" size={10} style={{ marginTop: 10, width: '100%' }}>
              {filteredCustomFieldExamples.length > 0 ? (
                filteredCustomFieldExamples.map((example) => (
                  <div
                    key={example.id}
                    style={{
                      background: 'var(--app-card-bg)',
                      border: '1px solid var(--app-border-strong)',
                      borderRadius: 8,
                      padding: 10,
                    }}
                  >
                    <Space size={6} wrap style={{ marginBottom: 4 }}>
                      <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>{example.title}</Text>
                      <Tag style={{ marginInlineEnd: 0 }}>{example.mode === 'value' ? 'Single Value' : 'JSON'}</Tag>
                      <Tag style={{ marginInlineEnd: 0 }}>{example.category}</Tag>
                    </Space>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>{example.description}</Text>
                    {(example.tags || []).length > 0 ? (
                      <Space size={4} wrap style={{ marginTop: 6 }}>
                        {(example.tags || []).slice(0, 5).map((tag) => (
                          <Tag
                            key={`${example.id}_${tag}`}
                            style={{
                              marginInlineEnd: 0,
                              background: 'var(--app-input-bg)',
                              border: '1px solid var(--app-border-strong)',
                              color: 'var(--app-text-subtle)',
                              fontSize: 10,
                            }}
                          >
                            {tag}
                          </Tag>
                        ))}
                      </Space>
                    ) : null}
                    <Input.TextArea
                      value={example.mode === 'value' ? example.expression : example.jsonTemplate}
                      readOnly
                      rows={example.mode === 'value' ? 2 : 8}
                      style={{
                        marginTop: 8,
                        background: 'var(--app-input-bg)',
                        border: '1px solid var(--app-border-strong)',
                        color: 'var(--app-text)',
                        fontFamily: 'monospace',
                        fontSize: 12,
                      }}
                    />
                    <Button
                      size="small"
                      style={{ marginTop: 8, width: '100%' }}
                      onClick={() =>
                        addCustomFieldDraft({
                          name: example.name,
                          mode: example.mode,
                          expression: example.mode === 'value' ? String(example.expression || '') : '',
                          jsonTemplate: example.mode === 'json' ? String(example.jsonTemplate || '') : '',
                        })
                      }
                    >
                      Add Example
                    </Button>
                  </div>
                ))
              ) : (
                <div
                  style={{
                    background: 'var(--app-card-bg)',
                    border: '1px solid var(--app-border-strong)',
                    borderRadius: 8,
                    padding: 10,
                  }}
                >
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                    No examples matched. Change filter or search text.
                  </Text>
                </div>
              )}
            </Space>
            <div style={{ marginTop: 12 }}>
              <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Expression Tips</Text>
              <Input.TextArea
                value={CUSTOM_FIELD_TIPS_TEXT}
                readOnly
                rows={8}
                style={{
                  marginTop: 8,
                  background: 'var(--app-input-bg)',
                  border: '1px solid var(--app-border-strong)',
                  color: 'var(--app-text)',
                  fontFamily: 'monospace',
                  fontSize: 12,
                }}
              />
            </div>
          </div>
        </div>
      </Modal>
    )}
    <Modal
      open={profileDataModalOpen}
      onCancel={() => setProfileDataModalOpen(false)}
      footer={null}
      centered
      width="90vw"
      styles={{
        content: {
          borderRadius: 12,
          border: '1px solid var(--app-border-strong)',
          background: 'var(--app-panel-bg)',
        },
        body: {
          maxHeight: '84vh',
          overflowY: 'auto',
          padding: 16,
        },
      }}
    >
      <Space direction="vertical" size={10} style={{ width: '100%' }}>
        <Space align="center" style={{ justifyContent: 'space-between', width: '100%' }}>
          <Text style={{ color: 'var(--app-text)', fontWeight: 700, fontSize: 16 }}>
            Stored Profile Data
          </Text>
          <Space size={6}>
            <Button
              size="small"
              loading={profileMonitorLoading}
              onClick={() => {
                void refreshProfileMonitor()
              }}
            >
              Refresh
            </Button>
            <Button size="small" onClick={() => setProfileDataModalOpen(false)}>Close</Button>
          </Space>
        </Space>
        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
          Node: {activeProfileNodeSummary?.node_id || selectedNodeId || 'N/A'} | Showing {activeProfileDocuments.length} sample profile document(s).
        </Text>
        <Select
          showSearch
          value={selectedProfileDocument ? String(selectedProfileDocument.entity_key) : undefined}
          onChange={(value) => setSelectedProfileEntityKey(String(value || ''))}
          options={activeProfileDocuments.map((doc) => ({
            value: String(doc.entity_key),
            label: String(doc.entity_key),
          }))}
          placeholder="Select entity key"
          style={{ width: '100%' }}
          optionFilterProp="label"
        />
        <Input.TextArea
          readOnly
          rows={28}
          value={JSON.stringify(selectedProfileDocument?.profile || {}, null, 2)}
          style={{
            background: 'var(--app-input-bg)',
            border: '1px solid var(--app-border-strong)',
            color: 'var(--app-text)',
            fontFamily: 'monospace',
            fontSize: 12,
          }}
        />
      </Space>
    </Modal>
    <Modal
      open={Boolean(expandedEditorField)}
      onCancel={() => {
        setUiConditionBuilderModalIndex(null)
        setUiFieldBuilderModalIndex(null)
        setExpandedEditorFieldId(null)
      }}
      footer={null}
      closable={false}
      centered
      width="96vw"
      styles={{
        content: {
          padding: 0,
          borderRadius: 12,
          overflow: 'hidden',
          border: '1px solid var(--app-border-strong)',
          background: 'var(--app-panel-bg)',
          height: '96vh',
          display: 'flex',
          flexDirection: 'column',
        },
        body: {
          padding: 0,
          flex: 1,
          minHeight: 0,
          display: 'flex',
          flexDirection: 'column',
        },
      }}
    >
      <div
        style={{
          borderBottom: '1px solid var(--app-border-strong)',
          background: 'var(--app-card-bg)',
          padding: '12px 16px',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          gap: 12,
        }}
      >
        <div>
          <Text style={{ color: 'var(--app-text)', fontWeight: 700, fontSize: 16 }}>
            Expanded Custom Field Editor
          </Text>
          <br />
          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
            {expandedEditorField ? `Field: ${expandedEditorField.name || '(unnamed)'}` : ''}
          </Text>
        </div>
        <Space wrap>
          <Select
            size="small"
            value={customEditorColorProfile}
            onChange={(value) => setCustomEditorColorProfile(value as CustomEditorColorProfile)}
            options={CUSTOM_EDITOR_COLOR_PROFILE_OPTIONS}
            style={{ width: 150 }}
          />
          <Select
            size="small"
            value={customEditorFontPreset}
            onChange={(value) => setCustomEditorFontPreset(value as CustomEditorFontPreset)}
            options={CUSTOM_EDITOR_FONT_FAMILY_OPTIONS}
            style={{ width: 150 }}
          />
          <Select
            size="small"
            value={customEditorBeautifyStyle}
            onChange={(value) => setCustomEditorBeautifyStyle(value as CustomBeautifyStyle)}
            options={CUSTOM_EDITOR_BEAUTIFY_STYLE_OPTIONS}
            style={{ width: 172 }}
          />
          <Select
            size="small"
            value={customEditorFontSize}
            onChange={(value) => setCustomEditorFontSize(Number(value || 13))}
            options={[11, 12, 13, 14, 15, 16, 18, 20].map((size) => ({ value: size, label: `Size ${size}` }))}
            style={{ width: 108 }}
          />
          <Space size={4}>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Beautify</Text>
            <Switch
              size="small"
              disabled={!expandedEditorField}
              checked={Boolean(expandedEditorField && customFieldBeautifyUndoById[expandedEditorField.id])}
              checkedChildren="Undo"
              unCheckedChildren="Beautify"
              onChange={(checked) => {
                if (!expandedEditorField) return
                if (checked) {
                  beautifyCustomFieldDraft(expandedEditorField.id)
                  return
                }
                undoBeautifyCustomFieldDraft(expandedEditorField.id)
              }}
            />
          </Space>
          <Tooltip title="Close">
            <Button
              shape="circle"
              icon={<CloseOutlined />}
              onClick={() => {
                setUiConditionBuilderModalIndex(null)
                setUiFieldBuilderModalIndex(null)
                setExpandedEditorFieldId(null)
              }}
              aria-label="Close expanded editor"
            />
          </Tooltip>
        </Space>
      </div>
      <div style={{ flex: 1, minHeight: 0, padding: '0 10px 10px' }}>
        {expandedEditorField ? (
          <div style={{ width: '100%', height: '100%', display: 'flex', flexDirection: 'column', gap: 8 }}>
            <Tabs
              size="small"
              activeKey={expandedEditorTab}
              onChange={(key) => setExpandedEditorTab(key as UiExpressionEditorTab)}
              items={[
                { key: 'expression', label: 'Expression' },
                { key: 'ui_expression', label: 'UI Expression' },
              ]}
              style={{ flex: '0 0 auto' }}
            />
            {expandedEditorTab === 'expression' ? (
              <div style={{ width: '100%', height: '100%', display: 'flex', flexDirection: 'column', gap: 8 }}>
                {expandedEditorMode === 'value' ? (
                  <Space size={8} align="center" wrap style={{ flex: '0 0 auto' }}>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Single Value Output</Text>
                    <Select
                      value={expandedEditorField.singleValueOutput}
                      options={[
                        { value: 'json', label: 'JSON' },
                        { value: 'plain_text', label: 'Plain Text' },
                      ]}
                      onChange={(value) => updateCustomFieldDraft(expandedEditorField.id, { singleValueOutput: value as SingleValueOutputMode })}
                      style={{ width: 180 }}
                    />
                  </Space>
                ) : null}
                <div style={{ flex: 1, minHeight: 0 }}>
                  <Editor
                    height="100%"
                    language={expandedEditorMode === 'json' ? JSON_TEMPLATE_LANGUAGE_ID : EXPR_LANGUAGE_ID}
                    value={expandedEditorMode === 'json' ? expandedEditorField.jsonTemplate : expandedEditorField.expression}
                    beforeMount={(monaco) => {
                      if (expandedEditorMode === 'value') ensureExpressionLanguage(monaco)
                      else ensureJsonTemplateLanguage(monaco)
                    }}
                    onMount={(editor, monaco) => {
                      if (expandedEditorMode === 'value') {
                        attachExpressionValidation(editor, monaco)
                      } else {
                        attachJsonTemplateExpressionValidation(editor, monaco)
                      }
                      attachExpressionAutoSuggest(editor)
                    }}
                    onChange={(value) => {
                      if (!expandedEditorField) return
                      if (expandedEditorMode === 'json') {
                        updateCustomFieldDraft(expandedEditorField.id, { jsonTemplate: value || '' })
                      } else {
                        updateCustomFieldDraft(expandedEditorField.id, { expression: value || '' })
                      }
                    }}
                    theme={customEditorThemeId}
                    options={{
                      minimap: { enabled: false },
                      fontFamily: customEditorFontFamily,
                      fontSize: customEditorFontSize,
                      lineHeight: customEditorLineHeight,
                      fontLigatures: customEditorLigatures,
                      scrollBeyondLastLine: false,
                      wordWrap: customEditorWordWrap ? 'on' : 'off',
                      quickSuggestions: { other: true, comments: false, strings: true },
                      suggestOnTriggerCharacters: true,
                      renderValidationDecorations: 'on',
                      glyphMargin: true,
                      matchBrackets: 'always',
                      autoClosingBrackets: 'always',
                      bracketPairColorization: {
                        enabled: true,
                        independentColorPoolPerBracketType: true,
                      },
                      guides: {
                        bracketPairs: true,
                        bracketPairsHorizontal: true,
                        highlightActiveBracketPair: true,
                      },
                      padding: { top: 10, bottom: 10 },
                    }}
                  />
                </div>
              </div>
            ) : (
              <div
                style={{
                  width: '100%',
                  height: '100%',
                  display: 'grid',
                  gridTemplateColumns: 'minmax(380px, 40%) minmax(0, 1fr)',
                  gap: 8,
                }}
              >
                <div
                  style={{
                    border: '1px solid var(--app-border-strong)',
                    borderRadius: 10,
                    background: 'var(--app-card-bg)',
                    padding: 10,
                    overflowY: 'auto',
                  }}
                >
                  <Space direction="vertical" size={8} style={{ width: '100%' }}>
                    <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>Visual Expression Builder</Text>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                      Visual builder for functions, params, variables, and fields.
                    </Text>

                    <Divider style={{ margin: '6px 0' }} />
                    <Space align="center" style={{ justifyContent: 'space-between', width: '100%' }}>
                      <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>1) Select Function</Text>
                      <Button size="small" onClick={loadUiExpressionFromEditor}>
                        Load From Editor
                      </Button>
                    </Space>
                    <Select
                      size="small"
                      showSearch
                      optionFilterProp="label"
                      value={selectedUiExpressionTemplate?.value}
                      options={uiExpressionTemplateOptions}
                      onChange={(value) => {
                        setUiExpressionBuilderTouched(true)
                        setUiExpressionTemplateLabel(String(value || ''))
                      }}
                      style={{ width: '100%' }}
                    />

                    <Divider style={{ margin: '6px 0' }} />
                    <Space align="center" style={{ justifyContent: 'space-between', width: '100%' }}>
                      <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>2) Manage Variables</Text>
                      <Button size="small" onClick={addUiExpressionVariable}>Add Variable</Button>
                    </Space>
                    {uiExpressionVariables.length === 0 ? (
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                        No variables defined.
                      </Text>
                    ) : (
                      <Space direction="vertical" size={8} style={{ width: '100%' }}>
                        {uiExpressionVariables.map((variable) => (
                          <div
                            key={variable.id}
                            style={{
                              border: '1px solid var(--app-border-strong)',
                              borderRadius: 8,
                              background: 'var(--app-panel-bg)',
                              padding: 8,
                            }}
                          >
                            <Space direction="vertical" size={6} style={{ width: '100%' }}>
                              <Input
                                size="small"
                                value={variable.name}
                                placeholder="variable_name"
                                onChange={(event) => updateUiExpressionVariable(variable.id, { name: event.target.value })}
                              />
                              <Input
                                size="small"
                                value={variable.value}
                                placeholder="value or expression, e.g. field('AMOUNT')"
                                onChange={(event) => updateUiExpressionVariable(variable.id, { value: event.target.value })}
                              />
                              <Button size="small" danger onClick={() => removeUiExpressionVariable(variable.id)}>
                                Delete Variable
                              </Button>
                            </Space>
                          </div>
                        ))}
                      </Space>
                    )}

                    <Divider style={{ margin: '6px 0' }} />
                    {isUiGroupAggregateTemplate ? (
                      <>
                        <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>3) Configure Group Aggregate</Text>
                        <div
                          style={{
                            border: '1px solid var(--app-border-strong)',
                            borderRadius: 8,
                            background: 'var(--app-panel-bg)',
                            padding: 8,
                          }}
                        >
                          <Space direction="vertical" size={8} style={{ width: '100%' }}>
                            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                              Group Key Field
                            </Text>
                            <AutoComplete
                              value={uiGroupAggregateKeyField}
                              options={uiFieldAutoCompleteOptions}
                              onChange={(value) => {
                                setUiExpressionBuilderTouched(true)
                                setUiGroupAggregateKeyField(String(value || ''))
                              }}
                              filterOption={(inputValue, option) => String(option?.value || '').toLowerCase().includes(String(inputValue || '').toLowerCase())}
                            >
                              <Input size="small" placeholder="CUSTACCOUNTNUMBER" />
                            </AutoComplete>
                            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                              Group Output Name
                            </Text>
                            <Input
                              size="small"
                              value={uiGroupAggregateKeyName}
                              placeholder="customer"
                              onChange={(event) => {
                                setUiExpressionBuilderTouched(true)
                                setUiGroupAggregateKeyName(event.target.value)
                              }}
                            />
                          </Space>
                        </div>

                        <Space align="center" style={{ justifyContent: 'space-between', width: '100%' }}>
                          <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Metrics</Text>
                          <Button size="small" onClick={addUiGroupAggregateMetric}>Add Metric</Button>
                        </Space>

                        <Space direction="vertical" size={8} style={{ width: '100%' }}>
                          {uiGroupAggregateMetrics.map((metric) => (
                            <div
                              key={metric.id}
                              style={{
                                border: '1px solid var(--app-border-strong)',
                                borderRadius: 8,
                                background: 'var(--app-panel-bg)',
                                padding: 6,
                              }}
                            >
                              <Space direction="vertical" size={6} style={{ width: '100%' }}>
                                <div style={{ display: 'grid', gridTemplateColumns: 'minmax(130px, 1fr) minmax(140px, 1.2fr) minmax(110px, 0.8fr) auto', gap: 6 }}>
                                  <Input
                                    size="small"
                                    value={metric.outputName}
                                    placeholder="metric_name"
                                    onChange={(event) => updateUiGroupAggregateMetric(metric.id, { outputName: event.target.value })}
                                  />
                                  <AutoComplete
                                    value={metric.path}
                                    options={uiFieldAutoCompleteOptions}
                                    onChange={(value) => updateUiGroupAggregateMetric(metric.id, { path: String(value || '') })}
                                    filterOption={(inputValue, option) => String(option?.value || '').toLowerCase().includes(String(inputValue || '').toLowerCase())}
                                  >
                                    <Input size="small" placeholder="source_path" />
                                  </AutoComplete>
                                  <Select
                                    size="small"
                                    value={metric.agg}
                                    options={EXPR_ALLOWED_AGG_VALUES_OPTIONS.map((agg) => ({ value: agg, label: agg }))}
                                    onChange={(value) => updateUiGroupAggregateMetric(metric.id, { agg: String(value || 'sum') })}
                                    style={{ width: '100%' }}
                                  />
                                  <Button size="small" danger onClick={() => removeUiGroupAggregateMetric(metric.id)}>
                                    Delete
                                  </Button>
                                </div>
                                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                                  Format: output_name + source_path + aggregate
                                </Text>
                              </Space>
                            </div>
                          ))}
                        </Space>
                      </>
                    ) : (
                      <>
                        <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>3) Configure Parameters</Text>
                        {uiExpressionPlaceholders.length === 0 ? (
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                            Selected function has no parameters.
                          </Text>
                        ) : (
                          <Space direction="vertical" size={8} style={{ width: '100%' }}>
                            {uiExpressionPlaceholders.map((placeholder) => {
                              const currentValue = String(
                                uiExpressionValuesByIndex[placeholder.index] ?? placeholder.defaultValue ?? ''
                              )
                              const currentMode = (
                                uiExpressionParamModeByIndex[placeholder.index]
                                || defaultUiExpressionParameterMode(placeholder)
                              ) as UiExpressionParameterMode
                              const isConditionParam = isConditionLikePlaceholder(placeholder)
                              const isBooleanParam = isBooleanLikePlaceholder(placeholder)
                              const choiceOptions = (
                                placeholder.kind === 'choice'
                                  ? placeholder.options
                                  : isAggregateLikePlaceholder(placeholder)
                                    ? EXPR_ALLOWED_AGG_VALUES_OPTIONS
                                    : isOperatorLikePlaceholder(placeholder)
                                      ? EXPR_ALLOWED_CONDITION_OPERATORS_OPTIONS
                                      : []
                              )
                              const literalChoiceOptions = (
                                isBooleanParam
                                  ? ['true', 'false']
                                  : choiceOptions
                              )
                              const conditionState = uiConditionBuilderByIndex[placeholder.index]
                                || createDefaultUiConditionBuilderState()
                              const conditionUndoCount = (uiConditionUndoByIndex[placeholder.index] || []).length
                              const conditionRedoCount = (uiConditionRedoByIndex[placeholder.index] || []).length
                              const conditionClusters = Array.isArray(conditionState.clusters)
                                ? conditionState.clusters
                                : []
                              const conditionClausesByCluster = conditionClusters.map((cluster) => ({
                                cluster,
                                clauses: (conditionState.clauses || []).filter((clause) => clause.clusterId === cluster.id),
                              }))
                              const fieldBuilderState = createDefaultUiFieldBuilderState(
                                uiFieldBuilderByIndex[placeholder.index] || createDefaultUiFieldBuilderState()
                              )
                              const parameterModeOptions: Array<{ value: UiExpressionParameterMode; label: string }> = [
                                { value: 'field', label: 'Field' },
                                { value: 'values', label: 'Values' },
                                { value: 'variable', label: 'Variable' },
                                { value: 'literal', label: 'Literal' },
                                { value: 'raw', label: 'Raw' },
                              ]
                              return (
                                <div
                                  key={`ui_expr_placeholder_${placeholder.index}`}
                                  style={{
                                    border: '1px solid var(--app-border-strong)',
                                    borderRadius: 8,
                                    background: 'var(--app-panel-bg)',
                                    padding: 8,
                                  }}
                                >
                                  <Space direction="vertical" size={6} style={{ width: '100%' }}>
                                    <div style={{ display: 'grid', gridTemplateColumns: 'minmax(165px, 1fr) minmax(105px, 0.8fr) minmax(170px, 1.4fr) auto', gap: 6 }}>
                                      <div
                                        style={{
                                          alignSelf: 'center',
                                          color: 'var(--app-text-subtle)',
                                          fontSize: 11,
                                          fontFamily: 'monospace',
                                          overflow: 'hidden',
                                          textOverflow: 'ellipsis',
                                          whiteSpace: 'nowrap',
                                        }}
                                        title={`Parameter #${placeholder.index} (${placeholder.defaultValue || 'value'})`}
                                      >
                                        #{placeholder.index} {placeholder.defaultValue || 'value'}
                                      </div>
                                      <Select
                                        size="small"
                                        value={currentMode}
                                        options={parameterModeOptions}
                                        onChange={(value) => updateUiExpressionPlaceholderMode(placeholder.index, value as UiExpressionParameterMode)}
                                        style={{ width: '100%' }}
                                      />
                                      {currentMode === 'field' || currentMode === 'values' ? (
                                        <AutoComplete
                                          value={currentValue}
                                          options={uiFieldAutoCompleteOptions}
                                          onChange={(value) => updateUiExpressionPlaceholderValue(placeholder.index, String(value || ''))}
                                          filterOption={(inputValue, option) => String(option?.value || '').toLowerCase().includes(String(inputValue || '').toLowerCase())}
                                        >
                                          <Input size="small" placeholder="field/path" />
                                        </AutoComplete>
                                      ) : currentMode === 'variable' ? (
                                        <Select
                                          size="small"
                                          showSearch
                                          value={currentValue || undefined}
                                          options={uiExpressionVariableOptions}
                                          onChange={(value) => updateUiExpressionPlaceholderValue(placeholder.index, String(value || ''))}
                                          placeholder="variable"
                                          style={{ width: '100%' }}
                                          optionFilterProp="label"
                                        />
                                      ) : literalChoiceOptions.length > 0 ? (
                                        <Select
                                          size="small"
                                          value={currentValue || placeholder.defaultValue}
                                          options={literalChoiceOptions.map((opt) => ({ value: opt, label: opt }))}
                                          onChange={(value) => updateUiExpressionPlaceholderValue(placeholder.index, String(value || ''))}
                                          style={{ width: '100%' }}
                                        />
                                      ) : (
                                        <Input
                                          size="small"
                                          value={currentValue}
                                          placeholder={currentMode === 'raw' ? 'raw expression' : 'value'}
                                          onChange={(event) => updateUiExpressionPlaceholderValue(placeholder.index, event.target.value)}
                                        />
                                      )}
                                      {isConditionParam ? (
                                        <>
                                          <Button
                                            size="small"
                                            onClick={() => openUiConditionBuilderModal(placeholder.index, currentValue)}
                                          >
                                            Condition
                                          </Button>
                                          <ConditionBuilderModal
                                            open={uiConditionBuilderModalIndex === placeholder.index}
                                            onClose={() => setUiConditionBuilderModalIndex(null)}
                                            placeholderIndex={placeholder.index}
                                            conditionState={conditionState}
                                            conditionClusters={conditionClusters}
                                            conditionClausesByCluster={conditionClausesByCluster}
                                            conditionUndoCount={conditionUndoCount}
                                            conditionRedoCount={conditionRedoCount}
                                            uiFieldAutoCompleteOptions={uiFieldAutoCompleteOptions}
                                            uiExpressionVariableOptions={uiExpressionVariableOptions}
                                            conditionOperatorOptions={EXPR_ALLOWED_CONDITION_OPERATORS_OPTIONS}
                                            addUiConditionCluster={addUiConditionCluster}
                                            undoUiConditionBuilder={undoUiConditionBuilder}
                                            redoUiConditionBuilder={redoUiConditionBuilder}
                                            updateUiConditionClusterJoin={updateUiConditionClusterJoin}
                                            addUiConditionClause={addUiConditionClause}
                                            removeUiConditionCluster={removeUiConditionCluster}
                                            updateUiConditionClause={updateUiConditionClause}
                                            removeUiConditionClause={removeUiConditionClause}
                                            applyUiConditionBuilderToPlaceholder={applyUiConditionBuilderToPlaceholder}
                                          />
                                        </>
                                      ) : (
                                        <>
                                          <Button
                                            size="small"
                                            onClick={() => openUiFieldBuilderModal(placeholder.index, currentMode, currentValue)}
                                          >
                                            Field Builder
                                          </Button>
                                          <FieldBuilderModal
                                            open={uiFieldBuilderModalIndex === placeholder.index}
                                            onClose={() => setUiFieldBuilderModalIndex(null)}
                                            placeholderIndex={placeholder.index}
                                            fieldBuilderState={fieldBuilderState}
                                            uiFieldAutoCompleteOptions={uiFieldAutoCompleteOptions}
                                            aggOptions={EXPR_ALLOWED_AGG_VALUES_OPTIONS}
                                            updateUiFieldBuilder={updateUiFieldBuilder}
                                            buildUiFieldBuilderExpression={buildUiFieldBuilderExpression}
                                            applyUiFieldBuilder={applyUiFieldBuilder}
                                          />
                                        </>
                                      )}
                                    </div>
                                  </Space>
                                </div>
                              )
                            })}
                          </Space>
                        )}
                      </>
                    )}

                    {expandedEditorMode === 'json' ? (
                      <>
                        <Divider style={{ margin: '6px 0' }} />
                        <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>4) JSON Key Mapping</Text>
                        <Select
                          size="small"
                          showSearch
                          value={String(uiExpressionJsonKey || '').trim() || undefined}
                          options={uiExpressionJsonKeyOptions}
                          onChange={(value) => setUiExpressionJsonKey(String(value || 'value'))}
                          style={{ width: '100%' }}
                          optionFilterProp="label"
                        />
                        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0,1fr) auto', gap: 6 }}>
                          <Input
                            size="small"
                            value={uiExpressionNewJsonKeyDraft}
                            placeholder="new_key_name"
                            onChange={(event) => setUiExpressionNewJsonKeyDraft(event.target.value)}
                          />
                          <Button size="small" onClick={createUiExpressionJsonRow}>
                            Create New Key
                          </Button>
                        </div>
                        <Space size={8} wrap>
                          <Button size="small" onClick={upsertUiExpressionJsonRow}>
                            Add/Update Key
                          </Button>
                          <Button size="small" onClick={loadUiExpressionRowsFromEditor}>
                            Load From Editor
                          </Button>
                          <Button
                            size="small"
                            onClick={() => setUiExpressionRows([])}
                            disabled={uiExpressionRows.length === 0}
                          >
                            Clear Keys
                          </Button>
                        </Space>
                      </>
                    ) : null}

                    <Divider style={{ margin: '6px 0' }} />
                    <Space size={8} wrap>
                      <Button type="primary" onClick={applyUiExpressionToEditor}>
                        Apply To Expression
                      </Button>
                      {expandedEditorMode === 'json' ? (
                        <Button onClick={applyUiExpressionJsonRowsToEditor} disabled={uiExpressionRows.length === 0}>
                          Apply JSON Template
                        </Button>
                      ) : null}
                    </Space>
                  </Space>
                </div>

                <div
                  style={{
                    border: '1px solid var(--app-border-strong)',
                    borderRadius: 10,
                    background: 'var(--app-card-bg)',
                    padding: 10,
                    overflowY: 'auto',
                  }}
                >
                  <Space direction="vertical" size={8} style={{ width: '100%' }}>
                    <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>Builder Output</Text>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                      Generated expression preview.
                    </Text>
                    <div
                      style={{
                        border: '1px solid var(--app-border-strong)',
                        borderRadius: 8,
                        overflow: 'hidden',
                      }}
                    >
                      <Editor
                        height="170px"
                        language={EXPR_LANGUAGE_ID}
                        value={uiExpressionPreview}
                        beforeMount={(monaco) => {
                          ensureCustomEditorTheme(monaco)
                          ensureExpressionLanguage(monaco)
                        }}
                        onMount={(editor, monaco) => {
                          attachExpressionValidation(editor, monaco)
                          attachExpressionAutoSuggest(editor)
                        }}
                        theme={customEditorThemeId}
                        options={{
                          readOnly: true,
                          minimap: { enabled: false },
                          fontFamily: customEditorFontFamily,
                          fontSize: 12,
                          lineHeight: customEditorLineHeight,
                          wordWrap: 'on',
                          glyphMargin: true,
                          renderLineHighlight: 'none',
                          scrollBeyondLastLine: false,
                          padding: { top: 8, bottom: 8 },
                        }}
                      />
                    </div>

                    <Divider style={{ margin: '6px 0' }} />
                    <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>
                      {isUiGroupAggregateTemplate ? 'Group Aggregate Mapping' : 'Resolved Parameters'}
                    </Text>
                    {isUiGroupAggregateTemplate ? (
                      <Space direction="vertical" size={6} style={{ width: '100%' }}>
                        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                          <Tag style={{ marginInlineEnd: 0 }}>key</Tag>
                          <Text style={{ color: 'var(--app-text-subtle)', minWidth: 140 }}>Group Key Field</Text>
                          <Text style={{ color: 'var(--app-text)', fontFamily: 'monospace', fontSize: 12, wordBreak: 'break-all' }}>
                            {uiGroupAggregateKeyField || '(empty)'}
                          </Text>
                        </div>
                        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                          <Tag style={{ marginInlineEnd: 0 }}>name</Tag>
                          <Text style={{ color: 'var(--app-text-subtle)', minWidth: 140 }}>Group Output Name</Text>
                          <Text style={{ color: 'var(--app-text)', fontFamily: 'monospace', fontSize: 12, wordBreak: 'break-all' }}>
                            {uiGroupAggregateKeyName || '(empty)'}
                          </Text>
                        </div>
                        {(uiGroupAggregateMetrics || []).map((metric, idx) => (
                          <div key={`ui_group_metric_preview_${metric.id}`} style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                            <Tag style={{ marginInlineEnd: 0 }}>m{idx + 1}</Tag>
                            <Text style={{ color: 'var(--app-text-subtle)', minWidth: 140 }}>
                              {metric.outputName || `metric_${idx + 1}`}
                            </Text>
                            <Text style={{ color: 'var(--app-text)', fontFamily: 'monospace', fontSize: 12, wordBreak: 'break-all' }}>
                              path='{metric.path || ''}', agg='{metric.agg || ''}'
                            </Text>
                          </div>
                        ))}
                      </Space>
                    ) : (
                      <Space direction="vertical" size={6} style={{ width: '100%' }}>
                        {uiExpressionPlaceholders.map((placeholder) => (
                          <div key={`ui_expr_preview_param_${placeholder.index}`} style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                            <Tag style={{ marginInlineEnd: 0 }}>#{placeholder.index}</Tag>
                            <Text style={{ color: 'var(--app-text-subtle)', minWidth: 140 }}>
                              {placeholder.defaultValue || 'value'}
                            </Text>
                            <Text
                              style={{
                                color: 'var(--app-text)',
                                fontFamily: 'monospace',
                                fontSize: 12,
                                wordBreak: 'break-all',
                              }}
                            >
                              {String(uiExpressionResolvedValuesByIndex[placeholder.index] || '')}
                            </Text>
                          </div>
                        ))}
                      </Space>
                    )}

                    {expandedEditorMode === 'json' ? (
                      <>
                        <Divider style={{ margin: '6px 0' }} />
                        <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>JSON Keys</Text>
                        {uiExpressionRows.length === 0 ? (
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                            No JSON keys yet. Add from left panel.
                          </Text>
                        ) : (
                          <Space direction="vertical" size={8} style={{ width: '100%' }}>
                            {uiExpressionRows.map((row) => (
                              <div
                                key={row.id}
                                style={{
                                  border: '1px solid var(--app-border-strong)',
                                  borderRadius: 8,
                                  background: 'var(--app-panel-bg)',
                                  padding: 8,
                                }}
                              >
                                <Space direction="vertical" size={6} style={{ width: '100%' }}>
                                  <div style={{ display: 'grid', gridTemplateColumns: 'minmax(140px, 1fr) minmax(120px, 0.8fr) auto auto', gap: 6 }}>
                                    <Input
                                      size="small"
                                      value={row.key}
                                      placeholder="json_key"
                                      onChange={(event) => updateUiExpressionJsonRow(row.id, { key: event.target.value })}
                                    />
                                    <Select
                                      size="small"
                                      value={row.valueType || 'expression'}
                                      options={[
                                        { value: 'expression', label: 'Expression' },
                                        { value: 'json', label: 'JSON' },
                                      ]}
                                      onChange={(value) => updateUiExpressionJsonRow(row.id, { valueType: value as UiExpressionBuilderRow['valueType'] })}
                                      style={{ width: '100%' }}
                                    />
                                    <Button size="small" onClick={() => editUiExpressionJsonRowInBuilder(row)}>
                                      Edit
                                    </Button>
                                    <Button size="small" danger onClick={() => removeUiExpressionJsonRow(row.id)}>
                                      Delete
                                    </Button>
                                  </div>
                                  <div
                                    style={{
                                      border: '1px solid var(--app-border-strong)',
                                      borderRadius: 8,
                                      overflow: 'hidden',
                                    }}
                                  >
                                    <Editor
                                      height={row.valueType === 'json' ? '220px' : '120px'}
                                      language={row.valueType === 'json' ? JSON_TEMPLATE_LANGUAGE_ID : EXPR_LANGUAGE_ID}
                                      value={row.expression}
                                      beforeMount={(monaco) => {
                                        ensureCustomEditorTheme(monaco)
                                        if (row.valueType === 'json') ensureJsonTemplateLanguage(monaco)
                                        else ensureExpressionLanguage(monaco)
                                      }}
                                      onMount={(editor, monaco) => {
                                        if (row.valueType === 'json') {
                                          attachJsonTemplateExpressionValidation(editor, monaco)
                                        } else {
                                          attachExpressionValidation(editor, monaco)
                                          attachExpressionAutoSuggest(editor)
                                        }
                                      }}
                                      onChange={(value) => updateUiExpressionJsonRow(row.id, { expression: String(value || '') })}
                                      theme={customEditorThemeId}
                                      options={{
                                        minimap: { enabled: false },
                                        fontFamily: customEditorFontFamily,
                                        fontSize: 12,
                                        lineHeight: customEditorLineHeight,
                                        wordWrap: 'on',
                                        glyphMargin: true,
                                        renderValidationDecorations: 'on',
                                        scrollBeyondLastLine: false,
                                        padding: { top: 8, bottom: 8 },
                                      }}
                                    />
                                  </div>
                                </Space>
                              </div>
                            ))}
                          </Space>
                        )}
                        <Text style={{ color: 'var(--app-text)', fontWeight: 700, marginTop: 6 }}>Generated JSON Template</Text>
                        <div
                          style={{
                            border: '1px solid var(--app-border-strong)',
                            borderRadius: 8,
                            overflow: 'hidden',
                          }}
                        >
                          <Editor
                            height="300px"
                            language={JSON_TEMPLATE_LANGUAGE_ID}
                            value={uiExpressionJsonPreview}
                            beforeMount={(monaco) => {
                              ensureCustomEditorTheme(monaco)
                              ensureJsonTemplateLanguage(monaco)
                            }}
                            onMount={(editor, monaco) => {
                              attachJsonTemplateExpressionValidation(editor, monaco)
                            }}
                            theme={customEditorThemeId}
                            options={{
                              readOnly: true,
                              minimap: { enabled: false },
                              fontFamily: customEditorFontFamily,
                              fontSize: 12,
                              lineHeight: customEditorLineHeight,
                              wordWrap: 'on',
                              glyphMargin: true,
                              renderLineHighlight: 'none',
                              scrollBeyondLastLine: false,
                              padding: { top: 8, bottom: 8 },
                            }}
                          />
                        </div>
                      </>
                    ) : null}
                  </Space>
                </div>
              </div>
            )}
          </div>
        ) : null}
      </div>
    </Modal>
    <Modal
      open={validationModalOpen}
      onCancel={() => setValidationModalOpen(false)}
      footer={null}
      centered
      width="92vw"
      styles={{
        content: {
          borderRadius: 12,
          border: '1px solid var(--app-border-strong)',
          background: 'var(--app-panel-bg)',
        },
        body: {
          maxHeight: '84vh',
          overflowY: 'auto',
          padding: 16,
        },
      }}
    >
      <Space direction="vertical" size={10} style={{ width: '100%' }}>
        <Space align="center" style={{ justifyContent: 'space-between', width: '100%' }}>
          <Text style={{ color: 'var(--app-text)', fontWeight: 700, fontSize: 16 }}>
            Custom Field Validation Output
          </Text>
          {(() => {
            const hasErrors = !validationResult?.ok || (validationResult?.errors || []).length > 0
            const hasWarnings = !hasErrors && (validationResult?.warnings || []).length > 0
            const bg = hasErrors ? '#ef44441a' : (hasWarnings ? '#f59e0b14' : '#22c55e1a')
            const border = hasErrors ? '1px solid #ef444440' : (hasWarnings ? '1px solid #f59e0b40' : '1px solid #22c55e40')
            const color = hasErrors ? '#ef4444' : (hasWarnings ? '#f59e0b' : '#22c55e')
            const text = hasErrors ? 'HAS ERRORS' : (hasWarnings ? 'HAS WARNINGS' : 'VALID')
            return (
          <Tag
            style={{
              marginInlineEnd: 0,
              background: bg,
              border,
              color,
            }}
          >
            {text}
          </Tag>
            )
          })()}
        </Space>
        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
          Source: {String(validationResult?.validation_source || 'lmdb').toUpperCase()} | Input rows: {Number(validationResult?.input_rows || 0)} | Output rows: {Number(validationResult?.output_rows || 0)}
        </Text>
        {(validationResult?.errors || []).length > 0 ? (
          <div
            style={{
              border: '1px solid #ef444440',
              borderRadius: 8,
              background: '#ef444414',
              padding: 10,
            }}
          >
            <Text style={{ color: '#ef4444', fontWeight: 600 }}>Errors</Text>
            {(validationResult?.errors || []).map((err, idx) => (
              <div key={`val_err_${idx}`} style={{ color: '#fecaca', fontSize: 12, marginTop: 4 }}>
                {String(err)}
              </div>
            ))}
          </div>
        ) : null}
        {(validationResult?.warnings || []).length > 0 ? (
          <div
            style={{
              border: '1px solid #f59e0b40',
              borderRadius: 8,
              background: '#f59e0b14',
              padding: 10,
            }}
          >
            <Text style={{ color: '#f59e0b', fontWeight: 600 }}>Warnings</Text>
            {(validationResult?.warnings || []).map((warn, idx) => (
              <div key={`val_warn_${idx}`} style={{ color: '#fde68a', fontSize: 12, marginTop: 4 }}>
                {String(warn)}
              </div>
            ))}
          </div>
        ) : null}
        <div>
          <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Sample Input</Text>
          <Input.TextArea
            readOnly
            rows={10}
            value={JSON.stringify(validationResult?.sample_input || [], null, 2)}
            style={{
              marginTop: 6,
              background: 'var(--app-input-bg)',
              border: '1px solid var(--app-border-strong)',
              color: 'var(--app-text)',
              fontFamily: 'monospace',
              fontSize: 12,
            }}
          />
        </div>
        <div>
          <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Sample Output</Text>
          <Input.TextArea
            readOnly
            rows={14}
            value={JSON.stringify(validationResult?.sample_output || [], null, 2)}
            style={{
              marginTop: 6,
              background: 'var(--app-input-bg)',
              border: '1px solid var(--app-border-strong)',
              color: 'var(--app-text)',
              fontFamily: 'monospace',
              fontSize: 12,
            }}
          />
        </div>
      </Space>
    </Modal>
    {isLmdbSource && (
      <Modal
        open={lmdbStudioOpen}
        onCancel={() => setLmdbStudioOpen(false)}
        footer={null}
        closable={false}
        maskClosable
        centered
        width="96vw"
        styles={{
          content: {
            padding: 0,
            borderRadius: 12,
            overflow: 'hidden',
            border: '1px solid var(--app-border-strong)',
            background: 'var(--app-panel-bg)',
            height: '96vh',
            display: 'flex',
            flexDirection: 'column',
          },
          body: {
            padding: 0,
            flex: 1,
            minHeight: 0,
            display: 'flex',
            flexDirection: 'column',
          },
        }}
      >
        <div
          style={{
            borderBottom: '1px solid var(--app-border-strong)',
            background: 'var(--app-card-bg)',
            padding: '12px 16px',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: 12,
          }}
        >
          <div>
            <Text style={{ color: 'var(--app-text)', fontWeight: 700, fontSize: 16 }}>
              {kvSourceLabel} Source Studio
            </Text>
            <br />
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
              Browse {kvSourceLabel} environment, set key filters, and query live source preview.
            </Text>
          </div>
          <Space>
            <Button
              onClick={() => { void runLmdbPreview({ page: 1, pageSize: lmdbTablePageSize }) }}
              loading={lmdbStudioLoading}
              style={{ background: '#14b8a61a', border: '1px solid #14b8a640', color: '#14b8a6' }}
            >
              Query Preview
            </Button>
            <Button
              danger
              loading={lmdbDeleteLoading}
              disabled={lmdbStudioLoading}
              onClick={() => confirmDeleteLmdbData('filtered')}
            >
              Delete Matching
            </Button>
            <Button
              danger
              loading={lmdbDeleteLoading}
              disabled={lmdbStudioLoading}
              onClick={() => confirmDeleteLmdbData('all')}
            >
              Delete All
            </Button>
            <Button onClick={() => setLmdbStudioOpen(false)}>Close</Button>
            <Button
              type="primary"
              onClick={saveLmdbStudio}
              style={{ background: 'linear-gradient(135deg, #14b8a6, #0f766e)', border: 'none' }}
            >
              Save {kvSourceLabel} Config
            </Button>
          </Space>
        </div>

        <div style={{ display: 'flex', flex: 1, minHeight: 0 }}>
          <div
            style={{
              width: '30%',
              minWidth: 320,
              borderRight: '1px solid var(--app-border-strong)',
              padding: 12,
              overflowY: 'auto',
            }}
          >
            <Space direction="vertical" size={4} style={{ width: '100%' }}>
              <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Query Configuration</Text>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                Set environment and key/value filters.
              </Text>
            </Space>

            <div style={{ marginTop: 8, display: 'grid', gap: 8 }}>
              <div style={{ display: 'grid', gridTemplateColumns: isRocksdbSource ? '1fr' : '1fr 1fr', gap: 8 }}>
                <div>
                  <Space size={6} style={{ width: '100%', justifyContent: 'space-between' }}>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Prelist Env Path</Text>
                    <Button
                      size="small"
                      loading={lmdbPathBrowseLoading}
                      onClick={() => { void loadLmdbEnvPathOptions(String(lmdbStudioDraft.env_path || '')) }}
                    >
                      Browse
                    </Button>
                  </Space>
                  <Select
                    size="small"
                    showSearch
                    allowClear
                    placeholder="Select env path preset"
                    options={lmdbEnvPathPresetOptions}
                    onChange={(value) => {
                      if (typeof value !== 'string' || !value) return
                      setLmdbStudioDraft((prev) => ({ ...prev, env_path: value }))
                    }}
                    style={{ width: '100%', marginTop: 4 }}
                  />
                </div>
                {!isRocksdbSource ? (
                  <div>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Prelist DB Name</Text>
                    <Select
                      size="small"
                      showSearch
                      allowClear
                      placeholder="Select DB preset"
                      options={lmdbDbNamePresetOptions}
                      onChange={(value) => {
                        setLmdbStudioDraft((prev) => ({ ...prev, db_name: String(value ?? '') }))
                      }}
                      style={{ width: '100%', marginTop: 4 }}
                    />
                  </div>
                ) : null}
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Prelist Key Prefix</Text>
                  <Select
                    size="small"
                    showSearch
                    allowClear
                    placeholder="Select prefix preset"
                    options={lmdbKeyPrefixPresetOptions}
                    onChange={(value) => {
                      if (typeof value !== 'string' || !value) return
                      setLmdbStudioDraft((prev) => ({ ...prev, key_prefix: value }))
                    }}
                    style={{ width: '100%', marginTop: 4 }}
                  />
                </div>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Prelist Start Key</Text>
                  <Select
                    size="small"
                    showSearch
                    allowClear
                    placeholder="Select start key"
                    options={lmdbKeyPresetOptions}
                    onChange={(value) => {
                      if (typeof value !== 'string' || !value) return
                      setLmdbStudioDraft((prev) => ({ ...prev, start_key: value }))
                    }}
                    style={{ width: '100%', marginTop: 4 }}
                  />
                </div>
              </div>

              <div>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Prelist End Key</Text>
                <Select
                  size="small"
                  showSearch
                  allowClear
                  placeholder="Select end key"
                  options={lmdbKeyPresetOptions}
                  onChange={(value) => {
                    if (typeof value !== 'string' || !value) return
                    setLmdbStudioDraft((prev) => ({ ...prev, end_key: value }))
                  }}
                  style={{ width: '100%', marginTop: 4 }}
                />
              </div>

              <div>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>{kvSourceLabel} Environment Path</Text>
                <Input
                  size="small"
                  value={lmdbStudioDraft.env_path}
                  placeholder={kvPathPlaceholder}
                  onChange={(e) => setLmdbStudioDraft((prev) => ({ ...prev, env_path: e.target.value }))}
                  style={{ ...commonInputStyle, marginTop: 4 }}
                />
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: isRocksdbSource ? '1fr' : '1fr 1fr', gap: 8 }}>
                {!isRocksdbSource ? (
                  <div>
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Named DB</Text>
                    <Input
                      size="small"
                      value={lmdbStudioDraft.db_name}
                      placeholder="profiles"
                      onChange={(e) => setLmdbStudioDraft((prev) => ({ ...prev, db_name: e.target.value }))}
                      style={{ ...commonInputStyle, marginTop: 4 }}
                    />
                  </div>
                ) : null}
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Key Prefix</Text>
                  <Input
                    size="small"
                    value={lmdbStudioDraft.key_prefix}
                    placeholder="pipeline:abc:"
                    onChange={(e) => setLmdbStudioDraft((prev) => ({ ...prev, key_prefix: e.target.value }))}
                    style={{ ...commonInputStyle, marginTop: 4 }}
                  />
                </div>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Start Key</Text>
                  <Input
                    size="small"
                    value={lmdbStudioDraft.start_key}
                    onChange={(e) => setLmdbStudioDraft((prev) => ({ ...prev, start_key: e.target.value }))}
                    style={{ ...commonInputStyle, marginTop: 4 }}
                  />
                </div>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>End Key</Text>
                  <Input
                    size="small"
                    value={lmdbStudioDraft.end_key}
                    onChange={(e) => setLmdbStudioDraft((prev) => ({ ...prev, end_key: e.target.value }))}
                    style={{ ...commonInputStyle, marginTop: 4 }}
                  />
                </div>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Key Contains</Text>
                  <Input
                    size="small"
                    value={lmdbStudioDraft.key_contains}
                    onChange={(e) => setLmdbStudioDraft((prev) => ({ ...prev, key_contains: e.target.value }))}
                    style={{ ...commonInputStyle, marginTop: 4 }}
                  />
                </div>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Value Contains</Text>
                  <Input
                    size="small"
                    value={lmdbStudioDraft.value_contains}
                    onChange={(e) => setLmdbStudioDraft((prev) => ({ ...prev, value_contains: e.target.value }))}
                    style={{ ...commonInputStyle, marginTop: 4 }}
                  />
                </div>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, alignItems: 'end' }}>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Decode</Text>
                  <Select
                    size="small"
                    value={lmdbStudioDraft.value_format}
                    onChange={(value) => setLmdbStudioDraft((prev) => ({
                      ...prev,
                      value_format: value as 'auto' | 'json' | 'text' | 'base64',
                    }))}
                    options={[
                      { value: 'auto', label: 'Auto' },
                      { value: 'json', label: 'JSON' },
                      { value: 'text', label: 'Text' },
                      { value: 'base64', label: 'Base64' },
                    ]}
                    style={{ width: '100%', marginTop: 4 }}
                  />
                </div>
                <div>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Limit</Text>
                  <InputNumber
                    size="small"
                    value={lmdbStudioDraft.limit}
                    min={0}
                    onChange={(value) => setLmdbStudioDraft((prev) => ({
                      ...prev,
                      limit: Math.max(0, Number.isFinite(Number(value)) ? Math.floor(Number(value)) : 0),
                    }))}
                    style={{ width: '100%', marginTop: 4 }}
                  />
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 10 }}>
                    0 = No limit (read all matching records)
                  </Text>
                </div>
              </div>

              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  background: 'var(--app-shell-bg)',
                  border: '1px solid var(--app-border)',
                  borderRadius: 6,
                  padding: '6px 8px',
                }}
              >
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Flatten JSON object values</Text>
                <Switch
                  size="small"
                  checked={lmdbStudioDraft.flatten_json_values}
                  onChange={(checked) => setLmdbStudioDraft((prev) => ({ ...prev, flatten_json_values: checked }))}
                />
              </div>

              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  background: 'var(--app-shell-bg)',
                  border: '1px solid var(--app-border)',
                  borderRadius: 6,
                  padding: '6px 8px',
                }}
              >
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  Expand profile documents (one row per profile)
                </Text>
                <Switch
                  size="small"
                  checked={lmdbStudioDraft.expand_profile_documents}
                  onChange={(checked) => setLmdbStudioDraft((prev) => ({ ...prev, expand_profile_documents: checked }))}
                />
              </div>
            </div>

            {lmdbPathBrowseError ? (
              <div
                style={{
                  marginTop: 8,
                  background: '#ef44441a',
                  border: '1px solid #ef444440',
                  borderRadius: 8,
                  padding: '6px 8px',
                }}
              >
                <Text style={{ color: '#fca5a5', fontSize: 12 }}>{lmdbPathBrowseError}</Text>
              </div>
            ) : null}

            {lmdbStudioError ? (
              <div
                style={{
                  marginTop: 8,
                  background: '#ef44441a',
                  border: '1px solid #ef444440',
                  borderRadius: 8,
                  padding: '6px 8px',
                }}
              >
                <Text style={{ color: '#fca5a5', fontSize: 12 }}>{lmdbStudioError}</Text>
              </div>
            ) : null}
          </div>

          <div style={{ flex: 1, padding: 14, overflowY: 'auto' }}>
            <Space style={{ justifyContent: 'space-between', width: '100%', marginBottom: 8 }}>
              <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Preview Result</Text>
              <Tag style={{ background: '#14b8a614', border: '1px solid #14b8a630', color: '#14b8a6' }}>
                {Number(lmdbStudioRowCount || lmdbStudioPreviewRows.length).toLocaleString()}
                {lmdbPreviewHasMore ? '+' : ''} rows
              </Tag>
            </Space>

            {lmdbStudioColumns.length > 0 ? (
              <div style={{ marginBottom: 10 }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Detected Columns</Text>
                <div style={{ marginTop: 6, display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                  {lmdbStudioColumns.map((name) => (
                    <Tag
                      key={`lmdb_col_${name}`}
                      style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }}
                    >
                      {name}
                    </Tag>
                  ))}
                </div>
              </div>
            ) : null}

            <div>
              <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Preview Rows</Text>
              <Tabs
                activeKey={lmdbPreviewView}
                onChange={(key) => setLmdbPreviewView(key as 'table' | 'json' | 'summary')}
                size="small"
                style={{ marginTop: 6 }}
                items={[
                  {
                    key: 'summary',
                    label: 'Summary',
                    children: (
                      <div style={{ display: 'grid', gap: 12 }}>
                        <div
                          style={{
                            border: '1px solid var(--app-border-strong)',
                            borderRadius: 8,
                            background: 'var(--app-card-bg)',
                            padding: 10,
                          }}
                        >
                          <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                            <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Complete Data Highlights</Text>
                            {lmdbSummaryLoading ? (
                              <Tag style={{ margin: 0, background: '#2563eb14', border: '1px solid #2563eb3a', color: '#93c5fd' }}>
                                Refreshing
                              </Tag>
                            ) : null}
                          </Space>
                          {lmdbSummaryError ? (
                            <div
                              style={{
                                marginTop: 8,
                                background: '#f59e0b14',
                                border: '1px solid #f59e0b40',
                                borderRadius: 6,
                                padding: '6px 8px',
                              }}
                            >
                              <Text style={{ color: '#fcd34d', fontSize: 11 }}>
                                Summary fallback in use: {lmdbSummaryError}
                              </Text>
                            </div>
                          ) : null}
                          <div
                            style={{
                              marginTop: 8,
                              display: 'grid',
                              gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
                              gap: 8,
                            }}
                          >
                            <div style={{ padding: 8, border: '1px solid var(--app-border)', borderRadius: 6, background: 'var(--app-shell-bg)' }}>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Records Processed (Complete)</Text>
                              <br />
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>
                                {Number(lmdbSummaryStats.totalRows || 0).toLocaleString()}
                              </Text>
                            </div>
                            <div style={{ padding: 8, border: '1px solid var(--app-border)', borderRadius: 6, background: 'var(--app-shell-bg)' }}>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Custom Fields Incremental (Live)</Text>
                              <br />
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>
                                {Number(lmdbSummaryStats.customFieldsIncrementalProcessedRows || 0).toLocaleString()}
                                {' '}processed
                              </Text>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 10, display: 'block' }}>
                                {Number(lmdbSummaryStats.customFieldsIncrementalValidatedRows || 0).toLocaleString()} validated
                                {' · '}
                                {Number(lmdbSummaryStats.customFieldsIncrementalOutputRows || 0).toLocaleString()} output
                              </Text>
                            </div>
                            <div style={{ padding: 8, border: '1px solid var(--app-border)', borderRadius: 6, background: 'var(--app-shell-bg)' }}>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Entries Scanned</Text>
                              <br />
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>
                                {Number(lmdbSummaryStats.scannedEntries || 0).toLocaleString()}
                              </Text>
                            </div>
                            <div style={{ padding: 8, border: '1px solid var(--app-border)', borderRadius: 6, background: 'var(--app-shell-bg)' }}>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Detected Columns</Text>
                              <br />
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>{Number(lmdbSummaryStats.detectedColumns || 0).toLocaleString()}</Text>
                            </div>
                            <div style={{ padding: 8, border: '1px solid var(--app-border)', borderRadius: 6, background: 'var(--app-shell-bg)' }}>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Unique {kvSourceLabelUpper} Keys (Complete)</Text>
                              <br />
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>{Number(lmdbSummaryStats.uniqueKeys || 0).toLocaleString()}</Text>
                            </div>
                            <div style={{ padding: 8, border: '1px solid var(--app-border)', borderRadius: 6, background: 'var(--app-shell-bg)' }}>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Distinct Entities (Complete)</Text>
                              <br />
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>{Number(lmdbSummaryStats.uniqueEntities || 0).toLocaleString()}</Text>
                            </div>
                            <div style={{ padding: 8, border: '1px solid var(--app-border)', borderRadius: 6, background: 'var(--app-shell-bg)' }}>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Profile Rows (Complete)</Text>
                              <br />
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>{Number(lmdbSummaryStats.profileRows || 0).toLocaleString()}</Text>
                            </div>
                            <div style={{ padding: 8, border: '1px solid var(--app-border)', borderRadius: 6, background: 'var(--app-shell-bg)' }}>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Non-Profile Rows (Complete)</Text>
                              <br />
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>{Number(lmdbSummaryStats.nonProfileRows || 0).toLocaleString()}</Text>
                            </div>
                            <div style={{ padding: 8, border: '1px solid var(--app-border)', borderRadius: 6, background: 'var(--app-shell-bg)' }}>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Profile-like Keys (Complete)</Text>
                              <br />
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>{Number(lmdbSummaryStats.profileLikeKeys || 0).toLocaleString()}</Text>
                            </div>
                            <div style={{ padding: 8, border: '1px solid var(--app-border)', borderRadius: 6, background: 'var(--app-shell-bg)' }}>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Processing Latency (records/sec)</Text>
                              <br />
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>
                                {lmdbSummaryStats.processingLatencyRps !== null
                                  ? Number(lmdbSummaryStats.processingLatencyRps).toLocaleString(undefined, { maximumFractionDigits: 2 })
                                  : 'N/A'}
                              </Text>
                              {lmdbSummaryStats.scanElapsedSeconds !== null ? (
                                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 10, display: 'block' }}>
                                  scan time: {Number(lmdbSummaryStats.scanElapsedSeconds).toLocaleString(undefined, { maximumFractionDigits: 2 })}s
                                </Text>
                              ) : null}
                            </div>
                          </div>
                          {lmdbSummaryStats.scanCapped ? (
                            <Text style={{ color: '#fbbf24', fontSize: 11, marginTop: 8, display: 'block' }}>
                              Summary scan is capped at {Number(lmdbSummaryStats.scanLimit || 0).toLocaleString()} matched rows.
                            </Text>
                          ) : null}
                        </div>
                      </div>
                    ),
                  },
                  {
                    key: 'table',
                    label: 'Data Table',
                    children: (
                      <div>
                        <div
                          style={{
                            marginBottom: 10,
                            padding: '8px 10px',
                            border: '1px solid var(--app-border)',
                            borderRadius: 8,
                            background: 'var(--app-shell-bg)',
                            display: 'flex',
                            flexWrap: 'wrap',
                            gap: 10,
                            alignItems: 'center',
                          }}
                        >
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Columns</Text>
                          <Select
                            mode="multiple"
                            size="small"
                            value={lmdbVisibleColumns}
                            options={lmdbAllPreviewColumns.map((name) => ({ value: name, label: name }))}
                            onChange={(values) => {
                              const normalized = uniqueFieldNames(
                                (values || []).map((item) => String(item || '').trim()).filter(Boolean)
                              )
                              setLmdbVisibleColumns(normalized)
                            }}
                            maxTagCount={6}
                            allowClear
                            style={{ minWidth: 360, flex: '1 1 360px' }}
                            placeholder="Select columns to show/hide"
                          />
                          <Button
                            size="small"
                            onClick={() => setLmdbVisibleColumns(lmdbAllPreviewColumns)}
                          >
                            Show All
                          </Button>
                          <Divider type="vertical" style={{ borderColor: 'var(--app-border-strong)' }} />
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Global Filter</Text>
                          <Select
                            size="small"
                            value={lmdbGlobalFilterColumn}
                            onChange={(value) => {
                              const nextColumn = String(value || LMDB_GLOBAL_ALL_COLUMNS)
                              setLmdbGlobalFilterColumn(nextColumn)
                              setLmdbGlobalFilterValues([])
                              void runLmdbPreview({
                                page: 1,
                                pageSize: lmdbTablePageSize,
                                quiet: true,
                                globalFilterColumnOverride: nextColumn,
                                globalFilterValuesOverride: [],
                              })
                            }}
                            options={lmdbGlobalFilterColumnOptions}
                            style={{ width: 190 }}
                          />
                          <Select
                            size="small"
                            mode="tags"
                            showSearch
                            allowClear
                            maxTagCount="responsive"
                            value={lmdbGlobalFilterValues}
                            onChange={(values) => {
                              const nextValues = uniqueFieldNames(
                                (values || []).map((v) => String(v || '').trim()).filter(Boolean)
                              )
                              setLmdbGlobalFilterValues(nextValues)
                              void runLmdbPreview({
                                page: 1,
                                pageSize: lmdbTablePageSize,
                                quiet: true,
                                globalFilterColumnOverride: lmdbGlobalFilterColumn,
                                globalFilterValuesOverride: nextValues,
                              })
                            }}
                            options={lmdbGlobalFilterValueOptions}
                            placeholder="Type/select multiple values"
                            optionFilterProp="label"
                            style={{ minWidth: 220, flex: '1 1 220px' }}
                          />
                          <Button
                            size="small"
                            onClick={() => {
                              setLmdbGlobalFilterColumn(LMDB_GLOBAL_ALL_COLUMNS)
                              setLmdbGlobalFilterValues([])
                              setLmdbColumnFilterValues({})
                              void runLmdbPreview({
                                page: 1,
                                pageSize: lmdbTablePageSize,
                                quiet: true,
                                globalFilterColumnOverride: LMDB_GLOBAL_ALL_COLUMNS,
                                globalFilterValuesOverride: [],
                                columnFiltersOverride: {},
                              })
                            }}
                          >
                            Clear Filters
                          </Button>
                          <Divider type="vertical" style={{ borderColor: 'var(--app-border-strong)' }} />
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Display Rows</Text>
                          <InputNumber
                            size="small"
                            min={1}
                            max={2000}
                            value={lmdbPreviewDisplayLimit}
                            onChange={(value) => setLmdbPreviewDisplayLimit(Math.max(1, Math.min(Number(value || 1), 2000)))}
                            style={{ width: 110 }}
                          />
                          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Page Size</Text>
                          <Select
                            size="small"
                            value={String(lmdbTablePageSize)}
                            onChange={(value) => {
                              const nextSize = Math.max(5, Math.min(Number(value || 25), 500))
                              setLmdbTablePageSize(nextSize)
                              setLmdbTableCurrentPage(1)
                              void runLmdbPreview({
                                page: 1,
                                pageSize: nextSize,
                                quiet: true,
                              })
                            }}
                            options={['10', '25', '50', '100', '200'].map((value) => ({ value, label: value }))}
                            style={{ width: 90 }}
                          />
                        </div>
                        <div style={{ border: '1px solid var(--app-border-strong)', borderRadius: 8, overflow: 'hidden' }}>
                          <Table
                            size="small"
                            scroll={{ x: 'max-content', y: 560 }}
                            columns={lmdbPreviewTableColumns}
                            dataSource={lmdbPreviewTableDataGlobalFiltered}
                            rowKey="__lmdb_preview_row_id"
                            onRow={(record) => ({
                              onClick: () => {
                                const rowIndexRaw = (record as Record<string, unknown>)?.__lmdb_preview_row_index
                                const rowIndex = Number.isFinite(Number(rowIndexRaw)) ? Number(rowIndexRaw) : null
                                const rowPayload: Record<string, unknown> = {}
                                Object.keys(record || {}).forEach((key) => {
                                  if (!String(key).startsWith('__lmdb_preview_')) {
                                    rowPayload[key] = (record as Record<string, unknown>)[key]
                                  }
                                })
                                openLmdbRowJsonEditor(rowIndex, rowPayload)
                              },
                            })}
                            pagination={{
                              current: lmdbTableCurrentPage,
                              total: lmdbPreviewHasMore
                                ? Math.max(
                                  Number(lmdbStudioRowCount || 0),
                                  (Math.max(1, Number(lmdbTableCurrentPage || 1)) * Math.max(1, Number(lmdbTablePageSize || 25))) + 1
                                )
                                : Math.max(Number(lmdbStudioRowCount || 0), lmdbPreviewTableDataGlobalFiltered.length),
                              pageSize: lmdbTablePageSize,
                              showSizeChanger: true,
                              pageSizeOptions: ['10', '25', '50', '100', '200'],
                            }}
                            onChange={(pagination, filters, _sorter, extra) => {
                              const normalizedFilters: Record<string, string[]> = {}
                              Object.entries(filters || {}).forEach(([name, values]) => {
                                if (!Array.isArray(values)) return
                                const normalized = uniqueFieldNames(values.map((v) => String(v || '')).filter(Boolean))
                                if (normalized.length > 0) normalizedFilters[name] = normalized
                              })
                              setLmdbColumnFilterValues(normalizedFilters)
                              if (extra?.action === 'filter') {
                                void runLmdbPreview({
                                  page: 1,
                                  pageSize: lmdbTablePageSize,
                                  quiet: true,
                                  columnFiltersOverride: normalizedFilters,
                                })
                                return
                              }
                              if (extra?.action !== 'paginate') return
                              const safePageSize = Math.max(5, Math.min(Number((pagination as any)?.pageSize || lmdbTablePageSize || 25), 500))
                              const sizeChanged = safePageSize !== lmdbTablePageSize
                              const targetPage = sizeChanged ? 1 : Math.max(1, Number((pagination as any)?.current || 1))
                              setLmdbTablePageSize(safePageSize)
                              setLmdbTableCurrentPage(targetPage)
                              void runLmdbPreview({
                                page: targetPage,
                                pageSize: safePageSize,
                                quiet: true,
                                columnFiltersOverride: normalizedFilters,
                              })
                            }}
                          />
                        </div>
                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                          Tip: use Global Filter + column header filters. Click any cell for cell JSON, or click row background / Row JSON for full row JSON.
                        </Text>
                      </div>
                    ),
                  },
                  {
                    key: 'json',
                    label: 'JSON',
                    children: (
                      <div style={{ border: '1px solid var(--app-border-strong)', borderRadius: 8, overflow: 'hidden' }}>
                        <Editor
                          language="json"
                          value={JSON.stringify(lmdbPreviewRowsLimited, null, 2)}
                          theme="vs-dark"
                          height="560px"
                          options={{
                            readOnly: true,
                            minimap: { enabled: false },
                            fontSize: 12,
                            scrollBeyondLastLine: false,
                            wordWrap: 'off',
                            lineNumbers: 'on',
                            renderLineHighlight: 'line',
                            automaticLayout: true,
                          }}
                        />
                      </div>
                    ),
                  },
                ]}
              />
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                Server pagination enabled. Page {lmdbTableCurrentPage} shows {lmdbPreviewTableDataGlobalFiltered.length.toLocaleString()} filtered row(s) with incremental fetch.
              </Text>
            </div>
          </div>
        </div>
      </Modal>
    )}
    {isLmdbSource && (
      <Modal
        open={lmdbJsonEditorOpen}
        onCancel={() => {
          setLmdbJsonEditorOpen(false)
          setLmdbJsonEditorError(null)
          setLmdbJsonTagFilter('')
        }}
        onOk={saveLmdbJsonEditor}
        okText="Apply"
        width="96vw"
        centered
        title={lmdbJsonEditorTitle || (lmdbJsonEditorMode === 'row' ? 'Edit Full Row JSON' : 'Edit Cell JSON')}
        styles={{
          content: {
            borderRadius: 12,
            overflow: 'hidden',
            background: 'var(--app-panel-bg)',
            border: '1px solid var(--app-border-strong)',
            height: '96vh',
            display: 'flex',
            flexDirection: 'column',
          },
          header: {
            background: 'var(--app-card-bg)',
            borderBottom: '1px solid var(--app-border-strong)',
          },
          body: {
            background: 'var(--app-panel-bg)',
            flex: 1,
            minHeight: 0,
            overflow: 'hidden',
          },
        }}
      >
        <Space direction="vertical" size={8} style={{ width: '100%', height: '100%', display: 'flex' }}>
          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
            Mode: {lmdbJsonEditorMode === 'row' ? 'Full Row JSON' : `Cell JSON (${lmdbJsonEditorField || '-'})`}
            {lmdbJsonEditorRowIndex !== null ? ` | Row #${lmdbJsonEditorRowIndex + 1}` : ''}
          </Text>
          <Space wrap size={8}>
            <Button size="small" onClick={formatLmdbJsonEditor}>Format</Button>
            <Button size="small" onClick={minifyLmdbJsonEditor}>Minify</Button>
            <Button size="small" onClick={resetLmdbJsonEditor}>Reset</Button>
          </Space>
          <div style={{ border: '1px solid var(--app-border-strong)', borderRadius: 8, overflow: 'hidden' }}>
            <Editor
              language="json"
              value={lmdbJsonEditorText}
              onChange={(value) => {
                setLmdbJsonEditorText(value || '')
                if (lmdbJsonEditorError) setLmdbJsonEditorError(null)
              }}
              theme="vs-dark"
              height="46vh"
              options={{
                minimap: { enabled: false },
                fontSize: 12,
                scrollBeyondLastLine: false,
                wordWrap: 'off',
                lineNumbers: 'on',
                renderLineHighlight: 'line',
                automaticLayout: true,
              }}
            />
          </div>
          <div
            style={{
              border: '1px solid var(--app-border-strong)',
              borderRadius: 8,
              background: 'var(--app-card-bg)',
              padding: 10,
            }}
          >
            <Space style={{ justifyContent: 'space-between', width: '100%' }} align="start" wrap>
              <div>
                <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>JSON Control Tags & Mappings</Text>
                <br />
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  Color-coded paths with ready mapping snippets for `field()`, `values()`, and control tags.
                </Text>
              </div>
              <Tag style={{ marginInlineEnd: 0 }}>
                {lmdbJsonEditorFilteredPathMappings.length.toLocaleString()} tags
              </Tag>
            </Space>
            <Input
              size="small"
              value={lmdbJsonTagFilter}
              onChange={(e) => setLmdbJsonTagFilter(e.target.value)}
              placeholder="Filter by path / type / sample value"
              style={{ marginTop: 8 }}
            />
            <div
              style={{
                marginTop: 8,
                maxHeight: '30vh',
                overflowY: 'auto',
                display: 'grid',
                gap: 6,
              }}
            >
              {lmdbJsonEditorFilteredPathMappings.length === 0 ? (
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  {lmdbJsonEditorHasParseError
                    ? 'JSON syntax is invalid. Fix editor JSON to populate mappings.'
                    : 'No JSON mappings detected. Ensure editor content is a JSON object/array.'}
                </Text>
              ) : (
                lmdbJsonEditorFilteredPathMappings.map((item) => (
                  <div
                    key={`${item.path}_${item.kind}`}
                    style={{
                      border: '1px solid var(--app-border-strong)',
                      borderRadius: 8,
                      padding: '6px 8px',
                      background: 'var(--app-shell-bg)',
                      display: 'grid',
                      gap: 6,
                    }}
                  >
                    <Space size={6} wrap>
                      <Tag
                        color={
                          item.kind === 'string'
                            ? 'blue'
                            : item.kind === 'number'
                              ? 'geekblue'
                              : item.kind === 'boolean'
                                ? 'green'
                                : item.kind === 'object'
                                  ? 'purple'
                                  : item.kind === 'array'
                                    ? 'gold'
                                    : 'default'
                        }
                      >
                        {item.kind}
                      </Tag>
                      <Tag style={{ marginInlineEnd: 0 }}>{item.path}</Tag>
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                        sample: {item.sample}
                      </Text>
                    </Space>
                    <Space size={6} wrap>
                      <Button size="small" onClick={() => { void copyLmdbJsonSnippet(item.fieldExpr, 'field() mapping') }}>
                        Copy `field()`
                      </Button>
                      <Button size="small" onClick={() => { void copyLmdbJsonSnippet(item.valuesExpr, 'values() mapping') }}>
                        Copy `values()`
                      </Button>
                      <Button size="small" onClick={() => { void copyLmdbJsonSnippet(item.controlTag, 'control tag') }}>
                        Copy tag
                      </Button>
                    </Space>
                  </div>
                ))
              )}
            </div>
          </div>
          {lmdbJsonEditorError ? (
            <Text style={{ color: '#fca5a5', fontSize: 12 }}>{lmdbJsonEditorError}</Text>
          ) : (
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
              Apply updates preview data only. Save {kvSourceLabel} Config to persist source settings.
            </Text>
          )}
        </Space>
      </Modal>
    )}
    {nodeType === 'oracle_destination' && (
      <Modal
        open={oracleStudioOpen}
        onCancel={() => setOracleStudioOpen(false)}
        footer={null}
        closable={false}
        maskClosable
        centered
        width="96vw"
        styles={{
          content: {
            padding: 0,
            borderRadius: 12,
            overflow: 'hidden',
            border: '1px solid var(--app-border-strong)',
            background: 'var(--app-panel-bg)',
            height: '96vh',
            display: 'flex',
            flexDirection: 'column',
          },
          body: {
            padding: 0,
            flex: 1,
            minHeight: 0,
            display: 'flex',
            flexDirection: 'column',
          },
        }}
      >
        <div
          style={{
            borderBottom: '1px solid var(--app-border-strong)',
            background: 'var(--app-card-bg)',
            padding: '12px 16px',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: 12,
          }}
        >
          <div>
            <Text style={{ color: 'var(--app-text)', fontWeight: 700, fontSize: 16 }}>
              Oracle Destination Studio
            </Text>
            <br />
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
              Map source columns to Oracle destination columns and run pre/post SQL scripts.
            </Text>
          </div>
          <Space>
            <Switch
              checked={oracleOnlyMappedDraft}
              onChange={setOracleOnlyMappedDraft}
            />
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Write only mapped columns</Text>
            <Button onClick={() => setOracleStudioOpen(false)}>Close</Button>
            <Button
              type="primary"
              onClick={saveOracleStudio}
              style={{ background: 'linear-gradient(135deg, #f97316, #ea580c)', border: 'none' }}
            >
              Save Oracle Config
            </Button>
          </Space>
        </div>

        <div style={{ display: 'flex', flex: 1, minHeight: 0 }}>
          <div
            style={{
              width: '22%',
              minWidth: 280,
              borderRight: '1px solid var(--app-border-strong)',
              padding: 14,
              overflowY: 'auto',
            }}
          >
            <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Source Fields</Text>
            <br />
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
              Fields inferred from upstream ETL flow connected to this Oracle node.
            </Text>

            <Space direction="vertical" size={8} style={{ width: '100%', marginTop: 10 }}>
              <Button
                onClick={autoMapOracleFromSource}
                style={{ background: '#22c55e1a', border: '1px solid #22c55e40', color: '#22c55e' }}
              >
                Auto-Map Same Names
              </Button>
              <Button
                onClick={() => addOracleMappingDraft()}
                style={{ background: '#f973161a', border: '1px solid #f9731640', color: '#f97316' }}
              >
                Add Mapping Row
              </Button>
            </Space>

            <div style={{ marginTop: 12, display: 'grid', gap: 6 }}>
              {oracleUpstreamFieldOptions.length > 0 ? (
                oracleUpstreamFieldOptions.map((fieldName) => (
                  <Button
                    key={`oracle_src_${fieldName}`}
                    size="small"
                    onClick={() => addOracleMappingDraft({ source: fieldName, destination: fieldName, enabled: true })}
                    style={{
                      textAlign: 'left',
                      justifyContent: 'flex-start',
                      background: 'var(--app-card-bg)',
                      border: '1px solid var(--app-border-strong)',
                      color: 'var(--app-text)',
                    }}
                  >
                    {fieldName}
                  </Button>
                ))
              ) : (
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                  Connect a source/transform node to get field suggestions.
                </Text>
              )}
            </div>
          </div>

          <div style={{ flex: 1, padding: 14, overflowY: 'auto' }}>
            <Space style={{ justifyContent: 'space-between', width: '100%', marginBottom: 8 }}>
              <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Source → Destination Mapping</Text>
              <Tag style={{ background: '#f9731614', border: '1px solid #f9731630', color: '#f97316' }}>
                {oracleMappingDraft.length} rows
              </Tag>
            </Space>

            {oracleMappingDraft.length === 0 ? (
              <div style={{ padding: '24px 0' }}>
                <Text style={{ color: 'var(--app-text-subtle)' }}>
                  No mapping rows added. Use Auto-Map or Add Mapping Row.
                </Text>
              </div>
            ) : (
              oracleMappingDraft.map((row) => (
                <div
                  key={row.id}
                  style={{
                    background: 'var(--app-card-bg)',
                    border: '1px solid var(--app-border-strong)',
                    borderRadius: 10,
                    padding: 10,
                    marginBottom: 10,
                    display: 'grid',
                    gridTemplateColumns: '1fr 28px 1fr 86px 42px',
                    gap: 8,
                    alignItems: 'center',
                  }}
                >
                  <Select
                    showSearch
                    allowClear
                    value={row.source || undefined}
                    placeholder="Source column"
                    options={oracleFieldOptions.map((name) => ({ value: name, label: name }))}
                    onChange={(value) => updateOracleMappingDraft(row.id, { source: String(value || '') })}
                  />
                  <Text style={{ color: 'var(--app-text-subtle)', textAlign: 'center' }}>→</Text>
                  <Input
                    value={row.destination}
                    placeholder="Destination column"
                    onChange={(e) => updateOracleMappingDraft(row.id, { destination: e.target.value })}
                    style={commonInputStyle}
                  />
                  <Space size={4}>
                    <Switch
                      checked={row.enabled}
                      onChange={(value) => updateOracleMappingDraft(row.id, { enabled: value })}
                    />
                    <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>On</Text>
                  </Space>
                  <Button
                    type="text"
                    danger
                    icon={<DeleteOutlined />}
                    onClick={() => removeOracleMappingDraft(row.id)}
                  />
                </div>
              ))
            )}
          </div>

          <div
            style={{
              width: '30%',
              minWidth: 360,
              borderLeft: '1px solid var(--app-border-strong)',
              padding: 14,
              overflowY: 'auto',
            }}
          >
            <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Target Table & Operation</Text>
            <br />
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
              Configure load table and write behavior here (inside Oracle Studio only).
            </Text>

            <div style={{ marginTop: 10 }}>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Target Table</Text>
              <Input
                value={oracleTableDraft}
                placeholder="IBPM_ANALYTICS_AGENT_PROFILE"
                onChange={(e) => setOracleTableDraft(e.target.value)}
                style={{ ...commonInputStyle, marginTop: 6 }}
              />
            </div>

            <div style={{ marginTop: 10 }}>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Operation</Text>
              <Select
                value={oracleOperationDraft}
                onChange={(v) => setOracleOperationDraft(v as 'insert' | 'update' | 'upsert')}
                style={{ width: '100%', marginTop: 6 }}
                options={[
                  { value: 'insert', label: 'Insert (bulk load)' },
                  { value: 'update', label: 'Update (by key columns)' },
                  { value: 'upsert', label: 'Upsert (update else insert)' },
                ]}
              />
            </div>

            {oracleOperationDraft === 'insert' ? (
              <div style={{ marginTop: 10 }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>If Table Exists</Text>
                <Select
                  value={oracleIfExistsDraft}
                  onChange={(v) => setOracleIfExistsDraft(v as 'append' | 'replace' | 'fail')}
                  style={{ width: '100%', marginTop: 6 }}
                  options={[
                    { value: 'append', label: 'Append' },
                    { value: 'replace', label: 'Replace' },
                    { value: 'fail', label: 'Fail' },
                  ]}
                />
              </div>
            ) : (
              <div style={{ marginTop: 10 }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Key Columns</Text>
                <Select
                  mode="multiple"
                  allowClear
                  showSearch
                  value={oracleKeyColumnsDraft}
                  options={oracleFieldOptions.map((name) => ({ value: name, label: name }))}
                  onChange={(values) => setOracleKeyColumnsDraft(uniqueFieldNames((values || []).map((v) => String(v))))}
                  placeholder="Select key columns used in WHERE clause"
                  style={{ width: '100%', marginTop: 6 }}
                />
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11, marginTop: 4, display: 'block' }}>
                  Required for update/upsert. Example: `AGENTCODE`
                </Text>
              </div>
            )}

            <Divider style={{ borderColor: 'var(--app-border)', margin: '14px 0 10px' }} />
            <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>SQL Scripts</Text>
            <br />
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
              Optional SQL scripts executed before and after dataframe load.
            </Text>

            <div style={{ marginTop: 10 }}>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Pre-Load SQL</Text>
              <Input.TextArea
                value={oraclePreSqlDraft}
                rows={8}
                placeholder="ALTER SESSION ...; DELETE FROM target_table WHERE ...;"
                onChange={(e) => setOraclePreSqlDraft(e.target.value)}
                style={{
                  ...commonInputStyle,
                  marginTop: 6,
                  fontFamily: 'monospace',
                  fontSize: 12,
                  resize: 'vertical',
                }}
              />
            </div>

            <div style={{ marginTop: 12 }}>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Post-Load SQL</Text>
              <Input.TextArea
                value={oraclePostSqlDraft}
                rows={8}
                placeholder="MERGE INTO ...; BEGIN pkg.refresh_stats; END;"
                onChange={(e) => setOraclePostSqlDraft(e.target.value)}
                style={{
                  ...commonInputStyle,
                  marginTop: 6,
                  fontFamily: 'monospace',
                  fontSize: 12,
                  resize: 'vertical',
                }}
              />
            </div>

            <div
              style={{
                marginTop: 12,
                background: '#f973160f',
                border: '1px solid #f9731630',
                borderRadius: 8,
                padding: '8px 10px',
              }}
            >
              <Text style={{ color: '#f97316', fontSize: 11, fontWeight: 600 }}>Tips</Text>
              <br />
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                Use `;` to separate multiple SQL statements. Leave scripts empty to skip execution.
              </Text>
            </div>
          </div>
        </div>
      </Modal>
    )}
    </>
  )
}
