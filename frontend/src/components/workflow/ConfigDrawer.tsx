import { useEffect, useMemo, useState } from 'react'
import {
  Drawer, Form, Input, Select, Switch, InputNumber,
  Button, Typography, Space, Tabs, Divider, Tag, Tooltip, notification, Modal
} from 'antd'
import { CloseOutlined, DeleteOutlined, InfoCircleOutlined } from '@ant-design/icons'
import Editor, { type Monaco } from '@monaco-editor/react'
import { useWorkflowStore } from '../../store'
import { SourceFilePicker, DestinationPathPicker } from './FilePicker'
import api from '../../api/client'
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

interface CustomFieldSpec {
  id: string
  name: string
  mode: CustomFieldMode
  expression: string
  jsonTemplate: string
  enabled: boolean
}

interface OracleColumnMappingSpec {
  id: string
  source: string
  destination: string
  enabled: boolean
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

const CUSTOM_FIELD_EXAMPLE_REPOSITORY: CustomFieldExampleItem[] = [
  {
    id: 'value_net_amount',
    title: 'Net Amount (Math)',
    category: 'Math & Text',
    mode: 'value',
    name: 'net_amount',
    expression: "=coalesce(field('AMOUNT'), 0) - coalesce(field('DISCOUNT'), 0) + coalesce(field('TAX'), 0)",
    description: 'Null-safe arithmetic expression.',
    tags: ['math', 'coalesce'],
  },
  {
    id: 'value_case_tier',
    title: 'Tier by Amount (CASE/IF)',
    category: 'Conditional Logic',
    mode: 'value',
    name: 'amount_tier',
    expression: "=if_(coalesce(field('AMOUNT'), 0) >= 10000, 'HIGH', if_(coalesce(field('AMOUNT'), 0) >= 5000, 'MEDIUM', 'LOW'))",
    description: 'Nested if_ to create business tier.',
    tags: ['if_', 'case'],
  },
  {
    id: 'value_sum_amount',
    title: 'Total Amount (SUM)',
    category: 'Aggregation',
    mode: 'value',
    name: 'total_amount',
    expression: "=sum(values('AMOUNT'))",
    description: 'Sum across grouped rows.',
    tags: ['sum'],
  },
  {
    id: 'value_avg_amount',
    title: 'Average Amount (MEAN)',
    category: 'Aggregation',
    mode: 'value',
    name: 'avg_amount',
    expression: "=mean(values('AMOUNT'))",
    description: 'Average across grouped rows.',
    tags: ['mean', 'avg'],
  },
  {
    id: 'value_min_max_amount',
    title: 'Min/Max Amount',
    category: 'Aggregation',
    mode: 'json',
    name: 'amount_range',
    jsonTemplate: `{
  "min_amount": "=min(values('AMOUNT'))",
  "max_amount": "=max(values('AMOUNT'))"
}`,
    description: 'Get minimum and maximum values.',
    tags: ['min', 'max'],
  },
  {
    id: 'value_distinct_customers',
    title: 'Distinct Customer Count',
    category: 'Aggregation',
    mode: 'value',
    name: 'distinct_customer_count',
    expression: "=count(distinct(values('CUSTACCOUNTNUMBER')))",
    description: 'Count unique customers within group.',
    tags: ['distinct', 'count'],
  },
  {
    id: 'value_count_non_null_txn',
    title: 'Transaction Count (Non-null)',
    category: 'Aggregation',
    mode: 'value',
    name: 'transaction_count',
    expression: "=count(values('TRANSACTIONID'))",
    description: 'Count non-null TRANSACTIONID values.',
    tags: ['count'],
  },
  {
    id: 'value_count_if_eq',
    title: 'count_if EQ (Success 00)',
    category: 'Conditional Aggregation',
    mode: 'value',
    name: 'success_rrn_count',
    expression: "=count_if('RRN','RESPTOCLIENT','00')",
    description: "Count RRN where RESPTOCLIENT = '00'.",
    tags: ['count_if', 'eq'],
  },
  {
    id: 'value_sum_if_eq',
    title: 'sum_if EQ (Success Amount)',
    category: 'Conditional Aggregation',
    mode: 'value',
    name: 'success_amount_sum',
    expression: "=sum_if('AMOUNT','RESPTOCLIENT','00')",
    description: "Sum AMOUNT where RESPTOCLIENT = '00'.",
    tags: ['sum_if', 'eq'],
  },
  {
    id: 'value_mean_if_like',
    title: 'mean_if LIKE',
    category: 'Conditional Aggregation',
    mode: 'value',
    name: 'upi_avg_amount',
    expression: "=mean_if('AMOUNT','SERVICENAME','UPI%','like')",
    description: "Average AMOUNT where SERVICENAME LIKE 'UPI%'.",
    tags: ['mean_if', 'like'],
  },
  {
    id: 'value_agg_if_generic',
    title: 'agg_if Generic',
    category: 'Conditional Aggregation',
    mode: 'value',
    name: 'success_amount_max',
    expression: "=agg_if('AMOUNT','RESPTOCLIENT','00','max')",
    description: 'Generic conditional aggregation using agg_if.',
    tags: ['agg_if', 'max'],
  },
  {
    id: 'value_count_if_not',
    title: 'count_if NOT / !=',
    category: 'Conditional Aggregation',
    mode: 'value',
    name: 'non_success_rrn_count',
    expression: "=count_if('RRN','RESPTOCLIENT','00','!=')",
    description: 'Count rows not equal to expected value.',
    tags: ['count_if', 'not', '!='],
  },
  {
    id: 'value_count_if_contains',
    title: 'count_if CONTAINS',
    category: 'Conditional Aggregation',
    mode: 'value',
    name: 'upi_txn_count',
    expression: "=count_if('RRN','SERVICENAME','UPI','contains')",
    description: "Count rows where SERVICENAME contains 'UPI'.",
    tags: ['count_if', 'contains'],
  },
  {
    id: 'value_count_if_like',
    title: 'count_if LIKE',
    category: 'Conditional Aggregation',
    mode: 'value',
    name: 'upi_prefix_count',
    expression: "=count_if('RRN','SERVICENAME','UPI%','like')",
    description: 'SQL-like match using % and _.',
    tags: ['count_if', 'like'],
  },
  {
    id: 'value_count_if_in',
    title: 'count_if IN List',
    category: 'Conditional Aggregation',
    mode: 'value',
    name: 'ok_or_warn_count',
    expression: "=count_if('RRN','RESPTOCLIENT',array('00','05'),'in')",
    description: 'Match multiple values using IN.',
    tags: ['count_if', 'in'],
  },
  {
    id: 'value_count_if_regex',
    title: 'count_if Regex',
    category: 'Conditional Aggregation',
    mode: 'value',
    name: 'upi_regex_count',
    expression: "=count_if('RRN','SERVICENAME','^UPI-','regex')",
    description: 'Regex match for advanced text conditions.',
    tags: ['count_if', 'regex'],
  },
  {
    id: 'value_running_latest',
    title: 'Latest Running Sum',
    category: 'Running & Rolling',
    mode: 'value',
    name: 'latest_running_sum',
    expression: "=last(running_sum(values('AMOUNT')))",
    description: 'Latest cumulative sum.',
    tags: ['running_sum', 'last'],
  },
  {
    id: 'value_rolling_latest',
    title: 'Latest Rolling Mean (7)',
    category: 'Running & Rolling',
    mode: 'value',
    name: 'latest_rolling_mean_7',
    expression: "=last(rolling_mean(values('AMOUNT'), 7))",
    description: 'Latest 7-window rolling mean.',
    tags: ['rolling_mean', 'window'],
  },
  {
    id: 'json_series',
    title: 'Series Output JSON',
    category: 'Running & Rolling',
    mode: 'json',
    name: 'amount_series',
    jsonTemplate: `{
  "running_sum_series": "=running_all(values('AMOUNT'), 'sum')",
  "rolling_mean_7_series": "=rolling_all(values('AMOUNT'), 7, 'mean')",
  "latest_running_sum": "=last(running_sum(values('AMOUNT')))",
  "latest_rolling_mean_7": "=last(rolling_mean(values('AMOUNT'), 7))"
}`,
    description: 'Expose full running/rolling series + latest values.',
    tags: ['running_all', 'rolling_all'],
  },
  {
    id: 'value_group_aggregate_customer',
    title: 'Customer Profile Aggregate',
    category: 'Group Aggregate',
    mode: 'value',
    name: 'customer_profiles',
    expression: "=group_aggregate('CUSTACCOUNTNUMBER', obj(mintime=obj(path='SERVERTIME',agg='min'), maxtime=obj(path='SERVERTIME',agg='max'), total_transaction_count=obj(path='TRANSACTIONID',agg='count_non_null')), 'customer')",
    description: 'Customer-wise min/max time and transaction count.',
    tags: ['group_aggregate', 'min', 'max', 'count_non_null'],
  },
  {
    id: 'value_group_aggregate_service_counts',
    title: 'Service-wise Frequency in Profile',
    category: 'Group Aggregate',
    mode: 'value',
    name: 'customer_profiles',
    expression: "=group_aggregate('CUSTACCOUNTNUMBER', obj(service_wise_count=obj(path='SERVICENAME',agg='value_counts'), total_service_name=obj(path='SERVICENAME',agg='distinct')), 'customer')",
    description: 'Per customer service frequency + distinct service names.',
    tags: ['group_aggregate', 'value_counts', 'distinct'],
  },
  {
    id: 'json_operational_summary',
    title: 'Operational Summary JSON',
    category: 'JSON Templates',
    mode: 'json',
    name: 'ops_summary',
    jsonTemplate: `{
  "success_rrn_count": "=count_if('RRN','RESPTOCLIENT','00')",
  "failure_rrn_count": "=count_if('RRN','RESPTOCLIENT','00','!=')",
  "total_amount": "=sum(values('AMOUNT'))",
  "avg_amount": "=mean(values('AMOUNT'))",
  "status": "=if_(count_if('RRN','RESPTOCLIENT','00','!=') > 0, 'REVIEW', 'OK')"
}`,
    description: 'JSON output for operational KPIs and status.',
    tags: ['json', 'count_if', 'sum', 'mean'],
  },
  {
    id: 'json_text_and_flags',
    title: 'Text Normalization + Flags',
    category: 'Math & Text',
    mode: 'json',
    name: 'text_flags',
    jsonTemplate: `{
  "service_upper": "=upper(field('SERVICENAME'))",
  "service_trimmed": "=trim(field('SERVICENAME'))",
  "is_upi": "=contains(lower(field('SERVICENAME')), 'upi')",
  "risk_band": "=if_(coalesce(field('AMOUNT'),0) >= 10000, 'HIGH', 'NORMAL')"
}`,
    description: 'Text transforms and boolean flags.',
    tags: ['upper', 'lower', 'trim', 'contains'],
  },
  {
    id: 'profile_txn_counter',
    title: 'Profile: Increment Transaction Count',
    category: 'Profile Documents',
    mode: 'value',
    name: 'txn_count',
    expression: "=inc('txn_count', 1)",
    description: 'Stateful increment using previous profile value.',
    tags: ['profile', 'inc', 'streaming'],
  },
  {
    id: 'profile_amount_totals',
    title: 'Profile: Total + Avg Amount',
    category: 'Profile Documents',
    mode: 'json',
    name: 'amount_profile',
    jsonTemplate: `{
  "total_amount": "=inc('total_amount', field('amount'))",
  "txn_count": "=inc('txn_count', 1)",
  "avg_amount": "=safe_div(prev('total_amount',0) + coalesce(field('amount'),0), prev('txn_count',0) + 1, 0)"
}`,
    description: 'Incremental total/count and derived average per entity.',
    tags: ['profile', 'safe_div', 'prev'],
  },
  {
    id: 'profile_txn_type_map',
    title: 'Profile: Transaction Type Frequency',
    category: 'Profile Documents',
    mode: 'value',
    name: 'txn_types',
    expression: "=map_inc('txn_types', field('txn_type'), 1)",
    description: 'Increment map counter by transaction type.',
    tags: ['profile', 'map_inc', 'object'],
  },
  {
    id: 'profile_customer_group_aggregate',
    title: 'Profile: Customer Aggregate Array',
    category: 'Profile Documents',
    mode: 'value',
    name: 'customer_profiles',
    expression: "=group_aggregate('CUSTACCOUNTNUMBER', obj(mintime=obj(path='MIS_TXNDATE',agg='min'), maxtime=obj(path='MIS_TXNDATE',agg='max'), total_transaction_count=obj(path='TRANSACTIONID',agg='count_non_null'), total_service_name=obj(path='SERVICENAME',agg='value_counts')), 'customer')",
    description: 'In profile mode this keeps incremental state and returns batch-like grouped array output.',
    tags: ['profile', 'group_aggregate', 'value_counts', 'count_non_null'],
  },
  {
    id: 'profile_rolling_windows',
    title: 'Profile: 1/7/30 Day Rolling Metrics',
    category: 'Profile Documents',
    mode: 'value',
    name: 'amount_rolling',
    expression: "=rolling_update('amount', field('amount'), field('txn_time'), array(1,7,30))",
    description: 'Rolling count/sum/avg/min/max for profile timeline.',
    tags: ['profile', 'rolling_update', 'windows'],
  },
]

const EXPRESSION_FUNCTION_SNIPPETS: Array<{ label: string; snippet: string }> = [
  { label: 'field(path)', snippet: "field('${1:column_name}')" },
  { label: 'values(path)', snippet: "values('${1:column_name}')" },
  { label: 'coalesce(a,b,...)', snippet: "coalesce(${1:value}, ${2:fallback})" },
  { label: 'if_(cond,yes,no)', snippet: "if_(${1:condition}, ${2:yes}, ${3:no})" },
  { label: 'count_if(value,cond,expected,op)', snippet: "count_if('${1:value_field}','${2:condition_field}','${3:00}','${4:eq}')" },
  { label: 'sum_if(value,cond,expected,op)', snippet: "sum_if('${1:value_field}','${2:condition_field}','${3:00}','${4:eq}')" },
  { label: 'mean_if(value,cond,expected,op)', snippet: "mean_if('${1:value_field}','${2:condition_field}','${3:00}','${4:eq}')" },
  { label: 'min_if(value,cond,expected,op)', snippet: "min_if('${1:value_field}','${2:condition_field}','${3:00}','${4:eq}')" },
  { label: 'max_if(value,cond,expected,op)', snippet: "max_if('${1:value_field}','${2:condition_field}','${3:00}','${4:eq}')" },
  { label: 'agg_if(value,cond,expected,agg,op)', snippet: "agg_if('${1:value_field}','${2:condition_field}','${3:00}','${4:sum}','${5:eq}')" },
  { label: 'distinct_count_if(value,cond,expected,op)', snippet: "distinct_count_if('${1:value_field}','${2:condition_field}','${3:00}','${4:eq}')" },
  { label: 'count_non_null_if(value,cond,expected,op)', snippet: "count_non_null_if('${1:value_field}','${2:condition_field}','${3:00}','${4:eq}')" },
  { label: 'round(x,2)', snippet: 'round(${1:value}, ${2:2})' },
  { label: 'count(values)', snippet: 'count(${1:values})' },
  { label: 'distinct(values)', snippet: 'distinct(${1:values})' },
  { label: 'group_aggregate(key,metrics,name)', snippet: "group_aggregate('${1:key_field}', obj(${2:metric}=obj(path='${3:value_field}',agg='${4:sum}')), '${5:key_name}')" },
  { label: 'value_counts metric', snippet: "group_aggregate('${1:key_field}', obj(service_wise_count=obj(path='${2:SERVICENAME}',agg='value_counts')), '${3:key_name}')" },
  { label: 'running_sum(values)', snippet: "running_sum(field('${1:column_name}'))" },
  { label: 'running_mean(values)', snippet: "running_mean(field('${1:column_name}'))" },
  { label: 'rolling_sum(values,w)', snippet: "rolling_sum(field('${1:column_name}'), ${2:7})" },
  { label: 'rolling_mean(values,w)', snippet: "rolling_mean(field('${1:column_name}'), ${2:7})" },
  { label: 'running_all(values,func)', snippet: "running_all(values('${1:column_name}'), '${2:sum}')" },
  { label: 'rolling_all(values,w,func)', snippet: "rolling_all(values('${1:column_name}'), ${2:7}, '${3:mean}')" },
  { label: 'rolling(values,w,func)', snippet: "rolling(field('${1:column_name}'), ${2:7}, '${3:mean}')" },
  { label: 'last(values)', snippet: "last(${1:running_sum(field('amount'))})" },
  { label: 'max(a,b)', snippet: 'max(${1:a}, ${2:b})' },
  { label: 'min(a,b)', snippet: 'min(${1:a}, ${2:b})' },
  { label: 'mean(list)', snippet: 'mean(${1:values})' },
  { label: 'upper(text)', snippet: 'upper(${1:text})' },
  { label: 'lower(text)', snippet: 'lower(${1:text})' },
  { label: 'trim(text)', snippet: 'trim(${1:text})' },
  { label: 'contains(text,needle)', snippet: 'contains(${1:text}, ${2:needle})' },
  { label: 'array(...)', snippet: 'array(${1:value1}, ${2:value2})' },
  { label: 'obj(...)', snippet: "obj(${1:key}=${2:value})" },
  { label: 'prev(path,default)', snippet: "prev('${1:field_name}', ${2:0})" },
  { label: 'inc(path,amount)', snippet: "inc('${1:txn_count}', ${2:1})" },
  { label: 'map_inc(path,key,amount)', snippet: "map_inc('${1:txn_types}', ${2:field('txn_type')}, ${3:1})" },
  { label: 'profile group_aggregate', snippet: "group_aggregate('${1:key_field}', obj(${2:mintime}=obj(path='${3:MIS_TXNDATE}',agg='min'), ${4:maxtime}=obj(path='${3:MIS_TXNDATE}',agg='max'), ${5:total_transaction_count}=obj(path='${6:TRANSACTIONID}',agg='count_non_null')), '${7:key_name}')" },
  { label: 'append_unique(path,value)', snippet: "append_unique('${1:agent_ids}', ${2:field('agent_id')})" },
  { label: 'safe_div(num,den,default)', snippet: 'safe_div(${1:numerator}, ${2:denominator}, ${3:0})' },
  { label: 'rolling_update(name,value,time,windows)', snippet: "rolling_update('${1:amount}', ${2:field('amount')}, ${3:field('txn_time')}, array(${4:1},${5:7},${6:30}))" },
]

const CUSTOM_FIELD_TIPS_TEXT = `Available functions:
field(path), values(path), coalesce(...), if_(cond,a,b), upper(), lower(), trim(), length(),
round(), abs(), min(), max(), sum(), mean(), count(), distinct(), agg(path,'sum'),
count_if(value_path, condition_path, expected, op, include_null_values, case_sensitive),
sum_if(value_path, condition_path, expected, op, include_null_values, case_sensitive),
mean_if(value_path, condition_path, expected, op, include_null_values, case_sensitive),
min_if(value_path, condition_path, expected, op, include_null_values, case_sensitive),
max_if(value_path, condition_path, expected, op, include_null_values, case_sensitive),
distinct_if(value_path, condition_path, expected, op, include_null_values, case_sensitive),
distinct_count_if(value_path, condition_path, expected, op, include_null_values, case_sensitive),
count_non_null_if(value_path, condition_path, expected, op, include_null_values, case_sensitive),
agg_if(value_path, condition_path, expected, agg, op, include_null_values, case_sensitive),
group_aggregate(key_path, obj(metric=obj(path='field',agg='sum')), 'key_name'),
running_sum(), running_mean(), running_min(), running_max(), running_count(), running_std(),
rolling_sum(values,window), rolling_mean(values,window), rolling_min(), rolling_max(), rolling_count(), rolling_std(),
rolling(values, window, 'mean'), running_all(values,'sum'), rolling_all(values,window,'mean'), last(values),
sqrt(), log(), exp(), ceil(), floor(), sin(), cos(), tan()

Profile document helpers:
prev(path, default), inc(path_or_value, amount, default), map_inc(path_or_map,key,amount,default),
append_unique(path_or_list,value,max_items), safe_div(num,den,default),
rolling_update(name,value,txn_time,windows,retention_days),
group_aggregate(key_path, metrics, key_name) [profile mode keeps incremental state and returns grouped array]

count_if operators:
eq / ==, != / not, contains / not_contains, like / not_like, in / not_in,
startswith / not_startswith, endswith / not_endswith, regex / not_regex

group_aggregate metric aggs:
sum, mean, min, max, count, count_non_null, distinct, distinct_count, value_counts, first, last, row_count

JSON template mode:
Any string starting with "=" is evaluated as expression.
Example:
{
  "score": "=round(coalesce(score,0),2)",
  "status": "=if_(count_if('RRN','RESPTOCLIENT','00','!=') > 0, 'REVIEW', 'OK')",
  "tags": ["=field('SERVICENAME')"]
}

Expression editor autocomplete:
- Functions list
- Source fields and custom fields
Use Ctrl+Space if suggestions are not shown automatically.`

type ExpressionCompletionEntry = {
  label: string
  insertText: string
  kind: 'function' | 'field' | 'keyword'
  detail?: string
  documentation?: string
}

let exprLanguageRegistered = false
let exprCompletionEntries: ExpressionCompletionEntry[] = []

function setExpressionCompletionEntries(entries: ExpressionCompletionEntry[]): void {
  exprCompletionEntries = entries
}

function ensureExpressionLanguage(monaco: Monaco): void {
  if (!exprLanguageRegistered) {
    monaco.languages.register({ id: 'etl-expr' })
    monaco.languages.setLanguageConfiguration('etl-expr', {
      brackets: [['(', ')'], ['[', ']'], ['{', '}']],
      autoClosingPairs: [
        { open: '(', close: ')' },
        { open: '[', close: ']' },
        { open: '{', close: '}' },
        { open: "'", close: "'" },
        { open: '"', close: '"' },
      ],
    })
    monaco.languages.registerCompletionItemProvider('etl-expr', {
      triggerCharacters: ['.', '(', "'", '"', '_'],
      provideCompletionItems(model: any, position: any) {
        const word = model.getWordUntilPosition(position)
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        }
        const prefix = word.word.toLowerCase()
        const items = exprCompletionEntries.filter((entry) => (
          !prefix || entry.label.toLowerCase().includes(prefix)
        ))
        const suggestions = items.map((entry) => ({
          label: entry.label,
          kind:
            entry.kind === 'function'
              ? monaco.languages.CompletionItemKind.Function
              : entry.kind === 'field'
                ? monaco.languages.CompletionItemKind.Field
                : monaco.languages.CompletionItemKind.Keyword,
          insertText: entry.insertText,
          insertTextRules: entry.insertText.includes('${')
            ? monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
            : undefined,
          detail: entry.detail,
          documentation: entry.documentation,
          range,
        }))
        return { suggestions }
      },
    })
    exprLanguageRegistered = true
  }
}

function createCustomFieldSpec(seed?: Partial<CustomFieldSpec>): CustomFieldSpec {
  return {
    id: seed?.id || `cf_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    name: seed?.name || '',
    mode: seed?.mode || 'value',
    expression: seed?.expression || '',
    jsonTemplate: seed?.jsonTemplate || '{\n  "value": "=field(\'id\')"\n}',
    enabled: seed?.enabled ?? true,
  }
}

function createOracleColumnMappingSpec(seed?: Partial<OracleColumnMappingSpec>): OracleColumnMappingSpec {
  return {
    id: seed?.id || `orm_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    source: String(seed?.source || '').trim(),
    destination: String(seed?.destination || '').trim(),
    enabled: seed?.enabled ?? true,
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
    out.push(
      createCustomFieldSpec({
        id: String(rec.id || ''),
        name: String(rec.name || rec.field || ''),
        mode,
        expression: String(rec.expression || rec.expr || ''),
        jsonTemplate: String(rec.jsonTemplate || rec.json_template || rec.template || ''),
        enabled: parseBoolLike(rec.enabled, true),
      })
    )
  })
  return out
}

function serializeCustomFieldSpecs(items: CustomFieldSpec[]): Array<Record<string, unknown>> {
  return items
    .map((item) => ({
      name: String(item.name || '').trim(),
      mode: item.mode,
      expression: item.mode === 'value' ? String(item.expression || '').trim() : '',
      json_template: item.mode === 'json' ? String(item.jsonTemplate || '').trim() : '',
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

function parseFieldList(value: unknown): string[] {
  if (Array.isArray(value)) {
    return uniqueFieldNames(value.map((v) => String(v)))
  }
  if (typeof value === 'string') {
    return uniqueFieldNames(value.split(/[,\n]/).map((p) => p.trim()))
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

  return uniqueFieldNames(fields)
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

  result = uniqueFieldNames(result)
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
  return fields
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

export default function ConfigDrawer({ open, onClose }: ConfigDrawerProps) {
  const { nodes, edges, selectedNodeId, pipeline, updateNodeConfig, updateNodeLabel, removeNode } = useWorkflowStore()
  const [form] = Form.useForm()
  const [activeTab, setActiveTab] = useState('config')
  const [jsonPathDetectLoading, setJsonPathDetectLoading] = useState(false)
  const [jsonPathDetectError, setJsonPathDetectError] = useState<string | null>(null)
  const [sourceFieldDetectLoading, setSourceFieldDetectLoading] = useState(false)
  const [sourceFieldDetectError, setSourceFieldDetectError] = useState<string | null>(null)
  const [customFieldStudioOpen, setCustomFieldStudioOpen] = useState(false)
  const [customFieldDraft, setCustomFieldDraft] = useState<CustomFieldSpec[]>([])
  const [customIncludeSourceDraft, setCustomIncludeSourceDraft] = useState(true)
  const [customPrimaryKeyFieldDraft, setCustomPrimaryKeyFieldDraft] = useState('')
  const [customProfileEnabledDraft, setCustomProfileEnabledDraft] = useState(false)
  const [customProfileEmitModeDraft, setCustomProfileEmitModeDraft] = useState<'changed_only' | 'all_entities'>('changed_only')
  const [customProfileRequiredFieldsDraft, setCustomProfileRequiredFieldsDraft] = useState<string[]>([])
  const [customProfileEventTimeFieldDraft, setCustomProfileEventTimeFieldDraft] = useState('')
  const [customProfileWindowDaysDraft, setCustomProfileWindowDaysDraft] = useState('1,7,30')
  const [customProfileRetentionDaysDraft, setCustomProfileRetentionDaysDraft] = useState(45)
  const [customProfileIncludeChangeFieldsDraft, setCustomProfileIncludeChangeFieldsDraft] = useState(false)
  const [profileMonitorLoading, setProfileMonitorLoading] = useState(false)
  const [profileMonitorClearing, setProfileMonitorClearing] = useState(false)
  const [profileMonitorData, setProfileMonitorData] = useState<ProfileMonitorResponse | null>(null)
  const [profileMonitorError, setProfileMonitorError] = useState<string | null>(null)
  const [expandedEditorFieldId, setExpandedEditorFieldId] = useState<string | null>(null)
  const [expandedEditorMode, setExpandedEditorMode] = useState<CustomFieldMode>('value')
  const [validationLoading, setValidationLoading] = useState(false)
  const [validationResult, setValidationResult] = useState<CustomFieldValidationResult | null>(null)
  const [validationModalOpen, setValidationModalOpen] = useState(false)
  const [activeExpressionFieldId, setActiveExpressionFieldId] = useState<string | null>(null)
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
      updateNodeConfig(selectedNodeId, patch)
    }
  }, [selectedNodeId, data?.nodeType])

  const definition = data?.definition
  const nodeType = data?.nodeType || ''
  const isFileSource = FILE_SOURCE_TYPES.includes(nodeType)
  const isFileDest   = FILE_DEST_TYPES.includes(nodeType)
  const isDatabaseSource = DB_SOURCE_TYPES.includes(nodeType)
  const fileType     = getFileType(nodeType)
  const isApiJsonNode = nodeType === 'rest_api_source' || nodeType === 'graphql_source'
  const nodeConfig = (data?.config && typeof data.config === 'object' ? data.config : {}) as Record<string, unknown>
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
  const schedulePresetValue = nodeType === 'schedule_trigger'
    ? String(nodeConfig.schedule_preset || detectSchedulePresetFromCron(nodeConfig.cron || DEFAULT_SCHEDULE_CRON))
    : ''
  const scheduleCronValue = nodeType === 'schedule_trigger'
    ? normalizeCronExpr(nodeConfig.cron || DEFAULT_SCHEDULE_CRON)
    : ''
  const mapInputFieldOptions = uniqueFieldNames([
    ...parseFieldList(nodeConfig.fields),
    ...(
    nodeType === 'map_transform' && selectedNodeId
      ? inferUpstreamInputFields(selectedNodeId, nodes, edges)
      : []
    ),
  ])
  const upstreamReferenceFields = useMemo(
    () => (
      nodeType === 'map_transform' && selectedNodeId
        ? inferUpstreamInputFields(selectedNodeId, nodes, edges)
        : []
    ),
    [nodeType, selectedNodeId, nodes, edges]
  )
  const expressionFieldOptions = uniqueFieldNames([
    ...upstreamReferenceFields,
    ...mapInputFieldOptions,
    ...customFieldDraft.map((item) => String(item.name || '').trim()).filter(Boolean),
  ])
  const expressionCompletionEntries = useMemo<ExpressionCompletionEntry[]>(() => {
    const out: ExpressionCompletionEntry[] = []
    EXPRESSION_FUNCTION_SNIPPETS.forEach((fn) => {
      out.push({
        label: fn.label,
        insertText: fn.snippet,
        kind: 'function',
        detail: 'ETL Function',
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
    ;[
      { label: 'null', value: 'null' },
      { label: 'true', value: 'true' },
      { label: 'false', value: 'false' },
    ].forEach((kw) => {
      out.push({ label: kw.label, insertText: kw.value, kind: 'keyword', detail: 'Literal' })
    })
    return out
  }, [expressionFieldOptions])
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
  const configuredProfileEnabled = Boolean(nodeConfig.custom_profile_enabled ?? false)
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
  const activePipelineId = String(pipeline?.id || '').trim()
  const activeProfileNodeSummary = useMemo(() => {
    if (!profileMonitorData || !Array.isArray(profileMonitorData.nodes) || !selectedNodeId) return null
    return (
      profileMonitorData.nodes.find((item) => String(item.node_id) === String(selectedNodeId))
      || profileMonitorData.nodes[0]
      || null
    )
  }, [profileMonitorData, selectedNodeId])
  const expandedEditorField = useMemo(
    () => customFieldDraft.find((item) => item.id === expandedEditorFieldId) || null,
    [customFieldDraft, expandedEditorFieldId]
  )
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
    setExpressionCompletionEntries(expressionCompletionEntries)
  }, [expressionCompletionEntries])

  useEffect(() => {
    if (!customFieldStudioOpen || nodeType !== 'map_transform' || !customProfileEnabledDraft) return
    if (!selectedNodeId || !activePipelineId) return
    void refreshProfileMonitor()
  }, [customFieldStudioOpen, customProfileEnabledDraft, nodeType, selectedNodeId, activePipelineId])

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
      if (suggestedPath && !String(nodeConfig.json_path || '').trim()) {
        patch.json_path = suggestedPath
      }

      updateNodeConfig(selectedNodeId, patch)
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
      const response = await api.detectSourceFieldOptions(nodeType, nodeConfig, 300)
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
        updateNodeConfig(selectedNodeId, patch)
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
    setProfileMonitorLoading(true)
    setProfileMonitorError(null)
    try {
      const response = await api.getPipelineProfileState(activePipelineId, selectedNodeId, 12)
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

  const openExpandedEditor = (fieldId: string, mode: CustomFieldMode) => {
    setExpandedEditorFieldId(fieldId)
    setExpandedEditorMode(mode)
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

    const directPreviewRows = extractPreviewRowsFromNode(node).slice(0, 20)
    const upstreamPreviewRows = selectedNodeId
      ? inferUpstreamPreviewRows(selectedNodeId, nodes, edges, 20)
      : []
    const sampleRows = [...directPreviewRows, ...upstreamPreviewRows]
      .filter((row) => row && typeof row === 'object')
      .slice(0, 30)

    const testConfig: Record<string, unknown> = {
      ...nodeConfig,
      custom_fields: normalized,
      custom_include_source_fields: customIncludeSourceDraft,
      custom_primary_key_field: primaryKeyField,
      custom_group_by_field: primaryKeyField,
      custom_profile_enabled: customProfileEnabledDraft,
      custom_profile_emit_mode: customProfileEmitModeDraft,
      custom_profile_required_fields: customProfileRequiredFieldsDraft.join(', '),
      custom_profile_event_time_field: customProfileEventTimeFieldDraft,
      custom_profile_window_days: profileWindowDays,
      custom_profile_retention_days: profileRetentionDays,
      custom_profile_include_change_fields: customProfileIncludeChangeFieldsDraft,
    }

    setValidationLoading(true)
    setValidationResult(null)
    try {
      const result = await api.validateCustomFields({
        config: testConfig,
        rows: sampleRows,
        max_rows: 30,
      })
      setValidationResult(result as CustomFieldValidationResult)
      setValidationModalOpen(true)
      notification.success({
        message: 'Validation completed',
        description: `Input ${Number((result as any)?.input_rows || 0)} row(s), output ${Number((result as any)?.output_rows || 0)} row(s).`,
        placement: 'bottomRight',
        duration: 2,
      })
    } catch (err: any) {
      const msg = String(err?.message || 'Custom field validation failed')
      notification.error({
        message: 'Validation failed',
        description: msg,
        placement: 'bottomRight',
      })
    } finally {
      setValidationLoading(false)
    }
  }

  const openCustomFieldStudio = () => {
    setCustomFieldDraft(
      configuredCustomFields.length > 0
        ? configuredCustomFields.map((item) => createCustomFieldSpec(item))
        : [createCustomFieldSpec()]
    )
    setCustomIncludeSourceDraft(Boolean(nodeConfig.custom_include_source_fields ?? true))
    setCustomPrimaryKeyFieldDraft(configuredPrimaryKeyField)
    setCustomProfileEnabledDraft(configuredProfileEnabled)
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
    setActiveExpressionFieldId(null)
    setExampleRepoCategory('all')
    setExampleRepoSearch('')
    setCustomFieldStudioOpen(true)
    setProfileMonitorData(null)
    setProfileMonitorError(null)
    setValidationResult(null)
    setValidationModalOpen(false)
    setCollapsedCustomFieldIds([])
  }

  const updateCustomFieldDraft = (id: string, patch: Partial<CustomFieldSpec>) => {
    setCustomFieldDraft((prev) =>
      prev.map((item) => (item.id === id ? { ...item, ...patch } : item))
    )
  }

  const addCustomFieldDraft = (seed?: Partial<CustomFieldSpec>) => {
    setCustomFieldDraft((prev) => [...prev, createCustomFieldSpec(seed)])
  }

  const removeCustomFieldDraft = (id: string) => {
    setCustomFieldDraft((prev) => prev.filter((item) => item.id !== id))
    setCollapsedCustomFieldIds((prev) => prev.filter((itemId) => itemId !== id))
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

  const toggleCustomFieldCollapsed = (id: string) => {
    setCollapsedCustomFieldIds((prev) => (
      prev.includes(id)
        ? prev.filter((itemId) => itemId !== id)
        : [...prev, id]
    ))
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

  const saveCustomFieldStudio = () => {
    const normalized = serializeCustomFieldSpecs(customFieldDraft)
    const primaryKeyField = String(customPrimaryKeyFieldDraft || '').trim()
    const profileWindowDays = String(customProfileWindowDaysDraft || '').trim() || '1,7,30'
    const profileRetentionDays = Number.isFinite(customProfileRetentionDaysDraft) && customProfileRetentionDaysDraft > 0
      ? Math.floor(customProfileRetentionDaysDraft)
      : 45
    updateNodeConfig(selectedNodeId!, {
      custom_fields: normalized,
      custom_include_source_fields: customIncludeSourceDraft,
      custom_primary_key_field: primaryKeyField,
      custom_group_by_field: primaryKeyField,
      custom_profile_enabled: customProfileEnabledDraft,
      custom_profile_emit_mode: customProfileEmitModeDraft,
      custom_profile_required_fields: customProfileRequiredFieldsDraft.join(', '),
      custom_profile_event_time_field: customProfileEventTimeFieldDraft,
      custom_profile_window_days: profileWindowDays,
      custom_profile_retention_days: profileRetentionDays,
      custom_profile_include_change_fields: customProfileIncludeChangeFieldsDraft,
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

  const commonInputStyle = {
    background: 'var(--app-input-bg)',
    border: '1px solid var(--app-border-strong)',
    color: 'var(--app-text)',
  }

  const renderField = (field: ConfigField) => {
    const val = nodeConfig[field.name]

    // ── File source picker ────────────────────────────────────────────
    if (field.name === 'file_path' && isFileSource) {
      return (
        <SourceFilePicker
          value={val as string}
          fileType={fileType as any}
          placeholder={`Click to browse ${fileType.toUpperCase()} file…`}
          onChange={(path, fileInfo) => {
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
            updateNodeConfig(selectedNodeId!, patch)
          }}
        />
      )
    }

    // ── File destination path picker ──────────────────────────────────
    if (field.name === 'file_path' && isFileDest) {
      return (
        <DestinationPathPicker
          value={val as string}
          fileType={fileType as any}
          placeholder={`/output/result.${fileType}`}
          nodeId={selectedNodeId!}
          onChange={(path) => handleFieldChange('file_path', path)}
        />
      )
    }

    switch (field.type) {
      case 'text':
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
                  onClick={openCustomFieldStudio}
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
          <Tooltip title="Delete node">
            <Button type="text" icon={<DeleteOutlined />} size="small" style={{ color: '#ef4444' }} onClick={handleDelete} />
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
            {(isFileSource || isDatabaseSource) && !!nodeConfig._detected_columns ? (
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
    </Drawer>
    {canUseCustomFieldStudio && (
      <Modal
        open={customFieldStudioOpen}
        onCancel={() => setCustomFieldStudioOpen(false)}
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
              Custom Fields Studio
            </Text>
            <br />
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
              Build formula fields + nested JSON object/array outputs using source fields and custom fields.
            </Text>
          </div>
          <Space>
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
                options={expressionFieldOptions.map((field) => ({ value: field, label: field }))}
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
                  disabled={customFieldDraftCount === 0 || customFieldDraftEnabledCount === customFieldDraftCount}
                  onClick={() => setAllCustomFieldsEnabled(true)}
                >
                  Enable All
                </Button>
                <Button
                  size="small"
                  disabled={customFieldDraftCount === 0 || customFieldDraftEnabledCount === 0}
                  onClick={() => setAllCustomFieldsEnabled(false)}
                >
                  Disable All
                </Button>
                <Button
                  size="small"
                  disabled={customFieldDraftCount === 0 || customFieldDraftCollapsedCount === 0}
                  onClick={expandAllCustomFields}
                >
                  Expand All
                </Button>
                <Button
                  size="small"
                  disabled={customFieldDraftCount === 0 || customFieldDraftCollapsedCount === customFieldDraftCount}
                  onClick={collapseAllCustomFields}
                >
                  Collapse All
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
                        <Tag
                          style={{
                            marginInlineEnd: 0,
                            background: item.enabled ? '#22c55e1a' : '#64748b1a',
                            border: item.enabled ? '1px solid #22c55e40' : '1px solid #64748b40',
                            color: item.enabled ? '#22c55e' : '#94a3b8',
                          }}
                        >
                          {item.enabled ? 'Enabled' : 'Disabled'}
                        </Tag>
                        <Tag
                          style={{
                            marginInlineEnd: 0,
                            background: isCollapsed ? '#0ea5e91a' : '#6366f11a',
                            border: isCollapsed ? '1px solid #0ea5e940' : '1px solid #6366f140',
                            color: isCollapsed ? '#0284c7' : '#6366f1',
                          }}
                        >
                          {isCollapsed ? 'Collapsed' : 'Expanded'}
                        </Tag>
                      </Space>
                      <Space size={6} align="center" wrap>
                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Enable</Text>
                        <Switch
                          size="small"
                          checked={item.enabled}
                          onChange={(value) => updateCustomFieldDraft(item.id, { enabled: value })}
                        />
                        <Button
                          size="small"
                          onClick={() => toggleCustomFieldCollapsed(item.id)}
                        >
                          {isCollapsed ? 'Expand' : 'Collapse'}
                        </Button>
                        <Button
                          size="small"
                          onClick={() => openExpandedEditor(item.id, item.mode)}
                        >
                          Expand Editor
                        </Button>
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
                        </Text>
                        <br />
                        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                          {preview ? preview.slice(0, 180) : 'No expression/template configured yet.'}
                          {preview.length > 180 ? '...' : ''}
                        </Text>
                      </div>
                    ) : (
                      <>
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 200px', gap: 10, marginTop: 8 }}>
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
                        </div>

                        {item.mode === 'value' ? (
                          <div style={{ marginTop: 10, border: '1px solid var(--app-border-strong)', borderRadius: 8, overflow: 'hidden' }}>
                            <Editor
                              height="120px"
                              language="etl-expr"
                              value={item.expression}
                              beforeMount={(monaco) => ensureExpressionLanguage(monaco)}
                              onMount={(editor) => {
                                editor.onDidFocusEditorText(() => {
                                  setActiveExpressionFieldId(item.id)
                                })
                              }}
                              onChange={(value) => updateCustomFieldDraft(item.id, { expression: value || '' })}
                              theme="vs-dark"
                              options={{
                                minimap: { enabled: false },
                                fontSize: 12,
                                scrollBeyondLastLine: false,
                                wordWrap: 'on',
                                quickSuggestions: true,
                                suggestOnTriggerCharacters: true,
                                padding: { top: 8, bottom: 8 },
                              }}
                            />
                          </div>
                        ) : (
                          <div style={{ marginTop: 10, border: '1px solid var(--app-border-strong)', borderRadius: 8, overflow: 'hidden' }}>
                            <Editor
                              height="200px"
                              language="json"
                              value={item.jsonTemplate}
                              onChange={(value) => updateCustomFieldDraft(item.id, { jsonTemplate: value || '' })}
                              theme="vs-dark"
                              options={{
                                minimap: { enabled: false },
                                fontSize: 12,
                                scrollBeyondLastLine: false,
                                wordWrap: 'on',
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
      open={Boolean(expandedEditorField)}
      onCancel={() => setExpandedEditorFieldId(null)}
      footer={null}
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
        <Space>
          <Tooltip title="Close">
            <Button
              shape="circle"
              icon={<CloseOutlined />}
              onClick={() => setExpandedEditorFieldId(null)}
              aria-label="Close expanded editor"
            />
          </Tooltip>
        </Space>
      </div>
      <div style={{ flex: 1, minHeight: 0, padding: 10 }}>
        {expandedEditorField ? (
          <Editor
            height="100%"
            language={expandedEditorMode === 'json' ? 'json' : 'etl-expr'}
            value={expandedEditorMode === 'json' ? expandedEditorField.jsonTemplate : expandedEditorField.expression}
            beforeMount={(monaco) => {
              if (expandedEditorMode === 'value') ensureExpressionLanguage(monaco)
            }}
            onChange={(value) => {
              if (!expandedEditorField) return
              if (expandedEditorMode === 'json') {
                updateCustomFieldDraft(expandedEditorField.id, { jsonTemplate: value || '' })
              } else {
                updateCustomFieldDraft(expandedEditorField.id, { expression: value || '' })
              }
            }}
            theme="vs-dark"
            options={{
              minimap: { enabled: false },
              fontSize: 14,
              scrollBeyondLastLine: false,
              wordWrap: 'on',
              quickSuggestions: true,
              suggestOnTriggerCharacters: true,
              padding: { top: 10, bottom: 10 },
            }}
          />
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
          <Tag
            style={{
              marginInlineEnd: 0,
              background: validationResult?.ok ? '#22c55e1a' : '#ef44441a',
              border: validationResult?.ok ? '1px solid #22c55e40' : '1px solid #ef444440',
              color: validationResult?.ok ? '#22c55e' : '#ef4444',
            }}
          >
            {validationResult?.ok ? 'VALID' : 'HAS ERRORS'}
          </Tag>
        </Space>
        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
          Input rows: {Number(validationResult?.input_rows || 0)} | Output rows: {Number(validationResult?.output_rows || 0)}
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
