import { useEffect, useMemo, useState } from 'react'
import {
  Drawer, Form, Input, Select, Switch, InputNumber,
  Button, Typography, Space, Tabs, Divider, Tag, Tooltip, Table, notification, Modal
} from 'antd'
import { CloseOutlined, CopyOutlined, DeleteOutlined, InfoCircleOutlined } from '@ant-design/icons'
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
type SingleValueOutputMode = 'plain_text' | 'json'
type CustomProfileProcessingMode = 'batch' | 'incremental' | 'incremental_batch'
type CustomProfileComputeStrategy = 'single' | 'parallel_by_profile_key'
type CustomProfileComputeExecutor = 'thread' | 'process'
type CustomExpressionEngine = 'auto' | 'python' | 'polars'
type CustomProfileStorage = 'lmdb' | 'rocksdb' | 'redis' | 'oracle'
type CustomProfileOracleWriteStrategy = 'single' | 'parallel_key'
type CustomEditorColorProfile = 'high_contrast' | 'soft' | 'js_like'
type CustomEditorFontPreset = 'jetbrains_mono' | 'fira_code' | 'consolas'

interface CustomFieldSpec {
  id: string
  name: string
  mode: CustomFieldMode
  singleValueOutput: SingleValueOutputMode
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

const EXPR_LANGUAGE_ID = 'etl-expr'
const JSON_TEMPLATE_LANGUAGE_ID = 'etl-json-template'
const EXPR_VALIDATION_OWNER = 'etl-expr-validator'
const JSON_TEMPLATE_VALIDATION_OWNER = 'etl-json-template-validator'
const EXPR_FIELD_SINGLE_QUOTED_PATTERN = /'(?=[^']*[A-Z])(?:[^'\\]|\\.)*'/
const EXPR_LITERAL_SINGLE_QUOTED_PATTERN = /'(?:min|max|sum|mean|avg|count|count_non_null|distinct|distinct_count|value_counts|first|last|row_count|customer|servicename|datewise|timeseries|daily|weekly|monthly|yearly|eq|not|contains|not_contains|like|not_like|in|not_in|startswith|not_startswith|endswith|not_endswith|regex|not_regex)'/
const CUSTOM_EDITOR_THEME_IDS = {
  high_contrast: 'opsfabric-custom-field-dark-hc',
  soft: 'opsfabric-custom-field-dark-soft',
  js_like: 'opsfabric-custom-field-dark-js',
} as const

const CUSTOM_EDITOR_COLOR_PROFILE_OPTIONS: Array<{ value: CustomEditorColorProfile; label: string }> = [
  { value: 'high_contrast', label: 'High Contrast' },
  { value: 'soft', label: 'Soft' },
  { value: 'js_like', label: 'JS-like' },
]

const CUSTOM_EDITOR_FONT_FAMILY_OPTIONS: Array<{ value: CustomEditorFontPreset; label: string }> = [
  { value: 'jetbrains_mono', label: 'JetBrains Mono' },
  { value: 'fira_code', label: 'Fira Code' },
  { value: 'consolas', label: 'Consolas' },
]

function resolveEditorFontFamily(preset: CustomEditorFontPreset): string {
  if (preset === 'fira_code') return '"Fira Code", Menlo, Monaco, Consolas, "Courier New", monospace'
  if (preset === 'consolas') return 'Consolas, Menlo, Monaco, "Courier New", monospace'
  return '"JetBrains Mono", Menlo, Monaco, Consolas, "Courier New", monospace'
}

let exprLanguageRegistered = false
let jsonTemplateLanguageRegistered = false
let customEditorThemeRegistered = false
let exprCompletionEntries: ExpressionCompletionEntry[] = []

function setExpressionCompletionEntries(entries: ExpressionCompletionEntry[]): void {
  exprCompletionEntries = entries
}

function ensureCustomEditorTheme(monaco: Monaco): void {
  if (customEditorThemeRegistered) return

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
  customEditorThemeRegistered = true
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

function extractExpressionStringRanges(text: string): Array<{ value: string; startOffset: number; endOffset: number }> {
  const out: Array<{ value: string; startOffset: number; endOffset: number }> = []
  let inString = false
  let escaped = false
  let stringStart = -1
  let buffer = ''

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i]
    if (!inString) {
      if (ch === '"') {
        inString = true
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
    if (ch === '"') {
      const decoded = decodeJsonStringContent(buffer)
      if (decoded.startsWith('=')) {
        out.push({
          value: decoded,
          startOffset: stringStart + 1,
          endOffset: i,
        })
      }
      inString = false
      escaped = false
      buffer = ''
      stringStart = -1
      continue
    }
    buffer += ch
  }

  return out
}

function validateExpressionSyntax(
  expression: string,
  baseOffset = 0,
  options?: { requireEqualsPrefix?: boolean }
): ExpressionValidationIssue[] {
  const issues: ExpressionValidationIssue[] = []
  const source = String(expression || '')
  const trimmed = source.trim()
  const requireEqualsPrefix = options?.requireEqualsPrefix ?? true

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
          message: `Unexpected closing "${ch}"`,
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
      message: 'Unclosed quoted string',
      severity: 'error',
    })
  }

  while (stack.length > 0) {
    const unclosed = stack.pop()!
    const expectedClose = unclosed.ch === '(' ? ')' : unclosed.ch === '[' ? ']' : '}'
    issues.push({
      startOffset: baseOffset + unclosed.offset,
      endOffset: baseOffset + unclosed.offset + 1,
      message: `Missing closing "${expectedClose}"`,
      severity: 'error',
    })
  }

  return issues
}

function toMonacoMarkers(
  monaco: Monaco,
  model: any,
  issues: ExpressionValidationIssue[],
) {
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
        ? monaco.editor.MarkerSeverity.Error
        : monaco.editor.MarkerSeverity.Warning,
    }
  })
}

function attachExpressionValidation(editor: any, monaco: Monaco): void {
  const validate = () => {
    const model = editor.getModel()
    if (!model) return
    const text = String(model.getValue() || '')
    const issues = validateExpressionSyntax(text, 0, { requireEqualsPrefix: true })
    monaco.editor.setModelMarkers(model, EXPR_VALIDATION_OWNER, toMonacoMarkers(monaco, model, issues))
  }
  validate()
  editor.onDidChangeModelContent(validate)
  editor.onDidDispose(() => {
    const model = editor.getModel()
    if (!model) return
    monaco.editor.setModelMarkers(model, EXPR_VALIDATION_OWNER, [])
  })
}

function attachJsonTemplateExpressionValidation(editor: any, monaco: Monaco): void {
  const validate = () => {
    const model = editor.getModel()
    if (!model) return
    const text = String(model.getValue() || '')
    const ranges = extractExpressionStringRanges(text)
    const issues = ranges.flatMap((entry) =>
      validateExpressionSyntax(entry.value, entry.startOffset, { requireEqualsPrefix: false })
    )
    monaco.editor.setModelMarkers(model, JSON_TEMPLATE_VALIDATION_OWNER, toMonacoMarkers(monaco, model, issues))
  }
  validate()
  editor.onDidChangeModelContent(validate)
  editor.onDidDispose(() => {
    const model = editor.getModel()
    if (!model) return
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
    out.push(
      createCustomFieldSpec({
        id: String(rec.id || ''),
        name: String(rec.name || rec.field || ''),
        mode,
        singleValueOutput: normalizeSingleValueOutput(
          rec.singleValueOutput ?? rec.single_value_output ?? rec.value_output ?? rec.output_format
        ),
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
      id: String(item.id || '').trim(),
      name: String(item.name || '').trim(),
      mode: item.mode,
      single_value_output: item.mode === 'value' ? item.singleValueOutput : 'json',
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
  const [customProfileOracleQueueWaitOnForceFlushDraft, setCustomProfileOracleQueueWaitOnForceFlushDraft] = useState(false)
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
  const [profileMonitorError, setProfileMonitorError] = useState<string | null>(null)
  const [profileDataModalOpen, setProfileDataModalOpen] = useState(false)
  const [selectedProfileEntityKey, setSelectedProfileEntityKey] = useState<string>('')
  const [expandedEditorFieldId, setExpandedEditorFieldId] = useState<string | null>(null)
  const [expandedEditorMode, setExpandedEditorMode] = useState<CustomFieldMode>('value')
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
  const [customEditorColorProfile, setCustomEditorColorProfile] = useState<CustomEditorColorProfile>('high_contrast')
  const [customEditorFontPreset, setCustomEditorFontPreset] = useState<CustomEditorFontPreset>('jetbrains_mono')
  const [customEditorFontSize, setCustomEditorFontSize] = useState(13)
  const [customEditorLineHeight, setCustomEditorLineHeight] = useState(22)
  const [customEditorWordWrap, setCustomEditorWordWrap] = useState(true)
  const [customEditorLigatures, setCustomEditorLigatures] = useState(false)
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
    ...(primaryKeyFieldForFilter ? [primaryKeyFieldForFilter] : []),
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
  const configuredProfileOracleQueueWaitOnForceFlush = Boolean(nodeConfig.custom_profile_oracle_queue_wait_on_force_flush ?? false)
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
        description: 'Run pipeline and refresh Profile Monitoring to load stored profile documents.',
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
    setCustomFieldStudioOpen(true)
    setProfileMonitorData(null)
    setProfileMonitorError(null)
    setValidationResult(null)
    setValidationModalOpen(false)
    const collapsedIds = parseStringList(nodeConfig.custom_collapsed_field_ids)
    const draftIdSet = new Set(initialDraft.map((item) => String(item.id || '').trim()).filter(Boolean))
    setCollapsedCustomFieldIds(collapsedIds.filter((id) => draftIdSet.has(id)))
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
          <Tooltip title="Duplicate node">
            <Button type="text" icon={<CopyOutlined />} size="small" style={{ color: 'var(--app-text-muted)' }} onClick={handleDuplicate} />
          </Tooltip>
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
          <Space wrap>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Editor Style</Text>
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
              value={customEditorFontSize}
              onChange={(value) => setCustomEditorFontSize(Number(value || 13))}
              options={[11, 12, 13, 14, 15, 16, 18, 20].map((size) => ({ value: size, label: `Size ${size}` }))}
              style={{ width: 108 }}
            />
            <Select
              size="small"
              value={customEditorLineHeight}
              onChange={(value) => setCustomEditorLineHeight(Number(value || 22))}
              options={[18, 20, 22, 24, 26, 28].map((line) => ({ value: line, label: `Line ${line}` }))}
              style={{ width: 108 }}
            />
            <Space size={4}>
              <Switch size="small" checked={customEditorWordWrap} onChange={setCustomEditorWordWrap} />
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Wrap</Text>
            </Space>
            <Space size={4}>
              <Switch size="small" checked={customEditorLigatures} onChange={setCustomEditorLigatures} />
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Ligatures</Text>
            </Space>
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
                                quickSuggestions: true,
                                suggestOnTriggerCharacters: true,
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
                                quickSuggestions: true,
                                suggestOnTriggerCharacters: true,
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
      onCancel={() => setExpandedEditorFieldId(null)}
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
            value={customEditorFontSize}
            onChange={(value) => setCustomEditorFontSize(Number(value || 13))}
            options={[11, 12, 13, 14, 15, 16, 18, 20].map((size) => ({ value: size, label: `Size ${size}` }))}
            style={{ width: 108 }}
          />
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
                  quickSuggestions: true,
                  suggestOnTriggerCharacters: true,
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
