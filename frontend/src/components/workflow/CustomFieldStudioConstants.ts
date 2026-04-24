// Extracted from ConfigDrawer to keep file size manageable.

export type CustomFieldMode = 'value' | 'json'

export interface CustomFieldExampleItem {
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

export const CUSTOM_FIELD_EXAMPLE_REPOSITORY: CustomFieldExampleItem[] = [
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

export const EXPRESSION_FUNCTION_SNIPPETS: Array<{ label: string; snippet: string }> = [
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
  { label: 'combo if_prev_min(date_field,profile_key)', snippet: "if_(field('${1:date_field}') == null, prev('${2:profile_key}'), min(array(prev('${2:profile_key}'), field('${1:date_field}'))))" },
  { label: 'combo if_prev_max(date_field,profile_key)', snippet: "if_(field('${1:date_field}') == null, prev('${2:profile_key}'), max(array(prev('${2:profile_key}'), field('${1:date_field}'))))" },
  { label: 'combo count_prev_array(profile_key)', snippet: "count(prev('${1:profile_key}', array()))" },
  { label: 'combo rolling_update(profile_key,value_field,time_field)', snippet: "rolling_update('${1:profile_key}', num(field('${2:value_field}'), 0), field('${3:event_time_field}'), '${4:1,7,30}', ${5:45})" },
  { label: 'combo inc(profile_key,coalesce(field,0))', snippet: "inc('${1:profile_key}', coalesce(field('${2:value_field}'), ${3:0}))" },
  { label: 'combo append_unique(profile_key,value_field)', snippet: "append_unique('${1:profile_key}', field('${2:value_field}'))" },
  { label: 'combo map_inc(datewise_txn_count)', snippet: "map_inc('${1:profile_key}', coalesce(str(field('${2:key_field}')), '${3:UNKNOWN}'), if_(field('${4:txn_id_field}') == null, 0, 1))" },
  { label: 'combo map_inc(datewise_service_name)', snippet: "map_inc('${1:profile_key}', coalesce(str(field('${2:key_field}')), '${3:UNKNOWN}') + '|' + coalesce(str(field('${4:service_field}')), '${3:UNKNOWN}'), ${5:1})" },
]

export const CUSTOM_FIELD_TIPS_TEXT = `Available functions:
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
