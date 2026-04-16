import type { NodeTypeDefinition } from '../types'

const triggerNodes: NodeTypeDefinition[] = [
  {
    type: 'ml_manual_trigger',
    label: 'Manual Run',
    category: 'trigger',
    icon: '▶',
    color: '#f97316',
    bgColor: 'rgba(249,115,22,0.08)',
    description: 'Run this MLOps workflow on-demand.',
    inputs: 0,
    outputs: 1,
    tags: ['trigger', 'manual', 'mlops'],
    configFields: [],
  },
  {
    type: 'ml_schedule_trigger',
    label: 'Schedule',
    category: 'trigger',
    icon: '⏰',
    color: '#f97316',
    bgColor: 'rgba(249,115,22,0.08)',
    description: 'Run workflow by schedule for recurrent retraining or scoring.',
    inputs: 0,
    outputs: 1,
    tags: ['trigger', 'schedule', 'retraining'],
    configFields: [
      { name: 'cron', label: 'Cron Expression', type: 'text', defaultValue: '0 2 * * *', required: true },
      { name: 'timezone', label: 'Timezone', type: 'text', defaultValue: 'UTC' },
    ],
  },
]

const sourceNodes: NodeTypeDefinition[] = [
  {
    type: 'ml_dataset_source',
    label: 'Dataset Source',
    category: 'source',
    icon: '🗂',
    color: '#3b82f6',
    bgColor: 'rgba(59,130,246,0.1)',
    description: 'Load raw training or scoring dataset from warehouse/lake.',
    inputs: 0,
    outputs: 1,
    tags: ['source', 'dataset', 'warehouse'],
    configFields: [
      { name: 'source_type', label: 'Source Type', type: 'select', defaultValue: 'warehouse', options: [
        { value: 'warehouse', label: 'Warehouse' },
        { value: 'lakehouse', label: 'Lakehouse' },
        { value: 'api', label: 'API' },
        { value: 'file', label: 'File' },
      ] },
      { name: 'dataset', label: 'Dataset / Table', type: 'text', placeholder: 'analytics.customer_churn', required: true },
      { name: 'query', label: 'Optional SQL', type: 'code', language: 'sql', placeholder: 'SELECT * FROM ...' },
    ],
  },
  {
    type: 'ml_pipeline_output_source',
    label: 'ETL Pipeline Output',
    category: 'source',
    icon: '🧩',
    color: '#3b82f6',
    bgColor: 'rgba(59,130,246,0.1)',
    description: 'Use data from an ETL pipeline run as model input.',
    inputs: 0,
    outputs: 1,
    tags: ['source', 'etl', 'feature-store'],
    configFields: [
      {
        name: 'pipeline_id',
        label: 'ETL Pipeline',
        type: 'select',
        required: true,
        description: 'Select the ETL pipeline whose latest successful output should feed this node.',
      },
      {
        name: 'pipeline_node_id',
        label: 'Pipeline Node (optional)',
        type: 'select',
        description: 'Pick a specific ETL node output. Leave empty to auto-detect latest tabular output.',
      },
    ],
  },
]

const transformNodes: NodeTypeDefinition[] = [
  {
    type: 'ml_data_staging',
    label: 'Data Staging',
    category: 'transform',
    icon: '🧱',
    color: '#a855f7',
    bgColor: 'rgba(168,85,247,0.1)',
    description: 'Apply schema checks, dedupe and null handling before modeling.',
    inputs: 1,
    outputs: 1,
    tags: ['staging', 'quality', 'transform'],
    configFields: [
      { name: 'deduplicate', label: 'Deduplicate', type: 'toggle', defaultValue: true },
      { name: 'drop_null_threshold', label: 'Null Threshold %', type: 'number', defaultValue: 30 },
      { name: 'target_column', label: 'Target Column', type: 'text', placeholder: 'churn_flag' },
    ],
  },
  {
    type: 'ml_feature_engineering',
    label: 'Feature Engineering',
    category: 'transform',
    icon: '🧠',
    color: '#a855f7',
    bgColor: 'rgba(168,85,247,0.1)',
    description: 'Encode categories, scale numeric values, and create derived features.',
    inputs: 1,
    outputs: 1,
    tags: ['feature', 'encoding', 'scaling'],
    configFields: [
      { name: 'categorical_strategy', label: 'Categorical Encoding', type: 'select', defaultValue: 'one_hot', options: [
        { value: 'one_hot', label: 'One-Hot' },
        { value: 'target', label: 'Target Encoding' },
        { value: 'label', label: 'Label Encoding' },
      ] },
      { name: 'scaler', label: 'Scaling', type: 'select', defaultValue: 'standard', options: [
        { value: 'none', label: 'None' },
        { value: 'standard', label: 'Standard' },
        { value: 'minmax', label: 'MinMax' },
      ] },
      { name: 'feature_script', label: 'Custom Feature Logic', type: 'code', language: 'python', placeholder: '# python feature logic' },
    ],
  },
  {
    type: 'ml_train_validation_split',
    label: 'Train/Test Split',
    category: 'transform',
    icon: '✂',
    color: '#a855f7',
    bgColor: 'rgba(168,85,247,0.1)',
    description: 'Split dataset for training and validation.',
    inputs: 1,
    outputs: 1,
    tags: ['split', 'validation', 'training'],
    configFields: [
      { name: 'test_size', label: 'Test Size %', type: 'number', defaultValue: 20 },
      { name: 'random_state', label: 'Random State', type: 'number', defaultValue: 42 },
      { name: 'stratify', label: 'Stratify by Target', type: 'toggle', defaultValue: true },
    ],
  },
  {
    type: 'ml_model_training',
    label: 'Model Training',
    category: 'transform',
    icon: '🏋️',
    color: '#a855f7',
    bgColor: 'rgba(168,85,247,0.1)',
    description: 'Train ML model for classification/regression/forecasting.',
    inputs: 1,
    outputs: 1,
    tags: ['training', 'model', 'ml'],
    configFields: [
      { name: 'task_type', label: 'Task Type', type: 'select', defaultValue: 'classification', options: [
        { value: 'classification', label: 'Classification' },
        { value: 'regression', label: 'Regression' },
        { value: 'forecasting', label: 'Forecasting' },
      ] },
      { name: 'algorithm', label: 'Algorithm', type: 'select', defaultValue: 'xgboost', options: [
        { value: 'xgboost', label: 'XGBoost' },
        { value: 'random_forest', label: 'Random Forest' },
        { value: 'lightgbm', label: 'LightGBM' },
        { value: 'prophet', label: 'Prophet' },
      ] },
      { name: 'target_column', label: 'Target Column', type: 'text', placeholder: 'target', required: true },
    ],
  },
  {
    type: 'ml_model_evaluation',
    label: 'Model Evaluation',
    category: 'transform',
    icon: '📈',
    color: '#a855f7',
    bgColor: 'rgba(168,85,247,0.1)',
    description: 'Evaluate model metrics and quality gates.',
    inputs: 1,
    outputs: 1,
    tags: ['evaluation', 'metrics', 'validation'],
    configFields: [
      { name: 'primary_metric', label: 'Primary Metric', type: 'select', defaultValue: 'accuracy', options: [
        { value: 'accuracy', label: 'Accuracy' },
        { value: 'f1', label: 'F1 Score' },
        { value: 'auc', label: 'AUC' },
        { value: 'rmse', label: 'RMSE' },
        { value: 'mape', label: 'MAPE' },
      ] },
      { name: 'threshold', label: 'Quality Threshold', type: 'number', defaultValue: 0.8 },
    ],
  },
  {
    type: 'ml_forecasting',
    label: 'Forecasting',
    category: 'transform',
    icon: '🔮',
    color: '#a855f7',
    bgColor: 'rgba(168,85,247,0.1)',
    description: 'Generate future predictions for selected horizon.',
    inputs: 1,
    outputs: 1,
    tags: ['forecast', 'prediction', 'timeseries'],
    configFields: [
      { name: 'horizon_days', label: 'Forecast Horizon (days)', type: 'number', defaultValue: 30 },
      { name: 'granularity', label: 'Granularity', type: 'select', defaultValue: 'daily', options: [
        { value: 'hourly', label: 'Hourly' },
        { value: 'daily', label: 'Daily' },
        { value: 'weekly', label: 'Weekly' },
        { value: 'monthly', label: 'Monthly' },
      ] },
    ],
  },
]

const destinationNodes: NodeTypeDefinition[] = [
  {
    type: 'ml_model_registry',
    label: 'Model Registry',
    category: 'destination',
    icon: '📚',
    color: '#22c55e',
    bgColor: 'rgba(34,197,94,0.1)',
    description: 'Register model version and metadata.',
    inputs: 1,
    outputs: 0,
    tags: ['registry', 'versioning', 'governance'],
    configFields: [
      { name: 'registry_name', label: 'Registry Name', type: 'text', defaultValue: 'default-registry' },
      { name: 'model_name', label: 'Model Name', type: 'text', placeholder: 'churn_prediction_model', required: true },
      { name: 'stage', label: 'Stage', type: 'select', defaultValue: 'staging', options: [
        { value: 'staging', label: 'Staging' },
        { value: 'production', label: 'Production' },
        { value: 'archived', label: 'Archived' },
      ] },
    ],
  },
  {
    type: 'ml_deployment_endpoint',
    label: 'Deploy Endpoint',
    category: 'destination',
    icon: '🚀',
    color: '#22c55e',
    bgColor: 'rgba(34,197,94,0.1)',
    description: 'Deploy model to real-time inference endpoint.',
    inputs: 1,
    outputs: 0,
    tags: ['deployment', 'serving', 'inference'],
    configFields: [
      { name: 'endpoint_name', label: 'Endpoint Name', type: 'text', placeholder: 'churn-prod-endpoint', required: true },
      { name: 'autoscale_min', label: 'Min Replicas', type: 'number', defaultValue: 1 },
      { name: 'autoscale_max', label: 'Max Replicas', type: 'number', defaultValue: 4 },
    ],
  },
  {
    type: 'ml_batch_scoring',
    label: 'Batch Scoring',
    category: 'destination',
    icon: '📦',
    color: '#22c55e',
    bgColor: 'rgba(34,197,94,0.1)',
    description: 'Run batch predictions and write output for analytics dashboards.',
    inputs: 1,
    outputs: 0,
    tags: ['batch', 'scoring', 'prediction'],
    configFields: [
      { name: 'output_table', label: 'Output Table', type: 'text', placeholder: 'analytics.batch_predictions', required: true },
      { name: 'write_mode', label: 'Write Mode', type: 'select', defaultValue: 'append', options: [
        { value: 'append', label: 'Append' },
        { value: 'replace', label: 'Replace' },
      ] },
    ],
  },
]

const flowNodes: NodeTypeDefinition[] = [
  {
    type: 'ml_condition_gate',
    label: 'Quality Gate',
    category: 'flow',
    icon: '⋄',
    color: 'var(--app-text-subtle)',
    bgColor: 'rgba(100,116,139,0.1)',
    description: 'Branch workflow based on metric threshold.',
    inputs: 1,
    outputs: 2,
    tags: ['flow', 'gate', 'quality'],
    configFields: [
      { name: 'metric_name', label: 'Metric Name', type: 'text', defaultValue: 'accuracy', required: true },
      { name: 'operator', label: 'Operator', type: 'select', defaultValue: 'greater_than', options: [
        { value: 'greater_than', label: 'Greater Than' },
        { value: 'greater_than_equal', label: 'Greater Than or Equal' },
        { value: 'less_than', label: 'Less Than' },
      ] },
      { name: 'value', label: 'Threshold', type: 'number', defaultValue: 0.8 },
    ],
  },
  {
    type: 'ml_merge_node',
    label: 'Merge Branches',
    category: 'flow',
    icon: '⟨⟩',
    color: 'var(--app-text-subtle)',
    bgColor: 'rgba(100,116,139,0.1)',
    description: 'Merge successful branches before downstream deployment.',
    inputs: 2,
    outputs: 1,
    tags: ['flow', 'merge'],
    configFields: [
      { name: 'strategy', label: 'Merge Strategy', type: 'select', defaultValue: 'first_success', options: [
        { value: 'first_success', label: 'First Success' },
        { value: 'concatenate', label: 'Concatenate' },
      ] },
    ],
  },
]

export const ALL_MLOPS_NODE_TYPES: NodeTypeDefinition[] = [
  ...triggerNodes,
  ...sourceNodes,
  ...transformNodes,
  ...destinationNodes,
  ...flowNodes,
]

export const MLOPS_NODE_CATEGORIES = {
  trigger: { label: 'Orchestration', color: '#f97316', icon: '▶' },
  source: { label: 'Data Sources', color: '#3b82f6', icon: '⬇' },
  transform: { label: 'ML Stages', color: '#a855f7', icon: '🧠' },
  destination: { label: 'Deployment', color: '#22c55e', icon: '⬆' },
  flow: { label: 'Control Gates', color: 'var(--app-text-subtle)', icon: '⋄' },
}

export const getMLOpsNodeDef = (type: string): NodeTypeDefinition | undefined =>
  ALL_MLOPS_NODE_TYPES.find((n) => n.type === type)
