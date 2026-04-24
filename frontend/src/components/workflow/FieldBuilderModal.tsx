import { AutoComplete, Button, Input, InputNumber, Modal, Select, Space, Typography } from 'antd'

const { Text } = Typography

type UiFieldBuilderTemplate =
  | 'field'
  | 'values'
  | 'upper_trim_field'
  | 'lower_trim_field'
  | 'coalesce_field'
  | 'num_field'
  | 'count_values'
  | 'sum_values'
  | 'mean_values'
  | 'min_values'
  | 'max_values'
  | 'distinct_values'
  | 'agg'
  | 'running_sum'
  | 'running_mean'
  | 'rolling_sum'
  | 'rolling_mean'

type UiFieldBuilderState = {
  template: UiFieldBuilderTemplate
  fieldPath: string
  fallback: string
  agg: string
  window: number
}

type SelectOption = {
  value: string
  label?: string
}

type FieldBuilderModalProps = {
  open: boolean
  onClose: () => void
  placeholderIndex: number
  fieldBuilderState: UiFieldBuilderState
  uiFieldAutoCompleteOptions: SelectOption[]
  aggOptions: string[]
  updateUiFieldBuilder: (index: number, patch: Partial<UiFieldBuilderState>) => void
  buildUiFieldBuilderExpression: (state: UiFieldBuilderState) => string
  applyUiFieldBuilder: (index: number) => void
}

const TEMPLATE_OPTIONS: Array<{ value: UiFieldBuilderTemplate; label: string }> = [
  { value: 'field', label: 'field(path)' },
  { value: 'values', label: 'values(path)' },
  { value: 'upper_trim_field', label: 'upper(trim(field(path)))' },
  { value: 'lower_trim_field', label: 'lower(trim(field(path)))' },
  { value: 'coalesce_field', label: 'coalesce(field(path), fallback)' },
  { value: 'num_field', label: 'num(field(path), fallback)' },
  { value: 'count_values', label: 'count(values(path))' },
  { value: 'sum_values', label: 'sum(values(path))' },
  { value: 'mean_values', label: 'mean(values(path))' },
  { value: 'min_values', label: 'min(values(path))' },
  { value: 'max_values', label: 'max(values(path))' },
  { value: 'distinct_values', label: 'distinct(values(path))' },
  { value: 'agg', label: "agg(path,'agg')" },
  { value: 'running_sum', label: 'running_sum(field(path))' },
  { value: 'running_mean', label: 'running_mean(field(path))' },
  { value: 'rolling_sum', label: 'rolling_sum(field(path), window)' },
  { value: 'rolling_mean', label: 'rolling_mean(field(path), window)' },
]

export default function FieldBuilderModal(props: FieldBuilderModalProps) {
  const {
    open,
    onClose,
    placeholderIndex,
    fieldBuilderState,
    uiFieldAutoCompleteOptions,
    aggOptions,
    updateUiFieldBuilder,
    buildUiFieldBuilderExpression,
    applyUiFieldBuilder,
  } = props

  return (
    <Modal
      open={open}
      onCancel={onClose}
      footer={null}
      centered
      width={640}
      title="Field Constructor"
    >
      <Space direction="vertical" size={10} style={{ width: '100%' }}>
        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
          Build field/value expressions from function templates without writing raw syntax.
        </Text>
        <div style={{ display: 'grid', gridTemplateColumns: '160px minmax(0,1fr)', gap: 8, alignItems: 'center' }}>
          <Text style={{ color: 'var(--app-text)' }}>Template</Text>
          <Select
            size="small"
            value={fieldBuilderState.template}
            options={TEMPLATE_OPTIONS}
            onChange={(value) => updateUiFieldBuilder(placeholderIndex, { template: value as UiFieldBuilderTemplate })}
            style={{ width: '100%' }}
          />
          <Text style={{ color: 'var(--app-text)' }}>Field/Path</Text>
          <AutoComplete
            value={fieldBuilderState.fieldPath}
            options={uiFieldAutoCompleteOptions}
            onChange={(value) => updateUiFieldBuilder(placeholderIndex, { fieldPath: String(value || '') })}
            filterOption={(inputValue, option) => String(option?.value || '').toLowerCase().includes(String(inputValue || '').toLowerCase())}
          >
            <Input size="small" placeholder="field/path" />
          </AutoComplete>
          {fieldBuilderState.template === 'coalesce_field' || fieldBuilderState.template === 'num_field' ? (
            <>
              <Text style={{ color: 'var(--app-text)' }}>Fallback</Text>
              <Input
                size="small"
                value={fieldBuilderState.fallback}
                placeholder={fieldBuilderState.template === 'num_field' ? '0' : "'UNKNOWN'"}
                onChange={(event) => updateUiFieldBuilder(placeholderIndex, { fallback: event.target.value })}
              />
            </>
          ) : null}
          {fieldBuilderState.template === 'agg' ? (
            <>
              <Text style={{ color: 'var(--app-text)' }}>Aggregate</Text>
              <Select
                size="small"
                value={fieldBuilderState.agg}
                options={aggOptions.map((agg) => ({ value: agg, label: agg }))}
                onChange={(value) => updateUiFieldBuilder(placeholderIndex, { agg: String(value || 'sum') })}
                style={{ width: '100%' }}
              />
            </>
          ) : null}
          {fieldBuilderState.template === 'rolling_sum' || fieldBuilderState.template === 'rolling_mean' ? (
            <>
              <Text style={{ color: 'var(--app-text)' }}>Window</Text>
              <InputNumber
                size="small"
                min={1}
                step={1}
                value={fieldBuilderState.window}
                onChange={(value) => updateUiFieldBuilder(placeholderIndex, { window: Number(value || 7) })}
                style={{ width: '100%' }}
              />
            </>
          ) : null}
        </div>
        <div
          style={{
            border: '1px solid var(--app-border-strong)',
            borderRadius: 8,
            background: 'var(--app-input-bg)',
            padding: 8,
            fontFamily: 'monospace',
            fontSize: 12,
            color: 'var(--app-text)',
          }}
        >
          {buildUiFieldBuilderExpression(fieldBuilderState)}
        </div>
        <Space style={{ justifyContent: 'flex-end', width: '100%' }}>
          <Button size="small" onClick={onClose}>
            Close
          </Button>
          <Button
            size="small"
            type="primary"
            onClick={() => applyUiFieldBuilder(placeholderIndex)}
          >
            Apply
          </Button>
        </Space>
      </Space>
    </Modal>
  )
}
