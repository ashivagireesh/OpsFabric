export type UiFieldBuilderTemplate =
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

export type UiFieldBuilderState = {
  template: UiFieldBuilderTemplate
  fieldPath: string
  fallback: string
  agg: string
  window: number
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

export function createDefaultUiFieldBuilderState(seed?: Partial<UiFieldBuilderState>): UiFieldBuilderState {
  const rawTemplate = String(seed?.template || 'field').trim() as UiFieldBuilderTemplate
  const allowedTemplates: UiFieldBuilderTemplate[] = [
    'field',
    'values',
    'upper_trim_field',
    'lower_trim_field',
    'coalesce_field',
    'num_field',
    'count_values',
    'sum_values',
    'mean_values',
    'min_values',
    'max_values',
    'distinct_values',
    'agg',
    'running_sum',
    'running_mean',
    'rolling_sum',
    'rolling_mean',
  ]
  const template: UiFieldBuilderTemplate = allowedTemplates.includes(rawTemplate) ? rawTemplate : 'field'
  const windowValue = Number(seed?.window)
  return {
    template,
    fieldPath: String(seed?.fieldPath || ''),
    fallback: String(seed?.fallback || '0'),
    agg: String(seed?.agg || 'sum').trim() || 'sum',
    window: Number.isFinite(windowValue) && windowValue > 0 ? Math.floor(windowValue) : 7,
  }
}

export function buildUiFieldBuilderExpression(state: UiFieldBuilderState): string {
  const safe = createDefaultUiFieldBuilderState(state)
  const path = String(safe.fieldPath || '').trim().replace(/'/g, "\\'")
  const fallback = String(safe.fallback || '').trim() || '0'
  const agg = String(safe.agg || 'sum').trim() || 'sum'
  const window = Number.isFinite(safe.window) && safe.window > 0 ? Math.floor(safe.window) : 7
  const fieldExpr = `field('${path}')`
  const valuesExpr = `values('${path}')`
  if (safe.template === 'values') return valuesExpr
  if (safe.template === 'upper_trim_field') return `upper(trim(${fieldExpr}))`
  if (safe.template === 'lower_trim_field') return `lower(trim(${fieldExpr}))`
  if (safe.template === 'coalesce_field') return `coalesce(${fieldExpr}, ${toExpressionLiteralToken(fallback)})`
  if (safe.template === 'num_field') return `num(${fieldExpr}, ${fallback})`
  if (safe.template === 'count_values') return `count(${valuesExpr})`
  if (safe.template === 'sum_values') return `sum(${valuesExpr})`
  if (safe.template === 'mean_values') return `mean(${valuesExpr})`
  if (safe.template === 'min_values') return `min(${valuesExpr})`
  if (safe.template === 'max_values') return `max(${valuesExpr})`
  if (safe.template === 'distinct_values') return `distinct(${valuesExpr})`
  if (safe.template === 'agg') return `agg('${path}', '${agg}')`
  if (safe.template === 'running_sum') return `running_sum(${fieldExpr})`
  if (safe.template === 'running_mean') return `running_mean(${fieldExpr})`
  if (safe.template === 'rolling_sum') return `rolling_sum(${fieldExpr}, ${window})`
  if (safe.template === 'rolling_mean') return `rolling_mean(${fieldExpr}, ${window})`
  return fieldExpr
}

export function parseUiFieldBuilderFromCurrent(mode: string, value: string): UiFieldBuilderState {
  const raw = String(value || '').trim()
  if (!raw) {
    return createDefaultUiFieldBuilderState({ template: mode === 'values' ? 'values' : 'field' })
  }
  if (mode === 'field') {
    return createDefaultUiFieldBuilderState({ template: 'field', fieldPath: raw })
  }
  if (mode === 'values') {
    return createDefaultUiFieldBuilderState({ template: 'values', fieldPath: raw })
  }
  const mField = raw.match(/^field\s*\(\s*['"](.*?)['"]\s*\)$/i)
  if (mField) return createDefaultUiFieldBuilderState({ template: 'field', fieldPath: String(mField[1] || '') })
  const mValues = raw.match(/^values\s*\(\s*['"](.*?)['"]\s*\)$/i)
  if (mValues) return createDefaultUiFieldBuilderState({ template: 'values', fieldPath: String(mValues[1] || '') })
  const mUpperTrim = raw.match(/^upper\s*\(\s*trim\s*\(\s*field\s*\(\s*['"](.*?)['"]\s*\)\s*\)\s*\)$/i)
  if (mUpperTrim) return createDefaultUiFieldBuilderState({ template: 'upper_trim_field', fieldPath: String(mUpperTrim[1] || '') })
  const mLowerTrim = raw.match(/^lower\s*\(\s*trim\s*\(\s*field\s*\(\s*['"](.*?)['"]\s*\)\s*\)\s*\)$/i)
  if (mLowerTrim) return createDefaultUiFieldBuilderState({ template: 'lower_trim_field', fieldPath: String(mLowerTrim[1] || '') })
  const mCoalesce = raw.match(/^coalesce\s*\(\s*field\s*\(\s*['"](.*?)['"]\s*\)\s*,\s*(.+)\)$/i)
  if (mCoalesce) {
    return createDefaultUiFieldBuilderState({
      template: 'coalesce_field',
      fieldPath: String(mCoalesce[1] || ''),
      fallback: String(mCoalesce[2] || '0').trim(),
    })
  }
  const mNum = raw.match(/^num\s*\(\s*field\s*\(\s*['"](.*?)['"]\s*\)\s*,\s*(.+)\)$/i)
  if (mNum) {
    return createDefaultUiFieldBuilderState({
      template: 'num_field',
      fieldPath: String(mNum[1] || ''),
      fallback: String(mNum[2] || '0').trim(),
    })
  }
  const mAgg = raw.match(/^agg\s*\(\s*['"](.*?)['"]\s*,\s*['"](.*?)['"]\s*\)$/i)
  if (mAgg) {
    return createDefaultUiFieldBuilderState({
      template: 'agg',
      fieldPath: String(mAgg[1] || ''),
      agg: String(mAgg[2] || 'sum'),
    })
  }
  const mRollingSum = raw.match(/^rolling_sum\s*\(\s*field\s*\(\s*['"](.*?)['"]\s*\)\s*,\s*(\d+)\s*\)$/i)
  if (mRollingSum) {
    return createDefaultUiFieldBuilderState({
      template: 'rolling_sum',
      fieldPath: String(mRollingSum[1] || ''),
      window: Number(mRollingSum[2] || 7),
    })
  }
  const mRollingMean = raw.match(/^rolling_mean\s*\(\s*field\s*\(\s*['"](.*?)['"]\s*\)\s*,\s*(\d+)\s*\)$/i)
  if (mRollingMean) {
    return createDefaultUiFieldBuilderState({
      template: 'rolling_mean',
      fieldPath: String(mRollingMean[1] || ''),
      window: Number(mRollingMean[2] || 7),
    })
  }
  return createDefaultUiFieldBuilderState({
    template: 'field',
    fieldPath: raw.replace(/^['"]|['"]$/g, ''),
  })
}
