import { EXPRESSION_FUNCTION_SNIPPETS } from './CustomFieldStudioConstants'

export const BRE_CLAUSE_OPERATOR_OPTIONS = [
  { value: '==', label: 'Equals (==)' },
  { value: '!=', label: 'Not Equals (!=)' },
  { value: '>', label: 'Greater Than (>)' },
  { value: '>=', label: 'Greater or Equal (>=)' },
  { value: '<', label: 'Less Than (<)' },
  { value: '<=', label: 'Less or Equal (<=)' },
  { value: 'raw', label: 'Raw Condition' },
]

export const DEFAULT_EXPRESSION_FUNCTION_NAMES = new Set(
  [
    ...EXPRESSION_FUNCTION_SNIPPETS.map((item) => item.snippet),
    ...EXPRESSION_FUNCTION_SNIPPETS.map((item) => item.label),
    'num(value,default)',
    'str(value)',
    'length(value)',
    'abs(value)',
    'sqrt(value)',
    'log(value)',
    'exp(value)',
    'ceil(value)',
    'floor(value)',
    'sin(value)',
    'cos(value)',
    'tan(value)',
    'agg(value,agg_name)',
  ]
    .map((text) => {
      const m = String(text || '').match(/\b([A-Za-z_]\w*)\s*\(/)
      return m ? m[1].toLowerCase() : ''
    })
    .filter(Boolean)
)
