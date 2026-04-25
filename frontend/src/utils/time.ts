const TIMESTAMP_HAS_TZ_RE = /(Z|[+-]\d{2}(?::?\d{2})?)$/i
const TIMESTAMP_HAS_TIME_RE = /[T\s]\d{2}:\d{2}/
const SPACE_SEPARATED_ISO_RE = /^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}/

export function normalizeTimestampInput(value: unknown): string {
  let text = String(value || '').trim()
  if (!text) return ''
  if (SPACE_SEPARATED_ISO_RE.test(text)) {
    text = text.replace(/\s+/, 'T')
  }
  // Normalize excessive fractional seconds for consistent browser parsing.
  // Example: 2026-04-25T10:11:12.123456 -> 2026-04-25T10:11:12.123
  text = text.replace(/(\.\d{3})\d+(?=(Z|[+-]\d{2}(?::?\d{2})?)?$)/i, '$1')
  // Backend frequently emits UTC timestamps without timezone (python datetime.utcnow().isoformat()).
  // If time exists and TZ is missing, force UTC to avoid local-offset drift (for example +05:30).
  if (!TIMESTAMP_HAS_TZ_RE.test(text) && TIMESTAMP_HAS_TIME_RE.test(text)) {
    return `${text}Z`
  }
  return text
}

export function parseTimestampMs(value: unknown): number | undefined {
  const normalized = normalizeTimestampInput(value)
  if (!normalized) return undefined
  const ms = Date.parse(normalized)
  if (!Number.isFinite(ms)) return undefined
  return Number(ms)
}

export function parseTimestampMsOrNaN(value: unknown): number {
  const ms = parseTimestampMs(value)
  return typeof ms === 'number' ? ms : Number.NaN
}
