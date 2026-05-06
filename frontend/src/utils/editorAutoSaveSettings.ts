export const EDITOR_AUTOSAVE_ENABLED_KEY = 'framework_editor_autosave_enabled'
export const EDITOR_AUTOSAVE_INTERVAL_MS_KEY = 'framework_editor_autosave_interval_ms'
export const EDITOR_AUTOSAVE_SETTINGS_CHANGED_EVENT = 'framework:editor-autosave-settings-changed'

export const EDITOR_AUTOSAVE_DEFAULT_ENABLED = true
export const EDITOR_AUTOSAVE_DEFAULT_INTERVAL_MS = 2500
export const EDITOR_AUTOSAVE_MIN_INTERVAL_MS = 500
export const EDITOR_AUTOSAVE_MAX_INTERVAL_MS = 120000

export type EditorAutoSaveSettings = {
  enabled: boolean
  intervalMs: number
}

function normalizeEnabled(value: unknown): boolean {
  if (value == null) return EDITOR_AUTOSAVE_DEFAULT_ENABLED
  const raw = String(value).trim().toLowerCase()
  if (['0', 'false', 'off', 'no'].includes(raw)) return false
  if (['1', 'true', 'on', 'yes'].includes(raw)) return true
  return EDITOR_AUTOSAVE_DEFAULT_ENABLED
}

function normalizeIntervalMs(value: unknown): number {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return EDITOR_AUTOSAVE_DEFAULT_INTERVAL_MS
  const rounded = Math.round(parsed)
  if (rounded < EDITOR_AUTOSAVE_MIN_INTERVAL_MS) return EDITOR_AUTOSAVE_MIN_INTERVAL_MS
  if (rounded > EDITOR_AUTOSAVE_MAX_INTERVAL_MS) return EDITOR_AUTOSAVE_MAX_INTERVAL_MS
  return rounded
}

export function getPersistedEditorAutoSaveSettings(): EditorAutoSaveSettings {
  if (typeof window === 'undefined') {
    return {
      enabled: EDITOR_AUTOSAVE_DEFAULT_ENABLED,
      intervalMs: EDITOR_AUTOSAVE_DEFAULT_INTERVAL_MS,
    }
  }
  try {
    return {
      enabled: normalizeEnabled(window.localStorage.getItem(EDITOR_AUTOSAVE_ENABLED_KEY)),
      intervalMs: normalizeIntervalMs(window.localStorage.getItem(EDITOR_AUTOSAVE_INTERVAL_MS_KEY)),
    }
  } catch {
    return {
      enabled: EDITOR_AUTOSAVE_DEFAULT_ENABLED,
      intervalMs: EDITOR_AUTOSAVE_DEFAULT_INTERVAL_MS,
    }
  }
}

export function persistEditorAutoSaveSettings(
  updates: Partial<EditorAutoSaveSettings>,
): EditorAutoSaveSettings {
  const current = getPersistedEditorAutoSaveSettings()
  const next: EditorAutoSaveSettings = {
    enabled: updates.enabled == null ? current.enabled : Boolean(updates.enabled),
    intervalMs: updates.intervalMs == null ? current.intervalMs : normalizeIntervalMs(updates.intervalMs),
  }
  if (typeof window !== 'undefined') {
    try {
      window.localStorage.setItem(EDITOR_AUTOSAVE_ENABLED_KEY, next.enabled ? '1' : '0')
      window.localStorage.setItem(EDITOR_AUTOSAVE_INTERVAL_MS_KEY, String(next.intervalMs))
      window.dispatchEvent(new CustomEvent(EDITOR_AUTOSAVE_SETTINGS_CHANGED_EVENT, { detail: next }))
    } catch {
      // best-effort persistence only
    }
  }
  return next
}
