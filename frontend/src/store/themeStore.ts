import { create } from 'zustand'

export type AppThemeMode = 'dark' | 'light'

const STORAGE_KEY = 'etlflow_app_theme_mode'

function resolveInitialMode(): AppThemeMode {
  if (typeof window === 'undefined') return 'dark'
  const saved = window.localStorage.getItem(STORAGE_KEY)
  if (saved === 'dark' || saved === 'light') return saved
  try {
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) {
      return 'light'
    }
  } catch {
    // ignore unsupported environments
  }
  return 'dark'
}

function persistMode(mode: AppThemeMode) {
  if (typeof window === 'undefined') return
  window.localStorage.setItem(STORAGE_KEY, mode)
}

interface ThemeState {
  mode: AppThemeMode
  setMode: (mode: AppThemeMode) => void
  toggleMode: () => void
}

export const useThemeStore = create<ThemeState>((set) => ({
  mode: resolveInitialMode(),
  setMode: (mode) => {
    persistMode(mode)
    set({ mode })
  },
  toggleMode: () =>
    set((state) => {
      const nextMode: AppThemeMode = state.mode === 'dark' ? 'light' : 'dark'
      persistMode(nextMode)
      return { mode: nextMode }
    }),
}))

