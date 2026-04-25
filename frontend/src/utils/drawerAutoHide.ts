export type DrawerAutoHideScope = 'etl' | 'business' | 'mlops'

const DEFAULT_IDLE_MS = 3000
const lastInteractionByScope = new Map<DrawerAutoHideScope, number>()

export function markDrawerInteraction(scope: DrawerAutoHideScope, atMs?: number): void {
  lastInteractionByScope.set(scope, typeof atMs === 'number' ? atMs : Date.now())
}

export function shouldCloseDrawerOnPaneClick(scope: DrawerAutoHideScope, minIdleMs = DEFAULT_IDLE_MS): boolean {
  const lastInteractionMs = lastInteractionByScope.get(scope)
  if (typeof lastInteractionMs !== 'number') return true
  return Date.now() - lastInteractionMs >= Math.max(0, Number(minIdleMs) || 0)
}

export function clearDrawerInteraction(scope: DrawerAutoHideScope): void {
  lastInteractionByScope.delete(scope)
}
