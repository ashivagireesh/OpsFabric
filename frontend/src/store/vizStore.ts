import { create } from 'zustand'
import axios from 'axios'

const BASE = 'http://localhost:8001'
const http = axios.create({ baseURL: BASE, timeout: 30000 })
const GRID_COLS = 24
const DEFAULT_MIN_W = 4
const DEFAULT_MIN_H = 3
let latestRequestedDashboardId: string | null = null
let widgetQuerySequence = 0
const latestWidgetQueryById: Record<string, number> = {}

export interface Widget {
  id: string
  type: string // 'kpi'|'bar'|'line'|'area'|'pie'|'donut'|'scatter'|'heatmap'|'treemap'|'funnel'|'gauge'|'table'|'radar'|'waterfall'|'text'
  title: string
  subtitle?: string
  data_source: {
    type: 'sample'|'pipeline'|'file'|'mlops'
    dataset?: string
    pipeline_id?: string
    pipeline_node_id?: string
    file_path?: string
    mlops_workflow_id?: string
    mlops_run_id?: string
    mlops_node_id?: string
    mlops_output_mode?: 'predictions' | 'metrics' | 'monitor' | 'evaluation'
    mlops_prediction_start_date?: string
    mlops_prediction_end_date?: string
    sql?: string
  }
  chart_config: {
    x_field?: string
    y_field?: string
    y_fields?: string[]
    group_by?: string
    aggregation?: string
    color_field?: string
    sort_by?: string
    sort_order?: string
    limit?: number
    kpi_field?: string
    kpi_label?: string
    trend_field?: string
    text_content?: string
  }
  style: {
    color_palette?: string[]
    theme?: string
    show_legend?: boolean
    show_grid?: boolean
    show_labels?: boolean
    bg_color?: string
  }
}

export interface LayoutItem {
  i: string; x: number; y: number; w: number; h: number; minW?: number; minH?: number
}

export interface Dashboard {
  id: string
  name: string
  description?: string
  owner: string
  widgets: Widget[]
  layout: LayoutItem[]
  theme: string
  global_filters: any[]
  tags: string[]
  is_public: boolean
  share_token?: string
  created_at?: string
  updated_at?: string
  widget_count?: number
}

export interface WidgetDataResult {
  data: any[]
  columns: string[]
  row_count: number
}

interface VizState {
  dashboards: Dashboard[]
  activeDashboard: Dashboard | null
  loading: boolean
  isDirty: boolean

  // data cache: widgetId -> {data, columns, row_count}
  widgetData: Record<string, WidgetDataResult>

  fetchDashboards: () => Promise<void>
  loadDashboard: (id: string) => Promise<void>
  createDashboard: (name: string, description?: string) => Promise<Dashboard>
  saveDashboard: (layoutOverride?: LayoutItem[]) => Promise<void>
  deleteDashboard: (id: string) => Promise<void>
  duplicateDashboard: (id: string) => Promise<void>
  shareDashboard: (id: string) => Promise<{ share_token: string; share_url: string; embed_url?: string }>

  setActiveDashboard: (d: Dashboard) => void
  updateLayout: (layout: LayoutItem[]) => void
  addWidget: (widget: Widget) => void
  updateWidget: (id: string, updates: Partial<Widget>) => void
  removeWidget: (id: string) => void

  fetchWidgetData: (widget: Widget) => Promise<void>
  fetchAllWidgetData: () => Promise<void>
}

function toSafeInt(value: unknown, fallback: number): number {
  const n = Number(value)
  return Number.isFinite(n) ? Math.trunc(n) : fallback
}

function defaultWidgetSize(type: string): { w: number; h: number } {
  if (type === 'kpi') return { w: 6, h: 4 }
  if (type === 'table') return { w: 24, h: 8 }
  return { w: 12, h: 9 }
}

function normalizeLayoutItem(raw: Partial<LayoutItem>, fallbackIndex: number): LayoutItem {
  const minW = Math.max(1, toSafeInt(raw.minW, DEFAULT_MIN_W))
  const minH = Math.max(1, toSafeInt(raw.minH, DEFAULT_MIN_H))
  const w = Math.max(minW, Math.min(GRID_COLS, toSafeInt(raw.w, 12)))
  const h = Math.max(minH, toSafeInt(raw.h, 9))
  const xRaw = Math.max(0, toSafeInt(raw.x, 0))
  const x = Math.min(xRaw, Math.max(0, GRID_COLS - w))
  const y = Math.max(0, toSafeInt(raw.y, fallbackIndex * minH))
  return {
    i: String(raw.i ?? ''),
    x,
    y,
    w,
    h,
    minW,
    minH,
  }
}

function sanitizeLayout(layout: LayoutItem[]): LayoutItem[] {
  const seen = new Set<string>()
  const clean: LayoutItem[] = []
  layout.forEach((item, idx) => {
    const id = String(item?.i ?? '')
    if (!id || seen.has(id)) return
    seen.add(id)
    clean.push(normalizeLayoutItem({ ...item, i: id }, idx))
  })
  return clean
}

function nextLayoutY(layout: LayoutItem[]): number {
  return layout.reduce((maxY, item) => Math.max(maxY, item.y + item.h), 0)
}

function mergeLayoutWithWidgets(layout: LayoutItem[], widgets: Widget[]): LayoutItem[] {
  const normalized = sanitizeLayout(layout)
  const layoutById = new Map(normalized.map(item => [item.i, item]))
  let nextY = nextLayoutY(normalized)

  widgets.forEach((widget) => {
    if (layoutById.has(widget.id)) return
    const { w, h } = defaultWidgetSize(widget.type)
    const baseIndex = layoutById.size
    const item = normalizeLayoutItem({
      i: widget.id,
      x: (baseIndex * 6) % GRID_COLS,
      y: nextY,
      w,
      h,
      minW: DEFAULT_MIN_W,
      minH: DEFAULT_MIN_H,
    }, baseIndex)
    layoutById.set(item.i, item)
    nextY = Math.max(nextY, item.y + item.h)
  })

  return Array.from(layoutById.values())
}

function toWidgetQueryPayload(widget: Widget) {
  return {
    source_type: widget.data_source.type,
    dataset: widget.data_source.dataset || 'sales',
    pipeline_id: widget.data_source.pipeline_id,
    pipeline_node_id: widget.data_source.pipeline_node_id,
    file_path: widget.data_source.file_path,
    mlops_workflow_id: widget.data_source.mlops_workflow_id,
    mlops_run_id: widget.data_source.mlops_run_id,
    mlops_node_id: widget.data_source.mlops_node_id,
    mlops_output_mode: widget.data_source.mlops_output_mode,
    mlops_prediction_start_date: widget.data_source.mlops_prediction_start_date,
    mlops_prediction_end_date: widget.data_source.mlops_prediction_end_date,
    sql: widget.data_source.sql,
  }
}

function normalizeWidgetDataResponse(raw: any): WidgetDataResult {
  const data = Array.isArray(raw?.data) ? raw.data : []
  const fallbackColumns = data.length > 0 && data[0] && typeof data[0] === 'object'
    ? Object.keys(data[0] as Record<string, unknown>)
    : []
  return {
    data,
    columns: Array.isArray(raw?.columns) ? raw.columns.map((c: unknown) => String(c)) : fallbackColumns,
    row_count: Number.isFinite(Number(raw?.row_count)) ? Number(raw.row_count) : data.length,
  }
}

function emptyWidgetData(): WidgetDataResult {
  return { data: [], columns: [], row_count: 0 }
}

function hasWidgetSourceChanged(prev: Widget, next: Widget): boolean {
  return JSON.stringify(prev.data_source || {}) !== JSON.stringify(next.data_source || {})
}

export const useVizStore = create<VizState>((set, get) => ({
  dashboards: [],
  activeDashboard: null,
  loading: false,
  isDirty: false,
  widgetData: {},

  fetchDashboards: async () => {
    set({ loading: true })
    try {
      const r = await http.get('/api/dashboards')
      set({ dashboards: r.data })
    } catch {
      set({ dashboards: [] })
    } finally {
      set({ loading: false })
    }
  },

  loadDashboard: async (id) => {
    latestRequestedDashboardId = id
    const requestedId = id
    const currentActive = get().activeDashboard
    const switchingDashboard = !currentActive || currentActive.id !== id
    if (switchingDashboard) {
      set({ loading: true, isDirty: false, activeDashboard: null, widgetData: {} })
    } else {
      set({ loading: true, isDirty: false })
    }
    try {
      const r = await http.get(`/api/dashboards/${id}`)
      if (requestedId !== latestRequestedDashboardId) return
      const dashboard = r.data as Dashboard
      const layout = mergeLayoutWithWidgets(dashboard.layout || [], dashboard.widgets || [])
      set({ activeDashboard: { ...dashboard, layout }, isDirty: false, widgetData: {} })
      await get().fetchAllWidgetData()
    } catch {
      // fallback
    } finally {
      if (requestedId === latestRequestedDashboardId) {
        set({ loading: false })
      }
    }
  },

  createDashboard: async (name, description = '') => {
    const r = await http.post('/api/dashboards', { name, description })
    await get().fetchDashboards()
    return r.data
  },

  saveDashboard: async (layoutOverride) => {
    const { activeDashboard } = get()
    if (!activeDashboard) return
    const sourceLayout = layoutOverride ?? activeDashboard.layout ?? []
    const layout = mergeLayoutWithWidgets(sourceLayout, activeDashboard.widgets || [])
    try {
      await http.put(`/api/dashboards/${activeDashboard.id}`, {
        name: activeDashboard.name,
        description: activeDashboard.description,
        widgets: activeDashboard.widgets,
        layout,
        theme: activeDashboard.theme,
        global_filters: activeDashboard.global_filters,
        tags: activeDashboard.tags,
        is_public: activeDashboard.is_public,
      })
      set((state) => ({
        activeDashboard: state.activeDashboard ? { ...state.activeDashboard, layout } : state.activeDashboard,
        isDirty: false
      }))
    } catch (e) {
      console.error('Save failed:', e)
      throw e
    }
  },

  deleteDashboard: async (id) => {
    await http.delete(`/api/dashboards/${id}`)
    await get().fetchDashboards()
    if (get().activeDashboard?.id === id) set({ activeDashboard: null })
  },

  duplicateDashboard: async (id) => {
    await http.post(`/api/dashboards/${id}/duplicate`)
    await get().fetchDashboards()
  },

  shareDashboard: async (id) => {
    const r = await http.post(`/api/dashboards/${id}/share`)
    await get().fetchDashboards()
    return r.data
  },

  setActiveDashboard: (d) => {
    const layout = mergeLayoutWithWidgets(d.layout || [], d.widgets || [])
    set({ activeDashboard: { ...d, layout }, isDirty: false })
  },

  updateLayout: (layout) => {
    set(state => ({
      activeDashboard: state.activeDashboard
        ? { ...state.activeDashboard, layout: mergeLayoutWithWidgets(layout, state.activeDashboard.widgets) }
        : null,
      isDirty: true
    }))
  },

  addWidget: (widget) => {
    set(state => {
      if (!state.activeDashboard) return {}
      const widgets = [...state.activeDashboard.widgets, widget]
      const currentLayout = mergeLayoutWithWidgets(state.activeDashboard.layout, state.activeDashboard.widgets)
      const { w, h } = defaultWidgetSize(widget.type)
      const candidate = normalizeLayoutItem({
        i: widget.id,
        x: (currentLayout.length * 6) % GRID_COLS,
        y: nextLayoutY(currentLayout),
        w,
        h,
        minW: DEFAULT_MIN_W,
        minH: DEFAULT_MIN_H,
      }, currentLayout.length)
      const layout = mergeLayoutWithWidgets([...currentLayout, candidate], widgets)
      return { activeDashboard: { ...state.activeDashboard, widgets, layout }, isDirty: true }
    })
    get().fetchWidgetData(widget)
  },

  updateWidget: (id, updates) => {
    let nextWidget: Widget | null = null
    let shouldRefetch = false
    set(state => {
      if (!state.activeDashboard) return {}
      const widgets = state.activeDashboard.widgets.map(w => {
        if (w.id !== id) return w
        const updatedWidget = { ...w, ...updates } as Widget
        shouldRefetch = hasWidgetSourceChanged(w, updatedWidget)
        nextWidget = updatedWidget
        return updatedWidget
      })
      return { activeDashboard: { ...state.activeDashboard, widgets }, isDirty: true }
    })
    if (shouldRefetch && nextWidget) {
      get().fetchWidgetData(nextWidget)
    }
  },

  removeWidget: (id) => {
    set(state => {
      if (!state.activeDashboard) return {}
      const widgets = state.activeDashboard.widgets.filter(w => w.id !== id)
      const layout = mergeLayoutWithWidgets(
        state.activeDashboard.layout.filter(l => l.i !== id),
        widgets
      )
      return {
        activeDashboard: {
          ...state.activeDashboard,
          widgets,
          layout,
        },
        isDirty: true
      }
    })
  },

  fetchWidgetData: async (widget) => {
    const requestId = ++widgetQuerySequence
    latestWidgetQueryById[widget.id] = requestId
    try {
      const r = await http.post('/api/query', toWidgetQueryPayload(widget))
      const nextData = normalizeWidgetDataResponse(r.data)
      if (latestWidgetQueryById[widget.id] !== requestId) {
        return
      }
      set(state => ({
        widgetData: { ...state.widgetData, [widget.id]: nextData }
      }))
    } catch (e) {
      if (latestWidgetQueryById[widget.id] !== requestId) {
        return
      }
      console.error('Widget data fetch failed:', widget.id, e)
      set(state => ({
        widgetData: {
          ...state.widgetData,
          [widget.id]: state.widgetData[widget.id] ?? emptyWidgetData(),
        }
      }))
    }
  },

  fetchAllWidgetData: async () => {
    const { activeDashboard } = get()
    if (!activeDashboard) return
    const snapshotDashboardId = activeDashboard.id
    const groups = new Map<string, { payload: ReturnType<typeof toWidgetQueryPayload>; widgetIds: string[] }>()

    activeDashboard.widgets.forEach((widget) => {
      const payload = toWidgetQueryPayload(widget)
      const key = JSON.stringify(payload)
      const existing = groups.get(key)
      if (existing) {
        existing.widgetIds.push(widget.id)
      } else {
        groups.set(key, { payload, widgetIds: [widget.id] })
      }
    })

    const groupResults = await Promise.all(
      Array.from(groups.values()).map(async (group) => {
        try {
          const response = await http.post('/api/query', group.payload)
          const data = normalizeWidgetDataResponse(response.data)
          return { widgetIds: group.widgetIds, data }
        } catch (error) {
          console.error('Widget data fetch failed:', group.widgetIds.join(','), error)
          return { widgetIds: group.widgetIds, data: emptyWidgetData() }
        }
      })
    )

    const merged: Record<string, WidgetDataResult> = {}
    groupResults.forEach((result) => {
      result.widgetIds.forEach((widgetId) => {
        merged[widgetId] = result.data
      })
    })
    if (get().activeDashboard?.id !== snapshotDashboardId) {
      return
    }
    set({ widgetData: merged })
  },
}))
