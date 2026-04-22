import { useCallback, useEffect, useMemo, useState, type ComponentType } from 'react'
import { useLocation, useParams } from 'react-router-dom'
import { Alert, Button, Segmented, Skeleton, Tag, Typography } from 'antd'
import { FilterOutlined, ReloadOutlined } from '@ant-design/icons'
import axios from 'axios'
import { ResponsiveGridLayout, useContainerWidth } from 'react-grid-layout'
import 'react-grid-layout/css/styles.css'
import 'react-resizable/css/styles.css'
import ChartWidget from '../../components/viz/ChartWidget'
import type { LayoutItem, Widget } from '../../store/vizStore'

const { Text, Title } = Typography
const ResponsiveGrid = ResponsiveGridLayout as unknown as ComponentType<any>
const API_BASE = import.meta.env.VITE_API_BASE || (import.meta.env.DEV ? 'http://localhost:8001' : '')
const http = axios.create({ baseURL: API_BASE, timeout: 30000 })

const GRID_COLS = 24
const ROW_HEIGHT = 30
const VIEW_MARGIN_Y = 12
const VIEW_HEADER_HEIGHT = 40
const DEFAULT_MIN_W = 4
const DEFAULT_MIN_H = 3

type DashboardDrillMode = 'down' | 'through'

interface DashboardDrillStep {
  field: string
  value: unknown
}

interface SharedDashboardPayload {
  id: string
  name: string
  description?: string
  widgets: Widget[]
  layout: LayoutItem[]
  theme?: string
  global_filters: any[]
}

interface WidgetDataEntry {
  data: any[]
  columns: string[]
  row_count: number
}

const FILTER_TAG_COLORS: Record<string, string> = {
  date: '#6366f1',
  region: '#06b6d4',
  category: '#8b5cf6',
  status: '#22c55e',
  default: 'var(--app-text-muted)',
}

function getFilterColor(key: string): string {
  return FILTER_TAG_COLORS[key.toLowerCase()] ?? FILTER_TAG_COLORS.default
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
  const layoutById = new Map(normalized.map((item) => [item.i, item]))
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

function computeWidgetBodyHeight(gridH: number, type: string): number {
  const totalCardHeight = gridH * ROW_HEIGHT + (gridH - 1) * VIEW_MARGIN_Y
  const bodyPadding = type === 'table' ? 0 : 8
  return Math.max(60, totalCardHeight - VIEW_HEADER_HEIGHT - 2 - bodyPadding)
}

interface FilterBarProps {
  filters: any[]
  theme?: string
}

function GlobalFilterBar({ filters, theme = 'dark' }: FilterBarProps) {
  if (!filters || filters.length === 0) return null
  const isLight = theme === 'light'
  const barBg = isLight ? '#f8fafc' : '#0a0a12'
  const barBorder = isLight ? '#dbe4f0' : 'var(--app-border)'
  const labelColor = isLight ? '#475569' : 'var(--app-text-dim)'

  return (
    <div style={{
      background: barBg,
      borderBottom: `1px solid ${barBorder}`,
      padding: '8px 24px',
      display: 'flex',
      alignItems: 'center',
      gap: 10,
      flexWrap: 'wrap',
      minHeight: 44,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexShrink: 0 }}>
        <FilterOutlined style={{ color: labelColor, fontSize: 13 }} />
        <Text style={{ color: labelColor, fontSize: 12, fontWeight: 600, letterSpacing: '0.05em', textTransform: 'uppercase' }}>
          Filters
        </Text>
      </div>
      <div style={{ width: 1, height: 16, background: 'var(--app-border)', flexShrink: 0 }} />
      {filters.map((filter: any, idx: number) => {
        const key = filter.field || filter.key || `filter_${idx}`
        const label = filter.label || filter.field || key
        const value = filter.value !== undefined ? String(filter.value) : null
        const color = getFilterColor(key)
        return (
          <Tag
            key={idx}
            style={{
              background: color + '15',
              border: `1px solid ${color}40`,
              color: color,
              borderRadius: 20,
              fontSize: 12,
              padding: '2px 12px',
              margin: 0,
              lineHeight: '22px',
              fontWeight: 500,
            }}
          >
            {label}{value ? `: ${value}` : ''}
          </Tag>
        )
      })}
    </div>
  )
}

interface DrillPathBarProps {
  mode: DashboardDrillMode
  onModeChange: (mode: DashboardDrillMode) => void
  path: DashboardDrillStep[]
  onStepUp: () => void
  onReset: () => void
  theme?: string
}

function DrillPathBar({ mode, onModeChange, path, onStepUp, onReset, theme = 'dark' }: DrillPathBarProps) {
  const isLight = theme === 'light'
  const barBg = isLight ? '#f8fafc' : '#0b1020'
  const barBorder = isLight ? '#dbe4f0' : '#1e293b'
  const titleColor = isLight ? '#1d4ed8' : '#93c5fd'
  const tagBg = isLight ? '#1d4ed815' : '#1d4ed820'
  const tagBorder = isLight ? '#93c5fd80' : '#3b82f655'
  const actionColor = isLight ? '#475569' : '#cbd5e1'
  return (
    <div style={{
      background: barBg,
      borderBottom: `1px solid ${barBorder}`,
      padding: '8px 24px',
      display: 'flex',
      alignItems: 'center',
      gap: 8,
      flexWrap: 'wrap',
      minHeight: 44,
    }}>
      <Text style={{ color: titleColor, fontSize: 12, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
        Drill Path
      </Text>
      <Segmented
        size="small"
        value={mode}
        options={[
          { label: 'Down', value: 'down' },
          { label: 'Through', value: 'through' },
        ]}
        onChange={(value) => onModeChange(value as DashboardDrillMode)}
      />
      {path.map((step, idx) => (
        <Tag
          key={`${step.field}_${idx}`}
          style={{
            background: tagBg,
            border: `1px solid ${tagBorder}`,
            color: titleColor,
            borderRadius: 16,
            margin: 0,
            fontSize: 12,
          }}
        >
          {step.field}: {String(step.value)}
        </Tag>
      ))}
      <Button
        size="small"
        onClick={onStepUp}
        disabled={!path.length}
        style={{ borderColor: 'var(--app-text-faint)', color: actionColor, background: 'transparent' }}
      >
        Up
      </Button>
      <Button
        size="small"
        onClick={onReset}
        disabled={!path.length}
        style={{ borderColor: 'var(--app-text-faint)', color: actionColor, background: 'transparent' }}
      >
        Reset
      </Button>
    </div>
  )
}

interface WidgetCardProps {
  widget: Widget
  dataEntry?: WidgetDataEntry
  chartHeight: number
  theme: string
  dashboardDrillMode: DashboardDrillMode
  dashboardDrillPath: DashboardDrillStep[]
  onDashboardDrill: (step: DashboardDrillStep) => void
}

function WidgetCard({
  widget,
  dataEntry,
  chartHeight,
  theme,
  dashboardDrillMode,
  dashboardDrillPath,
  onDashboardDrill,
}: WidgetCardProps) {
  const isLight = theme === 'light'
  const cardBg = isLight ? '#ffffff' : 'var(--app-panel-bg)'
  const cardBorder = isLight ? '#dbe4f0' : 'var(--app-border)'
  const headerBg = isLight ? '#f8fafc' : 'var(--app-panel-bg)'
  const headerBorder = isLight ? '#dbe4f0' : 'var(--app-border)'
  const titleColor = isLight ? '#0f172a' : 'var(--app-text)'
  const subtitleColor = isLight ? 'var(--app-text-subtle)' : 'var(--app-text-dim)'

  return (
    <div style={{
      height: '100%',
      background: cardBg,
      border: `1px solid ${cardBorder}`,
      borderRadius: 10,
      overflow: 'hidden',
      display: 'flex',
      flexDirection: 'column',
    }}>
      <div style={{
        background: headerBg,
        borderBottom: `1px solid ${headerBorder}`,
        padding: '8px 12px',
        display: 'flex',
        alignItems: 'center',
        minHeight: 40,
      }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <Text strong ellipsis style={{ color: titleColor, fontSize: 13, display: 'block', lineHeight: '1.3' }}>
            {widget.title}
          </Text>
          {widget.subtitle && (
            <Text ellipsis style={{ color: subtitleColor, fontSize: 11, display: 'block', lineHeight: '1.3' }}>
              {widget.subtitle}
            </Text>
          )}
        </div>
      </div>
      <div style={{ flex: 1, minHeight: 0, padding: widget.type === 'table' ? 0 : 4 }}>
        {!dataEntry ? (
          <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Skeleton active paragraph={{ rows: 3 }} style={{ padding: '16px 20px' }} />
          </div>
        ) : (
          <ChartWidget
            widget={widget}
            data={dataEntry.data}
            columns={dataEntry.columns}
            height={chartHeight}
            theme={theme}
            drillEnabled
            drillScope="dashboard"
            dashboardDrillMode={dashboardDrillMode}
            dashboardDrillPath={dashboardDrillPath}
            onDashboardDrill={onDashboardDrill}
          />
        )}
      </div>
    </div>
  )
}

export default function SharedDashboardPage() {
  const { token } = useParams<{ token: string }>()
  const location = useLocation()
  const embedOnly = location.pathname.startsWith('/embed/')
  const [dashboard, setDashboard] = useState<SharedDashboardPayload | null>(null)
  const [widgetData, setWidgetData] = useState<Record<string, WidgetDataEntry>>({})
  const [loadingDashboard, setLoadingDashboard] = useState(false)
  const [loadingWidgets, setLoadingWidgets] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [refreshKey, setRefreshKey] = useState(0)
  const [dashboardDrillMode, setDashboardDrillMode] = useState<DashboardDrillMode>('down')
  const [dashboardDrillPath, setDashboardDrillPath] = useState<DashboardDrillStep[]>([])
  const {
    width: gridWidth,
    containerRef: gridContainerRef,
    mounted: gridMounted,
  } = useContainerWidth({ initialWidth: 1280, measureBeforeMount: false })

  useEffect(() => {
    if (!token) return
    let active = true
    setLoadingDashboard(true)
    setError(null)
    setDashboard(null)
    setWidgetData({})
    setDashboardDrillMode('down')
    setDashboardDrillPath([])

    const loadSharedDashboard = async () => {
      try {
        const response = await http.get(`/api/share/${token}`)
        if (!active) return
        const payload = response.data as SharedDashboardPayload
        const widgets = Array.isArray(payload.widgets) ? payload.widgets : []
        const layout = mergeLayoutWithWidgets(payload.layout || [], widgets)
        setDashboard({
          ...payload,
          widgets,
          layout,
          theme: payload.theme || 'dark',
          global_filters: Array.isArray(payload.global_filters) ? payload.global_filters : [],
        })
      } catch {
        if (!active) return
        setError('Shared dashboard is unavailable or this link is invalid.')
      } finally {
        if (active) setLoadingDashboard(false)
      }
    }

    void loadSharedDashboard()
    return () => {
      active = false
    }
  }, [token])

  useEffect(() => {
    if (!dashboard) return
    let active = true
    setLoadingWidgets(true)

    const loadWidgetData = async () => {
      const entries = await Promise.all(
        dashboard.widgets.map(async (widget) => {
          try {
            const result = await http.post('/api/query', {
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
            })
            return [widget.id, result.data as WidgetDataEntry] as const
          } catch {
            return [widget.id, { data: [], columns: [], row_count: 0 }] as const
          }
        })
      )

      if (!active) return
      setWidgetData(Object.fromEntries(entries))
      setLoadingWidgets(false)
    }

    void loadWidgetData()
    return () => {
      active = false
    }
  }, [dashboard, refreshKey])

  const handleRefresh = useCallback(() => {
    setRefreshKey((prev) => prev + 1)
  }, [])

  const handleDashboardDrill = useCallback((step: DashboardDrillStep) => {
    setDashboardDrillPath((prev) => {
      const last = prev[prev.length - 1]
      if (last && last.field === step.field && String(last.value) === String(step.value)) return prev
      return [...prev, { field: step.field, value: step.value }]
    })
  }, [])

  const handleDrillStepUp = useCallback(() => {
    setDashboardDrillPath((prev) => prev.slice(0, -1))
  }, [])

  const handleDrillReset = useCallback(() => {
    setDashboardDrillPath([])
  }, [])

  const layout = useMemo(
    () => (dashboard ? mergeLayoutWithWidgets(dashboard.layout || [], dashboard.widgets || []) : []),
    [dashboard]
  )
  const layoutByWidgetId = useMemo(() => new Map(layout.map((item) => [item.i, item])), [layout])

  const baseLayout = useMemo(() => layout.map((item) => ({ ...item, static: true })), [layout])
  const layouts = useMemo(() => ({
    lg: baseLayout,
    md: baseLayout,
    sm: baseLayout.map((item) => ({ ...item, w: Math.min(item.w, 20), x: Math.min(item.x, 20) })),
    xs: baseLayout.map((item) => ({ ...item, w: Math.min(item.w, 8), x: 0 })),
    xxs: baseLayout.map((item) => ({ ...item, w: Math.min(item.w, 4), x: 0 })),
  }), [baseLayout])

  if (loadingDashboard) {
    return (
      <div style={{ minHeight: '100vh', background: 'var(--app-shell-bg)', padding: 24 }}>
        <Skeleton active paragraph={{ rows: 1 }} />
        <Skeleton active paragraph={{ rows: 8 }} style={{ marginTop: 16 }} />
      </div>
    )
  }

  if (error || !dashboard) {
    return (
      <div style={{ minHeight: '100vh', background: 'var(--app-shell-bg)', display: 'grid', placeItems: 'center', padding: 24 }}>
        <Alert type="error" message={error || 'Shared dashboard not found'} showIcon />
      </div>
    )
  }

  const theme = dashboard.theme || 'dark'

  return (
    <div style={{
      minHeight: embedOnly ? '100%' : '100vh',
      height: embedOnly ? '100%' : '100vh',
      background: theme === 'light' ? '#f1f5f9' : 'var(--app-shell-bg)',
      display: 'flex',
      flexDirection: 'column',
      overflow: 'hidden',
    }}>
      {!embedOnly && (
        <div style={{
          height: 56,
          background: theme === 'light' ? '#f8fafc' : '#0a0a12',
          borderBottom: `1px solid ${theme === 'light' ? '#dbe4f0' : 'var(--app-border)'}`,
          padding: '0 20px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 12,
          flexShrink: 0,
        }}>
          <div style={{ minWidth: 0 }}>
            <Title level={5} ellipsis style={{ margin: 0, color: 'var(--app-text)' }}>
              {dashboard.name}
            </Title>
            {dashboard.description && (
              <Text ellipsis style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                {dashboard.description}
              </Text>
            )}
          </div>
          <Button
            icon={<ReloadOutlined />}
            onClick={handleRefresh}
            style={{
              borderColor: 'var(--app-text-faint)',
              color: theme === 'light' ? '#475569' : '#cbd5e1',
              background: 'transparent',
            }}
          >
            Refresh
          </Button>
        </div>
      )}

      <GlobalFilterBar filters={dashboard.global_filters || []} theme={theme} />
      <DrillPathBar
        mode={dashboardDrillMode}
        onModeChange={setDashboardDrillMode}
        path={dashboardDrillPath}
        onStepUp={handleDrillStepUp}
        onReset={handleDrillReset}
        theme={theme}
      />

      <div style={{ flex: 1, overflow: 'auto', position: 'relative' }}>
        {loadingWidgets && (
          <div style={{
            position: 'absolute',
            inset: 0,
            background: 'var(--app-shell-overlay)',
            backdropFilter: 'blur(2px)',
            zIndex: 50,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}>
            <div style={{ color: 'var(--app-text-subtle)', fontSize: 14 }}>Loading widget data...</div>
          </div>
        )}

        {dashboard.widgets.length === 0 ? (
          <div style={{ minHeight: 320, display: 'grid', placeItems: 'center', padding: 24 }}>
            <Text style={{ color: 'var(--app-text-subtle)' }}>No widgets in this dashboard.</Text>
          </div>
        ) : (
          <div ref={gridContainerRef as any} style={{ minHeight: '100%' }}>
            {gridMounted && gridWidth > 0 && (
              <ResponsiveGrid
                width={gridWidth}
                layouts={layouts}
                cols={{ lg: 24, md: 20, sm: 12, xs: 8, xxs: 4 }}
                rowHeight={ROW_HEIGHT}
                margin={[12, 12]}
                containerPadding={[20, 20]}
                measureBeforeMount={false}
                style={{ minHeight: '100%' }}
              >
                {dashboard.widgets.map((widget) => (
                  <div key={widget.id} style={{ overflow: 'hidden' }}>
                    <WidgetCard
                      widget={widget}
                      dataEntry={widgetData[widget.id]}
                      theme={theme}
                      chartHeight={computeWidgetBodyHeight(layoutByWidgetId.get(widget.id)?.h ?? 6, widget.type)}
                      dashboardDrillMode={dashboardDrillMode}
                      dashboardDrillPath={dashboardDrillPath}
                      onDashboardDrill={handleDashboardDrill}
                    />
                  </div>
                ))}
              </ResponsiveGrid>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
