import ReactECharts from 'echarts-for-react'
import { useMemo, useState, useCallback, useEffect, memo } from 'react'
import { Typography, Table, Modal, Segmented, Empty, Button } from 'antd'
import { ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons'
import type { Widget } from '../../store/vizStore'

const { Text } = Typography

const COLOR_PALETTES: Record<string, string[]> = {
  indigo:   ['#6366f1','#8b5cf6','#a855f7','#ec4899','#f43f5e','#f97316','#eab308','#22c55e'],
  ocean:    ['#06b6d4','#0891b2','#0e7490','#155e75','#164e63','#0f766e','#0d9488','#14b8a6'],
  forest:   ['#22c55e','#16a34a','#15803d','#166534','#14532d','#4ade80','#86efac','#bbf7d0'],
  sunset:   ['#f97316','#ea580c','#ef4444','#dc2626','#b91c1c','#f59e0b','#d97706','#b45309'],
  slate:    ['var(--app-text-muted)','var(--app-text-subtle)','var(--app-text-dim)','var(--app-text-faint)','#1e293b','#cbd5e1','var(--app-text)','#f1f5f9'],
  vivid:    ['#3b82f6','#8b5cf6','#ec4899','#ef4444','#f97316','#eab308','#22c55e','#06b6d4'],
}

const DEFAULT_PALETTE = COLOR_PALETTES.indigo

type DataRow = Record<string, unknown>
type DrillMode = 'down' | 'through'

interface DrillStep {
  field: string
  value: unknown
}

interface DrillContext {
  label: string
  dimensionField?: string
  dimensionValue?: unknown
  measureField?: string
  measureValue?: number
}

function toNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null
  if (typeof value === 'number') return Number.isFinite(value) ? value : null
  if (typeof value !== 'string') return null
  const trimmed = value.trim()
  if (!trimmed) return null
  const normalized = trimmed
    .replace(/^\((.*)\)$/, '-$1')
    .replace(/,/g, '')
    .replace(/[^\d.+-]/g, '')
  if (!normalized || normalized === '-' || normalized === '.' || normalized === '+') return null
  const parsed = Number(normalized)
  return Number.isFinite(parsed) ? parsed : null
}

function parseColorToRgb(color: string): [number, number, number] | null {
  const value = color.trim().toLowerCase()
  if (!value || value === 'transparent') return null

  if (value.startsWith('#')) {
    const hex = value.slice(1)
    if (hex.length === 3) {
      const r = parseInt(hex[0] + hex[0], 16)
      const g = parseInt(hex[1] + hex[1], 16)
      const b = parseInt(hex[2] + hex[2], 16)
      if ([r, g, b].some(Number.isNaN)) return null
      return [r, g, b]
    }
    if (hex.length === 6) {
      const r = parseInt(hex.slice(0, 2), 16)
      const g = parseInt(hex.slice(2, 4), 16)
      const b = parseInt(hex.slice(4, 6), 16)
      if ([r, g, b].some(Number.isNaN)) return null
      return [r, g, b]
    }
    return null
  }

  const rgbMatch = value.match(/^rgba?\(([^)]+)\)$/)
  if (!rgbMatch) return null

  const parts = rgbMatch[1].split(',').map(p => p.trim())
  if (parts.length < 3) return null

  const r = Number(parts[0])
  const g = Number(parts[1])
  const b = Number(parts[2])
  if ([r, g, b].some(Number.isNaN)) return null
  return [Math.max(0, Math.min(255, r)), Math.max(0, Math.min(255, g)), Math.max(0, Math.min(255, b))]
}

function rgbToHex(r: number, g: number, b: number): string {
  const toHex = (value: number) => Math.max(0, Math.min(255, Math.round(value))).toString(16).padStart(2, '0')
  return `#${toHex(r)}${toHex(g)}${toHex(b)}`
}

const THEME_VAR_COLOR_MAP: Record<'light' | 'dark', Record<string, string>> = {
  light: {
    '--app-shell-bg': '#f5f7fb',
    '--app-shell-bg-2': '#eef2f8',
    '--app-panel-bg': '#ffffff',
    '--app-panel-2': '#f8fafc',
    '--app-card-bg': '#ffffff',
    '--app-input-bg': '#f8fafc',
    '--app-border': '#dbe4f0',
    '--app-border-strong': '#cbd5e1',
    '--app-text': '#0f172a',
    '--app-text-muted': '#334155',
    '--app-text-subtle': '#475569',
    '--app-text-dim': '#64748b',
    '--app-text-faint': '#94a3b8',
    '--app-border-soft-a': 'rgba(148, 163, 184, 0.12)',
    '--app-border-soft-b': 'rgba(148, 163, 184, 0.2)',
  },
  dark: {
    '--app-shell-bg': '#0f0f13',
    '--app-shell-bg-2': '#090910',
    '--app-panel-bg': '#0d0d18',
    '--app-panel-2': '#0a0a14',
    '--app-card-bg': '#1a1a24',
    '--app-input-bg': '#151520',
    '--app-border': '#1e1e2e',
    '--app-border-strong': '#2a2a3d',
    '--app-text': '#e2e8f0',
    '--app-text-muted': '#94a3b8',
    '--app-text-subtle': '#64748b',
    '--app-text-dim': '#475569',
    '--app-text-faint': '#334155',
    '--app-border-soft-a': 'rgba(30, 30, 46, 0.06)',
    '--app-border-soft-b': 'rgba(30, 30, 46, 0.12)',
  },
}

function resolveCssValue(value: string, isLight: boolean, depth = 0): string {
  const input = String(value || '').trim()
  if (!input || !input.includes('var(') || depth > 6) return input

  const themeMap = THEME_VAR_COLOR_MAP[isLight ? 'light' : 'dark']
  const replaced = input.replace(/var\((--[^),\s]+)(?:,\s*([^)]+))?\)/g, (_match, varName: string, fallback?: string) => {
    if (themeMap[varName]) return themeMap[varName]

    if (typeof window !== 'undefined' && typeof document !== 'undefined') {
      const computed = window.getComputedStyle(document.documentElement).getPropertyValue(varName).trim()
      if (computed) return computed
    }

    if (fallback) return resolveCssValue(fallback.trim(), isLight, depth + 1)
    return ''
  })

  if (replaced.includes('var(') && replaced !== input) {
    return resolveCssValue(replaced, isLight, depth + 1)
  }

  return replaced
}

function resolveChartColor(value: string | undefined, isLight: boolean, fallback: string): string {
  const raw = (value ?? '').trim()
  if (!raw) return fallback
  const resolved = resolveCssValue(raw, isLight) || fallback
  const rgb = parseColorToRgb(resolved)
  return rgb ? rgbToHex(rgb[0], rgb[1], rgb[2]) : resolved
}

function withAlpha(color: string, alpha: number): string {
  const rgb = parseColorToRgb(color)
  if (!rgb) return color
  const clamped = Math.max(0, Math.min(1, alpha))
  return `rgba(${rgb[0]}, ${rgb[1]}, ${rgb[2]}, ${clamped})`
}

function isLightColor(color?: string): boolean | null {
  if (!color) return null
  const rgb = parseColorToRgb(color)
  if (!rgb) return null
  const [r, g, b] = rgb
  const luminance = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255
  return luminance > 0.6
}

function aggregate(data: any[], groupField: string, valueField: string, method = 'sum') {
  const groups: Record<string, { nums: number[]; count: number }> = {}
  data.forEach(row => {
    const key = String(row[groupField] ?? 'Other')
    if (!groups[key]) groups[key] = { nums: [], count: 0 }
    groups[key].count += 1
    const v = toNumber(row[valueField])
    if (v !== null) groups[key].nums.push(v)
  })
  return Object.entries(groups).map(([name, bucket]) => {
    const vals = bucket.nums
    let value = 0
    if (method === 'count') {
      value = bucket.count
    } else if (vals.length === 0) {
      // For non-numeric measure fields, keep visualization useful via row counts.
      value = bucket.count
    } else if (method === 'sum') value = vals.reduce((a, b) => a + b, 0)
    else if (method === 'avg') value = vals.reduce((a, b) => a + b, 0) / vals.length
    else if (method === 'min') value = Math.min(...vals)
    else if (method === 'max') value = Math.max(...vals)
    return { name, value: Math.round(value * 100) / 100 }
  })
}

interface Props {
  widget: Widget
  data: any[]
  columns: string[]
  height?: number
  theme?: string
  drillEnabled?: boolean
  drillScope?: 'widget' | 'dashboard'
  dashboardDrillMode?: DrillMode
  dashboardDrillPath?: DrillStep[]
  onDashboardDrill?: (step: DrillStep) => void
}

function ChartWidget({
  widget,
  data,
  columns,
  height = 300,
  theme = 'dark',
  drillEnabled = false,
  drillScope = 'widget',
  dashboardDrillMode = 'down',
  dashboardDrillPath,
  onDashboardDrill,
}: Props) {
  const rawPalette = widget.style?.color_palette?.length ? widget.style.color_palette : DEFAULT_PALETTE
  const cfg = widget.chart_config || {}
  const showLegend = widget.style?.show_legend !== false
  const showGrid = widget.style?.show_grid !== false
  const rows = Array.isArray(data) ? (data as DataRow[]) : []

  const isLight = theme === 'light'
  const palette = useMemo(() => (
    rawPalette.map((color, index) => resolveChartColor(color, isLight, DEFAULT_PALETTE[index % DEFAULT_PALETTE.length]))
  ), [isLight, rawPalette])
  const baseTextColor = isLight ? '#475569' : '#cbd5e1'
  const gridLineColor = isLight ? '#dbe4f0' : '#334155'
  const bgColor = useMemo(() => {
    const rawBg = widget.style?.bg_color || (isLight ? '#ffffff' : 'transparent')
    if (rawBg === 'transparent') return 'transparent'
    return resolveCssValue(rawBg, isLight) || (isLight ? '#ffffff' : '#0f172a')
  }, [widget.style?.bg_color, isLight])
  const tooltipBg = isLight ? '#ffffff' : '#0f172a'
  const tooltipBorder = isLight ? '#cbd5e1' : '#334155'
  const tooltipText = isLight ? '#1e293b' : '#e2e8f0'
  const tableTextColor = isLight ? '#1e293b' : '#e2e8f0'
  const tableHeaderBg = isLight ? '#f8fafc' : '#111827'
  const overlayBorderColor = isLight ? '#dbe4f0' : '#334155'
  const overlayBgColor = isLight ? '#ffffffd9' : '#0f172ad9'
  const emptyTextColor = isLight ? '#64748b' : '#94a3b8'
  const radarSplitAreaColors = isLight
    ? ['rgba(148, 163, 184, 0.12)', 'rgba(148, 163, 184, 0.2)']
    : ['rgba(30, 30, 46, 0.06)', 'rgba(30, 30, 46, 0.12)']
  const radarLineColor = isLight ? '#cbd5e1' : '#2a2a3d'

  const [drillOpen, setDrillOpen] = useState(false)
  const [drillMode, setDrillMode] = useState<DrillMode>('down')
  const [interactionMode, setInteractionMode] = useState<DrillMode>('down')
  const [drillContext, setDrillContext] = useState<DrillContext | null>(null)
  const [drillPath, setDrillPath] = useState<DrillStep[]>([])
  const isDashboardDrill = drillEnabled && drillScope === 'dashboard'
  const isDashboardDrillThrough = isDashboardDrill && dashboardDrillMode === 'through'
  const activePath = isDashboardDrill ? (dashboardDrillPath ?? []) : drillPath

  const resolveNumericField = useCallback((preferred?: string): string => {
    const hasNumericValues = (field: string): boolean =>
      rows.some((row) => toNumber(row[field]) !== null)

    if (preferred && columns.includes(preferred) && hasNumericValues(preferred)) return preferred
    const fallback = columns.find(hasNumericValues)
    return fallback ?? (preferred || '')
  }, [columns, rows])

  const resolveDimensionField = useCallback((exclude: string[] = []): string => {
    const excluded = new Set(exclude.filter(Boolean))
    const hasTextualValues = (field: string): boolean => rows.some((row) => {
      const value = row[field]
      if (value === null || value === undefined || String(value).trim() === '') return false
      return toNumber(value) === null
    })

    const textual = columns.find((field) => !excluded.has(field) && hasTextualValues(field))
    if (textual) return textual
    const fallback = columns.find((field) => !excluded.has(field))
    return fallback ?? columns[0] ?? ''
  }, [columns, rows])

  const preferredMeasureField = cfg.y_field || cfg.kpi_field || columns[1] || columns[0]
  const resolvedMeasureField = resolveNumericField(preferredMeasureField)

  const drillDimensionFields = useMemo(() => {
    const hasTextualValues = (field: string): boolean => rows.some((row) => {
      const value = row[field]
      if (value === null || value === undefined || String(value).trim() === '') return false
      return toNumber(value) === null
    })

    const preferred = cfg.x_field || cfg.group_by || resolveDimensionField([resolvedMeasureField])
    const textualFields = columns.filter((field) => field !== resolvedMeasureField && hasTextualValues(field))
    const fallbackFields = columns.filter((field) => field !== resolvedMeasureField)
    const ordered = [preferred, ...textualFields, ...fallbackFields, columns[0]]
      .filter((field): field is string => Boolean(field))

    return Array.from(new Set(ordered))
  }, [cfg.group_by, cfg.x_field, columns, resolveDimensionField, resolvedMeasureField, rows])

  const activeDrillDimension = useMemo(() => {
    if (drillDimensionFields.length === 0) return ''
    const index = Math.min(activePath.length, drillDimensionFields.length - 1)
    return drillDimensionFields[index] || ''
  }, [activePath.length, drillDimensionFields])

  const rowsInDrillPath = useMemo(() => {
    if (activePath.length === 0) return rows
    return rows.filter((row) => (
      activePath.every((step) => {
        const hasField = rows.some((r) => Object.prototype.hasOwnProperty.call(r, step.field))
        if (!hasField) return true
        return String(row[step.field] ?? '') === String(step.value ?? '')
      })
    ))
  }, [activePath, rows])

  useEffect(() => {
    if (isDashboardDrill) return
    setDrillPath([])
    setDrillContext(null)
    setDrillOpen(false)
  }, [
    isDashboardDrill,
    widget.id,
    widget.data_source.type,
    widget.data_source.dataset,
    widget.data_source.pipeline_id,
    widget.data_source.file_path,
    widget.data_source.sql,
    columns.join('|'),
  ])

  useEffect(() => {
    if (isDashboardDrill) return
    if (drillPath.length > 0 && rows.length > 0 && rowsInDrillPath.length === 0) {
      setDrillPath([])
    }
  }, [isDashboardDrill, drillPath.length, rows.length, rowsInDrillPath.length])

  const openDrill = useCallback((context: DrillContext, mode: DrillMode = 'down') => {
    if (!drillEnabled) return
    setDrillContext(context)
    setDrillMode(mode)
    setDrillOpen(true)
  }, [drillEnabled])

  const stepUpDrill = useCallback(() => {
    if (isDashboardDrill) return
    setDrillPath((prev) => prev.slice(0, -1))
  }, [isDashboardDrill])

  const resetDrill = useCallback(() => {
    if (isDashboardDrill) return
    setDrillPath([])
  }, [isDashboardDrill])

  const drillBreadcrumbLabel = useMemo(
    () => activePath.map((step) => `${step.field}: ${String(step.value)}`).join(' / '),
    [activePath]
  )

  const handleChartClick = useCallback((params: any) => {
    if (!drillEnabled) return

    const defaultMeasureField = resolveNumericField(cfg.y_field || cfg.kpi_field || columns[1] || columns[0])
    const defaultDimensionField = activeDrillDimension || cfg.x_field || cfg.group_by || resolveDimensionField([defaultMeasureField])

    let dimensionValue: unknown = params?.name
    let measureValue = toNumber(params?.value ?? params?.data?.value) ?? undefined
    let measureField = defaultMeasureField

    if (widget.type === 'scatter' && Array.isArray(params?.value)) {
      dimensionValue = params.value[0]
      measureValue = toNumber(params.value[1]) ?? undefined
      measureField = cfg.y_field || defaultMeasureField
    } else if (Array.isArray(params?.value)) {
      measureValue = toNumber(params.value[1] ?? params.value[0]) ?? undefined
    }

    const shouldDrillDown = isDashboardDrill ? dashboardDrillMode === 'down' : interactionMode === 'down'
    if (shouldDrillDown && defaultDimensionField && dimensionValue !== undefined && dimensionValue !== null) {
      if (isDashboardDrill) {
        const dimensionAtLevel = drillDimensionFields[Math.min(activePath.length, drillDimensionFields.length - 1)] || defaultDimensionField
        if (dimensionAtLevel && onDashboardDrill) {
          onDashboardDrill({ field: dimensionAtLevel, value: dimensionValue })
        }
        return
      }
      setDrillPath((prev) => {
        if (prev.length >= drillDimensionFields.length) return prev
        const dimensionAtLevel = drillDimensionFields[prev.length]
        if (!dimensionAtLevel) return prev
        return [...prev, { field: dimensionAtLevel, value: dimensionValue }]
      })
      return
    }

    openDrill({
      label: String(params?.seriesName || params?.name || widget.title || 'Selection'),
      dimensionField: defaultDimensionField,
      dimensionValue,
      measureField,
      measureValue,
    }, 'through')
  }, [activeDrillDimension, activePath.length, cfg.group_by, cfg.kpi_field, cfg.x_field, cfg.y_field, columns, dashboardDrillMode, drillDimensionFields, drillEnabled, interactionMode, isDashboardDrill, onDashboardDrill, openDrill, resolveDimensionField, resolveNumericField, widget.title, widget.type])

  const filteredDrillRows = useMemo(() => {
    if (!drillContext?.dimensionField) return rowsInDrillPath
    if (drillContext.dimensionValue === undefined || drillContext.dimensionValue === null) return rowsInDrillPath
    const field = drillContext.dimensionField
    const target = String(drillContext.dimensionValue)
    return rowsInDrillPath.filter((row) => String(row[field] ?? '') === target)
  }, [drillContext, rowsInDrillPath])

  const drillDownResult = useMemo(() => {
    const measureField = resolveNumericField(drillContext?.measureField || cfg.y_field || cfg.kpi_field || columns[1] || columns[0])
    const dimensionField = resolveDimensionField([drillContext?.dimensionField || activeDrillDimension || '', measureField])

    if (!measureField || !dimensionField) {
      return { measureField: '', dimensionField: '', rows: [] as Array<{ _key: string; dimension: string; value: number }> }
    }

    const grouped = new Map<string, number>()
    filteredDrillRows.forEach((row) => {
      const groupValue = String(row[dimensionField] ?? 'Unknown')
      const value = toNumber(row[measureField]) ?? 0
      grouped.set(groupValue, (grouped.get(groupValue) ?? 0) + value)
    })

    const summaryRows = Array.from(grouped.entries())
      .map(([dimension, value], idx) => ({
        _key: `${dimension}_${idx}`,
        dimension,
        value: Math.round(value * 100) / 100,
      }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 100)

    return { measureField, dimensionField, rows: summaryRows }
  }, [activeDrillDimension, cfg.kpi_field, cfg.y_field, columns, drillContext?.dimensionField, drillContext?.measureField, filteredDrillRows, resolveDimensionField, resolveNumericField])

  const drillThroughColumns = useMemo(() => (
    columns.slice(0, 12).map((column) => ({
      title: column,
      dataIndex: column,
      key: column,
      ellipsis: true,
      width: 150,
      render: (value: unknown) => (
        <span style={{ color: tableTextColor, fontSize: 12 }}>
          {value === null || value === undefined || String(value) === '' ? '-' : String(value)}
        </span>
      ),
    }))
  ), [columns, tableTextColor])

  const drillThroughRows = useMemo(() => (
    filteredDrillRows.slice(0, 200).map((row, index) => ({ ...row, _drillRowKey: `${index}` }))
  ), [filteredDrillRows])

  const baseAxis = useMemo(() => ({
    axisLine: { lineStyle: { color: gridLineColor } },
    axisTick: { lineStyle: { color: gridLineColor } },
    axisLabel: { color: baseTextColor, fontSize: 11 },
    splitLine: { lineStyle: { color: gridLineColor, type: 'dashed' as const } },
  }), [baseTextColor, gridLineColor])

  const tooltip = useMemo(() => (
    { backgroundColor: tooltipBg, borderColor: tooltipBorder, textStyle: { color: tooltipText } }
  ), [tooltipBg, tooltipBorder, tooltipText])

  const option = useMemo(() => {
    const sourceRows = rowsInDrillPath

    if (!sourceRows || sourceRows.length === 0) {
      return { title: { text: 'No data', left: 'center', top: 'middle', textStyle: { color: emptyTextColor, fontSize: 13 } } }
    }

    const configuredXField = cfg.x_field || ''
    const configuredYField = cfg.y_field || cfg.kpi_field || ''
    const xField = activeDrillDimension || configuredXField || columns[0] || ''
    const yField = configuredYField || resolvedMeasureField || columns[1] || ''
    const hasNumericForField = (field: string): boolean => (
      !!field && sourceRows.some((row) => toNumber(row[field]) !== null)
    )
    const numericYField = hasNumericForField(yField)
      ? yField
      : resolveNumericField(configuredYField || columns[1] || columns[0] || '')
    const groupBy = cfg.group_by || ''
    const agg = cfg.aggregation || 'sum'
    const limit = cfg.limit || 50
    const effectiveAgg = hasNumericForField(yField) || agg === 'count' ? agg : 'count'

    const aggregated = xField && yField
      ? aggregate(sourceRows, xField, yField, effectiveAgg).slice(0, limit)
      : []

    switch (widget.type) {
      case 'bar': {
        const sorted = cfg.sort_by ? [...aggregated].sort((a, b) =>
          cfg.sort_order === 'desc' ? b.value - a.value : a.value - b.value) : aggregated
        return {
          backgroundColor: bgColor,
          tooltip: { trigger: 'axis', ...tooltip },
          legend: showLegend ? { textStyle: { color: baseTextColor }, top: 0, backgroundColor: 'transparent' } : undefined,
          grid: showGrid ? { left: 60, right: 20, top: showLegend ? 40 : 16, bottom: 60, containLabel: true } : undefined,
          xAxis: { ...baseAxis, type: 'category', data: sorted.map(d => d.name), axisLabel: { ...baseAxis.axisLabel, rotate: sorted.length > 8 ? 30 : 0 } },
          yAxis: { ...baseAxis, type: 'value' },
          series: [{ type: 'bar', data: sorted.map((d, i) => ({ value: d.value, itemStyle: { color: palette[i % palette.length], borderRadius: [4, 4, 0, 0] } })), barMaxWidth: 60, emphasis: { itemStyle: { opacity: 0.8 } } }],
        }
      }

      case 'bar_horizontal': {
        const sorted = [...aggregated].sort((a, b) => a.value - b.value).slice(-20)
        return {
          backgroundColor: bgColor,
          tooltip: { trigger: 'axis', ...tooltip },
          grid: { left: 120, right: 30, top: 10, bottom: 30, containLabel: true },
          xAxis: { ...baseAxis, type: 'value' },
          yAxis: { ...baseAxis, type: 'category', data: sorted.map(d => d.name) },
          series: [{ type: 'bar', data: sorted.map((d, i) => ({ value: d.value, itemStyle: { color: palette[i % palette.length], borderRadius: [0, 4, 4, 0] } })), barMaxWidth: 32 }],
        }
      }

      case 'line': {
        const explicitYFields = cfg.y_fields?.length
          ? cfg.y_fields
          : (cfg.y_field ? [cfg.y_field] : [])
        const yFields = Array.from(new Set((explicitYFields.length > 0 ? explicitYFields : [numericYField])
          .map((field) => (hasNumericForField(field) ? field : resolveNumericField(field)))
          .filter(Boolean)))
        return {
          backgroundColor: bgColor,
          tooltip: { trigger: 'axis', ...tooltip },
          legend: showLegend && yFields.length > 1 ? { textStyle: { color: baseTextColor }, top: 0 } : undefined,
          grid: { left: 60, right: 20, top: showLegend && yFields.length > 1 ? 40 : 16, bottom: 50, containLabel: true },
          xAxis: { ...baseAxis, type: 'category', data: sourceRows.slice(0, limit).map(r => r[xField]) },
          yAxis: { ...baseAxis, type: 'value' },
          series: yFields.map((f, i) => ({
            name: f, type: 'line',
            data: sourceRows.slice(0, limit).map(r => toNumber(r[f]) ?? 0),
            smooth: true, symbol: 'circle', symbolSize: 4,
            lineStyle: { color: palette[i % palette.length], width: 2.5 },
            itemStyle: { color: palette[i % palette.length] },
          })),
        }
      }

      case 'area': {
        const explicitYFields = cfg.y_fields?.length
          ? cfg.y_fields
          : (cfg.y_field ? [cfg.y_field] : [])
        const yFields = Array.from(new Set((explicitYFields.length > 0 ? explicitYFields : [numericYField])
          .map((field) => (hasNumericForField(field) ? field : resolveNumericField(field)))
          .filter(Boolean)))
        return {
          backgroundColor: bgColor,
          tooltip: { trigger: 'axis', ...tooltip },
          legend: showLegend && yFields.length > 1 ? { textStyle: { color: baseTextColor }, top: 0 } : undefined,
          grid: { left: 60, right: 20, top: showLegend && yFields.length > 1 ? 40 : 16, bottom: 50, containLabel: true },
          xAxis: { ...baseAxis, type: 'category', data: sourceRows.slice(0, limit).map(r => r[xField]) },
          yAxis: { ...baseAxis, type: 'value' },
          series: yFields.map((f, i) => ({
            name: f, type: 'line',
            data: sourceRows.slice(0, limit).map(r => toNumber(r[f]) ?? 0),
            smooth: true, symbol: 'none',
            areaStyle: {
              color: {
                type: 'linear',
                x: 0,
                y: 0,
                x2: 0,
                y2: 1,
                colorStops: [
                  { offset: 0, color: withAlpha(palette[i % palette.length], 0.38) },
                  { offset: 1, color: withAlpha(palette[i % palette.length], 0.03) },
                ],
              },
            },
            lineStyle: { color: palette[i % palette.length], width: 2.5 },
          })),
        }
      }

      case 'pie': {
        const agg2 = xField && yField ? aggregate(sourceRows, xField, yField, agg).slice(0, 12) : []
        return {
          backgroundColor: bgColor,
          tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)', ...tooltip },
          legend: showLegend ? { orient: 'vertical', right: 10, textStyle: { color: baseTextColor }, top: 'center' } : undefined,
          series: [{ type: 'pie', radius: ['0%', '70%'], center: showLegend ? ['40%', '50%'] : ['50%', '50%'],
            data: agg2.map((d, i) => ({ ...d, itemStyle: { color: palette[i % palette.length] } })),
            label: { color: baseTextColor, fontSize: 11 }, emphasis: { itemStyle: { shadowBlur: 10 } } }],
        }
      }

      case 'donut': {
        const agg2 = xField && yField ? aggregate(sourceRows, xField, yField, agg).slice(0, 10) : []
        const total = agg2.reduce((s, d) => s + d.value, 0)
        return {
          backgroundColor: bgColor,
          tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)', ...tooltip },
          legend: showLegend ? { orient: 'vertical', right: 10, textStyle: { color: baseTextColor }, top: 'center' } : undefined,
          series: [{ type: 'pie', radius: ['45%', '70%'], center: showLegend ? ['40%', '50%'] : ['50%', '50%'],
            data: agg2.map((d, i) => ({ ...d, itemStyle: { color: palette[i % palette.length] } })),
            label: { show: false }, labelLine: { show: false },
            emphasis: { label: { show: true, fontSize: 14, fontWeight: 'bold', color: tooltipText } } }],
          graphic: [{ type: 'text', left: showLegend ? '36%' : '45%', top: '45%', style: { text: `${(total/1000).toFixed(1)}K`, textAlign: 'center', fill: tooltipText, fontSize: 18, fontWeight: 'bold' } }],
        }
      }

      case 'scatter': {
        const xF = cfg.x_field || columns[0]
        const yF = cfg.y_field || numericYField || columns[1]
        return {
          backgroundColor: bgColor,
          tooltip: { trigger: 'item', formatter: (p: any) => `${xF}: ${p.data[0]}<br/>${yF}: ${p.data[1]}`, ...tooltip },
          grid: { left: 60, right: 20, top: 20, bottom: 50, containLabel: true },
          xAxis: { ...baseAxis, type: 'value', name: xF },
          yAxis: { ...baseAxis, type: 'value', name: yF },
          series: [{ type: 'scatter', data: sourceRows.slice(0, 500).map(r => [toNumber(r[xF]) ?? 0, toNumber(r[yF]) ?? 0]),
            symbolSize: 8, itemStyle: { color: palette[0], opacity: 0.7 } }],
        }
      }

      case 'heatmap': {
        const xF = cfg.x_field || columns[0]
        const yF = cfg.group_by || columns[1] || columns[0]
        const vF = cfg.y_field || numericYField || columns[2] || columns[1]
        const xVals = [...new Set(sourceRows.map(r => r[xF]))].slice(0, 20)
        const yVals = [...new Set(sourceRows.map(r => r[yF]))].slice(0, 15)
        const heatData = sourceRows.slice(0, 500).map(r => [xVals.indexOf(r[xF]), yVals.indexOf(r[yF]), toNumber(r[vF]) ?? 0]).filter(d => d[0] >= 0 && d[1] >= 0)
        const maxVal = Math.max(...heatData.map(d => d[2] as number))
        return {
          backgroundColor: bgColor,
          tooltip: { position: 'top', ...tooltip },
          grid: { left: 80, right: 20, top: 10, bottom: 60, containLabel: true },
          xAxis: { ...baseAxis, type: 'category', data: xVals, splitArea: { show: true } },
          yAxis: { ...baseAxis, type: 'category', data: yVals, splitArea: { show: true } },
          visualMap: { min: 0, max: maxVal, calculable: true, orient: 'horizontal', left: 'center', bottom: 0, inRange: { color: [withAlpha(palette[0], 0.2), palette[0]] }, textStyle: { color: baseTextColor } },
          series: [{ type: 'heatmap', data: heatData, label: { show: false } }],
        }
      }

      case 'treemap': {
        const agg2 = xField && yField ? aggregate(sourceRows, xField, yField, agg).slice(0, 30) : []
        return {
          backgroundColor: bgColor,
          tooltip: { formatter: '{b}: {c}', ...tooltip },
          series: [{ type: 'treemap', data: agg2.map((d, i) => ({ ...d, itemStyle: { color: palette[i % palette.length] } })),
            label: { show: true, formatter: '{b}\n{c}', color: '#fff', fontSize: 11 },
            breadcrumb: { show: false } }],
        }
      }

      case 'funnel': {
        const agg2 = xField && yField ? aggregate(sourceRows, xField, yField, agg).slice(0, 8) : []
        const sorted2 = [...agg2].sort((a, b) => b.value - a.value)
        return {
          backgroundColor: bgColor,
          tooltip: { trigger: 'item', formatter: '{b}: {c}', ...tooltip },
          legend: showLegend ? { textStyle: { color: baseTextColor }, top: 0, backgroundColor: 'transparent' } : undefined,
          series: [{ type: 'funnel', left: '10%', width: '80%', label: { position: 'inside', color: '#fff', fontSize: 11 },
            data: sorted2.map((d, i) => ({ ...d, itemStyle: { color: palette[i % palette.length] } })) }],
        }
      }

      case 'gauge': {
        const vField = cfg.kpi_field || cfg.y_field || numericYField || columns[1]
        const val = sourceRows.length > 0 ? (toNumber(sourceRows[0][vField]) ?? 0) : 0
        const maxVal = cfg.limit || 100
        return {
          backgroundColor: bgColor,
          series: [{
            type: 'gauge', startAngle: 210, endAngle: -30,
            min: 0, max: maxVal,
            axisLine: { lineStyle: { width: 18, color: [[0.3, '#ef4444'], [0.7, '#eab308'], [1, '#22c55e']] } },
            pointer: { itemStyle: { color: palette[0] } },
            axisTick: { show: false }, splitLine: { show: false }, axisLabel: { color: baseTextColor, fontSize: 10 },
            detail: { valueAnimation: true, formatter: '{value}', color: tooltipText, fontSize: 24, fontWeight: 'bold', offsetCenter: [0, '70%'] },
            title: { color: baseTextColor, fontSize: 12, offsetCenter: [0, '95%'] },
            data: [{ value: Math.round(val), name: cfg.kpi_label || vField }],
          }],
        }
      }

      case 'radar': {
        const dims = columns.filter(c => {
          const sample = sourceRows.slice(0, 10).map(r => toNumber(r[c]))
          return sample.length > 0 && sample.every(v => v !== null)
        }).slice(0, 8)
        const groups = groupBy ? [...new Set(sourceRows.map(r => r[groupBy]))].slice(0, 5) : ['All']
        const indicators = dims.map(d => {
          const vals = sourceRows.map(r => toNumber(r[d]) ?? 0)
          return { name: d, max: Math.max(...vals) * 1.1 }
        })
        return {
          backgroundColor: bgColor,
          tooltip: { ...tooltip },
          legend: showLegend && groups.length > 1 ? { textStyle: { color: baseTextColor }, top: 0 } : undefined,
          radar: {
            indicator: indicators,
            shape: 'circle',
            splitArea: { areaStyle: { color: radarSplitAreaColors } },
            axisLine: { lineStyle: { color: radarLineColor } },
            splitLine: { lineStyle: { color: radarLineColor } },
            name: { textStyle: { color: baseTextColor, fontSize: 11 } },
          },
          series: [{
            type: 'radar',
            data: groups.map((g, i) => ({
              name: g,
              value: dims.map(d => {
                const scoped = groupBy ? sourceRows.filter(r => r[groupBy] === g) : sourceRows
                const vals = scoped.map(r => toNumber(r[d]) ?? 0)
                return Math.round(vals.reduce((a, b) => a + b, 0) / Math.max(vals.length, 1) * 10) / 10
              }),
              lineStyle: { color: palette[i % palette.length] },
              areaStyle: { color: withAlpha(palette[i % palette.length], 0.2) },
              itemStyle: { color: palette[i % palette.length] },
            })),
          }],
        }
      }

      case 'waterfall': {
        const sorted3 = [...aggregated].slice(0, 12)
        let running = 0
        const baseData: number[] = []
        const barData: number[] = []
        sorted3.forEach(d => {
          baseData.push(running)
          barData.push(d.value)
          running += d.value
        })
        return {
          backgroundColor: bgColor,
          tooltip: { trigger: 'axis', ...tooltip },
          grid: { left: 60, right: 20, top: 16, bottom: 60, containLabel: true },
          xAxis: { ...baseAxis, type: 'category', data: sorted3.map(d => d.name) },
          yAxis: { ...baseAxis, type: 'value' },
          series: [
            { type: 'bar', stack: 'total', itemStyle: { borderColor: 'transparent', color: 'transparent' }, emphasis: { itemStyle: { borderColor: 'transparent', color: 'transparent' } }, data: baseData },
            { type: 'bar', stack: 'total', data: barData.map((v, i) => ({ value: v, itemStyle: { color: v >= 0 ? palette[2] : palette[4], borderRadius: v >= 0 ? [4, 4, 0, 0] : [0, 0, 4, 4] } })), barMaxWidth: 60 },
          ],
        }
      }

      default:
        return { title: { text: `${widget.type} chart`, left: 'center', top: 'middle', textStyle: { color: emptyTextColor } } }
    }
  }, [activeDrillDimension, rowsInDrillPath, resolvedMeasureField, resolveNumericField, widget, columns, bgColor, tooltip, baseAxis, baseTextColor, showLegend, showGrid, palette, cfg, emptyTextColor, tooltipText, radarLineColor, radarSplitAreaColors])

  const stableOption = useMemo(() => ({
    animation: false,
    animationDuration: 0,
    animationDurationUpdate: 0,
    animationEasing: 'linear',
    animationEasingUpdate: 'linear',
    ...option,
  }), [option])

  const showWidgetDrillUI = drillEnabled && !isDashboardDrill
  const allowDrillModal = drillEnabled && (!isDashboardDrill || isDashboardDrillThrough)

  const drillModal = allowDrillModal ? (
    <Modal
      open={drillOpen}
      title={`Drill Explorer - ${widget.title}`}
      onCancel={() => setDrillOpen(false)}
      footer={null}
      width={980}
      destroyOnClose
    >
      <div style={{ marginBottom: 12, display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
        <Segmented
          value={drillMode}
          options={[
            { label: 'Drill Down', value: 'down' },
            { label: 'Drill Through', value: 'through' },
          ]}
          onChange={(value) => setDrillMode(value as DrillMode)}
        />
        <Text style={{ color: baseTextColor, fontSize: 12 }}>
          {drillContext?.dimensionField && drillContext.dimensionValue !== undefined
            ? `${drillContext.dimensionField}: ${String(drillContext.dimensionValue)}`
            : 'All rows'}
        </Text>
      </div>

      {drillMode === 'down' ? (
        drillDownResult.rows.length > 0 ? (
          <Table
            dataSource={drillDownResult.rows}
            rowKey="_key"
            size="small"
            pagination={{ pageSize: 10, hideOnSinglePage: true }}
            columns={[
              {
                title: drillDownResult.dimensionField || 'Dimension',
                dataIndex: 'dimension',
                key: 'dimension',
                ellipsis: true,
                render: (value: string) => <span style={{ color: tableTextColor }}>{value}</span>,
              },
              {
                title: drillDownResult.measureField || 'Value',
                dataIndex: 'value',
                key: 'value',
                width: 220,
                render: (value: number) => (
                  <span style={{ color: tableTextColor }}>
                    {Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 })}
                  </span>
                ),
              },
            ]}
          />
        ) : (
          <Empty description="No drill-down data available for this selection." />
        )
      ) : (
        drillThroughRows.length > 0 ? (
          <Table
            dataSource={drillThroughRows}
            columns={drillThroughColumns}
            rowKey="_drillRowKey"
            size="small"
            pagination={{ pageSize: 10, hideOnSinglePage: true }}
            scroll={{ x: true, y: 420 }}
          />
        ) : (
          <Empty description="No drill-through rows available for this selection." />
        )
      )}
    </Modal>
  ) : null

  const drillOverlayControls = showWidgetDrillUI ? (
    <div
      style={{
        position: 'absolute',
        top: 8,
        right: 8,
        zIndex: 6,
        display: 'flex',
        alignItems: 'center',
        gap: 6,
        padding: '4px 6px',
        borderRadius: 8,
        border: `1px solid ${overlayBorderColor}`,
        background: overlayBgColor,
        backdropFilter: 'blur(3px)',
      }}
    >
      <Segmented
        size="small"
        value={interactionMode}
        options={[
          { label: 'Down', value: 'down' },
          { label: 'Through', value: 'through' },
        ]}
        onChange={(value) => setInteractionMode(value as DrillMode)}
      />
      <Button size="small" type="text" onClick={stepUpDrill} disabled={activePath.length === 0}>
        Up
      </Button>
      <Button size="small" type="text" onClick={resetDrill} disabled={activePath.length === 0}>
        Reset
      </Button>
    </div>
  ) : null

  const drillBreadcrumb = showWidgetDrillUI && activePath.length > 0 ? (
    <div
      style={{
        position: 'absolute',
        top: 8,
        left: 8,
        zIndex: 6,
        maxWidth: '70%',
        whiteSpace: 'nowrap',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        padding: '3px 8px',
        borderRadius: 999,
        border: `1px solid ${overlayBorderColor}`,
        background: overlayBgColor,
        color: isLight ? '#64748b' : '#cbd5e1',
        fontSize: 11,
        lineHeight: 1.4,
      }}
      title={drillBreadcrumbLabel}
    >
      {drillBreadcrumbLabel}
    </div>
  ) : null

  // KPI Widget — special rendering
  if (widget.type === 'kpi') {
    const vField = resolveNumericField(cfg.kpi_field || cfg.y_field || columns[1] || columns[0]) || columns[0] || ''
    const values = rowsInDrillPath
      .map((row) => toNumber(row[vField]))
      .filter((value): value is number => value !== null)
    const current = values.length > 0 ? values[values.length - 1] : 0
    const previous = values.length > 1 ? values[values.length - 2] : current
    const pct = previous !== 0 ? ((current - previous) / previous) * 100 : 0
    const up = pct >= 0
    const kpiBgIsLight = isLightColor(bgColor) ?? isLight
    const kpiLabelColor = kpiBgIsLight ? '#64748b' : '#94a3b8'
    const kpiTextColor = kpiBgIsLight ? '#1e293b' : '#e2e8f0'
    const safeHeight = Math.max(64, height)
    const kpiValueSize = Math.max(20, Math.min(38, Math.round(safeHeight * 0.34)))
    const hasKpiData = values.length > 0
    const kpiDisplayValue = hasKpiData ? current : 0

    return (
      <>
        <div
          style={{
            height,
            position: 'relative',
            display: 'flex',
            backgroundColor: bgColor === 'transparent' ? undefined : bgColor,
            borderRadius: 8,
          }}
        >
          {drillOverlayControls}
          {drillBreadcrumb}
          <div
            onClick={() => {
              if (isDashboardDrill) {
                if (dashboardDrillMode === 'down') {
                  const dimensionField = activeDrillDimension || resolveDimensionField([vField])
                  if (dimensionField && onDashboardDrill && rowsInDrillPath.length > 0) {
                    const row = rowsInDrillPath[rowsInDrillPath.length - 1]
                    const dimensionValue = row?.[dimensionField]
                    if (dimensionValue !== undefined && dimensionValue !== null) {
                      onDashboardDrill({ field: dimensionField, value: dimensionValue })
                    }
                  }
                  return
                }

                openDrill({
                  label: cfg.kpi_label || vField || 'KPI',
                  measureField: vField || undefined,
                  measureValue: kpiDisplayValue,
                }, 'through')
                return
              }

              if (interactionMode === 'down') {
                openDrill({
                  label: cfg.kpi_label || vField || 'KPI',
                  dimensionField: activeDrillDimension || undefined,
                  measureField: vField || undefined,
                  measureValue: kpiDisplayValue,
                }, 'down')
                return
              }
              openDrill({
                label: cfg.kpi_label || vField || 'KPI',
                measureField: vField || undefined,
                measureValue: kpiDisplayValue,
              }, 'through')
            }}
            style={{
              height: '100%',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              gap: 4,
              padding: 12,
              cursor: drillEnabled ? 'pointer' : 'default',
            }}
          >
            <Text style={{ color: kpiLabelColor, fontSize: 12 }}>{cfg.kpi_label || vField || 'KPI Value'}</Text>
            <div style={{ fontSize: kpiValueSize, fontWeight: 800, color: kpiTextColor, letterSpacing: '-1px', lineHeight: 1 }}>
              {kpiDisplayValue > 10000
                ? `${(kpiDisplayValue / 1000).toFixed(1)}K`
                : kpiDisplayValue.toLocaleString(undefined, { maximumFractionDigits: 2 })}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 13, color: up ? '#22c55e' : '#ef4444' }}>
              {up ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
              {Math.abs(pct).toFixed(1)}% vs prev
            </div>
          </div>
        </div>
        {drillModal}
      </>
    )
  }

  // Text widget
  if (widget.type === 'text') {
    const fallbackDimensionField = resolveDimensionField()
    const fallbackMeasureField = resolveNumericField(cfg.y_field || cfg.kpi_field || columns[1] || '')
    return (
      <>
        <div
          onClick={() => {
            if (isDashboardDrill) {
              if (dashboardDrillMode === 'down') {
                const dimensionField = activeDrillDimension || fallbackDimensionField
                if (dimensionField && onDashboardDrill && rowsInDrillPath.length > 0) {
                  const dimensionValue = rowsInDrillPath[rowsInDrillPath.length - 1]?.[dimensionField]
                  if (dimensionValue !== undefined && dimensionValue !== null) {
                    onDashboardDrill({ field: dimensionField, value: dimensionValue })
                  }
                }
                return
              }

              openDrill({
                label: widget.title || 'Text widget',
                dimensionField: fallbackDimensionField || undefined,
                measureField: fallbackMeasureField || undefined,
              }, 'through')
              return
            }
            openDrill({
              label: widget.title || 'Text widget',
              dimensionField: fallbackDimensionField || undefined,
              measureField: fallbackMeasureField || undefined,
            }, interactionMode === 'down' ? 'down' : 'through')
          }}
          style={{
            height,
            position: 'relative',
          }}
        >
          {drillOverlayControls}
          {drillBreadcrumb}
          <div style={{ height: '100%', padding: 16, overflow: 'auto', cursor: drillEnabled && rowsInDrillPath.length > 0 ? 'pointer' : 'default' }}>
            <Text style={{ color: isLight ? '#1e293b' : '#e2e8f0', fontSize: 14, whiteSpace: 'pre-wrap' }}>
              {cfg.text_content || 'Edit this text widget...'}
            </Text>
          </div>
        </div>
        {drillModal}
      </>
    )
  }

  // Table widget
  if (widget.type === 'table') {
    const cols = columns.slice(0, 8).map(c => ({
      title: c, dataIndex: c, key: c, ellipsis: true, width: 120,
      render: (v: unknown) => (
        <span style={{ color: tableTextColor, fontSize: 12 }}>
          {v === null || v === undefined || String(v) === ''
            ? '-'
            : typeof v === 'object'
              ? JSON.stringify(v)
              : String(v)}
        </span>
      )
    }))
    const tableRows = rowsInDrillPath.slice(0, 100).map((row, idx) => ({ ...row, _rowKey: String(idx) }))
    const primaryField = columns[0]
    return (
      <>
        <div style={{ height, position: 'relative' }}>
          {drillOverlayControls}
          {drillBreadcrumb}
          <Table
            dataSource={tableRows}
            columns={cols}
            rowKey="_rowKey"
            size="small"
            scroll={{ x: true, y: height - 50 }}
            pagination={false}
            style={{ height, background: tableHeaderBg }}
            className="dark-table"
            onRow={(record: Record<string, unknown>) => (
              drillEnabled && primaryField
                ? {
                    onClick: () => {
                      if (isDashboardDrill) {
                        if (dashboardDrillMode === 'down') {
                          const dimensionAtLevel = drillDimensionFields[Math.min(activePath.length, drillDimensionFields.length - 1)] || primaryField
                          if (dimensionAtLevel && onDashboardDrill) {
                            const nextValue = record[dimensionAtLevel] ?? record[primaryField]
                            if (nextValue !== undefined && nextValue !== null) {
                              onDashboardDrill({ field: dimensionAtLevel, value: nextValue })
                            }
                          }
                          return
                        }

                        openDrill({
                          label: `${primaryField}: ${String(record[primaryField] ?? '-')}`,
                          dimensionField: primaryField,
                          dimensionValue: record[primaryField],
                          measureField: resolveNumericField(cfg.y_field || cfg.kpi_field || columns[1] || ''),
                        }, 'through')
                        return
                      }

                      if (interactionMode === 'down') {
                        setDrillPath((prev) => {
                          const dimensionAtLevel = drillDimensionFields[prev.length] || primaryField
                          if (!dimensionAtLevel) return prev
                          const nextValue = record[dimensionAtLevel] ?? record[primaryField]
                          if (nextValue === undefined || nextValue === null) return prev
                          if (prev.length >= drillDimensionFields.length && dimensionAtLevel !== primaryField) return prev
                          return [...prev, { field: dimensionAtLevel, value: nextValue }]
                        })
                        return
                      }
                      openDrill({
                        label: `${primaryField}: ${String(record[primaryField] ?? '-')}`,
                        dimensionField: primaryField,
                        dimensionValue: record[primaryField],
                        measureField: resolveNumericField(cfg.y_field || cfg.kpi_field || columns[1] || ''),
                      }, 'through')
                    },
                    style: { cursor: 'pointer' },
                  }
                : {}
            )}
          />
        </div>
        {drillModal}
      </>
    )
  }

  return (
    <>
      <div style={{ height, position: 'relative' }}>
        {drillOverlayControls}
        {drillBreadcrumb}
        <ReactECharts
          option={stableOption}
          style={{ height, width: '100%' }}
          notMerge={false}
          lazyUpdate={true}
          opts={{ renderer: 'canvas' }}
          onEvents={drillEnabled ? { click: handleChartClick } : undefined}
        />
      </div>
      {drillModal}
    </>
  )
}

export default memo(ChartWidget)
