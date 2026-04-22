import { useEffect, useCallback, useState, useMemo, useRef, memo } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  Button,
  Tooltip,
  Tag,
  Skeleton,
  Typography,
  message,
  Dropdown,
  Space,
  Segmented,
  Modal,
  Input,
} from 'antd'
import type { MenuProps } from 'antd'
import {
  EditOutlined,
  ShareAltOutlined,
  CameraOutlined,
  FilterOutlined,
  MoreOutlined,
  ReloadOutlined,
  ArrowLeftOutlined,
  DashboardOutlined,
  ClockCircleOutlined,
  DownloadOutlined,
} from '@ant-design/icons'
import html2canvas from 'html2canvas'
import { ResponsiveGridLayout, useContainerWidth } from 'react-grid-layout'
import 'react-grid-layout/css/styles.css'
import 'react-resizable/css/styles.css'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import { useVizStore } from '../../store/vizStore'
import type { Widget, WidgetDataResult } from '../../store/vizStore'
import ChartWidget from '../../components/viz/ChartWidget'
import api from '../../api/client'

dayjs.extend(relativeTime)

const { Text, Title } = Typography
const ResponsiveGrid = ResponsiveGridLayout as unknown as React.ComponentType<any>

// ─── Constants ────────────────────────────────────────────────────────────────

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

interface DashboardDrillStep {
  field: string
  value: unknown
}

type DashboardDrillMode = 'down' | 'through'

interface ShareLinks {
  shareUrl: string
  embedUrl: string
  iframeCode: string
}

const VIEW_ROW_HEIGHT = 30
const VIEW_MARGIN_Y = 12
const VIEW_HEADER_HEIGHT = 40
const PYTHON_DASHBOARD_TAG = '__python_analytics__'

type CsvRow = Record<string, unknown>

function toCsvCell(value: unknown): string {
  if (value === null || value === undefined) return ''
  const text = typeof value === 'string'
    ? value
    : (typeof value === 'number' || typeof value === 'boolean')
      ? String(value)
      : JSON.stringify(value)
  const escaped = text.replace(/"/g, '""')
  return `"${escaped}"`
}

function downloadRowsAsCsv(rows: CsvRow[], baseName: string): void {
  if (!Array.isArray(rows) || rows.length === 0) return
  const headers = Array.from(new Set(rows.flatMap((row) => Object.keys(row || {}))))
  const lines: string[] = [headers.map((h) => toCsvCell(h)).join(',')]
  rows.forEach((row) => {
    lines.push(headers.map((h) => toCsvCell((row || {})[h])).join(','))
  })
  const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
  link.href = url
  link.download = `${baseName}-${timestamp}.csv`
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
  URL.revokeObjectURL(url)
}

function downloadFieldListAsCsv(columns: string[], baseName: string): void {
  const rows = (Array.isArray(columns) ? columns : [])
    .map((name, index) => ({ output_field: String(name || '').trim(), position: index + 1 }))
    .filter((row) => row.output_field)
  if (rows.length === 0) return
  downloadRowsAsCsv(rows, baseName)
}

function slugifyName(value: string): string {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function waitForCanvasPaint(root: HTMLElement, timeoutMs = 4000): Promise<void> {
  const startedAt = Date.now()
  while (Date.now() - startedAt < timeoutMs) {
    const canvases = Array.from(root.querySelectorAll('canvas')) as HTMLCanvasElement[]
    const visible = canvases.filter((canvas) => canvas.width > 0 && canvas.height > 0)
    if (visible.length === 0) return

    let readyCount = 0
    for (const canvas of visible) {
      try {
        const dataUrl = canvas.toDataURL('image/png')
        if (typeof dataUrl === 'string' && dataUrl.length > 2500) readyCount += 1
      } catch {
        // Some canvases may not allow readback; assume rendered and continue.
        readyCount += 1
      }
    }

    if (readyCount >= visible.length) return
    await delay(120)
  }
}

function computeWidgetBodyHeight(gridH: number, type: string): number {
  const totalCardHeight = gridH * VIEW_ROW_HEIGHT + (gridH - 1) * VIEW_MARGIN_Y
  const bodyPadding = type === 'table' ? 0 : 8
  return Math.max(60, totalCardHeight - VIEW_HEADER_HEIGHT - 2 - bodyPadding)
}

// ─── Widget Title Bar ─────────────────────────────────────────────────────────

interface WidgetTitleBarProps {
  title: string
  subtitle?: string
  widgetId: string
  onRefresh: (widgetId: string) => void
  onDownloadData: (widgetId: string) => void
  onDownloadFields: (widgetId: string) => void
  canDownloadData: boolean
  canDownloadFields: boolean
  theme?: string
}

function WidgetTitleBar({
  title,
  subtitle,
  widgetId,
  onRefresh,
  onDownloadData,
  onDownloadFields,
  canDownloadData,
  canDownloadFields,
  theme = 'dark',
}: WidgetTitleBarProps) {
  const isLight = theme === 'light'
  const headerBg = isLight ? '#f8fafc' : 'var(--app-panel-bg)'
  const headerBorder = isLight ? '#dbe4f0' : 'var(--app-border)'
  const titleColor = isLight ? '#0f172a' : 'var(--app-text)'
  const subtitleColor = isLight ? 'var(--app-text-subtle)' : 'var(--app-text-dim)'
  const menuColor = isLight ? 'var(--app-text-subtle)' : 'var(--app-text-dim)'

  const menuItems: MenuProps['items'] = [
    {
      key: 'refresh',
      icon: <ReloadOutlined />,
      label: 'Refresh data',
    },
    {
      key: 'download_data',
      icon: <DownloadOutlined />,
      label: 'Download Output CSV',
      disabled: !canDownloadData,
    },
    {
      key: 'download_fields',
      icon: <DownloadOutlined />,
      label: 'Download Output Fields CSV',
      disabled: !canDownloadFields,
    },
  ]

  const handleMenuClick: MenuProps['onClick'] = ({ key }) => {
    if (key === 'refresh') {
      onRefresh(widgetId)
      return
    }
    if (key === 'download_data') {
      onDownloadData(widgetId)
      return
    }
    if (key === 'download_fields') {
      onDownloadFields(widgetId)
    }
  }

  return (
    <div style={{
      background: headerBg,
      borderBottom: `1px solid ${headerBorder}`,
      padding: '8px 12px',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      gap: 8,
      minHeight: 40,
      flexShrink: 0,
    }}>
      <div style={{ flex: 1, minWidth: 0 }}>
        <Text
          strong
          ellipsis
          style={{ color: titleColor, fontSize: 13, display: 'block', lineHeight: '1.3' }}
        >
          {title}
        </Text>
        {subtitle && (
          <Text
            ellipsis
            style={{ color: subtitleColor, fontSize: 11, display: 'block', lineHeight: '1.3' }}
          >
            {subtitle}
          </Text>
        )}
      </div>
      <Dropdown menu={{ items: menuItems, onClick: handleMenuClick }} trigger={['click']} placement="bottomRight">
        <Button
          type="text"
          size="small"
          icon={<MoreOutlined />}
          style={{ color: menuColor, width: 24, height: 24, padding: 0, minWidth: 'unset', flexShrink: 0 }}
        />
      </Dropdown>
    </div>
  )
}

// ─── Widget Cell ──────────────────────────────────────────────────────────────

interface WidgetCellProps {
  widget: Widget
  dataEntry?: WidgetDataResult
  onRefresh: (widgetId: string) => void
  onDownloadData: (widgetId: string) => void
  onDownloadFields: (widgetId: string) => void
  theme?: string
  chartHeight?: number
  dashboardDrillMode?: DashboardDrillMode
  dashboardDrillPath?: DashboardDrillStep[]
  onDashboardDrill?: (step: DashboardDrillStep) => void
}

const WidgetCell = memo(function WidgetCell({
  widget,
  dataEntry,
  onRefresh,
  onDownloadData,
  onDownloadFields,
  theme = 'dark',
  chartHeight = 300,
  dashboardDrillMode = 'down',
  dashboardDrillPath = [],
  onDashboardDrill,
}: WidgetCellProps) {
  const isLight = theme === 'light'
  const data = dataEntry?.data ?? []
  const columns = dataEntry?.columns ?? []
  const cardBg = isLight ? '#ffffff' : 'var(--app-panel-bg)'
  const cardBorder = isLight ? '#dbe4f0' : 'var(--app-border)'

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
      <WidgetTitleBar
        title={widget.title}
        subtitle={widget.subtitle}
        widgetId={widget.id}
        onRefresh={onRefresh}
        onDownloadData={onDownloadData}
        onDownloadFields={onDownloadFields}
        canDownloadData={Array.isArray(data) && data.length > 0}
        canDownloadFields={Array.isArray(columns) && columns.length > 0}
        theme={theme}
      />
      <div style={{ flex: 1, minHeight: 0, padding: widget.type === 'table' ? 0 : 4 }}>
        {!dataEntry ? (
          <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Skeleton active paragraph={{ rows: 3 }} style={{ padding: '16px 20px' }} />
          </div>
        ) : (
          <ChartWidget
            widget={widget}
            data={data}
            columns={columns}
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
})

// ─── Global Filter Bar ────────────────────────────────────────────────────────

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
      <Button size="small" onClick={onStepUp} disabled={!path.length} style={{ borderColor: 'var(--app-text-faint)', color: actionColor, background: 'transparent' }}>
        Up
      </Button>
      <Button size="small" onClick={onReset} disabled={!path.length} style={{ borderColor: 'var(--app-text-faint)', color: actionColor, background: 'transparent' }}>
        Reset
      </Button>
    </div>
  )
}

// ─── Loading Skeleton ─────────────────────────────────────────────────────────

function ViewerSkeleton() {
  return (
    <div style={{ height: '100vh', background: 'var(--app-shell-bg)', display: 'flex', flexDirection: 'column' }}>
      {/* Top bar skeleton */}
      <div style={{
        background: '#0a0a12',
        borderBottom: '1px solid var(--app-border)',
        padding: '0 24px',
        height: 56,
        display: 'flex',
        alignItems: 'center',
        gap: 16,
      }}>
        <Skeleton.Button active style={{ width: 32, height: 32 }} />
        <Skeleton.Input active style={{ width: 200, height: 20 }} />
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
          <Skeleton.Button active style={{ width: 80, height: 32 }} />
          <Skeleton.Button active style={{ width: 80, height: 32 }} />
        </div>
      </div>
      {/* Grid skeleton */}
      <div style={{ flex: 1, padding: 24, display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 16 }}>
        {[...Array(6)].map((_, i) => (
          <div
            key={i}
            style={{
              background: 'var(--app-panel-bg)',
              border: '1px solid var(--app-border)',
              borderRadius: 10,
              padding: '12px 16px',
              height: i % 3 === 0 ? 200 : 280,
            }}
          >
            <Skeleton active paragraph={{ rows: 4 }} />
          </div>
        ))}
      </div>
    </div>
  )
}

// ─── Top Bar ──────────────────────────────────────────────────────────────────

interface TopBarProps {
  name: string
  updatedAt?: string
  onEdit: () => void
  onShare: () => void
  onSnapshot: () => void
  snapshotLoading?: boolean
  onBack: () => void
  theme?: string
}

function TopBar({ name, updatedAt, onEdit, onShare, onSnapshot, snapshotLoading = false, onBack, theme = 'dark' }: TopBarProps) {
  const updatedAgo = updatedAt ? dayjs(updatedAt).fromNow() : null
  const isLight = theme === 'light'

  return (
    <div style={{
      background: isLight ? '#f8fafc' : '#0a0a12',
      borderBottom: `1px solid ${isLight ? '#dbe4f0' : 'var(--app-border)'}`,
      padding: '0 20px',
      height: 56,
      display: 'flex',
      alignItems: 'center',
      gap: 12,
      flexShrink: 0,
      zIndex: 100,
    }}>
      {/* Back button */}
      <Tooltip title="Back to dashboards">
        <Button
          type="text"
          icon={<ArrowLeftOutlined />}
          onClick={onBack}
          style={{ color: 'var(--app-text-subtle)', height: 36, width: 36, padding: 0, borderRadius: 8, flexShrink: 0 }}
        />
      </Tooltip>

      {/* Divider */}
      <div style={{ width: 1, height: 20, background: 'var(--app-border)', flexShrink: 0 }} />

      {/* Dashboard icon + name */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, flex: 1, minWidth: 0 }}>
        <div style={{
          width: 28,
          height: 28,
          borderRadius: 7,
          background: 'linear-gradient(135deg, #6366f1, #8b5cf6)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          flexShrink: 0,
        }}>
          <DashboardOutlined style={{ color: '#fff', fontSize: 14 }} />
        </div>
        <Title
          level={5}
          ellipsis
          style={{ color: 'var(--app-text)', margin: 0, fontWeight: 600, fontSize: 15, flex: 1, minWidth: 0 }}
        >
          {name}
        </Title>
        {updatedAgo && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexShrink: 0, marginLeft: 4 }}>
            <ClockCircleOutlined style={{ color: 'var(--app-text-dim)', fontSize: 11 }} />
            <Text style={{ color: 'var(--app-text-dim)', fontSize: 12, whiteSpace: 'nowrap' }}>
              Updated {updatedAgo}
            </Text>
          </div>
        )}
      </div>

      {/* Action buttons */}
      <Space size={8} style={{ flexShrink: 0 }}>
        <Tooltip title="Capture dashboard snapshot for email body">
          <Button
            icon={<CameraOutlined />}
            onClick={onSnapshot}
            loading={snapshotLoading}
            style={{
              background: 'transparent',
              border: '1px solid var(--app-border-strong)',
              color: 'var(--app-text-muted)',
              borderRadius: 8,
              height: 34,
              fontSize: 13,
            }}
          >
            Snapshot
          </Button>
        </Tooltip>
        <Tooltip title="Share dashboard">
          <Button
            icon={<ShareAltOutlined />}
            onClick={onShare}
            style={{
              background: 'transparent',
              border: '1px solid var(--app-border-strong)',
              color: 'var(--app-text-muted)',
              borderRadius: 8,
              height: 34,
              fontSize: 13,
            }}
          >
            Share
          </Button>
        </Tooltip>
        <Button
          type="primary"
          icon={<EditOutlined />}
          onClick={onEdit}
          style={{
            background: '#6366f1',
            border: 'none',
            borderRadius: 8,
            height: 34,
            fontWeight: 600,
            fontSize: 13,
            paddingLeft: 14,
            paddingRight: 14,
          }}
        >
          Edit
        </Button>
      </Space>
    </div>
  )
}

// ─── Main Component ───────────────────────────────────────────────────────────

export default function DashboardViewer() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const [messageApi, contextHolder] = message.useMessage()
  const [dashboardDrillMode, setDashboardDrillMode] = useState<DashboardDrillMode>('down')
  const [dashboardDrillPath, setDashboardDrillPath] = useState<DashboardDrillStep[]>([])
  const [shareModalOpen, setShareModalOpen] = useState(false)
  const [shareLinks, setShareLinks] = useState<ShareLinks | null>(null)
  const [snapshotSaving, setSnapshotSaving] = useState(false)
  const snapshotCaptureRef = useRef<HTMLDivElement | null>(null)

  const { activeDashboard, loading, loadDashboard, fetchWidgetData, fetchAllWidgetData, shareDashboard, widgetData } = useVizStore()
  const {
    width: gridWidth,
    containerRef: gridContainerRef,
    mounted: gridMounted,
  } = useContainerWidth({ initialWidth: 1280, measureBeforeMount: false })

  useEffect(() => {
    if (id) loadDashboard(id)
    setDashboardDrillMode('down')
    setDashboardDrillPath([])
  }, [id, loadDashboard])

  const handleEdit = useCallback(() => {
    if (id) navigate(`/dashboards/${id}/edit`)
  }, [id, navigate])

  const handleBack = useCallback(() => {
    navigate('/dashboards')
  }, [navigate])

  const toAbsoluteUrl = useCallback((path: string): string => {
    if (/^https?:\/\//i.test(path)) return path
    const normalized = path.startsWith('/') ? path : `/${path}`
    return new URL(normalized, window.location.origin).toString()
  }, [])

  const copyText = useCallback(async (text: string, successText: string) => {
    try {
      await navigator.clipboard.writeText(text)
      messageApi.success(successText)
    } catch {
      messageApi.info(text)
    }
  }, [messageApi])

  const handleShare = useCallback(async () => {
    if (!id) return
    try {
      const shared = await shareDashboard(id)
      const shareUrl = toAbsoluteUrl(shared.share_url || `/share/${shared.share_token}`)
      const embedUrl = toAbsoluteUrl(shared.embed_url || `/embed/${shared.share_token}`)
      const iframeCode = `<iframe src="${embedUrl}" width="100%" height="840" style="border:0;" loading="lazy"></iframe>`
      setShareLinks({ shareUrl, embedUrl, iframeCode })
      setShareModalOpen(true)
    } catch {
      messageApi.error('Unable to create share link')
    }
  }, [id, messageApi, shareDashboard, toAbsoluteUrl])

  const handleCaptureSnapshot = useCallback(async () => {
    if (!id) return
    if (snapshotSaving) return
    const captureEl = snapshotCaptureRef.current
    if (!captureEl) {
      messageApi.error('Dashboard container not ready for snapshot')
      return
    }
    setSnapshotSaving(true)
    try {
      await fetchAllWidgetData()
      await delay(250)
      if (document.fonts?.ready) {
        await document.fonts.ready
      }
      await new Promise((resolve) => requestAnimationFrame(() => resolve(null)))
      await waitForCanvasPaint(captureEl, 5000)
      const scale = Math.min(2, Math.max(1, window.devicePixelRatio || 1))
      const canvas = await html2canvas(captureEl, {
        backgroundColor: activeDashboard?.theme === 'light' ? '#f8fafc' : '#0f172a',
        scale,
        useCORS: true,
        logging: false,
        ignoreElements: (element) => {
          const node = element as HTMLElement
          if (node?.dataset?.snapshotIgnore === 'true') return true
          if (node?.classList?.contains('ant-message')) return true
          if (node?.classList?.contains('ant-tooltip')) return true
          return false
        },
      })
      const thumbnail = canvas.toDataURL('image/png')
      await api.saveDashboardThumbnail(id, thumbnail)
      messageApi.success('Dashboard snapshot saved for email embedding')
    } catch {
      messageApi.error('Failed to capture dashboard snapshot')
    } finally {
      setSnapshotSaving(false)
    }
  }, [activeDashboard?.theme, fetchAllWidgetData, id, messageApi, snapshotSaving])

  const handleRefreshWidget = useCallback(async (widgetId: string) => {
    if (!activeDashboard) return
    const widget = activeDashboard.widgets.find(w => w.id === widgetId)
    if (widget) {
      await fetchWidgetData(widget)
      messageApi.success('Data refreshed')
    }
  }, [activeDashboard, fetchWidgetData, messageApi])

  const handleDownloadWidgetData = useCallback((widgetId: string) => {
    if (!activeDashboard) return
    const dataEntry = widgetData[widgetId]
    const rows = Array.isArray(dataEntry?.data) ? (dataEntry.data as CsvRow[]) : []
    if (rows.length === 0) {
      messageApi.warning('No output rows available to download')
      return
    }
    const widget = activeDashboard.widgets.find((w) => w.id === widgetId)
    const dashboardSlug = slugifyName(activeDashboard.name || 'dashboard') || 'dashboard'
    const widgetSlug = slugifyName(widget?.title || widgetId) || 'widget'
    downloadRowsAsCsv(rows, `${dashboardSlug}-${widgetSlug}-output`)
    messageApi.success('Output CSV downloaded')
  }, [activeDashboard, widgetData, messageApi])

  const handleDownloadWidgetFields = useCallback((widgetId: string) => {
    if (!activeDashboard) return
    const dataEntry = widgetData[widgetId]
    const columns = Array.isArray(dataEntry?.columns)
      ? dataEntry.columns.map((col) => String(col || '').trim()).filter(Boolean)
      : []
    if (columns.length === 0) {
      messageApi.warning('No output fields found for this widget')
      return
    }
    const widget = activeDashboard.widgets.find((w) => w.id === widgetId)
    const dashboardSlug = slugifyName(activeDashboard.name || 'dashboard') || 'dashboard'
    const widgetSlug = slugifyName(widget?.title || widgetId) || 'widget'
    downloadFieldListAsCsv(columns, `${dashboardSlug}-${widgetSlug}-fields`)
    messageApi.success('Output fields CSV downloaded')
  }, [activeDashboard, widgetData, messageApi])

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

  // Keep hook order stable across loading/not-found/dashboard states.
  const layouts = useMemo(() => {
    const baseLayout = (activeDashboard?.layout ?? []).map(l => ({ ...l, static: true }))
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const mapped: Record<string, any[]> = {
      lg: baseLayout,
      md: baseLayout,
      sm: baseLayout.map(l => ({ ...l, w: Math.min(l.w, 20), x: Math.min(l.x, 20) })),
      xs: baseLayout.map(l => ({ ...l, w: Math.min(l.w, 8), x: 0 })),
      xxs: baseLayout.map(l => ({ ...l, w: Math.min(l.w, 4), x: 0 })),
    }
    return mapped
  }, [activeDashboard?.layout])

  const dashboardForRoute = useMemo(() => {
    if (!id) return activeDashboard
    if (!activeDashboard) return null
    return activeDashboard.id === id ? activeDashboard : null
  }, [activeDashboard, id])

  useEffect(() => {
    if (!id || !dashboardForRoute) return
    const isPythonDashboard = Array.isArray(dashboardForRoute.tags)
      && dashboardForRoute.tags.some((tag) => String(tag).trim() === PYTHON_DASHBOARD_TAG)
    if (!isPythonDashboard) return
    navigate(`/python-analytics?dashboardId=${encodeURIComponent(id)}`, { replace: true })
  }, [id, dashboardForRoute, navigate])

  // ── Loading state ──
  if (loading && !dashboardForRoute) return <ViewerSkeleton />

  // ── Not found state ──
  if (!dashboardForRoute && !loading) {
    return (
      <div style={{
        height: '100vh',
        background: 'var(--app-shell-bg)',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        gap: 20,
      }}>
        <DashboardOutlined style={{ fontSize: 64, color: 'var(--app-border-strong)' }} />
        <Title level={4} style={{ color: 'var(--app-text-dim)', margin: 0 }}>Dashboard not found</Title>
        <Button
          onClick={handleBack}
          icon={<ArrowLeftOutlined />}
          style={{ background: 'transparent', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-muted)', borderRadius: 8 }}
        >
          Back to dashboards
        </Button>
      </div>
    )
  }

  const dashboard = dashboardForRoute!
  const dashboardTheme = dashboard.theme || 'dark'
  const layoutByWidgetId = new Map((dashboard.layout ?? []).map(item => [item.i, item]))

  return (
    <div ref={snapshotCaptureRef} style={{
      height: '100vh',
      background: 'var(--app-shell-bg)',
      display: 'flex',
      flexDirection: 'column',
      overflow: 'hidden',
    }}>
      {contextHolder}

      {/* ── Top bar ── */}
      <TopBar
        name={dashboard.name}
        updatedAt={dashboard.updated_at}
        onEdit={handleEdit}
        onShare={handleShare}
        onSnapshot={handleCaptureSnapshot}
        snapshotLoading={snapshotSaving}
        onBack={handleBack}
        theme={dashboardTheme}
      />

      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        {/* ── Global filter bar ── */}
        <GlobalFilterBar filters={dashboard.global_filters ?? []} theme={dashboardTheme} />
        <DrillPathBar
          mode={dashboardDrillMode}
          onModeChange={setDashboardDrillMode}
          path={dashboardDrillPath}
          onStepUp={handleDrillStepUp}
          onReset={handleDrillReset}
          theme={dashboardTheme}
        />

        {/* ── Main canvas ── */}
        <div style={{ flex: 1, overflow: 'auto', position: 'relative' }}>
          {loading && (
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
              <div data-snapshot-ignore="true" style={{ color: 'var(--app-text-subtle)', fontSize: 14 }}>Refreshing...</div>
            </div>
          )}

          {dashboard.widgets.length === 0 ? (
            <div style={{
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              height: '100%',
              minHeight: 300,
              gap: 16,
            }}>
              <DashboardOutlined style={{ fontSize: 56, color: 'var(--app-border-strong)' }} />
              <Title level={5} style={{ color: 'var(--app-text-dim)', margin: 0 }}>This dashboard has no widgets yet</Title>
              <Button
                type="primary"
                icon={<EditOutlined />}
                onClick={handleEdit}
                style={{ background: '#6366f1', border: 'none', borderRadius: 8 }}
              >
                Open Editor
              </Button>
            </div>
          ) : (
            <div ref={gridContainerRef as React.RefObject<HTMLDivElement>} style={{ minHeight: '100%' }}>
              {gridMounted && gridWidth > 0 && (
                <ResponsiveGrid
                  width={gridWidth}
                  layouts={layouts}
                  cols={{ lg: 24, md: 20, sm: 12, xs: 8, xxs: 4 }}
                  rowHeight={30}
                  margin={[12, 12]}
                  containerPadding={[20, 20]}
                  measureBeforeMount={false}
                  style={{ minHeight: '100%' }}
                >
                  {dashboard.widgets.map(widget => (
                    <div key={widget.id} style={{ overflow: 'hidden' }}>
                      <WidgetCell
                        widget={widget}
                        dataEntry={widgetData[widget.id]}
                        onRefresh={handleRefreshWidget}
                        onDownloadData={handleDownloadWidgetData}
                        onDownloadFields={handleDownloadWidgetFields}
                        theme={dashboardTheme}
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

      <Modal
        title="Share Dashboard"
        open={shareModalOpen}
        onCancel={() => setShareModalOpen(false)}
        footer={null}
        width={760}
        destroyOnClose
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
          <div>
            <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Public dashboard link</Text>
            <div style={{ display: 'flex', gap: 8, marginTop: 6 }}>
              <Input value={shareLinks?.shareUrl || ''} readOnly />
              <Button onClick={() => shareLinks && copyText(shareLinks.shareUrl, 'Share link copied')}>
                Copy
              </Button>
            </div>
          </div>
          <div>
            <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Embed URL (iframe src)</Text>
            <div style={{ display: 'flex', gap: 8, marginTop: 6 }}>
              <Input value={shareLinks?.embedUrl || ''} readOnly />
              <Button onClick={() => shareLinks && copyText(shareLinks.embedUrl, 'Embed URL copied')}>
                Copy
              </Button>
            </div>
          </div>
          <div>
            <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Iframe snippet</Text>
            <Input.TextArea value={shareLinks?.iframeCode || ''} readOnly rows={3} style={{ marginTop: 6 }} />
            <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: 8 }}>
              <Button type="primary" onClick={() => shareLinks && copyText(shareLinks.iframeCode, 'Iframe code copied')}>
                Copy iframe code
              </Button>
            </div>
          </div>
        </div>
      </Modal>
    </div>
  )
}
