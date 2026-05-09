import { memo, useCallback, useEffect, useMemo, useState, type ReactNode } from 'react'
import { Handle, Position } from 'reactflow'
import { Tag, Typography, Tooltip } from 'antd'
import {
  LoadingOutlined, CheckCircleFilled, CloseCircleFilled,
  ApiOutlined,
  ApartmentOutlined,
  BarChartOutlined,
  BranchesOutlined,
  CloudOutlined,
  CodeOutlined,
  ClockCircleOutlined,
  DatabaseOutlined,
  FileExcelOutlined,
  FileOutlined,
  FileZipOutlined,
  FilterOutlined,
  FunctionOutlined,
  LinkOutlined,
  CaretRightOutlined,
  MailOutlined,
  MessageOutlined,
  PauseOutlined,
  StopOutlined,
  SafetyCertificateOutlined,
  SortAscendingOutlined,
  SwapOutlined,
  TableOutlined,
  TagsOutlined,
  ThunderboltOutlined,
  UploadOutlined,
} from '@ant-design/icons'
import type { ETLNodeData } from '../../types'
import { useWorkflowStore } from '../../store'
import { useBusinessWorkflowStore } from '../../store/businessStore'
import { useMLOpsWorkflowStore } from '../../store/mlopsStore'
import { parseTimestampMsOrNaN } from '../../utils/time'

const { Text } = Typography

interface ETLNodeProps {
  id: string
  data: ETLNodeData
  selected: boolean
}

const statusIcon = {
  running: <LoadingOutlined spin style={{ color: '#6366f1', fontSize: 12 }} />,
  success: <CheckCircleFilled style={{ color: '#22c55e', fontSize: 12 }} />,
  error: <CloseCircleFilled style={{ color: '#ef4444', fontSize: 12 }} />,
  idle: null,
}

const statusMeta: Record<string, { text: string; color: string }> = {
  running: { text: 'Running', color: '#6366f1' },
  success: { text: 'Success', color: '#22c55e' },
  error: { text: 'Error', color: '#ef4444' },
  idle: { text: 'Idle', color: 'var(--app-text-subtle)' },
}

function resolveCategoryCode(category: string): string {
  const cat = String(category || '').toLowerCase()
  if (cat === 'trigger') return 'TRG'
  if (cat === 'source') return 'SRC'
  if (cat === 'transform') return 'TRN'
  if (cat === 'destination') return 'DST'
  if (cat === 'flow') return 'FLW'
  if (cat === 'quality') return 'QTY'
  if (cat === 'mlops') return 'MLO'
  return 'NOD'
}

function resolveTailwindAccent(category: string, fallback: string): string {
  const cat = String(category || '').toLowerCase()
  if (cat === 'trigger') return '#f59e0b' // amber-500
  if (cat === 'source') return '#0ea5e9' // sky-500
  if (cat === 'transform') return '#8b5cf6' // violet-500
  if (cat === 'destination') return '#10b981' // emerald-500
  if (cat === 'flow') return '#f97316' // orange-500
  if (cat === 'quality') return '#ef4444' // red-500
  if (cat === 'mlops') return '#06b6d4' // cyan-500
  return fallback || '#64748b' // slate-500
}

function resolveNinIcon(nodeType: string, category: string, fallbackIcon: string): ReactNode {
  const type = String(nodeType || '').toLowerCase()
  const cat = String(category || '').toLowerCase()

  if (cat === 'trigger') {
    if (type.includes('schedule')) return <ClockCircleOutlined />
    if (type.includes('webhook')) return <LinkOutlined />
    return <CaretRightOutlined />
  }

  if (cat === 'source') {
    if (/csv/.test(type)) return <TableOutlined />
    if (/json/.test(type)) return <CodeOutlined />
    if (/(excel|xlsx|xls)/.test(type)) return <FileExcelOutlined />
    if (/xml/.test(type)) return <TagsOutlined />
    if (/parquet/.test(type)) return <FileZipOutlined />
    if (/(s3|blob|gcs|cloud)/.test(type)) return <CloudOutlined />
    if (/(postgres|mysql|oracle|mongodb|mongo|redis|rocksdb|lmdb|elasticsearch|kafka)/.test(type)) return <DatabaseOutlined />
    if (/(api|rest|graphql|webhook)/.test(type)) return <ApiOutlined />
    if (/file/.test(type)) return <FileOutlined />
    return <DatabaseOutlined />
  }

  if (cat === 'transform') {
    if (type.includes('delay')) return <ClockCircleOutlined />
    if (type.includes('filter')) return <FilterOutlined />
    if (type.includes('join')) return <SwapOutlined />
    if (type.includes('aggregate')) return <BarChartOutlined />
    if (type.includes('sort')) return <SortAscendingOutlined />
    if (type.includes('deduplicate')) return <SafetyCertificateOutlined />
    return <FunctionOutlined />
  }

  if (cat === 'destination') {
    if (/(mail|email)/.test(type)) return <MailOutlined />
    if (/(whatsapp|chat|message|sms)/.test(type)) return <MessageOutlined />
    if (/csv/.test(type)) return <TableOutlined />
    if (/json/.test(type)) return <CodeOutlined />
    if (/(excel|xlsx|xls)/.test(type)) return <FileExcelOutlined />
    if (/xml/.test(type)) return <TagsOutlined />
    if (/parquet/.test(type)) return <FileZipOutlined />
    if (/(s3|blob|gcs|cloud)/.test(type)) return <CloudOutlined />
    if (/(postgres|mysql|oracle|mongodb|mongo|redis|rocksdb|lmdb|elasticsearch|kafka)/.test(type)) return <DatabaseOutlined />
    if (/(api|rest|graphql|webhook)/.test(type)) return <ApiOutlined />
    if (/file/.test(type)) return <FileOutlined />
    return <UploadOutlined />
  }

  if (cat === 'flow') return <BranchesOutlined />
  if (cat === 'quality') return <SafetyCertificateOutlined />
  if (cat === 'mlops') return <ThunderboltOutlined />

  if (type.includes('partition') || type.includes('group')) return <ApartmentOutlined />
  return fallbackIcon || <FunctionOutlined />
}

function normalizeConditionCaseHandleId(rawRouteId: unknown): string {
  const raw = String(rawRouteId || '').trim().toLowerCase()
  const safe = raw.replace(/[^a-z0-9_]+/g, '_').replace(/^_+|_+$/g, '') || 'route_unknown'
  return `case_${safe}`
}

function parseConditionCaseRouteHandles(config: Record<string, unknown>): string[] {
  const routingMode = String(config.condition_routing_mode || '').trim().toLowerCase()
  if (routingMode !== 'case') return []
  let rawRoutes: unknown = config.case_routes
  if (typeof rawRoutes === 'string') {
    try {
      rawRoutes = JSON.parse(rawRoutes)
    } catch {
      rawRoutes = []
    }
  }
  if (!Array.isArray(rawRoutes)) return []
  const out: string[] = []
  const seen = new Set<string>()
  rawRoutes.forEach((route) => {
    if (!route || typeof route !== 'object') return
    const src = route as Record<string, unknown>
    const routeId = String(src.id || '').trim()
    const explicitHandle = String(src.handle_id || src.handleId || '').trim()
    const handleId = explicitHandle || normalizeConditionCaseHandleId(routeId)
    if (!handleId) return
    if (handleId === 'output' || handleId === 'output_false') return
    if (seen.has(handleId)) return
    seen.add(handleId)
    out.push(handleId)
  })
  return out
}

export const ETLNode = memo(({ id, data, selected }: ETLNodeProps) => {
  const canvasWidgetStyle = useWorkflowStore((state) => state.canvasWidgetStyle)
  const updateWorkflowNodeConfig = useWorkflowStore((state) => state.updateNodeConfig)
  const updateBusinessNodeConfig = useBusinessWorkflowStore((state) => state.updateNodeConfig)
  const updateMLOpsNodeConfig = useMLOpsWorkflowStore((state) => state.updateNodeConfig)
  const {
    definition,
    label,
    status = 'idle',
    executionRows,
    executionProcessedRows,
    executionValidatedRows,
    executionStartedAt,
    executionFinishedAt,
    executionDurationMs,
  } = data
  if (!definition) return null
  const isNinStyle = canvasWidgetStyle === 'nin'

  const { color, bgColor, icon, inputs, outputs, category } = definition
  const accentColor = useMemo(() => resolveTailwindAccent(category, color), [category, color])
  const isRunning = status === 'running'
  const processingHighlightColor = '#f97316'
  const hasFinishedExecution = String(executionFinishedAt || '').trim().length > 0
  const workflowExecuting = useWorkflowStore((state) => state.isExecuting)
  const businessExecuting = useBusinessWorkflowStore((state) => state.isExecuting)
  const mlopsExecuting = useMLOpsWorkflowStore((state) => state.isExecuting)
  const anyExecutionActive = workflowExecuting || businessExecuting || mlopsExecuting
  const countersIndicateDone = (
    isRunning
    && typeof executionRows === 'number'
    && (
      (typeof executionProcessedRows === 'number' && executionProcessedRows >= executionRows)
      || (typeof executionValidatedRows === 'number' && executionValidatedRows >= executionRows)
    )
  )
  const shouldTickDuration = isRunning && !hasFinishedExecution && anyExecutionActive && !countersIndicateDone
  const statusInfo = statusMeta[status] || statusMeta.idle
  const [tickNowMs, setTickNowMs] = useState<number>(() => Date.now())
  const [isHovered, setIsHovered] = useState(false)

  useEffect(() => {
    if (!shouldTickDuration) return
    setTickNowMs(Date.now())
    const timer = window.setInterval(() => {
      setTickNowMs(Date.now())
    }, 1000)
    return () => window.clearInterval(timer)
  }, [shouldTickDuration, executionStartedAt])

  const executionDurationLabel = useMemo(() => {
    const formatDuration = (ms: number): string => {
      if (!Number.isFinite(ms) || ms <= 0) return '0s'
      if (ms < 1000) return `${Math.max(1, Math.round(ms))}ms`
      const totalSeconds = Math.floor(ms / 1000)
      const seconds = totalSeconds % 60
      const minutes = Math.floor(totalSeconds / 60) % 60
      const hours = Math.floor(totalSeconds / 3600)
      if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`
      if (minutes > 0) return `${minutes}m ${seconds}s`
      return `${seconds}s`
    }

    if (shouldTickDuration) {
      const startedAt = String(executionStartedAt || '').trim()
      if (!startedAt) return '0s'
      const startedMs = parseTimestampMsOrNaN(startedAt)
      if (!Number.isFinite(startedMs)) return '0s'
      return formatDuration(Math.max(0, tickNowMs - Number(startedMs)))
    }

    const startedAt = String(executionStartedAt || '').trim()
    const finishedAt = String(executionFinishedAt || '').trim()
    const startedMs = parseTimestampMsOrNaN(startedAt)
    const finishedMs = parseTimestampMsOrNaN(finishedAt)
    const inferredDurationMs = (
      Number.isFinite(startedMs) && Number.isFinite(finishedMs) && Number(finishedMs) >= Number(startedMs)
    )
      ? Math.max(0, Number(finishedMs) - Number(startedMs))
      : undefined
    const directDurationMs = (
      typeof executionDurationMs === 'number' && Number.isFinite(executionDurationMs)
    )
      ? Math.max(0, Number(executionDurationMs))
      : undefined

    const resolvedDurationMs = (() => {
      if (typeof directDurationMs === 'number' && typeof inferredDurationMs === 'number') {
        return Math.max(directDurationMs, inferredDurationMs)
      }
      if (typeof directDurationMs === 'number') return directDurationMs
      if (typeof inferredDurationMs === 'number') return inferredDurationMs
      return undefined
    })()

    if (typeof resolvedDurationMs === 'number') {
      if (resolvedDurationMs > 0) {
        return formatDuration(resolvedDurationMs)
      }
      if (status !== 'running' && (hasFinishedExecution || finishedAt)) {
        return '<1s'
      }
    }
    return status === 'running' ? '0s' : undefined
  }, [executionStartedAt, executionFinishedAt, executionDurationMs, shouldTickDuration, status, tickNowMs, hasFinishedExecution])
  const nodeEnabled = (() => {
    const raw = (data?.config as Record<string, unknown> | undefined)?.node_enabled
    if (typeof raw === 'boolean') return raw
    if (typeof raw === 'number') return raw !== 0
    if (typeof raw === 'string') {
      const norm = raw.trim().toLowerCase()
      if (['false', '0', 'no', 'off', 'disabled', 'disable'].includes(norm)) return false
      if (['true', '1', 'yes', 'on', 'enabled', 'enable'].includes(norm)) return true
    }
    return true
  })()
  const isDisabled = !nodeEnabled
  const nodeConfig = ((data?.config && typeof data.config === 'object') ? data.config : {}) as Record<string, unknown>
  const nodeType = String((data as any)?.nodeType || '').trim().toLowerCase()
  const isConditionNode = nodeType === 'condition_node'
  const caseRouteSourceHandleIds = useMemo(
    () => (isConditionNode ? parseConditionCaseRouteHandles(nodeConfig) : []),
    [nodeType, nodeConfig],
  )
  const isConditionCaseRouting = isConditionNode && caseRouteSourceHandleIds.length > 0

  const sourceOutputFieldNames = useMemo(() => {
    if (String(category || '').toLowerCase() !== 'source') return []

    const unique: string[] = []
    const seen = new Set<string>()
    const pushField = (name: unknown) => {
      const text = String(name ?? '').trim()
      if (!text) return
      const key = text.toLowerCase()
      if (seen.has(key)) return
      seen.add(key)
      unique.push(text)
    }
    const collectFromRow = (row: unknown) => {
      if (!row || typeof row !== 'object' || Array.isArray(row)) return
      Object.keys(row as Record<string, unknown>).forEach((name) => pushField(name))
    }
    const collectFromRows = (rows: unknown) => {
      if (!Array.isArray(rows)) return
      const firstRow = rows.find((item) => !!item && typeof item === 'object' && !Array.isArray(item))
      if (firstRow) collectFromRow(firstRow)
    }

    const detectedRaw = String(nodeConfig._detected_columns || '').trim()
    if (detectedRaw) {
      detectedRaw
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean)
        .forEach((fieldName) => pushField(fieldName))
    }

    collectFromRows(data.executionSampleOutput)
    collectFromRows(data.executionSampleInput)
    collectFromRows(nodeConfig._preview_rows)

    return unique
  }, [category, data.executionSampleInput, data.executionSampleOutput, nodeConfig._detected_columns, nodeConfig._preview_rows])

  const sourceOutputFieldsCompact = useMemo(() => {
    if (!sourceOutputFieldNames.length) return ''
    const maxCount = isNinStyle ? 3 : 4
    const preview = sourceOutputFieldNames.slice(0, maxCount).join(', ')
    const remaining = sourceOutputFieldNames.length - Math.min(sourceOutputFieldNames.length, maxCount)
    return remaining > 0 ? `${preview} +${remaining}` : preview
  }, [isNinStyle, sourceOutputFieldNames])

  const sourceOutputFieldsFull = useMemo(
    () => sourceOutputFieldNames.join(', '),
    [sourceOutputFieldNames],
  )

  const toggleNodeEnabled = useCallback((event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault()
    event.stopPropagation()
    const next = !nodeEnabled
    const patch: Record<string, unknown> = { node_enabled: next }
    updateWorkflowNodeConfig(id, patch)
    updateBusinessNodeConfig(id, patch)
    updateMLOpsNodeConfig(id, patch)
  }, [id, nodeEnabled, updateBusinessNodeConfig, updateMLOpsNodeConfig, updateWorkflowNodeConfig])
  const ninMetaLabel = useMemo(() => {
    if (!isNinStyle) return ''
    const parts: string[] = []
    if (typeof executionRows === 'number') parts.push(`${executionRows.toLocaleString()} rows`)
    if (status !== 'idle') parts.push(statusInfo.text.toLowerCase())
    if (executionDurationLabel) parts.push(executionDurationLabel)
    return parts.join(' • ')
  }, [executionDurationLabel, executionRows, isNinStyle, status, statusInfo.text])
  const categoryCode = useMemo(() => resolveCategoryCode(category), [category])
  const tintedCardBg = useMemo(
    () => 'var(--app-card-bg)',
    [],
  )
  const tintedPanelBg = useMemo(
    () => 'var(--app-panel-bg)',
    [],
  )

  if (isNinStyle) {
    const ninSurface = tintedCardBg
    const ninSurfaceAlt = tintedPanelBg
    const ninSurfaceBorder = 'var(--app-border-strong)'
    const ninBorderColor = isRunning
      ? processingHighlightColor
      : selected
      ? '#3b82f6'
      : (status === 'success'
        ? 'color-mix(in srgb, #22c55e 42%, var(--app-border-strong) 58%)'
        : (status === 'error' ? '#ef4444' : `color-mix(in srgb, ${accentColor} 62%, var(--app-border-strong) 38%)`))
    const ninCardShadow = selected
      ? '0 0 0 1px rgba(59,130,246,0.28), 0 6px 16px rgba(0,0,0,0.22)'
      : '0 1px 3px rgba(0,0,0,0.14)'

    return (
      <div
        style={{
          minWidth: 132,
          maxWidth: 190,
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          gap: 6,
          position: 'relative',
          opacity: isDisabled ? 0.66 : 1,
          filter: isDisabled ? 'grayscale(0.78) saturate(0.55)' : 'none',
        }}
        onMouseEnter={() => setIsHovered(true)}
        onMouseLeave={() => setIsHovered(false)}
      >
        <Tooltip title={isDisabled ? 'Enable Node' : 'Pause Node'}>
          <button
            type="button"
            className="nodrag nopan"
            onMouseDown={(event) => {
              event.preventDefault()
              event.stopPropagation()
            }}
            onClick={toggleNodeEnabled}
            style={{
              position: 'absolute',
              top: -1,
              left: '50%',
              transform: 'translate(-50%, -50%)',
              zIndex: 12,
              width: 30,
              height: 30,
              border: 'none',
              background: 'transparent',
              color: isDisabled ? '#22c55e' : '#f59e0b',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              cursor: 'pointer',
              opacity: isHovered ? 1 : 0,
              pointerEvents: isHovered ? 'auto' : 'none',
              transition: 'opacity 0.15s ease',
            }}
          >
            {isDisabled ? <CaretRightOutlined style={{ fontSize: 22 }} /> : <PauseOutlined style={{ fontSize: 20 }} />}
          </button>
        </Tooltip>
        <div
          style={{
            width: 92,
            height: 92,
            borderRadius: 16,
            border: `${isDisabled ? 2 : 2.5}px ${isDisabled ? 'dashed' : 'solid'} ${ninBorderColor}`,
            background: ninSurface,
            position: 'relative',
            boxShadow: ninCardShadow,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            overflow: 'visible',
          }}
        >
          {status === 'running' && (
            <>
              <div
                style={{
                  position: 'absolute',
                  inset: 0,
                  borderRadius: 16,
                  border: '2px solid rgba(191,219,254,0.38)',
                  pointerEvents: 'none',
                  zIndex: 4,
                }}
              />
              <div
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: 24,
                  height: 10,
                  pointerEvents: 'none',
                  zIndex: 5,
                  background: 'linear-gradient(90deg, rgba(147,197,253,0.15) 0%, rgba(191,219,254,0.6) 55%, #ffffff 100%)',
                  clipPath: 'polygon(0 50%, 55% 50%, 72% 0, 100% 50%, 72% 100%, 55% 50%)',
                  filter: 'drop-shadow(0 0 6px rgba(191,219,254,0.8))',
                  offsetPath: 'inset(1.25px round 14.75px)',
                  offsetDistance: '0%',
                  offsetRotate: 'auto',
                  animation: 'borderArrowFlow 2.7s linear infinite',
                }}
              />
              <div
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: 18,
                  height: 8,
                  pointerEvents: 'none',
                  zIndex: 5,
                  background: 'linear-gradient(90deg, rgba(147,197,253,0.1) 0%, rgba(191,219,254,0.45) 55%, rgba(255,255,255,0.95) 100%)',
                  clipPath: 'polygon(0 50%, 52% 50%, 70% 0, 100% 50%, 70% 100%, 52% 50%)',
                  filter: 'drop-shadow(0 0 4px rgba(191,219,254,0.65))',
                  offsetPath: 'inset(1.25px round 14.75px)',
                  offsetDistance: '0%',
                  offsetRotate: 'auto',
                  animation: 'borderArrowFlow 2.7s linear infinite',
                  animationDelay: '-1.35s',
                }}
              />
            </>
          )}

          {inputs > 0 && (
            <Handle
              type="target"
              position={Position.Left}
              id="input"
              style={{
                background: '#808a97',
                border: `2px solid ${ninSurface}`,
                width: 12,
                height: 12,
                left: -7,
                borderRadius: 999,
              }}
            />
          )}

          <div
            style={{
              width: '90%',
              height: '90%',
              borderRadius: 10,
              border: `1px solid ${accentColor}25`,
              background: bgColor,
              padding: '5%',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: 'inherit',
              fontFamily: 'system-ui',
              position: 'relative',
              overflow: 'hidden',
            }}
          >
            <div
              style={{
                width: '100%',
                height: '100%',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontSize: isConditionNode ? 52 : 48,
                lineHeight: 1,
              }}
            >
              {icon}
            </div>
          </div>

          {status === 'success' && (
            <div
              style={{
                position: 'absolute',
                right: 6,
                bottom: 6,
                width: 18,
                height: 18,
                borderRadius: 999,
                background: '#16a34a',
                border: `2px solid ${ninSurface}`,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: '#ffffff',
                fontSize: 11,
                fontWeight: 700,
              }}
            >
              ✓
            </div>
          )}

          {status === 'running' && (
            <div
              style={{
                position: 'absolute',
                right: 6,
                bottom: 6,
                width: 18,
                height: 18,
                borderRadius: 999,
                background: ninSurface,
                border: `1px solid ${ninSurfaceBorder}`,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
              }}
            >
              <LoadingOutlined spin style={{ color: '#6366f1', fontSize: 11 }} />
            </div>
          )}

          {status === 'error' && (
            <div
              style={{
                position: 'absolute',
                right: 6,
                bottom: 6,
                width: 18,
                height: 18,
                borderRadius: 999,
                background: '#ef4444',
                border: `2px solid ${ninSurface}`,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: '#ffffff',
                fontSize: 11,
                fontWeight: 700,
              }}
            >
              !
            </div>
          )}

          <>
            {outputs > 0 && (
              <Handle
                type="source"
                position={Position.Right}
                id="output"
                style={{
                  background: '#808a97',
                  border: `2px solid ${ninSurface}`,
                  width: 12,
                  height: 12,
                  right: -7,
                  top: isConditionNode ? '30%' : undefined,
                  borderRadius: 999,
                }}
              />
            )}
            {isConditionCaseRouting && caseRouteSourceHandleIds.map((handleId, index) => {
              const total = caseRouteSourceHandleIds.length
              const top = `${Math.round(((index + 1) * 100) / (total + 1))}%`
              return (
                <Handle
                  key={`nin_case_handle_${id}_${handleId}`}
                  type="source"
                  position={Position.Right}
                  id={handleId}
                  style={{
                    background: 'transparent',
                    border: 'none',
                    width: 1,
                    height: 1,
                    right: -1,
                    top,
                    borderRadius: 999,
                    opacity: 0,
                    pointerEvents: 'none',
                  }}
                />
              )
            })}
            {outputs > 1 && (
              <Handle
                type="source"
                position={Position.Right}
                id="output_false"
                style={{
                  background: '#ef4444',
                  border: `2px solid ${ninSurface}`,
                  width: 12,
                  height: 12,
                  right: -7,
                  top: '70%',
                  borderRadius: 999,
                }}
              />
            )}
          </>
        </div>

        <Text
          ellipsis
          style={{
            maxWidth: 184,
            textAlign: 'center',
            color: isDisabled ? 'var(--app-text-subtle)' : 'var(--app-text)',
            fontWeight: 600,
            fontSize: 11.5,
            lineHeight: 1.25,
          }}
        >
          {label}
        </Text>
        {sourceOutputFieldsCompact ? (
          <Tooltip title={sourceOutputFieldsFull}>
            <Text style={{ maxWidth: 184, textAlign: 'center', color: 'var(--app-text-subtle)', fontSize: 10 }}>
              fields: {sourceOutputFieldsCompact}
            </Text>
          </Tooltip>
        ) : null}
        {ninMetaLabel && (
          <Text style={{ maxWidth: 184, textAlign: 'center', color: 'var(--app-text-subtle)', fontSize: 10 }}>
            {ninMetaLabel}
          </Text>
        )}
        <style>{`
          @keyframes borderArrowFlow {
            0% { offset-distance: 0%; }
            100% { offset-distance: 100%; }
          }
        `}</style>
      </div>
    )
  }

  return (
    <div
      style={{
        minWidth: isNinStyle ? 196 : 180,
        maxWidth: isNinStyle ? 238 : 220,
        background: tintedCardBg,
        border: `${selected ? 2 : 1}px solid ${
          isRunning
            ? processingHighlightColor
            : selected
            ? (isNinStyle ? '#3b82f6' : accentColor)
            : isDisabled
              ? '#64748b'
              : `color-mix(in srgb, ${accentColor} 62%, var(--app-border-strong) 38%)`
        }`,
        borderStyle: isDisabled ? 'dashed' : 'solid',
        borderRadius: isNinStyle ? 8 : 12,
        boxShadow: selected
          ? (isNinStyle
            ? '0 0 0 1px rgba(59,130,246,0.32), 0 6px 16px rgba(0,0,0,0.24)'
            : `0 0 0 2px ${accentColor}55, 0 0 0 6px ${accentColor}22, 0 10px 36px rgba(0,0,0,0.48)`)
          : (isNinStyle ? '0 1px 3px rgba(0,0,0,0.16)' : '0 4px 16px rgba(0,0,0,0.3)'),
        transition: 'all 0.2s',
        overflow: 'visible',
        position: 'relative',
        opacity: selected ? 1 : isDisabled ? 0.66 : 1,
        filter: isDisabled ? 'grayscale(0.78) saturate(0.55)' : 'none',
        ...(isRunning ? {
          boxShadow: selected
            ? (isNinStyle ? '0 0 0 1px rgba(59,130,246,0.55)' : '0 0 0 2px #f97316')
            : (isNinStyle ? '0 0 0 1px rgba(59,130,246,0.35)' : '0 0 0 2px #f97316'),
        } : {}),
      }}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <Tooltip title={isDisabled ? 'Enable Node' : 'Pause Node'}>
        <button
          type="button"
          className="nodrag nopan"
          onMouseDown={(event) => {
            event.preventDefault()
            event.stopPropagation()
          }}
          onClick={toggleNodeEnabled}
          style={{
            position: 'absolute',
            top: -1,
            left: '50%',
            transform: 'translate(-50%, -50%)',
            zIndex: 12,
            width: 30,
            height: 30,
            border: 'none',
            background: 'transparent',
            color: isDisabled ? '#22c55e' : '#f59e0b',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            cursor: 'pointer',
            opacity: isHovered ? 1 : 0,
            pointerEvents: isHovered ? 'auto' : 'none',
            transition: 'opacity 0.15s ease',
          }}
        >
          {isDisabled ? <CaretRightOutlined style={{ fontSize: 22 }} /> : <PauseOutlined style={{ fontSize: 20 }} />}
        </button>
      </Tooltip>
      {selected && (
        <div
          style={{
            position: 'absolute',
            inset: isNinStyle ? -2 : -7,
            borderRadius: isNinStyle ? 10 : 16,
            border: isNinStyle ? '1px solid rgba(59,130,246,0.42)' : `1px solid ${accentColor}66`,
            pointerEvents: 'none',
          }}
        />
      )}

      {/* Status ring (running animation) */}
      {isRunning && !isNinStyle && (
        <>
          <div style={{
            position: 'absolute',
            inset: 0,
            borderRadius: 12,
            border: '2px solid rgba(191,219,254,0.38)',
            pointerEvents: 'none',
            zIndex: 4,
          }} />
          <div style={{
            position: 'absolute',
            top: 0,
            left: 0,
            width: 24,
            height: 10,
            pointerEvents: 'none',
            zIndex: 5,
            background: 'linear-gradient(90deg, rgba(147,197,253,0.15) 0%, rgba(191,219,254,0.6) 55%, #ffffff 100%)',
            clipPath: 'polygon(0 50%, 55% 50%, 72% 0, 100% 50%, 72% 100%, 55% 50%)',
            filter: 'drop-shadow(0 0 6px rgba(191,219,254,0.8))',
            offsetPath: 'inset(1px round 11px)',
            offsetDistance: '0%',
            offsetRotate: 'auto',
            animation: 'borderArrowFlow 2.7s linear infinite',
          }} />
          <div style={{
            position: 'absolute',
            top: 0,
            left: 0,
            width: 18,
            height: 8,
            pointerEvents: 'none',
            zIndex: 5,
            background: 'linear-gradient(90deg, rgba(147,197,253,0.1) 0%, rgba(191,219,254,0.45) 55%, rgba(255,255,255,0.95) 100%)',
            clipPath: 'polygon(0 50%, 52% 50%, 70% 0, 100% 50%, 70% 100%, 52% 50%)',
            filter: 'drop-shadow(0 0 4px rgba(191,219,254,0.65))',
            offsetPath: 'inset(1px round 11px)',
            offsetDistance: '0%',
            offsetRotate: 'auto',
            animation: 'borderArrowFlow 2.7s linear infinite',
            animationDelay: '-1.35s',
          }} />
        </>
      )}

      {/* Input handle(s) */}
      {inputs > 0 && (
        <Handle
          type="target"
          position={Position.Left}
          id="input"
          style={{
            background: isNinStyle ? '#9ca3af' : accentColor, border: `2px solid var(--app-card-bg)`,
            width: isNinStyle ? 10 : 12, height: isNinStyle ? 10 : 12, left: isNinStyle ? -5 : -6,
            borderRadius: 999,
            boxShadow: isNinStyle ? '0 0 0 1px rgba(148,163,184,0.25)' : 'none',
          }}
        />
      )}

      {/* Node Body */}
      <div style={{ padding: isNinStyle ? '9px 10px 8px' : '12px 14px 10px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {/* Icon */}
          <div style={{
            width: isNinStyle ? 44 : 44, height: isNinStyle ? 44 : 44,
            background: isNinStyle
              ? 'color-mix(in srgb, var(--app-card-bg) 78%, white 22%)'
              : bgColor,
            border: isNinStyle ? '1px solid #dbe2ea' : `1px solid ${accentColor}25`,
            borderRadius: 7,
            padding: '5%',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            flexShrink: 0,
            color: 'inherit',
            fontFamily: 'system-ui',
            position: 'relative',
            overflow: 'hidden',
          }}>
            <div
              style={{
                width: '100%',
                height: '100%',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontSize: isConditionNode ? 34 : 30,
                lineHeight: 1,
              }}
            >
              {icon}
            </div>
          </div>
          {/* Label + status */}
          <div style={{ flex: 1, minWidth: 0 }}>
            <Text
              ellipsis
              style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: isNinStyle ? 12 : 13, display: 'block', lineHeight: 1.3 }}
            >
              {label}
            </Text>
            {!isNinStyle && (
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                  {category}
                </Text>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 9 }}>
                  {categoryCode}
                </Text>
              </div>
            )}
            {!isNinStyle && sourceOutputFieldsCompact && (
              <Tooltip title={sourceOutputFieldsFull}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 10, display: 'block', marginTop: 2 }}>
                  fields: {sourceOutputFieldsCompact}
                </Text>
              </Tooltip>
            )}
            {!isNinStyle && (status === 'running' || status === 'error') && (
              <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 4 }}>
                <span
                  style={{
                    width: 6,
                    height: 6,
                    borderRadius: 999,
                    background: statusInfo.color,
                    boxShadow: status === 'running' ? `0 0 0 3px ${statusInfo.color}22` : 'none',
                  }}
                />
                <Text style={{ color: statusInfo.color, fontSize: 10, fontWeight: 600 }}>
                  {statusInfo.text}
                  {typeof executionRows === 'number' ? ` · ${executionRows.toLocaleString()}` : ''}
                </Text>
              </div>
            )}
          </div>
          {/* Status icon */}
          <div style={{ flexShrink: 0 }}>
            {isDisabled
              ? <StopOutlined style={{ color: '#ef4444', fontSize: isNinStyle ? 13 : 14 }} />
              : isNinStyle && status === 'idle'
                ? null
                : statusIcon[status]}
          </div>
        </div>

        {/* Execution stats */}
        {!isNinStyle && (executionRows !== undefined || executionProcessedRows !== undefined || executionValidatedRows !== undefined) && (
          <div style={{
            marginTop: 8,
            padding: '4px 8px',
            background: 'var(--app-panel-bg)',
            border: isNinStyle ? '1px solid var(--app-border-strong)' : 'none',
            borderRadius: isNinStyle ? 5 : 6,
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: 8,
          }}>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Rows out</Text>
              <Text style={{ color: accentColor, fontWeight: 600, fontSize: 11 }}>
                {typeof executionRows === 'number' ? executionRows.toLocaleString() : 'N/A'}
              </Text>
            </div>
            {(executionProcessedRows !== undefined || executionValidatedRows !== undefined) && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 2, alignItems: 'flex-end' }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 10 }}>
                  processed {typeof executionProcessedRows === 'number' ? executionProcessedRows.toLocaleString() : '0'}
                </Text>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 10 }}>
                  validated {typeof executionValidatedRows === 'number' ? executionValidatedRows.toLocaleString() : '0'}
                </Text>
              </div>
            )}
          </div>
        )}
        {isNinStyle && ninMetaLabel && (
          <div style={{ marginTop: 6 }}>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 10 }}>
              {ninMetaLabel}
            </Text>
          </div>
        )}
        {!isNinStyle && (status !== 'idle' && executionDurationLabel) && (
          <div style={{ marginTop: 6, display: 'flex', justifyContent: 'flex-end' }}>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 10 }}>
              time {executionDurationLabel}
            </Text>
          </div>
        )}
      </div>

      {/* Output handle(s) */}
      <>
        {outputs > 0 && (
          <Handle
            type="source"
            position={Position.Right}
            id="output"
            style={{
              background: isNinStyle ? '#9ca3af' : accentColor, border: `2px solid var(--app-card-bg)`,
              width: isNinStyle ? 10 : 12, height: isNinStyle ? 10 : 12, right: isNinStyle ? -5 : -6,
              top: isConditionNode ? '30%' : undefined,
              borderRadius: 999,
              boxShadow: isNinStyle ? '0 0 0 1px rgba(148,163,184,0.25)' : 'none',
            }}
          />
        )}
        {isConditionCaseRouting && caseRouteSourceHandleIds.map((handleId, index) => {
          const total = caseRouteSourceHandleIds.length
          const top = `${Math.round(((index + 1) * 100) / (total + 1))}%`
          return (
            <Handle
              key={`case_handle_${id}_${handleId}`}
              type="source"
              position={Position.Right}
              id={handleId}
              style={{
                background: 'transparent',
                border: 'none',
                width: 1,
                height: 1,
                right: -1,
                top,
                borderRadius: 999,
                opacity: 0,
                pointerEvents: 'none',
              }}
            />
          )
        })}
        {/* Second output (for condition nodes) */}
        {outputs > 1 && (
          <Handle
            type="source"
            position={Position.Right}
            id="output_false"
            style={{
              background: isNinStyle ? '#9ca3af' : '#ef4444', border: `2px solid var(--app-card-bg)`,
              width: isNinStyle ? 10 : 12, height: isNinStyle ? 10 : 12, right: isNinStyle ? -5 : -6, top: '70%',
              borderRadius: 999,
              boxShadow: isNinStyle ? '0 0 0 1px rgba(148,163,184,0.25)' : 'none',
            }}
          />
        )}
      </>

      <style>{`
        @keyframes borderArrowFlow {
          0% { offset-distance: 0%; }
          100% { offset-distance: 100%; }
        }
      `}</style>
    </div>
  )
})

ETLNode.displayName = 'ETLNode'
