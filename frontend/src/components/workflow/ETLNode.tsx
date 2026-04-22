import { memo, useEffect, useMemo, useState } from 'react'
import { Handle, Position } from 'reactflow'
import { Tag, Typography, Tooltip } from 'antd'
import {
  LoadingOutlined, CheckCircleFilled, CloseCircleFilled,
  PauseCircleFilled,
} from '@ant-design/icons'
import type { ETLNodeData } from '../../types'

const { Text } = Typography

interface ETLNodeProps {
  id: string
  data: ETLNodeData
  selected: boolean
}

const statusRing: Record<string, string> = {
  running: '#6366f1',
  success: '#22c55e',
  error: '#ef4444',
  idle: 'transparent',
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

export const ETLNode = memo(({ id, data, selected }: ETLNodeProps) => {
  const {
    definition,
    label,
    status = 'idle',
    executionRows,
    executionProcessedRows,
    executionValidatedRows,
    executionStartedAt,
    executionDurationMs,
  } = data
  if (!definition) return null

  const { color, bgColor, icon, inputs, outputs, category } = definition
  const ringColor = statusRing[status]
  const isRunning = status === 'running'
  const statusInfo = statusMeta[status] || statusMeta.idle
  const [tickNowMs, setTickNowMs] = useState<number>(() => Date.now())

  useEffect(() => {
    if (!isRunning) return
    const timer = window.setInterval(() => {
      setTickNowMs(Date.now())
    }, 1000)
    return () => window.clearInterval(timer)
  }, [isRunning])

  const executionDurationLabel = useMemo(() => {
    const formatDuration = (ms: number): string => {
      if (!Number.isFinite(ms) || ms <= 0) return '0s'
      const totalSeconds = Math.floor(ms / 1000)
      const seconds = totalSeconds % 60
      const minutes = Math.floor(totalSeconds / 60) % 60
      const hours = Math.floor(totalSeconds / 3600)
      if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`
      if (minutes > 0) return `${minutes}m ${seconds}s`
      return `${seconds}s`
    }

    if (isRunning) {
      const startedAt = String(executionStartedAt || '').trim()
      if (!startedAt) return '0s'
      const startedMs = Date.parse(startedAt)
      if (!Number.isFinite(startedMs)) return '0s'
      return formatDuration(Math.max(0, tickNowMs - Number(startedMs)))
    }

    if (typeof executionDurationMs === 'number' && Number.isFinite(executionDurationMs)) {
      return formatDuration(Math.max(0, executionDurationMs))
    }
    return undefined
  }, [executionStartedAt, executionDurationMs, isRunning, tickNowMs])
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

  return (
    <div
      style={{
        minWidth: 180,
        maxWidth: 220,
        background: 'var(--app-card-bg)',
        border: `1px solid ${selected ? color : isDisabled ? '#94a3b8' : 'var(--app-border-strong)'}`,
        borderRadius: 12,
        boxShadow: selected
          ? `0 0 0 2px ${color}40, 0 8px 32px rgba(0,0,0,0.4)`
          : '0 4px 16px rgba(0,0,0,0.3)',
        transition: 'all 0.2s',
        overflow: 'visible',
        position: 'relative',
        opacity: isDisabled ? 0.4 : 1,
        filter: isDisabled ? 'saturate(0.85)' : 'none',
        ...(isRunning ? {
          boxShadow: `0 0 0 2px ${color}80, 0 0 20px ${color}30`,
        } : {}),
      }}
    >
      {/* Category accent bar */}
      <div style={{
        position: 'absolute', top: 0, left: 0, right: 0,
        height: 3, background: color, borderRadius: '12px 12px 0 0',
      }} />

      {/* Status ring (running animation) */}
      {isRunning && (
        <div style={{
          position: 'absolute', inset: -3,
          borderRadius: 14,
          border: `2px solid ${color}`,
          animation: 'pulse 1.5s infinite',
          pointerEvents: 'none',
        }} />
      )}

      {/* Input handle(s) */}
      {inputs > 0 && (
        <Handle
          type="target"
          position={Position.Left}
          id="input"
          style={{
            background: color, border: `2px solid var(--app-card-bg)`,
            width: 12, height: 12, left: -6,
          }}
        />
      )}

      {/* Node Body */}
      <div style={{ padding: '12px 14px 10px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {/* Icon */}
          <div style={{
            width: 32, height: 32,
            background: bgColor,
            border: `1px solid ${color}30`,
            borderRadius: 8,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: 16, flexShrink: 0,
            fontFamily: 'system-ui',
          }}>
            {icon}
          </div>
          {/* Label + status */}
          <div style={{ flex: 1, minWidth: 0 }}>
            <Text
              ellipsis
              style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 13, display: 'block', lineHeight: 1.3 }}
            >
              {label}
            </Text>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              {category}
            </Text>
            {isDisabled && (
              <div style={{ marginTop: 4 }}>
                <Tag
                  style={{
                    marginInlineEnd: 0,
                    background: '#64748b1a',
                    border: '1px solid #64748b40',
                    color: '#cbd5e1',
                    borderRadius: 4,
                    fontSize: 10,
                    padding: '0 4px',
                  }}
                >
                  Disabled
                </Tag>
              </div>
            )}
            {(status === 'running' || status === 'error') && (
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
              ? <PauseCircleFilled style={{ color: '#94a3b8', fontSize: 12 }} />
              : statusIcon[status]}
          </div>
        </div>

        {/* Execution stats */}
        {(executionRows !== undefined || executionProcessedRows !== undefined || executionValidatedRows !== undefined) && (
          <div style={{
            marginTop: 8,
            padding: '4px 8px',
            background: `${color}10`,
            borderRadius: 6,
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: 8,
          }}>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Rows out</Text>
              <Text style={{ color: color, fontWeight: 600, fontSize: 11 }}>
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
        {(status !== 'idle' && executionDurationLabel) && (
          <div style={{ marginTop: 6, display: 'flex', justifyContent: 'flex-end' }}>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 10 }}>
              time {executionDurationLabel}
            </Text>
          </div>
        )}
      </div>

      {/* Output handle(s) */}
      {outputs > 0 && (
        <Handle
          type="source"
          position={Position.Right}
          id="output"
          style={{
            background: color, border: `2px solid var(--app-card-bg)`,
            width: 12, height: 12, right: -6,
          }}
        />
      )}
      {/* Second output (for condition nodes) */}
      {outputs > 1 && (
        <Handle
          type="source"
          position={Position.Right}
          id="output_false"
          style={{
            background: '#ef4444', border: `2px solid var(--app-card-bg)`,
            width: 12, height: 12, right: -6, top: '70%',
          }}
        />
      )}

      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50% { opacity: 0.5; transform: scale(1.02); }
        }
      `}</style>
    </div>
  )
})

ETLNode.displayName = 'ETLNode'
