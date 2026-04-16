import { memo } from 'react'
import { Handle, Position } from 'reactflow'
import { Typography, Tooltip } from 'antd'
import {
  LoadingOutlined, CheckCircleFilled, CloseCircleFilled,
  DeleteOutlined, CopyOutlined
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

export const ETLNode = memo(({ id, data, selected }: ETLNodeProps) => {
  const { definition, label, status = 'idle', executionRows } = data
  if (!definition) return null

  const { color, bgColor, icon, inputs, outputs, category } = definition
  const ringColor = statusRing[status]
  const isRunning = status === 'running'

  return (
    <div
      style={{
        minWidth: 180,
        maxWidth: 220,
        background: 'var(--app-card-bg)',
        border: `1px solid ${selected ? color : 'var(--app-border-strong)'}`,
        borderRadius: 12,
        boxShadow: selected
          ? `0 0 0 2px ${color}40, 0 8px 32px rgba(0,0,0,0.4)`
          : '0 4px 16px rgba(0,0,0,0.3)',
        transition: 'all 0.2s',
        overflow: 'visible',
        position: 'relative',
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
          </div>
          {/* Status icon */}
          <div style={{ flexShrink: 0 }}>
            {statusIcon[status]}
          </div>
        </div>

        {/* Execution stats */}
        {executionRows !== undefined && (
          <div style={{
            marginTop: 8,
            padding: '4px 8px',
            background: `${color}10`,
            borderRadius: 6,
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Rows out</Text>
            <Text style={{ color: color, fontWeight: 600, fontSize: 11 }}>
              {executionRows.toLocaleString()}
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
