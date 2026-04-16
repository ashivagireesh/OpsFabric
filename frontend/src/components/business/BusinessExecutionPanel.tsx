import { useEffect, useRef } from 'react'
import { Button, Space, Tag, Typography } from 'antd'
import {
  CheckCircleFilled,
  CloseCircleFilled,
  CloseOutlined,
  LoadingOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import { useBusinessWorkflowStore } from '../../store/businessStore'

const { Text } = Typography

function formatMetricValue(value: unknown): string {
  if (typeof value === 'number') {
    return Number.isInteger(value) ? value.toString() : value.toFixed(3)
  }
  if (value === null || value === undefined) return '-'
  return String(value)
}

export default function BusinessExecutionPanel() {
  const { runLogs, isExecuting, showLogs, setShowLogs, runMetrics } = useBusinessWorkflowStore()
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [runLogs])

  if (!showLogs) return null

  const hasError = runLogs.some((log) => log.status === 'error')
  const isSuccess = !isExecuting && runLogs.length > 0 && !hasError
  const totalRows = runLogs.reduce((sum, log) => sum + (log.rows || 0), 0)

  const visibleMetrics = Object.entries(runMetrics || {})
    .filter(([, value]) => value !== null && value !== undefined && value !== '')
    .slice(0, 8)

  return (
    <div
      style={{
        height: 260,
        background: 'var(--app-shell-bg-2)',
        borderTop: '1px solid var(--app-border)',
        display: 'flex',
        flexDirection: 'column',
        flexShrink: 0,
      }}
    >
      <div
        style={{
          padding: '6px 14px',
          borderBottom: '1px solid var(--app-card-bg)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          background: 'var(--app-panel-bg)',
          flexShrink: 0,
          minHeight: 36,
        }}
      >
        <Space size={6} style={{ flexWrap: 'nowrap', overflow: 'hidden' }}>
          {isExecuting
            ? <LoadingOutlined spin style={{ color: '#f59e0b', fontSize: 12, flexShrink: 0 }} />
            : isSuccess
            ? <CheckCircleFilled style={{ color: '#22c55e', fontSize: 12, flexShrink: 0 }} />
            : hasError
            ? <CloseCircleFilled style={{ color: '#ef4444', fontSize: 12, flexShrink: 0 }} />
            : null}

          <Text style={{ color: 'var(--app-text-muted)', fontSize: 12, fontWeight: 600, whiteSpace: 'nowrap', flexShrink: 0 }}>
            {isExecuting ? 'Running business workflow…' : 'Business Workflow Output'}
          </Text>

          {isSuccess && (
            <Tag style={{ background: '#22c55e15', border: '1px solid #22c55e30', color: '#22c55e', borderRadius: 4, fontSize: 11, flexShrink: 0, margin: 0 }}>
              SUCCESS · {totalRows.toLocaleString()} rows
            </Tag>
          )}
          {hasError && (
            <Tag style={{ background: '#ef444415', border: '1px solid #ef444430', color: '#ef4444', borderRadius: 4, fontSize: 11, flexShrink: 0, margin: 0 }}>
              FAILED
            </Tag>
          )}
        </Space>

        <Button
          type="text"
          icon={<CloseOutlined />}
          size="small"
          style={{ color: 'var(--app-text-dim)', flexShrink: 0 }}
          onClick={() => setShowLogs(false)}
        />
      </div>

      {visibleMetrics.length > 0 && (
        <div
          style={{
            padding: '6px 14px',
            borderBottom: '1px solid var(--app-input-bg)',
            background: 'var(--app-panel-2)',
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            flexWrap: 'wrap',
            flexShrink: 0,
          }}
        >
          <Text style={{ color: 'var(--app-text-dim)', fontSize: 11, flexShrink: 0 }}>Metrics:</Text>
          {visibleMetrics.map(([key, value]) => (
            <Tag key={key} style={{ background: '#f59e0b15', border: '1px solid #f59e0b35', color: '#fbbf24', borderRadius: 4, fontSize: 11, margin: 0 }}>
              {key}: {formatMetricValue(value)}
            </Tag>
          ))}
        </div>
      )}

      <div style={{ flex: 1, overflowY: 'auto', padding: '4px 0', fontFamily: 'monospace', fontSize: 12 }}>
        {runLogs.length === 0 && isExecuting && (
          <div style={{ padding: '10px 16px' }}>
            <LoadingOutlined spin style={{ marginRight: 8, color: '#f59e0b' }} />
            <span style={{ color: 'var(--app-text-dim)' }}>Preparing business workflow execution…</span>
          </div>
        )}

        {runLogs.map((log, idx) => (
          <div
            key={idx}
            style={{
              padding: '3px 16px',
              display: 'flex',
              alignItems: 'baseline',
              gap: 8,
              borderLeft: `2px solid ${
                log.status === 'error' ? '#ef444450' :
                log.status === 'success' ? '#22c55e40' : '#f59e0b40'
              }`,
              marginLeft: 8,
              marginBottom: 1,
            }}
          >
            <span style={{ color: 'var(--app-text-faint)', flexShrink: 0, minWidth: 80 }}>
              {dayjs(log.timestamp).format('HH:mm:ss.SSS')}
            </span>
            <span
              style={{
                flexShrink: 0,
                width: 14,
                color: log.status === 'error' ? '#ef4444' : log.status === 'success' ? '#22c55e' : '#f59e0b',
              }}
            >
              {log.status === 'success' ? '✓' : log.status === 'error' ? '✗' : '⏳'}
            </span>
            <span style={{ color: log.status === 'error' ? '#fca5a5' : 'var(--app-text-muted)', flex: 1, wordBreak: 'break-all' }}>
              {log.message}
            </span>
            {log.rows > 0 && (
              <span style={{ color: '#f59e0b', fontSize: 11, flexShrink: 0 }}>
                {log.rows.toLocaleString()} rows
              </span>
            )}
          </div>
        ))}

        <div ref={bottomRef} />
      </div>
    </div>
  )
}
