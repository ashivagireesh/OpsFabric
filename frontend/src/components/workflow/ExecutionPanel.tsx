import { useRef, useEffect, useState, useMemo, useCallback } from 'react'
import type { MouseEvent as ReactMouseEvent } from 'react'
import { Typography, Space, Tag, Button, Alert } from 'antd'
import {
  CloseOutlined, LoadingOutlined, CheckCircleFilled, CloseCircleFilled,
  CodeOutlined, DownloadOutlined, FolderOpenOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import { useWorkflowStore } from '../../store'
import api from '../../api/client'
import { destDirHandles, destFilenames } from './FilePicker'

const { Text } = Typography

const runColor = 'var(--app-accent)'
const runBg = 'var(--app-accent-soft)'
const runBorder = 'var(--app-accent-border)'
const successColor = '#89d185'
const successBg = 'rgba(137, 209, 133, 0.14)'
const successBorder = 'rgba(137, 209, 133, 0.34)'
const errorColor = '#f48771'
const errorBg = 'rgba(244, 135, 113, 0.14)'
const errorBorder = 'rgba(244, 135, 113, 0.4)'
const warningColor = '#d7ba7d'
const warningBg = 'rgba(215, 186, 125, 0.12)'
const warningBorder = 'rgba(215, 186, 125, 0.34)'

interface ExecutionPanelProps {
  height?: number
  onHeightChange?: (height: number) => void
}

export default function ExecutionPanel({ height, onHeightChange }: ExecutionPanelProps) {
  const {
    executionLogs,
    isExecuting,
    executionAbortRequested,
    abortExecution,
    setShowLogs,
    showLogs,
    nodes,
  } = useWorkflowStore()
  const bottomRef = useRef<HTMLDivElement>(null)
  const resizeStartRef = useRef<{ startY: number; startHeight: number } | null>(null)
  const [localWriteStatus, setLocalWriteStatus] = useState<Record<string, 'writing' | 'done' | 'error'>>({})

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: executionLogs.length > 180 ? 'auto' : 'smooth' })
  }, [executionLogs])

  const hasError       = executionLogs.some(l => l.status === 'error')
  const isOfflineError = executionLogs.some(l => l.nodeId === 'system' && l.status === 'error')
  const isSuccess      = !isExecuting && executionLogs.length > 0 && !hasError
  const totalRows      = executionLogs.reduce((s, l) => s + (l.rows || 0), 0)

  const nodeOutputPathById = useMemo<Record<string, string>>(() => {
    if (!isSuccess) return {}
    const mapping: Record<string, string> = {}
    executionLogs.forEach((log: any) => {
      if (!log?.nodeId) return
      if (typeof log.output_path === 'string' && log.output_path.trim()) {
        mapping[String(log.nodeId)] = String(log.output_path)
        return
      }
      const match = String(log.message || '').match(/written:\s+(.+?)\s+\(/)
      if (match?.[1]) {
        mapping[String(log.nodeId)] = String(match[1])
      }
    })
    return mapping
  }, [isSuccess, executionLogs])

  // Collect output file paths from destination logs
  const outputFiles = useMemo<string[]>(() => {
    if (!isSuccess) return []
    const files: string[] = []
    Object.values(nodeOutputPathById).forEach((path) => {
      if (!files.includes(path)) files.push(path)
    })
    return files
  }, [isSuccess, nodeOutputPathById])

  // Auto-write to picked local folder when execution completes
  useEffect(() => {
    if (!isSuccess || outputFiles.length === 0) return
    const destNodes = nodes.filter(n => destDirHandles.has(n.id))
    destNodes.forEach(async (node) => {
      const dirHandle = destDirHandles.get(node.id)
      const filename  = destFilenames.get(node.id)
      if (!dirHandle || !filename) return
      const serverPath = nodeOutputPathById[node.id] || outputFiles.find(p => p.endsWith(filename))
      if (!serverPath) return

      setLocalWriteStatus(s => ({ ...s, [node.id]: 'writing' }))
      try {
        const BASE = import.meta.env.DEV ? 'http://localhost:8001' : ''
        const resp = await fetch(`${BASE}/api/download?path=${encodeURIComponent(serverPath)}`)
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
        const blob = await resp.blob()
        const fh = await dirHandle.getFileHandle(filename, { create: true })
        const writable = await (fh as any).createWritable()
        await writable.write(blob)
        await writable.close()
        setLocalWriteStatus(s => ({ ...s, [node.id]: 'done' }))
      } catch (err) {
        console.error('Local folder write failed:', err)
        setLocalWriteStatus(s => ({ ...s, [node.id]: 'error' }))
      }
    })
  }, [isSuccess, outputFiles, nodes, nodeOutputPathById])

  const naturalPanelHeight = isOfflineError ? 148 : outputFiles.length > 0 ? 268 : 240
  const panelHeight = height ?? naturalPanelHeight

  const startResize = useCallback((event: ReactMouseEvent<HTMLDivElement>) => {
    if (!onHeightChange) return
    event.preventDefault()
    resizeStartRef.current = { startY: event.clientY, startHeight: panelHeight }

    const handleMove = (moveEvent: MouseEvent) => {
      const start = resizeStartRef.current
      if (!start) return
      const nextHeight = Math.max(120, Math.min(520, start.startHeight + (start.startY - moveEvent.clientY)))
      onHeightChange(nextHeight)
    }

    const handleUp = () => {
      resizeStartRef.current = null
      window.removeEventListener('mousemove', handleMove)
      window.removeEventListener('mouseup', handleUp)
    }

    window.addEventListener('mousemove', handleMove)
    window.addEventListener('mouseup', handleUp)
  }, [onHeightChange, panelHeight])

  if (!showLogs) return null

  return (
    <div style={{
      height: panelHeight,
      background: 'var(--app-shell-bg-2)',
      borderTop: '1px solid var(--app-border)',
      display: 'flex',
      flexDirection: 'column',
      transition: 'height 0.2s',
      flexShrink: 0,
      position: 'relative',
    }}>
      {onHeightChange ? (
        <div
          role="separator"
          aria-label="Resize execution output"
          title="Drag to resize Execution Output"
          onMouseDown={startResize}
          style={{
            position: 'absolute',
            top: -4,
            left: 0,
            right: 0,
            height: 8,
            cursor: 'ns-resize',
            zIndex: 2,
          }}
        >
          <div style={{
            width: 48,
            height: 3,
            borderRadius: 999,
            background: 'var(--app-border-strong)',
            margin: '2px auto 0',
            opacity: 0.85,
          }} />
        </div>
      ) : null}

      {/* ── Header row ───────────────────────────────────────────────────────── */}
      <div style={{
        padding: '6px 14px',
        borderBottom: '1px solid var(--app-card-bg)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        background: 'var(--app-panel-bg)',
        flexShrink: 0,
        minHeight: 36,
      }}>
        <Space size={6} style={{ flexWrap: 'nowrap', overflow: 'hidden' }}>
          {isExecuting
            ? <LoadingOutlined spin style={{ color: runColor, fontSize: 12, flexShrink: 0 }} />
            : isSuccess
              ? <CheckCircleFilled style={{ color: successColor, fontSize: 12, flexShrink: 0 }} />
              : hasError
                ? <CloseCircleFilled style={{ color: errorColor, fontSize: 12, flexShrink: 0 }} />
                : null}

          <Text style={{ color: 'var(--app-text-muted)', fontSize: 12, fontWeight: 600, whiteSpace: 'nowrap', flexShrink: 0 }}>
            {isExecuting ? (executionAbortRequested ? 'Aborting Pipeline…' : 'Executing Pipeline…') : 'Execution Output'}
          </Text>

          {isSuccess && (
            <Tag style={{ background: successBg, border: `1px solid ${successBorder}`, color: successColor, borderRadius: 4, fontSize: 11, flexShrink: 0, margin: 0 }}>
              SUCCESS · {totalRows.toLocaleString()} rows
            </Tag>
          )}
          {hasError && !isOfflineError && (
            <Tag style={{ background: errorBg, border: `1px solid ${errorBorder}`, color: errorColor, borderRadius: 4, fontSize: 11, flexShrink: 0, margin: 0 }}>
              FAILED
            </Tag>
          )}
        </Space>

        <Space size={6}>
          {isExecuting ? (
            <Button
              size="small"
              danger
              loading={executionAbortRequested}
              onClick={() => { void abortExecution() }}
            >
              {executionAbortRequested ? 'Aborting…' : 'Abort'}
            </Button>
          ) : null}
          <Button
            type="text"
            icon={<CloseOutlined />}
            size="small"
            style={{ color: 'var(--app-text-dim)', flexShrink: 0 }}
            onClick={() => setShowLogs(false)}
          />
        </Space>
      </div>

      {/* ── Output files bar (shown only when files were written) ────────────── */}
      {outputFiles.length > 0 && (
        <div style={{
          padding: '5px 14px',
          borderBottom: '1px solid var(--app-input-bg)',
          background: 'var(--app-panel-2)',
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          flexShrink: 0,
          flexWrap: 'wrap',
        }}>
          <Text style={{ color: 'var(--app-text-dim)', fontSize: 11, flexShrink: 0 }}>Output:</Text>
          {outputFiles.map((path, i) => {
            const filename = path.split('/').pop() || 'output'
            const nodeWithHandle = (
              nodes.find((n) => nodeOutputPathById[n.id] === path && destDirHandles.has(n.id))
              || nodes.find((n) => destDirHandles.has(n.id) && destFilenames.get(n.id) === filename)
            )
            const folderName  = nodeWithHandle ? destDirHandles.get(nodeWithHandle.id)?.name : null
            const writeStatus = nodeWithHandle ? localWriteStatus[nodeWithHandle.id] : undefined

            if (folderName) {
              // Local folder write mode
              const color = writeStatus === 'done'
                ? successColor
                : writeStatus === 'error'
                  ? errorColor
                  : writeStatus === 'writing'
                    ? runColor
                    : warningColor
              const bgCol = writeStatus === 'done'
                ? successBg
                : writeStatus === 'error'
                  ? errorBg
                  : writeStatus === 'writing'
                    ? runBg
                    : warningBg
              const border = writeStatus === 'done'
                ? `1px solid ${successBorder}`
                : writeStatus === 'error'
                  ? `1px solid ${errorBorder}`
                  : writeStatus === 'writing'
                    ? `1px solid ${runBorder}`
                    : `1px solid ${warningBorder}`
              return (
                <Tag key={i} style={{ background: bgCol, border, color, borderRadius: 4, fontSize: 11, margin: 0 }}>
                  {writeStatus === 'writing' && <LoadingOutlined spin style={{ marginRight: 5 }} />}
                  {writeStatus === 'done'    && <CheckCircleFilled style={{ marginRight: 5 }} />}
                  {!writeStatus && <FolderOpenOutlined style={{ marginRight: 5 }} />}
                  {writeStatus === 'done'
                    ? `Saved → ${folderName}/${filename}`
                    : writeStatus === 'writing'
                    ? `Saving to ${folderName}/…`
                    : writeStatus === 'error'
                    ? <>Write failed — <span style={{ cursor: 'pointer', textDecoration: 'underline' }} onClick={() => api.downloadOutputFile(path)}>download</span></>
                    : `${folderName}/${filename}`}
                </Tag>
              )
            }

            // Server path — show download button
            return (
              <Button
                key={i}
                size="small"
                icon={<DownloadOutlined />}
                style={{ background: runBg, border: `1px solid ${runBorder}`, color: runColor, fontSize: 11, height: 22 }}
                onClick={() => api.downloadOutputFile(path)}
              >
                {filename.length > 30 ? filename.slice(0, 28) + '…' : filename}
              </Button>
            )
          })}
        </div>
      )}

      {/* ── Backend offline notice ──────────────────────────────────────────── */}
      {isOfflineError ? (
        <div style={{ padding: '12px 16px' }}>
          <Alert
            type="warning"
            showIcon
            message={<Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>Python Backend Required</Text>}
            description={
              <Space direction="vertical" size={4}>
                <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>
                  Real ETL operations require the Python backend. Start it with:
                </Text>
                <div style={{
                  background: 'var(--app-shell-bg-2)', border: '1px solid var(--app-border-strong)', borderRadius: 6,
                  padding: '6px 12px', fontFamily: 'monospace', fontSize: 12, color: successColor,
                  display: 'inline-block',
                }}>
                  <CodeOutlined style={{ marginRight: 8 }} />
                  cd backend &amp;&amp; source venv/bin/activate &amp;&amp; python main.py
                </div>
              </Space>
            }
            style={{ background: warningBg, border: `1px solid ${warningBorder}`, borderRadius: 8 }}
          />
        </div>

      ) : (
        /* ── Log stream ─────────────────────────────────────────────────────── */
        <div style={{ flex: 1, overflowY: 'auto', padding: '4px 0', fontFamily: 'monospace', fontSize: 12 }}>
          {executionLogs.length === 0 && isExecuting && (
            <div style={{ padding: '10px 16px' }}>
              <LoadingOutlined spin style={{ marginRight: 8, color: runColor }} />
              <span style={{ color: 'var(--app-text-dim)' }}>Connecting to ETL engine…</span>
            </div>
          )}

          {executionLogs.map((log, i) => (
            <div
              key={i}
              style={{
                padding: '3px 16px',
                display: 'flex',
                alignItems: 'baseline',
                gap: 8,
                borderLeft: `2px solid ${
                  log.status === 'error'   ? errorBorder :
                  log.status === 'success' ? successBorder : runBorder
                }`,
                marginLeft: 8,
                marginBottom: 1,
              }}
            >
              <span style={{ color: 'var(--app-text-faint)', flexShrink: 0, minWidth: 80 }}>
                {dayjs(log.timestamp).format('HH:mm:ss.SSS')}
              </span>
              <span style={{
                flexShrink: 0, width: 14,
                color: log.status === 'error' ? errorColor : log.status === 'success' ? successColor : runColor,
              }}>
                {log.status === 'success' ? '✓' : log.status === 'error' ? '✗' : '⟳'}
              </span>
              <span style={{ color: log.status === 'error' ? '#ffb4a8' : 'var(--app-text-muted)', flex: 1, wordBreak: 'break-all' }}>
                {log.message}
              </span>
              {log.rows > 0 && (
                <span style={{ color: runColor, fontSize: 11, flexShrink: 0 }}>
                  {log.rows.toLocaleString()} rows
                </span>
              )}
            </div>
          ))}

          {/* ── Summary footer ── */}
          {!isExecuting && executionLogs.length > 0 && !hasError && (
            <div style={{
              padding: '5px 16px',
              borderTop: '1px solid var(--app-input-bg)',
              marginTop: 2,
              color: 'var(--app-text-faint)',
              fontSize: 11,
            }}>
              Total rows processed: <span style={{ color: successColor }}>{totalRows.toLocaleString()}</span>
              &nbsp;·&nbsp; Nodes completed: <span style={{ color: runColor }}>
                {executionLogs.filter(l => l.status === 'success').length}
              </span>
              {outputFiles.length > 0 && (
                <span>
                  &nbsp;·&nbsp;
                  {Object.values(localWriteStatus).some(s => s === 'done')
                    ? <span style={{ color: successColor }}>✓ saved to local folder</span>
                    : <span style={{ color: warningColor }}>{outputFiles.length} file{outputFiles.length > 1 ? 's' : ''} on server (↑ click to download)</span>
                  }
                </span>
              )}
            </div>
          )}

          <div ref={bottomRef} />
        </div>
      )}
    </div>
  )
}
