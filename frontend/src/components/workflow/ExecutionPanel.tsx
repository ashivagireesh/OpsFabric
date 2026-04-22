import { useRef, useEffect, useState, useMemo } from 'react'
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

export default function ExecutionPanel() {
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
  const [localWriteStatus, setLocalWriteStatus] = useState<Record<string, 'writing' | 'done' | 'error'>>({})

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
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

  if (!showLogs) return null

  const panelHeight = isOfflineError ? 148 : outputFiles.length > 0 ? 268 : 240

  return (
    <div style={{
      height: panelHeight,
      background: 'var(--app-shell-bg-2)',
      borderTop: '1px solid var(--app-border)',
      display: 'flex',
      flexDirection: 'column',
      transition: 'height 0.2s',
      flexShrink: 0,
    }}>

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
            ? <LoadingOutlined spin style={{ color: '#6366f1', fontSize: 12, flexShrink: 0 }} />
            : isSuccess
              ? <CheckCircleFilled style={{ color: '#22c55e', fontSize: 12, flexShrink: 0 }} />
              : hasError
                ? <CloseCircleFilled style={{ color: '#ef4444', fontSize: 12, flexShrink: 0 }} />
                : null}

          <Text style={{ color: 'var(--app-text-muted)', fontSize: 12, fontWeight: 600, whiteSpace: 'nowrap', flexShrink: 0 }}>
            {isExecuting ? (executionAbortRequested ? 'Aborting Pipeline…' : 'Executing Pipeline…') : 'Execution Output'}
          </Text>

          {isSuccess && (
            <Tag style={{ background: '#22c55e15', border: '1px solid #22c55e30', color: '#22c55e', borderRadius: 4, fontSize: 11, flexShrink: 0, margin: 0 }}>
              SUCCESS · {totalRows.toLocaleString()} rows
            </Tag>
          )}
          {hasError && !isOfflineError && (
            <Tag style={{ background: '#ef444415', border: '1px solid #ef444430', color: '#ef4444', borderRadius: 4, fontSize: 11, flexShrink: 0, margin: 0 }}>
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
              const color  = writeStatus === 'done' ? '#22c55e' : writeStatus === 'error' ? '#ef4444' : writeStatus === 'writing' ? '#6366f1' : '#f59e0b'
              const bgCol  = color + '15'
              const border = `1px solid ${color}40`
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
                style={{ background: '#6366f115', border: '1px solid #6366f130', color: '#6366f1', fontSize: 11, height: 22 }}
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
                  background: '#0a0a10', border: '1px solid var(--app-border-strong)', borderRadius: 6,
                  padding: '6px 12px', fontFamily: 'monospace', fontSize: 12, color: '#22c55e',
                  display: 'inline-block',
                }}>
                  <CodeOutlined style={{ marginRight: 8 }} />
                  cd backend &amp;&amp; source venv/bin/activate &amp;&amp; python main.py
                </div>
              </Space>
            }
            style={{ background: '#f59e0b08', border: '1px solid #f59e0b30', borderRadius: 8 }}
          />
        </div>

      ) : (
        /* ── Log stream ─────────────────────────────────────────────────────── */
        <div style={{ flex: 1, overflowY: 'auto', padding: '4px 0', fontFamily: 'monospace', fontSize: 12 }}>
          {executionLogs.length === 0 && isExecuting && (
            <div style={{ padding: '10px 16px' }}>
              <LoadingOutlined spin style={{ marginRight: 8, color: '#6366f1' }} />
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
                  log.status === 'error'   ? '#ef444450' :
                  log.status === 'success' ? '#22c55e40' : '#6366f140'
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
                color: log.status === 'error' ? '#ef4444' : log.status === 'success' ? '#22c55e' : '#6366f1',
              }}>
                {log.status === 'success' ? '✓' : log.status === 'error' ? '✗' : '⟳'}
              </span>
              <span style={{ color: log.status === 'error' ? '#fca5a5' : 'var(--app-text-muted)', flex: 1, wordBreak: 'break-all' }}>
                {log.message}
              </span>
              {log.rows > 0 && (
                <span style={{ color: '#6366f1', fontSize: 11, flexShrink: 0 }}>
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
              Total rows processed: <span style={{ color: '#22c55e' }}>{totalRows.toLocaleString()}</span>
              &nbsp;·&nbsp; Nodes completed: <span style={{ color: '#6366f1' }}>
                {executionLogs.filter(l => l.status === 'success').length}
              </span>
              {outputFiles.length > 0 && (
                <span>
                  &nbsp;·&nbsp;
                  {Object.values(localWriteStatus).some(s => s === 'done')
                    ? <span style={{ color: '#22c55e' }}>✓ saved to local folder</span>
                    : <span style={{ color: '#f59e0b' }}>{outputFiles.length} file{outputFiles.length > 1 ? 's' : ''} on server (↑ click to download)</span>
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
