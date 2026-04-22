import { useEffect, useRef, useState } from 'react'
import { Button, Space, Typography, Tag, Table, Spin, Alert, Input, Tooltip, Progress } from 'antd'
import {
  FolderOpenOutlined, FileTextOutlined, CheckCircleFilled,
  LoadingOutlined, CloseCircleOutlined, ReloadOutlined, DownloadOutlined
} from '@ant-design/icons'
import Papa from 'papaparse'
import axios from 'axios'

const { Text } = Typography
const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8001'
const UPLOAD_TIMEOUT_MS = 10 * 60 * 1000

// ─── Global registry for picked folder handles (not serialisable → lives outside React) ──
export const destDirHandles: Map<string, FileSystemDirectoryHandle> = new Map()
export const destFilenames:  Map<string, string>                    = new Map()

const ACCEPTED_TYPES: Record<string, string> = {
  csv: '.csv',
  excel: '.xlsx,.xls',
  json: '.json',
  xml: '.xml',
  parquet: '.parquet',
  all: '.csv,.xlsx,.xls,.json,.xml,.parquet,.tsv',
}

interface FileInfo {
  name: string
  size: number
  rows: number
  columns: string[]
  preview: Record<string, unknown>[]
  serverPath?: string
  localFile?: File
  detectedEncoding?: string
  jsonPaths?: Array<{ value: string; label: string; kind?: string; count?: number }>
  suggestedJsonPath?: string
}

interface FilePickerProps {
  nodeId?: string
  value?: string
  onChange?: (path: string, file?: FileInfo) => void
  accept?: string
  placeholder?: string
  mode?: 'source' | 'destination'
  fileType?: keyof typeof ACCEPTED_TYPES
}

function basenameFromPath(rawPath: string): string {
  const cleaned = String(rawPath || '')
    .replace(/^local:\/\//, '')
    .replace(/^__local__:\/\//, '')
  const token = cleaned.split(/[\\/]/).pop() || cleaned
  const decoded = decodeURIComponent(token)
  return decoded.trim() || 'selected_file'
}

// ─── Source file picker ───────────────────────────────────────────────────────
export function SourceFilePicker({ nodeId, value, onChange, fileType = 'all', placeholder }: FilePickerProps) {
  const inputRef = useRef<HTMLInputElement>(null)
  const [uploading, setUploading] = useState(false)
  const [uploadProgress, setUploadProgress] = useState(0)
  const [fileInfo, setFileInfo] = useState<FileInfo | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [backendAvailable, setBackendAvailable] = useState<boolean | null>(null)

  useEffect(() => {
    setError(null)
    setUploadProgress(0)
    setBackendAvailable(null)
    const currentPath = String(value || '').trim()
    if (!currentPath) {
      setFileInfo(null)
      return
    }
    setFileInfo((prev) => {
      if (prev && String(prev.serverPath || '') === currentPath) return prev
      return {
        name: basenameFromPath(currentPath),
        size: 0,
        rows: 0,
        columns: [],
        preview: [],
        serverPath: currentPath,
      }
    })
  }, [nodeId, value])

  const parseLocalCSV = (file: File): Promise<{ rows: number; columns: string[]; preview: Record<string, unknown>[] }> =>
    new Promise((resolve) => {
      Papa.parse(file, {
        header: true,
        preview: 10,
        skipEmptyLines: true,
        complete: (results) => {
          resolve({
            rows: 0, // unknown until full parse
            columns: results.meta.fields || [],
            preview: results.data as Record<string, unknown>[]
          })
        },
        error: () => resolve({ rows: 0, columns: [], preview: [] })
      })
    })

  const getFullRowCount = (file: File): Promise<number> =>
    new Promise((resolve) => {
      Papa.parse(file, {
        header: true,
        skipEmptyLines: true,
        complete: (r) => resolve(r.data.length),
        error: () => resolve(0)
      })
    })

  const handleFileSelect = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    setError(null)
    setUploading(true)
    setUploadProgress(0)

    try {
      // Always parse locally for immediate preview
      const local = await parseLocalCSV(file)
      setUploadProgress(30)

      // Try to upload to backend for server-side path
      let serverPath = value || ''
      try {
        const formData = new FormData()
        formData.append('file', file)
        const resp = await axios.post(`${API_BASE}/api/upload`, formData, {
          headers: { 'Content-Type': 'multipart/form-data' },
          onUploadProgress: (e) => setUploadProgress(30 + Math.round((e.loaded / (e.total || 1)) * 50)),
          timeout: UPLOAD_TIMEOUT_MS,
          maxBodyLength: Infinity,
          maxContentLength: Infinity,
        })
        serverPath = resp.data.tmp_path
        const serverRows = resp.data.rows
        setBackendAvailable(true)
        setUploadProgress(100)

        const info: FileInfo = {
          name: file.name,
          size: file.size,
          rows: serverRows,
          columns: resp.data.columns,
          preview: resp.data.preview,
          serverPath,
          localFile: file,
          detectedEncoding: typeof resp.data.encoding === 'string' ? resp.data.encoding : undefined,
          jsonPaths: Array.isArray(resp.data.json_paths) ? resp.data.json_paths : undefined,
          suggestedJsonPath: typeof resp.data.suggested_json_path === 'string' ? resp.data.suggested_json_path : undefined,
        }
        setFileInfo(info)
        onChange?.(serverPath, info)
      } catch (uploadErr: any) {
        // Backend offline — use local parse only
        setBackendAvailable(false)
        const reason = String(uploadErr?.response?.data?.detail || uploadErr?.message || '').trim()
        setError(
          reason
            ? `Upload to backend failed (${reason}). File is loaded in local preview only.`
            : 'Upload to backend failed. File is loaded in local preview only.'
        )
        setUploadProgress(80)
        const fullRows = await getFullRowCount(file)
        setUploadProgress(100)

        // Store as data URL for client-side processing
        const reader = new FileReader()
        reader.onload = (ev) => {
          const dataUri = ev.target?.result as string
          const info: FileInfo = {
            name: file.name,
            size: file.size,
            rows: fullRows,
            columns: local.columns,
            preview: local.preview,
            serverPath: `local://${file.name}`,
            localFile: file,
          }
          setFileInfo(info)
          onChange?.(`local://${file.name}`, info)
        }
        reader.readAsDataURL(file)
      }
    } catch (err: any) {
      setError(`Failed to process file: ${err.message}`)
    } finally {
      setUploading(false)
      // reset input so same file can be re-selected
      if (inputRef.current) inputRef.current.value = ''
    }
  }

  const fmtSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`
    if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)} KB`
    return `${(bytes / 1048576).toFixed(1)} MB`
  }

  return (
    <div>
      <input
        ref={inputRef}
        type="file"
        accept={ACCEPTED_TYPES[fileType] || ACCEPTED_TYPES.all}
        style={{ display: 'none' }}
        onChange={handleFileSelect}
      />

      {/* Drop zone / picker button */}
      <div
        onClick={() => !uploading && inputRef.current?.click()}
        style={{
          border: `2px dashed ${fileInfo ? '#22c55e40' : 'var(--app-border-strong)'}`,
          borderRadius: 10,
          padding: '14px 16px',
          cursor: uploading ? 'wait' : 'pointer',
          background: fileInfo ? 'rgba(34,197,94,0.05)' : 'var(--app-shell-bg)',
          transition: 'all 0.2s',
          userSelect: 'none',
        }}
        onMouseEnter={e => !uploading && (e.currentTarget.style.borderColor = '#6366f1')}
        onMouseLeave={e => (e.currentTarget.style.borderColor = fileInfo ? '#22c55e40' : 'var(--app-border-strong)')}
      >
        {uploading ? (
          <Space direction="vertical" style={{ width: '100%' }} size={6}>
            <Space>
              <LoadingOutlined spin style={{ color: '#6366f1' }} />
              <Text style={{ color: 'var(--app-text-muted)', fontSize: 12 }}>Processing file…</Text>
            </Space>
            <Progress percent={uploadProgress} strokeColor="#6366f1" trailColor="var(--app-border)" showInfo={false} size="small" />
          </Space>
        ) : fileInfo ? (
          <Space align="start" style={{ width: '100%', justifyContent: 'space-between' }}>
            <Space>
              <FileTextOutlined style={{ color: '#22c55e', fontSize: 18 }} />
              <div>
                <Text style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 13, display: 'block' }}>
                  {fileInfo.name}
                </Text>
                <Space size={6} style={{ marginTop: 2 }}>
                  <Tag style={{ background: '#22c55e15', border: '1px solid #22c55e30', color: '#22c55e', fontSize: 10 }}>
                    {fileInfo.rows.toLocaleString()} rows
                  </Tag>
                  <Tag style={{ background: '#6366f115', border: '1px solid #6366f130', color: '#6366f1', fontSize: 10 }}>
                    {fileInfo.columns.length} columns
                  </Tag>
                  <Tag style={{ background: 'var(--app-border)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text-subtle)', fontSize: 10 }}>
                    {fmtSize(fileInfo.size)}
                  </Tag>
                  {backendAvailable === false && (
                    <Tag style={{ background: '#f59e0b15', border: '1px solid #f59e0b30', color: '#f59e0b', fontSize: 10 }}>
                      local only
                    </Tag>
                  )}
                </Space>
              </div>
            </Space>
            <Tooltip title="Pick different file">
              <Button type="text" icon={<ReloadOutlined />} size="small" style={{ color: 'var(--app-text-dim)' }} onClick={e => { e.stopPropagation(); inputRef.current?.click() }} />
            </Tooltip>
          </Space>
        ) : (
          <Space>
            <FolderOpenOutlined style={{ color: '#6366f1', fontSize: 16 }} />
            <div>
              <Text style={{ color: 'var(--app-text-muted)', fontSize: 13 }}>{placeholder || 'Click to browse file…'}</Text>
              <br />
              <Text style={{ color: 'var(--app-text-dim)', fontSize: 11 }}>
                Supports: {ACCEPTED_TYPES[fileType] || ACCEPTED_TYPES.all}
              </Text>
            </div>
          </Space>
        )}
      </div>

      {/* Error */}
      {error && (
        <Alert type="error" message={error} style={{ marginTop: 8, fontSize: 12 }} showIcon closable onClose={() => setError(null)} />
      )}

      {/* Data Preview Table */}
      {fileInfo?.preview && fileInfo.preview.length > 0 && (
        <div style={{ marginTop: 10 }}>
          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11, display: 'block', marginBottom: 4 }}>
            Preview — first {fileInfo.preview.length} rows
          </Text>
          <div style={{ overflowX: 'auto', border: '1px solid var(--app-border)', borderRadius: 8 }}>
            <Table
              dataSource={fileInfo.preview.map((r, i) => ({ ...r, _key: i }))}
              rowKey="_key"
              columns={fileInfo.columns.slice(0, 8).map(col => ({
                title: <Text style={{ color: 'var(--app-text-muted)', fontSize: 11 }}>{col}</Text>,
                dataIndex: col,
                key: col,
                width: 120,
                ellipsis: true,
                render: (v: unknown) => (
                  <Text style={{ color: '#d1d5db', fontSize: 11 }} ellipsis>
                    {v === null || v === undefined ? <span style={{ color: 'var(--app-text-faint)' }}>null</span> : String(v)}
                  </Text>
                )
              }))}
              pagination={false}
              size="small"
              style={{ background: 'transparent' }}
              scroll={{ x: true }}
            />
          </div>
          {fileInfo.columns.length > 8 && (
            <Text style={{ color: 'var(--app-text-dim)', fontSize: 11 }}>
              +{fileInfo.columns.length - 8} more columns hidden
            </Text>
          )}
        </div>
      )}
    </div>
  )
}

interface DestinationPickerProps extends FilePickerProps {
  nodeId?: string
}

// ─── Destination path picker ──────────────────────────────────────────────────
export function DestinationPathPicker({ value, onChange, placeholder, fileType = 'csv', nodeId }: DestinationPickerProps) {
  const [pickedFolder, setPickedFolder] = useState<string | null>(
    nodeId && destDirHandles.has(nodeId) ? destDirHandles.get(nodeId)!.name : null
  )
  const [pickedFilename, setPickedFilename] = useState<string | null>(
    nodeId && destFilenames.has(nodeId) ? destFilenames.get(nodeId)! : null
  )
  const hasFSAccess = typeof (window as any).showDirectoryPicker === 'function'

  const ext = fileType === 'excel' ? '.xlsx' : fileType === 'json' ? '.json' : '.csv'

  useEffect(() => {
    if (!nodeId) {
      setPickedFolder(null)
      setPickedFilename(null)
      return
    }
    const existingHandle = destDirHandles.get(nodeId)
    const existingFilename = destFilenames.get(nodeId)
    if (existingHandle && existingFilename) {
      setPickedFolder(existingHandle.name)
      setPickedFilename(existingFilename)
      return
    }
    const raw = String(value || '').trim()
    if (raw.startsWith('__local__://')) {
      const relative = raw.replace('__local__://', '')
      const parts = relative.split('/').filter(Boolean)
      if (parts.length >= 2) {
        const filename = parts.pop() || null
        const folder = parts.join('/') || null
        setPickedFolder(folder)
        setPickedFilename(filename)
        return
      }
    }
    setPickedFolder(null)
    setPickedFilename(null)
  }, [nodeId, value])

  const handleFolderPick = async () => {
    try {
      const dirHandle: FileSystemDirectoryHandle = await (window as any).showDirectoryPicker({ mode: 'readwrite' })
      const stamp = new Date().toISOString().replace(/[:.]/g, '-')
      const nodeSuffix = nodeId ? nodeId.slice(0, 8) : Math.random().toString(36).slice(2, 8)
      const fileName = `output_${stamp}_${nodeSuffix}${ext}`

      // Store the handle for post-execution write
      if (nodeId) {
        destDirHandles.set(nodeId, dirHandle)
        destFilenames.set(nodeId, fileName)
      }

      setPickedFolder(dirHandle.name)
      setPickedFilename(fileName)

      // Signal path as a local:// URI — ETL engine will write to outputs/ on server
      // and after execution we copy to the picked local folder
      onChange?.(`__local__://${dirHandle.name}/${fileName}`)
    } catch (e: any) {
      if (e.name !== 'AbortError') console.warn('Folder picker error:', e)
    }
  }

  const handleClear = () => {
    if (nodeId) { destDirHandles.delete(nodeId); destFilenames.delete(nodeId) }
    setPickedFolder(null)
    setPickedFilename(null)
    onChange?.('')
  }

  // ── Picked-folder view ──────────────────────────────────────────────────────
  if (pickedFolder && pickedFilename) {
    return (
      <div>
        <div style={{
          border: '2px solid #22c55e40', borderRadius: 10, padding: '12px 14px',
          background: 'rgba(34,197,94,0.04)', display: 'flex', alignItems: 'center',
          justifyContent: 'space-between',
        }}>
          <Space>
            <CheckCircleFilled style={{ color: '#22c55e', fontSize: 16 }} />
            <div>
              <Text style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 13, display: 'block' }}>
                {pickedFolder}
              </Text>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                Will save as <span style={{ color: '#6366f1' }}>{pickedFilename}</span>
              </Text>
            </div>
          </Space>
          <Tooltip title="Change folder">
            <Button type="text" icon={<ReloadOutlined />} size="small" style={{ color: 'var(--app-text-dim)' }}
              onClick={handleFolderPick} />
          </Tooltip>
        </div>
        <Text style={{ color: '#22c55e', fontSize: 11, display: 'block', marginTop: 4 }}>
          ✓ File will be written directly to your local <strong>{pickedFolder}</strong> folder after execution.
        </Text>
      </div>
    )
  }

  // ── Default view ────────────────────────────────────────────────────────────
  return (
    <div>
      {hasFSAccess && (
        <div
          onClick={handleFolderPick}
          style={{
            border: '2px dashed var(--app-border-strong)', borderRadius: 10, padding: '12px 14px',
            cursor: 'pointer', background: 'var(--app-shell-bg)', marginBottom: 8,
            display: 'flex', alignItems: 'center', gap: 10,
          }}
          onMouseEnter={e => (e.currentTarget.style.borderColor = '#6366f1')}
          onMouseLeave={e => (e.currentTarget.style.borderColor = 'var(--app-border-strong)')}
        >
          <FolderOpenOutlined style={{ color: '#6366f1', fontSize: 18 }} />
          <div>
            <Text style={{ color: 'var(--app-text-muted)', fontSize: 13 }}>Click to pick output folder…</Text>
            <br />
            <Text style={{ color: 'var(--app-text-dim)', fontSize: 11 }}>File will be saved directly to your chosen folder</Text>
          </div>
        </div>
      )}

      <Space.Compact style={{ width: '100%' }}>
        <Input
          value={value?.startsWith('__local__://') ? '' : (value || '')}
          onChange={e => onChange?.(e.target.value)}
          placeholder={placeholder || `/output/result${ext}`}
          prefix={<FileTextOutlined style={{ color: '#6366f1' }} />}
          style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)', flex: 1 }}
        />
      </Space.Compact>
      <Text style={{ color: 'var(--app-text-dim)', fontSize: 11, display: 'block', marginTop: 4 }}>
        {hasFSAccess
          ? 'Or type a full server path (e.g. /Users/you/output.csv)'
          : 'Enter the full server path where the file should be saved.'
        }
      </Text>
    </div>
  )
}

export default SourceFilePicker
