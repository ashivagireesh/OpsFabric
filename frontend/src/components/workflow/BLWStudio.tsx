import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties, type MouseEvent } from 'react'
import {
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  addEdge,
  Handle,
  MarkerType,
  Position,
  useEdgesState,
  useNodesState,
  type Connection,
  type Edge,
  type Node,
  type NodeProps,
  type NodeTypes,
} from 'reactflow'
import ReactECharts from 'echarts-for-react'
import {
  Badge,
  Button,
  Col,
  Divider,
  Form,
  Input,
  InputNumber,
  Modal,
  Row,
  Select,
  Space,
  Switch,
  Table,
  Tabs,
  Tag,
  Typography,
  notification,
} from 'antd'
import {
  ApartmentOutlined,
  BranchesOutlined,
  CheckCircleOutlined,
  CheckOutlined,
  ClockCircleOutlined,
  CopyOutlined,
  DatabaseOutlined,
  DeleteOutlined,
  ForkOutlined,
  MailOutlined,
  MessageOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
  PlusOutlined,
  SaveOutlined,
  StopOutlined,
  SyncOutlined,
  ThunderboltOutlined,
  RedoOutlined,
  UndoOutlined,
  ArrowDownOutlined,
  ArrowUpOutlined,
  WarningOutlined,
  DownloadOutlined,
} from '@ant-design/icons'
import api from '../../api/client'

const { Text } = Typography

const csvEscape = (value: unknown): string => {
  if (value === null || value === undefined) return ''
  const text = typeof value === 'object' ? JSON.stringify(value) : String(value)
  if (/[",\n\r]/.test(text)) return `"${text.replace(/"/g, '""')}"`
  return text
}

const downloadBlwRowsAsCsv = (rows: any[], baseName: string): void => {
  if (!Array.isArray(rows) || rows.length <= 0) return
  const columns = Array.from(rows.reduce((set: Set<string>, row: any) => {
    Object.keys(row || {}).forEach((key) => set.add(key))
    return set
  }, new Set<string>()))
  const csv = [
    columns.map(csvEscape).join(','),
    ...rows.map((row) => columns.map((column) => csvEscape(row?.[column])).join(',')),
  ].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
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

const formatTrackerJson = (value: any): string => {
  if (value === undefined || value === null || value === '') return ''
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!trimmed) return ''
    try {
      return JSON.stringify(JSON.parse(trimmed), null, 2)
    } catch {
      return trimmed
    }
  }
  return JSON.stringify(value, null, 2)
}

const trackerArray = (value: any): any[] => Array.isArray(value) ? value : []

const trackerStatusColor = (status: unknown): string => {
  const norm = String(status || '').trim().toLowerCase()
  if (['success', 'completed', 'complete', 'sent', 'written'].includes(norm)) return '#22c55e'
  if (['running', 'processing', 'in_progress', 'active'].includes(norm)) return '#3b82f6'
  if (['waiting', 'wait', 'queued', 'pending', 'retrying'].includes(norm)) return '#f59e0b'
  if (['failed', 'error', 'invalid'].includes(norm)) return '#ef4444'
  if (['paused', 'terminated', 'cancelled', 'escalated'].includes(norm)) return '#a855f7'
  return '#64748b'
}

const latestTrackerSteps = (value: any): any[] => {
  const byNode = new Map<string, any>()
  trackerArray(value).forEach((row, index) => {
    const nodeId = String(row?.node_id || '').trim()
    if (!nodeId) return
    byNode.set(nodeId, { ...row, _event_index: index })
  })
  return Array.from(byNode.values())
}

const reconciledTrackerSteps = (row: any): any[] => {
  const currentStatus = String(row?.current_status || '').trim().toLowerCase()
  const currentStepId = String(row?.current_step_id || '').trim()
  const currentStage = String(row?.current_stage || '').trim()
  const isTerminal = ['success', 'failed', 'terminated', 'paused', 'escalated'].includes(currentStatus)
  const rows = latestTrackerSteps(row?.step_track_json)
    .filter((step) => !(isTerminal && String(step?.node_id || '') === '__queued__'))
  if (isTerminal) {
    const terminalNodeId = currentStepId || '__workflow__'
    const idx = rows.findIndex((step) => String(step?.node_id || '') === terminalNodeId)
    const terminalStep = {
      node_id: terminalNodeId,
      label: currentStage || 'Workflow Complete',
      status: currentStatus,
      rows: rows.reduce((max, step) => Math.max(max, Number(step?.rows || 0)), 0),
      at: row?.ended_at || row?.last_updated_at || '',
      error: row?.error_message || '',
    }
    if (idx >= 0) rows[idx] = { ...rows[idx], ...terminalStep }
    else rows.push(terminalStep)
  } else if (currentStepId) {
    const idx = rows.findIndex((step) => String(step?.node_id || '') === currentStepId)
    if (idx >= 0) rows[idx] = { ...rows[idx], status: currentStatus || rows[idx].status, at: rows[idx].at || row?.last_updated_at || '' }
  }
  return rows
}

type BlwNodeKind =
  | 'start'
  | 'activity'
  | 'condition'
  | 'parallel_start'
  | 'parallel_join'
  | 'wait'
  | 'delay'
  | 'pause'
  | 'escalation'
  | 'email'
  | 'sms'
  | 'oracle_update'
  | 'execute_child_node'
  | 'terminate'
  | 'end'

type BlwNodeData = {
  label: string
  blwType: BlwNodeKind
  enabled?: boolean
  status?: string
  inputContract?: string
  validationRules?: string
  outputContract?: string
  retry?: { enabled: boolean; maxAttempts: number; delaySeconds: number }
  iteration?: { maxIterations: number }
  escalation?: { enabled: boolean; afterFailures: number; to: string }
  wait?: { timeoutMinutes: number }
  delay?: { seconds: number }
  childNodeId?: string
  joinPolicy?: 'wait_all' | 'wait_any' | 'first_success'
  failurePolicy?: 'fail_workflow' | 'continue_failed_branch' | 'escalate'
  actionJson?: string
}

type BlwStep = {
  id: string
  name: string
  type: string
  status: string
  retryAttempts: number
  maxRetryAttempts: number
  iterationNo: number
  maxIterations: number
  escalationLevel: number
  next: string
}

type BlwInstanceVariable = {
  name: string
  source: 'input_field' | 'expression' | 'static'
  field?: string
  expression?: string
  defaultValue?: string
}

type BlwInputFieldMapping = {
  field: string
  alias: string
}

type BlwValidationIssue = {
  severity: 'error' | 'warning'
  area: string
  message: string
}

type BlwGraphSnapshot = {
  nodes: Node<BlwNodeData>[]
  edges: Edge[]
}

export type BlwStudioConfig = {
  enabled: boolean
  trackingMode: 'all_rows' | 'minimal' | 'exception_only' | 'none'
  workflowName: string
  trackerTable: string
  uniqueIdField: string
  parallelEnabled: boolean
  maxParallelBranches: number
  maxIterations: number
  maxRetryAttempts: number
  escalationEnabled: boolean
  escalationAfterFailures: number
  waitTimeoutMinutes: number
  oracleUpdateEnabled: boolean
  notificationEnabled: boolean
  inputFields: string[]
  inputFieldMappings: BlwInputFieldMapping[]
  instanceVariables: BlwInstanceVariable[]
  graphNodes: Node<BlwNodeData>[]
  graphEdges: Edge[]
  steps: BlwStep[]
}

type BLWStudioProps = {
  open: boolean
  nodeLabel: string
  config?: Record<string, unknown>
  upstreamInputFields?: string[]
  upstreamPreviewRows?: Record<string, unknown>[]
  onClose: () => void
  onSave: (config: BlwStudioConfig) => void
  onOpenChildCanvas?: () => void
}

const nodeTypeOptions: Array<{ type: BlwNodeKind; label: string; color: string }> = [
  { type: 'activity', label: 'Activity', color: '#4f83cc' },
  { type: 'condition', label: 'Condition Route', color: '#b8872f' },
  { type: 'parallel_start', label: 'Parallel Start', color: '#7c6bb3' },
  { type: 'parallel_join', label: 'Parallel Join', color: '#7c6bb3' },
  { type: 'execute_child_node', label: 'Execute Child Node', color: '#3f98ad' },
  { type: 'wait', label: 'Wait Timer', color: '#a66f35' },
  { type: 'delay', label: 'Delay', color: '#64748b' },
  { type: 'pause', label: 'Pause', color: '#7d6a9f' },
  { type: 'escalation', label: 'Escalation', color: '#b05d82' },
  { type: 'email', label: 'Email', color: '#4f9b72' },
  { type: 'sms', label: 'SMS', color: '#4a9c8f' },
  { type: 'oracle_update', label: 'Oracle Update', color: '#a66f35' },
  { type: 'terminate', label: 'Terminate', color: '#b85c5c' },
]

const nodeKindColor: Record<string, string> = {
  start: '#2e7d32',
  end: '#546e7a',
  activity: '#1565c0',
  condition: '#b26a00',
  parallel_start: '#5e35b1',
  parallel_join: '#5e35b1',
  execute_child_node: '#00838f',
  wait: '#8d6e63',
  delay: '#64748b',
  pause: '#6d4c41',
  escalation: '#ad1457',
  email: '#2e7d32',
  sms: '#00796b',
  oracle_update: '#ef6c00',
  terminate: '#c62828',
}

const nodeKindShape: Record<string, string> = {
  start: '999px',
  end: '999px',
  condition: '10px',
  parallel_start: '8px',
  parallel_join: '8px',
  wait: '18px',
  delay: '18px',
  pause: '18px',
  escalation: '6px',
  terminate: '999px',
}

const blwEdgeColor = '#8b98aa'
const blwEdgeStyle: CSSProperties = {
  stroke: blwEdgeColor,
  strokeWidth: 0.75,
  strokeDasharray: '2 5',
}

function nodeShapeStyle(type: BlwNodeKind): CSSProperties {
  if (type === 'condition') {
    return {
      width: 78,
      height: 78,
      borderRadius: 6,
      transform: 'rotate(45deg)',
    }
  }
  if (type === 'wait' || type === 'delay') {
    return {
      width: 54,
      height: 54,
      borderRadius: 999,
    }
  }
  if (type === 'parallel_start' || type === 'parallel_join') {
    return {
      width: 54,
      height: 54,
      borderRadius: 8,
      transform: 'rotate(45deg)',
    }
  }
  if (type === 'start' || type === 'end' || type === 'terminate') {
    return {
      width: 96,
      height: 38,
      borderRadius: 999,
    }
  }
  if (type === 'escalation') {
    return {
      width: 116,
      height: 48,
      borderRadius: 6,
      clipPath: 'polygon(10% 0, 90% 0, 100% 50%, 90% 100%, 10% 100%, 0 50%)',
    }
  }
  return {
    width: 122,
    minHeight: 46,
    borderRadius: nodeKindShape[type] || 8,
  }
}

function nodeIcon(type: BlwNodeKind, color: string) {
  const style = { color, fontSize: type === 'wait' || type === 'delay' ? 14 : 12 }
  if (type === 'start') return <PlayCircleOutlined style={style} />
  if (type === 'end') return <CheckCircleOutlined style={style} />
  if (type === 'condition') return <BranchesOutlined style={style} />
  if (type === 'parallel_start') return <ForkOutlined style={style} />
  if (type === 'parallel_join') return <ApartmentOutlined style={style} />
  if (type === 'execute_child_node') return <ThunderboltOutlined style={style} />
  if (type === 'wait') return <ClockCircleOutlined style={style} />
  if (type === 'delay') return <ClockCircleOutlined style={style} />
  if (type === 'pause') return <PauseCircleOutlined style={style} />
  if (type === 'escalation') return <WarningOutlined style={style} />
  if (type === 'email') return <MailOutlined style={style} />
  if (type === 'sms') return <MessageOutlined style={style} />
  if (type === 'oracle_update') return <DatabaseOutlined style={style} />
  if (type === 'terminate') return <StopOutlined style={style} />
  return <SyncOutlined style={style} />
}

function connectionHandlesForType(type: BlwNodeKind): Array<{
  id: string
  kind: 'source' | 'target'
  position: Position
  label?: string
  style?: CSSProperties
}> {
  if (type === 'start') {
    return [
      { id: 'out-right', kind: 'source', position: Position.Right },
      { id: 'out-bottom', kind: 'source', position: Position.Bottom },
    ]
  }
  if (type === 'end' || type === 'terminate') {
    return [
      { id: 'in-top', kind: 'target', position: Position.Top },
      { id: 'in-left', kind: 'target', position: Position.Left },
      { id: 'in-right', kind: 'target', position: Position.Right },
    ]
  }
  if (type === 'condition') {
    return [
      { id: 'in-top', kind: 'target', position: Position.Top },
      { id: 'true', kind: 'source', position: Position.Right, label: 'T' },
      { id: 'false', kind: 'source', position: Position.Left, label: 'F' },
      { id: 'out-bottom', kind: 'source', position: Position.Bottom },
    ]
  }
  if (type === 'parallel_start') {
    return [
      { id: 'in-top', kind: 'target', position: Position.Top },
      { id: 'out-left', kind: 'source', position: Position.Left },
      { id: 'out-right', kind: 'source', position: Position.Right },
      { id: 'out-bottom', kind: 'source', position: Position.Bottom },
    ]
  }
  if (type === 'parallel_join') {
    return [
      { id: 'in-top', kind: 'target', position: Position.Top },
      { id: 'in-left', kind: 'target', position: Position.Left },
      { id: 'in-right', kind: 'target', position: Position.Right },
      { id: 'out-bottom', kind: 'source', position: Position.Bottom },
    ]
  }
  return [
    { id: 'in-top', kind: 'target', position: Position.Top },
    { id: 'in-left', kind: 'target', position: Position.Left },
    { id: 'out-right', kind: 'source', position: Position.Right },
    { id: 'out-bottom', kind: 'source', position: Position.Bottom },
  ]
}

function BlwCanvasNode({ data, selected }: NodeProps<BlwNodeData>) {
  const type = data.blwType || 'activity'
  const color = nodeKindColor[type] || '#3b82f6'
  const isEnabled = data.enabled !== false
  const isRotatedShape = type === 'parallel_start' || type === 'parallel_join' || type === 'condition'
  const shapeStyle = nodeShapeStyle(type)
  const compactLabel = String(data.label || displayNodeType(type))
  const handles = [...connectionHandlesForType(type)]
  if (type === 'condition') {
    const action = safeJson(data.actionJson)
    const groups = conditionGroupRows(
      action.conditionGroups,
      conditionRuleRows(action.conditionRules),
      String(action.conditionMatchMode || 'all'),
    )
    groups.forEach((group, index) => {
      const safeGroupId = String(group.id || `group_${index + 1}`).replace(/[^A-Za-z0-9_-]/g, '_')
      const top = `${Math.min(76, 24 + index * 12)}%`
      handles.push(
        { id: `group_${safeGroupId}_true`, kind: 'source', position: Position.Right, label: `T${index + 1}`, style: { top } },
      )
    })
  }
  const baseHandleStyle: CSSProperties = {
    width: 5,
    height: 5,
    minWidth: 5,
    minHeight: 5,
    background: 'var(--app-panel-bg)',
    border: `0.75px solid ${color}`,
    boxShadow: '0 1px 3px rgba(0, 0, 0, 0.18)',
    opacity: 0.9,
  }
  const handleStyleFor = (kind: 'source' | 'target'): CSSProperties => ({
    ...baseHandleStyle,
    borderColor: kind === 'source' ? color : '#94a3b8',
    background: kind === 'source' ? 'var(--app-panel-bg)' : 'var(--app-shell-bg)',
  })
  const handleLabelStyle = (position: Position): CSSProperties => ({
    position: 'absolute',
    color,
    fontSize: 7,
    fontWeight: 500,
    pointerEvents: 'none',
    ...(position === Position.Right ? { right: -15, top: '50%', transform: 'translateY(-50%)' } : {}),
    ...(position === Position.Left ? { left: -15, top: '50%', transform: 'translateY(-50%)' } : {}),
    ...(position === Position.Bottom ? { left: '50%', bottom: -15, transform: 'translateX(-50%)' } : {}),
    ...(position === Position.Top ? { left: '50%', top: -15, transform: 'translateX(-50%)' } : {}),
  })
  const renderHandles = () => handles.map((item) => (
    <span key={`${item.kind}-${item.id}`}>
      <Handle id={item.id} type={item.kind} position={item.position} style={{ ...handleStyleFor(item.kind), ...(item.style || {}) }} />
      {item.label ? <span style={handleLabelStyle(item.position)}>{item.label}</span> : null}
    </span>
  ))
  const content = (
    <div style={{
      textAlign: 'center',
      display: 'grid',
      gap: 2,
      justifyItems: 'center',
      alignContent: 'center',
      height: '100%',
      padding: type === 'condition' ? '8px 18px' : type === 'wait' || type === 'delay' ? 4 : '6px 8px',
      transform: isRotatedShape ? 'rotate(-45deg)' : undefined,
    }}>
      {nodeIcon(type, color)}
      <Text
        title={compactLabel}
        style={{
          color: isEnabled ? 'var(--app-text)' : 'var(--app-text-faint)',
          fontSize: type === 'wait' || type === 'delay' ? 7 : isRotatedShape ? 7 : 8,
          fontWeight: 400,
          lineHeight: 1.1,
          maxWidth: type === 'condition' ? 42 : type === 'wait' || type === 'delay' ? 40 : 98,
          whiteSpace: 'nowrap',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
        }}
      >
        {compactLabel}
      </Text>
      {(type === 'parallel_start' || type === 'parallel_join') ? (
        <Text style={{ color: 'var(--app-text-faint)', fontSize: 7, lineHeight: 1, fontWeight: 400 }}>
          {type === 'parallel_start' ? 'split' : 'join'}
        </Text>
      ) : null}
    </div>
  )
  return (
    <div style={{ position: 'relative' }}>
      {renderHandles()}
      {!isEnabled ? (
        <Tag
          color="default"
          style={{
            position: 'absolute',
            right: -8,
            top: -12,
            zIndex: 2,
            margin: 0,
            fontSize: 8,
            lineHeight: '13px',
            padding: '0 4px',
            borderColor: 'var(--app-border)',
            color: 'var(--app-text-faint)',
            background: 'var(--app-shell-bg)',
          }}
        >
          off
        </Tag>
      ) : null}
      <div
        style={{
          ...shapeStyle,
          border: type === 'condition'
            ? `1.15px solid ${color}`
            : `0.75px solid ${selected ? color : 'var(--app-border-strong)'}`,
          borderLeft: type === 'condition' ? `1.15px solid ${color}` : `2px solid ${color}`,
          borderTop: `0.75px solid ${selected ? color : 'var(--app-border-strong)'}`,
          outline: type === 'condition'
            ? `1.15px solid ${color}`
            : `0.75px solid ${selected ? color : 'var(--app-border-strong)'}`,
          outlineOffset: -1,
          boxShadow: selected
            ? `0 0 0 1.5px ${color}55, 0 8px 18px rgba(0,0,0,.24)`
            : `0 5px 12px rgba(0,0,0,.18)`,
          background: 'var(--app-panel-bg)',
          color: 'var(--app-text)',
          display: 'grid',
          placeItems: 'center',
          opacity: isEnabled ? 1 : 0.48,
          filter: isEnabled ? undefined : 'grayscale(0.7)',
        }}
      >
        {content}
      </div>
    </div>
  )
}

const blwNodeTypes: NodeTypes = { blwNode: BlwCanvasNode }

function cloneBlwGraph(nodes: Node<BlwNodeData>[], edges: Edge[]): BlwGraphSnapshot {
  return {
    nodes: JSON.parse(JSON.stringify(nodes.map((node) => ({ ...node, selected: false, dragging: false })))),
    edges: JSON.parse(JSON.stringify(edges.map((edge) => ({ ...edge, selected: false })))),
  }
}

function blwGraphSignature(snapshot: BlwGraphSnapshot): string {
  return JSON.stringify(snapshot)
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}
}

function safeJson(value: unknown, fallback: Record<string, unknown> = {}): Record<string, unknown> {
  if (value && typeof value === 'object' && !Array.isArray(value)) return value as Record<string, unknown>
  if (typeof value !== 'string' || !value.trim()) return fallback
  try {
    const parsed = JSON.parse(value)
    return asRecord(parsed)
  } catch {
    return fallback
  }
}

function jsonPatch(current: unknown, patch: Record<string, unknown>): string {
  return JSON.stringify({ ...safeJson(current), ...patch }, null, 2)
}

function toStringArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map((item) => String(item || '').trim()).filter(Boolean)
  if (typeof value === 'string') {
    if (!value.trim() || value === 'all') return []
    return value.split(',').map((item) => item.trim()).filter(Boolean)
  }
  return []
}

function uniqueStrings(values: unknown[]): string[] {
  const out: string[] = []
  const seen = new Set<string>()
  values.flatMap((value) => toStringArray(value)).forEach((item) => {
    const key = item.toLowerCase()
    if (!key || seen.has(key)) return
    seen.add(key)
    out.push(item)
  })
  return out
}

function normalizeOutputReceivePath(value: unknown): string {
  let path = String(value || '').trim()
  if (!path) return ''
  if (path.startsWith('$[')) return path.slice(1).replace(/^\./, '')
  if (path.startsWith('$.')) return path.slice(2)
  if (path === '$') return ''
  return path.replace(/^\./, '')
}

function outputReceivePathHints(pathValue: unknown): string[] {
  const path = normalizeOutputReceivePath(pathValue)
  if (!path) return []
  return uniqueStrings([
    path,
    `${path}.status`,
    `${path}.rows`,
    `${path}.path`,
    `${path}.output_file`,
    `${path}.message`,
    `${path}.error`,
    `${path}.0.status`,
    `${path}.0.rows`,
    `${path}.0.path`,
    `${path}.0.output_file`,
    `${path}.0.message`,
    `${path}.0.error`,
  ])
}

function fieldsFromRows(value: unknown): string[] {
  const rows = Array.isArray(value) ? value : []
  const fields = new Set<string>()
  rows.slice(0, 50).forEach((row) => {
    if (row && typeof row === 'object' && !Array.isArray(row)) {
      Object.keys(row as Record<string, unknown>).forEach((key) => key && fields.add(key))
    }
  })
  return Array.from(fields)
}

function buildConditionExpression(field: string, operator: string, value: string, source: 'field' | 'variable' = 'field'): string {
  const left = source === 'variable' ? `var('${field}')` : `field('${field}')`
  if (operator === 'exists') return `${left} != None`
  if (operator === 'empty') return `${left} == ''`
  if (operator === 'contains') return `contains(${left}, '${String(value || '').replace(/'/g, "\\'")}')`
  const normalized = ['>', '<', '>=', '<='].includes(operator)
    ? String(value || '0')
    : `'${String(value || '').replace(/'/g, "\\'")}'`
  return `${left} ${operator || '=='} ${normalized}`
}

function conditionRuleRows(value: unknown): Array<Record<string, string>> {
  if (!Array.isArray(value)) return []
  return value
    .map((item, index) => {
      const row = asRecord(item)
      return {
        id: String(row.id || `rule_${index + 1}`),
        source: String(row.source || 'field') === 'variable' ? 'variable' : 'field',
        field: String(row.field || ''),
        operator: String(row.operator || '=='),
        value: String(row.value || ''),
      }
    })
    .filter((row) => row.field)
}

function buildConditionRulesExpression(rules: Array<Record<string, string>>, matchMode: string): string {
  const expressions = rules
    .map((rule) => buildConditionExpression(
      String(rule.field || ''),
      String(rule.operator || '=='),
      String(rule.value || ''),
      String(rule.source || 'field') === 'variable' ? 'variable' : 'field',
    ))
    .filter((expression) => expression && !expression.includes("field('')") && !expression.includes("var('')"))
  if (!expressions.length) return ''
  const joiner = matchMode === 'any' ? ' or ' : ' and '
  return expressions.map((expression) => `(${expression})`).join(joiner)
}

function conditionGroupRows(value: unknown, fallbackRules: Array<Record<string, string>>, fallbackMatchMode: string): Array<Record<string, unknown>> {
  if (Array.isArray(value) && value.length) {
    return value.map((item, index) => {
      const row = asRecord(item)
      return {
        id: String(row.id || `group_${index + 1}`),
        name: String(row.name || `Condition Group ${index + 1}`),
        enabled: row.enabled !== false,
        matchMode: String(row.matchMode || 'all') === 'any' ? 'any' : 'all',
        rules: conditionRuleRows(row.rules),
      }
    })
  }
  return [{
    id: 'group_default',
    name: 'Condition Group 1',
    enabled: true,
    matchMode: fallbackMatchMode === 'any' ? 'any' : 'all',
    rules: fallbackRules,
  }]
}

function buildConditionGroupsExpression(groups: Array<Record<string, unknown>>): string {
  const expressions = groups
    .filter((group) => group.enabled !== false)
    .map((group) => buildConditionRulesExpression(
      conditionRuleRows(group.rules),
      String(group.matchMode || 'all'),
    ))
    .filter(Boolean)
  return expressions.map((expression) => `(${expression})`).join(' or ')
}

function normalizeInstanceVariables(value: unknown): BlwInstanceVariable[] {
  if (!Array.isArray(value)) return []
  return value.map((item) => {
    const row = asRecord(item)
    return {
      name: String(row.name || '').trim(),
      source: ['input_field', 'expression', 'static'].includes(String(row.source || '')) ? row.source as BlwInstanceVariable['source'] : 'input_field',
      field: String(row.field || '').trim(),
      expression: String(row.expression || '').trim(),
      defaultValue: String(row.defaultValue || '').trim(),
    }
  }).filter((item) => item.name)
}

function normalizeInputFieldMappings(value: unknown, fallbackFields: string[]): BlwInputFieldMapping[] {
  if (Array.isArray(value) && value.length) {
    return value.map((item) => {
      const row = asRecord(item)
      const field = String(row.field || row.name || row.source || '').trim()
      const alias = String(row.alias || row.label || field).trim()
      return { field, alias: alias || field }
    }).filter((item) => item.field)
  }
  return fallbackFields.map((field) => ({ field, alias: field }))
}

function firstRuleExpression(value: unknown): string {
  const rules = Array.isArray(value) ? value : []
  const first = asRecord(rules[0])
  return String(first.expression || '')
}

function createGraphNode(type: BlwNodeKind, label: string, x: number, y: number): Node<BlwNodeData> {
  const color = nodeKindColor[type] || '#3b82f6'
  const id = `${type}_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`
  const defaultAction =
    type === 'condition' ? { condition: "status == 'APPROVED'", trueLabel: 'approved', falseLabel: 'rejected' }
      : type === 'oracle_update' ? { table: 'BLW_WORKFLOW_TRACKER', keyField: 'TRANSACTIONID', statusField: 'CURRENT_STATUS' }
        : type === 'email' ? { recipient: '$.email', template: 'Workflow notification for ${TRANSACTIONID}', sendMode: 'draft' }
          : type === 'sms' ? { recipient: '$.phone', template: 'Workflow update for ${TRANSACTIONID}' }
            : type === 'execute_child_node' ? { inputMode: 'pass_all', outputPath: '$.child_output' }
              : {}
  return {
    id,
    type: 'blwNode',
    position: { x, y },
    data: {
      label,
      blwType: type,
      enabled: true,
      status: 'draft',
      inputContract: '{"from":"previous_output","fields":"all"}',
      validationRules: '{"required":[],"rules":[]}',
      outputContract: '{"to":"workflow_context","fields":"all"}',
      retry: { enabled: true, maxAttempts: 3, delaySeconds: 60 },
      iteration: { maxIterations: 5 },
      escalation: { enabled: true, afterFailures: 3, to: 'operations' },
      wait: { timeoutMinutes: 30 },
      delay: { seconds: 30 },
      joinPolicy: 'wait_all',
      failurePolicy: 'fail_workflow',
      actionJson: JSON.stringify(defaultAction, null, 2),
    },
  }
}

function defaultGraphNodes(): Node<BlwNodeData>[] {
  const nodes = [
    createGraphNode('start', 'Start', 520, 20),
    createGraphNode('condition', 'Check Status', 520, 135),
    createGraphNode('terminate', 'Reject', 820, 170),
    createGraphNode('parallel_start', 'Split', 550, 305),
    createGraphNode('execute_child_node', 'Child Node', 260, 430),
    createGraphNode('email', 'Notify', 520, 410),
    createGraphNode('oracle_update', 'Oracle Update', 780, 430),
    createGraphNode('parallel_join', 'Join', 550, 565),
    createGraphNode('end', 'End', 520, 690),
  ]
  nodes[0].id = 'start'
  nodes[0].data.retry = { enabled: false, maxAttempts: 0, delaySeconds: 0 }
  const endNode = nodes.find((node) => node.data.blwType === 'end')
  if (endNode) {
    endNode.id = 'end'
    endNode.data.retry = { enabled: false, maxAttempts: 0, delaySeconds: 0 }
  }
  return nodes
}

function defaultGraphEdges(): Edge[] {
  return [
    { id: 'e_start_route', source: 'start', target: 'condition_1', label: 'start' },
    { id: 'e_route_parallel', source: 'condition_1', sourceHandle: 'true', target: 'parallel_start_1', label: 'approved' },
    { id: 'e_route_reject', source: 'condition_1', sourceHandle: 'false', target: 'terminate_1', label: 'rejected' },
    { id: 'e_parallel_child', source: 'parallel_start_1', target: 'execute_child_node_1', label: 'branch A' },
    { id: 'e_parallel_notify', source: 'parallel_start_1', target: 'email_1', label: 'branch B' },
    { id: 'e_parallel_oracle', source: 'parallel_start_1', target: 'oracle_update_1', label: 'branch C' },
    { id: 'e_child_join', source: 'execute_child_node_1', target: 'parallel_join_1', label: 'done' },
    { id: 'e_notify_join', source: 'email_1', target: 'parallel_join_1', label: 'done' },
    { id: 'e_oracle_join', source: 'oracle_update_1', target: 'parallel_join_1', label: 'done' },
    { id: 'e_join_end', source: 'parallel_join_1', target: 'end', label: 'continue' },
  ].map(normalizeEdgeStyle)
}

function defaultGraph(): { nodes: Node<BlwNodeData>[]; edges: Edge[] } {
  const nodes = defaultGraphNodes()
  const idByType: Record<string, string> = {}
  nodes.forEach((node) => {
    if (node.id !== 'start' && node.id !== 'end') {
      const key = String(node.data.blwType)
      idByType[key] = `${key}_1`
      node.id = idByType[key]
    }
  })
  return { nodes, edges: defaultGraphEdges() }
}

function normalizeNodes(raw: unknown): Node<BlwNodeData>[] {
  if (!Array.isArray(raw) || raw.length === 0) return defaultGraph().nodes
  return raw.map((node: any) => {
    const data = asRecord(node?.data)
    const blwType = String(data.blwType || data.type || 'activity') as BlwNodeKind
    const color = nodeKindColor[blwType] || '#3b82f6'
    return {
      ...node,
      type: 'blwNode',
      data: {
        label: String(data.label || node?.id || 'Activity'),
        blwType,
        enabled: data.enabled !== false,
        status: String(data.status || 'draft'),
        inputContract: String(data.inputContract || '{"from":"previous_output","fields":"all"}'),
        validationRules: String(data.validationRules || '{"required":[],"rules":[]}'),
        outputContract: String(data.outputContract || '{"to":"workflow_context","fields":"all"}'),
        retry: asRecord(data.retry) as BlwNodeData['retry'] || { enabled: true, maxAttempts: 3, delaySeconds: 60 },
        iteration: asRecord(data.iteration) as BlwNodeData['iteration'] || { maxIterations: 5 },
        escalation: asRecord(data.escalation) as BlwNodeData['escalation'] || { enabled: true, afterFailures: 3, to: 'operations' },
        wait: asRecord(data.wait) as BlwNodeData['wait'] || { timeoutMinutes: 30 },
        delay: asRecord(data.delay) as BlwNodeData['delay'] || { seconds: 30 },
        childNodeId: String(data.childNodeId || ''),
        joinPolicy: String(data.joinPolicy || 'wait_all') as BlwNodeData['joinPolicy'],
        failurePolicy: String(data.failurePolicy || 'fail_workflow') as BlwNodeData['failurePolicy'],
        actionJson: String(data.actionJson || '{}'),
      },
      style: node?.style || {},
    }
  })
}

function normalizeEdges(raw: unknown): Edge[] {
  if (!Array.isArray(raw) || raw.length === 0) return defaultGraph().edges
  return raw.map((edge: any) => normalizeEdgeStyle({
    ...edge,
    animated: Boolean(edge?.animated),
  }))
}

function normalizeEdgeStyle(edge: Edge): Edge {
  return {
    ...edge,
    type: String(edge.type || 'smoothstep'),
    markerEnd: edge.markerEnd || { type: MarkerType.ArrowClosed, color: blwEdgeColor, width: 8, height: 8 },
    style: { ...(edge.style || {}), ...blwEdgeStyle },
    labelStyle: { fill: 'var(--app-text-muted)', fontSize: 8, fontWeight: 500, ...(edge.labelStyle || {}) },
    labelBgStyle: { fill: 'var(--app-panel-bg)', fillOpacity: 0.92, ...(edge.labelBgStyle || {}) },
    labelBgPadding: edge.labelBgPadding || [4, 1],
    labelBgBorderRadius: edge.labelBgBorderRadius || 4,
  }
}

function stepsFromGraph(nodes: Node<BlwNodeData>[], edges: Edge[], maxRetryAttempts: number, maxIterations: number): BlwStep[] {
  return nodes.map((node) => {
    const next = edges.filter((edge) => edge.source === node.id).map((edge) => edge.target).join(', ')
    return {
      id: node.id,
      name: String(node.data?.label || node.id),
      type: String(node.data?.blwType || 'activity'),
      status: String(node.data?.status || 'draft'),
      retryAttempts: 0,
      maxRetryAttempts: Number(node.data?.retry?.maxAttempts ?? maxRetryAttempts),
      iterationNo: 1,
      maxIterations: Number(node.data?.iteration?.maxIterations ?? maxIterations),
      escalationLevel: 0,
      next,
    }
  })
}

function validateBlwGraph(nodes: Node<BlwNodeData>[], edges: Edge[], config: BlwStudioConfig): BlwValidationIssue[] {
  const issues: BlwValidationIssue[] = []
  const nodeById = new Map(nodes.map((node) => [node.id, node]))
  const outgoing = new Map<string, Edge[]>()
  const incoming = new Map<string, Edge[]>()
  edges.forEach((edge) => {
    if (!nodeById.has(edge.source) || !nodeById.has(edge.target)) {
      issues.push({ severity: 'error', area: 'Route', message: `Route ${edge.id || ''} points to a missing node.` })
      return
    }
    outgoing.set(edge.source, [...(outgoing.get(edge.source) || []), edge])
    incoming.set(edge.target, [...(incoming.get(edge.target) || []), edge])
  })
  const startNodes = nodes.filter((node) => node.data?.blwType === 'start')
  const endNodes = nodes.filter((node) => node.data?.blwType === 'end')
  if (startNodes.length !== 1) issues.push({ severity: 'error', area: 'Canvas', message: 'Workflow must have exactly one Start node.' })
  if (endNodes.length < 1) issues.push({ severity: 'error', area: 'Canvas', message: 'Workflow must have at least one End node.' })
  if (!String(config.trackerTable || '').trim()) issues.push({ severity: 'error', area: 'Oracle', message: 'Tracker table is required.' })
  if (!String(config.uniqueIdField || '').trim()) issues.push({ severity: 'error', area: 'Oracle', message: 'Unique input field is required.' })
  config.instanceVariables.forEach((item) => {
    if (!item.name) issues.push({ severity: 'error', area: 'Instance Variables', message: 'Variable name is required.' })
    if (item.source === 'input_field' && !item.field) issues.push({ severity: 'warning', area: item.name || 'Variable', message: 'Input field mapping is empty.' })
    if (item.source === 'expression' && !item.expression) issues.push({ severity: 'warning', area: item.name || 'Variable', message: 'Expression is empty.' })
  })
  nodes.forEach((node) => {
    const label = String(node.data?.label || node.id)
    const type = node.data?.blwType || 'activity'
    if (type !== 'start' && !incoming.get(node.id)?.length) {
      issues.push({ severity: 'warning', area: label, message: 'Node has no incoming route and may never run.' })
    }
    if (type !== 'end' && type !== 'terminate' && !outgoing.get(node.id)?.length) {
      issues.push({ severity: 'warning', area: label, message: 'Node has no outgoing route.' })
    }
    if (type === 'condition') {
      const action = safeJson(node.data.actionJson)
      const groups = Array.isArray(action.conditionGroups) ? action.conditionGroups : []
      const hasConfiguredRules = groups.some((group) => {
        const row = asRecord(group)
        return conditionRuleRows(row.rules).length > 0
      }) || conditionRuleRows(action.conditionRules).length > 0
      if (!String(action.condition || '').trim() && !hasConfiguredRules) {
        issues.push({ severity: 'error', area: label, message: 'Condition route requires at least one rule.' })
      }
      const conditionEdges = outgoing.get(node.id) || []
      const hasTrue = conditionEdges.some((edge) => {
        const handle = String(edge.sourceHandle || '')
        return handle === 'true' || handle.endsWith('_true')
      })
      const hasFalse = conditionEdges.some((edge) => {
        const handle = String(edge.sourceHandle || '')
        return handle === 'false' || handle.endsWith('_false')
      })
      if (!hasTrue || !hasFalse) issues.push({ severity: 'warning', area: label, message: 'Condition should have both true and false routes.' })
    }
    if (type === 'execute_child_node' && !String(node.data.childNodeId || '').trim()) {
      issues.push({ severity: 'warning', area: label, message: 'Child node alias is empty; runtime will only pass rows through.' })
    }
    if (type === 'oracle_update') {
      const action = safeJson(node.data.actionJson)
      if (!String(action.table || config.trackerTable || '').trim()) issues.push({ severity: 'error', area: label, message: 'Oracle update requires a target table.' })
      if (!String(action.keyField || config.uniqueIdField || '').trim()) issues.push({ severity: 'error', area: label, message: 'Oracle update requires a key field.' })
    }
    const validation = safeJson(node.data.validationRules)
    const rules = validation.rules
    if (rules && !Array.isArray(rules)) issues.push({ severity: 'error', area: label, message: 'Validation rules must be a list.' })
    const retry = node.data.retry
    if (retry?.enabled !== false && Number(retry?.maxAttempts ?? config.maxRetryAttempts) < 1) {
      issues.push({ severity: 'warning', area: label, message: 'Retry is enabled but max attempts is below 1.' })
    }
  })
  return issues
}

function normalizeConfig(raw: unknown, nodeLabel: string): BlwStudioConfig {
  const cfg = asRecord(raw)
  const graphNodes = normalizeNodes(cfg.graphNodes || cfg.nodes)
  const graphEdges = normalizeEdges(cfg.graphEdges || cfg.edges)
  const maxIterations = Number(cfg.maxIterations || 5)
  const maxRetryAttempts = Number(cfg.maxRetryAttempts || 3)
  const inputFields = uniqueStrings([cfg.inputFields, cfg.sourceFields, cfg._detected_columns])
  return {
    enabled: cfg.enabled !== false,
    trackingMode: (
      ['all_rows', 'minimal', 'exception_only', 'none'].includes(String(cfg.trackingMode || cfg.workflowTrackingMode || cfg.workflow_tracking_mode || '').trim())
        ? String(cfg.trackingMode || cfg.workflowTrackingMode || cfg.workflow_tracking_mode).trim()
        : 'all_rows'
    ) as BlwStudioConfig['trackingMode'],
    workflowName: String(cfg.workflowName || nodeLabel || 'Business Logic Workflow'),
    trackerTable: String(cfg.trackerTable || 'BLW_WORKFLOW_TRACKER'),
    uniqueIdField: String(cfg.uniqueIdField || 'TRANSACTIONID'),
    parallelEnabled: cfg.parallelEnabled !== false,
    maxParallelBranches: Number(cfg.maxParallelBranches || 4),
    maxIterations,
    maxRetryAttempts,
    escalationEnabled: cfg.escalationEnabled !== false,
    escalationAfterFailures: Number(cfg.escalationAfterFailures || 3),
    waitTimeoutMinutes: Number(cfg.waitTimeoutMinutes || 30),
    oracleUpdateEnabled: cfg.oracleUpdateEnabled !== false,
    notificationEnabled: cfg.notificationEnabled !== false,
    inputFields,
    inputFieldMappings: normalizeInputFieldMappings(cfg.inputFieldMappings, inputFields),
    instanceVariables: normalizeInstanceVariables(cfg.instanceVariables),
    graphNodes,
    graphEdges,
    steps: Array.isArray(cfg.steps) && cfg.steps.length > 0
      ? cfg.steps as BlwStep[]
      : stepsFromGraph(graphNodes, graphEdges, maxRetryAttempts, maxIterations),
  }
}

function resolveStoredBlwConfig(config: Record<string, unknown> | undefined): Record<string, unknown> {
  const topLevel = asRecord(config)
  const nested = asRecord(topLevel.blw_studio_config)
  return {
    ...nested,
    trackingMode: nested.trackingMode ?? nested.workflowTrackingMode ?? nested.workflow_tracking_mode ?? topLevel.workflow_tracking_mode ?? topLevel.workflowTrackingMode,
    workflowTrackingMode: nested.workflowTrackingMode ?? nested.trackingMode ?? nested.workflow_tracking_mode ?? topLevel.workflow_tracking_mode ?? topLevel.workflowTrackingMode,
    workflow_tracking_mode: nested.workflow_tracking_mode ?? nested.trackingMode ?? nested.workflowTrackingMode ?? topLevel.workflow_tracking_mode ?? topLevel.workflowTrackingMode,
    inputFields: nested.inputFields ?? topLevel.inputFields,
    inputFieldMappings: nested.inputFieldMappings ?? topLevel.inputFieldMappings,
    instanceVariables: nested.instanceVariables ?? topLevel.instanceVariables ?? topLevel.blw_instance_variables,
  }
}

function buildOracleDdl(config: BlwStudioConfig): string {
  const table = String(config.trackerTable || 'BLW_WORKFLOW_TRACKER').replace(/[^A-Za-z0-9_]/g, '').toUpperCase() || 'BLW_WORKFLOW_TRACKER'
  return `CREATE TABLE ${table} (
  RUN_ID                 VARCHAR2(64) PRIMARY KEY,
  WORKFLOW_ID            VARCHAR2(64),
  WORKFLOW_NAME          VARCHAR2(255),
  PIPELINE_ID            VARCHAR2(64),
  BUSINESS_NODE_ID       VARCHAR2(64),
  INPUT_UNIQUE_ID        VARCHAR2(255),
  INPUT_SOURCE           VARCHAR2(255),
  INPUT_PAYLOAD_JSON     CLOB,
  CURRENT_STAGE          VARCHAR2(255),
  CURRENT_STEP_ID        VARCHAR2(255),
  CURRENT_STATUS         VARCHAR2(50),
  ITERATION_NO           NUMBER DEFAULT 1,
  MAX_ITERATIONS         NUMBER DEFAULT ${Math.max(1, Number(config.maxIterations || 1))},
  RETRY_COUNT            NUMBER DEFAULT 0,
  MAX_RETRY_ATTEMPTS     NUMBER DEFAULT ${Math.max(0, Number(config.maxRetryAttempts || 0))},
  LAST_RETRY_AT          TIMESTAMP,
  ESCALATION_LEVEL       NUMBER DEFAULT 0,
  ESCALATION_STATUS      VARCHAR2(50),
  ESCALATED_TO           VARCHAR2(255),
  ESCALATED_AT           TIMESTAMP,
  WAIT_UNTIL             TIMESTAMP,
  PAUSED_AT              TIMESTAMP,
  RESUMED_AT             TIMESTAMP,
  TERMINATED_AT          TIMESTAMP,
  STARTED_AT             TIMESTAMP DEFAULT SYSTIMESTAMP,
  LAST_UPDATED_AT        TIMESTAMP DEFAULT SYSTIMESTAMP,
  ENDED_AT               TIMESTAMP,
  ERROR_CODE             VARCHAR2(255),
  ERROR_MESSAGE          CLOB,
  WORKFLOW_CONFIG_JSON   CLOB,
  STEP_TRACK_JSON        CLOB,
  ROUTING_HISTORY_JSON   CLOB,
  RETRY_HISTORY_JSON     CLOB,
  ESCALATION_JSON        CLOB,
  PARALLEL_BRANCH_JSON   CLOB,
  CONTEXT_JSON           CLOB
);

CREATE INDEX IDX_${table}_STATUS ON ${table} (CURRENT_STATUS, CURRENT_STAGE);
CREATE INDEX IDX_${table}_INPUT ON ${table} (INPUT_UNIQUE_ID);
CREATE INDEX IDX_${table}_UPDATED ON ${table} (LAST_UPDATED_AT);`
}

function displayNodeType(type: string): string {
  return type.split('_').map((part) => part.slice(0, 1).toUpperCase() + part.slice(1)).join(' ')
}

function hasRuntimeControl(type: BlwNodeKind): boolean {
  return ['activity', 'execute_child_node', 'email', 'sms', 'oracle_update', 'wait', 'delay', 'pause', 'escalation'].includes(type)
}

export default function BLWStudio({ open, nodeLabel, config, upstreamInputFields = [], upstreamPreviewRows = [], onClose, onSave, onOpenChildCanvas }: BLWStudioProps) {
  const [activeView, setActiveView] = useState<string>('designer')
  const initial = useMemo(() => normalizeConfig(resolveStoredBlwConfig(config), nodeLabel), [config, nodeLabel])
  const [draft, setDraft] = useState<BlwStudioConfig>(initial)
  const [nodes, setNodes, onNodesChange] = useNodesState<BlwNodeData>(initial.graphNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(initial.graphEdges)
  const [selectedNodeId, setSelectedNodeId] = useState<string>('start')
  const [selectedEdgeId, setSelectedEdgeId] = useState<string>('')
  const [trackerLoading, setTrackerLoading] = useState(false)
  const [trackerSummary, setTrackerSummary] = useState<any>(null)
  const [trackerDetailLoading, setTrackerDetailLoading] = useState(false)
  const [trackerDetail, setTrackerDetail] = useState<any>(null)
  const [trackerSearch, setTrackerSearch] = useState('')
  const [trackerDrillRows, setTrackerDrillRows] = useState<any[] | null>(null)
  const [trackerDrillLoading, setTrackerDrillLoading] = useState(false)
  const [trackerExportLoading, setTrackerExportLoading] = useState(false)
  const [dashboardTab, setDashboardTab] = useState('overview')
  const [dashboardRefreshSeconds, setDashboardRefreshSeconds] = useState(5)
  const [trackerPageSize, setTrackerPageSize] = useState(25)
  const trackerRefreshInFlightRef = useRef(false)
  const [childNodeConfigOpen, setChildNodeConfigOpen] = useState(false)
  const [conditionConfigOpen, setConditionConfigOpen] = useState(false)
  const [graphHistoryVersion, setGraphHistoryVersion] = useState(0)
  const wasOpenRef = useRef(false)
  const graphHistoryRef = useRef<{
    past: BlwGraphSnapshot[]
    future: BlwGraphSnapshot[]
    last: BlwGraphSnapshot | null
    applying: boolean
    timer: number | null
  }>({ past: [], future: [], last: null, applying: false, timer: null })

  useEffect(() => {
    if (!open) {
      wasOpenRef.current = false
      return
    }
    if (wasOpenRef.current) return
    wasOpenRef.current = true
    const next = normalizeConfig(resolveStoredBlwConfig(config), nodeLabel)
    const nextSnapshot = cloneBlwGraph(next.graphNodes, next.graphEdges)
    if (graphHistoryRef.current.timer) {
      window.clearTimeout(graphHistoryRef.current.timer)
    }
    graphHistoryRef.current = {
      past: [],
      future: [],
      last: nextSnapshot,
      applying: true,
      timer: null,
    }
    setDraft(next)
    setNodes(next.graphNodes)
    setEdges(next.graphEdges)
    setSelectedNodeId(next.graphNodes[0]?.id || '')
    setSelectedEdgeId('')
    setGraphHistoryVersion((value) => value + 1)
  }, [open, config, nodeLabel, setEdges, setNodes])

  const selectedNode = useMemo(() => nodes.find((node) => node.id === selectedNodeId) || null, [nodes, selectedNodeId])
  const selectedEdge = useMemo(() => edges.find((edge) => edge.id === selectedEdgeId) || null, [edges, selectedEdgeId])
  const upstreamNodeIds = useMemo(() => {
    const upstream = new Set<string>()
    if (!selectedNodeId) return upstream
    const stack = edges.filter((edge) => edge.target === selectedNodeId).map((edge) => edge.source)
    while (stack.length > 0) {
      const nodeId = stack.pop()
      if (!nodeId || upstream.has(nodeId)) continue
      upstream.add(nodeId)
      edges.forEach((edge) => {
        if (edge.target === nodeId && !upstream.has(edge.source)) stack.push(edge.source)
      })
    }
    return upstream
  }, [edges, selectedNodeId])
  const embeddedChildNodeOptions = useMemo(() => {
    const rawNodes = config?.embedded_workflow_nodes
    let childNodes: any[] = []
    if (Array.isArray(rawNodes)) {
      childNodes = rawNodes
    } else if (typeof rawNodes === 'string' && rawNodes.trim()) {
      try {
        const parsed = JSON.parse(rawNodes)
        childNodes = Array.isArray(parsed) ? parsed : []
      } catch {
        childNodes = []
      }
    }
    return childNodes
      .filter((node) => node && typeof node === 'object' && String(node.id || '').trim())
      .map((node) => {
        const data = node.data && typeof node.data === 'object' ? node.data : {}
        const nodeType = String(data.nodeType || node.nodeType || node.type || 'node')
        const label = String(data.label || node.label || node.id)
        return {
          value: String(node.id),
          label: `${label} (${displayNodeType(nodeType)})`,
        }
      })
  }, [config?.embedded_workflow_nodes])
  const conditionRouteTargetOptions = useMemo(() => nodes
    .filter((node) => node.id !== selectedNodeId && !['start'].includes(String(node.data?.blwType || '')))
    .map((node) => ({
      value: node.id,
      label: `${node.data?.label || node.id} (${displayNodeType(String(node.data?.blwType || 'activity'))})`,
    })), [nodes, selectedNodeId])
  const flowEdges = useMemo(() => edges.map((edge) => {
    const normalized = normalizeEdgeStyle(edge)
    if (edge.id !== selectedEdgeId) {
      return { ...normalized, selected: false }
    }
    return {
      ...normalized,
      selected: true,
      markerEnd: { type: MarkerType.ArrowClosed, color: '#38bdf8', width: 9, height: 9 },
      style: {
        ...(normalized.style || {}),
        stroke: '#38bdf8',
        strokeWidth: 1.2,
        strokeDasharray: '3 4',
      },
      labelStyle: {
        ...(normalized.labelStyle || {}),
        fill: '#e0f2fe',
        fontWeight: 600,
      },
      labelBgStyle: {
        ...(normalized.labelBgStyle || {}),
        fill: 'rgba(14, 116, 144, 0.72)',
        fillOpacity: 1,
      },
    }
  }), [edges, selectedEdgeId])
  const availableFields = useMemo(() => uniqueStrings([
    upstreamInputFields,
    draft.inputFields,
    draft.inputFieldMappings.map((item) => item.field),
    config?.inputFields,
    Array.isArray(config?.inputFieldMappings)
      ? config.inputFieldMappings.map((item) => asRecord(item).field || asRecord(item).name || asRecord(item).source)
      : [],
    config?._detected_columns,
    config?._source_schema_fields,
    fieldsFromRows(upstreamPreviewRows),
    fieldsFromRows(config?._preview_rows),
    fieldsFromRows(config?.preview_rows),
    fieldsFromRows(config?.sample_rows),
    draft.uniqueIdField,
    ['TRANSACTIONID', 'AGENTCODE', 'AMOUNT', 'STATUS', 'TXNDATE', 'SERVERTIME', 'CUSTACCOUNTNUMBER'],
  ]), [config, draft.inputFieldMappings, draft.inputFields, draft.uniqueIdField, upstreamInputFields, upstreamPreviewRows])
  const childOutputPathFields = useMemo(() => uniqueStrings(
    nodes.flatMap((node) => {
      const data = node.data || {}
      if (data.blwType !== 'execute_child_node') return []
      if (!upstreamNodeIds.has(node.id)) return []
      return outputReceivePathHints(safeJson(data.actionJson).outputPath || '$.child_output')
    })
  ), [nodes, upstreamNodeIds])
  const fieldPickerOptions = useMemo(() => availableFields.map((field) => ({ value: field, label: field })), [availableFields])
  const pathFieldOptions = useMemo(() => uniqueStrings([availableFields, childOutputPathFields]), [availableFields, childOutputPathFields])
  const pathPickerOptions = useMemo(() => [
    ...(availableFields.length > 0 ? [{ label: 'Upstream fields', options: fieldPickerOptions }] : []),
    ...(childOutputPathFields.length > 0
      ? [{ label: 'Child output paths', options: childOutputPathFields.map((field) => ({ value: field, label: field })) }]
      : []),
  ], [availableFields.length, childOutputPathFields, fieldPickerOptions])
  const variableOptions = useMemo(() => draft.instanceVariables.map((item) => item.name).filter(Boolean), [draft.instanceVariables])
  const validationIssues = useMemo(() => validateBlwGraph(nodes, edges, draft), [draft, edges, nodes])
  const validationErrors = validationIssues.filter((issue) => issue.severity === 'error')
  const canUndoGraph = useMemo(() => graphHistoryRef.current.past.length > 0, [graphHistoryVersion])
  const canRedoGraph = useMemo(() => graphHistoryRef.current.future.length > 0, [graphHistoryVersion])
  const selectedCopyableNodes = useMemo(() => {
    const multiSelected = nodes.filter((node) => Boolean(node.selected))
    const candidates = multiSelected.length > 0
      ? multiSelected
      : selectedNode
        ? [selectedNode]
        : []
    return candidates.filter((node) => !['start', 'end'].includes(String(node.data?.blwType || '')))
  }, [nodes, selectedNode])

  const metrics = useMemo(() => {
    const statuses = nodes.reduce<Record<string, number>>((acc, node) => {
      const status = String(node.data?.status || 'draft')
      acc[status] = (acc[status] || 0) + 1
      return acc
    }, {})
    return {
      total: nodes.length,
      routes: edges.length,
      parallelBlocks: nodes.filter((node) => node.data?.blwType === 'parallel_start').length,
      joins: nodes.filter((node) => node.data?.blwType === 'parallel_join').length,
      waiting: statuses.waiting || 0,
      escalated: statuses.escalated || 0,
    }
  }, [edges.length, nodes])

  const trackerRows = useMemo(() => {
    if (trackerSearch.trim() && Array.isArray(trackerDrillRows)) return trackerDrillRows
    const rows = Array.isArray(trackerSummary?.rows) ? trackerSummary.rows : []
    const q = trackerSearch.trim().toLowerCase()
    if (!q) return rows
    return rows.filter((row: any) => Object.values(row || {}).some((value) => String(value || '').toLowerCase().includes(q)))
  }, [trackerDrillRows, trackerSearch, trackerSummary])

  const dashboardRows = useMemo<any[]>(() => Array.isArray(trackerSummary?.rows) ? trackerSummary.rows : [], [trackerSummary])
  const dashboardInsights = useMemo(() => {
    const chartTextColor = '#cbd5e1'
    const chartSubtleColor = '#94a3b8'
    const chartGridColor = 'rgba(148,163,184,0.16)'
    const countBy = (field: string) => dashboardRows.reduce((acc: Record<string, number>, row: any) => {
      const key = String(row?.[field] || 'UNKNOWN')
      acc[key] = (acc[key] || 0) + 1
      return acc
    }, {} as Record<string, number>)
    const statusCounts: Record<string, number> = Object.keys(trackerSummary?.summary?.status_counts || {}).length
      ? trackerSummary.summary.status_counts
      : countBy('current_status')
    const stageCounts: Record<string, number> = Object.keys(trackerSummary?.summary?.stage_counts || {}).length
      ? trackerSummary.summary.stage_counts
      : countBy('current_stage')
    const nodeCounts: Record<string, number> = Object.keys(trackerSummary?.summary?.node_counts || {}).length
      ? trackerSummary.summary.node_counts
      : countBy('business_node_id')
    const errorCounts = dashboardRows.reduce((acc: Record<string, number>, row: any) => {
      const code = String(row?.error_code || row?.error_message || '').trim()
      if (!code) return acc
      acc[code] = (acc[code] || 0) + 1
      return acc
    }, {} as Record<string, number>)
    const durations = dashboardRows.map((row: any) => Number(row?.duration_seconds || 0)).filter((value: number) => Number.isFinite(value) && value >= 0)
    const waitLeft = dashboardRows.map((row: any) => Number(row?.wait_remaining_seconds || 0)).filter((value: number) => Number.isFinite(value) && value > 0)
    const sortedDurations = [...durations].sort((a, b) => a - b)
    const avg = durations.length ? durations.reduce((sum: number, value: number) => sum + value, 0) / durations.length : 0
    const p95 = sortedDurations.length ? sortedDurations[Math.min(sortedDurations.length - 1, Math.floor(sortedDurations.length * 0.95))] : 0
    const slowest = [...dashboardRows]
      .sort((a: any, b: any) => Number(b?.duration_seconds || 0) - Number(a?.duration_seconds || 0))
      .slice(0, 8)
    const errorRows = Object.entries(errorCounts)
      .sort((a, b) => Number(b[1]) - Number(a[1]))
      .map(([error, count]) => ({ error, count }))
    const stageRows = Object.entries(stageCounts)
      .sort((a, b) => Number(b[1]) - Number(a[1]))
      .map(([stage, count]) => ({ stage, count }))
    const statusRows = Object.entries(statusCounts)
      .sort((a, b) => Number(b[1]) - Number(a[1]))
      .map(([status, count]) => ({ status, count }))
    const statusChart = {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'item' },
      legend: { bottom: 0, textStyle: { color: chartSubtleColor, fontSize: 10 } },
      series: [{
        type: 'pie',
        radius: ['46%', '70%'],
        center: ['50%', '42%'],
        data: statusRows.map((row) => ({ name: row.status, value: row.count })),
        label: { color: chartTextColor, fontSize: 10 },
      }],
    }
    const stageChart = {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      grid: { left: 6, right: 10, top: 18, bottom: 10, containLabel: true },
      xAxis: { type: 'value', axisLabel: { color: chartSubtleColor, fontSize: 10 }, splitLine: { lineStyle: { color: chartGridColor } } },
      yAxis: { type: 'category', data: stageRows.slice(0, 10).map((row) => row.stage), axisLabel: { color: chartSubtleColor, fontSize: 10 } },
      series: [{ type: 'bar', data: stageRows.slice(0, 10).map((row) => row.count), itemStyle: { color: '#38bdf8' }, barWidth: 12 }],
    }
    const nodeChart = {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      grid: { left: 6, right: 10, top: 18, bottom: 10, containLabel: true },
      xAxis: { type: 'value', axisLabel: { color: chartSubtleColor, fontSize: 10 }, splitLine: { lineStyle: { color: chartGridColor } } },
      yAxis: { type: 'category', data: Object.entries(nodeCounts).sort((a, b) => Number(b[1]) - Number(a[1])).slice(0, 10).map(([node]) => node), axisLabel: { color: chartSubtleColor, fontSize: 10 } },
      series: [{ type: 'bar', data: Object.entries(nodeCounts).sort((a, b) => Number(b[1]) - Number(a[1])).slice(0, 10).map(([, count]) => count), itemStyle: { color: '#a78bfa' }, barWidth: 12 }],
    }
    const latencyBuckets = [
      { name: '0-5s', value: durations.filter((value: number) => value <= 5).length },
      { name: '5-30s', value: durations.filter((value: number) => value > 5 && value <= 30).length },
      { name: '30-120s', value: durations.filter((value: number) => value > 30 && value <= 120).length },
      { name: '>120s', value: durations.filter((value: number) => value > 120).length },
    ]
    const latencyChart = {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      grid: { left: 6, right: 10, top: 18, bottom: 10, containLabel: true },
      xAxis: { type: 'category', data: latencyBuckets.map((bucket) => bucket.name), axisLabel: { color: chartSubtleColor, fontSize: 10 } },
      yAxis: { type: 'value', axisLabel: { color: chartSubtleColor, fontSize: 10 }, splitLine: { lineStyle: { color: chartGridColor } } },
      series: [{ type: 'bar', data: latencyBuckets.map((bucket) => bucket.value), itemStyle: { color: '#f59e0b' }, barWidth: 22 }],
    }
    return {
      statusRows,
      stageRows,
      errorRows,
      slowest,
      total: Number(trackerSummary?.summary?.total || dashboardRows.length || 0),
      failed: statusRows.filter((row) => String(row.status).toLowerCase().includes('fail')).reduce((sum, row) => sum + Number(row.count || 0), 0),
      running: statusRows.filter((row) => ['running', 'pending', 'waiting'].includes(String(row.status).toLowerCase())).reduce((sum, row) => sum + Number(row.count || 0), 0),
      avg,
      p95,
      waitQueue: waitLeft.length,
      statusChart,
      stageChart,
      nodeChart,
      latencyChart,
    }
  }, [dashboardRows, trackerSummary])

  const refreshTrackerSummary = useCallback(async (options?: { refreshCounts?: boolean }) => {
    if (trackerRefreshInFlightRef.current) return
    trackerRefreshInFlightRef.current = true
    setTrackerLoading(true)
    try {
      const refreshCounts = Boolean(options?.refreshCounts)
      const payloadConfig = {
        ...(config || {}),
        workflow_tracking_table: draft.trackerTable,
        workflow_unique_id_field: draft.uniqueIdField,
        workflow_parallel_enabled: draft.parallelEnabled,
        workflow_max_parallel_branches: draft.maxParallelBranches,
        workflow_max_iterations: draft.maxIterations,
        workflow_max_retry_attempts: draft.maxRetryAttempts,
        blw_studio_config: {
          trackerTable: draft.trackerTable,
          uniqueIdField: draft.uniqueIdField,
          trackingMode: draft.trackingMode,
        },
      }
      const summary = await api.getBlwTrackerSummary({
        config: payloadConfig,
        limit: 100,
        fast: true,
        counts_ttl_seconds: refreshCounts ? 5 : 30,
        refresh_counts: refreshCounts,
      })
      setTrackerSummary(summary)
    } finally {
      trackerRefreshInFlightRef.current = false
      setTrackerLoading(false)
    }
  }, [config, draft.maxIterations, draft.maxParallelBranches, draft.maxRetryAttempts, draft.parallelEnabled, draft.trackerTable, draft.trackingMode, draft.uniqueIdField])

  const buildTrackerPayloadConfig = useCallback(() => ({
    ...(config || {}),
    workflow_tracking_table: draft.trackerTable,
    workflow_unique_id_field: draft.uniqueIdField,
    workflow_parallel_enabled: draft.parallelEnabled,
    workflow_max_parallel_branches: draft.maxParallelBranches,
    workflow_max_iterations: draft.maxIterations,
    workflow_max_retry_attempts: draft.maxRetryAttempts,
    blw_studio_config: {
      ...draft,
      graphNodes: nodes.map((node) => ({ ...node, selected: false, dragging: false })),
      graphEdges: edges.map((edge) => ({ ...edge, selected: false })),
    },
  }), [config, draft, edges, nodes])

  const openTrackerDetail = useCallback(async (runId: string) => {
    if (!runId) return
    setTrackerDetailLoading(true)
    try {
      const detail = await api.getBlwTrackerDetail({ config: buildTrackerPayloadConfig(), run_id: runId })
      setTrackerDetail(detail)
    } finally {
      setTrackerDetailLoading(false)
    }
  }, [buildTrackerPayloadConfig])

  const applyTrackerAction = useCallback(async (runId: string, action: 'resume' | 'retry' | 'pause' | 'terminate') => {
    if (!runId) return
    setTrackerLoading(true)
    try {
      const result = await api.applyBlwTrackerAction({ config: buildTrackerPayloadConfig(), run_id: runId, action })
      if (result?.ok) {
        notification.success({ message: `BLW ${action} requested`, placement: 'bottomRight' })
      } else {
        notification.error({ message: `BLW ${action} failed`, description: String(result?.message || 'No tracker row updated'), placement: 'bottomRight' })
      }
      await refreshTrackerSummary({ refreshCounts: true })
      if (trackerDetail?.row?.run_id === runId) {
        await openTrackerDetail(runId)
      }
    } finally {
      setTrackerLoading(false)
    }
  }, [buildTrackerPayloadConfig, openTrackerDetail, refreshTrackerSummary, trackerDetail])

  useEffect(() => {
    if (!open || activeView !== 'dashboard') return
    refreshTrackerSummary({ refreshCounts: true })
    if (!dashboardRefreshSeconds) return
    const timer = window.setInterval(() => {
      if (document.hidden) return
      refreshTrackerSummary({ refreshCounts: false })
    }, dashboardRefreshSeconds * 1000)
    return () => window.clearInterval(timer)
  }, [activeView, dashboardRefreshSeconds, open, refreshTrackerSummary])

  const drillIntoTracker = useCallback((value: string) => {
    const next = String(value || '').trim()
    if (!next) return
    setTrackerSearch(next)
    setDashboardTab('tracker')
    setTrackerDrillLoading(true)
    api.exportBlwTrackerRows({
      config: buildTrackerPayloadConfig(),
      search: next,
      limit: 5000,
    })
      .then((result) => {
        const rows = Array.isArray(result?.rows) ? result.rows : []
        setTrackerDrillRows(rows)
        if (rows.length <= 0) {
          notification.warning({
            message: 'No BLW drill records found',
            description: `No tracker rows matched "${next}".`,
            placement: 'bottomRight',
          })
        }
      })
      .catch((exc: any) => {
        setTrackerDrillRows([])
        notification.error({
          message: 'BLW drill failed',
          description: String(exc?.message || exc || 'Failed to load drill rows'),
          placement: 'bottomRight',
        })
      })
      .finally(() => setTrackerDrillLoading(false))
  }, [buildTrackerPayloadConfig])

  const downloadTrackerRows = useCallback(async () => {
    setTrackerExportLoading(true)
    try {
      const search = String(trackerSearch || '').trim()
      const result = await api.exportBlwTrackerRows({
        config: buildTrackerPayloadConfig(),
        search,
        limit: 5000,
      })
      const rows = Array.isArray(result?.rows) ? result.rows : []
      if (rows.length <= 0) {
        notification.warning({
          message: 'No BLW tracker records to download',
          description: search ? `No rows matched "${search}".` : 'Tracker export returned no rows.',
          placement: 'bottomRight',
        })
        return
      }
      const suffix = search ? `drill-${search.replace(/[^A-Za-z0-9_-]+/g, '_').slice(0, 40)}` : 'latest'
      downloadBlwRowsAsCsv(rows, `blw-tracker-${suffix}`)
      notification.success({
        message: 'BLW tracker CSV downloaded',
        description: `${rows.length.toLocaleString()} record(s) exported.`,
        placement: 'bottomRight',
      })
    } catch (exc: any) {
      notification.error({
        message: 'BLW tracker download failed',
        description: String(exc?.message || exc || 'Export failed'),
        placement: 'bottomRight',
      })
    } finally {
      setTrackerExportLoading(false)
    }
  }, [buildTrackerPayloadConfig, trackerSearch])

  const updateDraft = useCallback((patch: Partial<BlwStudioConfig>) => {
    setDraft((prev) => ({ ...prev, ...patch }))
  }, [])

  const updateSelectedNodeData = useCallback((patch: Partial<BlwNodeData>) => {
    if (!selectedNodeId) return
    setNodes((current) => current.map((node) => (
      node.id === selectedNodeId
        ? { ...node, data: { ...node.data, ...patch } }
        : node
    )))
  }, [selectedNodeId, setNodes])

  const updateSelectedEdge = useCallback((patch: Partial<Edge>) => {
    if (!selectedEdgeId) return
    setEdges((current) => current.map((edge) => (
      edge.id === selectedEdgeId ? { ...edge, ...patch } : edge
    )))
  }, [selectedEdgeId, setEdges])

  const updateConditionRouteTargets = useCallback((
    sourceNodeId: string,
    trueTargetIds: string[],
    falseTargetIds: string[],
    trueLabel = 'approved',
    falseLabel = 'rejected',
    groupId = '',
  ) => {
    const source = String(sourceNodeId || '').trim()
    if (!source) return
    const safeGroup = String(groupId || '').trim().replace(/[^A-Za-z0-9_-]/g, '_')
    const handleFor = (route: 'true' | 'false') => safeGroup ? `group_${safeGroup}_${route}` : route
    const labelPrefix = safeGroup ? `${safeGroup} ` : ''
    const makeEdge = (route: 'true' | 'false', target: string, index: number): Edge => normalizeEdgeStyle({
      id: `route_${source}_${handleFor(route)}_${target}_${index}`,
      source,
      target,
      sourceHandle: handleFor(route),
      targetHandle: 'input',
      type: 'smoothstep',
      label: `${labelPrefix}${route === 'true' ? trueLabel : falseLabel}${index > 0 ? ` ${index + 1}` : ''}`,
      markerEnd: { type: MarkerType.ArrowClosed, color: blwEdgeColor, width: 8, height: 8 },
      style: blwEdgeStyle,
      data: { route, groupId: safeGroup, position: index + 1 },
    } as Edge)
    setEdges((current) => {
      const retained = current.filter((edge) => !(
        edge.source === source
        && (
          safeGroup
            ? String(edge.sourceHandle || '').startsWith(`group_${safeGroup}_`)
            : ['true', 'false'].includes(String(edge.sourceHandle || ''))
        )
      ))
      const nextTrue = trueTargetIds
        .map((target) => String(target || '').trim())
        .filter((target, index, arr) => target && target !== source && arr.indexOf(target) === index)
        .map((target, index) => makeEdge('true', target, index))
      const nextFalse = falseTargetIds
        .map((target) => String(target || '').trim())
        .filter((target, index, arr) => target && target !== source && arr.indexOf(target) === index)
        .map((target, index) => makeEdge('false', target, index))
      return [...retained, ...nextTrue, ...nextFalse]
    })
  }, [setEdges])

  const onConnect = useCallback((connection: Connection) => {
    const sourceHandle = String(connection.sourceHandle || '').trim()
    const routeLabel = sourceHandle === 'true'
      ? 'true'
      : sourceHandle === 'false'
        ? 'false'
        : sourceHandle.includes('_true')
          ? 'true'
          : sourceHandle.includes('_false')
            ? 'false'
            : 'route'
    setEdges((current) => addEdge(normalizeEdgeStyle({
      ...connection,
      id: `route_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
      type: 'smoothstep',
      label: routeLabel,
      markerEnd: { type: MarkerType.ArrowClosed, color: blwEdgeColor, width: 8, height: 8 },
      style: blwEdgeStyle,
    } as Edge), current))
  }, [setEdges])

  const addCanvasNode = (type: BlwNodeKind, label: string) => {
    const next = createGraphNode(type, label, 520 + ((nodes.length % 3) - 1) * 240, 120 + nodes.length * 80)
    setNodes((current) => [...current, next])
    setSelectedNodeId(next.id)
    setSelectedEdgeId('')
  }

  const removeSelectedNode = () => {
    if (!selectedNode || selectedNode.id === 'start' || selectedNode.id === 'end') return
    setNodes((current) => current.filter((node) => node.id !== selectedNode.id))
    setEdges((current) => current.filter((edge) => edge.source !== selectedNode.id && edge.target !== selectedNode.id))
    setSelectedNodeId('start')
  }

  const removeSelectedEdge = () => {
    if (!selectedEdge) return
    setEdges((current) => current.filter((edge) => edge.id !== selectedEdge.id))
    setSelectedEdgeId('')
  }

  const applyGraphSnapshot = useCallback((snapshot: BlwGraphSnapshot) => {
    const history = graphHistoryRef.current
    if (history.timer) {
      window.clearTimeout(history.timer)
      history.timer = null
    }
    history.applying = true
    history.last = cloneBlwGraph(snapshot.nodes, snapshot.edges)
    setNodes(snapshot.nodes)
    setEdges(snapshot.edges)
    setSelectedNodeId(snapshot.nodes[0]?.id || '')
    setSelectedEdgeId('')
    setGraphHistoryVersion((value) => value + 1)
  }, [setEdges, setNodes])

  const undoGraph = useCallback(() => {
    const history = graphHistoryRef.current
    const previous = history.past.pop()
    if (!previous) return
    const current = cloneBlwGraph(nodes, edges)
    history.future = [current, ...history.future].slice(0, 80)
    applyGraphSnapshot(previous)
  }, [applyGraphSnapshot, edges, nodes])

  const redoGraph = useCallback(() => {
    const history = graphHistoryRef.current
    const next = history.future.shift()
    if (!next) return
    const current = cloneBlwGraph(nodes, edges)
    history.past = [...history.past, current].slice(-80)
    applyGraphSnapshot(next)
  }, [applyGraphSnapshot, edges, nodes])

  const copySelectedNodes = useCallback(() => {
    if (selectedCopyableNodes.length === 0) return
    const timestamp = Date.now()
    const idMap = new Map<string, string>()
    selectedCopyableNodes.forEach((node, index) => {
      idMap.set(node.id, `${node.id}_copy_${timestamp}_${index}`)
    })
    const copiedNodes = selectedCopyableNodes.map((node, index) => ({
      ...node,
      id: idMap.get(node.id) || `${node.id}_copy_${timestamp}_${index}`,
      selected: true,
      dragging: false,
      position: {
        x: (node.position?.x || 0) + 48,
        y: (node.position?.y || 0) + 48,
      },
      data: {
        ...node.data,
        label: `${String(node.data?.label || displayNodeType(String(node.data?.blwType || 'activity')))} Copy`,
      },
    }))
    const copiedEdges = edges
      .filter((edge) => idMap.has(edge.source) && idMap.has(edge.target))
      .map((edge, index) => normalizeEdgeStyle({
        ...edge,
        id: `${edge.id}_copy_${timestamp}_${index}`,
        source: idMap.get(edge.source) || edge.source,
        target: idMap.get(edge.target) || edge.target,
        selected: false,
      } as Edge))
    setNodes((current) => [
      ...current.map((node) => ({ ...node, selected: false })),
      ...copiedNodes,
    ])
    setEdges((current) => [...current, ...copiedEdges])
    setSelectedNodeId(copiedNodes[0]?.id || '')
    setSelectedEdgeId('')
  }, [edges, selectedCopyableNodes, setEdges, setNodes])

  useEffect(() => {
    if (!open || activeView !== 'designer') return
    const history = graphHistoryRef.current
    const snapshot = cloneBlwGraph(nodes, edges)
    const signature = blwGraphSignature(snapshot)
    const lastSignature = history.last ? blwGraphSignature(history.last) : ''
    if (history.applying) {
      history.last = snapshot
      history.applying = false
      setGraphHistoryVersion((value) => value + 1)
      return
    }
    if (!history.last) {
      history.last = snapshot
      setGraphHistoryVersion((value) => value + 1)
      return
    }
    if (signature === lastSignature) return
    if (history.timer) {
      window.clearTimeout(history.timer)
    }
    history.timer = window.setTimeout(() => {
      if (history.last) {
        history.past = [...history.past, history.last].slice(-80)
      }
      history.future = []
      history.last = snapshot
      history.timer = null
      setGraphHistoryVersion((value) => value + 1)
    }, 250)
    return () => {
      if (history.timer) {
        window.clearTimeout(history.timer)
        history.timer = null
      }
    }
  }, [activeView, edges, nodes, open])

  useEffect(() => {
    if (!open || activeView !== 'designer') return
    const isEditableTarget = (target: EventTarget | null) => {
      const element = target instanceof HTMLElement ? target : null
      if (!element) return false
      const tag = element.tagName.toUpperCase()
      return Boolean(
        element.isContentEditable
        || tag === 'INPUT'
        || tag === 'TEXTAREA'
        || tag === 'SELECT'
        || element.closest('.ant-select-selector')
        || element.closest('.ant-input')
        || element.closest('.monaco-editor')
      )
    }
    const handleKeyDown = (event: KeyboardEvent) => {
      const isModifier = event.metaKey || event.ctrlKey
      if (isModifier && event.key.toLowerCase() === 'z') {
        if (isEditableTarget(event.target)) return
        event.preventDefault()
        event.stopPropagation()
        event.stopImmediatePropagation?.()
        if (event.shiftKey) {
          redoGraph()
        } else {
          undoGraph()
        }
        return
      }
      if (isModifier && event.key.toLowerCase() === 'y') {
        if (isEditableTarget(event.target)) return
        event.preventDefault()
        event.stopPropagation()
        event.stopImmediatePropagation?.()
        redoGraph()
        return
      }
      if (isModifier && event.key.toLowerCase() === 'c') {
        if (isEditableTarget(event.target)) return
        if (selectedCopyableNodes.length === 0) return
        event.preventDefault()
        event.stopPropagation()
        event.stopImmediatePropagation?.()
        copySelectedNodes()
        return
      }
      if (event.key !== 'Delete' && event.key !== 'Backspace') return
      if (isEditableTarget(event.target)) return
      event.preventDefault()
      event.stopPropagation()
      event.stopImmediatePropagation?.()
      if (selectedEdgeId) {
        setEdges((current) => current.filter((edge) => edge.id !== selectedEdgeId))
        setSelectedEdgeId('')
        return
      }
      if (selectedNodeId && selectedNodeId !== 'start' && selectedNodeId !== 'end') {
        setNodes((current) => current.filter((node) => node.id !== selectedNodeId))
        setEdges((current) => current.filter((edge) => edge.source !== selectedNodeId && edge.target !== selectedNodeId))
        setSelectedNodeId('start')
      }
    }
    window.addEventListener('keydown', handleKeyDown, true)
    return () => window.removeEventListener('keydown', handleKeyDown, true)
  }, [activeView, copySelectedNodes, open, redoGraph, selectedCopyableNodes.length, selectedEdgeId, selectedNodeId, setEdges, setNodes, undoGraph])

  const updateInstanceVariable = (index: number, patch: Partial<BlwInstanceVariable>) => {
    updateDraft({
      instanceVariables: draft.instanceVariables.map((item, idx) => idx === index ? { ...item, ...patch } : item),
    })
  }

  const addInstanceVariable = () => {
    const nextIndex = draft.instanceVariables.length + 1
    updateDraft({
      instanceVariables: [
        ...draft.instanceVariables,
        { name: `var_${nextIndex}`, source: 'input_field', field: availableFields[0] || draft.uniqueIdField || '', defaultValue: '' },
      ],
    })
  }

  const removeInstanceVariable = (index: number) => {
    updateDraft({ instanceVariables: draft.instanceVariables.filter((_, idx) => idx !== index) })
  }

  const applyInputFieldMappings = (inputFieldMappings: BlwInputFieldMapping[]) => {
    updateDraft({
      inputFieldMappings,
      inputFields: inputFieldMappings.map((item) => item.field).filter(Boolean),
    })
  }

  const updateInputFieldMapping = (index: number, patch: Partial<BlwInputFieldMapping>) => {
    const next = draft.inputFieldMappings.map((item, idx) => {
      if (idx !== index) return item
      const merged = { ...item, ...patch }
      if (patch.field && (!merged.alias || merged.alias === item.field)) {
        merged.alias = patch.field
      }
      return merged
    })
    applyInputFieldMappings(next)
  }

  const addInputFieldMapping = () => {
    const used = new Set(draft.inputFieldMappings.map((item) => item.field))
    const field = availableFields.find((item) => !used.has(item)) || availableFields[0] || ''
    applyInputFieldMappings([
      ...draft.inputFieldMappings,
      { field, alias: field },
    ])
  }

  const removeInputFieldMapping = (index: number) => {
    applyInputFieldMappings(draft.inputFieldMappings.filter((_, idx) => idx !== index))
  }

  const moveInputFieldMapping = (index: number, direction: -1 | 1) => {
    const nextIndex = index + direction
    if (nextIndex < 0 || nextIndex >= draft.inputFieldMappings.length) return
    const next = [...draft.inputFieldMappings]
    const [item] = next.splice(index, 1)
    next.splice(nextIndex, 0, item)
    applyInputFieldMappings(next)
  }

  const inputFieldOptionsForRow = (index: number) => {
    const selectedByOtherRows = new Set(
      draft.inputFieldMappings
        .map((item, idx) => idx === index ? '' : item.field)
        .filter(Boolean),
    )
    const currentField = draft.inputFieldMappings[index]?.field
    return availableFields
      .map((field) => {
        const selectedElsewhere = field !== currentField && selectedByOtherRows.has(field)
        return {
          value: field,
          disabled: selectedElsewhere,
          label: (
            <Space size={6}>
              {field === currentField || selectedElsewhere ? <CheckOutlined style={{ fontSize: 10, color: 'var(--app-accent)' }} /> : <span style={{ width: 10, display: 'inline-block' }} />}
              <span>{field}</span>
              {selectedElsewhere ? <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>selected</Text> : null}
            </Space>
          ),
        }
      })
  }

  const save = () => {
    if (validationErrors.length > 0) {
      notification.error({
        message: 'Fix BLW validation errors before saving',
        description: validationErrors.slice(0, 3).map((issue) => `${issue.area}: ${issue.message}`).join(' | '),
        placement: 'bottomRight',
      })
      setActiveView('dashboard')
      return false
    }
    const cleanedNodes = nodes.map((node) => ({ ...node, selected: false, dragging: false }))
    const cleanedEdges = edges.map((edge) => ({ ...edge, selected: false }))
    const nextConfig: BlwStudioConfig = {
      ...draft,
      graphNodes: cleanedNodes,
      graphEdges: cleanedEdges,
      steps: stepsFromGraph(cleanedNodes, cleanedEdges, draft.maxRetryAttempts, draft.maxIterations),
    }
    setDraft(nextConfig)
    onSave(nextConfig)
    notification.success({ message: 'BLW visual workflow saved', placement: 'bottomRight' })
    return true
  }

  const openChildCanvasFromStudio = useCallback((event?: MouseEvent<HTMLElement>) => {
    event?.stopPropagation()
    if (!onOpenChildCanvas) return
    const saved = save()
    if (!saved) return
    onOpenChildCanvas()
    onClose()
  }, [onClose, onOpenChildCanvas, save])

  const ddl = useMemo(() => buildOracleDdl(draft), [draft])
  const nodeRows = useMemo(() => stepsFromGraph(nodes, edges, draft.maxRetryAttempts, draft.maxIterations), [draft.maxIterations, draft.maxRetryAttempts, edges, nodes])
  const renderInstanceVariableEditor = () => (
    <Space direction="vertical" size={8} style={{ width: '100%' }}>
      <Space style={{ width: '100%', justifyContent: 'space-between' }}>
        <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Instance Variables</Text>
        <Button size="small" icon={<PlusOutlined />} onClick={addInstanceVariable}>Add</Button>
      </Space>
      <Table
        size="small"
        rowKey={(_, index) => String(index)}
        pagination={false}
        dataSource={draft.instanceVariables}
        columns={[
          {
            title: 'Name',
            render: (_, row, index) => <Input size="small" value={row.name} onChange={(event) => updateInstanceVariable(index, { name: event.target.value })} />,
          },
          {
            title: 'Source',
            width: 126,
            render: (_, row, index) => (
              <Select
                size="small"
                value={row.source}
                style={{ width: '100%' }}
                options={[
                  { value: 'input_field', label: 'Input field' },
                  { value: 'expression', label: 'Expression' },
                  { value: 'static', label: 'Static' },
                ]}
                onChange={(source) => updateInstanceVariable(index, { source })}
              />
            ),
          },
          {
            title: 'Value',
            render: (_, row, index) => row.source === 'input_field' ? (
              <Select
                size="small"
                showSearch
                value={row.field || undefined}
                placeholder="Select input field/path"
                optionFilterProp="label"
                options={pathPickerOptions}
                onChange={(field) => updateInstanceVariable(index, { field })}
              />
            ) : (
              <Input size="small" value={row.expression || row.defaultValue || ''} onChange={(event) => updateInstanceVariable(index, row.source === 'static' ? { defaultValue: event.target.value } : { expression: event.target.value })} />
            ),
          },
          {
            title: '',
            width: 44,
            render: (_, __, index) => <Button size="small" danger icon={<DeleteOutlined />} onMouseDown={(event) => event.stopPropagation()} onClick={(event) => { event.stopPropagation(); removeInstanceVariable(index) }} />,
          },
        ]}
      />
    </Space>
  )

  const renderDataContract = () => {
    if (!selectedNode || selectedNode.data.blwType === 'start' || selectedNode.data.blwType === 'end') return null
    const input = safeJson(selectedNode.data.inputContract, { from: 'previous_output', fields: 'all' })
    const validation = safeJson(selectedNode.data.validationRules, { required: [], rules: [], on_invalid: 'escalate' })
    const output = safeJson(selectedNode.data.outputContract, { to: 'workflow_context', fields: 'all' })
    const inputFields = toStringArray(input.fields)
    const outputFields = toStringArray(output.fields)
    const contextPaths = toStringArray(input.context_paths)
    const requiredFields = toStringArray(validation.required)
    return (
      <>
        <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
        <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Data Contract</Text>
        <div style={{ margin: '6px 0 10px', padding: '8px 10px', border: '1px solid var(--app-border)', borderRadius: 8, background: 'var(--app-shell-bg)' }}>
          <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
            Configure what this node receives, validates, and writes back. Empty field lists mean all fields.
          </Text>
        </div>
        <Form.Item label="Input source">
          <Select
            value={String(input.from || 'previous_output')}
            options={[
              { value: 'previous_output', label: 'Previous node output' },
              { value: 'workflow_context', label: 'Workflow context' },
              { value: 'parent_payload', label: 'Parent input payload' },
              { value: 'static', label: 'Static values' },
            ]}
            onChange={(from) => updateSelectedNodeData({ inputContract: jsonPatch(selectedNode.data.inputContract, { from }) })}
          />
        </Form.Item>
        <Form.Item label="Fields received">
          <Select
            mode="tags"
            value={inputFields}
            placeholder="Blank = all fields"
            options={fieldPickerOptions}
            onChange={(fields) => updateSelectedNodeData({ inputContract: jsonPatch(selectedNode.data.inputContract, { fields: fields.length ? fields : 'all' }) })}
          />
        </Form.Item>
        <Form.Item label="Context paths">
          <Select
            mode="tags"
            value={contextPaths}
            placeholder="Optional JSON paths, e.g. $.customer"
            optionFilterProp="label"
            options={pathPickerOptions}
            onChange={(context_paths) => updateSelectedNodeData({ inputContract: jsonPatch(selectedNode.data.inputContract, { context_paths }) })}
          />
        </Form.Item>
        <Form.Item label="Required fields">
          <Select
            mode="tags"
            value={requiredFields}
            placeholder="Fields that must exist before running"
            options={fieldPickerOptions}
            onChange={(required) => updateSelectedNodeData({ validationRules: jsonPatch(selectedNode.data.validationRules, { required }) })}
          />
        </Form.Item>
        <Form.Item label="Validation expression">
          <Input
            value={firstRuleExpression(validation.rules)}
            placeholder="Example: field('AMOUNT') > 0 or var('risk_status') == 'OK'"
            onChange={(event) => updateSelectedNodeData({
              validationRules: jsonPatch(selectedNode.data.validationRules, {
                rules: event.target.value ? [{ expression: event.target.value }] : [],
              }),
            })}
          />
        </Form.Item>
        <Form.Item label="If validation fails">
          <Select
            value={String(validation.on_invalid || 'escalate')}
            options={[
              { value: 'skip', label: 'Skip this node' },
              { value: 'fail', label: 'Fail workflow' },
              { value: 'escalate', label: 'Escalate' },
              { value: 'pause', label: 'Pause workflow' },
            ]}
            onChange={(on_invalid) => updateSelectedNodeData({ validationRules: jsonPatch(selectedNode.data.validationRules, { on_invalid }) })}
          />
        </Form.Item>
        <Form.Item label="Output target">
          <Select
            value={String(output.to || 'workflow_context')}
            options={[
              { value: 'workflow_context', label: 'Workflow context' },
              { value: 'step_output', label: 'This step output' },
              { value: 'oracle_tracker', label: 'Oracle tracker JSON' },
              { value: 'parent_output', label: 'Parent output' },
            ]}
            onChange={(to) => updateSelectedNodeData({ outputContract: jsonPatch(selectedNode.data.outputContract, { to }) })}
          />
        </Form.Item>
        <Form.Item label="Output write path">
          <Input
            value={String(output.write_path || '')}
            placeholder="Example: $.steps.risk_score"
            onChange={(event) => updateSelectedNodeData({ outputContract: jsonPatch(selectedNode.data.outputContract, { write_path: event.target.value }) })}
          />
        </Form.Item>
        <Form.Item label="Output fields">
          <Select
            mode="tags"
            value={outputFields}
            placeholder="Blank = all generated fields"
            options={[...availableFields, ...variableOptions.map((name) => `var.${name}`)].map((field) => ({ value: field, label: field }))}
            onChange={(fields) => updateSelectedNodeData({ outputContract: jsonPatch(selectedNode.data.outputContract, { fields: fields.length ? fields : 'all' }) })}
          />
        </Form.Item>
      </>
    )
  }

  const renderNodeSpecificConfig = () => {
    if (!selectedNode) return null
    const action = safeJson(selectedNode.data.actionJson)
    if (selectedNode.data.blwType === 'condition') {
      const rules = conditionRuleRows(action.conditionRules)
      const matchMode = String(action.conditionMatchMode || 'all') === 'any' ? 'any' : 'all'
      const outgoingRoutes = edges.filter((edge) => edge.source === selectedNode.id)
      const trueTargets = outgoingRoutes.filter((edge) => String(edge.sourceHandle || 'output') === 'true')
      const falseTargets = outgoingRoutes.filter((edge) => String(edge.sourceHandle || 'output') === 'false')
      const targetLabel = (targetId: string) => nodes.find((node) => node.id === targetId)?.data?.label || targetId
      return (
        <>
          <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Condition Routing</Text>
          <div style={{ border: '1px solid var(--app-border)', borderRadius: 8, padding: 10, background: 'var(--app-panel-bg)', marginTop: 8 }}>
            <Space direction="vertical" size={6} style={{ width: '100%' }}>
              <Space size={6} wrap>
                <Tag>{rules.length ? `${rules.length} rule${rules.length === 1 ? '' : 's'}` : 'custom expression'}</Tag>
                <Tag>{rules.length ? `match ${matchMode.toUpperCase()}` : 'manual'}</Tag>
                <Tag color="green">true: {trueTargets.length || 0}</Tag>
                <Tag color="red">false: {falseTargets.length || 0}</Tag>
              </Space>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                True route: {trueTargets.map((edge) => targetLabel(edge.target)).join(', ') || 'not connected'}
              </Text>
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                False route: {falseTargets.map((edge) => targetLabel(edge.target)).join(', ') || 'not connected'}
              </Text>
            </Space>
          </div>
          <Button
            block
            size="small"
            style={{ marginTop: 8 }}
            onMouseDown={(event) => event.stopPropagation()}
            onClick={(event) => {
              event.stopPropagation()
              if (selectedNode?.data.blwType === 'condition') {
                const action = safeJson(selectedNode.data.actionJson)
                if (!Array.isArray(action.conditionGroups) || action.conditionGroups.length === 0) {
                  const groups = conditionGroupRows(
                    action.conditionGroups,
                    conditionRuleRows(action.conditionRules),
                    String(action.conditionMatchMode || 'all'),
                  )
                  updateSelectedNodeData({
                    actionJson: jsonPatch(selectedNode.data.actionJson, {
                      conditionGroups: groups,
                      condition: buildConditionGroupsExpression(groups),
                    }),
                  })
                }
              }
              setConditionConfigOpen(true)
            }}
          >
            Configure Condition Routing
          </Button>
        </>
      )
    }
    if (selectedNode.data.blwType === 'parallel_start') {
      return (
        <>
          <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Parallel Split</Text>
          <Form.Item label="Max active branches">
            <InputNumber min={1} max={64} style={{ width: '100%' }} value={draft.maxParallelBranches} onChange={(value) => updateDraft({ maxParallelBranches: Number(value || 1) })} />
          </Form.Item>
          <Form.Item label="Branch names">
            <Select mode="tags" value={toStringArray(action.branches)} placeholder="Add branch names" onChange={(branches) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { branches }) })} />
          </Form.Item>
        </>
      )
    }
    if (selectedNode.data.blwType === 'parallel_join') {
      return (
        <>
          <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Parallel Join</Text>
          <Form.Item label="Join policy">
            <Select value={selectedNode.data.joinPolicy || 'wait_all'} options={[
              { value: 'wait_all', label: 'Wait for all branches' },
              { value: 'wait_any', label: 'Wait for any branch' },
              { value: 'first_success', label: 'First success continues' },
            ]} onChange={(joinPolicy) => updateSelectedNodeData({ joinPolicy })} />
          </Form.Item>
          <Form.Item label="Failure policy">
            <Select value={selectedNode.data.failurePolicy || 'fail_workflow'} options={[
              { value: 'fail_workflow', label: 'Fail workflow if any branch fails' },
              { value: 'continue_failed_branch', label: 'Continue failed branch' },
              { value: 'escalate', label: 'Escalate failed branch' },
            ]} onChange={(failurePolicy) => updateSelectedNodeData({ failurePolicy })} />
          </Form.Item>
        </>
      )
    }
    if (selectedNode.data.blwType === 'execute_child_node') {
      const selectedChildOption = embeddedChildNodeOptions.find((item) => item.value === selectedNode.data.childNodeId)
      const childExecutionMode = String(action.childExecutionMode || 'downstream')
      const childRunMode = String(action.childRunMode || 'per_record')
      const sequenceLabels = toStringArray(action.childNodeIds)
        .map((id) => embeddedChildNodeOptions.find((item) => item.value === id)?.label || id)
      return (
        <>
          <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Child Node Execution</Text>
          <div style={{ border: '1px solid var(--app-border)', borderRadius: 8, padding: 10, background: 'var(--app-panel-bg)', marginTop: 8 }}>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
              {childExecutionMode === 'sequence' ? 'Selected sequence' : 'Selected start node'}
            </Text>
            <div style={{ color: 'var(--app-text)', fontWeight: 800, fontSize: 12, marginTop: 2 }}>
              {childExecutionMode === 'sequence'
                ? (sequenceLabels.length ? sequenceLabels.join(' -> ') : 'No child workflow sequence selected')
                : (selectedChildOption?.label || selectedNode.data.childNodeId || 'No child workflow canvas node selected')}
            </div>
            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
              Mode: {childExecutionMode === 'sequence' ? 'Explicit sequence' : 'Downstream cluster'} · Run: {childRunMode === 'batch' ? 'Batch routed rows' : 'Per record'} · Input: {String(action.inputMode || 'pass_all')} · Output: {String(action.outputPath || '$.child_output')}
            </Text>
          </div>
          <Button block size="small" style={{ marginTop: 8 }} onClick={() => setChildNodeConfigOpen(true)}>
            Configure Child Execution
          </Button>
        </>
      )
    }
    if (selectedNode.data.blwType === 'wait') {
      return (
        <>
          <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Wait Timer</Text>
          <Form.Item label="Wait timeout minutes">
            <InputNumber min={1} max={10080} style={{ width: '100%' }} value={selectedNode.data.wait?.timeoutMinutes ?? draft.waitTimeoutMinutes} onChange={(value) => updateSelectedNodeData({ wait: { timeoutMinutes: Number(value || 1) } })} />
          </Form.Item>
        </>
      )
    }
    if (selectedNode.data.blwType === 'delay') {
      return (
        <>
          <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Delay</Text>
          <Form.Item label="Delay seconds">
            <InputNumber min={0} max={86400} style={{ width: '100%' }} value={selectedNode.data.delay?.seconds ?? 30} onChange={(value) => updateSelectedNodeData({ delay: { seconds: Number(value || 0) } })} />
          </Form.Item>
        </>
      )
    }
    if (selectedNode.data.blwType === 'escalation') {
      return (
        <>
          <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Escalation</Text>
          <Form.Item label="Escalate after failures">
            <InputNumber min={1} max={100} style={{ width: '100%' }} value={selectedNode.data.escalation?.afterFailures ?? draft.escalationAfterFailures} onChange={(value) => updateSelectedNodeData({ escalation: { ...(selectedNode.data.escalation || { enabled: true, to: 'operations' }), afterFailures: Number(value || 1) } })} />
          </Form.Item>
          <Form.Item label="Escalation target">
            <Input value={selectedNode.data.escalation?.to || ''} onChange={(event) => updateSelectedNodeData({ escalation: { ...(selectedNode.data.escalation || { enabled: true, afterFailures: draft.escalationAfterFailures }), to: event.target.value } })} />
          </Form.Item>
        </>
      )
    }
    if (selectedNode.data.blwType === 'activity') {
      return (
        <>
          <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Activity Action</Text>
          <Form.Item label="Action name"><Input value={String(action.actionName || '')} onChange={(event) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { actionName: event.target.value }) })} /></Form.Item>
          <Form.Item label="Result path"><Input value={String(action.resultPath || '$.activity_result')} onChange={(event) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { resultPath: event.target.value }) })} /></Form.Item>
        </>
      )
    }
    if (selectedNode.data.blwType === 'email' || selectedNode.data.blwType === 'sms') {
      return (
        <>
          <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>{displayNodeType(selectedNode.data.blwType)} Action</Text>
          <Form.Item label="Recipient field / path"><Input value={String(action.recipient || '')} placeholder="Example: $.customer.email" onChange={(event) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { recipient: event.target.value }) })} /></Form.Item>
          <Form.Item label="Template / message"><Input.TextArea rows={3} value={String(action.template || '')} onChange={(event) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { template: event.target.value }) })} /></Form.Item>
        </>
      )
    }
    if (selectedNode.data.blwType === 'oracle_update') {
      return (
        <>
          <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Oracle Update</Text>
          <Form.Item label="Target table"><Input value={String(action.table || draft.trackerTable)} onChange={(event) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { table: event.target.value }) })} /></Form.Item>
          <Form.Item label="Key field"><Input value={String(action.keyField || draft.uniqueIdField)} onChange={(event) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { keyField: event.target.value }) })} /></Form.Item>
          <Form.Item label="Status field"><Input value={String(action.statusField || 'CURRENT_STATUS')} onChange={(event) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { statusField: event.target.value }) })} /></Form.Item>
        </>
      )
    }
    if (selectedNode.data.blwType === 'pause') {
      return (
        <>
          <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Pause</Text>
          <Form.Item label="Resume mode">
            <Select value={String(action.resume_when || 'manual')} options={[
              { value: 'manual', label: 'Manual resume' },
              { value: 'condition', label: 'Resume when condition is true' },
              { value: 'time', label: 'Resume after time' },
            ]} onChange={(resume_when) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { resume_when }) })} />
          </Form.Item>
          <Form.Item label="Resume condition">
            <Input value={String(action.resume_condition || '')} onChange={(event) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { resume_condition: event.target.value }) })} />
          </Form.Item>
        </>
      )
    }
    if (selectedNode.data.blwType === 'terminate') {
      return (
        <>
          <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Termination</Text>
          <Form.Item label="Termination reason"><Input value={String(action.reason || 'workflow terminated')} onChange={(event) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { reason: event.target.value }) })} /></Form.Item>
        </>
      )
    }
    return null
  }

  return (
    <Modal
      open={open}
      onCancel={(event) => {
        event?.stopPropagation?.()
        onClose()
      }}
      width="100vw"
      footer={null}
      closable={false}
      maskClosable={false}
      keyboard={false}
      destroyOnClose={false}
      styles={{
        body: { padding: 0, height: '100vh', overflow: 'hidden', background: 'var(--app-shell-bg)' },
        content: { padding: 0, background: 'var(--app-shell-bg)', border: 0, borderRadius: 0, minHeight: '100vh' },
        mask: { background: 'rgba(0, 0, 0, 0.72)' },
      }}
      title={null}
      style={{ top: 0, maxWidth: '100vw', paddingBottom: 0 }}
    >
      <div
        onMouseDown={(event) => event.stopPropagation()}
        onClick={(event) => event.stopPropagation()}
        onKeyDownCapture={(event) => {
          event.stopPropagation()
        }}
        tabIndex={-1}
        style={{ height: '100vh', display: 'flex', flexDirection: 'column', background: 'var(--app-shell-bg)', color: 'var(--app-text)' }}
      >
        <div style={{ minHeight: 58, padding: '10px 16px', borderBottom: '1px solid var(--app-border)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', background: 'var(--app-panel-bg)' }}>
          <Space size={10}>
            <BranchesOutlined style={{ color: '#3b82f6', fontSize: 18 }} />
            <div>
              <Text style={{ color: 'var(--app-text)', fontWeight: 800, fontSize: 15 }}>BLW Studio</Text>
              <br />
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Visual Business Logic Workflow canvas with route conditions, retry loops, escalation, and parallel start/join blocks.</Text>
            </div>
          </Space>
          <Space>
            <Tag style={{ margin: 0, color: '#22c55e', borderColor: '#22c55e55', background: '#22c55e18' }}>{draft.enabled ? 'ACTIVE' : 'DISABLED'}</Tag>
            <Space.Compact>
              <Button size="small" type="primary" icon={<BranchesOutlined />}>BLW Studio</Button>
              <Button
                size="small"
                icon={<ApartmentOutlined />}
                disabled={!onOpenChildCanvas}
                onMouseDown={(event) => event.stopPropagation()}
                onClick={openChildCanvasFromStudio}
              >
                Child Canvas
              </Button>
            </Space.Compact>
            {activeView === 'designer' ? (
              <>
                <Button
                  icon={<CopyOutlined />}
                  disabled={selectedCopyableNodes.length === 0}
                  onMouseDown={(event) => event.stopPropagation()}
                  onClick={(event) => { event.stopPropagation(); copySelectedNodes() }}
                >
                  Copy
                </Button>
                <Button
                  icon={<UndoOutlined />}
                  disabled={!canUndoGraph}
                  onMouseDown={(event) => event.stopPropagation()}
                  onClick={(event) => { event.stopPropagation(); undoGraph() }}
                />
                <Button
                  icon={<RedoOutlined />}
                  disabled={!canRedoGraph}
                  onMouseDown={(event) => event.stopPropagation()}
                  onClick={(event) => { event.stopPropagation(); redoGraph() }}
                />
              </>
            ) : null}
            <Button icon={<SaveOutlined />} type="primary" onClick={save}>Save BLW Config</Button>
            <Button onMouseDown={(event) => event.stopPropagation()} onClick={(event) => { event.stopPropagation(); onClose() }}>Done</Button>
          </Space>
        </div>

        <div style={{ flex: 1, minHeight: 0, display: 'grid', gridTemplateColumns: activeView === 'designer' ? '230px minmax(0, 1fr) 320px' : '250px minmax(0, 1fr)', overflow: 'hidden' }}>
          <div style={{ borderRight: '1px solid var(--app-border)', background: 'var(--app-panel-bg)', padding: 12, overflow: 'auto' }}>
            <Space direction="vertical" size={8} style={{ width: '100%' }}>
              {[
                ['designer', <BranchesOutlined />, 'Visual Designer'],
                ['dashboard', <ApartmentOutlined />, 'Dashboard'],
                ['configuration', <ThunderboltOutlined />, 'Configuration'],
                ['oracle', <DatabaseOutlined />, 'Oracle Tracker DDL'],
              ].map(([key, icon, label]) => (
                <Button key={String(key)} block icon={icon} type={activeView === key ? 'primary' : 'default'} onClick={() => setActiveView(String(key))} style={{ justifyContent: 'flex-start' }}>
                  {label}
                </Button>
              ))}

              <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Workflow</Text>
              <Input value={draft.workflowName} onChange={(event) => updateDraft({ workflowName: event.target.value })} />
              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>Unique input field</Text>
              <Select
                showSearch
                size="small"
                value={draft.uniqueIdField || undefined}
                placeholder="Select unique input field"
                optionFilterProp="label"
                options={fieldPickerOptions}
                onChange={(uniqueIdField) => updateDraft({ uniqueIdField })}
              />

              {activeView === 'designer' ? (
                <>
                  <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
                  <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Activities</Text>
                  {nodeTypeOptions.map((item) => (
                    <Button
                      key={item.type}
                      block
                      size="small"
                      icon={nodeIcon(item.type, item.color)}
                      onClick={() => addCanvasNode(item.type, item.label)}
                      style={{ justifyContent: 'flex-start', borderColor: `${item.color}66`, color: item.color }}
                    >
                      {item.label}
                    </Button>
                  ))}
                </>
              ) : null}
            </Space>
          </div>

          <div style={{ minWidth: 0, overflow: activeView === 'designer' ? 'hidden' : 'auto', padding: activeView === 'designer' ? 0 : 14 }}>
            {activeView === 'designer' ? (
              <ReactFlowProvider>
                <ReactFlow
                  nodes={nodes}
                  edges={flowEdges}
                  nodeTypes={blwNodeTypes}
                  onNodesChange={onNodesChange}
                  onEdgesChange={onEdgesChange}
                  onConnect={onConnect}
                  fitView
                  minZoom={0.2}
                  maxZoom={1.5}
                  onNodeClick={(_, node) => {
                    setSelectedNodeId(node.id)
                    setSelectedEdgeId('')
                  }}
                  onEdgeClick={(_, edge) => {
                    setSelectedEdgeId(edge.id)
                    setSelectedNodeId('')
                  }}
                  onPaneClick={() => {
                    setSelectedNodeId('')
                    setSelectedEdgeId('')
                  }}
                  deleteKeyCode={null}
                  style={{ background: 'var(--app-panel-bg)' }}
                >
                  <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="var(--app-canvas-dot)" />
                  <MiniMap
                    pannable
                    zoomable
                    nodeColor={(node) => nodeKindColor[String((node.data as BlwNodeData)?.blwType || '')] || '#6f7c8e'}
                    nodeStrokeColor={() => 'rgba(226, 232, 240, 0.32)'}
                    maskColor="rgba(15, 23, 42, 0.38)"
                    style={{
                      background: 'var(--app-panel-bg)',
                      border: '1px solid var(--app-border-strong)',
                      borderRadius: 8,
                      opacity: 0.72,
                    }}
                  />
                  <Controls style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8 }} />
                </ReactFlow>
              </ReactFlowProvider>
            ) : null}

            {activeView === 'dashboard' ? (
              <Tabs
                activeKey={dashboardTab}
                onChange={setDashboardTab}
                size="small"
                type="card"
                items={[
                  {
                    key: 'overview',
                    label: 'Overview',
                    children: (
                      <Space direction="vertical" size={12} style={{ width: '100%' }}>
                        <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                          <Space direction="vertical" size={0}>
                            <Text style={{ color: 'var(--app-text)', fontWeight: 800 }}>BLW Dashboard</Text>
                            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Status, stages, latency, error and performance summary.</Text>
                          </Space>
                          <Space size={8}>
                            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Refresh rate</Text>
                            <Select
                              size="small"
                              value={dashboardRefreshSeconds}
                              onChange={setDashboardRefreshSeconds}
                              style={{ width: 118 }}
                              options={[
                                { value: 0, label: 'Manual' },
                                { value: 3, label: '3 sec' },
                                { value: 5, label: '5 sec' },
                                { value: 10, label: '10 sec' },
                                { value: 30, label: '30 sec' },
                                { value: 60, label: '1 min' },
                              ]}
                            />
                            <Button size="small" icon={<SyncOutlined />} loading={trackerLoading} onClick={() => refreshTrackerSummary({ refreshCounts: true })}>Refresh</Button>
                          </Space>
                        </Space>
                        <Row gutter={[10, 10]}>
                          {[
                            ['Tracked Runs', dashboardInsights.total, '#22c55e'],
                            ['Running / Waiting', dashboardInsights.running, '#38bdf8'],
                            ['Failed / Bugs', dashboardInsights.failed, '#ef4444'],
                            ['Avg Latency', `${dashboardInsights.avg.toFixed(1)}s`, '#f59e0b'],
                            ['P95 Latency', `${dashboardInsights.p95.toFixed(1)}s`, '#a78bfa'],
                            ['Wait Queue', dashboardInsights.waitQueue, '#fb7185'],
                          ].map(([label, value, color]) => (
                            <Col span={4} key={String(label)}>
                              <div style={{ border: '1px solid var(--app-border)', background: 'var(--app-panel-bg)', borderRadius: 8, padding: 12 }}>
                                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>{label}</Text>
                                <div style={{ color: String(color), fontSize: 24, fontWeight: 800 }}>{value}</div>
                              </div>
                            </Col>
                          ))}
                        </Row>
                        <div style={{ border: `1px solid ${validationErrors.length ? '#ef444466' : 'var(--app-border)'}`, background: 'var(--app-panel-bg)', borderRadius: 8, padding: 12 }}>
                          <Space style={{ justifyContent: 'space-between', width: '100%', marginBottom: 8 }}>
                            <Text style={{ color: 'var(--app-text)', fontWeight: 800 }}>Run Readiness</Text>
                            <Tag color={validationErrors.length ? 'red' : validationIssues.length ? 'gold' : 'green'}>
                              {validationErrors.length ? 'BLOCKED' : validationIssues.length ? 'WARNINGS' : 'READY'}
                            </Tag>
                          </Space>
                          <Space direction="vertical" size={6} style={{ width: '100%', maxHeight: 152, overflow: 'auto' }}>
                            {validationIssues.length ? validationIssues.map((issue, index) => (
                              <div key={`${issue.area}-${index}`} style={{ display: 'grid', gridTemplateColumns: '64px 1fr', gap: 8, fontSize: 12 }}>
                                <Tag color={issue.severity === 'error' ? 'red' : 'gold'} style={{ margin: 0 }}>{issue.severity}</Tag>
                                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>{issue.area}: {issue.message}</Text>
                              </div>
                            )) : <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>No blocking validation issues found.</Text>}
                          </Space>
                        </div>
                        <Row gutter={[10, 10]}>
                          <Col span={6}>
                            <div style={{ border: '1px solid var(--app-border)', background: 'var(--app-panel-bg)', borderRadius: 8, padding: 10 }}>
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700, fontSize: 12 }}>Status Distribution</Text>
                              <ReactECharts
                                option={dashboardInsights.statusChart}
                                style={{ height: 230, width: '100%' }}
                                onEvents={{ click: (params: any) => drillIntoTracker(params?.name) }}
                              />
                            </div>
                          </Col>
                          <Col span={6}>
                            <div style={{ border: '1px solid var(--app-border)', background: 'var(--app-panel-bg)', borderRadius: 8, padding: 10 }}>
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700, fontSize: 12 }}>Stages</Text>
                              <ReactECharts
                                option={dashboardInsights.stageChart}
                                style={{ height: 230, width: '100%' }}
                                onEvents={{ click: (params: any) => drillIntoTracker(params?.name) }}
                              />
                            </div>
                          </Col>
                          <Col span={6}>
                            <div style={{ border: '1px solid var(--app-border)', background: 'var(--app-panel-bg)', borderRadius: 8, padding: 10 }}>
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700, fontSize: 12 }}>Node Load</Text>
                              <ReactECharts
                                option={dashboardInsights.nodeChart}
                                style={{ height: 230, width: '100%' }}
                                onEvents={{ click: (params: any) => drillIntoTracker(params?.name) }}
                              />
                            </div>
                          </Col>
                          <Col span={6}>
                            <div style={{ border: '1px solid var(--app-border)', background: 'var(--app-panel-bg)', borderRadius: 8, padding: 10 }}>
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700, fontSize: 12 }}>Latency Buckets</Text>
                              <ReactECharts
                                option={dashboardInsights.latencyChart}
                                style={{ height: 230, width: '100%' }}
                              />
                            </div>
                          </Col>
                        </Row>
                        <Row gutter={[10, 10]}>
                          <Col span={8}>
                            <Table
                              size="small"
                              rowKey="status"
                              title={() => <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>Status Drill Down</Text>}
                              dataSource={dashboardInsights.statusRows}
                              pagination={false}
                              scroll={{ y: 160 }}
                              onRow={(row) => ({ onClick: () => drillIntoTracker(row.status) })}
                              columns={[
                                { title: 'Status', dataIndex: 'status', render: (value) => <Tag>{String(value)}</Tag> },
                                { title: 'Runs', dataIndex: 'count', width: 90, sorter: (a: any, b: any) => Number(a.count || 0) - Number(b.count || 0) },
                              ]}
                            />
                          </Col>
                          <Col span={8}>
                            <Table
                              size="small"
                              rowKey="stage"
                              title={() => <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>Stage Drill Down</Text>}
                              dataSource={dashboardInsights.stageRows}
                              pagination={false}
                              scroll={{ y: 160 }}
                              onRow={(row) => ({ onClick: () => drillIntoTracker(row.stage) })}
                              columns={[
                                { title: 'Stage', dataIndex: 'stage', ellipsis: true },
                                { title: 'Runs', dataIndex: 'count', width: 90, sorter: (a: any, b: any) => Number(a.count || 0) - Number(b.count || 0) },
                              ]}
                            />
                          </Col>
                          <Col span={8}>
                            <Table
                              size="small"
                              rowKey="error"
                              title={() => <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>Errors / Bugs</Text>}
                              dataSource={dashboardInsights.errorRows}
                              pagination={false}
                              scroll={{ y: 160 }}
                              onRow={(row) => ({ onClick: () => drillIntoTracker(row.error) })}
                              columns={[
                                { title: 'Error', dataIndex: 'error', ellipsis: true, render: (value) => <Tag color="red">{String(value)}</Tag> },
                                { title: 'Count', dataIndex: 'count', width: 90, sorter: (a: any, b: any) => Number(a.count || 0) - Number(b.count || 0) },
                              ]}
                            />
                          </Col>
                        </Row>
                        <Tabs
                          size="small"
                          type="card"
                          items={[
                            {
                              key: 'slowest',
                              label: 'Slowest Runs',
                              children: (
                                <Table
                                  size="small"
                                  rowKey="run_id"
                                  dataSource={dashboardInsights.slowest}
                                  pagination={false}
                                  scroll={{ x: 980, y: 360 }}
                                  columns={[
                                    { title: 'Run', dataIndex: 'run_id', width: 190, ellipsis: true, render: (value) => <Button type="link" size="small" onClick={() => openTrackerDetail(String(value || ''))}>{String(value || '')}</Button> },
                                    { title: 'Input ID', dataIndex: 'input_unique_id', width: 160, ellipsis: true },
                                    { title: 'Status', dataIndex: 'current_status', width: 120, render: (value) => <Tag>{String(value || 'UNKNOWN')}</Tag> },
                                    { title: 'Stage', dataIndex: 'current_stage', width: 150, ellipsis: true },
                                    { title: 'Node', dataIndex: 'business_node_id', width: 160, ellipsis: true },
                                    { title: 'Duration', dataIndex: 'duration_seconds', width: 110, sorter: (a: any, b: any) => Number(a.duration_seconds || 0) - Number(b.duration_seconds || 0), render: (value) => `${Number(value || 0)}s` },
                                    { title: 'Updated', dataIndex: 'last_updated_at', width: 170, ellipsis: true },
                                  ]}
                                />
                              ),
                            },
                            {
                              key: 'design',
                              label: 'Workflow Node Design Status',
                              children: (
                                <Table
                                  size="small"
                                  rowKey="id"
                                  dataSource={nodeRows}
                                  pagination={false}
                                  scroll={{ y: 360 }}
                                  columns={[
                                    { title: 'Node', dataIndex: 'name' },
                                    { title: 'Type', dataIndex: 'type', render: (value) => <Tag>{displayNodeType(String(value))}</Tag> },
                                    { title: 'Status', dataIndex: 'status', render: (value) => <Badge color={nodeKindColor[String(value)] || '#64748b'} text={String(value).toUpperCase()} /> },
                                    { title: 'Retry', render: (_, row) => `${row.retryAttempts}/${row.maxRetryAttempts}` },
                                    { title: 'Iterations', render: (_, row) => `${row.iterationNo}/${row.maxIterations}` },
                                    { title: 'Next', dataIndex: 'next' },
                                  ]}
                                />
                              ),
                            },
                          ]}
                        />
                      </Space>
                    ),
                  },
                  {
                    key: 'tracker',
                    label: 'Tracker Runs',
                    children: (
                      <Space direction="vertical" size={12} style={{ width: '100%' }}>
                        <div style={{ border: '1px solid var(--app-border)', background: 'var(--app-panel-bg)', borderRadius: 8, padding: 12 }}>
                          <Space style={{ justifyContent: 'space-between', width: '100%', marginBottom: 8 }}>
                            <Text style={{ color: 'var(--app-text)', fontWeight: 800 }}>Oracle Tracker Summary</Text>
                            <Button size="small" loading={trackerLoading} onClick={() => refreshTrackerSummary({ refreshCounts: true })}>Refresh</Button>
                          </Space>
                          {trackerSummary?.ok === false ? (
                            <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>{String(trackerSummary?.message || 'Tracker is not connected.')}</Text>
                          ) : (
                            <Space wrap size={6}>
                              {Object.entries(trackerSummary?.summary?.status_counts || {}).map(([status, count]) => (
                                <Tag key={status} color={String(status).toLowerCase() === 'failed' ? 'red' : String(status).toLowerCase() === 'success' ? 'green' : 'blue'}>
                                  {status}: {String(count)}
                                </Tag>
                              ))}
                              {!Object.keys(trackerSummary?.summary?.status_counts || {}).length ? <Tag>no tracked runs</Tag> : null}
                              <Tag>table: {String(trackerSummary?.table || draft.trackerTable)}</Tag>
                            </Space>
                          )}
                        </div>
                        <Table
                          size="small"
                          rowKey="run_id"
                          title={() => (
                            <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                              <Text style={{ color: 'var(--app-text)', fontWeight: 700 }}>Tracker Runs</Text>
                              <Space size={8}>
                                <Button
                                  size="small"
                                  icon={<DownloadOutlined />}
                                  loading={trackerExportLoading}
                                  onClick={downloadTrackerRows}
                                >
                                  {trackerSearch.trim() ? 'Download Drill' : 'Download Rows'}
                                </Button>
                                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Rows</Text>
                                <Select
                                  size="small"
                                  value={trackerPageSize}
                                  onChange={setTrackerPageSize}
                                  style={{ width: 92 }}
                                  options={[
                                    { value: 25, label: '25' },
                                    { value: 50, label: '50' },
                                    { value: 100, label: '100' },
                                    { value: 200, label: '200' },
                                  ]}
                                />
                                <Input.Search
                                  size="small"
                                  allowClear
                                  placeholder="Search tracker rows"
                                  value={trackerSearch}
                                  onChange={(event) => {
                                    setTrackerSearch(event.target.value)
                                    setTrackerDrillRows(null)
                                  }}
                                  style={{ width: 260 }}
                                />
                              </Space>
                            </Space>
                          )}
                          dataSource={trackerRows}
                          loading={trackerLoading || trackerDrillLoading}
                          pagination={{
                            pageSize: trackerPageSize,
                            size: 'small',
                            showSizeChanger: true,
                            pageSizeOptions: [25, 50, 100, 200],
                            onShowSizeChange: (_, size) => setTrackerPageSize(size),
                            onChange: (_, size) => setTrackerPageSize(size),
                          }}
                          scroll={{ x: 2070, y: 'calc(100vh - 360px)' }}
                          columns={[
                            { title: 'Run', dataIndex: 'run_id', width: 190, ellipsis: true, sorter: (a: any, b: any) => String(a.run_id || '').localeCompare(String(b.run_id || '')), render: (value) => <Button type="link" size="small" onClick={() => openTrackerDetail(String(value || ''))}>{String(value || '')}</Button> },
                            { title: 'Input ID', dataIndex: 'input_unique_id', width: 160, ellipsis: true, sorter: (a: any, b: any) => String(a.input_unique_id || '').localeCompare(String(b.input_unique_id || '')) },
                            { title: 'Stage', dataIndex: 'current_stage', width: 150, ellipsis: true, sorter: (a: any, b: any) => String(a.current_stage || '').localeCompare(String(b.current_stage || '')) },
                            { title: 'Step', dataIndex: 'current_step_id', width: 150, ellipsis: true },
                            { title: 'Status', dataIndex: 'current_status', width: 120, filters: Object.keys(trackerSummary?.summary?.status_counts || {}).map((value) => ({ text: value, value })), onFilter: (value, row: any) => String(row.current_status || '') === String(value), render: (value) => <Tag>{String(value || 'UNKNOWN')}</Tag> },
                            { title: 'Source', dataIndex: 'input_source', width: 120, ellipsis: true },
                            { title: 'Node', dataIndex: 'business_node_id', width: 140, ellipsis: true },
                            { title: 'Retry', dataIndex: 'retry_count', width: 86, sorter: (a: any, b: any) => Number(a.retry_count || 0) - Number(b.retry_count || 0) },
                            { title: 'Esc', dataIndex: 'escalation_level', width: 76, sorter: (a: any, b: any) => Number(a.escalation_level || 0) - Number(b.escalation_level || 0) },
                            { title: 'Duration', dataIndex: 'duration_seconds', width: 96, sorter: (a: any, b: any) => Number(a.duration_seconds || 0) - Number(b.duration_seconds || 0), render: (value) => `${Number(value || 0)}s` },
                            { title: 'Wait Until', dataIndex: 'wait_until', width: 150, ellipsis: true, render: (value) => value ? String(value) : <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>-</Text> },
                            { title: 'Wait Left', dataIndex: 'wait_remaining_seconds', width: 96, sorter: (a: any, b: any) => Number(a.wait_remaining_seconds || 0) - Number(b.wait_remaining_seconds || 0), render: (value) => value === null || value === undefined ? '-' : `${Number(value || 0)}s` },
                            { title: 'Last Wait', dataIndex: 'last_wait_seconds', width: 96, sorter: (a: any, b: any) => Number(a.last_wait_seconds || 0) - Number(b.last_wait_seconds || 0), render: (value) => value === null || value === undefined ? '-' : `${Number(value || 0)}s` },
                            { title: 'Wait Node', dataIndex: 'last_wait_node_id', width: 150, ellipsis: true, render: (value) => value ? String(value) : <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>-</Text> },
                            { title: 'Error', dataIndex: 'error_code', width: 130, ellipsis: true, render: (value) => value ? <Tag color="red">{String(value)}</Tag> : <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>-</Text> },
                            { title: 'Started', dataIndex: 'started_at', width: 150, ellipsis: true },
                            { title: 'Updated', dataIndex: 'last_updated_at', width: 150, ellipsis: true, sorter: (a: any, b: any) => String(a.last_updated_at || '').localeCompare(String(b.last_updated_at || '')) },
                            { title: 'Ended', dataIndex: 'ended_at', width: 150, ellipsis: true },
                            {
                              title: 'Actions',
                              width: 210,
                              fixed: 'right',
                              render: (_, row: any) => {
                                const runId = String(row?.run_id || '')
                                return (
                                  <Space size={4}>
                                    <Button size="small" onClick={() => openTrackerDetail(runId)}>View</Button>
                                    <Button size="small" onClick={() => applyTrackerAction(runId, 'resume')}>Resume</Button>
                                    <Button size="small" onClick={() => applyTrackerAction(runId, 'retry')}>Retry</Button>
                                    <Button size="small" danger onClick={() => applyTrackerAction(runId, 'terminate')}>Stop</Button>
                                  </Space>
                                )
                              },
                            },
                          ]}
                        />
                      </Space>
                    ),
                  },
                ]}
              />
            ) : null}

            {activeView === 'configuration' ? (
              <Form layout="vertical" style={{ maxWidth: 940 }}>
                <Row gutter={12}>
                  <Col span={24}>
                    <div style={{ border: '1px solid var(--app-border)', borderRadius: 8, padding: 12, marginBottom: 12, background: 'var(--app-panel-bg)' }}>
                      <Space direction="vertical" size={8} style={{ width: '100%' }}>
                        <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                          <Space size={6} wrap>
                            <Text style={{ color: 'var(--app-text)', fontWeight: 800 }}>Available Input Data Fields</Text>
                            <Tag>{draft.inputFieldMappings.length} selected</Tag>
                            <Tag>preview rows: {upstreamPreviewRows.length}</Tag>
                          </Space>
                          <Button size="small" icon={<PlusOutlined />} onClick={addInputFieldMapping}>Add Field</Button>
                        </Space>
                        <Table
                          size="small"
                          rowKey={(_, index) => String(index)}
                          pagination={false}
                          dataSource={draft.inputFieldMappings}
                          locale={{ emptyText: 'Add fields for validation and routing.' }}
                          columns={[
                            {
                              title: 'Field',
                              render: (_, row, index) => (
                                <Select
                                  size="small"
                                  showSearch
                                  value={row.field || undefined}
                                  placeholder="Select source field"
                                  optionFilterProp="label"
                                  style={{ width: '100%' }}
                                  options={inputFieldOptionsForRow(index)}
                                  onChange={(field) => updateInputFieldMapping(index, { field })}
                                />
                              ),
                            },
                            {
                              title: 'Alias',
                              render: (_, row, index) => (
                                <Input
                                  size="small"
                                  value={row.alias}
                                  placeholder="Display alias"
                                  onChange={(event) => updateInputFieldMapping(index, { alias: event.target.value })}
                                />
                              ),
                            },
                            {
                              title: 'Sort',
                              width: 82,
                              render: (_, __, index) => (
                                <Space size={2}>
                                  <Button size="small" icon={<ArrowUpOutlined />} disabled={index === 0} onClick={() => moveInputFieldMapping(index, -1)} />
                                  <Button size="small" icon={<ArrowDownOutlined />} disabled={index === draft.inputFieldMappings.length - 1} onClick={() => moveInputFieldMapping(index, 1)} />
                                </Space>
                              ),
                            },
                            {
                              title: '',
                              width: 44,
                              render: (_, __, index) => (
                                <Button size="small" danger icon={<DeleteOutlined />} onClick={() => removeInputFieldMapping(index)} />
                              ),
                            },
                          ]}
                        />
                      </Space>
                    </div>
                  </Col>
                  <Col span={24}>
                    <div style={{ border: '1px solid var(--app-border)', borderRadius: 8, padding: 12, marginBottom: 12, background: 'var(--app-panel-bg)' }}>
                      {renderInstanceVariableEditor()}
                    </div>
                  </Col>
                  <Col span={12}><Form.Item label="Tracker table"><Input value={draft.trackerTable} onChange={(event) => updateDraft({ trackerTable: event.target.value })} /></Form.Item></Col>
                  <Col span={12}>
                    <Form.Item label="Unique input field">
                      <Select
                        showSearch
                        value={draft.uniqueIdField || undefined}
                        placeholder="Select unique input field"
                        optionFilterProp="label"
                        options={fieldPickerOptions}
                        onChange={(uniqueIdField) => updateDraft({ uniqueIdField })}
                      />
                    </Form.Item>
                  </Col>
                  <Col span={8}><Form.Item label="Max parallel branches"><InputNumber min={1} max={64} style={{ width: '100%' }} value={draft.maxParallelBranches} onChange={(value) => updateDraft({ maxParallelBranches: Number(value || 1) })} /></Form.Item></Col>
                  <Col span={8}><Form.Item label="Max iterations"><InputNumber min={1} max={1000} style={{ width: '100%' }} value={draft.maxIterations} onChange={(value) => updateDraft({ maxIterations: Number(value || 1) })} /></Form.Item></Col>
                  <Col span={8}><Form.Item label="Max retry attempts"><InputNumber min={0} max={100} style={{ width: '100%' }} value={draft.maxRetryAttempts} onChange={(value) => updateDraft({ maxRetryAttempts: Number(value || 0) })} /></Form.Item></Col>
                  <Col span={8}><Form.Item label="Escalate after failures"><InputNumber min={1} max={100} style={{ width: '100%' }} value={draft.escalationAfterFailures} onChange={(value) => updateDraft({ escalationAfterFailures: Number(value || 1) })} /></Form.Item></Col>
                  <Col span={8}><Form.Item label="Wait timeout minutes"><InputNumber min={1} max={10080} style={{ width: '100%' }} value={draft.waitTimeoutMinutes} onChange={(value) => updateDraft({ waitTimeoutMinutes: Number(value || 1) })} /></Form.Item></Col>
                  <Col span={8}><Form.Item label="Workflow enabled"><Switch checked={draft.enabled} onChange={(enabled) => updateDraft({ enabled })} /></Form.Item></Col>
                  <Col span={24}>
                    <Form.Item
                      label="Workflow tracking mode"
                      help="Use exception-only for high volume runs: normal rows complete in batch, tracker rows are created only for wait/retry/escalation/failure paths."
                    >
                      <Select
                        value={draft.trackingMode || 'all_rows'}
                        options={[
                          { value: 'all_rows', label: 'Track all rows' },
                          { value: 'minimal', label: 'Track all rows - minimal payload' },
                          { value: 'exception_only', label: 'Track only waiting / exception rows' },
                          { value: 'none', label: 'No tracker, batch only' },
                        ]}
                        onChange={(trackingMode) => updateDraft({ trackingMode: trackingMode as BlwStudioConfig['trackingMode'] })}
                      />
                    </Form.Item>
                  </Col>
                </Row>
              </Form>
            ) : null}

            {activeView === 'oracle' ? (
              <Space direction="vertical" size={10} style={{ width: '100%' }}>
                <Space style={{ justifyContent: 'space-between', width: '100%' }}>
                  <Text style={{ color: 'var(--app-text)', fontWeight: 800 }}>Single Oracle Tracker Table</Text>
                  <Button onClick={() => navigator.clipboard?.writeText(ddl)}>Copy DDL</Button>
                </Space>
                <Input.TextArea value={ddl} readOnly autoSize={{ minRows: 24, maxRows: 36 }} style={{ fontFamily: 'monospace', fontSize: 12 }} />
              </Space>
            ) : null}
          </div>

          {activeView === 'designer' ? (
            <div style={{ borderLeft: '1px solid var(--app-border)', background: 'var(--app-panel-bg)', padding: 12, overflow: 'auto' }}>
              {selectedNode ? (
                <Space direction="vertical" size={10} style={{ width: '100%' }}>
                  <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                    <Text style={{ color: 'var(--app-text)', fontWeight: 800 }}>Node Config</Text>
                    <Button
                      size="small"
                      danger
                      icon={<DeleteOutlined />}
                      disabled={selectedNode.id === 'start' || selectedNode.id === 'end'}
                      onMouseDown={(event) => event.stopPropagation()}
                      onClick={(event) => {
                        event.stopPropagation()
                        removeSelectedNode()
                      }}
                    />
                  </Space>
                  <Form layout="vertical">
                    <Form.Item label="Label"><Input value={selectedNode.data.label} onChange={(event) => updateSelectedNodeData({ label: event.target.value })} /></Form.Item>
                    <Form.Item label="Type">
                      <Select
                        value={selectedNode.data.blwType}
                        disabled={selectedNode.id === 'start' || selectedNode.id === 'end'}
                        options={[
                          { value: 'start', label: 'Start' },
                          { value: 'activity', label: 'Activity' },
                          { value: 'condition', label: 'Condition Route' },
                          { value: 'parallel_start', label: 'Parallel Start' },
                          { value: 'parallel_join', label: 'Parallel Join' },
                          { value: 'execute_child_node', label: 'Execute Child Node' },
                          { value: 'wait', label: 'Wait' },
                          { value: 'delay', label: 'Delay' },
                          { value: 'pause', label: 'Pause' },
                          { value: 'escalation', label: 'Escalation' },
                          { value: 'email', label: 'Email' },
                          { value: 'sms', label: 'SMS' },
                          { value: 'oracle_update', label: 'Oracle Update' },
                          { value: 'terminate', label: 'Terminate' },
                          { value: 'end', label: 'End' },
                        ]}
                        onChange={(value) => updateSelectedNodeData({ blwType: value as BlwNodeKind })}
                      />
                    </Form.Item>
                    <Form.Item label="Status">
                      <Select value={selectedNode.data.status || 'draft'} options={['draft', 'running', 'waiting', 'success', 'failed', 'escalated', 'paused'].map((value) => ({ value, label: value.toUpperCase() }))} onChange={(status) => updateSelectedNodeData({ status })} />
                    </Form.Item>
                    <Form.Item label="Node enabled">
                      <Switch
                        checked={selectedNode.data.enabled !== false}
                        disabled={selectedNode.id === 'start' || selectedNode.id === 'end'}
                        checkedChildren="Enabled"
                        unCheckedChildren="Disabled"
                        onChange={(enabled) => updateSelectedNodeData({ enabled })}
                      />
                    </Form.Item>
                    {['condition', 'execute_child_node'].includes(selectedNode.data.blwType) ? renderNodeSpecificConfig() : null}
                    {renderDataContract()}
                    {hasRuntimeControl(selectedNode.data.blwType) ? (
                      <>
                        <Divider style={{ margin: '8px 0', borderColor: 'var(--app-border)' }} />
                        <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>
                          Retry / Loop Control
                        </Text>
                        <Form.Item label="Retry enabled">
                          <Switch
                            checked={selectedNode.data.retry?.enabled !== false}
                            onChange={(enabled) => updateSelectedNodeData({ retry: { ...(selectedNode.data.retry || { maxAttempts: draft.maxRetryAttempts, delaySeconds: 60 }), enabled } })}
                          />
                        </Form.Item>
                        <Form.Item label="Max retry attempts">
                          <InputNumber min={0} max={100} style={{ width: '100%' }} value={selectedNode.data.retry?.maxAttempts ?? draft.maxRetryAttempts} onChange={(value) => updateSelectedNodeData({ retry: { ...(selectedNode.data.retry || { enabled: true, delaySeconds: 60 }), maxAttempts: Number(value || 0) } })} />
                        </Form.Item>
                        <Form.Item label="Retry delay seconds">
                          <InputNumber min={0} max={86400} style={{ width: '100%' }} value={selectedNode.data.retry?.delaySeconds ?? 60} onChange={(value) => updateSelectedNodeData({ retry: { ...(selectedNode.data.retry || { enabled: true, maxAttempts: draft.maxRetryAttempts }), delaySeconds: Number(value || 0) } })} />
                        </Form.Item>
                        <Form.Item label="Max iterations">
                          <InputNumber min={1} max={1000} style={{ width: '100%' }} value={selectedNode.data.iteration?.maxIterations ?? draft.maxIterations} onChange={(value) => updateSelectedNodeData({ iteration: { maxIterations: Number(value || 1) } })} />
                        </Form.Item>
                      </>
                    ) : null}

                    {!['condition', 'execute_child_node'].includes(selectedNode.data.blwType) ? renderNodeSpecificConfig() : null}
                  </Form>
                </Space>
              ) : selectedEdge ? (
                <Space direction="vertical" size={10} style={{ width: '100%' }}>
                  <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                    <Text style={{ color: 'var(--app-text)', fontWeight: 800 }}>Route Config</Text>
                    <Button
                      size="small"
                      danger
                      icon={<DeleteOutlined />}
                      onMouseDown={(event) => event.stopPropagation()}
                      onClick={(event) => {
                        event.stopPropagation()
                        removeSelectedEdge()
                      }}
                    />
                  </Space>
                  <Form layout="vertical">
                    <Form.Item label="Route label"><Input value={String(selectedEdge.label || '')} onChange={(event) => updateSelectedEdge({ label: event.target.value })} /></Form.Item>
                    <Form.Item label="Route condition"><Input.TextArea rows={5} value={String((selectedEdge.data as any)?.condition || '')} onChange={(event) => updateSelectedEdge({ data: { ...(selectedEdge.data || {}), condition: event.target.value } })} placeholder="always, status == 'SUCCESS', amount > 10000" /></Form.Item>
                  </Form>
                </Space>
              ) : (
                <Space direction="vertical" size={8}>
                  <Text style={{ color: 'var(--app-text)', fontWeight: 800 }}>Canvas</Text>
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>Select a node or route to configure it.</Text>
                </Space>
              )}
            </div>
          ) : null}
        </div>
      </div>
      <Modal
        open={conditionConfigOpen}
        title="Configure Condition Routing"
        onCancel={() => setConditionConfigOpen(false)}
        onOk={() => setConditionConfigOpen(false)}
        okText="Apply"
        width="min(980px, 96vw)"
        styles={{ body: { paddingTop: 12 } }}
        destroyOnClose
      >
        {selectedNode?.data.blwType === 'condition' ? (() => {
          const action = safeJson(selectedNode.data.actionJson)
          const rules = conditionRuleRows(action.conditionRules)
          const matchMode = String(action.conditionMatchMode || 'all') === 'any' ? 'any' : 'all'
          const groups = conditionGroupRows(action.conditionGroups, rules, matchMode)
          const outgoingRoutes = edges.filter((edge) => edge.source === selectedNode.id)
          const trueTargets = outgoingRoutes.filter((edge) => String(edge.sourceHandle || 'output') === 'true')
          const falseTargets = outgoingRoutes.filter((edge) => String(edge.sourceHandle || 'output') === 'false')
          const defaultFalseTargets = outgoingRoutes.filter((edge) => ['false', 'output_false'].includes(String(edge.sourceHandle || '')))
          const defaultFalseIds = defaultFalseTargets.map((edge) => edge.target)
          const targetLabel = (targetId: string) => nodes.find((node) => node.id === targetId)?.data?.label || targetId
          const writeGroups = (nextGroups: Array<Record<string, unknown>>) => {
            updateSelectedNodeData({
              actionJson: jsonPatch(selectedNode.data.actionJson, {
                conditionGroups: nextGroups,
                condition: buildConditionGroupsExpression(nextGroups),
                conditionRules: conditionRuleRows(nextGroups[0]?.rules),
                conditionMatchMode: String(nextGroups[0]?.matchMode || 'all'),
                conditionField: conditionRuleRows(nextGroups[0]?.rules)[0]?.field || '',
                conditionOperator: conditionRuleRows(nextGroups[0]?.rules)[0]?.operator || '==',
                conditionValue: conditionRuleRows(nextGroups[0]?.rules)[0]?.value || '',
                conditionSource: conditionRuleRows(nextGroups[0]?.rules)[0]?.source || 'field',
              }),
            })
          }
          const updateGroup = (groupId: string, patch: Record<string, unknown>) => {
            writeGroups(groups.map((group) => group.id === groupId ? { ...group, ...patch } : group))
          }
          const updateGroupRule = (groupId: string, ruleId: string, patch: Record<string, string>) => {
            writeGroups(groups.map((group) => {
              if (group.id !== groupId) return group
              const groupRules = conditionRuleRows(group.rules)
              return { ...group, rules: groupRules.map((rule) => rule.id === ruleId ? { ...rule, ...patch } : rule) }
            }))
          }
          const addRule = (groupId: string) => {
            const field = availableFields[0] || variableOptions[0] || ''
            writeGroups(groups.map((group) => group.id === groupId ? {
              ...group,
              rules: [
                ...conditionRuleRows(group.rules),
                {
                  id: `rule_${Date.now()}`,
                  source: availableFields[0] ? 'field' : 'variable',
                  field,
                  operator: '==',
                  value: '',
                },
              ],
            } : group))
          }
          const removeRule = (groupId: string, ruleId: string) => {
            writeGroups(groups.map((group) => group.id === groupId ? {
              ...group,
              rules: conditionRuleRows(group.rules).filter((rule) => rule.id !== ruleId),
            } : group))
          }
          const addGroup = () => {
            writeGroups([
              ...groups,
              { id: `group_${Date.now()}`, name: `Condition Group ${groups.length + 1}`, enabled: true, matchMode: 'all', rules: [] },
            ])
          }
          const removeGroup = (groupId: string) => {
            const nextGroups = groups.filter((group) => group.id !== groupId)
            writeGroups(nextGroups)
            updateConditionRouteTargets(selectedNode.id, [], [], String(action.trueLabel || 'approved'), String(action.falseLabel || 'rejected'), groupId)
          }
          return (
            <Space direction="vertical" size={10} style={{ width: '100%' }}>
              <Button size="small" icon={<PlusOutlined />} onClick={addGroup}>Add Condition Group</Button>
              {groups.map((group, groupIndex) => {
                const groupId = String(group.id || `group_${groupIndex + 1}`)
                const safeGroupId = groupId.replace(/[^A-Za-z0-9_-]/g, '_')
                const groupName = String(group.name || `Condition Group ${groupIndex + 1}`)
                const groupEnabled = group.enabled !== false
                const groupRules = conditionRuleRows(group.rules)
                const groupTrueHandle = `group_${safeGroupId}_true`
                const groupTrueTargets = outgoingRoutes.filter((edge) => String(edge.sourceHandle || '') === groupTrueHandle)
                const groupTrueIds = groupTrueTargets.map((edge) => edge.target)
                return (
                  <div key={groupId} style={{ border: '1px solid var(--app-border)', borderRadius: 8, padding: 10, background: 'var(--app-panel-bg)', opacity: groupEnabled ? 1 : 0.58 }}>
                    <Row gutter={8} align="middle">
                      <Col span={8}>
                        <Form.Item label="Group name">
                          <Input size="small" value={groupName} onChange={(event) => updateGroup(groupId, { name: event.target.value })} />
                        </Form.Item>
                      </Col>
                      <Col span={6}>
                        <Form.Item label="Rule match mode">
                          <Select
                            size="small"
                            value={String(group.matchMode || 'all')}
                            disabled={!groupEnabled}
                            options={[
                              { value: 'all', label: 'ALL rules' },
                              { value: 'any', label: 'ANY rule' },
                            ]}
                            onChange={(matchModeValue) => updateGroup(groupId, { matchMode: matchModeValue })}
                          />
                        </Form.Item>
                      </Col>
                      <Col span={6}>
                        <Form.Item label="Group enabled">
                          <Switch
                            size="small"
                            checked={groupEnabled}
                            checkedChildren="On"
                            unCheckedChildren="Off"
                            onChange={(enabled) => updateGroup(groupId, { enabled })}
                          />
                        </Form.Item>
                      </Col>
                      <Col span={2}>
                        <Tag color={groupEnabled ? 'green' : 'default'} style={{ margin: 0 }}>{groupEnabled ? `Group ${groupIndex + 1}` : 'Off'}</Tag>
                      </Col>
                      <Col span={2}>
                        <Button size="small" danger icon={<DeleteOutlined />} onClick={() => removeGroup(groupId)} />
                      </Col>
                    </Row>
                    <Table
                      size="small"
                      rowKey="id"
                      pagination={false}
                      dataSource={groupRules}
                      locale={{ emptyText: 'No rules in this group.' }}
                      columns={[
                        {
                          title: 'Source',
                          width: 112,
                          render: (_, rule) => (
                            <Select
                              size="small"
                              value={rule.source}
                              disabled={!groupEnabled}
                              style={{ width: '100%' }}
                              options={[
                                { value: 'field', label: 'Field' },
                                { value: 'variable', label: 'Variable' },
                              ]}
                              onChange={(source) => {
                                const field = source === 'variable' ? variableOptions[0] || '' : availableFields[0] || ''
                                updateGroupRule(groupId, rule.id, { source, field })
                              }}
                            />
                          ),
                        },
                        {
                          title: 'Field / Variable',
                          render: (_, rule) => {
                            const fieldOptions = rule.source === 'variable' ? variableOptions : pathFieldOptions
                            return (
                              <Select
                                showSearch
                                size="small"
                                value={rule.field || undefined}
                                placeholder="Pick field"
                                optionFilterProp="label"
                                disabled={!groupEnabled}
                                style={{ width: '100%' }}
                                options={fieldOptions.map((field) => ({ value: field, label: field }))}
                                onChange={(field) => updateGroupRule(groupId, rule.id, { field })}
                              />
                            )
                          },
                        },
                        {
                          title: 'Operator',
                          width: 120,
                          render: (_, rule) => (
                            <Select
                              size="small"
                              value={rule.operator}
                              disabled={!groupEnabled}
                              style={{ width: '100%' }}
                              options={[
                                { value: '==', label: '=' },
                                { value: '!=', label: '!=' },
                                { value: '>', label: '>' },
                                { value: '>=', label: '>=' },
                                { value: '<', label: '<' },
                                { value: '<=', label: '<=' },
                                { value: 'contains', label: 'contains' },
                                { value: 'exists', label: 'exists' },
                                { value: 'empty', label: 'empty' },
                              ]}
                              onChange={(operator) => updateGroupRule(groupId, rule.id, { operator })}
                            />
                          ),
                        },
                        {
                          title: 'Value',
                          render: (_, rule) => (
                            <Input
                              size="small"
                              value={rule.value}
                              disabled={!groupEnabled || ['exists', 'empty'].includes(rule.operator)}
                              placeholder="Compare value"
                              onChange={(event) => updateGroupRule(groupId, rule.id, { value: event.target.value })}
                            />
                          ),
                        },
                        {
                          title: '',
                          width: 44,
                          render: (_, rule) => (
                            <Button size="small" danger disabled={!groupEnabled} icon={<DeleteOutlined />} onClick={() => removeRule(groupId, rule.id)} />
                          ),
                        },
                      ]}
                    />
                    <Button size="small" disabled={!groupEnabled} icon={<PlusOutlined />} style={{ marginTop: 8 }} onClick={() => addRule(groupId)}>Add Rule</Button>
                    <Row gutter={8} style={{ marginTop: 8 }}>
                      <Col span={24}>
                        <Form.Item label="True route nodes">
                          <Select
                            mode="multiple"
                            showSearch
                            value={groupTrueIds}
                            placeholder="Select true route targets"
                            optionFilterProp="label"
                            disabled={!groupEnabled}
                            options={conditionRouteTargetOptions}
                            onChange={(nextTrueTargets) => {
                              writeGroups(groups)
                              updateConditionRouteTargets(selectedNode.id, nextTrueTargets, [], groupName, `${groupName} false`, groupId)
                            }}
                          />
                        </Form.Item>
                      </Col>
                    </Row>
                    <Space size={4} wrap>
                      {groupTrueTargets.map((edge, index) => <Tag key={edge.id} color="green">T{index + 1}. {targetLabel(edge.target)}</Tag>)}
                    </Space>
                  </div>
                )
              })}
              <div style={{ border: '1px solid var(--app-border)', borderRadius: 8, padding: 10, background: 'var(--app-panel-bg)' }}>
                <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 800 }}>Default / Else Route</Text>
                <Form.Item label="Route nodes when no enabled group matches" style={{ marginTop: 8, marginBottom: 6 }}>
                  <Select
                    mode="multiple"
                    showSearch
                    value={defaultFalseIds}
                    placeholder="Select else route targets"
                    optionFilterProp="label"
                    options={conditionRouteTargetOptions}
                    onChange={(nextFalseTargets) => {
                      writeGroups(groups)
                      updateConditionRouteTargets(selectedNode.id, [], nextFalseTargets, String(action.trueLabel || 'matched'), 'else', '')
                    }}
                  />
                </Form.Item>
                <Space size={4} wrap>
                  {defaultFalseTargets.map((edge, index) => <Tag key={edge.id} color="red">Else {index + 1}. {targetLabel(edge.target)}</Tag>)}
                </Space>
              </div>
            </Space>
          )
        })() : null}
      </Modal>
      <Modal
        open={childNodeConfigOpen}
        title="Configure Child Execution"
        onCancel={() => setChildNodeConfigOpen(false)}
        onOk={() => setChildNodeConfigOpen(false)}
        okText="Apply"
        width="min(760px, 94vw)"
        styles={{ body: { paddingTop: 12 } }}
        destroyOnClose
      >
        {selectedNode?.data.blwType === 'execute_child_node' ? (() => {
          const action = safeJson(selectedNode.data.actionJson)
          const inputMode = String(action.inputMode || 'pass_all')
          const childExecutionMode = String(action.childExecutionMode || 'downstream')
          const childRunMode = String(action.childRunMode || 'per_record')
          return (
            <Form layout="vertical">
              <Row gutter={10}>
                <Col span={10}>
                  <Form.Item label="Child execution mode">
                    <Select
                      value={childExecutionMode}
                      options={[
                        { value: 'downstream', label: 'Downstream cluster from start node' },
                        { value: 'sequence', label: 'Explicit selected node sequence' },
                      ]}
                      onChange={(nextMode) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { childExecutionMode: nextMode }) })}
                    />
                  </Form.Item>
                </Col>
                <Col span={14}>
                  {childExecutionMode === 'sequence' ? (
                    <Form.Item
                      label="Child node sequence"
                      help={embeddedChildNodeOptions.length ? 'Only these child canvas nodes run, in selected order. Other clusters are ignored.' : 'No child canvas nodes found. Use Open In Canvas Tab to add child workflow nodes first.'}
                    >
                      <Select
                        mode="multiple"
                        showSearch
                        value={toStringArray(action.childNodeIds)}
                        placeholder="Select nodes in execution order"
                        optionFilterProp="label"
                        options={embeddedChildNodeOptions}
                        onChange={(childNodeIds) => {
                          const childNodeId = Array.isArray(childNodeIds) && childNodeIds.length ? String(childNodeIds[0]) : ''
                          updateSelectedNodeData({
                            childNodeId,
                            actionJson: jsonPatch(selectedNode.data.actionJson, { childNodeIds, childNodeId }),
                          })
                        }}
                      />
                    </Form.Item>
                  ) : (
                    <Form.Item
                      label="Child workflow start node"
                      help={embeddedChildNodeOptions.length ? 'Starts from this child canvas node and follows only its downstream connected cluster.' : 'No child canvas nodes found. Use Open In Canvas Tab to add child workflow nodes first.'}
                    >
                      <Select
                        showSearch
                        value={selectedNode.data.childNodeId || undefined}
                        placeholder="Select start node to execute"
                        optionFilterProp="label"
                        options={embeddedChildNodeOptions}
                        onChange={(childNodeId) => {
                          updateSelectedNodeData({
                            childNodeId,
                            actionJson: jsonPatch(selectedNode.data.actionJson, { childNodeId }),
                          })
                        }}
                      />
                    </Form.Item>
                  )}
                </Col>
              </Row>
              <Row gutter={10}>
                <Col span={12}>
                  <Form.Item
                    label="Run mode"
                    help="Per record keeps one workflow instance per input row. Batch routed rows runs the child node sequence once for all rows routed to this Execute Child Node."
                  >
                    <Select
                      value={childRunMode}
                      options={[
                        { value: 'per_record', label: 'Per record workflow' },
                        { value: 'batch', label: 'Batch routed rows' },
                      ]}
                      onChange={(nextRunMode) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { childRunMode: nextRunMode }) })}
                    />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item label="Batch size">
                    <InputNumber
                      min={1}
                      max={5000}
                      style={{ width: '100%' }}
                      value={Number(action.childBatchSize || 500)}
                      disabled={childRunMode !== 'batch'}
                      onChange={(childBatchSize) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { childBatchSize: Number(childBatchSize || 500) }) })}
                    />
                  </Form.Item>
                </Col>
              </Row>
              <Row gutter={10}>
                <Col span={12}>
                  <Form.Item label="Input data to send">
                    <Select
                      value={inputMode}
                      options={[
                        { value: 'pass_all', label: 'Pass complete incoming row/context' },
                        { value: 'selected_fields', label: 'Pass selected fields only' },
                        { value: 'context_path', label: 'Read from field/path' },
                      ]}
                      onChange={(nextInputMode) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { inputMode: nextInputMode }) })}
                    />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item label="Output receive path">
                    <Input
                      value={String(action.outputPath || '$.child_output')}
                      placeholder="Optional, e.g. $.child_output"
                      onChange={(event) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { outputPath: event.target.value }) })}
                    />
                  </Form.Item>
                </Col>
              </Row>
              {inputMode === 'selected_fields' ? (
                <Form.Item label="Selected input fields">
                  <Select
                    mode="multiple"
                    showSearch
                    value={toStringArray(action.selectedFields)}
                    placeholder="Select fields to send"
                    optionFilterProp="label"
                    options={fieldPickerOptions}
                    onChange={(selectedFields) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { selectedFields }) })}
                  />
                </Form.Item>
              ) : null}
              {inputMode === 'context_path' ? (
                <Form.Item label="Input fields/paths">
                  <Select
                    mode="multiple"
                    showSearch
                    value={toStringArray(action.contextPaths).length ? toStringArray(action.contextPaths) : (String(action.contextPath || '').trim() ? [String(action.contextPath || '').trim()] : [])}
                    placeholder="Select input fields/paths"
                    optionFilterProp="label"
                    options={pathPickerOptions}
                    onChange={(contextPaths) => updateSelectedNodeData({ actionJson: jsonPatch(selectedNode.data.actionJson, { contextPaths, contextPath: Array.isArray(contextPaths) ? String(contextPaths[0] || '') : '' }) })}
                  />
                </Form.Item>
              ) : null}
              <div style={{ border: '1px solid var(--app-border)', borderRadius: 8, padding: 10, background: 'var(--app-panel-bg)' }}>
                <Text style={{ color: 'var(--app-text-subtle)', fontSize: 12 }}>
                  Execution starts at this Execute Child Node. Downstream cluster mode follows only the connected cluster from the start node. Explicit sequence mode runs only the selected child nodes in order.
                </Text>
              </div>
            </Form>
          )
        })() : null}
      </Modal>
      <Modal
        open={Boolean(trackerDetail)}
        title="BLW Run Detail"
        footer={null}
        width="82vw"
        onCancel={() => setTrackerDetail(null)}
        destroyOnClose
      >
        {trackerDetailLoading ? (
          <Text>Loading tracker detail...</Text>
        ) : trackerDetail?.ok === false ? (
          <Text type="danger">{String(trackerDetail?.message || 'Unable to load tracker detail')}</Text>
        ) : (
          <Space direction="vertical" size={8} style={{ width: '100%' }}>
            <Space style={{ justifyContent: 'space-between', width: '100%' }} wrap>
              <Space size={6} wrap>
                <Tag>{String(trackerDetail?.row?.current_status || 'UNKNOWN')}</Tag>
                <Tag>stage: {String(trackerDetail?.row?.current_stage || '-')}</Tag>
                <Tag>input: {String(trackerDetail?.row?.input_unique_id || '-')}</Tag>
              </Space>
              <Space size={6}>
                <Button size="small" onClick={() => applyTrackerAction(String(trackerDetail?.row?.run_id || ''), 'resume')}>Resume</Button>
                <Button size="small" onClick={() => applyTrackerAction(String(trackerDetail?.row?.run_id || ''), 'retry')}>Retry</Button>
                <Button size="small" onClick={() => applyTrackerAction(String(trackerDetail?.row?.run_id || ''), 'pause')}>Pause</Button>
                <Button size="small" danger onClick={() => applyTrackerAction(String(trackerDetail?.row?.run_id || ''), 'terminate')}>Terminate</Button>
              </Space>
            </Space>
            <Tabs
              size="small"
              items={[
                {
                  key: 'overview',
                  label: 'Overview',
                  children: (
                    <Space direction="vertical" size={8} style={{ width: '100%' }}>
                      <Row gutter={[8, 8]}>
                        {[
                          ['Run ID', trackerDetail?.row?.run_id],
                          ['Workflow', trackerDetail?.row?.workflow_name],
                          ['Workflow ID', trackerDetail?.row?.workflow_id],
                          ['Pipeline', trackerDetail?.row?.pipeline_id],
                          ['BLW Node', trackerDetail?.row?.business_node_id],
                          ['Input ID', trackerDetail?.row?.input_unique_id],
                          ['Input Source', trackerDetail?.row?.input_source],
                          ['Stage', trackerDetail?.row?.current_stage],
                          ['Step', trackerDetail?.row?.current_step_id],
                          ['Status', trackerDetail?.row?.current_status],
                          ['Retry', `${trackerDetail?.row?.retry_count || 0}/${trackerDetail?.row?.max_retry_attempts || 0}`],
                          ['Iteration', `${trackerDetail?.row?.iteration_no || 0}/${trackerDetail?.row?.max_iterations || 0}`],
                          ['Escalation', `${trackerDetail?.row?.escalation_level || 0} ${trackerDetail?.row?.escalation_status || ''}`],
                          ['Escalated To', trackerDetail?.row?.escalated_to],
                          ['Started', trackerDetail?.row?.started_at],
                          ['Updated', trackerDetail?.row?.last_updated_at],
                          ['Wait Until', trackerDetail?.row?.wait_until],
                          ['Wait Left', trackerDetail?.row?.wait_remaining_seconds === null || trackerDetail?.row?.wait_remaining_seconds === undefined ? '' : `${trackerDetail?.row?.wait_remaining_seconds}s`],
                          ['Last Wait Node', trackerDetail?.row?.context_json?.last_wait_node_id],
                          ['Last Wait', trackerDetail?.row?.context_json?.last_wait_seconds === null || trackerDetail?.row?.context_json?.last_wait_seconds === undefined ? '' : `${trackerDetail?.row?.context_json?.last_wait_seconds}s`],
                          ['Last Wait Until', trackerDetail?.row?.context_json?.last_wait_until],
                          ['Ended', trackerDetail?.row?.ended_at],
                          ['Error Code', trackerDetail?.row?.error_code],
                        ].map(([label, value]) => (
                          <Col span={6} key={String(label)}>
                            <div style={{ border: '1px solid var(--app-border)', borderRadius: 6, padding: 8, background: 'var(--app-panel-bg)' }}>
                              <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>{label}</Text>
                              <div style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 700, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{String(value || '')}</div>
                            </div>
                          </Col>
                        ))}
                      </Row>
                      {trackerDetail?.row?.error_message ? (
                        <Input.TextArea readOnly rows={4} value={String(trackerDetail?.row?.error_message || '')} style={{ fontFamily: 'monospace', fontSize: 12 }} />
                      ) : null}
                    </Space>
                  ),
                },
                {
                  key: 'steps',
                  label: `Steps (${reconciledTrackerSteps(trackerDetail?.row).length})`,
                  children: (
                    <Table
                      size="small"
                      rowKey={(record, index) => `${String(record?.node_id || 'step')}-${index}`}
                      dataSource={reconciledTrackerSteps(trackerDetail?.row)}
                      pagination={{ pageSize: 10, size: 'small' }}
                      scroll={{ x: 760, y: 360 }}
                      columns={[
                        { title: 'Node', dataIndex: 'label', ellipsis: true },
                        { title: 'ID', dataIndex: 'node_id', width: 180, ellipsis: true },
                        { title: 'Status', dataIndex: 'status', width: 110, render: (value) => {
                          const color = trackerStatusColor(value)
                          return <Tag style={{ color, borderColor: `${color}66`, background: `${color}16` }}>{String(value || '-')}</Tag>
                        } },
                        { title: 'Rows', dataIndex: 'rows', width: 80 },
                        { title: 'At', dataIndex: 'at', width: 180, ellipsis: true },
                        { title: 'Error', dataIndex: 'error', width: 220, ellipsis: true },
                      ]}
                    />
                  ),
                },
                {
                  key: 'routing',
                  label: `Routing (${trackerArray(trackerDetail?.row?.routing_history_json).length})`,
                  children: (
                    <Table
                      size="small"
                      rowKey={(record, index) => `${String(record?.node_id || 'route')}-${index}`}
                      dataSource={trackerArray(trackerDetail?.row?.routing_history_json)}
                      pagination={{ pageSize: 10, size: 'small' }}
                      scroll={{ x: 820, y: 360 }}
                      columns={[
                        { title: 'Node', dataIndex: 'label', ellipsis: true },
                        { title: 'Mode', dataIndex: 'mode', width: 140 },
                        { title: 'True', dataIndex: 'true_rows', width: 80 },
                        { title: 'False', dataIndex: 'false_rows', width: 80 },
                        { title: 'Expression', dataIndex: 'expression', width: 260, ellipsis: true },
                        { title: 'At', dataIndex: 'at', width: 180, ellipsis: true },
                      ]}
                    />
                  ),
                },
                {
                  key: 'payload',
                  label: 'Payload',
                  children: <Input.TextArea readOnly rows={18} value={formatTrackerJson(trackerDetail?.row?.input_payload_json)} style={{ fontFamily: 'monospace', fontSize: 12 }} />,
                },
                {
                  key: 'config',
                  label: 'Config',
                  children: <Input.TextArea readOnly rows={18} value={formatTrackerJson(trackerDetail?.row?.workflow_config_json)} style={{ fontFamily: 'monospace', fontSize: 12 }} />,
                },
                {
                  key: 'history',
                  label: 'History',
                  children: (
                    <Row gutter={[8, 8]}>
                      {[
                        ['Context', trackerDetail?.row?.context_json],
                        ['Retry History', trackerDetail?.row?.retry_history_json],
                        ['Escalation', trackerDetail?.row?.escalation_json],
                        ['Parallel Branches', trackerDetail?.row?.parallel_branch_json],
                      ].map(([label, value]) => (
                        <Col span={12} key={String(label)}>
                          <Text style={{ color: 'var(--app-text)', fontWeight: 800 }}>{String(label)}</Text>
                          <Input.TextArea readOnly rows={8} value={formatTrackerJson(value)} style={{ marginTop: 6, fontFamily: 'monospace', fontSize: 12 }} />
                        </Col>
                      ))}
                    </Row>
                  ),
                },
              ]}
            />
          </Space>
        )}
      </Modal>
    </Modal>
  )
}
