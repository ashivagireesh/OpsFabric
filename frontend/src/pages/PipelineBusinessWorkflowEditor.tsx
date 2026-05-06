import { useCallback, useEffect, useMemo, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { ReactFlowProvider } from 'reactflow'
import { ArrowLeftOutlined, SaveOutlined } from '@ant-design/icons'
import { Button, Input, Modal, Select, Space, Tag, Typography, notification } from 'antd'
import { useWorkflowStore } from '../store'
import type { ETLNode, ETLEdge, Pipeline } from '../types'
import type { WorkflowConnectorType } from '../constants/workflowConnectors'
import {
  applyConnectorTypeToEdges,
  resolveConnectorTypeFromEdges,
  WORKFLOW_CONNECTOR_OPTIONS,
} from '../constants/workflowConnectors'
import { getNodeDef } from '../constants/nodeTypes'
import api from '../api/client'
import NodePalette from '../components/workflow/NodePalette'
import WorkflowCanvas from '../components/workflow/WorkflowCanvas'
import ConfigDrawer from '../components/workflow/ConfigDrawer'
import ExecutionPanel from '../components/workflow/ExecutionPanel'

const { Text } = Typography

function toRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {}
  return value as Record<string, unknown>
}

function parseJsonArraySafe(value: unknown): unknown[] {
  if (Array.isArray(value)) return value
  if (typeof value === 'string') {
    const text = value.trim()
    if (!text) return []
    try {
      const parsed = JSON.parse(text)
      return Array.isArray(parsed) ? parsed : []
    } catch {
      return []
    }
  }
  return []
}

function normalizeEmbeddedNodes(rawNodes: unknown): ETLNode[] {
  const list = parseJsonArraySafe(rawNodes)
  const out: ETLNode[] = []
  list.forEach((item, idx) => {
    const row = toRecord(item)
    const id = String(row.id || `bw_node_${idx + 1}`).trim()
    if (!id) return
    const rawData = toRecord(row.data)
    const nodeType = String(
      rawData.nodeType || row.nodeType || row.type || '',
    ).trim()
    if (!nodeType || nodeType === 'etlNode') return
    const def = getNodeDef(nodeType)
    if (!def) return
    const label = String(rawData.label || row.label || def.label || nodeType).trim() || nodeType
    const rawConfig = rawData.config
    const config = toRecord(rawConfig || row.config)
    const rawPos = toRecord(row.position)
    const x = Number(rawPos.x)
    const y = Number(rawPos.y)
    out.push({
      id,
      type: 'etlNode',
      position: {
        x: Number.isFinite(x) ? x : 120 + (idx % 4) * 260,
        y: Number.isFinite(y) ? y : 100 + Math.floor(idx / 4) * 160,
      },
      data: {
        nodeType,
        label,
        definition: def,
        config,
        status: 'idle',
      },
    } as ETLNode)
  })
  return out
}

function normalizeEmbeddedEdges(rawEdges: unknown, nodes: ETLNode[], connectorType: WorkflowConnectorType): ETLEdge[] {
  const list = parseJsonArraySafe(rawEdges)
  const nodeIds = new Set(nodes.map((n) => String(n.id || '').trim()))
  const base: ETLEdge[] = []
  const seen = new Set<string>()
  list.forEach((item, idx) => {
    const row = toRecord(item)
    const source = String(row.source || '').trim()
    const target = String(row.target || '').trim()
    if (!source || !target) return
    if (!nodeIds.has(source) || !nodeIds.has(target)) return
    const sourceHandle = String(row.sourceHandle || 'output').trim() || 'output'
    const targetHandle = String(row.targetHandle || 'input').trim() || 'input'
    const uniq = `${source}|${sourceHandle}|${target}|${targetHandle}`
    if (seen.has(uniq)) return
    seen.add(uniq)
    base.push({
      id: String(row.id || `bw_edge_${idx + 1}_${source}_${target}`).trim() || `bw_edge_${idx + 1}_${source}_${target}`,
      source,
      target,
      sourceHandle,
      targetHandle,
      type: connectorType,
      reconnectable: true,
      updatable: true,
      animated: true,
      interactionWidth: 44,
      style: { stroke: '#6366f1', strokeWidth: 2 },
    } as ETLEdge)
  })
  return applyConnectorTypeToEdges(base, connectorType) as ETLEdge[]
}

function serializeEmbeddedNodes(nodes: ETLNode[]): Array<Record<string, unknown>> {
  return (Array.isArray(nodes) ? nodes : []).map((node) => {
    const cfg = toRecord(node?.data?.config)
    return {
      id: String(node.id || ''),
      position: {
        x: Number(node.position?.x || 0),
        y: Number(node.position?.y || 0),
      },
      data: {
        nodeType: String(node?.data?.nodeType || ''),
        label: String(node?.data?.label || ''),
        config: cfg,
      },
    }
  }).filter((node) => String(node.id || '').trim().length > 0)
}

function serializeEmbeddedEdges(edges: ETLEdge[]): Array<Record<string, unknown>> {
  return (Array.isArray(edges) ? edges : []).map((edge) => ({
    id: String(edge.id || ''),
    source: String(edge.source || ''),
    target: String(edge.target || ''),
    sourceHandle: edge.sourceHandle ? String(edge.sourceHandle) : undefined,
    targetHandle: edge.targetHandle ? String(edge.targetHandle) : undefined,
  })).filter((edge) => String(edge.source || '').trim() && String(edge.target || '').trim())
}

export default function PipelineBusinessWorkflowEditor() {
  const { id, nodeId } = useParams<{ id: string; nodeId: string }>()
  const navigate = useNavigate()
  const {
    nodes,
    edges,
    isDirty,
    isExecuting,
    pipeline,
    selectedNodeId,
    connectorType,
    canvasWidgetStyle,
    setConnectorType,
    setCanvasWidgetStyle,
    resetCanvas,
    setSelectedNode,
  } = useWorkflowStore()

  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [parentPipeline, setParentPipeline] = useState<Pipeline | null>(null)
  const [businessNodeLabel, setBusinessNodeLabel] = useState('Business Logic Workflow')

  const routePipelineId = String(id || '').trim()
  const routeNodeId = String(nodeId || '').trim()

  const loadChildCanvas = useCallback(async () => {
    if (!routePipelineId || !routeNodeId) {
      setLoading(false)
      return
    }
    setLoading(true)
    resetCanvas()
    try {
      const parent = await api.getPipeline(routePipelineId)
      const parentNodes = Array.isArray(parent?.nodes) ? parent.nodes : []
      const businessNode = parentNodes.find((node: any) => String(node?.id || '') === routeNodeId)
      if (!businessNode) {
        notification.error({ message: 'Business node not found in parent pipeline' })
        setLoading(false)
        return
      }
      const nodeType = String(businessNode?.data?.nodeType || businessNode?.type || '')
      if (nodeType !== 'business_workflow') {
        notification.error({ message: 'Selected node is not Business Logic Workflow node' })
        setLoading(false)
        return
      }

      const cfg = toRecord(businessNode?.data?.config)
      const embeddedNodes = normalizeEmbeddedNodes(cfg.embedded_workflow_nodes)
      const inferredConnector = resolveConnectorTypeFromEdges(Array.isArray(cfg.embedded_workflow_edges) ? (cfg.embedded_workflow_edges as any[]) : [])
      const nextConnector = inferredConnector || connectorType
      const embeddedEdges = normalizeEmbeddedEdges(cfg.embedded_workflow_edges, embeddedNodes, nextConnector)

      useWorkflowStore.setState((state) => ({
        ...state,
        nodes: embeddedNodes,
        edges: embeddedEdges,
        pipeline: {
          ...(parent || {}),
          id: routePipelineId,
          name: `${String(parent?.name || 'Pipeline')} :: ${String(businessNode?.data?.label || 'Business Logic Workflow')}`,
        } as Pipeline,
        selectedNodeId: null,
        isExecuting: false,
        executionId: null,
        executionAbortRequested: false,
        executionLogs: [],
        showLogs: false,
        connectorType: nextConnector,
        isDirty: false,
      }))
      setParentPipeline(parent)
      setBusinessNodeLabel(String(businessNode?.data?.label || 'Business Logic Workflow'))
    } finally {
      setLoading(false)
    }
  }, [routePipelineId, routeNodeId, resetCanvas, connectorType])

  useEffect(() => {
    void loadChildCanvas()
    return () => {
      setSelectedNode(null)
    }
  }, [loadChildCanvas, setSelectedNode])

  const handleBack = useCallback(() => {
    if (!routePipelineId) {
      navigate('/pipelines')
      return
    }
    if (isDirty) {
      Modal.confirm({
        title: 'Discard unsaved child workflow changes?',
        content: 'You have unsaved changes in this child canvas.',
        okText: 'Discard',
        okButtonProps: { danger: true },
        cancelText: 'Stay',
        onOk: () => navigate(`/pipelines/${routePipelineId}/edit`),
      })
      return
    }
    navigate(`/pipelines/${routePipelineId}/edit`)
  }, [routePipelineId, navigate, isDirty])

  const handleSave = useCallback(async () => {
    if (!routePipelineId || !routeNodeId) return
    setSaving(true)
    try {
      const latest = await api.getPipeline(routePipelineId)
      const latestNodes = Array.isArray(latest?.nodes) ? latest.nodes : []
      const latestEdges = Array.isArray(latest?.edges) ? latest.edges : []
      const updatedNodes = latestNodes.map((node: any) => {
        if (String(node?.id || '') !== routeNodeId) return node
        const nodeData = toRecord(node?.data)
        const nodeCfg = toRecord(nodeData.config)
        return {
          ...node,
          data: {
            ...nodeData,
            config: {
              ...nodeCfg,
              workflow_mode: 'embedded',
              embedded_workflow_enabled: true,
              embedded_workflow_nodes: serializeEmbeddedNodes(nodes),
              embedded_workflow_edges: serializeEmbeddedEdges(edges),
            },
          },
        }
      })
      await api.updatePipeline(routePipelineId, {
        nodes: updatedNodes,
        edges: latestEdges,
      })
      useWorkflowStore.setState((state) => ({ ...state, isDirty: false }))
      notification.success({ message: 'Child workflow saved to Business Logic node' })
    } catch (error: any) {
      notification.error({ message: 'Failed to save child workflow', description: String(error?.message || error || '') })
    } finally {
      setSaving(false)
    }
  }, [routePipelineId, routeNodeId, nodes, edges])

  const titleText = useMemo(() => {
    const pName = String(parentPipeline?.name || 'Pipeline')
    return `${pName} / ${businessNodeLabel}`
  }, [parentPipeline?.name, businessNodeLabel])

  return (
    <ReactFlowProvider>
      <div
        style={{
          height: '100vh',
          display: 'flex',
          flexDirection: 'column',
          background: 'var(--app-shell-bg)',
          color: 'var(--app-text)',
        }}
      >
        <div
          style={{
            height: 56,
            borderBottom: '1px solid var(--app-border-strong)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            padding: '0 14px',
            gap: 10,
            background: 'var(--app-panel-bg)',
          }}
        >
          <Space size={10}>
            <Button icon={<ArrowLeftOutlined />} onClick={handleBack}>
              Parent Pipeline
            </Button>
            <Tag style={{ borderColor: '#6366f188', color: '#6366f1', background: '#6366f114', marginInlineEnd: 0 }}>
              Business Logic Workflow
            </Tag>
            <Text style={{ color: 'var(--app-text)', fontWeight: 600 }}>{titleText}</Text>
          </Space>
          <Space size={8}>
            <Input
              size="small"
              value={String(pipeline?.name || '')}
              readOnly
              style={{ width: 320 }}
            />
            <Select
              size="small"
              value={connectorType}
              options={WORKFLOW_CONNECTOR_OPTIONS}
              onChange={(value) => setConnectorType(value as WorkflowConnectorType)}
              style={{ width: 130 }}
            />
            <Select
              size="small"
              value={canvasWidgetStyle}
              onChange={(value) => setCanvasWidgetStyle(String(value || '').toLowerCase() === 'nin' ? 'nin' : 'default')}
              options={[
                { value: 'default', label: 'Default' },
                { value: 'nin', label: 'nin' },
              ]}
              style={{ width: 110 }}
            />
            <Button
              type="primary"
              icon={<SaveOutlined />}
              loading={saving}
              disabled={loading || isExecuting}
              onClick={handleSave}
            >
              Save Child Workflow
            </Button>
          </Space>
        </div>

        <div style={{ flex: 1, minHeight: 0, display: 'flex' }}>
          <NodePalette />
          <div style={{ flex: 1, minWidth: 0, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
            <div style={{ flex: 1, minHeight: 0, display: 'flex' }}>
              <WorkflowCanvas />
              <ConfigDrawer open={Boolean(selectedNodeId)} onClose={() => setSelectedNode(null)} />
            </div>
            <ExecutionPanel />
          </div>
        </div>
      </div>
    </ReactFlowProvider>
  )
}
