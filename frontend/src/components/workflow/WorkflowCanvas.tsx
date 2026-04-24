import { useCallback, useMemo, useRef } from 'react'
import ReactFlow, {
  Background, Controls, MiniMap,
  BackgroundVariant, NodeTypes, Panel,
  useReactFlow,
} from 'reactflow'
import { ETLNode } from './ETLNode'
import { useWorkflowStore } from '../../store'
import { getNodeDef } from '../../constants/nodeTypes'
import { v4 as uuidv4 } from 'uuid'
import type { ETLNodeData } from '../../types'
import type { Connection, Edge, HandleType } from 'reactflow'

const nodeTypes: NodeTypes = { etlNode: ETLNode as React.ComponentType<any> }

export default function WorkflowCanvas() {
  const {
    nodes, edges,
    onNodesChange, onEdgesChange, onConnect, onReconnect,
    setSelectedNode, connectorType,
  } = useWorkflowStore()

  const { screenToFlowPosition } = useReactFlow()
  const canvasRef = useRef<HTMLDivElement>(null)
  const reconnectStateRef = useRef<{ edge: Edge | null; success: boolean }>({ edge: null, success: false })

  const nodeRuntimeById = useMemo(() => {
    const map = new Map<string, ETLNodeData>()
    for (const node of nodes) {
      map.set(node.id, node.data as ETLNodeData)
    }
    return map
  }, [nodes])

  const runtimeEdges = useMemo(() => {
    const edgeLabelPadding: [number, number] = [6, 3]

    return edges.map((edge) => {
      const source = nodeRuntimeById.get(edge.source)
      const target = nodeRuntimeById.get(edge.target)
      const sourceStatus = source?.status || 'idle'
      const targetStatus = target?.status || 'idle'
      const isRunningEdge = sourceStatus === 'running' || targetStatus === 'running'
      const isErrorEdge = sourceStatus === 'error' || targetStatus === 'error'
      const isSelectedEdge = !!edge.selected

      const flowStatus =
        isErrorEdge
          ? 'error'
          : isRunningEdge
            ? 'running'
            : sourceStatus === 'success'
              ? 'success'
              : 'idle'

      const strokeColor =
        flowStatus === 'error'
          ? '#ef4444'
          : flowStatus === 'success'
            ? '#22c55e'
            : '#64748b'

      const selectedStroke =
        flowStatus === 'error'
          ? '#fb7185'
          : flowStatus === 'running'
            ? '#a78bfa'
            : '#38bdf8'

      let label = ''
      if (isRunningEdge) {
        const runningRows =
          targetStatus === 'running' && typeof target?.executionRows === 'number'
            ? target.executionRows
            : sourceStatus === 'running' && typeof source?.executionRows === 'number'
              ? source.executionRows
              : undefined
        label = typeof runningRows === 'number'
          ? `${runningRows.toLocaleString()} processing`
          : 'Running...'
      } else if (isErrorEdge) {
        label = 'Error'
      }

      return {
        ...edge,
        animated: isRunningEdge,
        reconnectable: true,
        updatable: true,
        interactionWidth: typeof edge.interactionWidth === 'number' ? edge.interactionWidth : 44,
        style: {
          ...(edge.style || {}),
          stroke: isSelectedEdge
            ? selectedStroke
            : (isRunningEdge || isErrorEdge ? strokeColor : '#334155'),
          strokeWidth: isSelectedEdge ? 3.4 : isRunningEdge ? 2.6 : isErrorEdge ? 2.2 : 1.6,
          opacity: isSelectedEdge || isRunningEdge || isErrorEdge ? 1 : 0.6,
          filter: isSelectedEdge ? `drop-shadow(0 0 6px ${selectedStroke}88)` : undefined,
        },
        label,
        labelShowBg: !!label,
        labelBgPadding: label ? edgeLabelPadding : undefined,
        labelBgBorderRadius: label ? 8 : undefined,
        labelBgStyle: label
          ? {
            fill: 'var(--app-card-bg)',
            stroke: `${strokeColor}55`,
            strokeWidth: 1,
          }
          : undefined,
        labelStyle: label
          ? {
            fill: strokeColor,
            fontSize: 10,
            fontWeight: 700,
          }
          : undefined,
      }
    })
  }, [edges, nodeRuntimeById])

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
  }, [])

  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    const nodeType = e.dataTransfer.getData('nodeType')
    if (!nodeType) return
    const position = screenToFlowPosition({ x: e.clientX, y: e.clientY })
    const def = getNodeDef(nodeType)
    if (!def) return
    const id = uuidv4()
    const defaultConfig: Record<string, unknown> = {}
    def.configFields.forEach(f => {
      if (f.defaultValue !== undefined) defaultConfig[f.name] = f.defaultValue
    })
    useWorkflowStore.setState(state => ({
      nodes: [...state.nodes, {
        id,
        type: 'etlNode',
        position,
        data: {
          nodeType, label: def.label, definition: def,
          config: defaultConfig, status: 'idle',
        } as ETLNodeData,
      }],
      selectedNodeId: id,
      isDirty: true,
    }))
  }, [screenToFlowPosition])

  const onNodeClick = useCallback((_: React.MouseEvent, node: { id: string }) => {
    setSelectedNode(node.id)
  }, [setSelectedNode])

  const onPaneClick = useCallback(() => {
    // Intentionally keep current node selection.
    // Deselecting on pane click causes intermittent drawer unmounts while
    // interacting with right-panel controls (including Custom Field Studio open).
    // Node can still be closed explicitly via drawer close action.
  }, [])

  const handleReconnect = useCallback((oldEdge: Edge, connection: Connection) => {
    reconnectStateRef.current.success = true
    onReconnect(oldEdge, connection)
  }, [onReconnect])

  const handleReconnectStart = useCallback((_: React.MouseEvent, edge: Edge, __: HandleType) => {
    reconnectStateRef.current = { edge, success: false }
  }, [])

  const handleReconnectEnd = useCallback((_: MouseEvent | TouchEvent, edge: Edge, __: HandleType) => {
    const tracked = reconnectStateRef.current
    if (!tracked.success) {
      const fallbackEdge = tracked.edge || edge
      if (fallbackEdge?.id) {
        useWorkflowStore.setState((state) => {
          const exists = state.edges.some((e) => e.id === fallbackEdge.id)
          if (exists) return {}
          return {
            edges: [...state.edges, { ...fallbackEdge, reconnectable: true, updatable: true, interactionWidth: 44 } as any],
            isDirty: true,
          }
        })
      }
    }
    reconnectStateRef.current = { edge: null, success: false }
  }, [])

  return (
    <div ref={canvasRef} style={{ flex: 1, height: '100%' }}>
      <ReactFlow
        nodes={nodes}
        edges={runtimeEdges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onReconnect={handleReconnect}
        onEdgeUpdate={handleReconnect}
        onReconnectStart={handleReconnectStart}
        onReconnectEnd={handleReconnectEnd}
        onEdgeUpdateStart={handleReconnectStart}
        onEdgeUpdateEnd={handleReconnectEnd}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        onDrop={onDrop}
        onDragOver={onDragOver}
        edgesUpdatable
        deleteKeyCode={['Delete']}
        panActivationKeyCode={null}
        reconnectRadius={18}
        edgeUpdaterRadius={18}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        defaultEdgeOptions={{
          type: connectorType,
          animated: true,
          reconnectable: true,
          updatable: true,
          interactionWidth: 44,
          style: { stroke: '#6366f1', strokeWidth: 2 },
        }}
        connectionLineStyle={{ stroke: '#6366f1', strokeWidth: 2 }}
        snapToGrid
        snapGrid={[16, 16]}
        style={{ background: '#0a0a10' }}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={24}
          size={1}
          color="var(--app-border)"
        />
        <Controls
          style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8 }}
        />
        <MiniMap
          nodeColor={(node) => {
            const data = node.data as ETLNodeData
            return data?.definition?.color || '#6366f1'
          }}
          style={{
            background: 'var(--app-panel-bg)',
            border: '1px solid var(--app-border-strong)',
            borderRadius: 8,
          }}
          maskColor="rgba(0,0,0,0.7)"
        />

        {/* Empty state overlay */}
        {nodes.length === 0 && (
          <Panel position="top-center" style={{ pointerEvents: 'none' }}>
            <div style={{
              marginTop: '20vh',
              textAlign: 'center',
              color: 'var(--app-text-faint)',
              userSelect: 'none',
            }}>
              <div style={{ fontSize: 48, marginBottom: 12 }}>⚡</div>
              <div style={{ fontSize: 18, fontWeight: 600, color: 'var(--app-text-dim)', marginBottom: 8 }}>
                Start building your pipeline
              </div>
              <div style={{ fontSize: 13, color: 'var(--app-text-faint)' }}>
                Drag nodes from the left panel, or click any node to add it
              </div>
            </div>
          </Panel>
        )}
      </ReactFlow>
    </div>
  )
}
