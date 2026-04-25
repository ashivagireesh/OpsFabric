import { useCallback, useRef } from 'react'
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  BackgroundVariant,
  Panel,
  useReactFlow,
  type NodeTypes,
} from 'reactflow'
import { v4 as uuidv4 } from 'uuid'
import { ETLNode } from '../workflow/ETLNode'
import { useBusinessWorkflowStore } from '../../store/businessStore'
import { getBusinessNodeDef } from '../../constants/businessNodeTypes'
import type { ETLNodeData } from '../../types'

const nodeTypes: NodeTypes = { etlNode: ETLNode as React.ComponentType<any> }

export default function BusinessWorkflowCanvas() {
  const {
    nodes,
    edges,
    connectorType,
    onNodesChange,
    onEdgesChange,
    onConnect,
    setSelectedNode,
  } = useBusinessWorkflowStore()

  const { screenToFlowPosition } = useReactFlow()
  const canvasRef = useRef<HTMLDivElement>(null)

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
  }, [])

  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    const nodeType = e.dataTransfer.getData('nodeType')
    if (!nodeType) return
    const position = screenToFlowPosition({ x: e.clientX, y: e.clientY })
    const def = getBusinessNodeDef(nodeType)
    if (!def) return
    const id = uuidv4()
    const defaultConfig: Record<string, unknown> = {}
    def.configFields.forEach((field) => {
      if (field.defaultValue !== undefined) defaultConfig[field.name] = field.defaultValue
    })

    useBusinessWorkflowStore.setState((state) => ({
      nodes: [
        ...state.nodes,
        {
          id,
          type: 'etlNode',
          position,
          data: {
            nodeType,
            label: def.label,
            definition: def,
            config: defaultConfig,
            status: 'idle',
          } as ETLNodeData,
        },
      ],
      selectedNodeId: id,
      isDirty: true,
    }))
  }, [screenToFlowPosition])

  const onNodeClick = useCallback((_: React.MouseEvent, node: { id: string }) => {
    setSelectedNode(node.id)
  }, [setSelectedNode])

  const onPaneClick = useCallback(() => {
    setSelectedNode(null)
  }, [setSelectedNode])

  return (
    <div ref={canvasRef} style={{ flex: 1, height: '100%' }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        onDrop={onDrop}
        onDragOver={onDragOver}
        deleteKeyCode={['Backspace', 'Delete']}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        defaultEdgeOptions={{
          type: connectorType,
          animated: true,
          style: { stroke: '#f59e0b', strokeWidth: 2 },
        }}
        connectionLineStyle={{ stroke: '#f59e0b', strokeWidth: 2 }}
        snapToGrid
        snapGrid={[16, 16]}
        style={{ background: '#0a0a10' }}
      >
        <Background variant={BackgroundVariant.Dots} gap={24} size={1} color="var(--app-border)" />
        <Controls
          style={{ background: 'var(--app-card-bg)', border: '1px solid var(--app-border-strong)', borderRadius: 8 }}
        />
        <MiniMap
          nodeColor={(node) => {
            const data = node.data as ETLNodeData
            return data?.definition?.color || '#f59e0b'
          }}
          style={{
            background: 'var(--app-panel-bg)',
            border: '1px solid var(--app-border-strong)',
            borderRadius: 8,
          }}
          maskColor="rgba(0,0,0,0.7)"
        />

        {nodes.length === 0 && (
          <Panel position="top-center" style={{ pointerEvents: 'none' }}>
            <div
              style={{
                marginTop: '20vh',
                textAlign: 'center',
                color: 'var(--app-text-faint)',
                userSelect: 'none',
              }}
            >
              <div style={{ fontSize: 48, marginBottom: 12 }}>🧠</div>
              <div style={{ fontSize: 18, fontWeight: 600, color: 'var(--app-text-dim)', marginBottom: 8 }}>
                Build business logic workflow
              </div>
              <div style={{ fontSize: 13, color: 'var(--app-text-faint)' }}>
                Compose ETL/MLOps sources, prompt decisions, analytics, mail and WhatsApp actions
              </div>
            </div>
          </Panel>
        )}
      </ReactFlow>
    </div>
  )
}
