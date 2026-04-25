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
import { useMLOpsWorkflowStore } from '../../store/mlopsStore'
import { getMLOpsNodeDef } from '../../constants/mlopsNodeTypes'
import type { ETLNodeData } from '../../types'
import { shouldCloseDrawerOnPaneClick } from '../../utils/drawerAutoHide'

const nodeTypes: NodeTypes = { etlNode: ETLNode as React.ComponentType<any> }

export default function MLWorkflowCanvas() {
  const {
    nodes,
    edges,
    connectorType,
    onNodesChange,
    onEdgesChange,
    onConnect,
    setSelectedNode,
  } = useMLOpsWorkflowStore()

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
    const def = getMLOpsNodeDef(nodeType)
    if (!def) return
    const id = uuidv4()
    const defaultConfig: Record<string, unknown> = {}
    def.configFields.forEach((field) => {
      if (field.defaultValue !== undefined) defaultConfig[field.name] = field.defaultValue
    })
    useMLOpsWorkflowStore.setState((state) => ({
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
    if (!shouldCloseDrawerOnPaneClick('mlops')) return
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
          style: { stroke: '#22c55e', strokeWidth: 2 },
        }}
        connectionLineStyle={{ stroke: '#22c55e', strokeWidth: 2 }}
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
            return data?.definition?.color || '#22c55e'
          }}
          pannable
          zoomable
          style={{
            background: 'var(--app-panel-bg)',
            border: '1px solid var(--app-border-strong)',
            borderRadius: 8,
          }}
          maskColor="rgba(0,0,0,0.7)"
        />

        {nodes.length === 0 && (
          <Panel position="top-center" style={{ pointerEvents: 'none' }}>
            <div style={{
              marginTop: '20vh',
              textAlign: 'center',
              color: 'var(--app-text-faint)',
              userSelect: 'none',
            }}>
              <div style={{ fontSize: 48, marginBottom: 12 }}>🧠</div>
              <div style={{ fontSize: 18, fontWeight: 600, color: 'var(--app-text-dim)', marginBottom: 8 }}>
                Build your MLOps workflow
              </div>
              <div style={{ fontSize: 13, color: 'var(--app-text-faint)' }}>
                Drag blocks to configure data, features, training, evaluation and deployment
              </div>
            </div>
          </Panel>
        )}
      </ReactFlow>
    </div>
  )
}
