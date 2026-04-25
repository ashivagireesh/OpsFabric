import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import ReactFlow, {
  Background, Controls, MiniMap,
  BackgroundVariant, NodeTypes, Panel,
  useReactFlow, useViewport,
} from 'reactflow'
import { ETLNode } from './ETLNode'
import { useWorkflowStore } from '../../store'
import { getNodeDef } from '../../constants/nodeTypes'
import { v4 as uuidv4 } from 'uuid'
import type { ETLNodeData } from '../../types'
import type { Connection, Edge, HandleType } from 'reactflow'

const nodeTypes: NodeTypes = { etlNode: ETLNode as React.ComponentType<any> }
const RULER_SIZE = 20
const RULER_MAJOR_STEP = 100
const RULER_MINOR_STEP = 25
const NODE_ALIGN_THRESHOLD = 10
const NODE_FALLBACK_WIDTH = 220
const NODE_FALLBACK_HEIGHT = 110

function getNodeRect(node: any) {
  const width =
    typeof node?.measured?.width === 'number'
      ? node.measured.width
      : typeof node?.width === 'number'
        ? node.width
        : NODE_FALLBACK_WIDTH
  const height =
    typeof node?.measured?.height === 'number'
      ? node.measured.height
      : typeof node?.height === 'number'
        ? node.height
        : NODE_FALLBACK_HEIGHT
  const x = typeof node?.positionAbsolute?.x === 'number' ? node.positionAbsolute.x : node.position?.x || 0
  const y = typeof node?.positionAbsolute?.y === 'number' ? node.positionAbsolute.y : node.position?.y || 0
  return { x, y, width, height }
}

export default function WorkflowCanvas() {
  const {
    nodes, edges,
    onNodesChange, onEdgesChange, onConnect, onReconnect,
    setSelectedNode, connectorType,
  } = useWorkflowStore()

  const { screenToFlowPosition } = useReactFlow()
  const canvasRef = useRef<HTMLDivElement>(null)
  const { x: viewportX, y: viewportY, zoom: viewportZoom } = useViewport()
  const reconnectStateRef = useRef<{ edge: Edge | null; success: boolean }>({ edge: null, success: false })
  const [canvasSize, setCanvasSize] = useState<{ width: number; height: number }>({ width: 0, height: 0 })
  const [alignGuide, setAlignGuide] = useState<{ x: number | null; y: number | null; active: boolean }>({
    x: null,
    y: null,
    active: false,
  })

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

  useEffect(() => {
    const element = canvasRef.current
    if (!element) return
    const refresh = () => {
      const rect = element.getBoundingClientRect()
      setCanvasSize({ width: rect.width, height: rect.height })
    }
    refresh()
    const observer = new ResizeObserver(refresh)
    observer.observe(element)
    return () => observer.disconnect()
  }, [])

  const flowWidth = Math.max(0, canvasSize.width - RULER_SIZE)
  const flowHeight = Math.max(0, canvasSize.height - RULER_SIZE)
  const alignGuidePx = useMemo(() => {
    const x = alignGuide.x == null ? null : alignGuide.x * viewportZoom + viewportX
    const y = alignGuide.y == null ? null : alignGuide.y * viewportZoom + viewportY
    return { x, y }
  }, [alignGuide.x, alignGuide.y, viewportX, viewportY, viewportZoom])

  const topRulerTicks = useMemo(() => {
    if (!Number.isFinite(flowWidth) || flowWidth <= 0 || !Number.isFinite(viewportZoom) || viewportZoom <= 0) return []
    const startWorld = (-viewportX) / viewportZoom
    const endWorld = startWorld + (flowWidth / viewportZoom)
    const first = Math.floor(startWorld / RULER_MINOR_STEP) * RULER_MINOR_STEP
    const ticks: Array<{ px: number; isMajor: boolean; label?: string }> = []
    for (let world = first; world <= endWorld + RULER_MINOR_STEP; world += RULER_MINOR_STEP) {
      const px = world * viewportZoom + viewportX
      if (px < -RULER_MINOR_STEP * viewportZoom || px > flowWidth + RULER_MINOR_STEP * viewportZoom) continue
      const isMajor = Math.round(world / RULER_MAJOR_STEP) * RULER_MAJOR_STEP === world
      ticks.push({
        px,
        isMajor,
        label: isMajor ? String(Math.round(world)) : undefined,
      })
    }
    return ticks
  }, [flowWidth, viewportX, viewportZoom])

  const leftRulerTicks = useMemo(() => {
    if (!Number.isFinite(flowHeight) || flowHeight <= 0 || !Number.isFinite(viewportZoom) || viewportZoom <= 0) return []
    const startWorld = (-viewportY) / viewportZoom
    const endWorld = startWorld + (flowHeight / viewportZoom)
    const first = Math.floor(startWorld / RULER_MINOR_STEP) * RULER_MINOR_STEP
    const ticks: Array<{ px: number; isMajor: boolean; label?: string }> = []
    for (let world = first; world <= endWorld + RULER_MINOR_STEP; world += RULER_MINOR_STEP) {
      const px = world * viewportZoom + viewportY
      if (px < -RULER_MINOR_STEP * viewportZoom || px > flowHeight + RULER_MINOR_STEP * viewportZoom) continue
      const isMajor = Math.round(world / RULER_MAJOR_STEP) * RULER_MAJOR_STEP === world
      ticks.push({
        px,
        isMajor,
        label: isMajor ? String(Math.round(world)) : undefined,
      })
    }
    return ticks
  }, [flowHeight, viewportY, viewportZoom])

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

  const onNodeDragStart = useCallback(() => {
    setAlignGuide((prev) => ({ ...prev, active: true }))
  }, [])

  const onNodeDrag = useCallback((_: React.MouseEvent, draggingNode: any) => {
    const movingRect = getNodeRect(draggingNode)
    const movingX = [movingRect.x, movingRect.x + movingRect.width / 2, movingRect.x + movingRect.width]
    const movingY = [movingRect.y, movingRect.y + movingRect.height / 2, movingRect.y + movingRect.height]

    let bestX: { delta: number; target: number; moving: number } | null = null
    let bestY: { delta: number; target: number; moving: number } | null = null

    for (const node of nodes as any[]) {
      if (!node || node.id === draggingNode.id) continue
      const rect = getNodeRect(node)
      const targetX = [rect.x, rect.x + rect.width / 2, rect.x + rect.width]
      const targetY = [rect.y, rect.y + rect.height / 2, rect.y + rect.height]

      for (const m of movingX) {
        for (const t of targetX) {
          const delta = Math.abs(m - t)
          if (delta <= NODE_ALIGN_THRESHOLD && (!bestX || delta < bestX.delta)) {
            bestX = { delta, target: t, moving: m }
          }
        }
      }
      for (const m of movingY) {
        for (const t of targetY) {
          const delta = Math.abs(m - t)
          if (delta <= NODE_ALIGN_THRESHOLD && (!bestY || delta < bestY.delta)) {
            bestY = { delta, target: t, moving: m }
          }
        }
      }
    }

    const offsetX = bestX ? bestX.target - bestX.moving : 0
    const offsetY = bestY ? bestY.target - bestY.moving : 0
    if (offsetX !== 0 || offsetY !== 0) {
      useWorkflowStore.setState((state) => ({
        nodes: state.nodes.map((node) => (
          node.id === draggingNode.id
            ? { ...node, position: { x: node.position.x + offsetX, y: node.position.y + offsetY } }
            : node
        )),
      }))
    }

    setAlignGuide({
      x: bestX ? bestX.target : null,
      y: bestY ? bestY.target : null,
      active: true,
    })
  }, [nodes])

  const onNodeDragStop = useCallback(() => {
    setAlignGuide({ x: null, y: null, active: false })
  }, [])

  return (
    <div ref={canvasRef} style={{ flex: 1, height: '100%', position: 'relative', overflow: 'hidden' }}>
      <div
        style={{
          position: 'absolute',
          top: 0,
          left: 0,
          width: RULER_SIZE,
          height: RULER_SIZE,
          background: 'var(--app-ruler-bg)',
          borderRight: '1px solid var(--app-ruler-border)',
          borderBottom: '1px solid var(--app-ruler-border)',
          zIndex: 8,
          pointerEvents: 'none',
        }}
      />

      <div
        style={{
          position: 'absolute',
          top: 0,
          left: RULER_SIZE,
          right: 0,
          height: RULER_SIZE,
          background: 'var(--app-ruler-bg)',
          borderBottom: '1px solid var(--app-ruler-border)',
          overflow: 'hidden',
          zIndex: 8,
          pointerEvents: 'none',
        }}
      >
        {topRulerTicks.map((tick) => (
          <div key={`rt-${tick.px}-${tick.label || ''}`} style={{ position: 'absolute', left: tick.px, top: 0, bottom: 0 }}>
            <div
              style={{
                position: 'absolute',
                bottom: 0,
                left: 0,
                width: 1,
                height: tick.isMajor ? 12 : 6,
                background: tick.isMajor ? 'var(--app-ruler-major)' : 'var(--app-ruler-tick)',
              }}
            />
            {tick.isMajor && tick.label && (
              <div
                style={{
                  position: 'absolute',
                  top: 1,
                  left: 2,
                  fontSize: 9,
                  color: 'var(--app-ruler-text)',
                  lineHeight: 1,
                }}
              >
                {tick.label}
              </div>
            )}
          </div>
        ))}
      </div>

      <div
        style={{
          position: 'absolute',
          top: RULER_SIZE,
          left: 0,
          bottom: 0,
          width: RULER_SIZE,
          background: 'var(--app-ruler-bg)',
          borderRight: '1px solid var(--app-ruler-border)',
          overflow: 'hidden',
          zIndex: 8,
          pointerEvents: 'none',
        }}
      >
        {leftRulerTicks.map((tick) => (
          <div key={`rl-${tick.px}-${tick.label || ''}`} style={{ position: 'absolute', top: tick.px, left: 0, right: 0 }}>
            <div
              style={{
                position: 'absolute',
                right: 0,
                top: 0,
                height: 1,
                width: tick.isMajor ? 12 : 6,
                background: tick.isMajor ? 'var(--app-ruler-major)' : 'var(--app-ruler-tick)',
              }}
            />
            {tick.isMajor && tick.label && (
              <div
                style={{
                  position: 'absolute',
                  top: -4,
                  left: 1,
                  fontSize: 9,
                  color: 'var(--app-ruler-text)',
                  lineHeight: 1,
                  transform: 'rotate(-90deg)',
                  transformOrigin: 'left top',
                }}
              >
                {tick.label}
              </div>
            )}
          </div>
        ))}
      </div>

      <div style={{ position: 'absolute', top: RULER_SIZE, left: RULER_SIZE, right: 0, bottom: 0 }}>
      {alignGuide.active && (alignGuidePx.x != null || alignGuidePx.y != null) && (
        <div
          style={{
            position: 'absolute',
            inset: 0,
            pointerEvents: 'none',
            zIndex: 10,
          }}
        >
          {alignGuidePx.x != null && (
            <div
              style={{
                position: 'absolute',
                left: alignGuidePx.x,
                top: 0,
                bottom: 0,
                width: 2,
                opacity: 1,
                background: 'repeating-linear-gradient(to bottom, var(--app-ruler-major) 0 2px, transparent 2px 6px)',
              }}
            />
          )}
          {alignGuidePx.y != null && (
            <div
              style={{
                position: 'absolute',
                top: alignGuidePx.y,
                left: 0,
                right: 0,
                height: 2,
                opacity: 1,
                background: 'repeating-linear-gradient(to right, var(--app-ruler-major) 0 2px, transparent 2px 6px)',
              }}
            />
          )}
        </div>
      )}
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
        onNodeDragStart={onNodeDragStart}
        onNodeDrag={onNodeDrag}
        onNodeDragStop={onNodeDragStop}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        onDrop={onDrop}
        onDragOver={onDragOver}
        edgesUpdatable
        deleteKeyCode={['Backspace', 'Delete']}
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
        style={{ background: 'var(--app-panel-bg)' }}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={24}
          size={1.2}
          color="var(--app-canvas-dot)"
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
    </div>
  )
}
