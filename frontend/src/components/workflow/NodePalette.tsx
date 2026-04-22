import { useState, useMemo } from 'react'
import { Input, Typography, Tooltip, Badge } from 'antd'
import { SearchOutlined } from '@ant-design/icons'
import { ALL_NODE_TYPES, NODE_CATEGORIES } from '../../constants/nodeTypes'
import { useWorkflowStore } from '../../store'
import type { NodeTypeDefinition, NodeCategory } from '../../types'

const { Text } = Typography

interface NodePaletteProps {
  onClose?: () => void
}

export default function NodePalette({ onClose }: NodePaletteProps) {
  const [search, setSearch] = useState('')
  const [expandedCats, setExpandedCats] = useState<Set<NodeCategory>>(
    new Set(['trigger', 'source', 'transform', 'destination', 'flow'])
  )
  const addNode = useWorkflowStore(s => s.addNode)

  const handleDragStart = (e: React.DragEvent, nodeType: string) => {
    e.dataTransfer.setData('nodeType', nodeType)
    e.dataTransfer.effectAllowed = 'move'
  }

  const handleClick = (nodeType: string) => {
    addNode(nodeType)
    onClose?.()
  }

  const toggleCat = (cat: NodeCategory) => {
    setExpandedCats(prev => {
      const next = new Set(prev)
      if (next.has(cat)) next.delete(cat)
      else next.add(cat)
      return next
    })
  }

  const filtered = useMemo(() => {
    const q = search.toLowerCase()
    if (!q) return ALL_NODE_TYPES
    return ALL_NODE_TYPES.filter(n =>
      n.label.toLowerCase().includes(q) ||
      n.description.toLowerCase().includes(q) ||
      n.tags?.some(t => t.includes(q))
    )
  }, [search])

  const groupedNodes = useMemo(() => {
    const groups: Partial<Record<NodeCategory, NodeTypeDefinition[]>> = {}
    for (const node of filtered) {
      if (!groups[node.category]) groups[node.category] = []
      groups[node.category]!.push(node)
    }
    return groups
  }, [filtered])

  const categoryOrder: NodeCategory[] = ['trigger', 'source', 'transform', 'destination', 'flow']

  return (
    <div style={{
      width: 260,
      height: '100%',
      background: 'var(--app-panel-bg)',
      borderRight: '1px solid var(--app-border)',
      display: 'flex',
      flexDirection: 'column',
      overflow: 'hidden',
    }}>
      {/* Header */}
      <div style={{ padding: '16px 16px 12px', borderBottom: '1px solid var(--app-border)' }}>
        <Text style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 13, display: 'block', marginBottom: 10 }}>
          Add Nodes
        </Text>
        <Input
          placeholder="Search nodes..."
          prefix={<SearchOutlined style={{ color: 'var(--app-text-subtle)', fontSize: 12 }} />}
          value={search}
          onChange={e => setSearch(e.target.value)}
          size="small"
          style={{
            background: 'var(--app-card-bg)',
            border: '1px solid var(--app-border-strong)',
            color: 'var(--app-text)',
            borderRadius: 7,
            fontSize: 12,
          }}
          allowClear
        />
      </div>

      {/* Node List */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '8px 0' }}>
        {categoryOrder.map(cat => {
          const nodes = groupedNodes[cat]
          if (!nodes?.length) return null
          const catInfo = NODE_CATEGORIES[cat]
          const isExpanded = expandedCats.has(cat)

          return (
            <div key={cat}>
              {/* Category header */}
              <div
                onClick={() => toggleCat(cat)}
                style={{
                  display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                  padding: '8px 16px 6px',
                  cursor: 'pointer',
                  userSelect: 'none',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <span style={{ color: catInfo.color, fontSize: 12 }}>{catInfo.icon}</span>
                  <Text style={{ color: 'var(--app-text-muted)', fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                    {catInfo.label}
                  </Text>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <Badge
                    count={nodes.length}
                    style={{
                      background: `${catInfo.color}20`,
                      color: catInfo.color,
                      boxShadow: 'none',
                      fontSize: 10,
                    }}
                  />
                  <span style={{ color: 'var(--app-text-dim)', fontSize: 10, transform: isExpanded ? 'rotate(90deg)' : 'none', transition: 'transform 0.2s' }}>›</span>
                </div>
              </div>

              {/* Nodes in category */}
              {isExpanded && (
                <div style={{ paddingBottom: 4 }}>
                  {nodes.map(node => (
                    <NodeCard
                      key={node.type}
                      node={node}
                      onDragStart={(e) => handleDragStart(e, node.type)}
                      onClick={() => handleClick(node.type)}
                    />
                  ))}
                </div>
              )}
            </div>
          )
        })}

        {filtered.length === 0 && (
          <div style={{ padding: '32px 16px', textAlign: 'center' }}>
            <Text style={{ color: 'var(--app-text-dim)', fontSize: 13 }}>No nodes match "{search}"</Text>
          </div>
        )}
      </div>

      {/* Hint */}
      <div style={{
        padding: '10px 16px',
        borderTop: '1px solid var(--app-border)',
        color: 'var(--app-text-dim)',
        fontSize: 10,
        textAlign: 'center',
      }}>
        Click or drag nodes onto canvas
      </div>
    </div>
  )
}

function NodeCard({ node, onDragStart, onClick }: {
  node: NodeTypeDefinition
  onDragStart: (e: React.DragEvent) => void
  onClick: () => void
}) {
  return (
    <Tooltip title={node.description} placement="right" mouseEnterDelay={0.5}>
      <div
        draggable
        onDragStart={onDragStart}
        onClick={onClick}
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 10,
          padding: '7px 16px',
          cursor: 'grab',
          transition: 'background 0.15s',
          borderLeft: '2px solid transparent',
        }}
        onMouseEnter={e => {
          e.currentTarget.style.background = `${node.color}10`
          e.currentTarget.style.borderLeftColor = node.color
        }}
        onMouseLeave={e => {
          e.currentTarget.style.background = 'transparent'
          e.currentTarget.style.borderLeftColor = 'transparent'
        }}
      >
        {/* Mini icon */}
        <div style={{
          width: 28, height: 28,
          background: node.bgColor,
          border: `1px solid ${node.color}25`,
          borderRadius: 7,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          fontSize: 14, flexShrink: 0,
          fontFamily: 'system-ui',
        }}>
          {node.icon}
        </div>
        <div style={{ minWidth: 0 }}>
          <Text style={{ color: 'var(--app-text)', fontSize: 12, fontWeight: 500, display: 'block' }}>
            {node.label}
          </Text>
        </div>
      </div>
    </Tooltip>
  )
}
