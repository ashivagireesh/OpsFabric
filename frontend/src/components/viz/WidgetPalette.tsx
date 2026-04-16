import { useState } from 'react'
import { Typography, Input, Tooltip } from 'antd'
import {
  BarChartOutlined, LineChartOutlined, PieChartOutlined, DotChartOutlined,
  TableOutlined, NumberOutlined, FontSizeOutlined, RadarChartOutlined,
  FunnelPlotOutlined, HeatMapOutlined,
} from '@ant-design/icons'
import { v4 as uuidv4 } from 'uuid'
import { useVizStore } from '../../store/vizStore'
import type { Widget } from '../../store/vizStore'

const { Text } = Typography

const CHART_TYPES = [
  { type: 'kpi',          label: 'KPI Card',       icon: <NumberOutlined />,    color: '#6366f1', desc: 'Big number with trend', category: 'kpi' },
  { type: 'text',         label: 'Text Block',      icon: <FontSizeOutlined />,  color: 'var(--app-text-subtle)', desc: 'Rich text / markdown',  category: 'kpi' },
  { type: 'bar',          label: 'Bar Chart',       icon: <BarChartOutlined />,  color: '#6366f1', desc: 'Vertical bars',          category: 'chart' },
  { type: 'bar_horizontal', label: 'Horiz. Bar',   icon: <BarChartOutlined />,  color: '#8b5cf6', desc: 'Horizontal bars',        category: 'chart' },
  { type: 'line',         label: 'Line Chart',      icon: <LineChartOutlined />, color: '#06b6d4', desc: 'Trend over time',        category: 'chart' },
  { type: 'area',         label: 'Area Chart',      icon: <LineChartOutlined />, color: '#0891b2', desc: 'Filled area',            category: 'chart' },
  { type: 'pie',          label: 'Pie Chart',       icon: <PieChartOutlined />,  color: '#f97316', desc: 'Part-to-whole',          category: 'chart' },
  { type: 'donut',        label: 'Donut Chart',     icon: <PieChartOutlined />,  color: '#ec4899', desc: 'Donut + center total',   category: 'chart' },
  { type: 'scatter',      label: 'Scatter Plot',    icon: <DotChartOutlined />,  color: '#22c55e', desc: 'Correlation',            category: 'chart' },
  { type: 'radar',        label: 'Radar Chart',     icon: <RadarChartOutlined />,color: '#eab308', desc: 'Multi-dimension',        category: 'advanced' },
  { type: 'heatmap',      label: 'Heatmap',         icon: <HeatMapOutlined />,   color: '#ef4444', desc: 'Density / intensity',    category: 'advanced' },
  { type: 'treemap',      label: 'Treemap',         icon: <BarChartOutlined />,  color: '#8b5cf6', desc: 'Hierarchical data',      category: 'advanced' },
  { type: 'funnel',       label: 'Funnel',          icon: <FunnelPlotOutlined />,color: '#f59e0b', desc: 'Conversion stages',      category: 'advanced' },
  { type: 'gauge',        label: 'Gauge',           icon: <DotChartOutlined />,  color: '#06b6d4', desc: 'Progress / meter',       category: 'advanced' },
  { type: 'waterfall',    label: 'Waterfall',       icon: <BarChartOutlined />,  color: '#22c55e', desc: 'Running total',          category: 'advanced' },
  { type: 'table',        label: 'Data Table',      icon: <TableOutlined />,     color: 'var(--app-text-muted)', desc: 'Raw tabular data',       category: 'advanced' },
]

const SAMPLE_DATASETS = [
  { key: 'sales',         label: 'Sales by Region', icon: '📊' },
  { key: 'web_analytics', label: 'Web Analytics',   icon: '🌐' },
  { key: 'financials',    label: 'Financials',       icon: '💰' },
  { key: 'employees',     label: 'Employee Data',    icon: '👥' },
  { key: 'supply_chain',  label: 'Supply Chain',     icon: '🔗' },
  { key: 'customer',      label: 'Customer Data',    icon: '🎯' },
]

const TABS = [
  { key: 'all',      label: 'All' },
  { key: 'kpi',      label: 'KPI' },
  { key: 'chart',    label: 'Charts' },
  { key: 'advanced', label: 'Advanced' },
]

export default function WidgetPalette() {
  const { addWidget } = useVizStore()
  const [search, setSearch] = useState('')
  const [activeTab, setActiveTab] = useState('all')

  const filtered = CHART_TYPES.filter(ct =>
    (!search || ct.label.toLowerCase().includes(search.toLowerCase())) &&
    (activeTab === 'all' || ct.category === activeTab)
  )

  const handleAdd = (chartType: typeof CHART_TYPES[0]) => {
    const widget: Widget = {
      id: uuidv4(),
      type: chartType.type,
      title: chartType.label,
      data_source: { type: 'sample', dataset: 'sales' },
      chart_config: getDefaultConfig(chartType.type),
      style: { show_legend: true, show_grid: true, show_labels: false },
    }
    addWidget(widget)
  }

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>

      {/* ── Header ── */}
      <div style={{ padding: '10px 10px 6px', flexShrink: 0 }}>
        <Text style={{ color: '#6366f1', fontSize: 11, fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase', display: 'block', marginBottom: 6 }}>
          + Add Widget
        </Text>
        <Input
          placeholder="Search charts…"
          size="small"
          value={search}
          onChange={e => setSearch(e.target.value)}
          allowClear
          style={{ background: 'var(--app-input-bg)', border: '1px solid var(--app-border-strong)', color: 'var(--app-text)' }}
        />
      </div>

      {/* ── Custom tab bar (no Ant Tabs height issue) ── */}
      <div style={{
        display: 'flex',
        borderBottom: '1px solid var(--app-border)',
        flexShrink: 0,
        padding: '0 4px',
        gap: 2,
      }}>
        {TABS.map(tab => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            style={{
              background: 'none',
              border: 'none',
              borderBottom: `2px solid ${activeTab === tab.key ? '#6366f1' : 'transparent'}`,
              color: activeTab === tab.key ? '#6366f1' : 'var(--app-text-dim)',
              padding: '6px 8px',
              cursor: 'pointer',
              fontSize: 12,
              fontWeight: activeTab === tab.key ? 600 : 400,
              transition: 'color 0.15s, border-color 0.15s',
            }}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* ── Scrollable content ── */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '8px 6px', minHeight: 0 }}>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6 }}>
          {filtered.map(ct => (
            <Tooltip key={ct.type} title={ct.desc} placement="right">
              <div
                onClick={() => handleAdd(ct)}
                style={{
                  background: 'var(--app-shell-bg)',
                  border: '1px solid var(--app-border)',
                  borderRadius: 8,
                  padding: '10px 6px 8px',
                  cursor: 'pointer',
                  textAlign: 'center',
                  transition: 'all 0.15s',
                }}
                onMouseEnter={e => {
                  e.currentTarget.style.borderColor = ct.color
                  e.currentTarget.style.background = ct.color + '18'
                }}
                onMouseLeave={e => {
                  e.currentTarget.style.borderColor = 'var(--app-border)'
                  e.currentTarget.style.background = 'var(--app-shell-bg)'
                }}
              >
                <div style={{ fontSize: 18, color: ct.color, marginBottom: 4 }}>{ct.icon}</div>
                <Text style={{ color: 'var(--app-text-muted)', fontSize: 10, display: 'block', lineHeight: 1.3 }}>{ct.label}</Text>
              </div>
            </Tooltip>
          ))}
        </div>

        {filtered.length === 0 && search && (
          <div style={{ textAlign: 'center', padding: '24px 8px', color: 'var(--app-text-faint)', fontSize: 12 }}>
            No charts match "{search}"
          </div>
        )}

        {/* Sample datasets — only show on All tab when no search */}
        {(activeTab === 'all' || activeTab === 'kpi') && !search && (
          <div style={{ marginTop: 14, paddingTop: 10, borderTop: '1px solid var(--app-card-bg)' }}>
            <Text style={{ color: 'var(--app-text-faint)', fontSize: 10, fontWeight: 700, letterSpacing: '0.07em', textTransform: 'uppercase', display: 'block', marginBottom: 6 }}>
              Sample Datasets
            </Text>
            {SAMPLE_DATASETS.map(ds => (
              <div
                key={ds.key}
                style={{
                  padding: '5px 6px',
                  borderRadius: 5,
                  cursor: 'default',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 7,
                  marginBottom: 2,
                }}
                onMouseEnter={e => { e.currentTarget.style.background = 'var(--app-input-bg)' }}
                onMouseLeave={e => { e.currentTarget.style.background = 'transparent' }}
              >
                <span style={{ fontSize: 13 }}>{ds.icon}</span>
                <Text style={{ color: 'var(--app-text-dim)', fontSize: 11 }}>{ds.label}</Text>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function getDefaultConfig(type: string): Widget['chart_config'] {
  const defaults: Record<string, Widget['chart_config']> = {
    bar:            { x_field: 'region',  y_field: 'revenue', aggregation: 'sum', sort_by: 'value', sort_order: 'desc', limit: 10 },
    bar_horizontal: { x_field: 'region',  y_field: 'revenue', aggregation: 'sum', limit: 10 },
    line:           { x_field: 'quarter', y_field: 'revenue', y_fields: ['revenue', 'profit'], aggregation: 'sum', limit: 50 },
    area:           { x_field: 'quarter', y_field: 'revenue', y_fields: ['revenue', 'profit'], aggregation: 'sum', limit: 50 },
    pie:            { x_field: 'region',  y_field: 'revenue', aggregation: 'sum', limit: 8 },
    donut:          { x_field: 'region',  y_field: 'revenue', aggregation: 'sum', limit: 6 },
    scatter:        { x_field: 'units',   y_field: 'revenue', limit: 200 },
    heatmap:        { x_field: 'quarter', group_by: 'region', y_field: 'revenue', limit: 100 },
    treemap:        { x_field: 'product', y_field: 'revenue', aggregation: 'sum', limit: 20 },
    funnel:         { x_field: 'region',  y_field: 'revenue', aggregation: 'sum', limit: 6 },
    gauge:          { kpi_field: 'revenue', kpi_label: 'Total Revenue', limit: 1000000 },
    radar:          { group_by: 'region', limit: 50 },
    waterfall:      { x_field: 'region',  y_field: 'profit', aggregation: 'sum', limit: 8 },
    kpi:            { kpi_field: 'revenue', kpi_label: 'Total Revenue' },
    table:          { limit: 50 },
    text:           { text_content: '## Dashboard Notes\n\nAdd your notes here…' },
  }
  return defaults[type] || {}
}
