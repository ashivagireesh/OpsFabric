import { useEffect, useMemo, useRef, useState } from 'react'
import type { KeyboardEvent } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import {
  Alert,
  Badge,
  Button,
  Card,
  Divider,
  Drawer,
  Empty,
  Input,
  InputNumber,
  List,
  Select,
  Space,
  Switch,
  Tag,
  Tooltip,
  Typography,
  message,
} from 'antd'
import {
  ApiOutlined,
  AppstoreOutlined,
  ArrowsAltOutlined,
  BugOutlined,
  CloseOutlined,
  CodeOutlined,
  CompressOutlined,
  DatabaseOutlined,
  MessageOutlined,
  PlusOutlined,
  RobotOutlined,
  SaveOutlined,
  SendOutlined,
  SettingOutlined,
} from '@ant-design/icons'
import api from '../api/client'
import { useWorkflowStore } from '../store'
import { useThemeStore } from '../store/themeStore'
import { ALL_NODE_TYPES } from '../constants/nodeTypes'

const { Text, Paragraph } = Typography
const { TextArea } = Input

type DevProvider = {
  id: string
  name: string
  provider: string
  model: string
  base_url?: string
  api_key?: string
  has_api_key?: boolean
  enabled: boolean
  is_default: boolean
  temperature: number
  max_tokens: number
  timeout_seconds: number
  streaming?: boolean
  system_prompt?: string
}

type ChatRole = 'user' | 'assistant'

type ChatTurn = {
  id: string
  role: ChatRole
  content: string
  meta?: {
    provider?: string
    model?: string
    status?: string
    error?: string
  }
}

type PromptActionResult = {
  handled: boolean
  response?: string
}

const secretKeyRe = /(api[_-]?key|token|secret|password|passwd|pwd|credential|auth)/i

const defaultSystemPrompt =
  'You are Framework Copilot, an environment-aware, configuration-aware, and data-aware development assistant inside this ETL/MLOps framework. Use exact node names, field names, route paths, and config keys from context. Keep answers actionable.'

const providerOptions = [
  { value: 'openai', label: 'OpenAI / ChatGPT / Codex' },
  { value: 'gemini', label: 'Google Gemini' },
  { value: 'ollama', label: 'Ollama Local' },
  { value: 'mistral', label: 'Mistral' },
  { value: 'custom', label: 'Custom compatible' },
]

function newProvider(partial: Partial<DevProvider> = {}): DevProvider {
  const provider = partial.provider || 'openai'
  return {
    id: partial.id || `local_${Date.now()}`,
    name: partial.name || 'OpenAI',
    provider,
    model: partial.model || (provider === 'ollama' ? 'llama3.1:8b' : 'gpt-4o-mini'),
    base_url: partial.base_url || (provider === 'ollama' ? 'http://localhost:11434' : ''),
    api_key: partial.api_key || '',
    has_api_key: Boolean(partial.has_api_key),
    enabled: partial.enabled ?? true,
    is_default: partial.is_default ?? false,
    temperature: partial.temperature ?? 0.2,
    max_tokens: partial.max_tokens ?? 1200,
    timeout_seconds: partial.timeout_seconds ?? 60,
    streaming: partial.streaming ?? false,
    system_prompt: partial.system_prompt || defaultSystemPrompt,
  }
}

function redact(value: unknown, depth = 0): unknown {
  if (depth > 5) return '...truncated...'
  if (Array.isArray(value)) return value.slice(0, 40).map((item) => redact(item, depth + 1))
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {}
    Object.entries(value as Record<string, unknown>).slice(0, 120).forEach(([key, item]) => {
      out[key] = secretKeyRe.test(key) ? (item ? '***REDACTED***' : '') : redact(item, depth + 1)
    })
    return out
  }
  if (typeof value === 'string' && value.length > 2000) return `${value.slice(0, 2000)}...truncated...`
  return value
}

function compactNode(node: any) {
  const data = node?.data || {}
  const config = data?.config || {}
  return {
    id: node?.id,
    label: data?.label,
    nodeType: data?.nodeType,
    status: data?.status,
    position: node?.position,
    config_keys: Object.keys(config).slice(0, 60),
    config: redact(config),
    rows: data?.executionRows,
    error: data?.executionError,
    sample_input: redact(data?.executionSampleInput || []),
    sample_output: redact(data?.executionSampleOutput || []),
  }
}

function inferPipelineId(pathname: string): string | undefined {
  const match = pathname.match(/\/(?:pipelines|mlops|business)\/([^/]+)/)
  return match?.[1]
}

function shortId() {
  return `${Date.now()}_${Math.random().toString(16).slice(2)}`
}

function normalizeSearchText(value: unknown): string {
  return String(value || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim()
}

function parsePromptValue(value: string): unknown {
  const text = value.trim().replace(/^['"]|['"]$/g, '')
  if (!text) return ''
  if (/^(true|false)$/i.test(text)) return /^true$/i.test(text)
  if (/^null$/i.test(text)) return null
  if (/^-?\d+(\.\d+)?$/.test(text)) return Number(text)
  if ((text.startsWith('{') && text.endsWith('}')) || (text.startsWith('[') && text.endsWith(']'))) {
    try { return JSON.parse(text) } catch { return text }
  }
  return text
}

function extractKeyValuePairs(prompt: string): Record<string, unknown> {
  const pairs: Record<string, unknown> = {}
  const pairRe = /([a-zA-Z_][\w.-]*)\s*(?:=|:)\s*("[^"]*"|'[^']*'|\{[^}]*\}|\[[^\]]*\]|[^\s,;]+)/g
  let match: RegExpExecArray | null
  while ((match = pairRe.exec(prompt)) !== null) {
    const key = String(match[1] || '').trim()
    if (!key) continue
    pairs[key] = parsePromptValue(String(match[2] || ''))
  }
  return pairs
}

function findRequestedNodeType(prompt: string): string | null {
  const normalizedPrompt = normalizeSearchText(prompt)
  if (!normalizedPrompt) return null
  const ranked = ALL_NODE_TYPES
    .map((def) => {
      const label = normalizeSearchText(def.label)
      const type = normalizeSearchText(def.type)
      const tags = (def.tags || []).map(normalizeSearchText)
      let score = 0
      if (normalizedPrompt.includes(type)) score += 12
      if (normalizedPrompt.includes(label)) score += 10
      tags.forEach((tag) => {
        if (tag && normalizedPrompt.includes(tag)) score += 3
      })
      label.split(' ').forEach((part) => {
        if (part.length > 2 && normalizedPrompt.includes(part)) score += 1
      })
      return { def, score }
    })
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
  return ranked[0]?.def.type || null
}

function extractPipelineName(prompt: string): string | null {
  const quoted = prompt.match(/(?:pipeline|workflow)\s+(?:named|called)\s+["']([^"']+)["']/i)
    || prompt.match(/(?:create|new|make)\s+(?:a\s+)?(?:pipeline|workflow)\s+["']([^"']+)["']/i)
  if (quoted?.[1]) return quoted[1].trim()
  const plain = prompt.match(/(?:create|new|make)\s+(?:a\s+)?(?:pipeline|workflow)(?:\s+named|\s+called)?\s+(.+)$/i)
  const name = String(plain?.[1] || '').trim().replace(/[.;]+$/g, '')
  if (!name || /^(and|with|then)$/i.test(name)) return null
  return name.slice(0, 80)
}

export default function DevAssistant() {
  const location = useLocation()
  const navigate = useNavigate()
  const themeMode = useThemeStore((state) => state.mode)
  const nodes = useWorkflowStore((state) => state.nodes)
  const edges = useWorkflowStore((state) => state.edges)
  const pipeline = useWorkflowStore((state) => state.pipeline)
  const selectedNodeId = useWorkflowStore((state) => state.selectedNodeId)
  const executionLogs = useWorkflowStore((state) => state.executionLogs)
  const isDirty = useWorkflowStore((state) => state.isDirty)
  const isExecuting = useWorkflowStore((state) => state.isExecuting)
  const addNode = useWorkflowStore((state) => state.addNode)
  const updateNodeConfig = useWorkflowStore((state) => state.updateNodeConfig)
  const updateNodeLabel = useWorkflowStore((state) => state.updateNodeLabel)
  const setSelectedNode = useWorkflowStore((state) => state.setSelectedNode)
  const savePipeline = useWorkflowStore((state) => state.savePipeline)

  const [open, setOpen] = useState(false)
  const [fullscreen, setFullscreen] = useState(false)
  const [highContrast, setHighContrast] = useState(false)
  const [largeText, setLargeText] = useState(false)
  const [settingsOpen, setSettingsOpen] = useState(false)
  const [providers, setProviders] = useState<DevProvider[]>([])
  const [selectedProviderId, setSelectedProviderId] = useState<string>()
  const [input, setInput] = useState('')
  const [turns, setTurns] = useState<ChatTurn[]>([])
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [testingProviderId, setTestingProviderId] = useState<string>()
  const listRef = useRef<HTMLDivElement | null>(null)

  const pipelineId = pipeline?.id || inferPipelineId(location.pathname)
  const selectedNode = useMemo(
    () => nodes.find((node) => node.id === selectedNodeId) || null,
    [nodes, selectedNodeId],
  )

  const activeProvider = useMemo(() => {
    return providers.find((item) => item.id === selectedProviderId)
      || providers.find((item) => item.enabled && item.is_default)
      || providers.find((item) => item.enabled)
      || providers[0]
  }, [providers, selectedProviderId])

  const context = useMemo(() => ({
    route: {
      pathname: location.pathname,
      search: location.search,
    },
    browser: {
      userAgent: navigator.userAgent,
      viewport: { width: window.innerWidth, height: window.innerHeight },
    },
    workflow_state: {
      isDirty,
      isExecuting,
      selectedNodeId,
      pipeline_id: pipelineId,
      pipeline_name: pipeline?.name,
      node_count: nodes.length,
      edge_count: edges.length,
      nodes: nodes.slice(0, 80).map(compactNode),
      edges: redact(edges.slice(0, 160)),
      selected_node: selectedNode ? compactNode(selectedNode) : null,
      execution_logs_tail: redact(executionLogs.slice(-20)),
    },
  }), [edges, executionLogs, isDirty, isExecuting, location.pathname, location.search, nodes, pipeline?.name, pipelineId, selectedNode, selectedNodeId])

  useEffect(() => {
    if (!open) return
    void loadProviders()
  }, [open])

  useEffect(() => {
    const handleOpen = () => setOpen(true)
    window.addEventListener('framework-copilot:open', handleOpen)
    return () => window.removeEventListener('framework-copilot:open', handleOpen)
  }, [])

  useEffect(() => {
    listRef.current?.scrollTo({ top: listRef.current.scrollHeight, behavior: 'smooth' })
  }, [turns, loading])

  async function loadProviders() {
    try {
      const result = await api.listDevChatProviders()
      const normalized = Array.isArray(result?.providers)
        ? result.providers.map((item: Partial<DevProvider>) => newProvider(item))
        : []
      setProviders(normalized)
      const nextDefault = normalized.find((item: DevProvider) => item.enabled && item.is_default) || normalized[0]
      setSelectedProviderId((current) => current || nextDefault?.id)
    } catch (error) {
      message.error(`Unable to load assistant providers: ${String((error as Error)?.message || error)}`)
    }
  }

  function updateProvider(id: string, patch: Partial<DevProvider>) {
    setProviders((current) => current.map((provider) => {
      if (provider.id !== id) return provider
      const next = { ...provider, ...patch }
      if (patch.provider && patch.provider !== provider.provider) {
        if (patch.provider === 'ollama') {
          next.model = 'llama3.1:8b'
          next.base_url = next.base_url || 'http://localhost:11434'
        } else if (patch.provider === 'gemini') {
          next.model = 'gemini-1.5-flash'
        } else if (patch.provider === 'mistral') {
          next.model = 'mistral-small-latest'
        } else {
          next.model = 'gpt-4o-mini'
        }
      }
      return next
    }))
  }

  function setDefaultProvider(id: string) {
    setProviders((current) => current.map((provider) => ({
      ...provider,
      enabled: provider.id === id ? true : provider.enabled,
      is_default: provider.id === id,
    })))
    setSelectedProviderId(id)
  }

  async function saveProviders() {
    setSaving(true)
    try {
      const result = await api.saveDevChatProviders(providers as unknown as Array<Record<string, unknown>>)
      const normalized = Array.isArray(result?.providers)
        ? result.providers.map((item: Partial<DevProvider>) => newProvider(item))
        : providers
      setProviders(normalized)
      message.success('Assistant provider configuration saved')
    } catch (error) {
      message.error(`Provider save failed: ${String((error as Error)?.message || error)}`)
    } finally {
      setSaving(false)
    }
  }

  async function testProvider(provider: DevProvider) {
    setTestingProviderId(provider.id)
    try {
      const result = await api.testDevChatProvider(provider as unknown as Record<string, unknown>)
      if (result?.ok) {
        message.success(result.message || 'Provider test passed')
      } else {
        message.error(result?.message || 'Provider test failed')
      }
    } catch (error) {
      message.error(`Provider test failed: ${String((error as Error)?.message || error)}`)
    } finally {
      setTestingProviderId(undefined)
    }
  }

  async function runPromptAction(content: string): Promise<PromptActionResult> {
    const prompt = content.trim()
    const normalized = normalizeSearchText(prompt)
    const isEditorRoute = /^\/pipelines\/[^/]+\/edit/.test(location.pathname)

    if (/\b(create|new|make)\b/.test(normalized) && /\b(pipeline|workflow)\b/.test(normalized)) {
      const name = extractPipelineName(prompt) || 'Untitled AI Pipeline'
      const created = await api.createPipeline({
        name,
        description: 'Created from Framework Copilot prompt.',
      })
      if (created?.id) {
        setOpen(false)
        navigate(`/pipelines/${created.id}/edit`)
        return {
          handled: true,
          response: `Created pipeline "${created.name || name}" and opened it. You can now ask me to add nodes or configure fields.`,
        }
      }
      return { handled: true, response: `Tried to create pipeline "${name}", but the backend did not return a pipeline id.` }
    }

    if ((/\b(add|create|insert|make)\b/.test(normalized) && /\b(node|source|target|destination|trigger|transform)\b/.test(normalized))) {
      if (!isEditorRoute) {
        return { handled: true, response: 'Open a pipeline editor first, then ask me to add nodes.' }
      }
      const nodeType = findRequestedNodeType(prompt)
      if (!nodeType) {
        return {
          handled: true,
          response: 'I could not identify the node type. Try a concrete request like "add Oracle node", "create CSV File source", or "add Data Query node".',
        }
      }
      const beforeIds = new Set(useWorkflowStore.getState().nodes.map((node) => node.id))
      const x = 220 + (useWorkflowStore.getState().nodes.length % 5) * 170
      const y = 120 + Math.floor(useWorkflowStore.getState().nodes.length / 5) * 120
      addNode(nodeType, { x, y })
      const afterState = useWorkflowStore.getState()
      const createdNode = afterState.nodes.find((node) => !beforeIds.has(node.id)) || afterState.nodes[afterState.nodes.length - 1]
      const configPatch = extractKeyValuePairs(prompt)
      if (createdNode && Object.keys(configPatch).length > 0) {
        const labelValue = configPatch.label || configPatch.name
        if (typeof labelValue === 'string' && labelValue.trim()) {
          updateNodeLabel(createdNode.id, labelValue.trim())
          delete configPatch.label
          delete configPatch.name
        }
        if (Object.keys(configPatch).length > 0) updateNodeConfig(createdNode.id, configPatch)
      }
      const label = createdNode?.data?.label || ALL_NODE_TYPES.find((def) => def.type === nodeType)?.label || nodeType
      return {
        handled: true,
        response: `Added ${label} node${Object.keys(configPatch).length ? ` and applied config: ${Object.keys(configPatch).join(', ')}` : ''}. It is selected on the canvas.`,
      }
    }

    if (/\b(select|focus)\b/.test(normalized) && /\bnode\b/.test(normalized)) {
      const requestedType = findRequestedNodeType(prompt)
      const requestedText = normalizeSearchText(prompt.replace(/\b(select|focus|node)\b/gi, ''))
      const match = nodes.find((node) => {
        const label = normalizeSearchText(node.data?.label)
        const type = normalizeSearchText(node.data?.nodeType)
        return (requestedType && node.data?.nodeType === requestedType)
          || (requestedText && (label.includes(requestedText) || requestedText.includes(label) || type.includes(requestedText)))
      })
      if (match) {
        setSelectedNode(match.id)
        return { handled: true, response: `Selected node "${match.data?.label || match.id}".` }
      }
      return { handled: true, response: 'I could not find a matching node to select.' }
    }

    if (/\b(configure|config|set|update|change)\b/.test(normalized) && /\b(selected node|node|field|config)\b/.test(normalized)) {
      let targetNodeId = selectedNodeId
      if (!targetNodeId) {
        const requestedType = findRequestedNodeType(prompt)
        const requestedText = normalizeSearchText(prompt.replace(/\b(configure|config|set|update|change|node|field)\b/gi, ''))
        const targetNode = nodes.find((node) => {
          const label = normalizeSearchText(node.data?.label)
          const type = normalizeSearchText(node.data?.nodeType)
          return (requestedType && node.data?.nodeType === requestedType)
            || (requestedText && label && requestedText.includes(label))
            || (requestedText && type && requestedText.includes(type))
        })
        targetNodeId = targetNode?.id || null
      }
      if (!targetNodeId) {
        return { handled: true, response: 'Select a node first, mention a node type/name, or ask me to create a node and include config values in the same prompt.' }
      }
      const patch = extractKeyValuePairs(prompt)
      if (Object.keys(patch).length === 0) {
        return {
          handled: true,
          response: 'I need key=value pairs to update config. Example: set host=localhost port=1521 database=ORCL on selected node.',
        }
      }
      const labelValue = patch.label || patch.name
      if (typeof labelValue === 'string' && labelValue.trim()) {
        updateNodeLabel(targetNodeId, labelValue.trim())
        delete patch.label
        delete patch.name
      }
      setSelectedNode(targetNodeId)
      if (Object.keys(patch).length > 0) updateNodeConfig(targetNodeId, patch)
      return {
        handled: true,
        response: `Updated selected node${Object.keys(patch).length ? ` config: ${Object.keys(patch).join(', ')}` : ' label'}.`,
      }
    }

    if (/\b(save|persist)\b/.test(normalized) && /\b(pipeline|workflow|canvas)\b/.test(normalized)) {
      await savePipeline()
      return { handled: true, response: 'Saved the current pipeline canvas.' }
    }

    return { handled: false }
  }

  async function sendMessage(seed?: string) {
    const content = String(seed ?? input).trim()
    if (!content || loading) return
    const userTurn: ChatTurn = { id: shortId(), role: 'user', content }
    setTurns((current) => [...current, userTurn])
    setInput('')
    try {
      const actionResult = await runPromptAction(content)
      if (actionResult.handled) {
        setTurns((current) => [...current, {
          id: shortId(),
          role: 'assistant',
          content: actionResult.response || 'Done.',
          meta: { provider: 'Framework actions', model: 'client-side', status: 'applied' },
        }])
        return
      }
    } catch (error) {
      setTurns((current) => [...current, {
        id: shortId(),
        role: 'assistant',
        content: `I tried to apply that action, but it failed: ${String((error as Error)?.message || error)}`,
        meta: { provider: 'Framework actions', model: 'client-side', status: 'action_error' },
      }])
      return
    }
    setLoading(true)
    try {
      const result = await api.sendDevChatMessage({
        message: content,
        provider_id: activeProvider?.id,
        history: [...turns, userTurn].slice(-12).map((turn) => ({ role: turn.role, content: turn.content })),
        context,
        pipeline_id: pipelineId,
        selected_node_id: selectedNodeId,
      })
      setTurns((current) => [...current, {
        id: shortId(),
        role: 'assistant',
        content: String(result?.answer || 'No response returned.'),
        meta: {
          provider: result?.provider_name || result?.provider,
          model: result?.model,
          status: result?.status,
          error: result?.error,
        },
      }])
    } catch (error) {
      setTurns((current) => [...current, {
        id: shortId(),
        role: 'assistant',
        content: `The assistant request failed: ${String((error as Error)?.message || error)}`,
        meta: { status: 'request_error' },
      }])
    } finally {
      setLoading(false)
    }
  }

  function handleKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault()
      void sendMessage()
    }
  }

  const quickActions = [
    { icon: <AppstoreOutlined />, label: 'Explain current node', prompt: 'Explain the selected node configuration, inputs, outputs, and likely risks.' },
    { icon: <BugOutlined />, label: 'Why validation failed?', prompt: 'Inspect the current context and tell me why validation or training may be failing. Give exact checks.' },
    { icon: <ApiOutlined />, label: 'Provider setup', prompt: 'Suggest the best chat provider configuration for this local environment, including models and base URLs.' },
    { icon: <DatabaseOutlined />, label: 'Inspect data sample', prompt: 'Inspect the available sample rows and point out schema, null, type, and target-field issues.' },
  ]

  const drawerWidth = fullscreen ? '100vw' : Math.min(720, Math.max(520, Math.floor(window.innerWidth * 0.42)))
  const fontSize = largeText ? 15 : 13
  const isDark = themeMode === 'dark' || themeMode === 'enterprise-dark'
  const panelTone = highContrast ? '#050505' : isDark ? 'var(--app-shell-bg)' : '#f8fafc'
  const surfaceTone = highContrast ? '#111111' : isDark ? 'var(--app-card-bg)' : '#ffffff'
  const surfaceToneAlt = highContrast ? '#050505' : isDark ? 'var(--app-panel-2)' : '#f1f5f9'
  const textTone = highContrast ? '#ffffff' : isDark ? 'var(--app-text)' : '#0f172a'
  const mutedTone = highContrast ? '#d4d4d4' : isDark ? 'var(--app-text-muted)' : '#64748b'
  const borderTone = highContrast ? '#ffffff' : isDark ? 'var(--app-border-strong)' : '#dbe4f0'
  const hasEditorToolbarLauncher = /^\/pipelines\/[^/]+\/edit/.test(location.pathname)

  return (
    <>
      {!hasEditorToolbarLauncher && !open ? (
      <Tooltip title="Framework Copilot">
        <Badge dot={Boolean(selectedNode || executionLogs.length)}>
          <Button
            aria-label="Open Framework Copilot"
            type="primary"
            shape="circle"
            size="large"
            icon={<RobotOutlined />}
            onClick={() => setOpen(true)}
            style={{
              position: 'fixed',
              right: 22,
              bottom: 22,
              zIndex: 2000,
              width: 54,
              height: 54,
              boxShadow: '0 16px 40px var(--app-accent-border-strong)',
            }}
          />
        </Badge>
      </Tooltip>
      ) : null}

      <Drawer
        open={open}
        onClose={() => setOpen(false)}
        width={drawerWidth}
        placement="right"
        closable={false}
        styles={{
          body: { padding: 0, background: panelTone },
          header: { display: 'none' },
        }}
        destroyOnClose={false}
      >
        <div style={{ height: '100%', display: 'flex', flexDirection: 'column', color: textTone, fontSize }}>
          <div style={{ padding: 14, borderBottom: `1px solid ${borderTone}`, display: 'flex', alignItems: 'center', gap: 10, background: surfaceTone }}>
            <RobotOutlined style={{ fontSize: 22, color: 'var(--app-accent)' }} />
            <div style={{ flex: 1, minWidth: 0 }}>
              <Text style={{ color: textTone, fontWeight: 700 }}>Framework Copilot</Text>
              <div>
                <Text style={{ color: mutedTone, fontSize: 12 }}>
                  {activeProvider ? `${activeProvider.name} / ${activeProvider.model}` : 'Local context fallback'}
                </Text>
              </div>
            </div>
            <Tooltip title="Large text">
              <Switch size="small" checked={largeText} onChange={setLargeText} aria-label="Toggle large text" />
            </Tooltip>
            <Tooltip title="High contrast">
              <Switch size="small" checked={highContrast} onChange={setHighContrast} aria-label="Toggle high contrast" />
            </Tooltip>
            <Tooltip title="Configure providers">
              <Button icon={<SettingOutlined />} onClick={() => setSettingsOpen(true)} aria-label="Configure assistant providers" />
            </Tooltip>
            <Tooltip title={fullscreen ? 'Dock assistant' : 'Fullscreen assistant'}>
              <Button icon={fullscreen ? <CompressOutlined /> : <ArrowsAltOutlined />} onClick={() => setFullscreen((value) => !value)} aria-label="Toggle assistant fullscreen" />
            </Tooltip>
            <Button icon={<CloseOutlined />} onClick={() => setOpen(false)} aria-label="Close assistant" />
          </div>

          <div style={{ padding: '10px 14px', borderBottom: `1px solid ${borderTone}`, background: panelTone }}>
            <Space wrap size={[8, 8]}>
              <Tag color={pipelineId ? 'blue' : 'default'}>{pipelineId ? `Pipeline ${pipelineId}` : 'No pipeline route'}</Tag>
              <Tag color={selectedNode ? 'purple' : 'default'}>{selectedNode ? selectedNode.data?.label || selectedNode.id : 'No selected node'}</Tag>
              <Tag color={isExecuting ? 'processing' : isDirty ? 'warning' : 'success'}>{isExecuting ? 'executing' : isDirty ? 'unsaved' : 'clean'}</Tag>
            </Space>
          </div>

          <div ref={listRef} style={{ flex: 1, overflowY: 'auto', padding: 14 }}>
            {turns.length === 0 ? (
              <Empty
                image={Empty.PRESENTED_IMAGE_SIMPLE}
                description={<span style={{ color: mutedTone }}>Ask about config, provider setup, data samples, training failures, API routes, or current node behavior.</span>}
              />
            ) : (
              <List
                dataSource={turns}
                split={false}
                renderItem={(turn) => (
                  <List.Item style={{ padding: '8px 0', justifyContent: turn.role === 'user' ? 'flex-end' : 'flex-start' }}>
                    <Card
                      size="small"
                      style={{
                        width: turn.role === 'user' ? '86%' : '96%',
                        background: turn.role === 'user' ? 'var(--app-accent)' : surfaceTone,
                        borderColor: turn.role === 'user' ? 'var(--app-accent-border-strong)' : borderTone,
                        color: turn.role === 'user' ? '#fff' : textTone,
                      }}
                      styles={{ body: { padding: 12 } }}
                    >
                      <Paragraph style={{ color: turn.role === 'user' ? '#fff' : textTone, whiteSpace: 'pre-wrap', marginBottom: turn.meta ? 8 : 0, fontSize }}>
                        {turn.content}
                      </Paragraph>
                      {turn.meta ? (
                        <Space wrap size={6}>
                          {turn.meta.provider ? <Tag>{turn.meta.provider}</Tag> : null}
                          {turn.meta.model ? <Tag>{turn.meta.model}</Tag> : null}
                          {turn.meta.status ? <Tag color={turn.meta.status === 'remote' ? 'green' : 'gold'}>{turn.meta.status}</Tag> : null}
                        </Space>
                      ) : null}
                    </Card>
                  </List.Item>
                )}
              />
            )}
            {loading ? <Alert type="info" showIcon message="Thinking with current environment, config, and data context..." /> : null}
          </div>

          <div style={{ padding: 14, borderTop: `1px solid ${borderTone}`, background: surfaceToneAlt }}>
            <Space wrap size={[8, 8]} style={{ marginBottom: 10 }}>
              {quickActions.map((action) => (
                <Button
                  key={action.label}
                  size="small"
                  icon={action.icon}
                  disabled={loading}
                  onClick={() => void sendMessage(action.prompt)}
                >
                  {action.label}
                </Button>
              ))}
            </Space>
            <TextArea
              aria-label="Framework Copilot message"
              value={input}
              onChange={(event) => setInput(event.target.value)}
              onKeyDown={handleKeyDown}
              autoSize={{ minRows: 2, maxRows: 7 }}
              placeholder="Ask Framework Copilot..."
              style={{ marginBottom: 10, fontSize }}
            />
            <Space style={{ width: '100%', justifyContent: 'space-between' }}>
              <Select
                aria-label="Assistant provider"
                value={activeProvider?.id}
                onChange={setSelectedProviderId}
                style={{ width: 230 }}
                options={providers.map((provider) => ({
                  value: provider.id,
                  label: `${provider.name}${provider.is_default ? ' (default)' : ''}`,
                  disabled: !provider.enabled,
                }))}
                placeholder="Provider"
              />
              <Button type="primary" icon={<SendOutlined />} loading={loading} onClick={() => void sendMessage()}>
                Send
              </Button>
            </Space>
          </div>
        </div>
      </Drawer>

      <Drawer
        title="Assistant Provider Configuration"
        open={settingsOpen}
        onClose={() => setSettingsOpen(false)}
        width={Math.min(860, Math.max(620, Math.floor(window.innerWidth * 0.54)))}
        destroyOnClose={false}
        extra={(
          <Space>
            <Button
              icon={<PlusOutlined />}
              onClick={() => setProviders((current) => [...current, newProvider({ name: `Provider ${current.length + 1}` })])}
            >
              Add Provider
            </Button>
            <Button type="primary" icon={<SaveOutlined />} loading={saving} onClick={() => void saveProviders()}>
              Save
            </Button>
          </Space>
        )}
      >
        <Alert
          type="info"
          showIcon
          message="Configure any OpenAI-compatible model, Gemini, Ollama, or Mistral. API keys are stored in backend system settings and redacted from chat context."
          style={{ marginBottom: 14 }}
        />
        <Space direction="vertical" style={{ width: '100%' }} size={14}>
          {providers.map((provider) => (
            <Card
              key={provider.id}
              size="small"
              title={(
                <Space>
                  <CodeOutlined />
                  <span>{provider.name || provider.provider}</span>
                  {provider.is_default ? <Tag color="green">default</Tag> : null}
                  {provider.has_api_key ? <Tag color="blue">key saved</Tag> : null}
                </Space>
              )}
              extra={(
                <Space>
                  <Switch checked={provider.enabled} onChange={(enabled) => updateProvider(provider.id, { enabled })} />
                  <Button size="small" onClick={() => setDefaultProvider(provider.id)}>Default</Button>
                  <Button size="small" loading={testingProviderId === provider.id} onClick={() => void testProvider(provider)}>Test</Button>
                </Space>
              )}
            >
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: 12 }}>
                <label>
                  <Text strong>Name</Text>
                  <Input value={provider.name} onChange={(event) => updateProvider(provider.id, { name: event.target.value })} />
                </label>
                <label>
                  <Text strong>Provider</Text>
                  <Select
                    value={provider.provider}
                    onChange={(value) => updateProvider(provider.id, { provider: value })}
                    options={providerOptions}
                    style={{ width: '100%' }}
                  />
                </label>
                <label>
                  <Text strong>Model</Text>
                  <Input value={provider.model} onChange={(event) => updateProvider(provider.id, { model: event.target.value })} placeholder="gpt-4o-mini, gemini-1.5-pro, llama3.1:8b" />
                </label>
                <label>
                  <Text strong>Base URL</Text>
                  <Input value={provider.base_url} onChange={(event) => updateProvider(provider.id, { base_url: event.target.value })} placeholder="https://api.openai.com/v1" />
                </label>
                <label>
                  <Text strong>API key</Text>
                  <Input.Password
                    value={provider.api_key}
                    onChange={(event) => updateProvider(provider.id, { api_key: event.target.value })}
                    placeholder={provider.has_api_key ? 'Saved key retained if left blank' : 'Provider API key'}
                  />
                </label>
                <Space align="end">
                  <label style={{ width: 120 }}>
                    <Text strong>Temperature</Text>
                    <InputNumber min={0} max={2} step={0.1} value={provider.temperature} onChange={(value) => updateProvider(provider.id, { temperature: Number(value ?? 0.2) })} style={{ width: '100%' }} />
                  </label>
                  <label style={{ width: 130 }}>
                    <Text strong>Max tokens</Text>
                    <InputNumber min={128} max={16000} value={provider.max_tokens} onChange={(value) => updateProvider(provider.id, { max_tokens: Number(value ?? 1200) })} style={{ width: '100%' }} />
                  </label>
                  <label style={{ width: 130 }}>
                    <Text strong>Timeout sec</Text>
                    <InputNumber min={5} max={300} value={provider.timeout_seconds} onChange={(value) => updateProvider(provider.id, { timeout_seconds: Number(value ?? 60) })} style={{ width: '100%' }} />
                  </label>
                </Space>
              </div>
              <Divider style={{ margin: '12px 0' }} />
              <label>
                <Text strong>System prompt</Text>
                <TextArea
                  value={provider.system_prompt}
                  onChange={(event) => updateProvider(provider.id, { system_prompt: event.target.value })}
                  autoSize={{ minRows: 3, maxRows: 7 }}
                />
              </label>
            </Card>
          ))}
        </Space>
      </Drawer>
    </>
  )
}
