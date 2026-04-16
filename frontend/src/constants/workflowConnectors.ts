import type { ETLEdge } from '../types'

export const WORKFLOW_CONNECTOR_VALUES = ['smoothstep', 'step', 'straight'] as const
export type WorkflowConnectorType = (typeof WORKFLOW_CONNECTOR_VALUES)[number]

export const WORKFLOW_CONNECTOR_OPTIONS: Array<{ value: WorkflowConnectorType; label: string }> = [
  { value: 'smoothstep', label: 'Smooth' },
  { value: 'step', label: 'Stepped' },
  { value: 'straight', label: 'Straight' },
]

export const DEFAULT_WORKFLOW_CONNECTOR_TYPE: WorkflowConnectorType = 'smoothstep'

const VALID_CONNECTOR_TYPES = new Set<WorkflowConnectorType>(
  WORKFLOW_CONNECTOR_VALUES,
)

export function isWorkflowConnectorType(value: unknown): value is WorkflowConnectorType {
  return typeof value === 'string' && VALID_CONNECTOR_TYPES.has(value as WorkflowConnectorType)
}

export function resolveConnectorTypeFromEdges(edges: ETLEdge[]): WorkflowConnectorType {
  for (const edge of edges) {
    if (isWorkflowConnectorType(edge.type)) return edge.type
  }
  return DEFAULT_WORKFLOW_CONNECTOR_TYPE
}

export function applyConnectorTypeToEdges(
  edges: ETLEdge[],
  connectorType: WorkflowConnectorType,
): ETLEdge[] {
  return edges.map((edge) => ({ ...edge, type: connectorType }))
}
