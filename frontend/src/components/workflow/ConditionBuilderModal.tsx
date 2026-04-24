import { AutoComplete, Button, Input, Modal, Select, Space, Typography } from 'antd'

const { Text } = Typography

type UiConditionLogicalJoin = 'and' | 'or'
type UiConditionClauseRightMode = 'literal' | 'field' | 'values' | 'variable' | 'raw'

type UiConditionCluster = {
  id: string
  joinWithPrev: UiConditionLogicalJoin
}

type UiConditionClause = {
  id: string
  clusterId: string
  joinWithPrev: UiConditionLogicalJoin
  leftField: string
  operator: string
  rightMode: UiConditionClauseRightMode
  rightValue: string
}

type SelectOption = {
  value: string
  label?: string
}

type ClusterWithClauses = {
  cluster: UiConditionCluster
  clauses: UiConditionClause[]
}

type ConditionBuilderModalProps = {
  open: boolean
  onClose: () => void
  placeholderIndex: number
  conditionState: { clauses: UiConditionClause[] }
  conditionClusters: UiConditionCluster[]
  conditionClausesByCluster: ClusterWithClauses[]
  conditionUndoCount: number
  conditionRedoCount: number
  uiFieldAutoCompleteOptions: SelectOption[]
  uiExpressionVariableOptions: SelectOption[]
  conditionOperatorOptions: string[]
  addUiConditionCluster: (index: number) => void
  undoUiConditionBuilder: (index: number) => void
  redoUiConditionBuilder: (index: number) => void
  updateUiConditionClusterJoin: (index: number, clusterId: string, joinWithPrev: UiConditionLogicalJoin) => void
  addUiConditionClause: (index: number, clusterId: string) => void
  removeUiConditionCluster: (index: number, clusterId: string) => void
  updateUiConditionClause: (index: number, clauseId: string, patch: Partial<UiConditionClause>) => void
  removeUiConditionClause: (index: number, clauseId: string) => void
  applyUiConditionBuilderToPlaceholder: (index: number) => void
}

export default function ConditionBuilderModal(props: ConditionBuilderModalProps) {
  const {
    open,
    onClose,
    placeholderIndex,
    conditionState,
    conditionClusters,
    conditionClausesByCluster,
    conditionUndoCount,
    conditionRedoCount,
    uiFieldAutoCompleteOptions,
    uiExpressionVariableOptions,
    conditionOperatorOptions,
    addUiConditionCluster,
    undoUiConditionBuilder,
    redoUiConditionBuilder,
    updateUiConditionClusterJoin,
    addUiConditionClause,
    removeUiConditionCluster,
    updateUiConditionClause,
    removeUiConditionClause,
    applyUiConditionBuilderToPlaceholder,
  } = props

  return (
    <Modal
      open={open}
      onCancel={onClose}
      footer={null}
      centered
      width="78vw"
      styles={{
        body: {
          padding: 12,
          maxHeight: '74vh',
          overflowY: 'auto',
        },
      }}
      title="Condition Builder"
    >
      <Space direction="vertical" size={8} style={{ width: '100%' }}>
        <Space align="center" style={{ justifyContent: 'space-between', width: '100%' }}>
          <Text style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 12 }}>
            Grouped Logical Conditions
          </Text>
          <Space size={6}>
            <Button size="small" onClick={() => addUiConditionCluster(placeholderIndex)}>
              Add Group
            </Button>
            <Button
              size="small"
              disabled={conditionUndoCount === 0}
              onClick={() => undoUiConditionBuilder(placeholderIndex)}
            >
              Undo
            </Button>
            <Button
              size="small"
              disabled={conditionRedoCount === 0}
              onClick={() => redoUiConditionBuilder(placeholderIndex)}
            >
              Redo
            </Button>
          </Space>
        </Space>
        <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
          Build grouped clauses using logical joins (`and`/`or`). Each group is wrapped in parentheses.
        </Text>
        {conditionClausesByCluster.map(({ cluster, clauses }, clusterIndex) => (
          <div
            key={`cond_cluster_${placeholderIndex}_${cluster.id}`}
            style={{
              border: '1px solid var(--app-border-strong)',
              borderRadius: 8,
              background: 'var(--app-panel-bg)',
              padding: 8,
            }}
          >
            <Space direction="vertical" size={6} style={{ width: '100%' }}>
              <div style={{ display: 'grid', gridTemplateColumns: 'minmax(100px, 1fr) minmax(120px, 0.8fr) auto', gap: 6, alignItems: 'center' }}>
                <Text style={{ color: 'var(--app-text)', fontWeight: 600, fontSize: 12 }}>
                  Group {clusterIndex + 1}
                </Text>
                {clusterIndex > 0 ? (
                  <Select
                    size="small"
                    value={cluster.joinWithPrev}
                    options={[
                      { value: 'and', label: 'and' },
                      { value: 'or', label: 'or' },
                    ]}
                    onChange={(value) => updateUiConditionClusterJoin(
                      placeholderIndex,
                      cluster.id,
                      value === 'or' ? 'or' : 'and',
                    )}
                    style={{ width: '100%' }}
                  />
                ) : (
                  <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                    First group
                  </Text>
                )}
                <Space size={4}>
                  <Button size="small" onClick={() => addUiConditionClause(placeholderIndex, cluster.id)}>
                    Add Condition
                  </Button>
                  <Button
                    size="small"
                    danger
                    disabled={conditionClusters.length <= 1}
                    onClick={() => removeUiConditionCluster(placeholderIndex, cluster.id)}
                  >
                    Delete Group
                  </Button>
                </Space>
              </div>
              {clauses.map((clause, clauseIndex) => (
                <div
                  key={`cond_clause_${placeholderIndex}_${clause.id}`}
                  style={{
                    border: '1px dashed var(--app-border-strong)',
                    borderRadius: 6,
                    padding: 6,
                  }}
                >
                  <div style={{ display: 'grid', gridTemplateColumns: '74px minmax(140px, 1fr) 100px 108px minmax(140px, 1fr) auto', gap: 6, alignItems: 'center' }}>
                    {clauseIndex > 0 ? (
                      <Select
                        size="small"
                        value={clause.joinWithPrev}
                        options={[
                          { value: 'and', label: 'and' },
                          { value: 'or', label: 'or' },
                        ]}
                        onChange={(value) => updateUiConditionClause(placeholderIndex, clause.id, {
                          joinWithPrev: value === 'or' ? 'or' : 'and',
                        })}
                        style={{ width: '100%' }}
                      />
                    ) : (
                      <Text style={{ color: 'var(--app-text-subtle)', fontSize: 11 }}>
                        If
                      </Text>
                    )}
                    <AutoComplete
                      value={clause.leftField}
                      options={uiFieldAutoCompleteOptions}
                      onChange={(value) => updateUiConditionClause(placeholderIndex, clause.id, {
                        leftField: String(value || ''),
                      })}
                      filterOption={(inputValue, option) => String(option?.value || '').toLowerCase().includes(String(inputValue || '').toLowerCase())}
                    >
                      <Input
                        size="small"
                        placeholder={String(clause.operator || '').toLowerCase() === 'raw' ? 'left field (optional)' : 'left field'}
                        disabled={String(clause.operator || '').toLowerCase() === 'raw'}
                      />
                    </AutoComplete>
                    <Select
                      size="small"
                      value={clause.operator}
                      options={[
                        ...conditionOperatorOptions.map((value) => ({ value, label: value })),
                        { value: 'raw', label: 'raw' },
                      ]}
                      onChange={(value) => {
                        const operator = String(value || '==')
                        updateUiConditionClause(placeholderIndex, clause.id, {
                          operator,
                          rightMode: operator.toLowerCase() === 'raw' ? 'raw' : (
                            clause.rightMode === 'raw' ? 'literal' : clause.rightMode
                          ),
                        })
                      }}
                      style={{ width: '100%' }}
                    />
                    <Select
                      size="small"
                      value={clause.rightMode}
                      options={[
                        { value: 'literal', label: 'Literal' },
                        { value: 'field', label: 'Field' },
                        { value: 'values', label: 'Values' },
                        { value: 'variable', label: 'Variable' },
                        { value: 'raw', label: 'Raw' },
                      ]}
                      onChange={(value) => updateUiConditionClause(placeholderIndex, clause.id, {
                        rightMode: value as UiConditionClauseRightMode,
                      })}
                      disabled={String(clause.operator || '').toLowerCase() === 'raw'}
                      style={{ width: '100%' }}
                    />
                    {String(clause.operator || '').toLowerCase() === 'raw' || clause.rightMode === 'raw' ? (
                      <Input
                        size="small"
                        value={clause.rightValue}
                        placeholder="raw clause expression"
                        onChange={(event) => updateUiConditionClause(placeholderIndex, clause.id, {
                          rightValue: event.target.value,
                        })}
                      />
                    ) : clause.rightMode === 'field' || clause.rightMode === 'values' ? (
                      <AutoComplete
                        value={clause.rightValue}
                        options={uiFieldAutoCompleteOptions}
                        onChange={(value) => updateUiConditionClause(placeholderIndex, clause.id, {
                          rightValue: String(value || ''),
                        })}
                        filterOption={(inputValue, option) => String(option?.value || '').toLowerCase().includes(String(inputValue || '').toLowerCase())}
                      >
                        <Input size="small" placeholder="right value" />
                      </AutoComplete>
                    ) : clause.rightMode === 'variable' ? (
                      <Select
                        size="small"
                        showSearch
                        value={clause.rightValue || undefined}
                        options={uiExpressionVariableOptions}
                        onChange={(value) => updateUiConditionClause(placeholderIndex, clause.id, {
                          rightValue: String(value || ''),
                        })}
                        placeholder="variable"
                        style={{ width: '100%' }}
                        optionFilterProp="label"
                      />
                    ) : (
                      <Input
                        size="small"
                        value={clause.rightValue}
                        placeholder="literal value"
                        onChange={(event) => updateUiConditionClause(placeholderIndex, clause.id, {
                          rightValue: event.target.value,
                        })}
                      />
                    )}
                    <Button
                      size="small"
                      danger
                      disabled={conditionState.clauses.length <= 1}
                      onClick={() => removeUiConditionClause(placeholderIndex, clause.id)}
                    >
                      Delete
                    </Button>
                  </div>
                </div>
              ))}
            </Space>
          </div>
        ))}
        <Space style={{ justifyContent: 'flex-end', width: '100%' }}>
          <Button size="small" onClick={onClose}>
            Close
          </Button>
          <Button
            size="small"
            type="primary"
            onClick={() => {
              applyUiConditionBuilderToPlaceholder(placeholderIndex)
              onClose()
            }}
          >
            Apply Grouped Condition
          </Button>
        </Space>
      </Space>
    </Modal>
  )
}
