import { type NodeType } from './ModelLineageContext'

export function getNodeTypeColorVar(nodeType: NodeType) {
  return {
    sql: 'var(--color-lineage-node-type-background-sql)',
    python: 'var(--color-lineage-node-type-background-python)',
    'cte/subquery': 'var(--color-lineage-node-type-background-cte-subquery)',
    source: 'var(--color-lineage-node-type-background-source)',
  }[nodeType]
}

export function getNodeTypeColor(nodeType: NodeType) {
  return {
    sql: 'bg-lineage-node-type-background-sql',
    python: 'bg-lineage-node-type-background-python',
    'cte/subquery': 'bg-lineage-node-type-background-cte-subquery',
    source: 'bg-lineage-node-type-background-source',
  }[nodeType]
}

export function getNodeTypeTextColor(nodeType: NodeType) {
  return {
    sql: 'text-lineage-node-type-foreground-sql',
    python: 'text-lineage-node-type-foreground-python',
    'cte/subquery': 'text-lineage-node-type-foreground-cte-subquery',
    source: 'text-lineage-node-type-foreground-source',
  }[nodeType]
}

export function getNodeTypeBorderColor(nodeType: NodeType) {
  return {
    sql: 'border-lineage-node-type-border-sql',
    python: 'border-lineage-node-type-border-python',
    'cte/subquery': 'border-lineage-node-type-border-cte-subquery',
    source: 'border-lineage-node-type-border-source',
  }[nodeType]
}
