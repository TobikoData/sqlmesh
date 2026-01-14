import { type NodeType } from './ModelLineageContext'

export function getNodeTypeColorVar(nodeType: NodeType) {
  return {
    sql: 'var(--color-lineage-node-type-background-sql)',
    python: 'var(--color-lineage-node-type-background-python)',
  }[nodeType]
}

export function getNodeTypeColor(nodeType: NodeType) {
  return {
    sql: 'bg-lineage-node-type-background-sql',
    python: 'bg-lineage-node-type-background-python',
  }[nodeType]
}

export function getNodeTypeTextColor(nodeType: NodeType) {
  return {
    sql: 'text-lineage-node-type-foreground-sql',
    python: 'text-lineage-node-type-foreground-python',
  }[nodeType]
}

export function getNodeTypeBorderColor(nodeType: NodeType) {
  return {
    sql: 'border-lineage-node-type-border-sql',
    python: 'border-lineage-node-type-border-python',
  }[nodeType]
}
