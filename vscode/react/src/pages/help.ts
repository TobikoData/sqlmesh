import type { ModelType } from '@/api/client'

type NodeType = ModelType

export const NODE_TYPE_COLOR_VAR: Record<NodeType, string> = {
  sql: 'var(--color-lineage-node-type-background-sql)',
  python: 'var(--color-lineage-node-type-background-python)',
  source: 'var(--color-lineage-node-type-background-source)',
  seed: 'var(--color-lineage-node-type-background-source)',
  external: 'var(--color-lineage-node-type-background-source)',
}
export const NODE_TYPE_COLOR: Record<NodeType, string> = {
  sql: 'bg-lineage-node-type-background-sql',
  python: 'bg-lineage-node-type-background-python',
  source: 'bg-lineage-node-type-background-source',
  seed: 'bg-lineage-node-type-background-source',
  external: 'bg-lineage-node-type-background-source',
}
export const NODE_TYPE_TEXT_COLOR: Record<NodeType, string> = {
  sql: 'text-lineage-node-type-foreground-sql',
  python: 'text-lineage-node-type-foreground-python',
  source: 'text-lineage-node-type-foreground-source',
  seed: 'text-lineage-node-type-foreground-source',
  external: 'text-lineage-node-type-foreground-source',
}
export const NODE_TYPE_BORDER_COLOR: Record<NodeType, string> = {
  sql: 'border-lineage-node-type-border-sql',
  python: 'border-lineage-node-type-border-python',
  source: 'border-lineage-node-type-border-source',
  seed: 'border-lineage-node-type-border-source',
  external: 'border-lineage-node-type-border-source',
}
