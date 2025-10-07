import type { ModelType } from '@/api/client'

type NodeType = ModelType | 'cte/subquery'

export const NODE_TYPE_COLOR_VAR: Record<NodeType, string> = {
  sql: 'var(--color-lineage-node-type-background-sql)',
  python: 'var(--color-lineage-node-type-background-python)',
  'cte/subquery': 'var(--color-lineage-node-type-background-cte-subquery)',
  source: 'var(--color-lineage-node-type-background-source)',
  seed: 'var(--color-lineage-node-type-background-source)',
  external: 'var(--color-lineage-node-type-background-source)',
}
export const NODE_TYPE_COLOR: Record<NodeType, string> = {
  sql: 'bg-lineage-node-type-background-sql',
  python: 'bg-lineage-node-type-background-python',
  'cte/subquery': 'bg-lineage-node-type-background-cte-subquery',
  source: 'bg-lineage-node-type-background-source',
  seed: 'bg-lineage-node-type-background-source',
  external: 'bg-lineage-node-type-background-source',
}
export const NODE_TYPE_TEXT_COLOR: Record<NodeType, string> = {
  sql: 'text-lineage-node-type-foreground-sql',
  python: 'text-lineage-node-type-foreground-python',
  'cte/subquery': 'text-lineage-node-type-foreground-cte-subquery',
  source: 'text-lineage-node-type-foreground-source',
  seed: 'text-lineage-node-type-foreground-source',
  external: 'text-lineage-node-type-foreground-source',
}
export const NODE_TYPE_BORDER_COLOR: Record<NodeType, string> = {
  sql: 'border-lineage-node-type-border-sql',
  python: 'border-lineage-node-type-border-python',
  'cte/subquery': 'border-lineage-node-type-border-cte-subquery',
  source: 'border-lineage-node-type-border-source',
  seed: 'border-lineage-node-type-border-source',
  external: 'border-lineage-node-type-border-source',
}
