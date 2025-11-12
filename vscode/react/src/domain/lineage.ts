import {
  type LineageColumn as ApiLineageColumn,
  type ModelLineageApiLineageModelNameGet200,
} from '@/api/client'
import type { ModelEncodedFQN, ModelFQN } from '@/domain/models'
import type { ColumnName } from './column'

export interface Lineage {
  models: ModelEncodedFQN[]
  columns?: Record<ColumnName, LineageColumn>
}

export interface LineageColumn {
  source?: string
  expression?: string
  models: {
    [key: ModelEncodedFQN]: ColumnName[]
  }
}

export const toLineageColumn = (column: ApiLineageColumn): LineageColumn => {
  return {
    source: column.source ?? undefined,
    expression: column.expression ?? undefined,
    models: column.models as Record<ModelEncodedFQN, ColumnName[]>,
  }
}

export interface ModelLineage {
  [key: ModelFQN]: ModelFQN[]
}

export const toModelLineage = (
  lineage: ModelLineageApiLineageModelNameGet200,
): ModelLineage => {
  return lineage as Record<ModelFQN, ModelFQN[]>
}
