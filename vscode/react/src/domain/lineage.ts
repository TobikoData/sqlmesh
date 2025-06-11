import { type LineageColumn } from '@/api/client'
import type { ModelFQN, ModelName } from '@/types/models'

export interface Lineage {
  models: ModelFQN[]
  columns?: Record<ModelName, LineageColumn>
}
