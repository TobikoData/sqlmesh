import { type LineageColumn } from '@/api/client'
import type { ModelEncodedFQN, ModelName } from '@/domain/models'

export interface Lineage {
  models: ModelEncodedFQN[]
  columns?: Record<ModelName, LineageColumn>
}
