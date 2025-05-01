import { type LineageColumn } from '@/api/client'

export interface Lineage {
  models: string[]
  columns?: Record<string, LineageColumn>
}
