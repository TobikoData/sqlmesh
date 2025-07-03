import { type Column as APIColumn } from '@/api/client'
import { type Branded } from '@bus/brand'

export type ColumnName = Branded<string, 'ColumnName'>

export type Column = {
  name: ColumnName
  type: string
  description?: string
}

export function fromAPIColumn(column: APIColumn): Column {
  return {
    name: column.name as ColumnName,
    type: column.type,
    description: column.description ?? undefined,
  }
}
