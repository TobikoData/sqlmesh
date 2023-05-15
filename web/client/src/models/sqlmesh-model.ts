import { type Column, type ModelDetails, type Model } from '@api/client'
import { type Lineage } from '@context/editor'
import { ModelInitial } from './initial'

export interface InitialSQLMeshModel extends Model {
  lineage?: Record<string, Lineage>
}

const REPLACE_DOT_SYMBOL = '@'

export class ModelSQLMeshModel<
  T extends InitialSQLMeshModel = InitialSQLMeshModel,
> extends ModelInitial<T> {
  path: string
  name: string
  dialect: string
  type: string
  columns: Column[]
  details: ModelDetails
  description?: string
  sql?: string

  constructor(initial?: T | ModelSQLMeshModel) {
    super(
      (initial as ModelSQLMeshModel<T>)?.isModel
        ? (initial as ModelSQLMeshModel<T>).initial
        : {
            ...(initial as T),
            dialect: initial?.dialect ?? 'Default',
            columns: initial?.columns ?? [],
            details: initial?.details ?? {},
          },
    )

    this.path = this.initial.path
    this.name = this.initial.name
    this.dialect = this.initial.dialect
    this.type = this.initial.type
    this.description = this.initial.description
    this.sql = this.initial.sql
    this.columns = this.initial.columns ?? []
    this.details = this.initial.details ?? {}
  }

  get index(): string {
    return [
      this.path,
      this.name,
      this.dialect,
      this.type,
      this.description,
      this.columns.map(column => String(Object.values(column))),
      Object.values(this.details),
    ]
      .flat()
      .filter(Boolean)
      .join(' ')
      .toLocaleLowerCase()
  }

  update(initial: Partial<InitialSQLMeshModel> = {}): void {
    for (const [key, value] of Object.entries(initial)) {
      if (key === 'columns') {
        this.columns = value as Column[]
      } else if (key === 'details') {
        this.details = value as ModelDetails
      } else if (key in this) {
        this[key as 'path' | 'name' | 'dialect' | 'description' | 'sql'] =
          value as string
      }
    }
  }

  static encodeName(modelName: string): string {
    return modelName.replaceAll('.', REPLACE_DOT_SYMBOL)
  }

  static decodeName(modelName: string): string {
    return modelName.replaceAll(REPLACE_DOT_SYMBOL, '.')
  }
}
