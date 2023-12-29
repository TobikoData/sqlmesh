import {
  type Column,
  type ModelDetails,
  type Model,
  type ModelDescription,
  type ModelSql,
  ModelType,
  type ModelDefaultCatalog,
} from '@api/client'
import { type Lineage } from '@context/editor'
import { ModelInitial } from './initial'

export interface InitialSQLMeshModel extends Model {
  lineage?: Record<string, Lineage>
}

export class ModelSQLMeshModel<
  T extends InitialSQLMeshModel = InitialSQLMeshModel,
> extends ModelInitial<T> {
  name: string
  fqn: string
  path: string
  dialect: string
  type: ModelType
  columns: Column[]
  details: ModelDetails
  default_catalog?: ModelDefaultCatalog
  description?: ModelDescription
  sql?: ModelSql

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

    this.name = encodeURI(this.initial.name)
    this.fqn = encodeURI(this.initial.fqn)
    this.default_catalog = this.initial.default_catalog
    this.path = this.initial.path
    this.dialect = this.initial.dialect
    this.description = this.initial.description
    this.sql = this.initial.sql
    this.columns = this.initial.columns ?? []
    this.details = this.initial.details ?? {}
    this.type = this.initial.type
  }

  get defaultCatalog(): Optional<ModelDefaultCatalog> {
    return this.default_catalog
  }

  get index(): string {
    return [
      this.path,
      this.displayName,
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

  get isModelPython(): boolean {
    return this.type === ModelType.python
  }

  get isModelSQL(): boolean {
    return this.type === ModelType.sql
  }

  get isModelSeed(): boolean {
    return this.type === ModelType.seed
  }

  get isModelExternal(): boolean {
    return this.type === ModelType.external
  }

  get displayName(): string {
    return decodeURI(this.name)
  }

  update(initial: Partial<InitialSQLMeshModel> = {}): void {
    for (const [key, value] of Object.entries(initial)) {
      if (key === 'columns') {
        this.columns = value as Column[]
      } else if (key === 'details') {
        this.details = value as ModelDetails
      } else if (key === 'name') {
        this.name = encodeURI(value as string)
      } else if (key === 'fqn') {
        this.fqn = encodeURI(value as string)
      } else if (key === 'type') {
        this.type = value as ModelType
      } else if (key === 'default_catalog') {
        this.default_catalog = value as ModelDefaultCatalog
      } else if (key === 'description') {
        this.description = value as ModelDescription
      } else if (key in this) {
        this[key as 'path' | 'dialect' | 'sql'] = value as string
      }
    }
  }
}
