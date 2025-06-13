import {
  type Column,
  type ModelDetails,
  type Model,
  type ModelDescription,
  type ModelSql,
  ModelType,
  type ModelDefaultCatalog,
  type ModelDefinition,
} from '@/api/client'
import type { ModelFQN, ModelName, ModelPath } from '@/types/models'
import { isArrayNotEmpty } from '@/utils/index'
import { ModelInitial } from './initial'
import type { Lineage } from './lineage'

export interface InitialSQLMeshModel
  extends Omit<Model, 'name' | 'fqn' | 'path' | 'full_path'> {
  name: ModelName
  fqn: ModelFQN
  path: ModelPath
  full_path: ModelPath
  lineage?: Record<ModelName, Lineage>
}

export class ModelSQLMeshModel<
  T extends InitialSQLMeshModel = InitialSQLMeshModel,
> extends ModelInitial<T> {
  _details: ModelDetails = {}
  _detailsIndex: string = ''

  name: ModelName
  fqn: ModelFQN
  path: ModelPath
  full_path: ModelPath
  dialect: string
  type: ModelType
  columns: Column[]
  default_catalog?: ModelDefaultCatalog
  description?: ModelDescription
  sql?: ModelSql
  definition?: ModelDefinition
  hash: string

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

    this.name = encodeURI(this.initial.name) as ModelName
    this.fqn = encodeURI(this.initial.fqn) as ModelFQN
    this.default_catalog = this.initial.default_catalog
    this.path = this.initial.path as ModelPath
    this.full_path = this.initial.full_path as ModelPath
    this.dialect = this.initial.dialect
    this.description = this.initial.description
    this.sql = this.initial.sql
    this.definition = this.initial.definition
    this.columns = this.initial.columns ?? []
    this.type = this.initial.type
    this.hash = this.initial.hash
    this.details = this.initial.details ?? {}
  }

  get defaultCatalog(): ModelDefaultCatalog | undefined {
    return this.default_catalog
  }

  get details(): ModelDetails {
    return this._details
  }

  set details(details: ModelDetails) {
    const output = []

    for (const value of Object.values(details)) {
      if (isArrayNotEmpty(value)) {
        value.forEach(v => {
          output.push(...Object.values(v))
        })
      } else {
        output.push(value)
      }
    }

    this._details = details
    this._detailsIndex = output.join(' ')
  }

  get index(): string {
    return [
      this.displayName,
      this.path,
      this.type,
      ...this.columns.map(column => Object.values(column)).flat(),
      this._detailsIndex,
      this.dialect,
      this.description,
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase()
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
        this.name = encodeURI(value as string) as ModelName
      } else if (key === 'fqn') {
        this.fqn = encodeURI(value as string) as ModelFQN
      } else if (key === 'type') {
        this.type = value as ModelType
      } else if (key === 'default_catalog') {
        this.default_catalog = value as ModelDefaultCatalog
      } else if (key === 'description') {
        this.description = value as ModelDescription
      } else if (key in this) {
        if (key === 'path' || key === 'full_path') {
          ;(this as any)[key] = value as ModelPath
        } else {
          ;(this as any)[key] = value as string
        }
      }
    }
  }
}
