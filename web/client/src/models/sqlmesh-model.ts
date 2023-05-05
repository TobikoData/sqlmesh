import {
  type Column,
  type ModelDetails,
  type Model,
  type ColumnLineageApiLineageModelNameColumnNameGet200,
} from '@api/client'
import { type Lineage } from '@context/editor'
import { ModelInitial } from './initial'
import { isObjectEmpty } from '@utils/index'

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
  lineage: Record<string, Lineage>
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
            lineage: initial?.lineage ?? {},
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
    this.lineage = this.initial.lineage ?? {}
  }

  update(initial: Partial<InitialSQLMeshModel> = {}): void {
    for (const [key, value] of Object.entries(initial)) {
      if (key === 'columns') {
        this.columns = value as Column[]
      } else if (key === 'details') {
        this.details = value as ModelDetails
      } else if (key === 'lineage') {
        this.lineage = value as Record<string, Lineage>
      } else if (key in this) {
        this[key as 'path' | 'name' | 'dialect' | 'description' | 'sql'] =
          value as string
      }
    }
  }

  // TODO: use better merge
  static mergeLineage(
    models: Map<string, Model>,
    lineage: Record<string, Lineage>,
    columns: ColumnLineageApiLineageModelNameColumnNameGet200 = {},
  ): Record<string, Lineage> {
    lineage = structuredClone(lineage)

    for (const model in columns) {
      const lineageModel = lineage[model]
      const columnsModel = columns[model]

      if (lineageModel == null || columnsModel == null) continue

      if (lineageModel.columns == null) {
        lineageModel.columns = {}
      }

      for (const columnName in columnsModel) {
        const columnsModelColumn = columnsModel[columnName]

        if (columnsModelColumn == null) continue

        const lineageModelColumn = lineageModel.columns[columnName] ?? {}

        lineageModelColumn.source = columnsModelColumn.source
        lineageModelColumn.models = {}

        lineageModel.columns[columnName] = lineageModelColumn

        if (isObjectEmpty(columnsModelColumn.models)) continue

        for (const columnModel in columnsModelColumn.models) {
          const columnsModelColumnModel = columnsModelColumn.models[columnModel]

          if (columnsModelColumnModel == null) continue

          const lineageModelColumnModel = lineageModelColumn.models[columnModel]

          if (lineageModelColumnModel == null) {
            lineageModelColumn.models[columnModel] = columnsModelColumnModel
          } else {
            lineageModelColumn.models[columnModel] = Array.from(
              new Set(lineageModelColumnModel.concat(columnsModelColumnModel)),
            )
          }
        }
      }
    }

    for (const modelName in lineage) {
      const model = models.get(modelName)
      const modelLineage = lineage[modelName]

      if (model == null || modelLineage == null) {
        delete lineage[modelName]

        continue
      }

      if (modelLineage.columns == null) continue

      if (model.columns == null) {
        delete modelLineage.columns

        continue
      }

      for (const columnName in modelLineage.columns) {
        const found = model.columns.find(c => c.name === columnName)

        if (found == null) {
          delete modelLineage.columns[columnName]

          continue
        }
      }
    }

    return lineage
  }

  static encodeName(modelName: string): string {
    return modelName.replaceAll('.', REPLACE_DOT_SYMBOL)
  }

  static decodeName(modelName: string): string {
    return modelName.replaceAll(REPLACE_DOT_SYMBOL, '.')
  }
}
