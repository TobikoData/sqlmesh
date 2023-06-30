import { syntaxTree } from '@codemirror/language'
import {
  type EditorView,
  type DecorationSet,
  Decoration,
} from '@codemirror/view'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { isFalse } from '@utils/index'
import { type Column } from '@api/client'
import clsx from 'clsx'

export function findModel(
  event: MouseEvent,
  models: Map<string, ModelSQLMeshModel>,
): ModelSQLMeshModel | undefined {
  if (event.target == null) return

  const el = event.target as HTMLElement
  const modelName =
    el.getAttribute('model') ?? el.parentElement?.getAttribute('model')

  if (modelName == null) return

  return models.get(modelName)
}

export function findColumn(
  event: MouseEvent,
  model: ModelSQLMeshModel,
): Column | undefined {
  if (event.target == null) return

  const el = event.target as HTMLElement
  const columnName =
    el.getAttribute('column') ?? el.parentElement?.getAttribute('column')

  if (columnName == null) return

  return model.columns.find(c => c.name === columnName)
}

export function getDecorations(
  models: Map<string, ModelSQLMeshModel>,
  view: EditorView,
  model: ModelSQLMeshModel,
  columns: Set<string>,
  isActionMode: boolean,
): DecorationSet {
  const decorations: any = []
  const modelColumns = model.columns.map(c => c.name)

  for (const range of view.visibleRanges) {
    syntaxTree(view.state).iterate({
      from: range.from,
      to: range.to,
      enter({ from, to }) {
        // In case model name represented in qoutes
        // like in python files, we need to remove qoutes
        let maybeModelOrColumn = view.state.doc
          .sliceString(from - 1, to + 1)
          .replaceAll('"', '')
          .replaceAll("'", '')
        let isOriginal = false

        if (
          maybeModelOrColumn.startsWith('.') ||
          maybeModelOrColumn.endsWith(':')
        ) {
          isOriginal = true
        }

        maybeModelOrColumn = maybeModelOrColumn.slice(
          1,
          maybeModelOrColumn.length - 1,
        )

        if (
          isFalse(isOriginal) &&
          columns.has(maybeModelOrColumn) &&
          !modelColumns.includes(maybeModelOrColumn)
        ) {
          isOriginal = true
        }

        let decoration

        if (maybeModelOrColumn === model.name) {
          decoration = Decoration.mark({
            attributes: {
              class: clsx(
                'sqlmesh-model --is-active-model',
                isActionMode && '--is-action-mode',
              ),
              model: maybeModelOrColumn,
            },
          }).range(from, to)
        } else if (models.get(maybeModelOrColumn) != null) {
          decoration = Decoration.mark({
            attributes: {
              class: clsx('sqlmesh-model', isActionMode && '--is-action-mode'),
              model: maybeModelOrColumn,
            },
          }).range(from, to)
        } else if (modelColumns.includes(maybeModelOrColumn)) {
          decoration = Decoration.mark({
            attributes: {
              class: clsx(
                'sqlmesh-model__column --is-active-model',
                isOriginal ? '--is-original' : '--is-derived',
                isActionMode && '--is-action-mode',
              ),
              column: maybeModelOrColumn,
            },
          }).range(from, to)
        } else if (columns.has(maybeModelOrColumn)) {
          decoration = Decoration.mark({
            attributes: {
              class: clsx(
                'sqlmesh-model__column',
                isOriginal ? '--is-original' : '--is-derived',
                isActionMode && '--is-action-mode',
              ),
              column: maybeModelOrColumn,
            },
          }).range(from, to)
        }

        decoration != null && decorations.push(decoration)
      },
    })
  }

  return Decoration.set(decorations)
}
