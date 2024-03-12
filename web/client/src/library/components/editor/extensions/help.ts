import {
  type EditorView,
  type DecorationSet,
  Decoration,
} from '@codemirror/view'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { isFalse, isNil, isNotNil } from '@utils/index'
import { type Column } from '@api/client'
import clsx from 'clsx'

interface DecorationRange {
  from: number
  to: number
}
type MarkDecorationCallback = (
  options: DecorationRange & {
    name: string
    key: string
  },
) => void

export function findModel(
  event: MouseEvent,
  models: Map<string, ModelSQLMeshModel>,
): ModelSQLMeshModel | undefined {
  if (isNil(event.target)) return

  const el = event.target as HTMLElement
  const modelName =
    el.getAttribute('model') ?? el.parentElement?.getAttribute('model')

  if (isNil(modelName)) return

  return models.get(modelName)
}

export function findColumn(
  event: MouseEvent,
  model: ModelSQLMeshModel,
): Column | undefined {
  if (isNil(event.target)) return

  const el = event.target as HTMLElement
  const columnName =
    el.getAttribute('column') ?? el.parentElement?.getAttribute('column')

  if (isNil(columnName)) return

  return model.columns.find(c => c.name === columnName)
}

export function getDecorations(
  models: Map<string, ModelSQLMeshModel>,
  view: EditorView,
  columns: Set<string>,
  isActionMode: boolean,
  model?: ModelSQLMeshModel,
): DecorationSet {
  const ranges: any = []
  const columnNames = Array.from(columns)
  const allModels = Array.from(new Set(models.values()))
  const modelNames = allModels.map(m => m.name)
  const modelColumns = model?.columns.map(c => c.name.toLowerCase()) ?? []
  const validLeftCharColumn = new Set(['.', '(', '[', ' ', '\n'])
  const validRightCharColumn = new Set([':', ')', ',', ']', ' ', '\n'])

  for (const range of view.visibleRanges) {
    getMarkDecorations(
      modelNames,
      view.state.doc.sliceString(range.from, range.to),
      range,
      ({ from, to, name }) => {
        ranges.push(
          createMarkDecorationModel({
            model: name,
            isActionMode,
            isActiveModel: model?.name === name,
          }).range(from, to),
        )
      },
    )

    getMarkDecorations(
      isNil(model) ? columnNames : modelColumns,
      view.state.doc.sliceString(range.from, range.to),
      range,
      ({ from, to, key }) => {
        const column = view.state.doc.sliceString(from, to)
        const word = view.state.doc.sliceString(from - 1, to + 1)
        const leftChar = view.state.doc.sliceString(from - 1, from)
        const rightChar = view.state.doc.sliceString(to, to + 1)

        if (
          (isNotNil(leftChar) && isFalse(validLeftCharColumn.has(leftChar))) ||
          (isNotNil(rightChar) && isFalse(validRightCharColumn.has(rightChar)))
        )
          return

        ranges.push(
          createMarkDecorationColumn({
            column,
            isOriginalColumn: isOriginalColumn(word),
            isActiveModel: modelColumns.includes(key),
            isActionMode,
          }).range(from, to),
        )
      },
    )
  }

  return Decoration.set(
    ranges.sort((a: DecorationRange, b: DecorationRange) => a.from - b.from),
  )
}

function getMarkDecorations(
  list: string[],
  doc: string,
  range: DecorationRange,
  callback: MarkDecorationCallback,
): void {
  const visisted = new Set()

  for (const name of list) {
    const key = name.toLowerCase()

    if (visisted.has(key)) continue

    visisted.add(key)

    const regex = new RegExp(`\\b${name}\\b`, 'ig')
    const regex_normalized = new RegExp(
      `\\b${alternativeNameFormat(name)}\\b`,
      'ig',
    )
    let found

    while (isNotNil((found = regex_normalized.exec(doc) ?? regex.exec(doc)))) {
      const from = range.from + found.index
      const to = from + found[0].length
      const options = {
        from,
        to,
        name,
        key,
      }

      isNotNil(callback) && callback(options)
    }
  }
}

function createMarkDecorationModel({
  model,
  isActionMode,
  isActiveModel,
}: {
  model: string
  isActionMode: boolean
  isActiveModel: boolean
}): Decoration {
  return Decoration.mark({
    attributes: {
      class: clsx(
        'sqlmesh-model',
        isActiveModel && '--is-active-model',
        isActionMode && '--is-action-mode',
      ),
      model,
    },
  })
}

function createMarkDecorationColumn({
  column,
  isOriginalColumn,
  isActionMode,
  isActiveModel,
}: {
  column: string
  isOriginalColumn: boolean
  isActiveModel: boolean
  isActionMode: boolean
}): Decoration {
  return Decoration.mark({
    attributes: {
      class: clsx(
        'sqlmesh-model__column',
        isOriginalColumn ? '--is-original' : '--is-alias',
        isActiveModel && '--is-active-model',
        isActionMode && '--is-action-mode',
      ),
      column,
    },
  })
}

function isOriginalColumn(column: string): boolean {
  return (
    column.startsWith('.') ||
    column.endsWith(':') ||
    column.startsWith('(') ||
    column.endsWith(')')
  )
}

function alternativeNameFormat(modelName: string): string {
  return modelName.includes('"')
    ? modelName
    : modelName
        .split('.')
        .map(name => `"${name}"`)
        .join('.')
}
