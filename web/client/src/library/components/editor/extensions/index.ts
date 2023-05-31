import { syntaxTree } from '@codemirror/language'
import { RangeSetBuilder, type Extension } from '@codemirror/state'
import {
  ViewPlugin,
  type DecorationSet,
  Decoration,
  type ViewUpdate,
  EditorView,
  type Tooltip,
  hoverTooltip,
} from '@codemirror/view'
import { useSqlMeshExtension } from './SqlMeshDialect'
import { isFalse } from '@utils/index'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { type Column } from '@api/client'

export { useSqlMeshExtension }

export function SqlMeshModel(
  models: Map<string, ModelSQLMeshModel>,
  model: ModelSQLMeshModel,
  columns: Set<string>,
): Extension {
  return ViewPlugin.fromClass(
    class SqlMeshModelView {
      decorations: DecorationSet = Decoration.set([])
      constructor(readonly view: EditorView) {
        this.decorations = getDecorations(models, view, model, columns)
      }

      update(viewUpdate: ViewUpdate): void {
        this.decorations = getDecorations(
          models,
          viewUpdate.view,
          model,
          columns,
        )
      }
    },
    {
      decorations: value => value.decorations,
    },
  )
}

export function events(handler: (event: MouseEvent) => void): Extension {
  return EditorView.domEventHandlers({
    click(event: MouseEvent) {
      handler(event)
    },
  })
}

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

export function HoverTooltip(
  models: Map<string, ModelSQLMeshModel>,
): Extension {
  return hoverTooltip(
    (view: EditorView, pos: number, side: number): Tooltip | null => {
      const { from, to, text } = view.state.doc.lineAt(pos)

      let start = pos
      let end = pos

      while (start > from && /\w|\./.test(text[start - from - 1] as string))
        start--
      while (end < to && /\w|\./.test(text[end - from] as string)) end++

      if ((start === pos && side < 0) || (end === pos && side > 0)) return null

      const modelName = view.state.doc.sliceString(start, end)
      const model = models.get(modelName)

      if (model == null) return null

      return {
        pos: start,
        end,
        above: true,
        create() {
          const dom = document.createElement('div')
          const template = `
            <div class="flex items-center">
              <span>Model Name:</span>
              <span class="px-2 py-1 inline-block ml-1 bg-alternative-100 text-alternative-500 rounded">${model.name}</span>
            </div>
          `

          dom.className =
            'text-xs font-bold px-3 py-3 bg-white border-2 border-secondary-100 rounded outline-none shadow-lg mb-2'

          dom.innerHTML = template

          return { dom }
        },
      }
    },
    { hoverTime: 50 },
  )
}

export function SqlMeshExpression(expression: string): Extension {
  return ViewPlugin.fromClass(
    class SqlMeshModelView {
      decorations: DecorationSet = Decoration.set([])

      constructor(readonly view: EditorView) {
        this.decorations = markExpressionLine(expression, view)
      }

      update(viewUpdate: ViewUpdate): void {
        this.decorations = markExpressionLine(expression, viewUpdate.view)
      }
    },
    {
      decorations: value => value.decorations,
    },
  )
}

function markExpressionLine(
  expression: string,
  view: EditorView,
): DecorationSet {
  const mark = Decoration.line({
    attributes: {
      id: expression,
      class: 'sqlmesh-expression',
    },
  })

  const builder = new RangeSetBuilder<Decoration>()

  for (const { from, to } of view.visibleRanges) {
    for (let pos = from; pos <= to; ) {
      const line = view.state.doc.lineAt(pos)

      if (line.text.includes(expression)) {
        builder.add(line.from, line.from, mark)
      }

      pos = line.to + 1
    }
  }

  return builder.finish()
}

function getDecorations(
  models: Map<string, ModelSQLMeshModel>,
  view: EditorView,
  model: ModelSQLMeshModel,
  columns: Set<string>,
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
              class: 'sqlmesh-model --is-active-model',
              model: maybeModelOrColumn,
            },
          }).range(from, to)
        } else if (models.get(maybeModelOrColumn) != null) {
          decoration = Decoration.mark({
            attributes: {
              class: 'sqlmesh-model',
              model: maybeModelOrColumn,
            },
          }).range(from, to)
        } else if (modelColumns.includes(maybeModelOrColumn)) {
          decoration = Decoration.mark({
            attributes: {
              class: `sqlmesh-model__column --is-active-model ${
                isOriginal ? '--is-original' : ' --is-derived'
              }`,
              column: maybeModelOrColumn,
            },
          }).range(from, to)
        } else if (columns.has(maybeModelOrColumn)) {
          decoration = Decoration.mark({
            attributes: {
              class: `sqlmesh-model__column ${
                isOriginal ? '--is-original' : ' --is-derived'
              }`,
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
