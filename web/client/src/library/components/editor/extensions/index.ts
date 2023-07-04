import { RangeSetBuilder, type Extension } from '@codemirror/state'
import {
  EditorView,
  type Tooltip,
  hoverTooltip,
  Decoration,
  type DecorationSet,
  ViewPlugin,
  type ViewUpdate,
} from '@codemirror/view'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import SQLMeshModel from './SQLMeshModel'

export { events, HoverTooltip, SQLMeshModel, SQLMeshExpression }

function events(
  events: Record<string, (event: MouseEvent) => void>,
): Extension {
  return EditorView.domEventHandlers(events)
}

function HoverTooltip(models: Map<string, ModelSQLMeshModel>): Extension {
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

function SQLMeshExpression(expression: string): Extension {
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
