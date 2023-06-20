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

export { HoverTooltip, events, useSqlMeshExtension, SqlMeshModel }

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
