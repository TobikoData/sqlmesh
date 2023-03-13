import { syntaxTree } from '@codemirror/language'
import { Extension } from '@codemirror/state'
import {
  ViewPlugin,
  DecorationSet,
  Decoration,
  ViewUpdate,
  EditorView,
  Tooltip,
  hoverTooltip,
} from '@codemirror/view'
import { ModelsModels } from '~/api/client'
import { ModelFile } from '~/models'
import { isNil } from '~/utils'

import { useSqlMeshExtention } from './SqlMeshDialect'

export { useSqlMeshExtention }

export function SqlMeshModel(models: ModelsModels): Extension {
  return ViewPlugin.fromClass(
    class SqlMeshModelView {
      decorations: DecorationSet = Decoration.set([])
      update(viewUpdate: ViewUpdate): void {
        if (viewUpdate.docChanged || viewUpdate.viewportChanged) {
          const decorations: any[] = []

          for (const range of viewUpdate.view.visibleRanges) {
            syntaxTree(viewUpdate.view.state).iterate({
              from: range.from,
              to: range.to,
              enter({ from, to }) {
                const model = viewUpdate.view.state.doc.sliceString(from, to)

                if (isNil(models[model])) return true

                const decoration = Decoration.mark({
                  attributes: {
                    class: 'sqlmesh-model',
                    model,
                  },
                }).range(from, to)

                decorations.push(decoration)
              },
            })
          }

          this.decorations = Decoration.set(decorations)
        }
      }
    },
    {
      decorations: value => value.decorations,
    },
  )
}

export function events(
  models: ModelsModels,
  files: Map<ID, ModelFile>,
  selectFile: (file: ModelFile) => void,
): Extension {
  return EditorView.domEventHandlers({
    click(event: MouseEvent) {
      handleClickOnSqlMeshModel(event, models, files, selectFile)
    },
  })
}

export function HoverTooltip(models: ModelsModels): Extension {
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
      const model = models[modelName]

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

function handleClickOnSqlMeshModel(
  event: MouseEvent,
  models: ModelsModels,
  files: Map<ID, ModelFile>,
  selectFile: (file: ModelFile) => void,
): void {
  if (event.target == null) return

  const el = event.target as HTMLElement
  const modelName = el.getAttribute('model')

  if (modelName == null) return

  const model = models[modelName]

  if (model == null) return

  const id = model.path as ID
  const file = files.get(id)

  if (file != null) {
    selectFile(file)
  }
}
