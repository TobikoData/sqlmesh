import { syntaxTree } from '@codemirror/language'
import { type Extension } from '@codemirror/state'
import {
  ViewPlugin,
  type DecorationSet,
  Decoration,
  type ViewUpdate,
  EditorView,
  type Tooltip,
  hoverTooltip,
} from '@codemirror/view'
import { type Model } from '~/api/client'
import { type ModelFile } from '~/models'
import { isArrayEmpty, isNil } from '~/utils'

import { useSqlMeshExtension } from './SqlMeshDialect'

export { useSqlMeshExtension }

export function SqlMeshModel(models: Map<string, Model>): Extension {
  return ViewPlugin.fromClass(
    class SqlMeshModelView {
      decorations: DecorationSet = Decoration.set([])
      constructor(readonly view: EditorView) {
        this.decorations = getDecorations(models, view)
      }

      update(viewUpdate: ViewUpdate): void {
        if (viewUpdate.docChanged) {
          this.decorations = getDecorations(models, viewUpdate.view)
        }
      }
    },
    {
      decorations: value => value.decorations,
    },
  )
}

export function events(
  models: Map<string, Model>,
  files: Map<ID, ModelFile>,
  selectFile: (file: ModelFile) => void,
): Extension {
  return EditorView.domEventHandlers({
    click(event: MouseEvent) {
      handleClickOnSqlMeshModel(event, models, files, selectFile)
    },
  })
}

export function HoverTooltip(
  models: Map<string, Model>,
  graph: Record<string, { upstream: string[]; downstream: string[] }>,
  files: Map<ID, ModelFile>,
  selectFile: (file: ModelFile) => void,
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

          dom.addEventListener('click', handleClick)

          const template = `
            <div>
              <div class="flex items-center">
                <span>Model Name:</span>
                <span class="px-2 py-1 inline-block ml-1 bg-alternative-100 text-alternative-500 rounded text-secondary-500 dark:text-primary-500">${
                  model.name
                }</span>
              </div>
              ${getModelDependenciesListHTML(
                'Children Models',
                graph[model.name]?.upstream,
              )}
              ${getModelDependenciesListHTML(
                'Parent Models',
                graph[model.name]?.downstream,
              )}
            </div>
          `
          dom.className =
            'text-xs font-bold px-3 py-3 bg-white border-2 border-secondary-100 rounded outline-none shadow-lg mb-2'

          dom.innerHTML = template

          return {
            dom,
            destroy() {
              dom.removeEventListener('click', handleClick)
            },
          }

          function handleClick(event: MouseEvent): void {
            handleClickOnSqlMeshModel(event, models, files, selectFile)
          }
        },
      }
    },
    { hoverTime: 50 },
  )
}

function getDecorations(
  models: Map<string, Model>,
  view: EditorView,
): DecorationSet {
  const decorations: any = []

  for (const range of view.visibleRanges) {
    syntaxTree(view.state).iterate({
      from: range.from,
      to: range.to,
      enter({ from, to }) {
        // In case model name represented in qoutes
        // like in python files, we need to remove qoutes
        const model = view.state.doc
          .sliceString(from, to)
          .replaceAll('"', '')
          .replaceAll("'", '')

        if (isNil(models.get(model))) return true

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

  return Decoration.set(decorations)
}

function getModelDependenciesListHTML(
  title: string,
  dependencies?: string[],
): string {
  if (dependencies == null || isArrayEmpty(dependencies)) return ''

  const list = dependencies.map(
    modelName => `<li  class='px-2 mb-1'>
    <span model="${modelName}" class='text-secondary-500 dark:text-primary-500 inline-block pb-0.5 border-b cursor-pointer'>
      ${modelName}
    </span>
  </li>`,
  )

  return `
    <div class='my-2'>
      <h3>${title}</h3>
      <ul>
        ${list?.join('')}
      </ul>
    </div>
  `
}

function handleClickOnSqlMeshModel(
  event: MouseEvent,
  models: Map<string, Model>,
  files: Map<ID, ModelFile>,
  selectFile: (file: ModelFile) => void,
): void {
  if (event.target == null) return

  const el = event.target as HTMLElement
  const modelName =
    el.getAttribute('model') ?? el.parentElement?.getAttribute('model')

  if (modelName == null) return

  const model = models.get(modelName)

  if (model == null) return

  const id = model.path as ID
  const file = files.get(id)

  if (file != null) {
    selectFile(file)
  }
}
