import { type Extension } from '@codemirror/state'
import {
  ViewPlugin,
  type DecorationSet,
  Decoration,
  type EditorView,
  type ViewUpdate,
} from '@codemirror/view'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { getDecorations } from './help'

export default function SQLMeshModel(
  models: Map<string, ModelSQLMeshModel>,
  columns: Set<string>,
  isActionMode: boolean,
  model?: ModelSQLMeshModel,
): Extension {
  return ViewPlugin.fromClass(
    class SQLMeshModelView {
      decorations: DecorationSet = Decoration.set([])
      constructor(readonly view: EditorView) {
        this.decorations = getDecorations(
          models,
          view,
          columns,
          isActionMode,
          model,
        )
      }

      update(viewUpdate: ViewUpdate): void {
        this.decorations = getDecorations(
          models,
          viewUpdate.view,
          columns,
          isActionMode,
          model,
        )
      }
    },
    {
      decorations: value => value.decorations,
    },
  )
}
