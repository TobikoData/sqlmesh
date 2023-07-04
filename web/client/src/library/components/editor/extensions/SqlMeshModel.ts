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
  model: ModelSQLMeshModel,
  columns: Set<string>,
  isActionMode: boolean,
): Extension {
  return ViewPlugin.fromClass(
    class SQLMeshModelView {
      decorations: DecorationSet = Decoration.set([])
      constructor(readonly view: EditorView) {
        this.decorations = getDecorations(
          models,
          view,
          model,
          columns,
          isActionMode,
        )
      }

      update(viewUpdate: ViewUpdate): void {
        this.decorations = getDecorations(
          models,
          viewUpdate.view,
          model,
          columns,
          isActionMode,
        )
      }
    },
    {
      decorations: value => value.decorations,
    },
  )
}
