import { type Extension } from '@codemirror/state'
import { type KeyBinding } from '@codemirror/view'
import { useLineageFlow } from '@components/graph/context'
import { useStoreEditor } from '@context/editor'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { type Column } from '~/api/client'
import { isFalse, isNil, isNotNil } from '@utils/index'
import { useMemo, useState } from 'react'
import { events, HoverTooltip, SQLMeshModel } from './extensions'
import { findModel, findColumn } from './extensions/help'
import { useStoreProject } from '@context/project'
import {
  type ExtensionSQLMeshDialect,
  SQLMeshDialect,
  SQLMeshDialectCleanUp,
} from './extensions/SQLMeshDialect'

export {
  useDefaultKeymapsEditorTab,
  useSQLMeshModelExtensions,
  useSQLMeshDialect,
}

function useDefaultKeymapsEditorTab(): KeyBinding[] {
  const tab = useStoreEditor(s => s.tab)

  if (isNil(tab)) return []

  const selectTab = useStoreEditor(s => s.selectTab)
  const createTab = useStoreEditor(s => s.createTab)
  const closeTab = useStoreEditor(s => s.closeTab)
  const addTab = useStoreEditor(s => s.addTab)

  return [
    {
      key: 'Mod-Alt-[',
      preventDefault: true,
      run() {
        const newTab = createTab()

        addTab(newTab)
        selectTab(newTab)

        return true
      },
    },
    {
      key: 'Mod-Alt-]',
      preventDefault: true,
      run() {
        closeTab(tab.file)

        return true
      },
    },
  ]
}

function useSQLMeshModelExtensions(
  path?: string,
  handleModelClick?: (model: ModelSQLMeshModel) => void,
  handleModelColumn?: (model: ModelSQLMeshModel, column: Column) => void,
): Extension[] {
  const { models, lineage } = useLineageFlow()
  const files = useStoreProject(s => s.files)
  const model = path == null ? undefined : models.get(path)

  const [isActionMode, setIsActionMode] = useState(false)

  const extensions = useMemo(() => {
    const columns =
      lineage == null
        ? new Set<string>()
        : new Set(
            Object.keys(lineage)
              .map(modelName => models.get(modelName)?.columns.map(c => c.name))
              .flat()
              .filter(Boolean) as string[],
          )

    function handleEventModelClick(event: MouseEvent): void {
      if (event.metaKey) {
        const model = findModel(event, models)

        if (isNil(model)) return

        handleModelClick?.(model)
      }
    }

    function handleEventlColumnClick(event: MouseEvent): void {
      if (event.metaKey) {
        if (isNil(model)) return

        const column = findColumn(event, model)

        if (isNil(column)) return

        handleModelColumn?.(model, column)
      }
    }

    return [
      models.size > 0 && isActionMode && HoverTooltip(models),
      events({
        keydown: e => {
          if (e.metaKey) {
            setIsActionMode(true)
          }
        },
        keyup: e => {
          if (isFalse(e.metaKey)) {
            setIsActionMode(false)
          }
        },
      }),
      isNotNil(handleModelClick) && events({ click: handleEventModelClick }),
      isNotNil(handleModelColumn) && events({ click: handleEventlColumnClick }),
      isNotNil(model) && SQLMeshModel(models, model, columns, isActionMode),
    ].filter(Boolean) as Extension[]
  }, [model, models, files, handleModelClick, handleModelColumn, isActionMode])

  return extensions
}

function useSQLMeshDialect(): [ExtensionSQLMeshDialect, Callback] {
  return [SQLMeshDialect, SQLMeshDialectCleanUp]
}
