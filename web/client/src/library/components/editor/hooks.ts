import { type Extension } from '@codemirror/state'
import { type KeyBinding } from '@codemirror/view'
import { useLineageFlow } from '@components/graph/context'
import { useStoreEditor } from '@context/editor'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { type Column } from '~/api/client'
import { isFalse, isNil, isNotNil, isObjectEmpty, isTrue } from '@utils/index'
import { useCallback, useMemo, useState } from 'react'
import { events, HoverTooltip, SQLMeshModel } from './extensions'
import { findModel, findColumn } from './extensions/help'
import { useStoreProject } from '@context/project'
import { useStoreContext } from '@context/context'

export { useDefaultKeymapsEditorTab, useSQLMeshModelExtensions }

function useDefaultKeymapsEditorTab(): KeyBinding[] {
  const addConfirmation = useStoreContext(s => s.addConfirmation)

  const tab = useStoreEditor(s => s.tab)
  const selectTab = useStoreEditor(s => s.selectTab)
  const closeTab = useStoreEditor(s => s.closeTab)
  const createTab = useStoreEditor(s => s.createTab)
  const addTab = useStoreEditor(s => s.addTab)

  const file = tab?.file

  const closeEditorTabWithConfirmation = useCallback(
    function closeEditorTabWithConfirmation(): void {
      if (isNil(file)) return

      if (isTrue(file.isChanged)) {
        addConfirmation({
          headline: 'Closing Tab',
          description:
            'All unsaved changes will be lost. Do you want to close the tab anyway?',
          yesText: 'Yes, Close Tab',
          noText: 'No, Cancel',
          action: () => {
            closeTab(file)
          },
        })
      } else {
        closeTab(file)
      }
    },
    [file],
  )

  const keymaps = useMemo(() => {
    return isNil(file)
      ? []
      : [
          {
            key: 'Shift-Mod-.',
            preventDefault: true,
            run() {
              const newTab = createTab()

              addTab(newTab)
              selectTab(newTab)

              return true
            },
          },
          {
            key: 'Shift-Mod-,',
            preventDefault: true,
            run() {
              closeEditorTabWithConfirmation()

              return true
            },
          },
        ]
  }, [file, selectTab, closeTab, createTab, addTab])

  return keymaps
}

function useSQLMeshModelExtensions(
  path?: string,
  handleModelClick?: (model: ModelSQLMeshModel) => void,
  handleModelColumn?: (model: ModelSQLMeshModel, column: Column) => void,
): Extension[] {
  const { lineage } = useLineageFlow()

  const models = useStoreContext(s => s.models)

  const files = useStoreProject(s => s.files)

  const model = isNil(path) ? undefined : models.get(path)

  const [isActionMode, setIsActionMode] = useState(false)

  const extensions = useMemo(() => {
    const columns = new Set(
      isNil(lineage) || isObjectEmpty(lineage)
        ? Array.from(new Set(models.values())).flatMap(m =>
            m.columns.map(c => c.name),
          )
        : (Object.keys(lineage)
            .flatMap(
              modelName => models.get(modelName)?.columns.map(c => c.name),
            )
            .filter(Boolean) as string[]),
    )

    function handleEventModelClick(event: MouseEvent): void {
      if (event.ctrlKey) {
        const model = findModel(event, models)

        if (isNil(model)) return

        handleModelClick?.(model)
      }
    }

    function handleEventlColumnClick(event: MouseEvent): void {
      if (event.ctrlKey) {
        if (isNil(model)) return

        const column = findColumn(event, model)

        if (isNil(column)) return

        handleModelColumn?.(model, column)
      }
    }

    return isNil(model)
      ? []
      : ([
          models.size > 0 && isActionMode && HoverTooltip(models),
          events({
            keydown: e => {
              if (e.ctrlKey) {
                setIsActionMode(true)
              }
            },
            keyup: e => {
              if (isFalse(e.ctrlKey)) {
                setIsActionMode(false)
              }
            },
          }),
          isNotNil(handleModelClick) &&
            events({ click: handleEventModelClick }),
          isNotNil(handleModelColumn) &&
            events({ click: handleEventlColumnClick }),
          SQLMeshModel(models, columns, isActionMode, model),
        ].filter(Boolean) as Extension[])
  }, [handleModelClick, handleModelColumn, model, models, files, isActionMode])

  return extensions
}
