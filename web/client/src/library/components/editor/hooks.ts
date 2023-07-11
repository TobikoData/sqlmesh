import { useApiFileByPath, useMutationApiSaveFile } from '@api/index'
import { type Extension } from '@codemirror/state'
import { type KeyBinding } from '@codemirror/view'
import { useLineageFlow } from '@components/graph/context'
import { useStoreEditor } from '@context/editor'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { useQueryClient, isCancelledError } from '@tanstack/react-query'
import { type Column, type File } from '~/api/client'
import {
  debounceAsync,
  debounceSync,
  isFalse,
  isNil,
  isNotNil,
  isStringEmptyOrNil,
} from '@utils/index'
import { useMemo, useCallback, useEffect, useState } from 'react'
import { events, HoverTooltip, SQLMeshModel } from './extensions'
import { dracula, tomorrow } from 'thememirror'
import { python } from '@codemirror/lang-python'
import { StreamLanguage } from '@codemirror/language'
import { yaml } from '@codemirror/legacy-modes/mode/yaml'
import { EnumColorScheme, useColorScheme } from '@context/theme'
import { type FileExtensions, EnumFileExtensions } from '@models/file'
import { findModel, findColumn } from './extensions/help'
import { useStoreProject } from '@context/project'
import {
  type ExtensionSQLMeshDialect,
  SQLMeshDialect,
  SQLMeshDialectCleanUp,
} from './extensions/SQLMeshDialect'

export {
  useDefaultExtensions,
  useDefaultKeymapsEditorTab,
  useSQLMeshModelExtensions,
  useKeymapsRemoteFile,
  useSQLMeshDialect,
}

function useDefaultExtensions(type: FileExtensions): Extension[] {
  const { mode } = useColorScheme()

  return useMemo(() => {
    return [
      mode === EnumColorScheme.Dark ? dracula : tomorrow,
      type === EnumFileExtensions.PY && python(),
      type === EnumFileExtensions.YAML && StreamLanguage.define(yaml),
      type === EnumFileExtensions.YML && StreamLanguage.define(yaml),
    ].filter(Boolean) as Extension[]
  }, [type, mode])
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

function useKeymapsRemoteFile(path: string): KeyBinding[] {
  const client = useQueryClient()

  const files = useStoreProject(s => s.files)
  const file = files.get(path)

  if (isNil(file)) return []

  const refreshTab = useStoreEditor(s => s.refreshTab)

  const { refetch: getFileContent } = useApiFileByPath(file.path)
  const debouncedGetFileContent = debounceAsync(getFileContent, 1000, true)

  const saveChangeSuccess = useCallback(
    function saveChangeSuccess(newFile: File): void {
      if (isNil(newFile)) return

      file.update(newFile)

      refreshTab()
    },
    [file.path],
  )

  const mutationSaveFile = useMutationApiSaveFile(client, {
    onSuccess: saveChangeSuccess,
  })

  const saveChange = useCallback(
    function saveChange(): void {
      mutationSaveFile.mutate({
        path: file.path,
        body: { content: file.content },
      })
    },
    [file.path],
  )

  const debouncedSaveChange = useCallback(
    debounceSync(saveChange, 1000, true),
    [file.path],
  )

  useEffect(() => {
    if (isStringEmptyOrNil(file.content)) {
      debouncedGetFileContent({
        throwOnError: true,
      })
        .then(({ data }) => {
          file.update(data)
        })
        .catch(error => {
          if (isCancelledError(error)) {
            console.log('getFileContent', 'Request aborted by React Query')
          } else {
            console.log('getFileContent', error)
          }
        })
        .finally(() => {
          refreshTab()
        })
    }
  }, [file.path])

  return [
    {
      mac: 'Cmd-s',
      win: 'Ctrl-s',
      linux: 'Ctrl-s',
      preventDefault: true,
      run() {
        debouncedSaveChange()

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
