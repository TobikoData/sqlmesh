import { useApiFileByPath, useMutationApiSaveFile } from '@api/index'
import { type Extension } from '@codemirror/state'
import { type KeyBinding } from '@codemirror/view'
import { useLineageFlow } from '@components/graph/context'
import { useStoreEditor } from '@context/editor'
import { useStoreFileTree } from '@context/fileTree'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { useQueryClient, isCancelledError } from '@tanstack/react-query'
import { type Column, type File } from '~/api/client'
import {
  debounceAsync,
  debounceSync,
  isFalse,
  isStringEmptyOrNil,
} from '@utils/index'
import { useMemo, useCallback, useEffect, useState } from 'react'
import { events, HoverTooltip, SqlMeshModel } from './extensions'
import { dracula, tomorrow } from 'thememirror'
import { python } from '@codemirror/lang-python'
import { StreamLanguage } from '@codemirror/language'
import { yaml } from '@codemirror/legacy-modes/mode/yaml'
import { EnumColorScheme, useColorScheme } from '@context/theme'
import { type FileExtensions, EnumFileExtensions } from '@models/file'
import { findModel, findColumn } from './extensions/help'

export function useDefaultExtensions(type: FileExtensions): Extension[] {
  const { mode } = useColorScheme()

  return useMemo(() => {
    return [
      mode === EnumColorScheme.Dark ? dracula : tomorrow,
      type === EnumFileExtensions.Python && python(),
      type === EnumFileExtensions.YAML && StreamLanguage.define(yaml),
      type === EnumFileExtensions.YML && StreamLanguage.define(yaml),
    ].filter(Boolean) as Extension[]
  }, [type, mode])
}

export function useSQLMeshModelExtensions(
  path?: string,
  handleModelClick?: (model: ModelSQLMeshModel) => void,
  handleModelColumn?: (model: ModelSQLMeshModel, column: Column) => void,
): Extension[] {
  const { models, lineage } = useLineageFlow()
  const files = useStoreFileTree(s => s.files)
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

        if (model == null) return

        handleModelClick?.(model)
      }
    }

    function handleEventlColumnClick(event: MouseEvent): void {
      if (event.metaKey) {
        if (model == null) return

        const column = findColumn(event, model)

        if (column == null) return

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
      handleModelClick != null && events({ click: handleEventModelClick }),
      handleModelColumn != null && events({ click: handleEventlColumnClick }),
      model != null && SqlMeshModel(models, model, columns, isActionMode),
    ].filter(Boolean) as Extension[]
  }, [model, models, files, handleModelClick, handleModelColumn, isActionMode])

  return extensions
}

export function useSQLMeshModelKeymaps(path: string): KeyBinding[] {
  const client = useQueryClient()

  const files = useStoreFileTree(s => s.files)
  const refreshTab = useStoreEditor(s => s.refreshTab)

  const { refetch: getFileContent } = useApiFileByPath(path)
  const debouncedGetFileContent = debounceAsync(getFileContent, 1000, true)

  const mutationSaveFile = useMutationApiSaveFile(client, {
    onSuccess: saveChangeSuccess,
  })

  const debouncedSaveChange = useCallback(
    debounceSync(saveChange, 1000, true),
    [path],
  )

  useEffect(() => {
    const file = files.get(path)

    if (file == null) return

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
  }, [path])

  function saveChange(): void {
    const file = files.get(path)

    if (file == null) return

    mutationSaveFile.mutate({
      path: file.path,
      body: { content: file.content },
    })
  }

  function saveChangeSuccess(newfile: File): void {
    const file = files.get(path)

    if (newfile == null || file == null) return

    file.update(newfile)

    refreshTab()
  }

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
