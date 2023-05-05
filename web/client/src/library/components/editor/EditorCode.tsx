import React, { useCallback, useEffect, useMemo, useState } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { python } from '@codemirror/lang-python'
import { sql } from '@codemirror/lang-sql'
import { StreamLanguage } from '@codemirror/language'
import { type KeyBinding, keymap } from '@codemirror/view'
import { yaml } from '@codemirror/legacy-modes/mode/yaml'
import { type Extension } from '@codemirror/state'
import { type Column, type File } from '~/api/client'
import { useStoreFileTree } from '~/context/fileTree'
import {
  events,
  SqlMeshModel,
  HoverTooltip,
  useSqlMeshExtension,
  findModel,
  findColumn,
} from './extensions'
import { dracula, tomorrow } from 'thememirror'
import { useColorScheme, EnumColorScheme } from '~/context/theme'

import { useApiFileByPath, useApiPlanRun, useMutationApiSaveFile } from '~/api'
import {
  debounceAsync,
  debounceSync,
  isFalse,
  isStringEmptyOrNil,
  isStringNotEmpty,
} from '~/utils'
import { isCancelledError, useQueryClient } from '@tanstack/react-query'
import { useStoreContext } from '~/context/context'
import { useStoreEditor } from '~/context/editor'
import {
  EnumFileExtensions,
  type ModelFile,
  type FileExtensions,
} from '@models/file'
import clsx from 'clsx'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'

function CodeEditorDefault({
  type,
  content = '',
  children,
}: {
  type: FileExtensions
  content: string
  children: (options: {
    extensions: Extension[]
    content: string
  }) => JSX.Element
}): JSX.Element {
  const { mode } = useColorScheme()

  const extensions = useMemo(() => {
    return [
      mode === EnumColorScheme.Dark ? dracula : tomorrow,
      type === EnumFileExtensions.Python && python(),
      type === EnumFileExtensions.YAML && StreamLanguage.define(yaml),
      type === EnumFileExtensions.SQL && sql(),
    ].filter(Boolean) as Extension[]
  }, [type, mode])

  return (
    <div className="flex overflow-auto h-full">
      {children({ extensions, content })}
    </div>
  )
}

function CodeEditorSQLMesh({
  content = '',
  children,
}: {
  content?: string
  children: (options: {
    extensions: Extension[]
    content: string
  }) => JSX.Element
}): JSX.Element {
  const { mode } = useColorScheme()

  const [SqlMeshDialect, SqlMeshDialectCleanUp] = useSqlMeshExtension()

  const models = useStoreContext(s => s.models)

  const engine = useStoreEditor(s => s.engine)
  const dialects = useStoreEditor(s => s.dialects)

  const [dialectOptions, setDialectOptions] = useState<{
    types: string
    keywords: string
  }>()

  const updateDialectOptions = useCallback((e: MessageEvent): void => {
    if (e.data.topic === 'dialect') {
      setDialectOptions(e.data.payload)
    }
  }, [])

  const dialectsTitles = useMemo(
    () => dialects.map(d => d.dialect_title),
    [dialects],
  )

  const extensions = useMemo(() => {
    return [
      mode === EnumColorScheme.Dark ? dracula : tomorrow,
      SqlMeshDialect(models, dialectOptions, dialectsTitles),
    ]
  }, [mode, dialectsTitles, dialectOptions])

  useEffect(() => {
    return () => {
      SqlMeshDialectCleanUp()
    }
  }, [])

  useEffect(() => {
    engine.addEventListener('message', updateDialectOptions)

    return () => {
      engine.removeEventListener('message', updateDialectOptions)
    }
  }, [updateDialectOptions])

  useEffect(() => {
    engine.postMessage({
      topic: 'dialect',
    })
  }, [content])

  return (
    <div className="flex overflow-auto h-full">
      {children({ extensions, content })}
    </div>
  )
}

function CodeEditorRemoteFile({
  path,
  children,
}: {
  path: string
  children: (options: { file: ModelFile }) => JSX.Element
}): JSX.Element {
  const files = useStoreFileTree(s => s.files)

  const { refetch: getFileContent } = useApiFileByPath(path)
  const debouncedGetFileContent = debounceAsync(getFileContent, 1000, true)

  const [file, setFile] = useState<ModelFile>()

  useEffect(() => {
    const file = files.get(path)

    if (file == null) return
    if (isStringNotEmpty(file.content)) {
      setFile(file)
      return
    }

    debouncedGetFileContent({
      throwOnError: true,
    })
      .then(({ data }) => {
        file.updateContent(data?.content ?? '')

        setFile(file)
      })
      .catch(error => {
        if (isCancelledError(error)) {
          console.log('getFileContent', 'Request aborted by React Query')
        } else {
          console.log('getFileContent', error)
        }
      })
  }, [files, path])

  return file == null ? (
    <div className="flex justify-center items-center w-full h-full">
      <Loading className="inline-block ">
        <Spinner className="w-5 h-5 border border-neutral-10 mr-4" />
        <h3 className="text-xl">Waiting for file...</h3>
      </Loading>
    </div>
  ) : (
    children({ file })
  )
}

export function useSQLMeshModelExtensions(
  path?: string,
  handleModelClick?: (model: ModelSQLMeshModel) => void,
  handleModelColumn?: (model: ModelSQLMeshModel, column: Column) => void,
): Extension[] {
  if (path == null) return []

  const models = useStoreContext(s => s.models)
  const files = useStoreFileTree(s => s.files)

  const extensions = useMemo(() => {
    const model = models.get(path)
    const columns =
      model?.lineage == null
        ? new Set<string>()
        : new Set(
            Object.keys(model.lineage)
              .map(modelName => models.get(modelName)?.columns.map(c => c.name))
              .flat()
              .filter(Boolean) as string[],
          )

    return [
      HoverTooltip(models),
      handleModelClick != null &&
        events(event => {
          const model = findModel(event, models)

          if (model == null) return

          handleModelClick(model)
        }),
      handleModelColumn != null &&
        model != null &&
        events(event => {
          const column = findColumn(event, model)

          if (column == null) return

          handleModelColumn(model, column)
        }),
      model != null && SqlMeshModel(models, model, columns),
    ].filter(Boolean) as Extension[]
  }, [path, models, files])

  return extensions
}

export function useSQLMeshModelKeymaps(file?: ModelFile): KeyBinding[] {
  if (file == null) return []

  const client = useQueryClient()

  const environment = useStoreContext(s => s.environment)

  const refreshTab = useStoreEditor(s => s.refreshTab)

  const { refetch: getFileContent } = useApiFileByPath(file.path)
  const debouncedGetFileContent = debounceAsync(getFileContent, 1000, true)

  const mutationSaveFile = useMutationApiSaveFile(client, {
    onSuccess: saveChangeSuccess,
  })

  const { refetch: planRun } = useApiPlanRun(environment.name, {
    planOptions: {
      skip_tests: true,
    },
  })

  const debouncedPlanRun = useCallback(debounceAsync(planRun, 1000, true), [
    planRun,
  ])

  const debouncedSaveChange = useCallback(
    debounceSync(saveChange, 1000, true),
    [file],
  )

  useEffect(() => {
    if (isStringEmptyOrNil(file.content)) {
      debouncedGetFileContent({
        throwOnError: true,
      })
        .then(({ data }) => {
          file.updateContent(data?.content ?? '')
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

  function saveChange(): void {
    if (file == null) return

    mutationSaveFile.mutate({
      path: file.path,
      body: { content: file.content },
    })
  }

  function saveChangeSuccess(newfile: File): void {
    if (newfile == null || file == null) return

    file.updateContent(newfile.content)

    refreshTab()

    void debouncedPlanRun()
  }

  return [
    {
      mac: 'Cmd-s',
      win: 'Ctrl-s',
      linux: 'Ctrl-s',
      preventDefault: true,
      run() {
        debouncedSaveChange(file.content)

        return true
      },
    },
  ]
}

const CodeEditor = function CodeEditor({
  keymaps = [],
  extensions = [],
  content = '',
  onChange,
  className,
}: {
  content?: string
  keymaps?: KeyBinding[]
  extensions?: Extension[]
  onChange?: (value: string) => void
  className?: string
}): JSX.Element {
  const extensionKeymap = useMemo(() => keymap.of([...keymaps]), [keymaps])
  const extensionsAll = useMemo(
    () => [...extensions, extensionKeymap],
    [extensionKeymap, extensions],
  )

  return (
    <CodeMirror
      height="100%"
      width="100%"
      className={clsx('flex w-full h-full text-sm font-mono', className)}
      value={content}
      extensions={extensionsAll}
      onChange={onChange}
      readOnly={isFalse(Boolean(onChange))}
    />
  )
}

CodeEditor.Default = CodeEditorDefault
CodeEditor.SQLMeshDialect = CodeEditorSQLMesh
CodeEditor.RemoteFile = CodeEditorRemoteFile

export default CodeEditor
