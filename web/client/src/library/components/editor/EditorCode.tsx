import React, { useCallback, useEffect, useMemo, useState } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { python } from '@codemirror/lang-python'
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
import { useApiFileByPath, useMutationApiSaveFile } from '~/api'
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
import { sql } from '@codemirror/lang-sql'
import { useLineageFlow } from '@components/graph/context'

function CodeEditorDefault({
  type,
  content = '',
  children,
  className,
}: {
  type: FileExtensions
  content: string
  className?: string
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
      type === EnumFileExtensions.YML && StreamLanguage.define(yaml),
      type === EnumFileExtensions.SQL && sql(),
    ].filter(Boolean) as Extension[]
  }, [type, mode])

  return (
    <div className={clsx('flex w-full h-full', className)}>
      {children({ extensions, content })}
    </div>
  )
}

function CodeEditorSQLMesh({
  type,
  content = '',
  children,
  className,
}: {
  type: FileExtensions
  content?: string
  className?: string
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
      type === EnumFileExtensions.Python && python(),
      type === EnumFileExtensions.YAML && StreamLanguage.define(yaml),
      type === EnumFileExtensions.YML && StreamLanguage.define(yaml),
      SqlMeshDialect(models, dialectOptions, dialectsTitles),
    ].filter(Boolean) as Extension[]
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
    <div className={clsx('flex w-full h-full', className)}>
      {children({ extensions, content })}
    </div>
  )
}

function CodeEditorRemoteFile({
  path,
  children,
}: {
  path: string
  children: (options: { file: ModelFile; keymaps: KeyBinding[] }) => JSX.Element
}): JSX.Element {
  const files = useStoreFileTree(s => s.files)

  const { refetch: getFileContent, isFetching } = useApiFileByPath(path)
  const debouncedGetFileContent = debounceAsync(getFileContent, 1000, true)

  const keymaps = useSQLMeshModelKeymaps(path)
  const [file, setFile] = useState<ModelFile>()

  useEffect(() => {
    setFile(undefined)

    const tempFile = files.get(path)

    if (tempFile == null) return
    if (isStringNotEmpty(tempFile.content)) {
      setFile(tempFile)
      return
    }

    debouncedGetFileContent({
      throwOnError: true,
    })
      .then(({ data }) => {
        tempFile.update(data)

        setFile(tempFile)
      })
      .catch(error => {
        if (isCancelledError(error)) {
          console.log('getFileContent', 'Request aborted by React Query')
        } else {
          console.log('getFileContent', error)
        }
      })
  }, [path])

  return isFetching ? (
    <div className="flex justify-center items-center w-full h-full">
      <Loading className="inline-block">
        <Spinner className="w-5 h-5 border border-neutral-10 mr-4" />
        <h3 className="text-xl">Waiting for File...</h3>
      </Loading>
    </div>
  ) : file == null ? (
    <div className="flex justify-center items-center w-full h-full">
      <h3 className="text-xl">File Not Found</h3>
    </div>
  ) : (
    children({ file, keymaps })
  )
}

export function useSQLMeshModelExtensions(
  path?: string,
  handleModelClick?: (model: ModelSQLMeshModel) => void,
  handleModelColumn?: (model: ModelSQLMeshModel, column: Column) => void,
): Extension[] {
  const { models, lineage } = useLineageFlow()
  const files = useStoreFileTree(s => s.files)

  const extensions = useMemo(() => {
    const model = path == null ? undefined : models.get(path)
    const columns =
      lineage == null
        ? new Set<string>()
        : new Set(
            Object.keys(lineage)
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
      className={clsx('flex w-full h-full font-mono text-sm', className)}
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
