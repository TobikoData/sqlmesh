import { useCallback, useEffect, useMemo, useState } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { type KeyBinding, keymap } from '@codemirror/view'
import { type Extension } from '@codemirror/state'
import { useStoreFileTree } from '~/context/fileTree'
import { useApiFileByPath } from '~/api'
import { debounceAsync, isNil, isStringNotEmpty } from '~/utils'
import { isCancelledError } from '@tanstack/react-query'
import { useStoreContext } from '~/context/context'
import { useStoreEditor } from '~/context/editor'
import {
  type ModelFile,
  type FileExtensions,
  EnumFileExtensions,
} from '@models/file'
import clsx from 'clsx'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import {
  useDefaultExtensions,
  useKeymapsRemoteFile,
  useSQLMeshDialect,
} from './hooks'

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
  const [SQLMeshDialect, SQLMeshDialectCleanUp] = useSQLMeshDialect()

  const extensionsDefault = useDefaultExtensions(type)

  const models = useStoreContext(s => s.models)
  const engine = useStoreEditor(s => s.engine)
  const dialects = useStoreEditor(s => s.dialects)
  const setDialects = useStoreEditor(s => s.setDialects)

  const [dialectOptions, setDialectOptions] = useState<{
    types: string
    keywords: string
  }>()

  const handleEngineWorkerMessage = useCallback((e: MessageEvent): void => {
    console.log('handleEngineWorkerMessage', e.data)

    if (e.data.topic === 'dialects') {
      setDialects(e.data.payload)
    }

    if (e.data.topic === 'dialect') {
      console.log('setDialectOptions', e.data.payload)
      setDialectOptions(e.data.payload)
    }
  }, [])

  const dialectsTitles = useMemo(
    () => dialects.map(d => d.dialect_title),
    [dialects],
  )

  const extensions = useMemo(() => {
    return [
      ...extensionsDefault,
      type === EnumFileExtensions.SQL &&
        SQLMeshDialect(models, dialectOptions, dialectsTitles),
    ]
      .filter(Boolean)
      .flat(10) as Extension[]
  }, [models, type, dialectsTitles, dialectOptions])

  useEffect(() => {
    return () => {
      SQLMeshDialectCleanUp()
    }
  }, [])

  useEffect(() => {
    engine.addEventListener('message', handleEngineWorkerMessage)

    return () => {
      engine.removeEventListener('message', handleEngineWorkerMessage)
    }
  }, [handleEngineWorkerMessage])

  useEffect(() => {
    engine.postMessage({
      topic: 'validate',
      payload: content,
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

  const keymaps = useKeymapsRemoteFile(path)

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
  const extensionKeymap = useMemo(
    () => keymap.of([...keymaps].flat()),
    [keymaps],
  )
  const extensionsAll = useMemo(
    () => [...extensions, extensionKeymap].flat(),
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
      readOnly={isNil(onChange)}
      autoFocus
    />
  )
}

CodeEditor.Default = CodeEditorSQLMesh
CodeEditor.RemoteFile = CodeEditorRemoteFile

export default CodeEditor
