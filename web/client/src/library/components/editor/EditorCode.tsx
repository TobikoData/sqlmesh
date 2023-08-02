import { useCallback, useEffect, useMemo, useState } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { type KeyBinding, keymap } from '@codemirror/view'
import { type Extension } from '@codemirror/state'
import { useApiFileByPath, useMutationApiSaveFile } from '~/api'
import { debounceSync, isNil, isNotNil } from '~/utils'
import { useStoreContext } from '~/context/context'
import { useStoreEditor } from '~/context/editor'
import {
  ModelFile,
  type FileExtensions,
  EnumFileExtensions,
} from '@models/file'
import clsx from 'clsx'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { useSQLMeshDialect } from './hooks'
import { useStoreProject } from '@context/project'
import { useQueryClient } from '@tanstack/react-query'
import { EnumColorScheme, useColorScheme } from '@context/theme'
import { dracula, tomorrow } from 'thememirror'
import { python } from '@codemirror/lang-python'
import { StreamLanguage } from '@codemirror/language'
import { yaml } from '@codemirror/legacy-modes/mode/yaml'

export { CodeEditorDefault, CodeEditorRemoteFile }

function CodeEditorDefault({
  type,
  dialect = '',
  content = '',
  className,
  keymaps = [],
  extensions = [],
  onChange,
}: {
  type: FileExtensions
  dialect?: string
  className?: string
  content?: string
  keymaps?: KeyBinding[]
  extensions?: Extension[]
  onChange?: (value: string) => void
}): JSX.Element {
  const [SQLMeshDialect, SQLMeshDialectCleanUp] = useSQLMeshDialect()
  const { mode } = useColorScheme()

  const extensionsDefault = useMemo(() => {
    return [
      mode === EnumColorScheme.Dark ? dracula : tomorrow,
      type === EnumFileExtensions.PY && python(),
      type === EnumFileExtensions.YAML && StreamLanguage.define(yaml),
      type === EnumFileExtensions.YML && StreamLanguage.define(yaml),
    ].filter(Boolean) as Extension[]
  }, [type, mode])

  const models = useStoreContext(s => s.models)
  const engine = useStoreEditor(s => s.engine)
  const dialects = useStoreEditor(s => s.dialects)

  const [dialectOptions, setDialectOptions] = useState<{
    types: string
    keywords: string
  }>()

  const handleEngineWorkerMessage = useCallback((e: MessageEvent): void => {
    if (e.data.topic === 'dialect') {
      setDialectOptions(e.data.payload)
    }
  }, [])

  const dialectsTitles = useMemo(
    () => dialects.map(d => d.dialect_title),
    [dialects],
  )

  const extensionKeymap = useMemo(
    () => keymap.of([...keymaps].flat()),
    [keymaps],
  )

  const extensionsAll = useMemo(() => {
    return [
      ...extensionsDefault,
      extensions,
      extensionKeymap,
      type === EnumFileExtensions.SQL &&
        SQLMeshDialect(models, dialectOptions, dialectsTitles),
    ]
      .filter(Boolean)
      .flat() as Extension[]
  }, [
    models,
    type,
    dialectsTitles,
    dialectOptions,
    extensionsDefault,
    extensions,
    extensionKeymap,
  ])

  useEffect(() => {
    engine.postMessage({
      topic: 'dialects',
    })

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
      topic: 'dialect',
      payload: dialect,
    })
  }, [dialect])

  useEffect(() => {
    engine.postMessage({
      topic: 'validate',
      payload: content,
    })
  }, [content])

  return (
    <div className={clsx('flex w-full h-full', className)}>
      <CodeMirror
        height="100%"
        width="100%"
        className={clsx('flex w-full h-full font-mono text-sm', className)}
        value={content}
        extensions={extensionsAll}
        onChange={onChange}
        readOnly={isNil(onChange)}
      />
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
  const client = useQueryClient()

  const files = useStoreProject(s => s.files)

  const {
    refetch: getFileContent,
    isFetching,
    cancel: cancelRequestFileByPath,
  } = useApiFileByPath(path)

  const [file, setFile] = useState<ModelFile | undefined>(files.get(path))

  const mutationSaveFile = useMutationApiSaveFile(client)
  const debouncedSaveChange = useCallback(
    debounceSync(
      function saveChange(): void {
        mutationSaveFile.mutate({
          path,
          body: { content: file?.content },
        })
      },
      1000,
      true,
    ),
    [path],
  )

  const keymaps = useMemo(
    () => [
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
    ],
    [debouncedSaveChange],
  )

  useEffect(() => {
    if (isNotNil(file) && file.isSynced) return

    void getFileContent().then(({ data }) => {
      if (isNil(data)) return

      if (isNil(file)) {
        setFile(new ModelFile(data))
      } else {
        file.update(data)
      }
    })

    return () => {
      void cancelRequestFileByPath()
    }
  }, [])

  return isFetching ? (
    <div className="flex justify-center items-center w-full h-full">
      <Loading className="inline-block">
        <Spinner className="w-5 h-5 border border-neutral-10 mr-4" />
        <h3 className="text-xl">Waiting for File...</h3>
      </Loading>
    </div>
  ) : isNil(file) ? (
    <div className="flex justify-center items-center w-full h-full">
      <h3 className="text-xl">File Not Found</h3>
    </div>
  ) : (
    children({ file, keymaps })
  )
}
