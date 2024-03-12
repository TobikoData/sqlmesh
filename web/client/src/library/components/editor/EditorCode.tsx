import { useCallback, useEffect, useMemo, useState } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { type KeyBinding, keymap } from '@codemirror/view'
import { type Extension } from '@codemirror/state'
import { useApiFileByPath, useMutationApiSaveFile } from '~/api'
import { debounceSync, isNil } from '~/utils'
import { useStoreContext } from '~/context/context'
import { useStoreEditor } from '~/context/editor'
import {
  completionStatus,
  acceptCompletion,
  autocompletion,
} from '@codemirror/autocomplete'
import {
  ModelFile,
  type FileExtensions,
  EnumFileExtensions,
} from '@models/file'
import clsx from 'clsx'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { useStoreProject } from '@context/project'
import { useQueryClient } from '@tanstack/react-query'
import { EnumColorScheme, useColorScheme } from '@context/theme'
import { dracula, tomorrow } from 'thememirror'
import { python } from '@codemirror/lang-python'
import { StreamLanguage } from '@codemirror/language'
import { yaml } from '@codemirror/legacy-modes/mode/yaml'
import './Editor.css'
import {
  SQLMeshDialect,
  SQLMeshDialectCleanUp,
} from './extensions/SQLMeshDialect'
import { indentMore } from '@codemirror/commands'

export { CodeEditorDefault, CodeEditorRemoteFile }

function CodeEditorDefault({
  type,
  dialect = '',
  content = '',
  className,
  keymaps,
  extensions,
  onChange,
}: {
  type: FileExtensions
  dialect?: string
  className?: string
  content: string
  keymaps?: KeyBinding[]
  extensions?: Extension[]
  onChange?: (value: string) => void
}): JSX.Element {
  const { mode } = useColorScheme()

  const models = useStoreContext(s => s.models)

  const engine = useStoreEditor(s => s.engine)
  const dialects = useStoreEditor(s => s.dialects)

  const [value, setValue] = useState(content)

  const [dialectOptions, setDialectOptions] = useState<{
    types: string
    keywords: string
  }>()

  const debouncedChange = useCallback(
    debounceSync(function handleChange(value): void {
      setValue(value)
      onChange?.(value)
    }, 500),
    [],
  )

  const handleEngineWorkerMessage = useCallback((e: MessageEvent): void => {
    if (e.data.topic === 'dialect') {
      setDialectOptions(e.data.payload)
    }
  }, [])

  const extensionsDefault = useMemo(() => {
    return [
      autocompletion({
        selectOnOpen: false,
        maxRenderedOptions: 50,
      }),
      mode === EnumColorScheme.Dark ? dracula : tomorrow,
      type === EnumFileExtensions.PY && python(),
      type === EnumFileExtensions.YAML && StreamLanguage.define(yaml),
      type === EnumFileExtensions.YML && StreamLanguage.define(yaml),
    ].filter(Boolean) as Extension[]
  }, [type, mode])
  const dialectsTitles = useMemo(
    () => dialects.map(d => d.dialect_title),
    [dialects],
  )
  const extensionKeymap = useMemo(
    () =>
      keymap.of(
        [
          ...(keymaps ?? []),
          [
            {
              key: 'Tab',
              preventDefault: true,
              run(e: any) {
                return isNil(completionStatus(e.state))
                  ? indentMore(e)
                  : acceptCompletion(e)
              },
            },
          ],
        ].flat(),
      ),
    [keymaps],
  )
  const allModels = useMemo(() => Array.from(models.values()), [models])
  const allModelsNames = useMemo(
    () =>
      allModels.map(model => ({
        label: model.name,
        type: 'model',
      })),
    [allModels],
  )
  const allColumnsNames = useMemo(
    () =>
      Array.from(
        new Set(
          allModels.flatMap(model => model.columns.map(column => column.name)),
        ),
      ).map(name => ({
        label: name,
        type: 'column',
      })),
    [allModels],
  )
  const extensionsAll = useMemo(() => {
    return [
      ...extensionsDefault,
      extensions,
      type === EnumFileExtensions.SQL &&
        SQLMeshDialect(
          models,
          allModelsNames,
          allColumnsNames,
          dialectOptions,
          dialectsTitles,
        ),
      extensionKeymap,
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
    allModelsNames,
    allColumnsNames,
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
    setValue(content)
  }, [content])

  return (
    <div className={clsx('flex w-full h-full', className)}>
      <CodeMirror
        height="100%"
        width="100%"
        className={clsx('flex w-full h-full font-mono text-xs', className)}
        value={value}
        indentWithTab={false}
        extensions={extensionsAll}
        onChange={debouncedChange}
        readOnly={isNil(onChange)}
        basicSetup={{
          autocompletion: false,
        }}
      />
    </div>
  )
}

function CodeEditorRemoteFile({
  path,
  children,
  keymaps,
}: {
  path: string
  keymaps?: KeyBinding[]
  children: (options: { file: ModelFile; keymaps: KeyBinding[] }) => JSX.Element
}): JSX.Element {
  const client = useQueryClient()

  const files = useStoreProject(s => s.files)
  const refreshFiles = useStoreProject(s => s.refreshFiles)

  const {
    refetch: getFileContent,
    isFetching,
    cancel: cancelRequestFileByPath,
  } = useApiFileByPath(path)

  const mutationSaveFile = useMutationApiSaveFile(client)
  const debouncedSaveChange = useCallback(
    debounceSync(
      function saveChange(view): void {
        mutationSaveFile.mutate({
          path,
          body: { content: view.state.doc.toString() },
        })
      },
      500,
      true,
    ),
    [path],
  )

  const extensionKeymap = useMemo(
    () =>
      (keymaps ?? []).concat([
        {
          mac: 'Cmd-s',
          win: 'Ctrl-s',
          linux: 'Ctrl-s',
          preventDefault: true,
          run(view) {
            debouncedSaveChange(view)

            return true
          },
        },
      ]),
    [keymaps, debouncedSaveChange],
  )

  useEffect(() => {
    void getFileContent().then(({ data }) => {
      if (isNil(data)) return

      const file = files.get(data.path)

      if (isNil(file)) {
        files.set(path, new ModelFile(data))
      } else {
        file.update(data)
      }

      refreshFiles()
    })

    return () => {
      void cancelRequestFileByPath()
    }
  }, [path])

  const file = files.get(path)

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
    children({ file, keymaps: extensionKeymap })
  )
}
