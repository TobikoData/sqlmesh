import { useCallback, useEffect, useMemo, useState } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { python } from '@codemirror/lang-python'
import { StreamLanguage } from '@codemirror/language'
import { type KeyBinding, keymap } from '@codemirror/view'
import { yaml } from '@codemirror/legacy-modes/mode/yaml'
import { type Extension } from '@codemirror/state'
import { type File } from '~/api/client'
import { useStoreFileTree } from '~/context/fileTree'
import {
  events,
  SqlMeshModel,
  HoverTooltip,
  useSqlMeshExtension,
} from './extensions'
import { dracula, tomorrow } from 'thememirror'
import { useColorScheme, EnumColorScheme } from '~/context/theme'

import {
  apiCancelFiles,
  apiCancelPlanRun,
  useApiFileByPath,
  useApiPlanRun,
  useMutationApiSaveFile,
} from '~/api'
import {
  debounceAsync,
  debounceSync,
  isFalse,
  isStringEmptyOrNil,
} from '~/utils'
import { isCancelledError, useQueryClient } from '@tanstack/react-query'
import { useStoreContext } from '~/context/context'
import { useStoreEditor } from '~/context/editor'

export default function CodeEditor(): JSX.Element {
  const { mode } = useColorScheme()
  const [SqlMeshDialect, SqlMeshDialectCleanUp] = useSqlMeshExtension()

  const models = useStoreContext(s => s.models)

  const files = useStoreFileTree(s => s.files)
  const selectFile = useStoreFileTree(s => s.selectFile)

  const tab = useStoreEditor(s => s.tab)
  const dialects = useStoreEditor(s => s.dialects)
  const engine = useStoreEditor(s => s.engine)
  const refreshTab = useStoreEditor(s => s.refreshTab)
  const closeTab = useStoreEditor(s => s.closeTab)
  const selectTab = useStoreEditor(s => s.selectTab)
  const createTab = useStoreEditor(s => s.createTab)

  const [sqlDialectOptions, setSqlDialectOptions] = useState()

  const handleEngineWorkerMessage = useCallback(
    (e: MessageEvent): void => {
      if (e.data.topic === 'parse') {
        tab.isValid =
          e.data.payload?.type !== 'error' || tab.file.content === ''

        refreshTab()
      }

      if (e.data.topic === 'dialect') {
        setSqlDialectOptions(e.data.payload)
      }
    },
    [tab.file],
  )

  const dialectsTitles = useMemo(
    () => dialects.map(d => d.dialect_title),
    [dialects],
  )

  const extensions = useMemo(() => {
    const showSqlMeshDialect =
      tab.file.extension === '.sql' && sqlDialectOptions != null

    return [
      mode === EnumColorScheme.Dark ? dracula : tomorrow,
      HoverTooltip(models),
      events(models, files, selectFile),
      SqlMeshModel(models),
      tab.file.extension === '.py' && python(),
      tab.file.extension === '.yaml' && StreamLanguage.define(yaml),
      showSqlMeshDialect &&
        SqlMeshDialect(models, tab.file, sqlDialectOptions, dialectsTitles),
    ].filter(Boolean) as Extension[]
  }, [tab.file, models, mode, sqlDialectOptions, files, dialectsTitles])

  const keymaps = useMemo(
    () => [
      {
        key: 'Mod-Alt-[',
        preventDefault: true,
        run() {
          selectTab(createTab())

          return true
        },
      },
      {
        key: 'Mod-Alt-]',
        preventDefault: true,
        run() {
          closeTab(tab.file.id)

          return true
        },
      },
    ],
    [closeTab, selectTab, createTab, tab.file.id],
  )

  useEffect(() => {
    engine.addEventListener('message', handleEngineWorkerMessage)

    return () => {
      engine.removeEventListener('message', handleEngineWorkerMessage)

      SqlMeshDialectCleanUp()
    }
  }, [handleEngineWorkerMessage])

  useEffect(() => {
    engine.postMessage({
      topic: 'parse',
      payload: tab.file.content,
    })
  }, [tab.file.content])

  function updateFileContent(value: string): void {
    tab.file.content = value
    tab.isSaved = isFalse(tab.file.isChanged)

    refreshTab()
  }

  return tab.file.isLocal ? (
    <CodeEditorFileLocal
      keymaps={keymaps}
      extensions={extensions}
      onChange={updateFileContent}
    />
  ) : (
    <CodeEditorFileRemote
      keymaps={keymaps}
      extensions={extensions}
      onChange={updateFileContent}
    />
  )
}

function CodeEditorFileLocal({
  keymaps,
  extensions,
  onChange,
}: {
  keymaps: KeyBinding[]
  extensions: Extension[]
  onChange: (value: string) => void
}): JSX.Element {
  const tab = useStoreEditor(s => s.tab)

  const extensionKeymap = useMemo(() => keymap.of([...keymaps]), [keymaps])

  const extensionsAll = useMemo(
    () => [...extensions, extensionKeymap],
    [extensionKeymap, extensions],
  )

  return (
    <CodeMirror
      height="100%"
      width="100%"
      className="w-full h-full overflow-auto text-sm font-mono"
      value={tab.file.content}
      extensions={extensionsAll}
      onChange={onChange}
    />
  )
}

function CodeEditorFileRemote({
  keymaps,
  extensions,
  onChange,
}: {
  keymaps: KeyBinding[]
  extensions: Extension[]
  onChange: (value: string) => void
}): JSX.Element {
  const client = useQueryClient()

  const models = useStoreContext(s => s.models)
  const environment = useStoreContext(s => s.environment)

  const tab = useStoreEditor(s => s.tab)
  const engine = useStoreEditor(s => s.engine)
  const refreshTab = useStoreEditor(s => s.refreshTab)

  const { refetch: getFileContent } = useApiFileByPath(tab.file.path)
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
    [tab],
  )

  const extensionKeymap = useMemo(
    () =>
      keymap.of([
        ...keymaps,
        {
          mac: 'Cmd-s',
          win: 'Ctrl-s',
          linux: 'Ctrl-s',
          preventDefault: true,
          run() {
            debouncedSaveChange(tab.file.content)

            return true
          },
        },
      ]),
    [debouncedSaveChange, keymaps, tab.file.content],
  )

  const extensionsAll = useMemo(
    () => [...extensions, extensionKeymap],
    [extensionKeymap, extensions],
  )

  useEffect(() => {
    return () => {
      debouncedPlanRun.cancel()
      debouncedGetFileContent.cancel()

      apiCancelPlanRun(client)
      apiCancelFiles(client)
    }
  }, [])

  useEffect(() => {
    if (isStringEmptyOrNil(tab.file.content)) {
      debouncedGetFileContent({
        throwOnError: true,
      })
        .then(({ data }) => {
          const model = models.get(tab.file.path)

          tab.file.updateContent(data?.content ?? '')

          if (model != null) {
            tab.dialect = model.dialect

            engine.postMessage({
              topic: 'dialect',
              payload: model.dialect,
            })
          }

          engine.postMessage({
            topic: 'parse',
            payload: tab.file.content,
          })
        })
        .catch(error => {
          if (isCancelledError(error)) {
            console.log('getFileContent', 'Request aborted by React Query')
          } else {
            console.log('getFileContent', error)

            tab.isSaved = false
            tab.isValid = false
          }
        })
        .finally(() => {
          refreshTab()
        })
    }
  }, [tab.file.path])

  function saveChange(): void {
    mutationSaveFile.mutate({
      path: tab.file.path,
      body: { content: tab.file.content },
    })
  }

  function saveChangeSuccess(file: File): void {
    if (file == null) return

    tab.file.updateContent(file.content)
    tab.isSaved = true

    refreshTab()

    void debouncedPlanRun()
  }

  return (
    <CodeMirror
      height="100%"
      width="100%"
      className="w-full h-full overflow-auto text-sm font-mono"
      value={tab.file.content}
      extensions={extensionsAll}
      onChange={onChange}
    />
  )
}
