import { useCallback, useEffect, useMemo, useState } from 'react'
import { Divider } from '../divider/Divider'
import { useStoreProject } from '../../../context/project'
import SplitPane from '../splitPane/SplitPane'
import {
  debounceSync,
  isFalse,
  isNil,
  isStringEmptyOrNil,
} from '../../../utils'
import EditorFooter from './EditorFooter'
import EditorTabs from './EditorTabs'
import EditorInspector from './EditorInspector'
import './Editor.css'
import EditorPreview from './EditorPreview'
import { type EditorTab, useStoreEditor } from '~/context/editor'
import clsx from 'clsx'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { EnumFileExtensions } from '@models/file'
import { useLineageFlow } from '@components/graph/context'
import { CodeEditorRemoteFile, CodeEditorDefault } from './EditorCode'
import { useDefaultKeymapsEditorTab, useSQLMeshModelExtensions } from './hooks'
import { useApiFetchdf } from '@api/index'
import { getTableDataFromArrowStreamResult } from '@components/table/help'
import { type Table } from 'apache-arrow'
import { type KeyBinding } from '@codemirror/view'

function Editor(): JSX.Element {
  const tab = useStoreEditor(s => s.tab)
  const engine = useStoreEditor(s => s.engine)

  const [isReadyEngine, setIsreadyEngine] = useState(false)

  const handleEngineWorkerMessage = useCallback(
    (e: MessageEvent): void => {
      if (e.data.topic === 'init' && isFalse(isReadyEngine)) {
        setIsreadyEngine(true)
      }
    },
    [engine],
  )

  useEffect(() => {
    isFalse(isReadyEngine) &&
      engine.postMessage({
        topic: 'init',
      })
  }, [])

  useEffect(() => {
    engine.addEventListener('message', handleEngineWorkerMessage)

    return () => {
      engine.removeEventListener('message', handleEngineWorkerMessage)
    }
  }, [handleEngineWorkerMessage])

  return (
    <div className="w-full h-full flex flex-col overflow-hidden">
      {isReadyEngine ? (
        <>
          <EditorTabs />
          <Divider />
          {isNil(tab) ? <EditorEmpty /> : <EditorMain tab={tab} />}
        </>
      ) : (
        <EditorLoading />
      )}
    </div>
  )
}

function EditorEmpty(): JSX.Element {
  return (
    <div className="flex justify-center items-center w-full h-full">
      <div className="p-4 text-center text-theme-darker dark:text-theme-lighter">
        <h2 className="text-3xl">Select File or Add New SQL Tab</h2>
      </div>
    </div>
  )
}

function EditorLoading(): JSX.Element {
  return (
    <div className="flex justify-center items-center w-full h-full">
      <Loading className="inline-block">
        <Spinner className="w-5 h-5 border border-neutral-10 mr-4" />
        <h3 className="text-xl">Starting Editor...</h3>
      </Loading>
    </div>
  )
}

function EditorMain({ tab }: { tab: EditorTab }): JSX.Element {
  const files = useStoreProject(s => s.files)
  const setSelectedFile = useStoreProject(s => s.setSelectedFile)

  const direction = useStoreEditor(s => s.direction)
  const engine = useStoreEditor(s => s.engine)
  const previewTable = useStoreEditor(s => s.previewTable)
  const previewDiff = useStoreEditor(s => s.previewDiff)
  const refreshTab = useStoreEditor(s => s.refreshTab)
  const setPreviewQuery = useStoreEditor(s => s.setPreviewQuery)
  const setPreviewTable = useStoreEditor(s => s.setPreviewTable)
  const setPreviewDiff = useStoreEditor(s => s.setPreviewDiff)
  const setDialects = useStoreEditor(s => s.setDialects)

  const { models, setManuallySelectedColumn } = useLineageFlow()

  const defaultKeymapsEditorTab = useDefaultKeymapsEditorTab()
  const modelExtensions = useSQLMeshModelExtensions(
    tab.file.path,
    model => {
      setSelectedFile(files.get(model.path))
    },
    (model, column) => {
      setManuallySelectedColumn([model, column])
    },
  )

  const customSQLKeymaps = useMemo(() => {
    return [
      ...defaultKeymapsEditorTab,
      {
        key: 'Ctrl-Enter',
        preventDefault: true,
        run: debounceSync(() => {
          sendQuery()

          return true
        }),
      },
    ] as KeyBinding[]
  }, [defaultKeymapsEditorTab])

  const handleEngineWorkerMessage = useCallback(
    (e: MessageEvent): void => {
      if (e.data.topic === 'dialects') {
        setDialects(e.data.payload)
      }

      if (e.data.topic === 'validate') {
        tab.isValid = e.data.payload

        refreshTab()
      }

      if (e.data.topic === 'format') {
        if (isStringEmptyOrNil(e.data.payload)) return

        tab.file.content = e.data.payload

        refreshTab()
      }
    },
    [tab.id],
  )

  const updateFileContent = useCallback(
    function updateFileContent(value: string): void {
      if (tab == null) return

      tab.file.content = value

      refreshTab()
    },
    [tab.id],
  )

  const sizesCodeEditorAndInspector = useMemo(() => {
    const model = models.get(tab?.file.path)
    const showInspector =
      ((tab.file.isSQLMeshModel && model != null) || tab.file.isLocal) &&
      isFalse(isStringEmptyOrNil(tab.file.content))

    return showInspector ? [75, 25] : [100, 0]
  }, [tab, models])

  const sizesCodeEditorAndPreview = useMemo(() => {
    const model = models.get(tab.file.path)
    const showPreview =
      (tab.file.isLocal && [previewTable, previewDiff].some(Boolean)) ||
      (tab.file.isSQLMeshModel && model != null)

    return showPreview ? [70, 30] : [100, 0]
  }, [tab, models, previewTable, previewDiff])

  useEffect(() => {
    engine.addEventListener('message', handleEngineWorkerMessage)

    return () => {
      engine.removeEventListener('message', handleEngineWorkerMessage)
    }
  }, [handleEngineWorkerMessage])

  useEffect(() => {
    const model = models.get(tab.file.path)

    tab.dialect = model?.dialect ?? ''

    refreshTab()
  }, [tab.id, models])

  useEffect(() => {
    tab.isSaved = isFalse(tab.file.isChanged)

    engine.postMessage({
      topic: 'validate',
      payload: tab.file.content,
    })
  }, [tab.file.fingerprint, tab.file.content])

  useEffect(() => {
    setPreviewQuery(undefined)
    setPreviewTable(undefined)
    setPreviewDiff(undefined)
  }, [tab.id, tab.file.fingerprint])

  const { refetch: getFetchdf } = useApiFetchdf({
    sql: tab.file.content,
  })

  function sendQuery(): void {
    setPreviewTable(undefined)
    setPreviewQuery(tab.file.content)

    void getFetchdf().then(({ data }) => {
      setPreviewTable(getTableDataFromArrowStreamResult(data as Table<any>))
    })
  }

  return (
    <SplitPane
      key={direction}
      className={clsx(
        'w-full h-full overflow-hidden',
        direction === 'vertical' ? 'flex flex-col' : 'flex',
      )}
      sizes={sizesCodeEditorAndPreview}
      direction={direction}
      minSize={0}
      snapOffset={0}
    >
      <div
        className={clsx(
          'flex flex-col overflow-hidden',
          direction === 'vertical' ? 'w-full ' : 'h-full',
        )}
      >
        <div className="flex flex-col h-full overflow-hidden">
          <SplitPane
            key={tab.id}
            className="flex h-full"
            sizes={sizesCodeEditorAndInspector}
            minSize={0}
            snapOffset={0}
          >
            <div className="flex flex-col h-full">
              {tab.file.isLocal && (
                <CodeEditorDefault
                  key={tab.id}
                  type={EnumFileExtensions.SQL}
                  dialect={tab.dialect}
                  content={tab.file.content}
                  keymaps={customSQLKeymaps}
                  onChange={updateFileContent}
                />
              )}
              {tab.file.isRemote && (
                <CodeEditorRemoteFile path={tab.file.path}>
                  {({ file, keymaps }) => (
                    <CodeEditorDefault
                      type={file.extension}
                      dialect={tab.dialect}
                      content={file.content}
                      extensions={modelExtensions}
                      keymaps={keymaps.concat(defaultKeymapsEditorTab)}
                      onChange={updateFileContent}
                    />
                  )}
                </CodeEditorRemoteFile>
              )}
            </div>
            <div className="flex flex-col h-full">
              <EditorInspector tab={tab} />
            </div>
          </SplitPane>
        </div>
        <Divider />
        <div className="px-2 flex justify-between items-center min-h-[2rem]">
          <EditorFooter tab={tab} />
        </div>
      </div>
      <EditorPreview
        tab={tab}
        className={clsx(direction === 'vertical' ? 'flex flex-col' : 'flex')}
      />
    </SplitPane>
  )
}

Editor.Empty = EditorEmpty
Editor.Loading = EditorEmpty
Editor.Main = EditorMain

export default Editor
