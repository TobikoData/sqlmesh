import { useCallback, useEffect, useMemo, useState } from 'react'
import { Divider } from '../divider/Divider'
import { useStoreFileTree } from '../../../context/fileTree'
import SplitPane from '../splitPane/SplitPane'
import { debounceSync, isFalse, isStringEmptyOrNil } from '../../../utils'
import CodeEditor, {
  useSQLMeshModelExtensions,
} from '@components/editor/EditorCode'
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
import { type KeyBinding } from '@codemirror/view'
import { useLineageFlow } from '@components/graph/context'

function Editor(): JSX.Element {
  const files = useStoreFileTree(s => s.files)
  const selectedFile = useStoreFileTree(s => s.selectedFile)

  const tab = useStoreEditor(s => s.tab)
  const storedTabsIds = useStoreEditor(s => s.storedTabsIds)
  const engine = useStoreEditor(s => s.engine)
  const selectTab = useStoreEditor(s => s.selectTab)
  const createTab = useStoreEditor(s => s.createTab)
  const addTab = useStoreEditor(s => s.addTab)
  const setDialects = useStoreEditor(s => s.setDialects)
  const refreshTab = useStoreEditor(s => s.refreshTab)

  const [isReadyEngine, setIsreadyEngine] = useState(false)

  const handleEngineWorkerMessage = useCallback(
    (e: MessageEvent): void => {
      setIsreadyEngine(true)

      if (e.data.topic === 'dialects') {
        setDialects(e.data.payload.dialects ?? [])
      }

      if (e.data.topic === 'validate') {
        if (tab == null) return

        tab.isValid = e.data.payload

        refreshTab()
      }
    },
    [tab],
  )

  useEffect(() => {
    engine.postMessage({
      topic: 'dialects',
    })
  }, [])

  useEffect(() => {
    files.forEach(file => {
      if (storedTabsIds.includes(file.id)) {
        addTab(createTab(file))
      }
    })
  }, [files])

  useEffect(() => {
    if (selectedFile == null || tab?.file === selectedFile) return

    selectTab(createTab(selectedFile))
  }, [selectedFile])

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
          {tab == null ? <EditorEmpty /> : <EditorMain tab={tab} />}
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
  const files = useStoreFileTree(s => s.files)
  const selectFile = useStoreFileTree(s => s.selectFile)

  const direction = useStoreEditor(s => s.direction)
  const engine = useStoreEditor(s => s.engine)
  const previewTable = useStoreEditor(s => s.previewTable)
  const previewConsole = useStoreEditor(s => s.previewConsole)
  const selectTab = useStoreEditor(s => s.selectTab)
  const createTab = useStoreEditor(s => s.createTab)
  const closeTab = useStoreEditor(s => s.closeTab)
  const refreshTab = useStoreEditor(s => s.refreshTab)
  const setPreviewQuery = useStoreEditor(s => s.setPreviewQuery)
  const setPreviewConsole = useStoreEditor(s => s.setPreviewConsole)
  const setPreviewTable = useStoreEditor(s => s.setPreviewTable)

  const { models, setManuallySelectedColumn } = useLineageFlow()

  const modelExtensions = useSQLMeshModelExtensions(
    tab.file.path,
    model => {
      selectFile(files.get(model.path))
    },
    (model, column) => {
      setManuallySelectedColumn([model, column])
    },
  )

  const updateFileContent = useCallback(
    debounceSync(function updateFileContent(value: string): void {
      if (tab == null) return

      tab.file.content = value

      refreshTab()
    }),
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
      (tab.file.isLocal && [previewTable, previewConsole].some(Boolean)) ||
      (tab.file.isSQLMeshModel && model != null)

    return showPreview ? [70, 30] : [100, 0]
  }, [tab, models, previewConsole, previewTable])

  const keymaps: KeyBinding[] = useMemo(
    () =>
      tab == null
        ? []
        : [
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
                closeTab(tab.file)

                return true
              },
            },
          ],
    [tab?.file],
  )

  useEffect(() => {
    if (tab?.id == null) return

    const model = models.get(tab.file.path)

    if (model == null) {
      engine.postMessage({
        topic: 'dialects',
      })
    } else {
      tab.dialect = model.dialect

      engine.postMessage({
        topic: 'dialect',
        payload: tab.dialect,
      })
    }
  }, [tab.id])

  useEffect(() => {
    if (tab == null) return

    tab.isSaved = isFalse(tab.file.isChanged)

    engine.postMessage({
      topic: 'validate',
      payload: tab.file.content,
    })
  }, [tab.file.fingerprint, tab.file.content])

  useEffect(() => {
    setPreviewQuery(undefined)
    setPreviewTable(undefined)
    setPreviewConsole(undefined)
  }, [tab.id, tab.file.fingerprint])

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
                <CodeEditor.Default
                  key={tab.id}
                  type={EnumFileExtensions.SQL}
                  content={tab.file.content}
                >
                  {({ extensions, content }) => (
                    <CodeEditor
                      extensions={extensions}
                      content={content}
                      keymaps={keymaps}
                      onChange={updateFileContent}
                    />
                  )}
                </CodeEditor.Default>
              )}
              {tab.file.isRemote && (
                <CodeEditor.RemoteFile path={tab.file.path}>
                  {({ file, keymaps: additional }) => (
                    <CodeEditor.SQLMeshDialect
                      type={file.extension}
                      content={file.content}
                    >
                      {({ extensions, content }) => (
                        <CodeEditor
                          extensions={extensions.concat(modelExtensions)}
                          content={content}
                          keymaps={keymaps.concat(additional)}
                          onChange={updateFileContent}
                        />
                      )}
                    </CodeEditor.SQLMeshDialect>
                  )}
                </CodeEditor.RemoteFile>
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
