import { useCallback, useEffect, useMemo, useState } from 'react'
import { Divider } from '../divider/Divider'
import { useStoreFileTree } from '../../../context/fileTree'
import SplitPane from '../splitPane/SplitPane'
import { isFalse, isStringEmptyOrNil } from '../../../utils'
import CodeEditor, {
  useSQLMeshModelExtensions,
  useSQLMeshModelKeymaps,
} from '@components/editor/EditorCode'
import EditorFooter from './EditorFooter'
import EditorTabs from './EditorTabs'
import EditorInspector from './EditorInspector'
import './Editor.css'
import EditorPreview from './EditorPreview'
import { useStoreEditor } from '~/context/editor'
import { useStoreLineage } from '@context/lineage'
import clsx from 'clsx'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { EnumFileExtensions } from '@models/file'
import { useStoreContext } from '@context/context'
import { type KeyBinding } from '@codemirror/view'

export default function Editor(): JSX.Element {
  const models = useStoreContext(s => s.models)

  const files = useStoreFileTree(s => s.files)
  const selectedFile = useStoreFileTree(s => s.selectedFile)
  const selectFile = useStoreFileTree(s => s.selectFile)

  const tab = useStoreEditor(s => s.tab)
  const storedTabsIds = useStoreEditor(s => s.storedTabsIds)
  const engine = useStoreEditor(s => s.engine)
  const previewTable = useStoreEditor(s => s.previewTable)
  const previewConsole = useStoreEditor(s => s.previewConsole)
  const lineage = useStoreEditor(s => s.previewLineage)
  const selectTab = useStoreEditor(s => s.selectTab)
  const createTab = useStoreEditor(s => s.createTab)
  const closeTab = useStoreEditor(s => s.closeTab)
  const addTab = useStoreEditor(s => s.addTab)
  const setDialects = useStoreEditor(s => s.setDialects)
  const refreshTab = useStoreEditor(s => s.refreshTab)

  const clearActiveEdges = useStoreLineage(s => s.clearActiveEdges)

  const modelExtensions = useSQLMeshModelExtensions(
    tab?.file.path,
    lineage,
    model => {
      selectFile(files.get(model.path))
    },
  )

  const modelKeymaps = useSQLMeshModelKeymaps(tab?.file)

  const [isReadyEngine, setIsreadyEngine] = useState(false)
  const [direction, setDirection] = useState<'vertical' | 'horizontal'>(
    'vertical',
  )

  const handleEngineWorkerMessage = useCallback((e: MessageEvent): void => {
    setIsreadyEngine(true)

    if (e.data.topic === 'dialects') {
      setDialects(e.data.payload.dialects ?? [])
    }
  }, [])

  const sizesCodeEditorAndInspector = useMemo(() => {
    const showInspector =
      tab != null &&
      (tab.file.isSQLMeshModel || tab.file.isLocal) &&
      isFalse(isStringEmptyOrNil(tab.file.content))

    return showInspector ? [75, 25] : [100, 0]
  }, [tab])

  const sizesCodeEditorAndPreview = useMemo(() => {
    const showPreview =
      tab != null &&
      ((tab.file.isLocal && [previewTable, previewConsole].some(Boolean)) ||
        tab.file.isSQLMeshModel)

    return showPreview ? [70, 30] : [100, 0]
  }, [tab, previewConsole, previewTable])

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
    [closeTab, selectTab, createTab, tab?.file],
  )

  useEffect(() => {
    engine.postMessage({
      topic: 'dialects',
    })

    engine.addEventListener('message', handleEngineWorkerMessage)

    return () => {
      engine.removeEventListener('message', handleEngineWorkerMessage)
    }
  }, [])

  useEffect(() => {
    files.forEach(file => {
      if (storedTabsIds.includes(file.id)) {
        addTab(createTab(file))
      }
    })
  }, [files])

  useEffect(() => {
    clearActiveEdges()
  }, [tab?.file.fingerprint, tab?.id])

  useEffect(() => {
    if (selectedFile == null || tab?.file === selectedFile) return

    selectTab(createTab(selectedFile))
  }, [selectedFile])

  useEffect(() => {
    if (tab == null) return

    const model = models.get(tab.file.path)

    if (model == null) return

    tab.dialect = model.dialect

    engine.postMessage({
      topic: 'dialect',
      payload: tab.dialect,
    })
  }, [tab])

  function toggleDirection(): void {
    setDirection(direction =>
      direction === 'vertical' ? 'horizontal' : 'vertical',
    )
  }

  function updateFileContent(value: string): void {
    if (tab == null) return

    tab.file.content = value
    tab.isSaved = isFalse(tab.file.isChanged)

    refreshTab()
  }

  return (
    <div className="w-full h-full flex flex-col overflow-hidden">
      {isFalse(isReadyEngine) ? (
        <div className="flex justify-center items-center w-full h-full">
          <Loading className="inline-block ">
            <Spinner className="w-5 h-5 border border-neutral-10 mr-4" />
            <h3 className="text-xl">Starting Editor...</h3>
          </Loading>
        </div>
      ) : (
        <>
          <EditorTabs />
          <Divider />
          {tab == null ? (
            <div className="flex justify-center items-center w-full h-full">
              <div className="p-4 text-center text-theme-darker dark:text-theme-lighter">
                <h2 className="text-3xl">Select File or Add New SQL Tab</h2>
              </div>
            </div>
          ) : (
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
                    className="flex h-full"
                    sizes={sizesCodeEditorAndInspector}
                    minSize={0}
                    snapOffset={0}
                  >
                    <div className="flex flex-col h-full">
                      {tab.file.isLocal && (
                        <CodeEditor.Default
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
                          {({ file }) =>
                            file.isSQLMeshModelSQL ? (
                              <CodeEditor.SQLMeshDialect content={file.content}>
                                {({ extensions, content }) => (
                                  <CodeEditor
                                    extensions={extensions.concat(
                                      modelExtensions,
                                    )}
                                    content={content}
                                    keymaps={keymaps.concat(modelKeymaps)}
                                    onChange={updateFileContent}
                                  />
                                )}
                              </CodeEditor.SQLMeshDialect>
                            ) : (
                              <CodeEditor.Default
                                type={file.extension}
                                content={file.content}
                              >
                                {({ extensions, content }) => (
                                  <CodeEditor
                                    extensions={extensions.concat(
                                      modelExtensions,
                                    )}
                                    content={content}
                                    keymaps={keymaps.concat(modelKeymaps)}
                                    onChange={updateFileContent}
                                  />
                                )}
                              </CodeEditor.Default>
                            )
                          }
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
              <div
                className={clsx(
                  direction === 'vertical' ? 'flex flex-col' : 'flex',
                )}
              >
                <EditorPreview
                  tab={tab}
                  toggleDirection={toggleDirection}
                />
              </div>
            </SplitPane>
          )}
        </>
      )}
    </div>
  )
}
