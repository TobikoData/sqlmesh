import { useCallback, useEffect, useMemo, useState } from 'react'
import { Divider } from '../divider/Divider'
import { useStoreFileTree } from '../../../context/fileTree'
import SplitPane from '../splitPane/SplitPane'
import { isFalse, isStringEmptyOrNil } from '../../../utils'
import CodeEditor from './EditorCode'
import EditorFooter from './EditorFooter'
import EditorTabs from './EditorTabs'
import EditorInspector from './EditorInspector'
import './Editor.css'
import EditorPreview from './EditorPreview'
import { useStoreEditor } from '~/context/editor'
import clsx from 'clsx'

export default function Editor(): JSX.Element {
  const files = useStoreFileTree(s => s.files)
  const selectedFile = useStoreFileTree(s => s.selectedFile)

  const tab = useStoreEditor(s => s.tab)
  const storedTabsIds = useStoreEditor(s => s.storedTabsIds)
  const engine = useStoreEditor(s => s.engine)
  const previewTable = useStoreEditor(s => s.previewTable)
  const previewConsole = useStoreEditor(s => s.previewConsole)
  const selectTab = useStoreEditor(s => s.selectTab)
  const createTab = useStoreEditor(s => s.createTab)
  const addTab = useStoreEditor(s => s.addTab)
  const setDialects = useStoreEditor(s => s.setDialects)
  const setPreviewQuery = useStoreEditor(s => s.setPreviewQuery)
  const setPreviewConsole = useStoreEditor(s => s.setPreviewConsole)
  const setPreviewTable = useStoreEditor(s => s.setPreviewTable)

  const [isReadyEngine, setIsreadyEngine] = useState(false)
  const [direction, setDirection] = useState<'vertical' | 'horizontal'>(
    'vertical',
  )

  const handleEngineWorkerMessage = useCallback((e: MessageEvent): void => {
    if (e.data.topic === 'init') {
      setIsreadyEngine(true)
    }

    if (e.data.topic === 'dialects') {
      setDialects(e.data.payload.dialects ?? [])
    }
  }, [])

  const sizesCodeEditorAndInspector = useMemo(
    () => getSizesCodeEditorAndInspector(),
    [tab],
  )

  const sizesCodeEditorAndPreview = useMemo(
    () => getSizesCodeEditorAndPreview(),
    [tab, previewConsole, previewTable],
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
    setPreviewQuery(undefined)
    setPreviewTable(undefined)
    setPreviewConsole(undefined)
  }, [tab])

  useEffect(() => {
    if (selectedFile == null || tab?.file === selectedFile) return

    selectTab(createTab(selectedFile))
  }, [selectedFile])

  function getSizesCodeEditorAndPreview(): [number, number] {
    const showPreview =
      tab != null &&
      ((tab.file.isLocal && [previewTable, previewConsole].some(Boolean)) ||
        tab.file.isSQLMeshModel)
    return showPreview ? [80, 20] : [100, 0]
  }

  function getSizesCodeEditorAndInspector(): [number, number] {
    const showInspector =
      tab != null &&
      (tab.file.isSQLMeshModel || tab.file.isLocal) &&
      isFalse(isStringEmptyOrNil(tab.file.content))

    return showInspector ? [75, 25] : [100, 0]
  }

  function toggleDirection(): void {
    setDirection(direction =>
      direction === 'vertical' ? 'horizontal' : 'vertical',
    )
  }

  return (
    <div className="w-full h-full flex flex-col overflow-hidden">
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
          <div className="flex flex-col h-full overflow-hidden">
            {isReadyEngine && (
              <>
                <div className="flex flex-col h-full overflow-hidden">
                  <SplitPane
                    className="flex h-full"
                    sizes={sizesCodeEditorAndInspector}
                    minSize={0}
                    snapOffset={0}
                  >
                    <div className="flex flex-col h-full">
                      <CodeEditor tab={tab} />
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
              </>
            )}
          </div>
          <div className="w-full">
            {tab != null && (
              <EditorPreview
                tab={tab}
                toggleDirection={toggleDirection}
              />
            )}
          </div>
        </SplitPane>
      )}
    </div>
  )
}
