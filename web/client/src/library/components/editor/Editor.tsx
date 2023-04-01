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
    if (selectedFile == null) return

    selectTab(createTab(selectedFile))
  }, [selectedFile])

  function getSizesCodeEditorAndPreview(): [number, number] {
    const showPreview =
      (tab.file.isSQLMeshModel || tab.file.isLocal) &&
      [previewTable, previewConsole].some(Boolean)

    return showPreview ? [80, 20] : [100, 0]
  }

  function getSizesCodeEditorAndInspector(): [number, number] {
    const showInspector =
      (tab.file.isSQLMeshModel || tab.file.isLocal) &&
      isFalse(isStringEmptyOrNil(tab.file.content))

    return showInspector ? [75, 25] : [100, 0]
  }

  return (
    <SplitPane
      sizes={sizesCodeEditorAndPreview}
      direction="vertical"
      minSize={0}
      snapOffset={0}
    >
      <div className="flex flex-col overflow-hidden">
        {isReadyEngine && (
          <>
            <EditorTabs />
            <Divider />
            <div className="flex flex-col h-full overflow-hidden">
              <SplitPane
                className="flex h-full"
                sizes={sizesCodeEditorAndInspector}
                minSize={0}
                snapOffset={0}
              >
                <div className="flex flex-col h-full">
                  <CodeEditor />
                </div>
                <div className="flex flex-col h-full">
                  <EditorInspector />
                </div>
              </SplitPane>
            </div>
            <Divider />
            <div className="px-2 flex justify-between items-center min-h-[2rem]">
              <EditorFooter />
            </div>
          </>
        )}
      </div>
      <div className="w-full">
        <EditorPreview />
      </div>
    </SplitPane>
  )
}
