import { useCallback, useEffect, useMemo, useState } from 'react'
import { Divider } from '../divider/Divider'
import { useStoreProject } from '../../../context/project'
import SplitPane from '../splitPane/SplitPane'
import { isFalse, isNil, isNotNil, isStringEmptyOrNil } from '../../../utils'
import EditorFooter from './EditorFooter'
import EditorTabs from './EditorTabs'
import EditorInspector from './EditorInspector'
import EditorPreview from './EditorPreview'
import { type EditorTab, useStoreEditor } from '~/context/editor'
import clsx from 'clsx'
import { EnumFileExtensions, ModelFile } from '@models/file'
import { useLineageFlow } from '@components/graph/context'
import { CodeEditorRemoteFile, CodeEditorDefault } from './EditorCode'
import { useDefaultKeymapsEditorTab, useSQLMeshModelExtensions } from './hooks'
import { useApiFetchdf } from '@api/index'
import { getTableDataFromArrowStreamResult } from '@components/table/help'
import { type Table } from 'apache-arrow'
import { type KeyBinding } from '@codemirror/view'
import { useStoreContext } from '@context/context'
import { useIDE } from '~/library/pages/ide/context'
import { ModelDirectory } from '@models/directory'

function Editor(): JSX.Element {
  const tab = useStoreEditor(s => s.tab)

  return (
    <div className="w-full h-full flex flex-col overflow-hidden">
      <EditorTabs />
      <Divider />
      {isNil(tab) ? <EditorEmpty /> : <EditorMain tab={tab} />}
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

function EditorMain({ tab }: { tab: EditorTab }): JSX.Element {
  const { errors } = useIDE()
  const environment = useStoreContext(s => s.environment)
  const models = useStoreContext(s => s.models)
  const isModel = useStoreContext(s => s.isModel)

  const files = useStoreProject(s => s.files)
  const selectedFile = useStoreProject(s => s.selectedFile)
  const setSelectedFile = useStoreProject(s => s.setSelectedFile)

  const tabs = useStoreEditor(s => s.tabs)
  const direction = useStoreEditor(s => s.direction)
  const engine = useStoreEditor(s => s.engine)
  const previewTable = useStoreEditor(s => s.previewTable)
  const previewDiff = useStoreEditor(s => s.previewDiff)
  const refreshTab = useStoreEditor(s => s.refreshTab)
  const setPreviewQuery = useStoreEditor(s => s.setPreviewQuery)
  const setPreviewTable = useStoreEditor(s => s.setPreviewTable)
  const setPreviewDiff = useStoreEditor(s => s.setPreviewDiff)
  const setDialects = useStoreEditor(s => s.setDialects)
  const replaceTab = useStoreEditor(s => s.replaceTab)
  const createTab = useStoreEditor(s => s.createTab)
  const selectTab = useStoreEditor(s => s.selectTab)
  const addTab = useStoreEditor(s => s.addTab)

  const { setManuallySelectedColumn } = useLineageFlow()

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

  const [isOpenInspector, setIsOpenInspector] = useState(false)

  const customSQLKeymaps = useMemo(() => {
    return [
      ...defaultKeymapsEditorTab,
      {
        key: 'Ctrl-Enter',
        preventDefault: true,
        run() {
          sendQuery()

          return true
        },
      },
    ] as KeyBinding[]
  }, [defaultKeymapsEditorTab])

  const handleEngineWorkerMessage = useCallback(
    (e: MessageEvent): void => {
      if (e.data.topic === 'dialects') {
        const model = models.get(tab.file.path)

        tab.dialect = model?.dialect ?? ''

        setDialects(e.data.payload)
        refreshTab(tab)
      }

      if (e.data.topic === 'format') {
        if (isStringEmptyOrNil(e.data.payload)) return

        tab.file.content = e.data.payload

        refreshTab(tab)
      }
    },
    [tab.id],
  )

  const updateFileContent = useCallback(
    function updateFileContent(value: string): void {
      tab.file.content = value
      tab.isSaved = isFalse(tab.file.isChanged)

      refreshTab(tab)
    },
    [tab.id],
  )

  useEffect(() => {
    engine.addEventListener('message', handleEngineWorkerMessage)

    setIsOpenInspector(false)

    if (isNil(selectedFile)) {
      setSelectedFile(tab?.file)
    }

    return () => {
      engine.removeEventListener('message', handleEngineWorkerMessage)
    }
  }, [tab.id])

  useEffect(() => {
    setPreviewQuery(undefined)
    setPreviewTable(undefined)
    setPreviewDiff(undefined)
  }, [tab.id, tab.file.fingerprint])

  useEffect(() => {
    if (
      isNil(selectedFile) ||
      tab?.file === selectedFile ||
      selectedFile instanceof ModelDirectory
    )
      return

    const newTab = createTab(selectedFile)
    const shouldReplaceTab =
      isNotNil(tab) &&
      tab.file instanceof ModelFile &&
      isFalse(tab.file.isChanged) &&
      tab.file.isRemote &&
      isFalse(tabs.has(selectedFile))

    if (shouldReplaceTab) {
      replaceTab(tab, newTab)
    } else {
      addTab(newTab)
    }

    selectTab(newTab)
  }, [selectedFile])

  useEffect(() => {
    setPreviewDiff(undefined)
  }, [environment])

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

  function getPaneSizesPreview(): number[] {
    const model = models.get(tab.file.path)
    const showLineage =
      isFalse(tab.file.isEmpty) && isNotNil(model) && isModel(tab.file.path)
    const showPreview =
      (tab.file.isLocal && [previewTable, previewDiff].some(Boolean)) ||
      showLineage ||
      errors.size > 0

    return showPreview ? [70, 30] : [100, 0]
  }

  function getPaneSizesInspector(): number[] {
    const model = models.get(tab?.file.path)
    const showInspector =
      isOpenInspector &&
      ((isNotNil(model) && isModel(tab.file.path)) || tab.file.isLocal) &&
      isFalse(isStringEmptyOrNil(tab.file.content))

    return showInspector ? [70, 30] : [100, 0]
  }

  const EDITOR_PANE_MIN_WIDTH = 36
  const INSPECTOR_PANE_WIDTH_THRESHOLD = 2

  return (
    <SplitPane
      key={direction}
      className={clsx(
        'w-full h-full overflow-hidden',
        direction === 'vertical' ? 'flex flex-col' : 'flex',
      )}
      sizes={getPaneSizesPreview()}
      direction={direction}
      minSize={[320, 0]}
      snapOffset={0}
    >
      <div
        className={clsx(
          'flex flex-col',
          direction === 'vertical' ? 'w-full ' : 'h-full',
        )}
      >
        <SplitPane
          key={tab.id}
          className="flex h-full overflow-hidden"
          sizes={getPaneSizesInspector()}
          minSize={[320, EDITOR_PANE_MIN_WIDTH]}
          snapOffset={EDITOR_PANE_MIN_WIDTH}
          handleDrag={(sizes, el) => {
            const containerWidth = el.parent.getBoundingClientRect().width
            const inspectorPaneWidth = (containerWidth * (sizes[1] ?? 0)) / 100

            setIsOpenInspector(
              inspectorPaneWidth >=
                EDITOR_PANE_MIN_WIDTH + INSPECTOR_PANE_WIDTH_THRESHOLD,
            )
          }}
        >
          <div className="flex flex-col h-full">
            {tab.file.isLocal && (
              <CodeEditorDefault
                type={EnumFileExtensions.SQL}
                dialect={tab.dialect}
                keymaps={customSQLKeymaps}
                content={tab.file.content}
                onChange={updateFileContent}
              />
            )}
            {tab.file.isRemote && (
              <CodeEditorRemoteFile
                keymaps={defaultKeymapsEditorTab}
                path={tab.file.path}
              >
                {({ file, keymaps }) => (
                  <CodeEditorDefault
                    type={file.extension}
                    dialect={tab.dialect}
                    extensions={modelExtensions}
                    keymaps={keymaps}
                    content={file.content}
                    onChange={updateFileContent}
                  />
                )}
              </CodeEditorRemoteFile>
            )}
          </div>
          <div className="flex flex-col h-full">
            <EditorInspector
              tab={tab}
              toggle={() => setIsOpenInspector(s => !s)}
              isOpen={isOpenInspector}
            />
          </div>
        </SplitPane>
        <Divider />
        <EditorFooter
          key={tab.file.fingerprint}
          tab={tab}
        />
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
