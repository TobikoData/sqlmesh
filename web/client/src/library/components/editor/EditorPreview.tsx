import { lazy, useEffect, useMemo, useState } from 'react'
import { Tab } from '@headlessui/react'
import clsx from 'clsx'
import { isArrayEmpty, isNotNil } from '~/utils'
import { type EditorTab, useStoreEditor } from '~/context/editor'
import { ViewColumnsIcon } from '@heroicons/react/24/solid'
import { Button } from '@components/button/Button'
import { EnumSize, EnumVariant } from '~/types/enum'
import { ModelSQLMeshModel } from '@models/sqlmesh-model'
import { useLineageFlow } from '@components/graph/context'
import { EnumFileExtensions } from '@models/file'
import CodeEditor from './EditorCode'
import { EnumRoutes } from '~/routes'
import { useNavigate } from 'react-router-dom'
import { DisplayError } from '@components/report/ReportErrors'
import TableDiff from '@components/tableDiff/TableDiff'
import TabList from '@components/tab/Tab'
import { useSQLMeshModelExtensions } from './hooks'
import Table from '@components/table/Table'

const ModelLineage = lazy(
  async () => await import('@components/graph/ModelLineage'),
)

export const EnumEditorPreviewTabs = {
  Query: 'Query',
  Table: 'Data Preview',
  Console: 'Logs',
  Lineage: 'Lineage',
  Diff: 'Diff',
} as const

export type EditorPreviewTabs = KeyOf<typeof EnumEditorPreviewTabs>

export default function EditorPreview({
  tab,
  className,
}: {
  tab: EditorTab
  className?: string
}): JSX.Element {
  const navigate = useNavigate()

  const { models } = useLineageFlow()

  const direction = useStoreEditor(s => s.direction)
  const previewQuery = useStoreEditor(s => s.previewQuery)
  const previewConsole = useStoreEditor(s => s.previewConsole)
  const previewTable = useStoreEditor(s => s.previewTable)
  const previewDiff = useStoreEditor(s => s.previewDiff)
  const setDirection = useStoreEditor(s => s.setDirection)

  const [activeTabIndex, setActiveTabIndex] = useState(-1)

  const modelExtensions = useSQLMeshModelExtensions(tab.file.path, model => {
    navigate(
      `${EnumRoutes.IdeDocsModels}/${ModelSQLMeshModel.encodeName(model.name)}`,
    )
  })

  const tabs: string[] = useMemo(
    () =>
      [
        isNotNil(previewTable) && EnumEditorPreviewTabs.Table,
        isNotNil(previewConsole) && EnumEditorPreviewTabs.Console,
        isNotNil(previewQuery) && EnumEditorPreviewTabs.Query,
        tab.file.isSQLMeshModel && EnumEditorPreviewTabs.Lineage,
        isNotNil(previewDiff) && EnumEditorPreviewTabs.Diff,
      ].filter(Boolean) as string[],
    [tab.id, previewTable, previewConsole, previewQuery, previewDiff],
  )

  useEffect(() => {
    if (isNotNil(previewConsole)) {
      setActiveTabIndex(tabs.indexOf(EnumEditorPreviewTabs.Console))
    } else if (isNotNil(previewDiff)) {
      setActiveTabIndex(tabs.indexOf(EnumEditorPreviewTabs.Diff))
    } else if (isNotNil(previewTable)) {
      setActiveTabIndex(tabs.indexOf(EnumEditorPreviewTabs.Table))
    } else if (tab.file.isSQLMeshModel) {
      setActiveTabIndex(tabs.indexOf(EnumEditorPreviewTabs.Lineage))
    }
  }, [tabs])

  const model = models.get(tab.file.path)

  return (
    <div
      className={clsx(
        'w-full h-full flex flex-col text-prose overflow-auto hover:scrollbar scrollbar--vertical',
        className,
      )}
    >
      {isArrayEmpty(tabs) ? (
        <div className="flex justify-center items-center w-full h-full">
          <h3 className="text-md">No Data To Preview</h3>
        </div>
      ) : (
        <Tab.Group
          key={tabs.join('-')}
          onChange={setActiveTabIndex}
          selectedIndex={activeTabIndex}
        >
          <TabList list={tabs}>
            <div className="ml-2">
              <Button
                className="!m-0 !py-0.5 px-[0.25rem] border-none"
                variant={EnumVariant.Alternative}
                size={EnumSize.sm}
                onClick={() => {
                  setDirection(
                    direction === 'horizontal' ? 'vertical' : 'horizontal',
                  )
                }}
              >
                <ViewColumnsIcon className="text-primary-500 w-5" />
              </Button>
            </div>
          </TabList>
          <Tab.Panels className="h-full w-full overflow-hidden">
            {isNotNil(previewTable) && (
              <Tab.Panel
                unmount={false}
                className={clsx(
                  'w-full h-full pt-4 relative px-2',
                  'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
                )}
              >
                <Table data={previewTable} />
              </Tab.Panel>
            )}
            {isNotNil(previewConsole) && (
              <Tab.Panel
                unmount={false}
                className={clsx(
                  'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2',
                )}
              >
                <div className="w-full h-full p-2 bg-primary-10 rounded-lg overflow-auto hover:scrollbar scrollbar--horizontal scrollbar--vertical">
                  <DisplayError
                    scope={previewConsole[0]}
                    error={previewConsole[1]}
                  />
                </div>
              </Tab.Panel>
            )}
            {isNotNil(previewQuery) && (
              <Tab.Panel
                unmount={false}
                className="w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2"
              >
                <div className="w-full h-full p-2 bg-primary-10 rounded-lg overflow-auto hover:scrollbar scrollbar--horizontal scrollbar--vertical">
                  <CodeEditor.Default
                    type={EnumFileExtensions.SQL}
                    content={previewQuery ?? ''}
                  >
                    {({ extensions, content }) => (
                      <CodeEditor
                        extensions={extensions.concat(modelExtensions)}
                        content={content}
                        className="text-xs"
                      />
                    )}
                  </CodeEditor.Default>
                </div>
              </Tab.Panel>
            )}
            {isNotNil(model) && (
              <Tab.Panel
                unmount={false}
                className={clsx(
                  'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 py-2',
                )}
              >
                <ModelLineage
                  model={model}
                  fingerprint={tab.file.fingerprint}
                />
              </Tab.Panel>
            )}
            {isNotNil(previewDiff?.row_diff) && (
              <Tab.Panel
                unmount={false}
                className={clsx(
                  'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 py-2',
                )}
              >
                <TableDiff diff={previewDiff} />
              </Tab.Panel>
            )}
          </Tab.Panels>
        </Tab.Group>
      )}
    </div>
  )
}
