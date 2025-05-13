import { Fragment, Suspense, lazy, useEffect, useMemo, useState } from 'react'
import { TabGroup, TabPanel, TabPanels } from '@headlessui/react'
import clsx from 'clsx'
import { includes, isArrayEmpty, isFalse, isNotNil } from '~/utils'
import { type EditorTab, useStoreEditor } from '~/context/editor'
import { ViewColumnsIcon } from '@heroicons/react/24/solid'
import { Button } from '@components/button/Button'
import { EnumSize, EnumVariant } from '~/types/enum'
import { EnumFileExtensions } from '@models/file'
import { CodeEditorDefault } from './EditorCode'
import { EnumRoutes } from '~/routes'
import { useNavigate } from 'react-router'
import TableDiff from '@components/tableDiff/TableDiff'
import TabList from '@components/tab/Tab'
import { useSQLMeshModelExtensions } from './hooks'
import Table from '@components/table/Table'
import { useStoreContext } from '@context/context'
import { DisplayError } from '@components/report/ReportErrors'
import {
  EnumErrorKey,
  useNotificationCenter,
} from '~/library/pages/root/context/notificationCenter'
import LoadingSegment from '@components/loading/LoadingSegment'

const ModelLineage = lazy(
  async () => await import('@components/graph/ModelLineage'),
)

export const EnumEditorPreviewTabs = {
  Query: 'Query',
  Table: 'Data Preview',
  Console: 'Logs',
  Lineage: 'Lineage',
  Diff: 'Diff',
  Errors: 'Errors',
} as const

export type EditorPreviewTabs = KeyOf<typeof EnumEditorPreviewTabs>

export default function EditorPreview({
  tab,
  className,
}: {
  tab: EditorTab
  className?: string
}): JSX.Element {
  const { errors, removeError } = useNotificationCenter()
  const navigate = useNavigate()

  const models = useStoreContext(s => s.models)
  const isModel = useStoreContext(s => s.isModel)

  const direction = useStoreEditor(s => s.direction)
  const previewQuery = useStoreEditor(s => s.previewQuery)
  const previewTable = useStoreEditor(s => s.previewTable)
  const previewDiff = useStoreEditor(s => s.previewDiff)
  const setDirection = useStoreEditor(s => s.setDirection)

  const [activeTabIndex, setActiveTabIndex] = useState(-1)

  const modelExtensions = useSQLMeshModelExtensions(tab.file.path, model => {
    navigate(`${EnumRoutes.DataCatalogModels}/${model.name}`)
  })

  const model = models.get(tab.file.path)
  const showLineage =
    isFalse(tab.file.isEmpty) && isNotNil(model) && isModel(tab.file.path)
  const showErrors = errors.size > 0

  const tabs: string[] = useMemo(
    () =>
      [
        isNotNil(previewTable) && EnumEditorPreviewTabs.Table,
        isNotNil(previewQuery) &&
          tab.file.isRemote &&
          EnumEditorPreviewTabs.Query,
        showLineage && EnumEditorPreviewTabs.Lineage,
        isNotNil(previewDiff) && EnumEditorPreviewTabs.Diff,
        showErrors && EnumEditorPreviewTabs.Errors,
      ].filter(Boolean) as string[],
    [
      tab.id,
      previewTable,
      previewQuery,
      previewDiff,
      showLineage,
      errors,
      showErrors,
    ],
  )

  useEffect(() => {
    if (isNotNil(previewTable)) {
      setActiveTabIndex(tabs.indexOf(EnumEditorPreviewTabs.Table))
    } else {
      setActiveTabIndex(0)
    }
  }, [previewTable])

  useEffect(() => {
    if (isNotNil(previewDiff)) {
      setActiveTabIndex(tabs.indexOf(EnumEditorPreviewTabs.Diff))
    } else {
      setActiveTabIndex(0)
    }
  }, [previewDiff])

  useEffect(() => {
    if (isNotNil(showLineage)) {
      setActiveTabIndex(tabs.indexOf(EnumEditorPreviewTabs.Lineage))
    } else {
      setActiveTabIndex(0)
    }
  }, [showLineage])

  useEffect(() => {
    if (showErrors) {
      setActiveTabIndex(tabs.indexOf(EnumEditorPreviewTabs.Errors))
    } else {
      setActiveTabIndex(0)
    }
  }, [showErrors])

  useEffect(() => {
    for (const error of errors) {
      if (
        includes(
          [
            EnumErrorKey.Fetchdf,
            EnumErrorKey.EvaluateModel,
            EnumErrorKey.RenderQuery,
            EnumErrorKey.ColumnLineage,
            EnumErrorKey.ModelLineage,
            EnumErrorKey.TableDiff,
            EnumErrorKey.Table,
            EnumErrorKey.SaveFile,
          ],
          error.key,
        )
      ) {
        removeError(error)
      }
    }
  }, [previewTable, previewDiff, previewQuery, showLineage])

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
        <TabGroup
          as={Fragment}
          key={tab.id}
          onChange={setActiveTabIndex}
          selectedIndex={activeTabIndex}
        >
          <TabList
            key={tabs.join('-')}
            list={tabs}
          >
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
                <ViewColumnsIcon
                  aria-label={
                    direction === 'horizontal'
                      ? 'Use vertical layout'
                      : 'Use horizontal layout'
                  }
                  className="text-primary-500 w-5"
                />
              </Button>
            </div>
          </TabList>
          <TabPanels className="h-full w-full overflow-hidden">
            {isNotNil(previewTable) && (
              <TabPanel
                unmount={false}
                className={clsx(
                  'w-full h-full pt-4 relative px-2',
                  'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
                )}
              >
                <Table data={previewTable} />
              </TabPanel>
            )}
            {isNotNil(previewQuery) && tab.file.isRemote && (
              <TabPanel
                unmount={false}
                className="w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2"
              >
                <div className="w-full h-full p-2 bg-primary-10 rounded-lg overflow-auto hover:scrollbar scrollbar--horizontal scrollbar--vertical">
                  <CodeEditorDefault
                    type={EnumFileExtensions.SQL}
                    content={previewQuery ?? ''}
                    extensions={modelExtensions}
                    className="text-xs"
                  />
                </div>
              </TabPanel>
            )}
            {showLineage && (
              <TabPanel
                as="div"
                unmount={false}
                className={clsx(
                  'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
                )}
              >
                <Suspense
                  fallback={
                    <LoadingSegment>Loading Model page...</LoadingSegment>
                  }
                >
                  <ModelLineage model={model} />
                </Suspense>
              </TabPanel>
            )}
            {isNotNil(previewDiff?.row_diff) && (
              <TabPanel
                as="div"
                unmount={false}
                className={clsx(
                  'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 py-2',
                )}
              >
                <TableDiff
                  key={tab.id}
                  diff={previewDiff}
                />
              </TabPanel>
            )}
            {showErrors && (
              <TabPanel
                unmount={false}
                className="w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 py-2"
              >
                <ul className="w-full h-full p-2 overflow-auto hover:scrollbar scrollbar--vertical scrollbar--horizontal">
                  {Array.from(errors)
                    .reverse()
                    .map(error => (
                      <li
                        key={error.id}
                        className="bg-danger-10 mb-4 last:m-0 p-2 rounded-md"
                      >
                        <DisplayError
                          scope={error.key}
                          error={error}
                        />
                      </li>
                    ))}
                </ul>
              </TabPanel>
            )}
          </TabPanels>
        </TabGroup>
      )}
    </div>
  )
}
