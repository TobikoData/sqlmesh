import { lazy, useEffect, useMemo, useState } from 'react'
import { Tab } from '@headlessui/react'
import clsx from 'clsx'
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { isArrayEmpty } from '~/utils'
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
import TableDiff from '@components/table/TableDiff'
import TabList from '@components/tab/Tab'
import { useSQLMeshModelExtensions } from './hooks'

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
        previewTable != null && EnumEditorPreviewTabs.Table,
        previewConsole != null && EnumEditorPreviewTabs.Console,
        previewQuery != null && EnumEditorPreviewTabs.Query,
        tab.file.isSQLMeshModel && EnumEditorPreviewTabs.Lineage,
        previewDiff != null && EnumEditorPreviewTabs.Diff,
      ].filter(Boolean) as string[],
    [tab.id, previewTable, previewConsole, previewQuery, previewDiff],
  )

  const [headers, data] = useMemo(
    () =>
      previewTable == null
        ? [[], []]
        : [previewTable[0] ?? [], previewTable[1] ?? []],
    [previewTable],
  )

  const columns = useMemo(
    () =>
      headers.map((accessorKey: string) => ({
        accessorKey,
      })),
    [headers],
  )

  useEffect(() => {
    if (previewConsole != null) {
      setActiveTabIndex(tabs.indexOf(EnumEditorPreviewTabs.Console))
    } else if (previewDiff != null) {
      setActiveTabIndex(tabs.indexOf(EnumEditorPreviewTabs.Diff))
    } else if (previewTable != null) {
      setActiveTabIndex(tabs.indexOf(EnumEditorPreviewTabs.Table))
    } else if (tab.file.isSQLMeshModel) {
      setActiveTabIndex(tabs.indexOf(EnumEditorPreviewTabs.Lineage))
    }
  }, [tabs])

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })
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
            {previewTable != null && (
              <Tab.Panel
                unmount={false}
                className={clsx(
                  'w-full h-full pt-4 relative px-2',
                  'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
                )}
              >
                {table != null && (
                  <div className="w-full h-full overflow-auto hover:scrollbar scrollbar--horizontal scrollbar--vertical">
                    <table className="w-full h-full">
                      <thead className="sticky top-0 bg-theme">
                        {table.getHeaderGroups().map(headerGroup => (
                          <tr key={headerGroup.id}>
                            {headerGroup.headers.map(header => (
                              <th
                                key={header.id}
                                className="px-2 text-sm text-left border-b-2 border-neutral-50"
                              >
                                {header.isPlaceholder
                                  ? null
                                  : flexRender(
                                      header.column.columnDef.header,
                                      header.getContext(),
                                    )}
                              </th>
                            ))}
                          </tr>
                        ))}
                      </thead>
                      <tbody>
                        {table.getRowModel().rows.map(row => (
                          <tr key={row.id}>
                            {row.getVisibleCells().map(cell => (
                              <td
                                key={cell.id}
                                className="px-2 py-1 text-sm text-left border-b border-neutral-50 whitespace-nowrap"
                              >
                                {flexRender(
                                  cell.column.columnDef.cell,
                                  cell.getContext(),
                                )}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                      <tfoot className="text-left sticky bottom-0 bg-neutral-50">
                        {table.getFooterGroups().map(footerGroup => (
                          <tr key={footerGroup.id}>
                            {footerGroup.headers.map(header => (
                              <th
                                key={header.id}
                                className="px-3"
                              >
                                {header.isPlaceholder
                                  ? null
                                  : flexRender(
                                      header.column.columnDef.footer,
                                      header.getContext(),
                                    )}
                              </th>
                            ))}
                          </tr>
                        ))}
                      </tfoot>
                    </table>
                  </div>
                )}
              </Tab.Panel>
            )}
            {previewConsole != null && (
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
            {previewQuery != null && (
              <Tab.Panel
                unmount={false}
                className={clsx(
                  'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2',
                )}
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
            {model != null && (
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
            <Tab.Panel
              unmount={false}
              className={clsx(
                'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 py-2',
              )}
            >
              {previewDiff?.row_diff != null && (
                <TableDiff diff={previewDiff} />
              )}
            </Tab.Panel>
          </Tab.Panels>
        </Tab.Group>
      )}
    </div>
  )
}
