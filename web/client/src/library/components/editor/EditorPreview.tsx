import { useEffect, useMemo, useState } from 'react'
import { Tab } from '@headlessui/react'
import clsx from 'clsx'
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { isNil, isTrue } from '~/utils'
import { type EditorTab, useStoreEditor } from '~/context/editor'
import { ViewColumnsIcon } from '@heroicons/react/24/solid'
import { Button } from '@components/button/Button'
import { EnumVariant } from '~/types/enum'
import ModelLineage from '@components/graph/ModelLineage'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { useLineageFlow } from '@components/graph/context'

export const EnumEditorPreviewTabs = {
  Query: 'Query',
  Table: 'Table',
  Console: 'Console',
  Lineage: 'Lineage',
} as const

export type EditorPreviewTabs = KeyOf<typeof EnumEditorPreviewTabs>

export default function EditorPreview({
  tab,
  toggleDirection,
}: {
  tab: EditorTab
  toggleDirection: () => void
}): JSX.Element {
  const { models } = useLineageFlow()

  const previewQuery = useStoreEditor(s => s.previewQuery)
  const previewConsole = useStoreEditor(s => s.previewConsole)
  const previewTable = useStoreEditor(s => s.previewTable)
  const setPreviewQuery = useStoreEditor(s => s.setPreviewQuery)
  const setPreviewConsole = useStoreEditor(s => s.setPreviewConsole)
  const setPreviewTable = useStoreEditor(s => s.setPreviewTable)

  const [activeTabIndex, setActiveTabIndex] = useState(-1)
  const [model, setModel] = useState<ModelSQLMeshModel>()

  const tabs = useMemo(() => {
    if (tab.file.isLocal)
      return [
        EnumEditorPreviewTabs.Table,
        EnumEditorPreviewTabs.Query,
        EnumEditorPreviewTabs.Console,
      ]
    if (tab.file.isSQLMeshModel)
      return [
        EnumEditorPreviewTabs.Table,
        EnumEditorPreviewTabs.Query,
        EnumEditorPreviewTabs.Console,
        EnumEditorPreviewTabs.Lineage,
      ]

    return [EnumEditorPreviewTabs.Console]
  }, [tab.id, tab.file.fingerprint])

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

  const activeTab = useMemo(
    () =>
      [
        Boolean(previewTable),
        Boolean(previewQuery),
        Boolean(previewConsole),
      ].findIndex(isTrue),
    [previewTable, previewQuery, previewConsole],
  )

  useEffect(() => {
    setPreviewQuery(undefined)
    setPreviewTable(undefined)
    setPreviewConsole(undefined)
  }, [tab.id])

  useEffect(() => {
    setActiveTabIndex(tab.file.isSQLMeshModel ? 3 : -1)
  }, [previewTable, previewQuery, previewConsole, tab])

  useEffect(() => {
    setModel(models.get(tab.file.path))
  }, [models])

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  function isDisabledPreviewTable(tabName: string): boolean {
    return tabName === EnumEditorPreviewTabs.Table && isNil(previewTable)
  }

  function isDisabledPreviewConsole(tabName: string): boolean {
    return tabName === EnumEditorPreviewTabs.Console && isNil(previewConsole)
  }

  function isDisabledPreviewQuery(tabName: string): boolean {
    return tabName === EnumEditorPreviewTabs.Query && isNil(previewQuery)
  }

  return (
    <div
      className={clsx(
        'w-full h-full flex flex-col text-prose overflow-auto scrollbar scrollbar--vertical',
      )}
    >
      <Tab.Group
        onChange={setActiveTabIndex}
        selectedIndex={activeTabIndex < 0 ? activeTab : activeTabIndex}
      >
        <Tab.List className="w-full whitespace-nowrap px-2 pt-3 flex justigy-between items-center">
          <div className="w-full overflow-hidden overflow-x-auto py-1 scrollbar scrollbar--horizontal">
            {tabs.map(tabName => (
              <Tab
                key={tabName}
                disabled={
                  isDisabledPreviewTable(tabName) ||
                  isDisabledPreviewConsole(tabName) ||
                  isDisabledPreviewQuery(tabName)
                }
                className={({ selected }) =>
                  clsx(
                    'inline-block text-sm font-medium px-3 py-1 mr-2 last-child:mr-0 rounded-md relative',
                    isDisabledPreviewTable(tabName) ||
                      isDisabledPreviewConsole(tabName) ||
                      isDisabledPreviewQuery(tabName)
                      ? 'cursor-not-allowed opacity-50 bg-neutral-10'
                      : selected
                      ? 'bg-secondary-500 text-secondary-100 cursor-default'
                      : 'bg-secondary-10 cursor-pointer',
                  )
                }
              >
                {tabName === EnumEditorPreviewTabs.Console &&
                  previewConsole != null && (
                    <span className="absolute right-[-0.25rem] top-[-0.25rem] rounded-xl w-2 h-2 bg-danger-500"></span>
                  )}
                {tabName}
              </Tab>
            ))}
          </div>
          <div className="ml-2">
            <Button
              className="m-0 py-0.5 px-[0.25rem] border-none"
              variant={EnumVariant.Alternative}
              onClick={toggleDirection}
            >
              <ViewColumnsIcon className="text-primary-500 w-6" />
            </Button>
          </div>
        </Tab.List>
        <Tab.Panels className="h-full w-full overflow-hidden">
          <Tab.Panel
            className={clsx(
              'w-full h-full pt-4 relative px-2',
              'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
            )}
          >
            {table != null && (
              <div className="w-full h-full overflow-auto scrollbar scrollbar--horizontal scrollbar--vertical">
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
          <Tab.Panel
            className={clsx(
              'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2',
            )}
          >
            <pre className="w-full h-full p-4 bg-primary-10 rounded-lg overflow-auto scrollbar scrollbar--horizontal scrollbar--vertical text-xs">
              {previewQuery}
            </pre>
          </Tab.Panel>
          <Tab.Panel
            className={clsx(
              'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2',
            )}
          >
            <pre className="w-full h-full p-4 bg-primary-10 rounded-lg text-danger-500 overflow-auto text-xs scrollbar scrollbar--horizontal scrollbar--vertical">
              {previewConsole}
            </pre>
          </Tab.Panel>
          <Tab.Panel
            className={clsx(
              'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2',
            )}
          >
            {model == null ? (
              <div>Model Does Not Exist</div>
            ) : (
              <ModelLineage
                model={model}
                fingerprint={tab.file.fingerprint}
              />
            )}
          </Tab.Panel>
        </Tab.Panels>
      </Tab.Group>
    </div>
  )
}
