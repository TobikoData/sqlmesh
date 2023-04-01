import { useEffect, useMemo, useState } from 'react'
import { Tab } from '@headlessui/react'
import clsx from 'clsx'
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { isNil, isTrue } from '~/utils'
import { useStoreEditor } from '~/context/editor'

export const EnumEditorPreviewTabs = {
  Query: 'Query',
  Table: 'Table',
  Console: 'Console',
} as const

export type EditorPreviewTabs = KeyOf<typeof EnumEditorPreviewTabs>
const TABS: EditorPreviewTabs[] = [
  EnumEditorPreviewTabs.Table,
  EnumEditorPreviewTabs.Query,
  EnumEditorPreviewTabs.Console,
]

export default function EditorPreview(): JSX.Element {
  const tab = useStoreEditor(s => s.tab)

  const previewTable = useStoreEditor(s => s.previewTable)
  const previewQuery = useStoreEditor(s => s.previewQuery)
  const previewConsole = useStoreEditor(s => s.previewConsole)

  const [activeTabIndex, setActiveTabIndex] = useState(-1)

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
    setActiveTabIndex(-1)
  }, [previewTable, previewQuery, previewConsole])

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
        'flex flex-col text-prose overflow-auto scrollbar scrollbar--vertical',
      )}
    >
      <Tab.Group
        onChange={setActiveTabIndex}
        selectedIndex={activeTabIndex < 0 ? activeTab : activeTabIndex}
      >
        <Tab.List className="w-full whitespace-nowrap px-2 pt-3">
          <div className="w-full overflow-hidden overflow-x-auto py-1">
            {TABS.map(tabName => (
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
                {(tabName === EnumEditorPreviewTabs.Table ||
                  tabName === EnumEditorPreviewTabs.Query) &&
                  tab.file.content !== previewQuery && (
                    <span
                      title="Outdated Data. Does not match editor query!"
                      className="absolute right-[-0.25rem] top-[-0.25rem] rounded-xl w-2 h-2 bg-warning-500"
                    ></span>
                  )}
                {tabName === EnumEditorPreviewTabs.Console &&
                  previewConsole != null && (
                    <span
                      title="Outdated Data. Does not match editor query!"
                      className="absolute right-[-0.25rem] top-[-0.25rem] rounded-xl w-2 h-2 bg-danger-500"
                    ></span>
                  )}
                {tabName}
              </Tab>
            ))}
          </div>
        </Tab.List>
        <Tab.Panels className="w-full overflow-hidden">
          <Tab.Panel
            className={clsx(
              'w-full h-full overflow-hidden pt-4 relative pl-2',
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
        </Tab.Panels>
      </Tab.Group>
    </div>
  )
}
