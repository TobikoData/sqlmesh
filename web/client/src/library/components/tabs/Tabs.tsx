import { useEffect, useMemo, useState } from 'react'
import { Tab } from '@headlessui/react'
import clsx from 'clsx'
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { useStoreEditor } from '../../../context/editor'
import { useStoreFileTree } from '../../../context/fileTree'
import { isNil, isTrue } from '~/utils'

const TABS = ['Table', 'Query Preview', 'Terminal Output']

interface PropsTabs extends React.HTMLAttributes<HTMLElement> {}

export default function Tabs({ className }: PropsTabs): JSX.Element {
  const tabTableContent = useStoreEditor(s => s.tabTableContent)
  const tabQueryPreviewContent = useStoreEditor(s => s.tabQueryPreviewContent)
  const tabTerminalContent = useStoreEditor(s => s.tabTerminalContent)
  const activeFile = useStoreFileTree(s => s.activeFile)

  const [activeTabIndex, setActiveTabIndex] = useState(-1)

  const [headers, data] = useMemo(
    () =>
      tabTableContent == null
        ? [[], []]
        : [tabTableContent[0] ?? [], tabTableContent[1] ?? []],
    [tabTableContent],
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
        Boolean(tabTableContent),
        Boolean(tabQueryPreviewContent),
        Boolean(tabTerminalContent),
      ].findIndex(isTrue),
    [tabTableContent, tabQueryPreviewContent, tabTerminalContent],
  )

  useEffect(() => {
    setActiveTabIndex(-1)
  }, [tabTableContent, tabQueryPreviewContent, tabTerminalContent])

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  function isDisabledTabTable(tabName: string): boolean {
    return tabName === 'Table' && isNil(tabTableContent)
  }

  function isDisabledTabTerminal(tabName: string): boolean {
    return tabName === 'Terminal Output' && isNil(tabTerminalContent)
  }

  function isDisabledTabQueryPreview(tabName: string): boolean {
    return tabName === 'Query Preview' && isNil(tabQueryPreviewContent)
  }

  return (
    <div className={clsx('flex flex-col overflow-hidden', className)}>
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
                  isDisabledTabTable(tabName) ||
                  isDisabledTabTerminal(tabName) ||
                  isDisabledTabQueryPreview(tabName)
                }
                className={({ selected }) =>
                  clsx(
                    'inline-block text-sm font-medium px-3 py-1 mr-2 last-child:mr-0 rounded-md relative',
                    isDisabledTabTable(tabName) ||
                      isDisabledTabTerminal(tabName) ||
                      isDisabledTabQueryPreview(tabName)
                      ? 'text-neutral-400 cursor-not-allowed'
                      : selected
                      ? 'bg-secondary-100 text-secondary-500 cursor-default'
                      : 'text-neutral-900 hover:bg-light/[0.12] hover:text-neutral-500 cursor-pointer',
                  )
                }
              >
                {(tabName === 'Table' || tabName === 'Query Preview') &&
                  activeFile?.content !== tabQueryPreviewContent && (
                    <span
                      title="Outdated Data. Does not match editor query!"
                      className="absolute right-[-0.25rem] top-[-0.25rem] rounded-xl w-2 h-2 bg-warning-500"
                    ></span>
                  )}
                {tabName === 'Terminal Output' &&
                  tabTerminalContent != null && (
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
              <div className="w-full h-full overflow-hidden overflow-y-auto">
                <table className="w-full h-full">
                  <thead className="sticky top-0 bg-secondary-100">
                    {table.getHeaderGroups().map(headerGroup => (
                      <tr key={headerGroup.id}>
                        {headerGroup.headers.map(header => (
                          <th
                            key={header.id}
                            className="px-2 text-sm text-left border-b-2 border-neutral-100"
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
                            className="px-2 py-1 text-sm text-left border-b border-neutral-100"
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
                  <tfoot className="text-left sticky bottom-0 bg-neutral-100">
                    {table.getFooterGroups().map(footerGroup => (
                      <tr key={footerGroup.id}>
                        {footerGroup.headers.map(header => (
                          <th
                            key={header.id}
                            className="px-1 px-3"
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
            <pre className="w-full h-full p-4 bg-secondary-100 rounded-lg overflow-auto scrollbar scrollbar--vertical">
              {tabQueryPreviewContent}
            </pre>
          </Tab.Panel>
          <Tab.Panel
            className={clsx(
              'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2',
            )}
          >
            <pre className="w-full h-full p-4 bg-secondary-100 rounded-lg text-danger-500 overflow-auto text-xs  scrollbar scrollbar--vertical">
              {tabTerminalContent}
            </pre>
          </Tab.Panel>
        </Tab.Panels>
      </Tab.Group>
    </div>
  )
}
