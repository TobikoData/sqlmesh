import { useMemo } from 'react'
import { Tab } from '@headlessui/react'
import clsx from 'clsx'
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { useStoreEditor } from '../../../context/editor'
import { useStoreFileTree } from '../../../context/fileTree'
import { isNil } from '~/utils'

const TABS = ['Table', 'Query Preview', 'Terminal Output']

interface PropsTabs extends React.HTMLAttributes<HTMLElement> {}

export default function Tabs({ className }: PropsTabs): JSX.Element {
  const tabTableContent = useStoreEditor(s => s.tabTableContent)
  const tabQueryPreviewContent = useStoreEditor(s => s.tabQueryPreviewContent)
  const tabTerminalContent = useStoreEditor(s => s.tabTerminalContent)
  const openedFiles = useStoreFileTree(s => s.openedFiles)
  const activeFileId = useStoreFileTree(s => s.activeFileId)

  const activeFile = useMemo(
    () => openedFiles.get(activeFileId),
    [openedFiles, activeFileId],
  )

  const data: any = useMemo(
    () => (tabTableContent == null ? [] : tabTableContent.toArray()),
    [tabTableContent],
  )

  const columns = useMemo(
    () =>
      tabTableContent == null
        ? []
        : tabTableContent.schema.fields.map(field => ({
            accessorKey: field['name'],
            cell: (info: any) => info.getValue(),
          })),
    [tabTableContent],
  )

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
      <Tab.Group defaultIndex={1}>
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
                    'inline-block text-sm font-medium px-3 py-1 mr-2 last-chald:mr-0 rounded-md relative',
                    isDisabledTabTable(tabName) ||
                      isDisabledTabTerminal(tabName) ||
                      isDisabledTabQueryPreview(tabName)
                      ? 'text-gray-400 cursor-not-allowed'
                      : selected
                      ? 'bg-secondary-100 text-secondary-500 cursor-default'
                      : 'text-gray-900 hover:bg-white/[0.12] hover:text-gray-500 cursor-pointer',
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
                  <thead className="sticky top-0">
                    {table.getHeaderGroups().map(headerGroup => (
                      <tr key={headerGroup.id}>
                        {headerGroup.headers.map(header => (
                          <th
                            key={header.id}
                            className="px-2 text-sm text-left text-gray-600 border-b-2 border-gray-100"
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
                            className="px-2 py-1 text-sm text-left text-gray-600 border-b border-gray-100"
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
                  <tfoot className="text-left sticky bottom-0 bg-gray-100">
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
            <pre className="w-full h-full  p-4 bg-secondary-100 rounded-lg">
              {tabQueryPreviewContent}
            </pre>
          </Tab.Panel>
          <Tab.Panel
            className={clsx(
              'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2',
            )}
          >
            <pre className="w-full h-full p-4 bg-secondary-100 rounded-lg text-danger-500 overflow-auto text-xs">
              {tabTerminalContent}
            </pre>
          </Tab.Panel>
        </Tab.Panels>
      </Tab.Group>
    </div>
  )
}
