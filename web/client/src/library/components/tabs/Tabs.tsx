import { useState } from 'react'
import { Tab } from '@headlessui/react'
import clsx from 'clsx'
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from '@tanstack/react-table'

const data: Array<{ id: string }> = []
const columnHelper = createColumnHelper<{ id: string }>()

const columns = [
  columnHelper.accessor('id', {
    cell: info => info.getValue(),
    footer: info => info.column.id,
  }),
]

export default function Tabs(): JSX.Element {
  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  const [categories] = useState({
    Table: [],
    'Query Preview': [],
  })

  return (
    <div className="flex flex-col h-full w-full overflow-hidden">
      <Tab.Group>
        <Tab.List className="w-full whitespace-nowrap px-2 pt-2">
          <div className="w-full overflow-hidden overflow-x-auto">
            {Object.keys(categories).map(category => (
              <Tab
                key={category}
                className={({ selected }) =>
                  clsx(
                    'inline-block text-sm font-medium px-3 py-1 mr-2 last-chald:mr-0 rounded-md cursor-pointer',
                    selected
                      ? 'bg-secondary-100 text-secondary-500'
                      : 'text-gray-900 hover:bg-white/[0.12] hover:text-gray-500'
                  )
                }
              >
                {category}
              </Tab>
            ))}
          </div>
        </Tab.List>
        <Tab.Panels className="w-full h-full overflow-hidden">
          <Tab.Panel
            className={clsx(
              'w-full h-full overflow-hidden pt-4',
              'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2'
            )}
          >
            <div className="w-full h-full overflow-hidden overflow-y-auto">
              <table className="w-full h-full">
                <thead className="sticky top-0 bg-gray-100">
                  {table.getHeaderGroups().map(headerGroup => (
                    <tr key={headerGroup.id}>
                      {headerGroup.headers.map(header => (
                        <th
                          key={header.id}
                          className="px-1 px-3 text-left"
                        >
                          {header.isPlaceholder
                            ? null
                            : flexRender(header.column.columnDef.header, header.getContext())}
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
                          className="px-1"
                        >
                          {flexRender(cell.column.columnDef.cell, cell.getContext())}
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
                            : flexRender(header.column.columnDef.footer, header.getContext())}
                        </th>
                      ))}
                    </tr>
                  ))}
                </tfoot>
              </table>
            </div>
          </Tab.Panel>
          <Tab.Panel
            className={clsx(
              'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2'
            )}
          >
            Query Preview
          </Tab.Panel>
        </Tab.Panels>
      </Tab.Group>
    </div>
  )
}
