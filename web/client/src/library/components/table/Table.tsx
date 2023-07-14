import { useMemo, useState, useRef } from 'react'
import '@tanstack/react-table'
import {
  type RowData,
  type SortingState,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  getFilteredRowModel,
} from '@tanstack/react-table'
import clsx from 'clsx'
import { type TableColumn, type TableRow } from './help'
import {
  ChevronDownIcon,
  ChevronUpDownIcon,
  ChevronUpIcon,
} from '@heroicons/react/24/solid'
import { useVirtual } from '@tanstack/react-virtual'
import Input from '@components/input/Input'
import { EnumSize } from '~/types/enum'
import { isArrayNotEmpty } from '@utils/index'

declare module '@tanstack/table-core' {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  interface ColumnMeta<TData extends RowData, TValue> {
    type: string
  }
}

const MIN_HEIGHT_ROW = 24

export default function Table({
  data = [[], []],
}: {
  data: [TableColumn[], TableRow[]]
}): JSX.Element {
  const elTableContainer = useRef<HTMLDivElement>(null)

  const columns = useMemo(
    () =>
      data[0].map(({ name, type }) => ({
        accessorKey: name,
        meta: {
          type,
        },
      })),
    [data[0]],
  )

  const [sorting, setSorting] = useState<SortingState>([])
  const [filter, setFilter] = useState('')

  const table = useReactTable({
    data: data[1],
    columns,
    state: {
      sorting,
      globalFilter: filter,
    },
    onGlobalFilterChange: setFilter,
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
  })

  const { rows } = table.getRowModel()
  const { virtualItems: virtualRows, totalSize } = useVirtual({
    parentRef: elTableContainer,
    size: rows.length,
    overscan: 10,
  })

  const paddingTop = virtualRows.length > 0 ? virtualRows?.[0]?.start ?? 0 : 0
  const paddingBottom =
    virtualRows.length > 0
      ? totalSize - (virtualRows?.[virtualRows.length - 1]?.end ?? 0)
      : 0

  console.log('rows', rows, filter)

  return (
    <div className="w-full h-full flex flex-col">
      <Header
        filter={filter}
        setFilter={setFilter}
      />
      <div
        ref={elTableContainer}
        className="w-full h-full rounded-lg overflow-auto hover:scrollbar scrollbar--horizontal scrollbar--vertical"
      >
        <table
          cellPadding={0}
          cellSpacing={0}
          className={clsx(
            'w-full slashed-zero tabular-nums',
            'text-neutral-700 dark:text-neutral-300 text-xs font-medium whitespace-nowrap text-left',
          )}
        >
          <thead className="sticky top-0">
            {table.getHeaderGroups().map(headerGroup => (
              <tr
                key={headerGroup.id}
                className="bg-primary-10 dark:bg-secondary-10 backdrop-blur-lg"
                style={{ height: `${MIN_HEIGHT_ROW}px` }}
              >
                {headerGroup.headers.map(header => (
                  <th
                    key={header.id}
                    className="pl-2 pr-4 pt-2 text-sm pb-1 border-r-2 last:border-r-0 border-light dark:border-dark"
                  >
                    {header.isPlaceholder ? (
                      <></>
                    ) : (
                      <div
                        className={clsx(
                          header.column.getCanSort()
                            ? 'flex cursor-pointer select-none'
                            : '',
                          ['int', 'float'].includes(
                            header.column.columnDef.meta!.type,
                          ) && 'justify-end',
                        )}
                        onClick={header.column.getToggleSortingHandler()}
                      >
                        {header.column.getCanSort() && (
                          <ChevronUpDownIcon className="mr-1 w-4" />
                        )}
                        {flexRender(
                          header.column.columnDef.header,
                          header.getContext(),
                        )}
                        {{
                          asc: <ChevronDownIcon className="ml-1 w-4" />,
                          desc: <ChevronUpIcon className="ml-1 w-4" />,
                        }[header.column.getIsSorted() as string] ?? null}
                      </div>
                    )}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {paddingTop > 0 && (
              <tr style={{ height: `${MIN_HEIGHT_ROW}px` }}>
                <td style={{ minHeight: `${paddingTop}px` }} />
              </tr>
            )}
            {isArrayNotEmpty(virtualRows) ? (
              virtualRows.map(({ index }) => {
                const row = rows[index]!

                return (
                  <tr
                    key={row.id}
                    className="even:bg-neutral-10 hover:text-neutral-900 hover:bg-secondary-10 dark:hover:text-neutral-100"
                    style={{ height: `${MIN_HEIGHT_ROW}px` }}
                  >
                    {row.getVisibleCells().map(cell => (
                      <td
                        key={cell.id}
                        className={clsx(
                          'p-4 py-1 border-r-2 last:border-r-0 border-light dark:border-dark',
                          ['int', 'float'].includes(
                            cell.column.columnDef.meta!.type,
                          ) && 'text-right',
                        )}
                      >
                        {flexRender(
                          cell.column.columnDef.cell,
                          cell.getContext(),
                        )}
                      </td>
                    ))}
                  </tr>
                )
              })
            ) : (
              <GhostRows columns={columns.length} />
            )}
            {paddingBottom > 0 && (
              <tr style={{ height: `${MIN_HEIGHT_ROW}px` }}>
                <td style={{ minHeight: `${paddingBottom}px` }} />
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <Footer count={rows.length} />
    </div>
  )
}

function Header({
  filter,
  setFilter,
}: {
  filter: string
  setFilter: (search: string) => void
}): JSX.Element {
  return (
    <div className="text-neutral-700 dark:text-neutral-300 text-xs font-medium py-2">
      <div className="flex justify-end items-center">
        <Input
          className="!m-0 mb-2"
          size={EnumSize.sm}
        >
          {({ className }) => (
            <Input.Textfield
              className={clsx(className, 'w-full')}
              value={filter}
              placeholder="Filter Rows"
              onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                setFilter(e.target.value)
              }}
            />
          )}
        </Input>
      </div>
    </div>
  )
}

function Footer({ count }: { count: number }): JSX.Element {
  return (
    <div className="text-neutral-700 dark:text-neutral-300 text-xs font-medium py-2">
      <p>Total Rows: {count}</p>
    </div>
  )
}

function GhostRows({
  rows = 6,
  columns = 5,
}: {
  rows?: number
  columns?: number
}): JSX.Element {
  return (
    <>
      {Array(rows)
        .fill(undefined)
        .map((_, row) => (
          <tr
            key={row}
            className="even:bg-neutral-10 hover:text-neutral-900 hover:bg-secondary-10 dark:hover:text-neutral-100"
            style={{ height: `${MIN_HEIGHT_ROW}px` }}
          >
            {Array(columns)
              .fill(undefined)
              .map((_, col) => (
                <td
                  key={col}
                  className="p-4 py-1 border-r-2 last:border-r-0 border-light dark:border-dark"
                ></td>
              ))}
          </tr>
        ))}
    </>
  )
}
