import Toggle from '@components/toggle/Toggle'
import clsx from 'clsx'
import { useState } from 'react'
import {
  TEST_GRAIN,
  getCellContent,
  getCellContentSource,
  getCellContentTarget,
  getHeaders,
  getRows,
  hasModified,
  isAddedRow,
  isDeletedRow,
  isModified,
} from './help'
import Input from '@components/input/Input'
import { EnumSize } from '~/types/enum'

export interface Filters {
  addedRows: boolean
  removedRows: boolean
  addedColumns: boolean
  removedColumns: boolean
  modifiedRows: boolean
  modifiedColumns: boolean
  search: string
}

export default function TableDiff({ diff }: { diff: any }): JSX.Element {
  const [filters, setFilters] = useState<Filters>({
    addedRows: true,
    removedRows: true,
    addedColumns: true,
    removedColumns: true,
    modifiedRows: true,
    modifiedColumns: true,
    search: '',
  })

  const headers = getHeaders(diff.schema_diff, filters)
  const rows = getRows(diff, filters)
  const isOnlyAddedRows =
    filters.addedRows && !filters.removedRows && !filters.modifiedRows
  const isOnlyRemovedRows =
    !filters.addedRows && filters.removedRows && !filters.modifiedRows

  return (
    <div className="p-2 h-full flex flex-col">
      <TableDiffFilters
        filters={filters}
        setFilters={setFilters}
      />
      <div className="overflow-auto h-full scrollbar scrollbar--horizontal scrollbar--vertical">
        <table className="w-full text-xs text-neutral-600 dark:text-neutral-200 font-normal border-separate ">
          <thead className="sticky top-0 bg-theme z-10">
            <tr>
              {headers.map(header => (
                <th
                  key={header}
                  colSpan={hasModified(diff, rows, header) ? 2 : 1}
                  className={clsx(
                    'text-left whitespace-nowrap py-1 px-2 font-bold bg-primary-10',
                    header in diff.schema_diff.added &&
                      'border-t-2 border-l-2 border-r-2 border-success-500',
                    header in diff.schema_diff.removed &&
                      'border-t-2 border-l-2 border-r-2 border-danger-500',
                    TEST_GRAIN.includes(header)
                      ? 'sticky top-0 left-0 z-10 border-r border-neutral-20 backdrop-filter backdrop-blur-lg'
                      : 'sticky top-0',
                  )}
                >
                  <span>{header}</span>&nbsp;
                  <small className="text-neutral-500 font-medium">
                    (
                    {diff.schema_diff.source_schema[header] ??
                      diff.schema_diff.target_schema[header]}
                    )
                  </small>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map(rowKey => (
              <tr key={rowKey}>
                {headers.map(header =>
                  hasModified(diff, rows, header) &&
                  isModified(diff, header, rowKey) ? (
                    <>
                      <td
                        key={`${rowKey}-${header}-source`}
                        className={clsx(
                          'px-2 py-1 whitespace-nowrap font-bold',
                          isAddedRow(diff, rowKey)
                            ? 'bg-success-10 text-success-500'
                            : isDeletedRow(diff, rowKey)
                            ? 'bg-danger-5 text-danger-500'
                            : 'bg-primary-10 text-primary-500',
                        )}
                      >
                        {getCellContentSource(diff, header, rowKey)}
                      </td>
                      <td
                        key={`${rowKey}-${header}-target`}
                        className={clsx(
                          'px-2 py-1 whitespace-nowrap font-bold',
                          isAddedRow(diff, rowKey)
                            ? 'bg-success-20 text-success-500'
                            : isDeletedRow(diff, rowKey)
                            ? 'bg-danger-10 text-danger-500'
                            : 'bg-primary-20 text-primary-500',
                        )}
                      >
                        {getCellContentTarget(diff, header, rowKey)}
                      </td>
                    </>
                  ) : (
                    <td
                      key={`${rowKey}-${header}`}
                      className={clsx(
                        'px-2 py-1 whitespace-nowrap',
                        header in diff.schema_diff.added &&
                          'border-l-2 border-r-2 border-success-500 text-success-500 font-bold',
                        header in diff.schema_diff.removed &&
                          'border-l-2 border-r-2 border-danger-500 !text-danger-500 font-bold',
                        isDeletedRow(diff, rowKey) &&
                          'bg-danger-5 !text-danger-500 font-bold',
                        isAddedRow(diff, rowKey) &&
                          'bg-success-10 text-success-500 font-bold',
                        TEST_GRAIN.includes(header) &&
                          'sticky left-0 border-r border-neutral-200 backdrop-filter backdrop-blur-lg font-bold',
                      )}
                    >
                      {getCellContent(diff, header, rowKey)}
                    </td>
                  ),
                )}
              </tr>
            ))}
          </tbody>
          <tfoot className="sticky bg-theme bottom-0">
            <tr>
              {headers.map(header =>
                hasModified(diff, rows, header) ? (
                  <>
                    <th
                      key={`${header}-source`}
                      className={clsx(
                        'text-left whitespace-nowrap px-2 py-1 bg-primary-10',
                      )}
                    >
                      Source
                    </th>
                    <th
                      key={`${header}-target`}
                      className={clsx(
                        'text-left whitespace-nowrap px-2 py-1 bg-primary-10',
                      )}
                    >
                      Target
                    </th>
                  </>
                ) : (
                  <th
                    key={header}
                    className={clsx(
                      'text-left whitespace-nowrap px-2 py-1 bg-primary-10 font-bold',
                      header in diff.schema_diff.added &&
                        'border-b-2 border-l-2 border-r-2 border-success-500',
                      header in diff.schema_diff.removed &&
                        'border-b-2 border-l-2 border-r-2 border-danger-500',
                      TEST_GRAIN.includes(header)
                        ? 'sticky top-0 left-0 z-20 border-r border-neutral-20 backdrop-filter backdrop-blur-lg'
                        : 'sticky top-0',
                    )}
                  >
                    {(header in diff.schema_diff.removed ||
                      isOnlyRemovedRows) && <span>Source</span>}
                    {(header in diff.schema_diff.added || isOnlyAddedRows) && (
                      <span>Target</span>
                    )}
                  </th>
                ),
              )}
            </tr>
          </tfoot>
        </table>
      </div>
    </div>
  )
}

function TableDiffFilters({
  filters,
  setFilters,
}: {
  filters: Filters
  setFilters: React.Dispatch<React.SetStateAction<Filters>>
}): JSX.Element {
  return (
    <div className="mb-3 p-1 bg-primary-10  rounded-lg">
      <div className="w-full flex justify-between">
        <Input
          className="w-full mb-2"
          size={EnumSize.sm}
          value={filters.search}
          placeholder="Filter Table"
          onInput={e => {
            setFilters({ ...filters, search: e.currentTarget.value })
          }}
        />
      </div>
      <div className="grid grid-cols-3">
        <Toggle
          label="Added Rows"
          enabled={filters.addedRows}
          setEnabled={(value: boolean) => {
            setFilters({ ...filters, addedRows: value })
          }}
        />
        <Toggle
          label="Removed Rows"
          enabled={filters.removedRows}
          setEnabled={(value: boolean) => {
            setFilters({ ...filters, removedRows: value })
          }}
        />
        <Toggle
          label="Modified Rows"
          enabled={filters.modifiedRows}
          setEnabled={(value: boolean) => {
            setFilters({ ...filters, modifiedRows: value })
          }}
        />
        <Toggle
          label="Added Columns"
          enabled={filters.addedColumns}
          setEnabled={(value: boolean) => {
            setFilters({ ...filters, addedColumns: value })
          }}
        />
        <Toggle
          label="Removed Columns"
          enabled={filters.removedColumns}
          setEnabled={(value: boolean) => {
            setFilters({ ...filters, removedColumns: value })
          }}
        />
        <Toggle
          label="Modified Columns"
          enabled={filters.modifiedColumns}
          setEnabled={(value: boolean) => {
            setFilters({ ...filters, modifiedColumns: value })
          }}
        />
      </div>
    </div>
  )
}
