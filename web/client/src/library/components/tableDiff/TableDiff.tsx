import clsx from 'clsx'
import React, { useState } from 'react'
import {
  EMPTY_TABLE_CELL,
  SOURCE_PREFIX,
  TARGET_PREFIX,
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
import { Disclosure } from '@headlessui/react'
import { MinusCircleIcon, PlusCircleIcon } from '@heroicons/react/24/solid'
import { isArrayNotEmpty, isFalse, isNotNil } from '@utils/index'
import { Footer, GhostRows } from '@components/table/Table'
import ListboxShow from '@components/listbox/ListboxShow'

export interface Filters extends Record<string, boolean> {
  modifiedRows: boolean
  addedRows: boolean
  removedRows: boolean
  modifiedColumns: boolean
  addedColumns: boolean
  removedColumns: boolean
}

export default function TableDiff({ diff }: { diff: any }): JSX.Element {
  const [filters, setFilters] = useState<Filters>({
    modifiedRows: true,
    addedRows: true,
    removedRows: true,
    modifiedColumns: true,
    addedColumns: true,
    removedColumns: true,
  })

  const headers = getHeaders(diff.schema_diff, filters, diff.on)
  const rows = getRows(diff, filters, diff.on)
  const isOnlyAddedRows =
    filters.addedRows && !filters.removedRows && !filters.modifiedRows
  const isOnlyRemovedRows =
    !filters.addedRows && filters.removedRows && !filters.modifiedRows
  const grain: string[] = Array.from(new Set(diff.on.flat()))
  const hasRows = Object.values(diff.row_diff.sample).some(
    (v: any) => Object.keys(v).length > 0,
  )

  return (
    <div className="px-2 h-full flex flex-col rounded-lg">
      {hasRows && (
        <>
          <TableDiffStats
            diff={diff}
            rows={rows}
            columns={headers}
          />
          <div className="mt-2 mb-1 flex rounded-lg items-center">
            <div className="w-full flex justify-end items-center">
              <ListboxShow
                options={Object.keys(filters).reduce(
                  (acc: Record<string, (value: boolean) => void>, key) => {
                    acc[key] = (value: boolean) =>
                      setFilters(oldVal => ({
                        ...oldVal,
                        [key]: value,
                      }))

                    return acc
                  },
                  {},
                )}
                value={
                  Object.keys(filters)
                    .map(key => (isFalse(filters[key]) ? undefined : key))
                    .filter(Boolean) as string[]
                }
              />
            </div>
          </div>
        </>
      )}
      <div className="overflow-auto h-full hover:scrollbar scrollbar--horizontal scrollbar--vertical">
        <table
          cellPadding={0}
          cellSpacing={0}
          className="w-full text-xs text-neutral-600 dark:text-neutral-200 font-normal border-separate"
        >
          <thead className="sticky bg-theme top-0 z-10">
            <tr>
              {headers.all.map(header => (
                <th
                  key={header}
                  colSpan={hasModified(diff, rows.all, header, diff.on) ? 2 : 1}
                  className={clsx(
                    'text-left whitespace-nowrap py-1 px-2 font-bold',
                    header in diff.schema_diff.added
                      ? 'border-t-2 border-l-2 border-r-2 border-success-500'
                      : header in diff.schema_diff.removed
                      ? 'border-t-2 border-l-2 border-r-2 border-danger-500'
                      : grain.includes(header)
                      ? 'border-brand-500 border-l-2 border-t-2 border-r-2'
                      : 'border-r border-b border-neutral-100 dark:border-neutral-700 last:border-r-0',
                    grain.includes(header) ? 'bg-brand-10' : 'bg-neutral-5',
                  )}
                >
                  <div className="flex justify-between">
                    <div className="mr-2">
                      <span>{header}</span>&nbsp;
                      <small className="text-neutral-500 font-medium">
                        (
                        {diff.schema_diff.source_schema[header] ??
                          diff.schema_diff.target_schema[header]}
                        )
                      </small>
                    </div>
                    {isFalse(grain.includes(header)) && (
                      <div className="ml-2">
                        <small className="inline-block bg-neutral-10 px-2 py-0.5 rounded-full">
                          {Math.round(
                            (rows.all.filter(key =>
                              isModified(diff, header, key),
                            ).length /
                              rows.all.length) *
                              100,
                          )}
                          %
                        </small>
                      </div>
                    )}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {isArrayNotEmpty(rows.all) ? (
              rows.all.map(rowKey => (
                <tr key={rowKey}>
                  {headers.all.map(header =>
                    hasModified(diff, rows.all, header, diff.on) ? (
                      <>
                        <td
                          key={`${rowKey}-${header}-source`}
                          className={clsx(
                            'p-1 border-r border-b border-neutral-100 dark:border-neutral-700 last:border-r-0',
                            isAddedRow(diff, rowKey, diff.on) &&
                              'bg-success-10 text-success-500',
                            isDeletedRow(diff, rowKey, diff.on) &&
                              'bg-danger-5 text-danger-500',
                          )}
                        >
                          <div
                            className={clsx(
                              'px-2 py-1 whitespace-nowrap font-bold rounded-md ',
                              isAddedRow(diff, rowKey, diff.on) &&
                                'bg-success-10 text-success-500',
                              isDeletedRow(diff, rowKey, diff.on) &&
                                'bg-danger-5 text-danger-500',
                            )}
                          >
                            {getCellContentSource(diff, header, rowKey)}
                          </div>
                        </td>
                        <td
                          key={`${rowKey}-${header}-target`}
                          className={clsx(
                            'p-1 border-r border-b border-neutral-100 dark:border-neutral-700 last:border-r-0',
                            isAddedRow(diff, rowKey, diff.on) &&
                              'bg-success-10 text-success-500',
                            isDeletedRow(diff, rowKey, diff.on) &&
                              'bg-danger-5 text-danger-500',
                          )}
                        >
                          <div
                            className={clsx(
                              'px-2 py-1 whitespace-nowrap font-bold rounded-md',
                              isModified(diff, header, rowKey) &&
                                'bg-primary-10 text-primary-500',
                              isAddedRow(diff, rowKey, diff.on) &&
                                '!bg-success-10 !text-success-500',
                              isDeletedRow(diff, rowKey, diff.on) &&
                                '!bg-danger-5 !text-danger-500',
                            )}
                          >
                            {getCellContentTarget(diff, header, rowKey)}
                          </div>
                        </td>
                      </>
                    ) : (
                      <td
                        key={`${rowKey}-${header}`}
                        className={clsx(
                          'p-1',
                          header in diff.schema_diff.added
                            ? 'bg-success-10 border-l-2 border-r-2 border-success-500 text-success-500 font-bold'
                            : header in diff.schema_diff.removed
                            ? 'bg-danger-5 border-l-2 border-r-2 border-danger-500 !text-danger-500 font-bold'
                            : grain.includes(header)
                            ? 'border-brand-500 border-l-2 border-r-2'
                            : 'border-r border-b border-neutral-100 dark:border-neutral-700 last:border-r-0',
                          isDeletedRow(diff, rowKey, diff.on) &&
                            '!bg-danger-5 text-danger-500 font-bold',
                          isAddedRow(diff, rowKey, diff.on) &&
                            'bg-success-10 text-success-500 font-bold',
                        )}
                      >
                        <div
                          className={clsx(
                            'px-2 py-1 whitespace-nowrap rounded-md',
                            (header in diff.schema_diff.added ||
                              isAddedRow(diff, rowKey, diff.on)) &&
                              'bg-success-10 text-success-500 font-bold',
                            (header in diff.schema_diff.removed ||
                              isDeletedRow(diff, rowKey, diff.on)) &&
                              '!bg-danger-5 !text-danger-500 font-bold',
                          )}
                        >
                          {getCellContent(
                            diff,
                            SOURCE_PREFIX,
                            header,
                            rowKey,
                          ) ??
                            getCellContent(
                              diff,
                              TARGET_PREFIX,
                              header,
                              rowKey,
                            ) ??
                            EMPTY_TABLE_CELL}
                        </div>
                      </td>
                    ),
                  )}
                </tr>
              ))
            ) : (
              <GhostRows
                columns={
                  headers.all.length > 0 ? headers.all.length : undefined
                }
              />
            )}
          </tbody>
          {isArrayNotEmpty(rows.all) && (
            <tfoot className="sticky bg-theme bottom-0">
              <tr>
                {headers.all.map(header =>
                  hasModified(diff, rows.all, header, diff.on) ? (
                    <>
                      <th
                        key={`${header}-source`}
                        className={clsx(
                          'text-left whitespace-nowrap px-2 py-1 border-r border-t border-neutral-100 dark:border-neutral-700 last:border-r-0',
                          grain.includes(header)
                            ? 'bg-brand-10'
                            : 'bg-neutral-10',
                        )}
                      >
                        Source
                      </th>
                      <th
                        key={`${header}-target`}
                        className={clsx(
                          'text-left whitespace-nowrap px-2 py-1 border-r border-t border-neutral-100 dark:border-neutral-700 last:border-r-0',
                          grain.includes(header)
                            ? 'bg-brand-10'
                            : 'bg-primary-10',
                        )}
                      >
                        Target
                      </th>
                    </>
                  ) : (
                    <th
                      key={header}
                      className={clsx(
                        'text-left whitespace-nowrap px-2 py-1 font-bold',
                        header in diff.schema_diff.added
                          ? 'border-b-2 border-l-2 border-r-2 border-success-500'
                          : header in diff.schema_diff.removed
                          ? 'border-b-2 border-l-2 border-r-2 border-danger-500'
                          : grain.includes(header)
                          ? 'border-brand-500 border-l-2 border-b-2 border-r-2'
                          : 'border-r border-t border-neutral-100 dark:border-neutral-700 last:border-r-0',
                        grain.includes(header)
                          ? 'bg-brand-10'
                          : 'bg-neutral-10',
                      )}
                    >
                      {(header in diff.schema_diff.removed ||
                        isOnlyRemovedRows) && <span>Source</span>}
                      {(header in diff.schema_diff.added ||
                        isOnlyAddedRows) && <span>Target</span>}
                    </th>
                  ),
                )}
              </tr>
            </tfoot>
          )}
        </table>
      </div>
      <div className="flex justify-between items-center px-2 mt-2">
        <Footer count={rows.all.length} />
        <Legend />
      </div>
    </div>
  )
}

function Legend(): JSX.Element {
  const items = [
    ['Grain', 'bg-brand-500'],
    ['Changed', 'bg-primary-500'],
    ['Added', 'bg-success-500'],
    ['Deleted', 'bg-danger-500'],
  ]
  return (
    <div className="flex text-xs">
      {items.map(([text = '', className]) => (
        <LegendItem
          key={text}
          text={text}
          className={className}
        />
      ))}
    </div>
  )
}

function LegendItem({
  text,
  className,
}: {
  text: string
  className?: string
}): JSX.Element {
  return (
    <div className="flex ml-2 items-center">
      <span
        className={clsx('inline-block w-3 h-3 mr-2 rounded-full', className)}
      ></span>
      <small className="text-neutral-600 dark:text-neutral-400">{text}</small>
    </div>
  )
}

function TableDiffStats({
  diff,
  rows,
  columns,
}: {
  diff: any
  rows: any
  columns: any
}): JSX.Element {
  return (
    <Disclosure defaultOpen={false}>
      {({ open }) => (
        <>
          <Disclosure.Button className="flex items-center w-full justify-between rounded-lg text-left text-sm px-4 pt-3 pb-2 bg-neutral-10 hover:bg-theme-darker dark:hover:bg-theme-lighter text-neutral-600 dark:text-neutral-400">
            <h2 className="whitespace-nowrap text-xl font-bold mb-1">Stats</h2>
            {open ? (
              <MinusCircleIcon className="h-6 w-6 text-primary-500" />
            ) : (
              <PlusCircleIcon className="h-6 w-6 text-primary-500" />
            )}
          </Disclosure.Button>
          <Disclosure.Panel className="px-4 pb-2 text-sm text-neutral-500">
            <div className="p-2 grid grid-cols-3 gap-4 mb-3">
              <TableDiffStatsCard text="Row Count Change">
                <p className="text-6xl font-light text-primary-500 mt-3">
                  {Math.round(Math.abs(diff.row_diff.count_pct_change))}
                  <small className="text-sm">%</small>
                </p>
              </TableDiffStatsCard>
              <TableDiffStatsCard
                text="Column Count Change"
                count={rows.all.length}
              >
                <p className="text-center text-6xl font-light text-primary-500 mt-3">
                  {rows.modified}
                </p>
                <p className="text-center text-6xl font-light text-success-500 mt-3">
                  {rows.added}
                </p>
                <p className="text-center text-6xl font-light text-danger-500 mt-3">
                  {rows.deleted}
                </p>
              </TableDiffStatsCard>
              <TableDiffStatsCard
                text="Column Changes"
                count={columns.all.length}
              >
                <p className="text-center text-6xl font-light text-primary-500 mt-3">
                  {columns.modified}
                </p>
                <p className="text-center text-6xl font-light text-success-500 mt-3">
                  {columns.added}
                </p>
                <p className="text-center text-6xl font-light text-danger-500 mt-3">
                  {columns.deleted}
                </p>
              </TableDiffStatsCard>
            </div>
          </Disclosure.Panel>
        </>
      )}
    </Disclosure>
  )
}

function TableDiffStatsCard({
  text,
  children,
  className,
  count,
}: {
  text: string
  children: React.ReactNode
  count?: number
  className?: string
}): JSX.Element {
  return (
    <div
      className={clsx(
        'rounded-xl overflow-hidden px-3 py-6 bg-primary-10',
        className,
      )}
    >
      <div className="flex justify-between">
        <h3 className="text-neutral-500 dark:text-neutral-300 text-sm font-bold">
          {text}
        </h3>
        {isNotNil(count) && (
          <div>
            <small className="inline-block px-2 py-0.5 bg-neutral-10 rounded-full">
              {count}
            </small>
          </div>
        )}
      </div>
      <div className="grid grid-cols-3 gap-2">{children}</div>
    </div>
  )
}
