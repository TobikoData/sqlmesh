import clsx from 'clsx'
import { Fragment, useState } from 'react'
import {
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
import { Disclosure, Listbox, Transition } from '@headlessui/react'
import {
  CheckIcon,
  MinusCircleIcon,
  PlusCircleIcon,
  ChevronDownIcon,
} from '@heroicons/react/24/solid'
import { isFalse } from '@utils/index'

export interface Filters {
  [key: string]: boolean
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

  return (
    <div className="px-2 h-full flex flex-col">
      <TableDiffStats
        diff={diff}
        rows={rows}
        columns={headers}
      />
      <div className="mt-2 flex rounded-lg items-center px-2">
        <div className="w-full flex justify-end items-center">
          <SelectFilters
            filters={filters}
            setFilters={setFilters}
          />
        </div>
      </div>
      <div className="overflow-auto h-full hover:scrollbar scrollbar--horizontal scrollbar--vertical">
        <table
          cellPadding={0}
          cellSpacing={0}
          className="w-full text-xs text-neutral-600 dark:text-neutral-200 font-normal border-separate "
        >
          <thead className="sticky top-0 bg-theme z-10">
            <tr>
              {headers.all.map(header => (
                <th
                  key={header}
                  colSpan={hasModified(diff, rows.all, header, diff.on) ? 2 : 1}
                  className={clsx(
                    'text-left whitespace-nowrap py-1 px-2 font-bold bg-primary-10',
                    header in diff.schema_diff.added &&
                      'border-t-2 border-l-2 border-r-2 border-success-500',
                    header in diff.schema_diff.removed &&
                      'border-t-2 border-l-2 border-r-2 border-danger-500',
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
            {rows.all.map(rowKey => (
              <tr
                key={rowKey}
                className="even:bg-neutral-10"
              >
                {headers.all.map(header =>
                  hasModified(diff, rows.all, header, diff.on) ? (
                    <>
                      <td
                        key={`${rowKey}-${header}-source`}
                        className={clsx(
                          'px-2 py-1 whitespace-nowrap font-bold',
                          isAddedRow(diff, rowKey, diff.on)
                            ? 'bg-success-10 text-success-500'
                            : isDeletedRow(diff, rowKey, diff.on)
                            ? 'bg-danger-5 text-danger-500'
                            : isModified(diff, header, rowKey)
                            ? 'bg-primary-20 text-primary-500'
                            : '',
                        )}
                      >
                        {getCellContentSource(diff, header, rowKey)}
                      </td>
                      <td
                        key={`${rowKey}-${header}-target`}
                        className={clsx(
                          'px-2 py-1 whitespace-nowrap font-bold',
                          isAddedRow(diff, rowKey, diff.on)
                            ? 'bg-success-20 text-success-500'
                            : isDeletedRow(diff, rowKey, diff.on)
                            ? 'bg-danger-10 text-danger-500'
                            : isModified(diff, header, rowKey)
                            ? 'bg-primary-20 text-primary-500'
                            : '',
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
                        isDeletedRow(diff, rowKey, diff.on) &&
                          'bg-danger-5 !text-danger-500 font-bold',
                        isAddedRow(diff, rowKey, diff.on) &&
                          'bg-success-10 text-success-500 font-bold',
                        grain.includes(header) && 'bg-brand-10',
                      )}
                    >
                      {getCellContent(diff, header, rowKey, diff.on)}
                    </td>
                  ),
                )}
              </tr>
            ))}
          </tbody>
          <tfoot className="sticky bg-theme bottom-0">
            <tr>
              {headers.all.map(header =>
                hasModified(diff, rows.all, header, diff.on) ? (
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
      <div className="flex px-2 mt-2">
        <div className="flex ml-2 items-center">
          <span className="inline-block w-3 h-3 bg-brand-500 mr-2 rounded-full"></span>
          <small className="text-neutral-600 dark:text-neutral-400">
            Grain
          </small>
        </div>
        <div className="flex ml-2 items-center">
          <span className="inline-block w-3 h-3 bg-primary-500 mr-2 rounded-full"></span>
          <small className="text-neutral-600 dark:text-neutral-400">
            Changed
          </small>
        </div>
        <div className="flex ml-2 items-center">
          <span className="inline-block w-3 h-3 bg-success-500 mr-2 rounded-full"></span>
          <small className="text-neutral-600 dark:text-neutral-400">
            Added
          </small>
        </div>
        <div className="flex ml-2 items-center">
          <span className="inline-block w-3 h-3 bg-danger-500 mr-2 rounded-full"></span>
          <small className="text-neutral-600 dark:text-neutral-400">
            Deleted
          </small>
        </div>
      </div>
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
              <div className="rounded-xl overflow-hidden px-3 py-6 bg-primary-10">
                <h3 className="text-neutral-500 dark:text-neutral-300 text-sm font-bold">
                  Row Count Change
                </h3>
                <p className="text-6xl font-light text-primary-500 mt-3">
                  {Math.round(Math.abs(diff.row_diff.count_pct_change))}
                  <small className="text-sm">%</small>
                </p>
              </div>
              <div className="rounded-xl overflow-hidden px-3 py-6 bg-primary-10">
                <div className="flex justify-between">
                  <h3 className="text-neutral-500 dark:text-neutral-300 text-sm font-bold">
                    Row Changes
                  </h3>
                  <small className="inline-block px-2 py-0.5 bg-neutral-10 rounded-full">
                    {rows.all.length}
                  </small>
                </div>
                <div className="grid grid-cols-3 gap-2">
                  <p className="text-center text-6xl font-light text-primary-500 mt-3">
                    {rows.modified}
                  </p>
                  <p className="text-center text-6xl font-light text-success-500 mt-3">
                    {rows.added}
                  </p>
                  <p className="text-center text-6xl font-light text-danger-500 mt-3">
                    {rows.deleted}
                  </p>
                </div>
              </div>
              <div className="rounded-xl overflow-hidden px-3 py-6 bg-primary-10">
                <div className="flex justify-between">
                  <h3 className="text-neutral-500 dark:text-neutral-300 text-sm font-bold">
                    Column Changes
                  </h3>
                  <small className="inline-block px-2 py-0.5 bg-neutral-10 rounded-full">
                    {columns.all.length}
                  </small>
                </div>
                <div className="grid grid-cols-3 gap-2">
                  <p className="text-center text-6xl font-light text-primary-500 mt-3">
                    {columns.modified}
                  </p>
                  <p className="text-center text-6xl font-light text-success-500 mt-3">
                    {columns.added}
                  </p>
                  <p className="text-center text-6xl font-light text-danger-500 mt-3">
                    {columns.deleted}
                  </p>
                </div>
              </div>
            </div>
          </Disclosure.Panel>
        </>
      )}
    </Disclosure>
  )
}

function SelectFilters({
  filters,
  setFilters,
}: {
  filters: Filters
  setFilters: React.Dispatch<React.SetStateAction<Filters>>
}): JSX.Element {
  const [selected, setSelected] = useState(Object.keys(filters))

  return (
    <Listbox
      value={selected}
      onChange={value => {
        setSelected(value)
        setFilters(
          Object.keys(filters).reduce(
            (acc: Filters, key) => {
              acc[key] = value.includes(key)

              return acc
            },
            { ...filters },
          ),
        )
      }}
      multiple
    >
      <div className="relative m-1 flex">
        <Listbox.Button className="flex items-center relative w-full cursor-default text-xs rounded-md text-primary-500 border-2 border-primary-500 py-1 px-2 text-center focus:outline-none focus-visible:border-accent-500 focus-visible:ring-2 focus-visible:ring-light focus-visible:ring-opacity-75 focus-visible:ring-offset-2 focus-visible:ring-offset-brand-300">
          <span className="block truncate">Show</span>
          <ChevronDownIcon
            className="ml-2 h-4 w-4"
            aria-hidden="true"
          />
        </Listbox.Button>
        <Transition
          as={Fragment}
          leave="transition ease-in duration-100"
          leaveFrom="opacity-100"
          leaveTo="opacity-0"
        >
          <Listbox.Options className="absolute top-10 right-0 z-50 max-h-60 min-w-16 overflow-auto rounded-md bg-theme py-2 text-base shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none sm:text-sm">
            {Object.keys(filters).map(key => (
              <Listbox.Option
                key={key}
                className={({ active }) =>
                  `relative cursor-default select-none py-1 pl-10 pr-4 ${
                    active ? 'bg-warning-100 text-warning-900' : 'text-gray-900'
                  }`
                }
                value={key}
              >
                {({ selected }) => (
                  <>
                    <span
                      className={`block truncate ${
                        selected ? 'font-medium' : 'font-normal'
                      }`}
                    >
                      {key}
                    </span>
                    {selected ? (
                      <span className="absolute inset-y-0 left-0 flex items-center pl-3 text-warning-600">
                        <CheckIcon
                          className="h-4 w-4"
                          aria-hidden="true"
                        />
                      </span>
                    ) : null}
                  </>
                )}
              </Listbox.Option>
            ))}
          </Listbox.Options>
        </Transition>
      </div>
    </Listbox>
  )
}
