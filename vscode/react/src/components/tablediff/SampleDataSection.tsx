import { useMemo } from 'react'
import {
  type TableDiffData,
  type SampleRow,
  type SampleValue,
  formatCellValue,
} from './types'
import { twColors, twMerge } from './tailwind-utils'

interface SampleDataSectionProps {
  rowDiff: TableDiffData['row_diff']
}

interface TableHeaderCellProps {
  columnKey: string
  sourceName?: SampleValue
  targetName?: SampleValue
}

const TableHeaderCell = ({
  columnKey,
  sourceName,
  targetName,
}: TableHeaderCellProps) => {
  const isSource = columnKey === sourceName
  const isTarget = columnKey === targetName

  return (
    <th
      className={twMerge(
        'text-left py-2 px-2 font-medium whitespace-nowrap',
        isSource && twColors.textSource,
        isTarget && twColors.textTarget,
        !isSource && !isTarget && twColors.textMuted,
      )}
    >
      {columnKey}
    </th>
  )
}

interface DiffTableCellProps {
  columnKey: string
  value: SampleValue
  sourceName?: SampleValue
  targetName?: SampleValue
  decimals?: number
}

const DiffTableCell = ({
  columnKey,
  value,
  sourceName,
  targetName,
  decimals = 3,
}: DiffTableCellProps) => {
  const isSource = columnKey === sourceName
  const isTarget = columnKey === targetName

  return (
    <td
      className={twMerge(
        'py-2 px-2 font-mono whitespace-nowrap',
        isSource && twColors.textSource + ' bg-blue-500/10',
        isTarget && twColors.textTarget + ' bg-green-500/10',
        !isSource && !isTarget && twColors.textForeground,
      )}
    >
      {formatCellValue(value, decimals)}
    </td>
  )
}

interface DiffTableRowProps {
  row: SampleRow
  sourceName?: SampleValue
  targetName?: SampleValue
  decimals?: number
}

const DiffTableRow = ({
  row,
  sourceName,
  targetName,
  decimals,
}: DiffTableRowProps) => (
  <tr
    className={twMerge(
      'transition-colors',
      twColors.borderPanel,
      'border-b',
      twColors.bgHover,
    )}
  >
    {Object.entries(row)
      .filter(([key]) => !key.startsWith('__'))
      .map(([key, cell]) => (
        <DiffTableCell
          key={key}
          columnKey={key}
          value={cell}
          sourceName={sourceName}
          targetName={targetName}
          decimals={decimals}
        />
      ))}
  </tr>
)

interface SimpleTableCellProps {
  value: SampleValue
  colorClass: string
  decimals?: number
}

const SimpleTableCell = ({
  value,
  colorClass,
  decimals = 3,
}: SimpleTableCellProps) => (
  <td
    className={twMerge(
      'py-3 px-4 font-mono whitespace-nowrap text-sm',
      colorClass,
    )}
  >
    {formatCellValue(value, decimals)}
  </td>
)

interface SimpleTableRowProps {
  row: SampleRow
  colorClass: string
  borderColorClass: string
  decimals?: number
}

const SimpleTableRow = ({
  row,
  colorClass,
  borderColorClass,
  decimals,
}: SimpleTableRowProps) => (
  <tr
    className={twMerge(
      'transition-colors border-b',
      borderColorClass,
      twColors.bgHover,
    )}
  >
    {Object.values(row).map((cell, cellIdx) => (
      <SimpleTableCell
        key={cellIdx}
        value={cell}
        colorClass={colorClass}
        decimals={decimals}
      />
    ))}
  </tr>
)

interface ColumnDifferenceGroupProps {
  columnName: string
  rows: SampleRow[]
  decimals: number
}

const ColumnDifferenceGroup = ({
  columnName,
  rows,
  decimals,
}: ColumnDifferenceGroupProps) => {
  if (!rows || rows.length === 0) return null

  const sourceName = rows[0].__source_name__
  const targetName = rows[0].__target_name__

  return (
    <div className="mb-4">
      <div
        className={twMerge(
          'flex items-center gap-2 text-sm font-medium mb-3',
          twColors.textForeground,
        )}
      >
        <span className="font-semibold">Column: {columnName}</span>
        <span
          className={twMerge(
            'text-xs px-2 py-0.5 rounded-full',
            twColors.bgNeutral10,
          )}
        >
          {rows.length} difference{rows.length > 1 ? 's' : ''}
        </span>
      </div>
      <div
        className={twMerge(
          'border rounded-lg overflow-hidden',
          twColors.borderNeutral100,
        )}
      >
        <div className="overflow-auto max-h-60">
          <table className="w-full">
            <thead
              className={twMerge(
                'sticky top-0 z-10',
                twColors.bgNeutral10,
                'border-b',
                twColors.borderNeutral100,
              )}
            >
              <tr>
                {Object.keys(rows[0] || {})
                  .filter(key => !key.startsWith('__'))
                  .map(key => (
                    <TableHeaderCell
                      key={key}
                      columnKey={key}
                      sourceName={sourceName}
                      targetName={targetName}
                    />
                  ))}
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 10).map((row, rowIdx) => (
                <DiffTableRow
                  key={rowIdx}
                  row={row}
                  sourceName={sourceName}
                  targetName={targetName}
                  decimals={decimals}
                />
              ))}
            </tbody>
          </table>
        </div>
        {rows.length > 10 && (
          <p className={twMerge('text-xs mt-2', twColors.textMuted)}>
            Showing first 10 of {rows.length} differing rows
          </p>
        )}
      </div>
    </div>
  )
}

export function SampleDataSection({ rowDiff }: SampleDataSectionProps) {
  const { processed_sample_data, decimals = 3 } = rowDiff

  if (!processed_sample_data) {
    return (
      <div className="px-8 py-3">
        <p className={twMerge('text-sm', twColors.textMuted)}>
          No processed sample data available
        </p>
      </div>
    )
  }

  const { column_differences, source_only, target_only } = processed_sample_data

  // Group column differences by column name
  const groupedDifferences = useMemo(() => {
    const groups: Record<string, SampleRow[]> = {}

    column_differences.forEach((row: SampleRow) => {
      const columnName = String(row.__column_name__ || 'unknown')
      if (!groups[columnName]) {
        groups[columnName] = []
      }
      groups[columnName].push(row)
    })

    return groups
  }, [column_differences])

  return (
    <div className="px-8 py-3 space-y-6">
      {/* COMMON ROWS diff */}
      <div>
        <h4
          className={twMerge(
            'text-base font-semibold mb-4',
            twColors.textPrimary,
          )}
        >
          Common Rows
        </h4>
        {Object.keys(groupedDifferences).length > 0 ? (
          <div className="space-y-4">
            {Object.entries(groupedDifferences).map(([columnName, rows]) => (
              <ColumnDifferenceGroup
                key={columnName}
                columnName={columnName}
                rows={rows}
                decimals={decimals}
              />
            ))}
          </div>
        ) : (
          <p className={twMerge('text-sm', twColors.textSuccess)}>
            ✓ All joined rows match
          </p>
        )}
      </div>

      {/* SOURCE ONLY & TARGET ONLY tables */}
      {source_only && source_only.length > 0 && (
        <div>
          <h4
            className={twMerge(
              'text-base font-semibold mb-4',
              twColors.textSource,
            )}
          >
            Source Only Rows
          </h4>
          <div
            className={twMerge(
              'border-2 rounded-lg overflow-hidden',
              twColors.borderSource,
            )}
          >
            <div className="overflow-auto max-h-80">
              <table className="w-full">
                <thead
                  className={twMerge('sticky top-0 z-10', twColors.bgNeutral10)}
                >
                  <tr
                    className={twMerge('border-b', twColors.borderNeutral100)}
                  >
                    {Object.keys(source_only[0] || {}).map(col => (
                      <th
                        key={col}
                        className={twMerge(
                          'text-left py-3 px-4 font-medium whitespace-nowrap text-sm',
                          twColors.textForeground,
                        )}
                      >
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {source_only.slice(0, 10).map((row, rowIdx) => (
                    <SimpleTableRow
                      key={rowIdx}
                      row={row}
                      colorClass={twColors.textSource}
                      borderColorClass={twColors.borderNeutral100}
                      decimals={decimals}
                    />
                  ))}
                </tbody>
              </table>
            </div>
            {source_only.length > 10 && (
              <div
                className={twMerge(
                  'px-4 py-2 text-xs',
                  twColors.bgNeutral5,
                  twColors.textMuted,
                )}
              >
                Showing first 10 of {source_only.length} rows
              </div>
            )}
          </div>
        </div>
      )}

      {target_only && target_only.length > 0 && (
        <div>
          <h4
            className={twMerge(
              'text-base font-semibold mb-4',
              twColors.textTarget,
            )}
          >
            Target Only Rows
          </h4>
          <div
            className={twMerge(
              'border-2 rounded-lg overflow-hidden',
              twColors.borderTarget,
            )}
          >
            <div className="overflow-auto max-h-80">
              <table className="w-full">
                <thead
                  className={twMerge('sticky top-0 z-10', twColors.bgNeutral10)}
                >
                  <tr
                    className={twMerge('border-b', twColors.borderNeutral100)}
                  >
                    {Object.keys(target_only[0] || {}).map(col => (
                      <th
                        key={col}
                        className={twMerge(
                          'text-left py-3 px-4 font-medium whitespace-nowrap text-sm',
                          twColors.textForeground,
                        )}
                      >
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {target_only.slice(0, 10).map((row, rowIdx) => (
                    <SimpleTableRow
                      key={rowIdx}
                      row={row}
                      colorClass={twColors.textTarget}
                      borderColorClass={twColors.borderNeutral100}
                      decimals={decimals}
                    />
                  ))}
                </tbody>
              </table>
            </div>
            {target_only.length > 10 && (
              <div
                className={twMerge(
                  'px-4 py-2 text-xs',
                  twColors.bgNeutral5,
                  twColors.textMuted,
                )}
              >
                Showing first 10 of {target_only.length} rows
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
