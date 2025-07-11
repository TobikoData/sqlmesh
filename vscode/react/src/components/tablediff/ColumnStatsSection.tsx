import { SectionToggle } from './SectionToggle'
import { type TableDiffData, type SampleValue } from './types'
import { twColors, twMerge } from './tailwind-utils'

interface ColumnStatsSectionProps {
  columnStats: TableDiffData['row_diff']['column_stats']
  expanded: boolean
  onToggle: () => void
}

interface StatHeaderProps {
  stat: string
}

const StatHeader = ({ stat }: StatHeaderProps) => (
  <th
    key={stat}
    className={twMerge(
      'text-left py-2 px-1 font-medium w-16',
      twColors.textMuted,
    )}
    title={stat}
  >
    {stat.length > 6 ? stat.slice(0, 6) + '..' : stat}
  </th>
)

interface StatCellProps {
  value: SampleValue
}

const StatCell = ({ value }: StatCellProps) => (
  <td
    className={twMerge(
      'py-2 px-1 font-mono text-xs truncate',
      twColors.textMuted,
    )}
    title={String(value)}
  >
    {typeof value === 'number'
      ? value.toFixed(1)
      : String(value).length > 8
        ? String(value).slice(0, 8) + '..'
        : String(value)}
  </td>
)

interface ColumnStatRowProps {
  columnName: string
  statsValue: TableDiffData['row_diff']['column_stats'][string]
}

const ColumnStatRow = ({ columnName, statsValue }: ColumnStatRowProps) => (
  <tr
    className={twMerge(
      'transition-colors border-b',
      twColors.borderPanel,
      twColors.bgHover,
    )}
  >
    <td
      className="py-2 pr-2 font-mono truncate"
      title={columnName}
    >
      {columnName}
    </td>
    {statsValue && typeof statsValue === 'object'
      ? Object.values(statsValue as Record<string, SampleValue>).map(
          (value, idx) => (
            <StatCell
              key={idx}
              value={value}
            />
          ),
        )
      : [
          <StatCell
            key="single-value"
            value={statsValue}
          />,
        ]}
  </tr>
)

export function ColumnStatsSection({
  columnStats,
  expanded,
  onToggle,
}: ColumnStatsSectionProps) {
  if (Object.keys(columnStats || {}).length === 0) {
    return null
  }

  // Get the first stats object to determine the column headers
  const firstStatsValue = Object.values(columnStats)[0]
  const statKeys =
    firstStatsValue && typeof firstStatsValue === 'object'
      ? Object.keys(firstStatsValue as Record<string, SampleValue>)
      : []

  return (
    <SectionToggle
      id="columnStats"
      title="Column Statistics"
      expanded={expanded}
      onToggle={onToggle}
    >
      <div className="px-8 py-3">
        <div className="overflow-auto max-h-80">
          <table className="w-full text-xs table-fixed">
            <thead className={twMerge('sticky top-0 z-10', twColors.bgEditor)}>
              <tr className={twMerge('border-b', twColors.borderPanel)}>
                <th
                  className={twMerge(
                    'text-left py-2 pr-2 font-medium w-28',
                    twColors.textMuted,
                  )}
                >
                  Column
                </th>
                {statKeys.map(stat => (
                  <StatHeader
                    key={stat}
                    stat={stat}
                  />
                ))}
              </tr>
            </thead>
            <tbody>
              {Object.entries(columnStats).map(([col, statsValue]) => (
                <ColumnStatRow
                  key={col}
                  columnName={col}
                  statsValue={statsValue}
                />
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </SectionToggle>
  )
}
