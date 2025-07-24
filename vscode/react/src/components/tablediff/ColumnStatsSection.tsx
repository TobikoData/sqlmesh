import { useState } from 'react'
import { type TableDiffData, type SampleValue } from './types'
import { twColors, twMerge } from './tailwind-utils'
import { Card } from './Card'
import {
  ArrowsUpDownIcon,
  ArrowsRightLeftIcon,
} from '@heroicons/react/24/outline'

interface ColumnStatsSectionProps {
  columnStats: TableDiffData['row_diff']['column_stats']
}

interface StatHeaderProps {
  stat: string
}

const StatHeader = ({ stat }: StatHeaderProps) => (
  <th
    key={stat}
    className={twMerge(
      'text-left py-3 px-4 font-medium text-sm',
      twColors.textForeground,
    )}
    title={stat}
  >
    {stat}
  </th>
)

interface StatCellProps {
  value: SampleValue
}

const StatCell = ({ value }: StatCellProps) => (
  <td
    className={twMerge('py-3 px-4 font-mono text-sm', twColors.textMuted)}
    title={String(value)}
  >
    {typeof value === 'number' ? value.toFixed(1) : String(value)}
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
      twColors.borderNeutral100,
      twColors.bgHover,
    )}
  >
    <td
      className={twMerge(
        'py-3 px-4 font-medium text-sm',
        twColors.textForeground,
      )}
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

export function ColumnStatsSection({ columnStats }: ColumnStatsSectionProps) {
  const [isVertical, setIsVertical] = useState(false)

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
    <div className="grid grid-cols-1 gap-4">
      {/* Statistics Table Card */}
      <Card className="overflow-hidden">
        {/* Toggle Button */}
        <div
          className={twMerge(
            'flex justify-end p-2 border-b',
            twColors.borderNeutral100,
          )}
        >
          <button
            onClick={() => setIsVertical(!isVertical)}
            className={twMerge(
              'flex items-center gap-1 px-2 py-1 text-xs rounded transition-colors',
              twColors.bgHover,
              twColors.textMuted,
            )}
            title={`Switch to ${isVertical ? 'horizontal' : 'vertical'} layout`}
          >
            {isVertical ? (
              <>
                <ArrowsRightLeftIcon className="w-3 h-3" />
                Horizontal
              </>
            ) : (
              <>
                <ArrowsUpDownIcon className="w-3 h-3" />
                Vertical
              </>
            )}
          </button>
        </div>

        <div className="overflow-auto max-h-96">
          {isVertical ? (
            // Vertical layout: Each stat as a separate row
            <table className="w-full">
              <thead
                className={twMerge('sticky top-0 z-10', twColors.bgNeutral10)}
              >
                <tr className={twMerge('border-b', twColors.borderNeutral100)}>
                  <th
                    className={twMerge(
                      'text-left py-3 px-4 font-medium text-sm',
                      twColors.textForeground,
                    )}
                  >
                    Column
                  </th>
                  {Object.keys(columnStats).map(col => (
                    <th
                      key={col}
                      className={twMerge(
                        'text-left py-3 px-4 font-medium text-sm',
                        twColors.textForeground,
                      )}
                    >
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {statKeys.map(stat => (
                  <tr
                    key={stat}
                    className={twMerge(
                      'transition-colors border-b',
                      twColors.borderNeutral100,
                      twColors.bgHover,
                    )}
                  >
                    <td
                      className={twMerge(
                        'py-3 px-4 font-medium text-sm',
                        twColors.textForeground,
                      )}
                    >
                      {stat}
                    </td>
                    {Object.entries(columnStats).map(([col, statsValue]) => (
                      <StatCell
                        key={col}
                        value={
                          statsValue && typeof statsValue === 'object'
                            ? (statsValue as Record<string, SampleValue>)[stat]
                            : statsValue
                        }
                      />
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            // Horizontal layout: Original layout
            <table className="w-full">
              <thead
                className={twMerge('sticky top-0 z-10', twColors.bgNeutral10)}
              >
                <tr className={twMerge('border-b', twColors.borderNeutral100)}>
                  <th
                    className={twMerge(
                      'text-left py-3 px-4 font-medium text-sm',
                      twColors.textForeground,
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
          )}
        </div>
      </Card>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {(() => {
          let percentages: { column: string; percentage: number }[] = []

          if (columnStats && typeof columnStats === 'object') {
            if (
              'pct_match' in columnStats &&
              typeof columnStats.pct_match === 'object' &&
              columnStats.pct_match !== null
            ) {
              const pctMatchData = columnStats.pct_match as Record<
                string,
                number
              >
              percentages = Object.entries(pctMatchData)
                .map(([col, value]) => ({
                  column: col,
                  percentage: Number(value) || 0,
                }))
                .filter(item => !isNaN(item.percentage))
            } else {
              percentages = Object.entries(columnStats)
                .map(([col, stats]) => {
                  if (!stats || typeof stats !== 'object') return null

                  const statsObj = stats as Record<string, number>
                  const pctMatch =
                    statsObj.pct_match ||
                    statsObj.match_pct ||
                    statsObj.percentage ||
                    0

                  return { column: col, percentage: Number(pctMatch) }
                })
                .filter(
                  (item): item is { column: string; percentage: number } =>
                    item !== null &&
                    !isNaN(item.percentage) &&
                    item.column !== 'pct_match',
                )
            }
          }

          const validPercentages = percentages.map(p => p.percentage)
          const highest =
            percentages.length > 0
              ? percentages.find(
                  p => p.percentage === Math.max(...validPercentages),
                )
              : null
          const lowest =
            percentages.length > 0
              ? percentages.find(
                  p => p.percentage === Math.min(...validPercentages),
                )
              : null
          const average =
            validPercentages.length > 0
              ? validPercentages.reduce((a, b) => a + b, 0) /
                validPercentages.length
              : 0

          return (
            <>
              <Card className="overflow-hidden">
                <div className={twMerge('h-1', twColors.bgSuccess)} />
                <div className="text-center py-2 px-2">
                  <div
                    className={twMerge(
                      'text-lg font-light mb-0.5',
                      twColors.textSuccess500,
                    )}
                  >
                    {highest ? `${highest.percentage.toFixed(1)}%` : 'N/A'}
                  </div>
                  <div
                    className={twMerge(
                      'text-xs font-medium',
                      twColors.textMuted,
                    )}
                  >
                    Highest Match
                  </div>
                  <div
                    className={twMerge('text-xs mt-0.5', twColors.textMuted)}
                  >
                    {highest ? highest.column : 'No data'}
                  </div>
                </div>
              </Card>

              <Card className="overflow-hidden">
                <div className={twMerge('h-1', twColors.bgPrimary)} />
                <div className="text-center py-2 px-2">
                  <div
                    className={twMerge(
                      'text-lg font-light mb-0.5',
                      twColors.textPrimary,
                    )}
                  >
                    {average > 0 ? `${average.toFixed(1)}%` : 'N/A'}
                  </div>
                  <div
                    className={twMerge(
                      'text-xs font-medium',
                      twColors.textMuted,
                    )}
                  >
                    Average Match
                  </div>
                  <div
                    className={twMerge('text-xs mt-0.5', twColors.textMuted)}
                  >
                    Across {validPercentages.length} columns
                  </div>
                </div>
              </Card>

              <Card className="overflow-hidden">
                <div className={twMerge('h-1', twColors.bgDanger)} />
                <div className="text-center py-2 px-2">
                  <div
                    className={twMerge(
                      'text-lg font-light mb-0.5',
                      twColors.textDanger500,
                    )}
                  >
                    {lowest ? `${lowest.percentage.toFixed(1)}%` : 'N/A'}
                  </div>
                  <div
                    className={twMerge(
                      'text-xs font-medium',
                      twColors.textMuted,
                    )}
                  >
                    Lowest Match
                  </div>
                  <div
                    className={twMerge('text-xs mt-0.5', twColors.textMuted)}
                  >
                    {lowest ? lowest.column : 'No data'}
                  </div>
                </div>
              </Card>
            </>
          )
        })()}
      </div>
    </div>
  )
}
