import { type TableDiffData } from './types'
import { twColors, twMerge } from './tailwind-utils'
import { Card, CardContent } from './Card'

interface RowStatsSectionProps {
  rowDiff: TableDiffData['row_diff']
}

export function RowStatsSection({ rowDiff }: RowStatsSectionProps) {
  const formatPercentage = (v: number) => `${(v * 100).toFixed(1)}%`
  const formatCount = (v: number) => v.toLocaleString()

  const fullMatchCount = Math.round(rowDiff.stats.full_match_count || 0)
  const joinCount = Math.round(rowDiff.stats.join_count || 0)
  const partialMatchCount = joinCount - fullMatchCount
  const sOnlyCount = Math.round(rowDiff.stats.s_only_count || 0)
  const tOnlyCount = Math.round(rowDiff.stats.t_only_count || 0)
  const totalRows = rowDiff.source_count + rowDiff.target_count
  const fullMatchPct = totalRows > 0 ? (2 * fullMatchCount) / totalRows : 0

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
      {/* Full Match Card */}
      <Card className="overflow-hidden">
        <div className={twMerge('h-1', twColors.bgSuccess)} />
        <CardContent className="text-center py-3 px-3">
          <div
            className={twMerge(
              'text-2xl font-light mb-1',
              twColors.textSuccess500,
            )}
          >
            {formatCount(fullMatchCount)}
          </div>
          <div className={twMerge('text-xs font-medium', twColors.textMuted)}>
            Full Matches
          </div>
          <div className={twMerge('text-xs mt-0.5', twColors.textMuted)}>
            {formatPercentage(fullMatchPct)}
          </div>
        </CardContent>
      </Card>

      {/* Partial Match Card */}
      <Card className="overflow-hidden">
        <div className={twMerge('h-1', twColors.bgPrimary)} />
        <CardContent className="text-center py-3 px-3">
          <div
            className={twMerge(
              'text-2xl font-light mb-1',
              twColors.textPrimary,
            )}
          >
            {formatCount(partialMatchCount)}
          </div>
          <div className={twMerge('text-xs font-medium', twColors.textMuted)}>
            Partial Matches
          </div>
          <div className={twMerge('text-xs mt-0.5', twColors.textMuted)}>
            {formatPercentage(partialMatchCount / totalRows)}
          </div>
        </CardContent>
      </Card>

      {/* Source Only Card */}
      <Card className="overflow-hidden">
        <div className={twMerge('h-1', twColors.bgSource)} />
        <CardContent className="text-center py-3 px-3">
          <div
            className={twMerge('text-2xl font-light mb-1', twColors.textSource)}
          >
            {formatCount(sOnlyCount)}
          </div>
          <div className={twMerge('text-xs font-medium', twColors.textMuted)}>
            Source Only
          </div>
          <div className={twMerge('text-xs mt-0.5', twColors.textMuted)}>
            {formatPercentage(sOnlyCount / totalRows)}
          </div>
        </CardContent>
      </Card>

      {/* Target Only Card */}
      <Card className="overflow-hidden">
        <div className={twMerge('h-1', twColors.bgTarget)} />
        <CardContent className="text-center py-3 px-3">
          <div
            className={twMerge('text-2xl font-light mb-1', twColors.textTarget)}
          >
            {formatCount(tOnlyCount)}
          </div>
          <div className={twMerge('text-xs font-medium', twColors.textMuted)}>
            Target Only
          </div>
          <div className={twMerge('text-xs mt-0.5', twColors.textMuted)}>
            {formatPercentage(tOnlyCount / totalRows)}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
