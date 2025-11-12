import { Card, CardContent } from './Card'
import { DiffConfig } from './Legend'
import { twColors, twMerge } from './tailwind-utils'
import type { TableDiffData } from './types'

interface HeaderCardProps {
  schemaDiff: TableDiffData['schema_diff']
  rowDiff: TableDiffData['row_diff']
  limit: number
  whereClause: string
  onColumns: string
  on: string[][] | undefined
  where: string | undefined
  isRerunning: boolean
  onLimitChange: (limit: number) => void
  onWhereClauseChange: (where: string) => void
  onOnColumnsChange: (on: string) => void
  onRerun: () => void
  hasChanges: boolean
}

export function HeaderCard({
  schemaDiff,
  rowDiff,
  limit,
  whereClause,
  onColumns,
  on,
  where,
  isRerunning,
  onLimitChange,
  onWhereClauseChange,
  onOnColumnsChange,
  onRerun,
  hasChanges,
}: HeaderCardProps) {
  const formatPercentage = (v: number) => `${v.toFixed(1)}%`
  const formatCount = (v: number) => v.toLocaleString()

  return (
    <Card className="mb-4">
      <CardContent className="py-4">
        <div className="flex items-center gap-3 flex-wrap">
          <span className={twMerge('text-sm font-medium', twColors.textSource)}>
            Source:
          </span>
          <code
            className={twMerge(
              'px-3 py-1.5 rounded-lg text-sm whitespace-nowrap',
              twColors.bgSource,
              'text-white',
            )}
          >
            {schemaDiff.source}
          </code>
          <span
            className={twMerge('text-sm font-medium ml-4', twColors.textTarget)}
          >
            Target:
          </span>
          <code
            className={twMerge(
              'px-3 py-1.5 rounded-lg text-sm whitespace-nowrap',
              twColors.bgTarget,
              'text-white',
            )}
          >
            {schemaDiff.target}
          </code>
        </div>
        <div className="flex items-center gap-6 text-sm flex-wrap mt-3">
          <div className="flex items-center gap-2">
            <span className={twColors.textMuted}>Source rows:</span>
            <span className={twMerge('font-semibold', twColors.textSource)}>
              {formatCount(rowDiff.source_count)}
            </span>
          </div>
          <div className="flex items-center gap-2">
            <span className={twColors.textMuted}>Target rows:</span>
            <span className={twMerge('font-semibold', twColors.textTarget)}>
              {formatCount(rowDiff.target_count)}
            </span>
          </div>
          <div className="flex items-center gap-2">
            <span className={twColors.textMuted}>Change:</span>
            <span
              className={twMerge(
                'font-semibold',
                rowDiff.count_pct_change > 0
                  ? twColors.textSuccess500
                  : rowDiff.count_pct_change < 0
                    ? twColors.textDanger500
                    : twColors.textMuted,
              )}
            >
              {formatPercentage(rowDiff.count_pct_change)}
            </span>
          </div>
        </div>
        <div className="mt-4 space-y-3">
          <div className="flex flex-wrap gap-3 items-end">
            <div className="flex flex-col gap-1">
              <label
                className={twMerge('text-xs font-medium', twColors.textMuted)}
              >
                Limit:
              </label>
              <input
                type="number"
                value={limit}
                onChange={e =>
                  onLimitChange(Math.max(1, parseInt(e.target.value) || 1))
                }
                className={twMerge(
                  'w-20 px-2 py-1 text-sm rounded border',
                  'bg-[var(--vscode-input-background)]',
                  'border-[var(--vscode-input-border)]',
                  'text-[var(--vscode-input-foreground)]',
                  'focus:outline-none focus:ring-1 focus:ring-[var(--vscode-focusBorder)]',
                )}
                min="1"
                max="10000"
                disabled={isRerunning}
              />
            </div>
            <div className="flex flex-col gap-1 flex-1 max-w-sm">
              <label
                className={twMerge('text-xs font-medium', twColors.textMuted)}
              >
                Where:
              </label>
              <input
                type="text"
                value={whereClause}
                onChange={e => onWhereClauseChange(e.target.value)}
                placeholder="e.g. created_at > '2024-01-01'"
                className={twMerge(
                  'px-2 py-1 text-sm rounded border',
                  'bg-[var(--vscode-input-background)]',
                  'border-[var(--vscode-input-border)]',
                  'text-[var(--vscode-input-foreground)]',
                  'placeholder:text-[var(--vscode-input-placeholderForeground)]',
                  'focus:outline-none focus:ring-1 focus:ring-[var(--vscode-focusBorder)]',
                )}
                disabled={isRerunning}
              />
            </div>
            <div className="flex flex-col gap-1 flex-1 max-w-xs">
              <label
                className={twMerge('text-xs font-medium', twColors.textMuted)}
              >
                On (grain):
              </label>
              <input
                type="text"
                value={onColumns}
                onChange={e => onOnColumnsChange(e.target.value)}
                placeholder="e.g. s.id = t.id AND s.date = t.date"
                className={twMerge(
                  'px-2 py-1 text-sm rounded border',
                  'bg-[var(--vscode-input-background)]',
                  'border-[var(--vscode-input-border)]',
                  'text-[var(--vscode-input-foreground)]',
                  'placeholder:text-[var(--vscode-input-placeholderForeground)]',
                  'focus:outline-none focus:ring-1 focus:ring-[var(--vscode-focusBorder)]',
                )}
                disabled={isRerunning}
              />
            </div>
            <button
              onClick={onRerun}
              disabled={isRerunning || !hasChanges}
              className={twMerge(
                'px-4 py-1.5 text-sm rounded font-medium transition-colors',
                'bg-[var(--vscode-button-background)]',
                'text-[var(--vscode-button-foreground)]',
                'hover:bg-[var(--vscode-button-hoverBackground)]',
                'disabled:opacity-50 disabled:cursor-not-allowed',
                'focus:outline-none focus:ring-1 focus:ring-[var(--vscode-focusBorder)]',
                hasChanges &&
                  !isRerunning &&
                  'bg-[var(--vscode-button-secondaryBackground)] ring-1 ring-[var(--vscode-button-secondaryForeground)]',
              )}
            >
              {isRerunning ? 'Running...' : 'Rerun'}
            </button>
          </div>
          <div className="flex justify-end">
            {on && (
              <DiffConfig
                on={on}
                limit={limit}
                where={where}
              />
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
