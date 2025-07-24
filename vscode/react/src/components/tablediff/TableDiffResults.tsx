import { HeaderCard } from './HeaderCard'
import { ContentSections } from './ContentSections'
import { RerunController } from './RerunController'
import { type TableDiffData } from './types'
import { twColors, twMerge } from './tailwind-utils'

interface Props {
  data: TableDiffData
  onDataUpdate?: (data: TableDiffData) => void
}

export function TableDiffResults({ data, onDataUpdate }: Props) {
  if (!data)
    return (
      <div className={twMerge('p-4', twColors.textForeground)}>
        No data available
      </div>
    )

  return (
    <RerunController
      data={data}
      onDataUpdate={onDataUpdate}
    >
      {({
        limit,
        whereClause,
        onColumns,
        isRerunning,
        hasChanges,
        setLimit,
        setWhereClause,
        setOnColumns,
        handleRerun,
      }) => (
        <div
          className={twMerge(
            'h-full w-full text-[13px] font-sans p-4',
            'bg-[var(--vscode-editor-background)]',
            twColors.textForeground,
            isRerunning && 'pointer-events-none opacity-75',
          )}
        >
          <HeaderCard
            schemaDiff={data.schema_diff}
            rowDiff={data.row_diff}
            limit={limit}
            whereClause={whereClause}
            onColumns={onColumns}
            on={data.on}
            where={data.where}
            isRerunning={isRerunning}
            onLimitChange={setLimit}
            onWhereClauseChange={setWhereClause}
            onOnColumnsChange={setOnColumns}
            onRerun={handleRerun}
            hasChanges={hasChanges}
          />
          <ContentSections data={data} />
        </div>
      )}
    </RerunController>
  )
}
