import { useMemo } from 'react'
import { SectionToggle } from './SectionToggle'
import { type TableDiffData } from './types'
import { twColors, twMerge } from './tailwind-utils'

interface SchemaDiffSectionProps {
  schemaDiff: TableDiffData['schema_diff']
  expanded: boolean
  onToggle: () => void
}

interface SchemaChangeItemProps {
  column: string
  type: string
  changeType: 'added' | 'removed' | 'modified'
}

const SchemaChangeItem = ({
  column,
  type,
  changeType,
}: SchemaChangeItemProps) => {
  const styleMap = {
    added: {
      bgClass: twColors.bgAdded,
      borderClass: 'border-l-2 ' + twColors.borderAdded,
      textClass: twColors.textAdded,
      symbol: '+',
    },
    removed: {
      bgClass: twColors.bgRemoved,
      borderClass: 'border-l-2 ' + twColors.borderRemoved,
      textClass: twColors.textRemoved,
      symbol: '-',
    },
    modified: {
      bgClass: twColors.bgModified,
      borderClass: 'border-l-2 ' + twColors.borderModified,
      textClass: twColors.textModified,
      symbol: '~',
    },
  }

  const { bgClass, borderClass, textClass, symbol } = styleMap[changeType]

  return (
    <div
      className={twMerge(
        'flex items-center gap-2 text-xs pl-3 py-1 rounded-r',
        bgClass,
        borderClass,
      )}
    >
      <span className={twMerge('font-mono font-bold', textClass)}>
        {symbol}
      </span>
      <span
        className={twMerge('font-mono truncate', textClass)}
        title={column}
      >
        {column}
      </span>
      <span className={twColors.textMuted}>:</span>
      <span
        className={twMerge('truncate', textClass)}
        title={type}
      >
        {type}
      </span>
    </div>
  )
}

export function SchemaDiffSection({
  schemaDiff,
  expanded,
  onToggle,
}: SchemaDiffSectionProps) {
  const schemaHasChanges = useMemo(() => {
    return (
      Object.keys(schemaDiff.added || {}).length > 0 ||
      Object.keys(schemaDiff.removed || {}).length > 0 ||
      Object.keys(schemaDiff.modified || {}).length > 0
    )
  }, [schemaDiff])

  return (
    <SectionToggle
      id="schema"
      title="Schema Changes"
      expanded={expanded}
      onToggle={onToggle}
    >
      <div className="px-8 py-3 space-y-2">
        {!schemaHasChanges ? (
          <div className={twMerge('text-xs', twColors.textSuccess)}>
            ✓ Schemas are identical
          </div>
        ) : (
          <>
            {Object.entries(schemaDiff.added).map(([col, type]) => (
              <SchemaChangeItem
                key={col}
                column={col}
                type={type}
                changeType="added"
              />
            ))}
            {Object.entries(schemaDiff.removed).map(([col, type]) => (
              <SchemaChangeItem
                key={col}
                column={col}
                type={type}
                changeType="removed"
              />
            ))}
            {Object.entries(schemaDiff.modified).map(([col, type]) => (
              <SchemaChangeItem
                key={col}
                column={col}
                type={type}
                changeType="modified"
              />
            ))}
          </>
        )}
      </div>
    </SectionToggle>
  )
}
