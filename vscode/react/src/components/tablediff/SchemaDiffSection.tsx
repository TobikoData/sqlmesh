import { useMemo } from 'react'
import { type TableDiffData } from './types'
import { twColors, twMerge } from './tailwind-utils'

interface SchemaDiffSectionProps {
  schemaDiff: TableDiffData['schema_diff']
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
      bgClass: twColors.bgSuccess10,
      borderClass: 'border-l-4 ' + twColors.borderSuccess500,
      textClass: twColors.textSuccess500,
      symbol: '+',
    },
    removed: {
      bgClass: twColors.bgDanger10,
      borderClass: 'border-l-4 ' + twColors.borderDanger500,
      textClass: twColors.textDanger500,
      symbol: '-',
    },
    modified: {
      bgClass: twColors.bgPrimary10,
      borderClass: 'border-l-4 ' + twColors.borderPrimary,
      textClass: twColors.textPrimary,
      symbol: '~',
    },
  }

  const { bgClass, borderClass, textClass, symbol } = styleMap[changeType]

  return (
    <div
      className={twMerge(
        'flex items-center gap-3 text-sm px-4 py-3 rounded-lg mb-2',
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

export function SchemaDiffSection({ schemaDiff }: SchemaDiffSectionProps) {
  const schemaHasChanges = useMemo(() => {
    return (
      Object.keys(schemaDiff.added || {}).length > 0 ||
      Object.keys(schemaDiff.removed || {}).length > 0 ||
      Object.keys(schemaDiff.modified || {}).length > 0
    )
  }, [schemaDiff])

  return (
    <div>
      {!schemaHasChanges ? (
        <div
          className={twMerge(
            'text-sm px-4 py-3 rounded-lg',
            twColors.bgSuccess10,
            twColors.textSuccess500,
          )}
        >
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
  )
}
