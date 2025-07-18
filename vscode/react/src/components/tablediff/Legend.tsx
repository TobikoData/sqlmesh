import { twColors, twMerge } from './tailwind-utils'

interface DiffConfigProps {
  on: string[] | string[][]
  limit?: number
  where?: string
}

interface ConfigItemProps {
  label: string
  value: string | number
}

function ConfigItem({ label, value }: ConfigItemProps) {
  return (
    <div className="flex items-center gap-1">
      <span className={twMerge('text-xs font-medium', twColors.textMuted)}>
        {label}:
      </span>
      <code
        className={twMerge(
          'text-xs px-1 py-0.5 rounded',
          twColors.bgNeutral10,
          twColors.textForeground,
        )}
      >
        {value}
      </code>
    </div>
  )
}

export function DiffConfig({ on, limit, where }: DiffConfigProps) {
  // Handle the grain (join keys)
  const grainColumns = Array.isArray(on[0])
    ? on.flat().filter((col, index, arr) => arr.indexOf(col) === index) // Remove duplicates from nested array
    : (on as string[])

  return (
    <div className="flex items-center gap-4 flex-wrap text-xs">
      <ConfigItem
        label="Grain"
        value={grainColumns.join(', ')}
      />
      {limit && (
        <ConfigItem
          label="Limit"
          value={limit}
        />
      )}
      {where && (
        <ConfigItem
          label="Where"
          value={where}
        />
      )}
    </div>
  )
}
