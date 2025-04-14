import { EnumSize } from '~/types/enum'
import Toggle from '../toggle/Toggle'
import clsx from 'clsx'

export default function InputToggle({
  label,
  info,
  enabled,
  disabled = false,
  setEnabled,
  className,
}: {
  label: string
  enabled: boolean
  setEnabled: (enabled: boolean) => void
  info?: string
  disabled?: boolean
  className?: string
}): JSX.Element {
  return (
    <div className={clsx('flex justify-between', className)}>
      <label className="block mb-1 px-3 text-sm font-bold">
        {label}
        <small className="block text-xs text-neutral-500">{info}</small>
      </label>
      <Toggle
        disabled={disabled}
        enabled={enabled}
        setEnabled={setEnabled}
        size={EnumSize.lg}
      />
    </div>
  )
}
