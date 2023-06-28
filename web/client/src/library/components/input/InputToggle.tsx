import { EnumSize } from '~/types/enum'
import Toggle from '../toggle/Toggle'

interface PropsInputToggle {
  label: string
  info: string
  enabled: boolean
  disabled?: boolean
  setEnabled: (enabled: boolean) => void
}

export default function InputToggle({
  label,
  info,
  enabled,
  disabled = false,
  setEnabled,
}: PropsInputToggle): JSX.Element {
  return (
    <div className="flex justify-between">
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
