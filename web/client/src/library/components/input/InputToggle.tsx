import Toggle from '../toggle/Toggle'

interface PropsInputToggle {
  label: string
  info: string
  enabled: boolean
  setEnabled: (enabled: boolean) => void
}

export default function InputToggle({
  label,
  info,
  enabled,
  setEnabled,
}: PropsInputToggle): JSX.Element {
  return (
    <div className="flex justify-between">
      <label className="block mb-1 px-3 text-sm font-bold">
        {label}
        <small className="block text-xs text-gray-500">{info}</small>
      </label>
      <Toggle
        className="mt-2"
        enabled={enabled}
        setEnabled={setEnabled}
      />
    </div>
  )
}
