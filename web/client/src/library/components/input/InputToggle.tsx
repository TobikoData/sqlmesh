import Toggle from '../toggle/Toggle'

export default function InputToggle({
  label,
  info,
  setEnabled,
  enabled,
}: any): JSX.Element {
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
