import clsx from 'clsx'
import { EnumSize } from '~/types/enum'
import { isTrue } from '~/utils'

export interface PropsInput extends React.HTMLAttributes<HTMLElement> {
  value: string | number
  type?: string
  label?: string
  info?: string
  placeholder?: string
  size?: Size
  disabled?: boolean
  onInput?: (e: React.ChangeEvent<HTMLInputElement>) => void
}

export default function Input({
  type = 'text',
  label,
  info,
  value,
  placeholder,
  className,
  size = EnumSize.md,
  disabled = false,
  onInput,
}: PropsInput): JSX.Element {
  return (
    <div className={clsx('inline-block relative m-1', className)}>
      {label != null && (
        <label className="block mb-1 px-3 text-sm font-bold">{label}</label>
      )}
      <input
        className={clsx(
          'flex w-full bg-secondary-100 rounded-md border-secondary-100',
          'border-2 focus:ring-4 focus:outline-none focus:border-secondary-500',
          'ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100',
          size === EnumSize.md && 'px-3 py-2 text-base leading-6',
          size === EnumSize.sm && 'px-3 py-1 text-xs leading-4',
          isTrue(disabled) && 'opacity-50 cursor-not-allowed',
        )}
        type={type}
        value={value}
        placeholder={placeholder}
        onInput={onInput}
        disabled={disabled}
      />
      {info != null && (
        <small className="block text-xs mt-1 px-3 text-gray-500">{info}</small>
      )}
    </div>
  )
}
