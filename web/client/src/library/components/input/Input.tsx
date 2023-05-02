import clsx from 'clsx'
import { EnumSize, type Size } from '~/types/enum'
import { isTrue } from '~/utils'

export interface PropsInput extends React.HTMLAttributes<HTMLElement> {
  value?: string | number
  type?: string
  label?: string
  info?: string
  placeholder?: string
  size?: Size
  disabled?: boolean
  onInput: (e: React.ChangeEvent<HTMLInputElement>) => void
}

function Input({
  type = 'text',
  label,
  info,
  value = '',
  placeholder,
  className,
  size = EnumSize.md,
  disabled = false,
  autoFocus = false,
  onInput,
}: PropsInput): JSX.Element {
  return (
    <div className={clsx('inline-block relative m-1', className)}>
      {label != null && <InputLabel>{label}</InputLabel>}
      <input
        className={clsx(
          'flex w-full text-prose-lighter bg-theme-lighter border-theme-darker dark:border-theme-lighter dark:text-prose-darker rounded-md',
          'border-2 focus:ring-4 focus:outline-none focus:border-secondary-500',
          'ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100',
          size === EnumSize.md && 'px-3 py-2 text-sm leading-6',
          size === EnumSize.sm && 'px-3 py-1 text-xs leading-4',
          isTrue(disabled) && 'opacity-50 cursor-not-allowed',
        )}
        type={type}
        value={value}
        placeholder={placeholder}
        onInput={onInput}
        disabled={disabled}
        autoFocus={autoFocus}
      />
      {info != null && <InputInfo>{info}</InputInfo>}
    </div>
  )
}

function InputLabel({
  htmlFor,
  className,
  children,
}: React.LabelHTMLAttributes<HTMLLabelElement>): JSX.Element {
  return (
    <label
      htmlFor={htmlFor}
      className={clsx(
        'block mb-1 px-3 text-sm font-bold whitespace-nowrap',
        className,
      )}
    >
      {children}
    </label>
  )
}

function InputInfo({
  className,
  children,
}: React.HTMLAttributes<HTMLElement>): JSX.Element {
  return (
    <small
      className={clsx('block text-xs mt-1 px-3 text-neutral-500p', className)}
    >
      {children}
    </small>
  )
}

Input.Label = InputLabel
Input.Info = InputInfo

export default Input
