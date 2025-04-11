import clsx from 'clsx'
import React from 'react'
import { isFalse } from '@/utils/index'

export interface PropsTextfield {
  value?: string | number | undefined
  type?: string
  placeholder?: string
  disabled?: boolean
  autoFocus?: boolean
  className?: string
  onInput?: (e: React.ChangeEvent<HTMLInputElement>) => void
  onKeyDown?: (e: React.KeyboardEvent<HTMLInputElement>) => void
}

export default React.forwardRef<HTMLInputElement, PropsTextfield>(
  function Input(
    {
      type = 'text',
      value,
      placeholder,
      className,
      disabled = false,
      autoFocus = false,
      onInput,
      onKeyDown,
    }: PropsTextfield,
    ref?: React.Ref<HTMLInputElement>,
  ): JSX.Element {
    value = value ?? ''
    return (
      <input
        ref={ref}
        className={clsx(
          'placeholder:text-neutral-300 dark:placeholder:text-neutral-700',
          disabled && 'opacity-50 cursor-not-allowed',
          className,
        )}
        type={type}
        value={value}
        placeholder={placeholder}
        disabled={disabled}
        autoFocus={autoFocus}
        readOnly={isFalse(Boolean(onInput))}
        onInput={onInput}
        onKeyDown={onKeyDown}
      />
    )
  },
)
