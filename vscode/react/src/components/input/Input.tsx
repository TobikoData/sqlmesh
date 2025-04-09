import clsx from 'clsx'
import React from 'react'
import { isNotNil } from '@/utils/index'
import { EnumSize, type Size } from '@/style/variants'
import Textfield from './Textfield'
import Selector from './Selector'

export interface PropsInput {
  label?: string
  info?: string
  size?: Size
  disabled?: boolean
  required?: boolean
  autoFocus?: boolean
  className?: string
  children?: ({
    disabled,
    required,
    autoFocus,
    size,
    className,
  }: {
    className: string
    disabled: boolean
    required: boolean
    autoFocus: boolean
    size: Size
  }) => React.ReactNode | React.ReactNode
}

function Input({
  label,
  info,
  size = EnumSize.md,
  className,
  children,
  disabled = false,
  required = false,
  autoFocus = false,
}: PropsInput): JSX.Element {
  const cn = clsx(
    'text-left relative block bg-theme-lighter border-neutral-200 dark:border-neutral-700',
    'focus:outline-none focus:border-secondary-500',
    'ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100',
    size === EnumSize.sm &&
      'px-2 py-0.5 text-xs leading-4 border-2 focus:ring-2 rounded-[0.25rem] min-w-[7rem]',
    size === EnumSize.md &&
      'px-3 py-2 text-sm leading-4 border-2 focus:ring-4 rounded-md min-w-[10rem]',
    size === EnumSize.lg &&
      'px-3 py-2 text-sm leading-6 border-2 focus:ring-4 rounded-md min-w-[10rem]',
  )

  return (
    <div
      className={clsx(
        'inline-block relative m-1 text-left',
        disabled && 'opacity-50 cursor-not-allowed',
        className,
      )}
    >
      {isNotNil(label) && <InputLabel size={size}>{label}</InputLabel>}
      {typeof children === 'function'
        ? children({ disabled, required, autoFocus, size, className: cn })
        : children}
      {isNotNil(info) && <InputInfo>{info}</InputInfo>}
    </div>
  )
}

function InputLabel({
  htmlFor,
  className,
  children,
  size = EnumSize.md,
}: {
  htmlFor?: string
  className?: string
  children: React.ReactNode
  size?: Size
}): JSX.Element {
  return (
    <label
      htmlFor={htmlFor}
      className={clsx(
        'block mb-1 px-2 text-sm whitespace-nowrap',
        size === EnumSize.sm && 'text-xs',
        size === EnumSize.md && 'text-sm',
        size === EnumSize.lg && 'text-md',
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
}: {
  className?: string
  children: React.ReactNode
}): JSX.Element {
  return (
    <small
      className={clsx(
        'block text-xs mt-1 px-3 text-neutral-400 dark:text-neutral-600',
        className,
      )}
    >
      {children}
    </small>
  )
}

Input.Label = InputLabel
Input.Info = InputInfo
Input.Textfield = Textfield
Input.Selector = Selector

export default Input
