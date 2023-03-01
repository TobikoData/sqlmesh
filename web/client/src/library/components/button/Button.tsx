import clsx from 'clsx'
import { EnumSize } from '../../../types/enum'
import { Menu } from '@headlessui/react'
import { ForwardedRef, forwardRef } from 'react'

export type ButtonVariant =
  | 'primary'
  | 'secondary'
  | 'success'
  | 'danger'
  | 'warning'
  | 'alternative'

export type ButtonSize = Subset<
  Size,
  | typeof EnumSize.xs
  | typeof EnumSize.sm
  | typeof EnumSize.md
  | typeof EnumSize.lg
>
export type ButtonShape = 'square' | 'rounded' | 'circle' | 'pill'

export interface PropsButton
  extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
  size?: ButtonSize
  shape?: ButtonShape
  value?: string
  form?: string
  onClick?: (e: React.MouseEvent<HTMLButtonElement>) => void
}

const VARIANT = new Map<ButtonVariant, string>([
  [
    'primary',
    'border-secondary-100 bg-secondary-100 hover:bg-secondary-100 active:bg-secondary-100 text-secondary-600',
  ],
  [
    'secondary',
    'border-secondary-500 bg-secondary-500 hover:bg-secondary-600 active:bg-secondary-400 text-gray-100',
  ],
  [
    'success',
    'bg-success-500 hover:bg-success-600 active:bg-success-400 text-gray-100',
  ],
  [
    'danger',
    'bg-danger-500 hover:bg-danger-600 active:bg-danger-400 text-gray-100',
  ],
  [
    'warning',
    'bg-warning-500 hover:bg-warning-600 active:bg-warning-400 text-gray-100',
  ],
  [
    'alternative',
    'border-gray-100 bg-gray-100 hover:bg-gray-200 active:bg-gray-200 text-gray-800',
  ],
])

const SHAPE = new Map<ButtonShape, string>([
  ['rounded', `rounded-md`],
  ['circle', `rounded-full`],
])

const SIZE = new Map<ButtonSize, string>([
  [EnumSize.xs, `px-2 py-0 text-xs leading-2`],
  [EnumSize.sm, `px-2 py-1 text-xs leading-4`],
  [EnumSize.md, `px-3 py-2 text-base leading-6`],
  [EnumSize.lg, `px-4 py-3 text-lg`],
])

export const Button = forwardRef(function Button(
  {
    type = 'button',
    disabled = false,
    variant = 'secondary',
    shape = 'rounded',
    size = EnumSize.md,
    children = [],
    form,
    className,
    autoFocus,
    tabIndex,
    onClick,
  }: PropsButton,
  ref: ForwardedRef<HTMLButtonElement>,
): JSX.Element {
  return (
    <button
      ref={ref}
      type={type}
      autoFocus={autoFocus}
      tabIndex={tabIndex}
      form={form}
      disabled={disabled}
      className={clsx(
        'whitespace-nowrap flex m-1 items-center justify-center',
        'border-2 focus:ring-4 focus:outline-none focus:border-secondary-500',
        'ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100',
        SHAPE.get(shape),
        SIZE.get(size),
        disabled
          ? 'opacity-50 bg-gray-100 hover:bg-gray-100 active:bg-gray-100 text-gray-900 cursor-not-allowed'
          : VARIANT.get(variant),
        className,
      )}
      onClick={onClick}
    >
      {children}
    </button>
  )
})

export const ButtonMenu = forwardRef(function ButtonMenu(
  {
    variant = 'secondary',
    shape = 'rounded',
    size = EnumSize.md,
    children = [],
    disabled = false,
    className,
  }: PropsButton,
  ref: ForwardedRef<HTMLButtonElement>,
): JSX.Element {
  return (
    <Menu.Button
      ref={ref}
      className={clsx(
        'whitespace-nowrap flex m-1 items-center justify-center',
        'border-2 focus:ring-4 focus:outline-none focus:border-secondary-500',
        'ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100',
        SHAPE.get(shape),
        SIZE.get(size),
        disabled
          ? 'opacity-50 bg-gray-100 hover:bg-gray-100 active:bg-gray-100 text-gray-900 cursor-not-allowed'
          : VARIANT.get(variant),
        className,
      )}
      disabled={disabled}
    >
      {children}
    </Menu.Button>
  )
})
