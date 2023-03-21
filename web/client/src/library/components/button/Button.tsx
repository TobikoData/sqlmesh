import clsx from 'clsx'
import {
  EnumSize,
  type Size,
  type Variant,
  type EnumVariant,
} from '../../../types/enum'
import { Menu } from '@headlessui/react'
import { type ForwardedRef, forwardRef } from 'react'

export type ButtonVariant = Subset<
  Variant,
  | typeof EnumVariant.Primary
  | typeof EnumVariant.Secondary
  | typeof EnumVariant.Success
  | typeof EnumVariant.Danger
  | typeof EnumVariant.Warning
  | typeof EnumVariant.Alternative
  | typeof EnumVariant.Nutral
>

export type ButtonSize = Subset<
  Size,
  | typeof EnumSize.xs
  | typeof EnumSize.sm
  | typeof EnumSize.md
  | typeof EnumSize.lg
>

export const EnumButtonShape = {
  Square: 'square',
  Rounded: 'rounded',
  Circle: 'circle',
  Pill: 'pill',
} as const

export const EnumButtonFormat = {
  Solid: 'solid',
  Outline: 'outline',
  Ghost: 'ghost',
  Link: 'link',
} as const

export type ButtonShape = (typeof EnumButtonShape)[keyof typeof EnumButtonShape]
export type ButtonFormat =
  (typeof EnumButtonFormat)[keyof typeof EnumButtonFormat]

export interface PropsButton
  extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
  size?: ButtonSize
  shape?: ButtonShape
  format?: ButtonFormat
  value?: string
  form?: string
  onClick?: (e: React.MouseEvent<HTMLButtonElement>) => void
}

const VARIANT = new Map<ButtonVariant, string>([
  [
    'primary',
    'border-primary-500 bg-primary-500 hover:bg-primary-400 active:bg-primary-400 text-primary-100',
  ],
  [
    'alternative',
    'border-primary-500 bg-primary-10 hover:bg-primary-20 active:bg-primary-200 text-primary-500',
  ],
  [
    'secondary',
    'border-secondary-500 bg-secondary-500 hover:bg-secondary-600 active:bg-secondary-400 text-nutral-100',
  ],
  [
    'success',
    'bg-success-500 hover:bg-success-600 active:bg-success-400 text-nutral-100',
  ],
  [
    'danger',
    'bg-danger-500 hover:bg-danger-600 active:bg-danger-400 text-nutral-100',
  ],
  [
    'warning',
    'bg-warning-500 hover:bg-warning-600 active:bg-warning-400 text-nutral-100',
  ],
  [
    'nutral',
    'border-nutral-200 bg-nutral-200 hover:bg-nutral-300 active:bg-nutral-300 text-primary-900',
  ],
])

const SHAPE = new Map<ButtonShape, string>([
  ['rounded', `rounded-md`],
  ['circle', `rounded-full`],
])

const SIZE = new Map<ButtonSize, string>([
  [EnumSize.xs, `px-2 py-0 text-xs leading-2 border`],
  [EnumSize.sm, `px-2 py-[0.125rem] text-xs leading-4 border-2`],
  [EnumSize.md, `px-3 py-2 text-base leading-6 border-2`],
  [EnumSize.lg, `px-4 py-3 text-lg border-4`],
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
        'whitespace-nowrap flex m-1 items-center justify-center font-bold',
        'focus:ring-4 focus:outline-none focus:border-secondary-500',
        'ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100',
        SHAPE.get(shape),
        SIZE.get(size),
        disabled
          ? 'opacity-50 bg-nutral-200 border-nutral-300 text-nutral-700 cursor-not-allowed'
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
          ? 'opacity-50 bg-nutral-100 hover:bg-nutral-100 active:bg-nutral-100 text-nutral-900 cursor-not-allowed'
          : VARIANT.get(variant),
        className,
      )}
      disabled={disabled}
    >
      {children}
    </Menu.Button>
  )
})
