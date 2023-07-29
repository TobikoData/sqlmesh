import React from 'react'
import clsx from 'clsx'
import {
  EnumSize,
  type Size,
  type Variant,
  type EnumVariant,
} from '../../../types/enum'

export type ButtonVariant = Subset<
  Variant,
  | typeof EnumVariant.Primary
  | typeof EnumVariant.Secondary
  | typeof EnumVariant.Success
  | typeof EnumVariant.Danger
  | typeof EnumVariant.Warning
  | typeof EnumVariant.Alternative
  | typeof EnumVariant.Neutral
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
    'border-primary-500 bg-primary-500 hover:bg-primary-400 active:bg-primary-400 text-light',
  ],
  [
    'alternative',
    'border-primary-500 bg-primary-10 hover:bg-primary-20 active:bg-primary-200 text-primary-500',
  ],
  [
    'secondary',
    'border-secondary-500 bg-secondary-500 hover:bg-secondary-600 active:bg-secondary-400 text-neutral-100',
  ],
  [
    'success',
    'border-success-500 bg-success-500 hover:bg-success-600 active:bg-success-400 text-neutral-100',
  ],
  [
    'danger',
    'border-danger-500 bg-danger-500 hover:bg-danger-600 active:bg-danger-400 text-neutral-100',
  ],
  [
    'warning',
    'border-warning-500 bg-warning-500 hover:bg-warning-600 active:bg-warning-400 text-neutral-100',
  ],
  [
    'neutral',
    'border-neutral-200 bg-neutral-200 hover:bg-neutral-300 active:bg-neutral-300 text-primary-900',
  ],
])

const SHAPE = new Map<ButtonShape, string>([
  ['rounded', `rounded-md`],
  ['square', `rounded-none`],
  ['circle', `rounded-full`],
])

const SIZE = new Map<ButtonSize, string>([
  [EnumSize.xs, `text-xs leading-2 border`],
  [EnumSize.sm, `px-2 py-[0.125rem] text-xs leading-4 border-2`],
  [EnumSize.md, `px-3 py-2 text-base leading-6 border-2`],
  [EnumSize.lg, `px-4 py-3 text-lg border-4`],
])

const Button = makeButton(
  React.forwardRef<HTMLButtonElement, PropsButton>(ButtonPlain),
)

const ButtonLink = makeButton(
  React.forwardRef<HTMLDivElement, PropsButton>(ButtonLinkPlain),
)

export { VARIANT, SHAPE, SIZE, Button, ButtonLink, makeButton }

function ButtonPlain(
  {
    type = 'button',
    disabled = false,
    children = [],
    form,
    autoFocus,
    tabIndex,
    onClick,
    className,
  }: PropsButton,
  ref?: React.ForwardedRef<HTMLButtonElement>,
): JSX.Element {
  return (
    <button
      ref={ref}
      type={type}
      autoFocus={autoFocus}
      tabIndex={tabIndex}
      form={form}
      disabled={disabled}
      onClick={onClick}
      onKeyDown={(e: React.KeyboardEvent<HTMLButtonElement>) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault()
          e.stopPropagation()

          onClick?.(e as unknown as React.MouseEvent<HTMLButtonElement>)
        }
      }}
      className={className}
    >
      {children}
    </button>
  )
}

function ButtonLinkPlain(
  { children = [], autoFocus, tabIndex, className }: PropsButton,
  ref?: React.ForwardedRef<HTMLDivElement>,
): JSX.Element {
  return (
    <div
      ref={ref}
      autoFocus={autoFocus}
      tabIndex={tabIndex}
      className={className}
    >
      {children}
    </div>
  )
}

function makeButton<TElement = HTMLButtonElement>(
  Component: React.ElementType,
): React.ForwardRefExoticComponent<
  PropsButton & React.RefAttributes<TElement>
> {
  return React.forwardRef<TElement, PropsButton>(function Wrapper(
    {
      type = 'button',
      disabled = false,
      variant = 'primary',
      shape = 'rounded',
      size = EnumSize.md,
      children = [],
      className,
      form,
      autoFocus,
      tabIndex,
      onClick,
    }: PropsButton,
    ref?: React.ForwardedRef<TElement>,
  ): JSX.Element {
    return (
      <Component
        ref={ref}
        type={type}
        disabled={disabled}
        form={form}
        autoFocus={autoFocus}
        tabIndex={tabIndex}
        onClick={onClick}
        className={clsx(
          'whitespace-nowrap flex m-1 items-center justify-center font-bold',
          'focus:ring-4 focus:outline-none focus:border-secondary-500',
          'ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100',
          SHAPE.get(shape),
          SIZE.get(size),
          disabled
            ? 'opacity-50 bg-neutral-10 border-neutral-300 text-prose cursor-not-allowed'
            : VARIANT.get(variant),
          className,
        )}
      >
        {children}
      </Component>
    )
  })
}
