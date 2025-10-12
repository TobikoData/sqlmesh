import React from 'react'
import { Slot } from '@radix-ui/react-slot'
import { cva } from 'class-variance-authority'

import { cn } from '@sqlmesh-common/utils'
import type { Shape, Size } from '@sqlmesh-common/types'

import './Button.css'

export type ButtonVariant =
  | 'primary'
  | 'secondary'
  | 'alternative'
  | 'destructive'
  | 'danger'
  | 'transparent'

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
  size?: Size
  shape?: Shape
  asChild?: boolean
}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, disabled, size, asChild = false, ...props }, ref) => {
    const Comp = asChild ? Slot : 'button'
    return (
      <Comp
        data-component="Button"
        className={cn(
          buttonVariants({ variant, size, className }),
          disabled && 'pointer-events-none bg-neutral-150 text-prose',
        )}
        disabled={disabled}
        ref={ref}
        {...props}
      />
    )
  },
)
Button.displayName = 'Button'

const size: Record<Size, string> = {
  '2xs': 'h-5 px-2 text-2xs leading-none rounded-2xs',
  xs: 'h-6 px-2 text-2xs rounded-xs',
  s: 'h-7 px-3 text-xs rounded-sm',
  m: 'h-8 px-4 rounded-md',
  l: 'h-9 px-4 rounded-lg',
  xl: 'h-10 px-4 rounded-xl',
  '2xl': 'h-11 px-6 rounded-2xl',
}

const variant: Record<ButtonVariant, string> = {
  primary:
    'bg-button-primary-background text-button-primary-foreground hover:bg-button-primary-hover active:bg-button-primary-active',
  secondary:
    'bg-button-secondary-background text-button-secondary-foreground hover:bg-button-secondary-hover active:bg-button-secondary-active',
  alternative:
    'bg-button-alternative-background text-button-alternative-foreground border-neutral-200 hover:bg-button-alternative-hover active:bg-button-alternative-active',
  destructive:
    'bg-button-destructive-background text-button-destructive-foreground hover:bg-button-destructive-hover active:bg-button-destructive-active',
  danger:
    'bg-button-danger-background text-button-danger-foreground hover:bg-button-danger-hover active:bg-button-danger-active',
  transparent:
    'bg-button-transparent-background text-button-transparent-foreground hover:bg-button-transparent-hover active:bg-button-transparent-active',
}

const shape: Record<Shape, string> = {
  square: 'rounded-none',
  round: 'rounded-inherit',
  pill: 'rounded-full',
}

const buttonVariants = cva(
  'inline-flex items-center w-fit justify-center gap-1 whitespace-nowrap leading-none font-semibold ring-offset-light transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-focused focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:shrink-0 border border-[transparent]',
  {
    variants: {
      variant,
      size,
      shape,
    },
    defaultVariants: {
      variant: 'primary',
      size: 's',
      shape: 'round',
    },
  },
)
