import { Slot } from '@radix-ui/react-slot'
import React from 'react'

import type { Shape, Size } from '@sqlmesh-common/types'
import { cn } from '@sqlmesh-common/utils'
import { cva } from 'class-variance-authority'

import './Badge.css'

export interface BadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  asChild?: boolean
  size?: Size
  shape?: Shape
}

export const Badge = React.forwardRef<HTMLSpanElement, BadgeProps>(
  ({ className, size, shape, asChild = false, ...props }, ref) => {
    const Comp = asChild ? Slot : 'span'
    return (
      <Comp
        data-component="Badge"
        className={cn(badgeVariants({ size, shape, className }))}
        ref={ref}
        {...props}
      />
    )
  },
)
Badge.displayName = 'Badge'

const size: Record<Size, string> = {
  '2xs': 'h-5 px-2 text-2xs leading-none rounded-2xs',
  xs: 'h-6 px-2 text-2xs rounded-xs',
  s: 'h-7 px-3 text-xs rounded-sm',
  m: 'h-8 px-4 rounded-md',
  l: 'h-9 px-4 rounded-lg',
  xl: 'h-10 px-4 rounded-xl',
  '2xl': 'h-11 px-6 rounded-2xl',
}

const shape: Record<Shape, string> = {
  square: 'rounded-none',
  round: 'rounded-inherit',
  pill: 'rounded-full',
}

const badgeVariants = cva(
  'bg-badge-background text-badge-foreground font-mono inline-flex align-middle items-center justify-center gap-2 leading-none whitespace-nowrap font-semibold',
  {
    variants: {
      size,
      shape,
    },
    defaultVariants: {
      size: 's',
      shape: 'round',
    },
  },
)
