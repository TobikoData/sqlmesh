import { Slot } from '@radix-ui/react-slot'
import { type VariantProps } from 'class-variance-authority'
import React from 'react'

import { type Size, type Shape } from '@/types/enums'
import { cn } from '@/utils'
import { badgeVariants } from './help'

import './Badge.css'

export interface BadgeProps
  extends React.HTMLAttributes<HTMLSpanElement>,
    VariantProps<typeof badgeVariants> {
  asChild?: boolean
  size?: Size
  shape?: Shape
}

export const Badge = React.forwardRef<HTMLSpanElement, BadgeProps>(
  ({ className, size, shape, asChild = false, ...props }, ref) => {
    const Comp = asChild ? Slot : 'span'
    return (
      <Comp
        className={cn(badgeVariants({ size, shape, className }))}
        ref={ref}
        data-component="Badge"
        {...props}
      />
    )
  },
)
Badge.displayName = 'Badge'
