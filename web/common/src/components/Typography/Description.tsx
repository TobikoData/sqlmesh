import { cn } from '@/utils'
import React from 'react'

export const Description = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>(
  (
    {
      children,
      className,
      ...props
    }: {
      children?: React.ReactNode
      className?: string
    },
    ref,
  ) => {
    return (
      <div
        ref={ref}
        className={cn('text-typography-description text-sm', className)}
        {...props}
      >
        {children}
      </div>
    )
  },
)
