import { cn } from '@/utils'
import React from 'react'

export const Text = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>(({ className, children, ...props }, ref) => {
  return (
    <div
      ref={ref}
      className={cn('whitespace-wrap text-prose', className)}
      {...props}
    >
      {children}
    </div>
  )
})
