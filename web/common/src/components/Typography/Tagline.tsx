import React from 'react'

import { cn } from '@/utils'

export const Tagline = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>(({ className, children, ...props }, ref) => {
  return (
    <div
      ref={ref}
      className={cn(
        'text-typography-tagline text-xs whitespace-nowrap overflow-hidden text-ellipsis',
        className,
      )}
      {...props}
    >
      {children}
    </div>
  )
})
