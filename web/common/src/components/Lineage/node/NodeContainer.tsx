import React from 'react'

import { cn } from '@/utils'
import { VerticalContainer } from '@/components/VerticalContainer/VerticalContainer'

export const NodeContainer = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>(({ className, children, ...props }, ref) => {
  return (
    <VerticalContainer
      data-component="NodeContainer"
      className={cn('gap-1 overflow-visible relative', className)}
      ref={ref}
      {...props}
    >
      {children}
    </VerticalContainer>
  )
})
NodeContainer.displayName = 'NodeContainer'
