import React from 'react'

import { cn } from '@sqlmesh-common/utils'
import { VerticalContainer } from '@sqlmesh-common/components/VerticalContainer/VerticalContainer'

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
