import { type HTMLAttributes, forwardRef } from 'react'

import { cn } from '@sqlmesh-common/utils'

export const BaseNode = forwardRef<
  HTMLDivElement,
  HTMLAttributes<HTMLDivElement> & { selected?: boolean }
>(({ className, ...props }, ref) => (
  <div
    ref={ref}
    className={cn('relative rounded-md border-2 p-0', className)}
    tabIndex={0}
    {...props}
  />
))

BaseNode.displayName = 'BaseNode'
