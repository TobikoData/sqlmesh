import React from 'react'

import { cn } from '@sqlmesh-common/utils'
import { Badge, type BadgeProps } from '@sqlmesh-common/components/Badge/Badge'

export const NodeBadge = React.forwardRef<HTMLSpanElement, BadgeProps>(
  ({ className, children, ...props }, ref) => {
    return (
      <Badge
        ref={ref}
        data-component="NodeBadge"
        className={cn(
          'NodeBadge font-mono bg-lineage-node-badge-background h-[18px] text-lineage-node-badge-foreground rounded-sm px-1.5 pt-0.5 font-extrabold',
          className,
        )}
        size="2xs"
        {...props}
      >
        {children}
      </Badge>
    )
  },
)
NodeBadge.displayName = 'NodeBadge'
