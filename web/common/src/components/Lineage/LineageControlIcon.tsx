import React from 'react'

import { cn } from '@sqlmesh-common/utils'

export interface LineageControlIconProps extends React.SVGProps<SVGSVGElement> {
  Icon: React.ElementType
  size?: number
  className?: string
}

export const LineageControlIcon = React.forwardRef<
  HTMLSpanElement,
  LineageControlIconProps
>(
  (
    {
      Icon,
      size = 16,
      className,
      ...props
    }: {
      Icon: React.ElementType
      size?: number
      className?: string
    },
    ref,
  ) => {
    return (
      <Icon
        ref={ref}
        data-component="LineageControlIcon"
        size={size}
        className={cn(
          'text-lineage-control-icon-foreground stroke-lineage-control-icon-background',
          className,
        )}
        {...props}
      />
    )
  },
)

LineageControlIcon.displayName = 'LineageControlIcon'
