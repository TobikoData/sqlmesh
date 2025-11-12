import { type HTMLAttributes, forwardRef } from 'react'

import { cn } from '@sqlmesh-common/utils'

/* NODE HEADER -------------------------------------------------------------- */

export type NodeHeaderProps = HTMLAttributes<HTMLElement>

/**
 * A container for a consistent header layout intended to be used inside the
 * `<BaseNode />` component.
 */
export const NodeHeader = forwardRef<HTMLElement, NodeHeaderProps>(
  ({ className, ...props }, ref) => {
    return (
      <header
        data-component="NodeHeader"
        ref={ref}
        {...props}
        className={cn(
          'flex w-full items-center justify-between p-0 relative',
          className,
        )}
      />
    )
  },
)

NodeHeader.displayName = 'NodeHeader'
