import { Handle, type HandleProps } from '@xyflow/react'
import { forwardRef } from 'react'
import type { ForwardRefExoticComponent, RefAttributes } from 'react'

import { cn } from '@/utils'

export const BaseHandle: ForwardRefExoticComponent<
  HandleProps & RefAttributes<HTMLDivElement>
> = forwardRef<HTMLDivElement, HandleProps>(
  ({ className, children, ...props }, ref) => {
    return (
      <Handle
        ref={ref}
        {...props}
        className={cn(
          'fixed flex justify-center items-center border-none transition',
          className,
        )}
      >
        {children}
      </Handle>
    )
  },
)

BaseHandle.displayName = 'BaseHandle'
