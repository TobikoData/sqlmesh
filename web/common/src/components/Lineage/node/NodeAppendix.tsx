import { cva, type VariantProps } from 'class-variance-authority'
import { forwardRef, type HTMLAttributes } from 'react'

import { cn } from '@sqlmesh-common/utils'

const appendixVariants = cva(
  'node-appendix absolute flex w-full flex-col items-center',
  {
    variants: {
      position: {
        top: '-translate-y-[100%] -my-1',
        bottom: 'top-[100%] my-1',
        left: '-left-[100%] -mx-1',
        right: 'left-[100%] mx-1',
      },
    },
    defaultVariants: {
      position: 'top',
    },
  },
)

export interface NodeAppendixProps
  extends HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof appendixVariants> {
  className?: string
  position?: 'top' | 'bottom' | 'left' | 'right'
}

export const NodeAppendix = forwardRef<HTMLDivElement, NodeAppendixProps>(
  ({ children, className, position, ...props }, ref) => {
    return (
      <div
        ref={ref}
        data-component="NodeAppendix"
        className={cn(appendixVariants({ position }), className)}
        {...props}
      >
        {children}
      </div>
    )
  },
)

NodeAppendix.displayName = 'NodeAppendix'
