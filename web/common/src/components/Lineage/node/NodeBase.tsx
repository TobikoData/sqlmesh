import { type NodeProps } from '@xyflow/react'
import React from 'react'

import { BaseNode } from '@sqlmesh-common/components/Lineage/node/base-node'
import { cn } from '@sqlmesh-common/utils'

export interface NodeBaseProps extends NodeProps {
  className?: string
  children?: React.ReactNode
}

export const NodeBase = React.memo(
  React.forwardRef<HTMLDivElement, NodeBaseProps>(
    ({ className, children }, ref) => {
      return (
        <BaseNode
          data-component="BaseNode"
          className={cn(
            'bg-lineage-node-background text-lineage-node-foreground',
            'h-full flex flex-col flex-1 justify-center flex-shrink-0 rounded-md',
            className,
          )}
          ref={ref}
        >
          {children}
        </BaseNode>
      )
    },
  ),
)
NodeBase.displayName = 'NodeBase'
