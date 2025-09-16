import React from 'react'

import { cn } from '@/utils'
import { HorizontalContainer } from '@/components/HorizontalContainer/HorizontalContainer'
import { NodeHandle } from './NodeHandle'

export const NodeHandles = React.memo(function NodeHandles({
  leftIcon,
  rightIcon,
  leftId,
  rightId,
  className,
  handleClassName,
  children,
}: {
  leftId?: string
  rightId?: string
  className?: string
  handleClassName?: string
  children: React.ReactNode
  leftIcon: React.ReactNode
  rightIcon: React.ReactNode
}) {
  return (
    <HorizontalContainer
      className={cn('items-center', className)}
      data-component="NodeHandles"
    >
      {leftId && (
        <NodeHandle
          type="target"
          id={leftId}
          className={cn('left-0', handleClassName)}
        >
          {leftIcon}
        </NodeHandle>
      )}
      {children}
      {rightId && (
        <NodeHandle
          type="source"
          id={rightId}
          className={cn('right-0', handleClassName)}
        >
          {rightIcon}
        </NodeHandle>
      )}
    </HorizontalContainer>
  )
})
