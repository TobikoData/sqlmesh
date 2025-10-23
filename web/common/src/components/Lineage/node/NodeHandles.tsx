import React from 'react'

import { cn } from '@sqlmesh-common/utils'
import { HorizontalContainer } from '@sqlmesh-common/components/HorizontalContainer/HorizontalContainer'
import { NodeHandle } from './NodeHandle'
import type { HandleId } from '../utils'

export function NodeHandles<
  TLeftHandleId extends string = HandleId,
  TRightHandleId extends string = HandleId,
>({
  leftIcon,
  rightIcon,
  leftId,
  rightId,
  className,
  handleClassName,
  children,
}: {
  leftId?: TLeftHandleId
  rightId?: TRightHandleId
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
        <NodeHandle<TLeftHandleId>
          type="target"
          id={leftId}
          className={cn('left-0', handleClassName)}
        >
          {leftIcon}
        </NodeHandle>
      )}
      {children}
      {rightId && (
        <NodeHandle<TRightHandleId>
          type="source"
          id={rightId}
          className={cn('right-0', handleClassName)}
        >
          {rightIcon}
        </NodeHandle>
      )}
    </HorizontalContainer>
  )
}
