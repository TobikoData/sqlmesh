import { Position } from '@xyflow/react'
import React from 'react'

import { cn } from '@sqlmesh-common/utils'
import { BaseHandle } from './base-handle'
import type { HandleId } from '../utils'

export function NodeHandle<THandleId extends string = HandleId>({
  type,
  id,
  children,
  className,
  ...props
}: {
  type: 'target' | 'source'
  id: THandleId
  children: React.ReactNode
  className?: string
}) {
  return (
    <BaseHandle
      data-component="NodeHandle"
      type={type}
      position={type === 'target' ? Position.Left : Position.Right}
      id={id}
      isConnectable={false}
      className={cn(className)}
      {...props}
    >
      {children}
    </BaseHandle>
  )
}
