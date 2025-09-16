import { Position } from '@xyflow/react'
import React from 'react'

import { cn } from '@/utils'
import { BaseHandle } from './base-handle'

export const NodeHandle = React.memo(function NodeHandle({
  type,
  id,
  children,
  className,
  ...props
}: {
  type: 'target' | 'source'
  id: string
  children: React.ReactNode
  className?: string
}) {
  return (
    <BaseHandle
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
})
