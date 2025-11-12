import { cn } from '@sqlmesh-common/utils'
import React from 'react'

export interface DescriptionProps {
  children?: React.ReactNode
  className?: string
}

export function Description({
  children,
  className,
  ...props
}: DescriptionProps) {
  return (
    <div
      data-component="Description"
      className={cn('text-typography-description text-sm', className)}
      {...props}
    >
      {children}
    </div>
  )
}
