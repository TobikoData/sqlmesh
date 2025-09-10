import { cn } from '@/utils'
import React from 'react'

export function Description({
  children,
  className,
  ...props
}: {
  children?: React.ReactNode
  className?: string
}) {
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
