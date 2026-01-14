import React from 'react'
import { getHeadlineTextSize } from './help'
import type { HeadlineLevel } from '@sqlmesh-common/types'
import { cn } from '@sqlmesh-common/utils'

export interface HeadlineProps {
  level: HeadlineLevel
  children: React.ReactNode
  className?: string
}

export function Headline({
  level = 1,
  children,
  className,
  ...props
}: HeadlineProps) {
  const Tag = `h${level}` as keyof JSX.IntrinsicElements

  return (
    <Tag
      data-component="Headline"
      className={cn(
        getHeadlineTextSize(level),
        'truncate text-typography-headline',
        className,
      )}
      {...props}
    >
      {children}
    </Tag>
  )
}
