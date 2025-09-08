import React from 'react'
import { getHeadlineTextSize } from './help'
import type { HeadlineLevel } from '@/types'
import { cn } from '@/utils'

export function Headline({
  id,
  level = 1,
  children,
  className,
}: {
  id?: string
  level?: HeadlineLevel
  children: React.ReactNode
  className?: string
}) {
  const Tag = `h${level}` as keyof JSX.IntrinsicElements

  return (
    <Tag
      id={id}
      className={cn(
        getHeadlineTextSize(level),
        'truncate text-typography-heading',
        className,
      )}
    >
      {children}
    </Tag>
  )
}
