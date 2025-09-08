import React from 'react'
import { Headline } from './Headline'
import { Information } from './Information'
import { Tagline } from './Tagline'
import type { HeadlineLevel } from '@/types'
import { cn } from '@/utils'

export interface TextBlockProps extends React.HTMLAttributes<HTMLDivElement> {
  level?: HeadlineLevel
  headline?: string
  tagline?: string
  info?: string
}

export const TextBlock = React.forwardRef<HTMLDivElement, TextBlockProps>(
  (
    {
      level = 1,
      headline,
      tagline,
      children,
      className,
      info,
      ...props
    }: TextBlockProps,
    ref,
  ) => {
    function renderHeadline() {
      return <Headline level={level}>{headline}</Headline>
    }

    return (
      <div
        ref={ref}
        className={cn('flex flex-col gap-2', className)}
        {...props}
      >
        {headline &&
          (info ? (
            <Information info={info}>{renderHeadline()}</Information>
          ) : (
            renderHeadline()
          ))}
        {tagline && <Tagline>{tagline}</Tagline>}
        {children}
      </div>
    )
  },
)
