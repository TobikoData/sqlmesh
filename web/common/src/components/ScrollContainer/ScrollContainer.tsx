import React from 'react'

import { cn } from '@sqlmesh-common/utils'
import type { LayoutDirection } from '@sqlmesh-common/types'

import './ScrollContainer.css'

export interface ScrollContainerProps
  extends React.HTMLAttributes<HTMLDivElement> {
  direction?: LayoutDirection
}

export const ScrollContainer = React.forwardRef<
  HTMLDivElement,
  ScrollContainerProps
>(({ children, className, direction = 'vertical', ...props }, ref) => {
  const vertical = direction === 'vertical' || direction === 'both'
  const horizontal = direction === 'horizontal' || direction === 'both'
  return (
    <div
      ref={ref}
      {...props}
      data-component="ScrollContainer"
      className={cn(
        'w-full h-full',
        vertical ? 'overflow-y-scroll scrollbar-w-[6px]' : 'overflow-y-hidden',
        horizontal
          ? 'overflow-x-scroll scrollbar-h-[6px]'
          : 'overflow-x-hidden',
        className,
        'scrollbar scrollbar-thumb-rounded-full',
      )}
    >
      {children}
    </div>
  )
})

ScrollContainer.displayName = 'ScrollContainer'
