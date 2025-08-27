import React from 'react'

import { cn } from '@/utils'
import { EnumLayoutDirection, type LayoutDirection } from '@/types/enums'

export interface ScrollContainerProps
  extends React.HTMLAttributes<HTMLDivElement> {
  direction?: LayoutDirection
}

export const ScrollContainer = React.forwardRef<
  HTMLDivElement,
  ScrollContainerProps
>(
  (
    { children, className, direction = EnumLayoutDirection.VERTICAL, ...props },
    ref,
  ) => {
    const vertical =
      direction === EnumLayoutDirection.VERTICAL ||
      direction === EnumLayoutDirection.BOTH
    const horizontal =
      direction === EnumLayoutDirection.HORIZONTAL ||
      direction === EnumLayoutDirection.BOTH
    return (
      <div
        ref={ref}
        {...props}
        data-component="ScrollContainer"
        className={cn(
          'w-full h-full',
          vertical
            ? 'overflow-y-scroll scrollbar-w-[6px]'
            : 'overflow-y-hidden',
          horizontal
            ? 'overflow-x-scroll scrollbar-h-[6px]'
            : 'overflow-x-hidden',
          className,
          'scrollbar scrollbar-thumb-neutral-300 scrollbar-track-neutral-100 scrollbar-thumb-rounded-full',
        )}
      >
        {children}
      </div>
    )
  },
)

ScrollContainer.displayName = 'ScrollContainer'
