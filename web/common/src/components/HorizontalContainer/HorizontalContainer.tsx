import React from 'react'

import { cn } from '@/utils'
import { EnumLayoutDirection } from '@/types/enums'
import { ScrollContainer } from '../ScrollContainer/ScrollContainer'

export interface HorizontalContainerProps
  extends React.HTMLAttributes<HTMLDivElement> {
  scroll?: boolean
}

export const HorizontalContainer = React.forwardRef<
  HTMLDivElement,
  HorizontalContainerProps
>(({ children, className, scroll = false, ...props }, ref) => {
  return scroll ? (
    <ScrollContainer
      ref={ref}
      direction={EnumLayoutDirection.HORIZONTAL}
    >
      <HorizontalContainer
        {...props}
        scroll={false}
        className={cn('overflow-visible w-fit', className)}
      >
        {children}
      </HorizontalContainer>
    </ScrollContainer>
  ) : (
    <div
      ref={ref}
      data-component="HorizontalContainer"
      {...props}
      className={cn(
        'w-full h-full overflow-hidden',
        className,
        'flex flex-row',
      )}
    >
      {children}
    </div>
  )
})

HorizontalContainer.displayName = 'HorizontalContainer'
