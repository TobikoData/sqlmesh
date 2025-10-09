import React from 'react'

import { cn } from '@sqlmesh-common/utils'
import { ScrollContainer } from '../ScrollContainer/ScrollContainer'

export interface VerticalContainerProps
  extends React.HTMLAttributes<HTMLDivElement> {
  scroll?: boolean
}

export const VerticalContainer = React.forwardRef<
  HTMLDivElement,
  VerticalContainerProps
>(({ children, className, scroll = false, ...props }, ref) => {
  return scroll ? (
    <ScrollContainer
      ref={ref}
      direction="vertical"
    >
      <VerticalContainer
        {...props}
        scroll={false}
        className={cn('h-auto', className)}
      >
        {children}
      </VerticalContainer>
    </ScrollContainer>
  ) : (
    <div
      ref={ref}
      data-component="VerticalContainer"
      {...props}
      className={cn(
        'w-full h-full overflow-hidden',
        className,
        'flex flex-col',
      )}
    >
      {children}
    </div>
  )
})

VerticalContainer.displayName = 'VerticalContainer'
