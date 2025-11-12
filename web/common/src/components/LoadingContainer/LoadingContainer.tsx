import type { Side } from '@sqlmesh-common/types'
import { cn } from '@sqlmesh-common/utils'
import React from 'react'
import { LoadingIcon } from './LoadingIcon'

export interface LoadingContainerProps
  extends React.HTMLAttributes<HTMLDivElement> {
  isLoading?: boolean
  message?: React.ReactNode
  side?: Side
  className?: string
}

export const LoadingContainer = React.forwardRef<
  HTMLDivElement,
  LoadingContainerProps
>(
  (
    {
      isLoading = true,
      side = 'left',
      message,
      children,
      className,
    }: LoadingContainerProps,
    ref,
  ) => {
    function renderLoading() {
      return (
        <>
          <LoadingIcon className="shrink-0" />
          {message && <span className="text-sm">{message}</span>}
        </>
      )
    }

    return isLoading ? (
      <div
        ref={ref}
        data-component="LoadingContainer"
        className={cn('flex items-center justify-center gap-2', className)}
      >
        {(side === 'left' || side === 'both') && renderLoading()}
        {children}
        {(side === 'right' || side === 'both') && renderLoading()}
      </div>
    ) : (
      children
    )
  },
)
