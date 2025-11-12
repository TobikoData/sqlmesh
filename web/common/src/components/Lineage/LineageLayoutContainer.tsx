import { cn } from '@sqlmesh-common/utils'

import React from 'react'

import { VerticalContainer } from '../VerticalContainer/VerticalContainer'
import { MessageContainer } from '../MessageContainer/MessageContainer'
import { LoadingContainer } from '../LoadingContainer/LoadingContainer'

export function LineageLayoutContainer({
  isBuildingLayout,
  loadingMessage = 'Building layout...',
  className,
  children,
}: {
  isBuildingLayout?: boolean
  loadingMessage?: string
  className?: string
  children: React.ReactNode
}) {
  return (
    <VerticalContainer
      data-component="LineageLayoutContainer"
      className={cn(
        'border-2 border-lineage-border bg-lineage-background relative h-full',
        className,
      )}
    >
      {isBuildingLayout && (
        <MessageContainer
          className={cn('absolute inset-0 backdrop-blur-sm z-10 rounded-none')}
        >
          <LoadingContainer
            isLoading={isBuildingLayout}
            className="px-4 py-2 font-semibold shadow-lg bg-lineage-background text-lineage-foreground rounded-md"
          >
            {loadingMessage}
          </LoadingContainer>
        </MessageContainer>
      )}
      {children}
    </VerticalContainer>
  )
}
