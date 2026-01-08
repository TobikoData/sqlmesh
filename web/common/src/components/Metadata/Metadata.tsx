import React from 'react'
import { HorizontalContainer } from '../HorizontalContainer/HorizontalContainer'
import { cn } from '@sqlmesh-common/utils'

export interface MetadataProps extends React.HTMLAttributes<HTMLDivElement> {
  label: React.ReactNode
  value: React.ReactNode
}

export const Metadata = React.forwardRef<HTMLDivElement, MetadataProps>(
  ({ label, value, className, ...props }, ref) => {
    return (
      <HorizontalContainer
        ref={ref}
        data-component="Metadata"
        className={cn(
          'Metadata justify-between gap-2 items-center whitespace-nowrap h-auto',
          className,
        )}
        {...props}
      >
        {typeof label === 'string' ? (
          <div
            title={label}
            className="text-metadata-label truncate shrink-0"
          >
            {label}
          </div>
        ) : (
          label
        )}
        {typeof value === 'string' ? (
          <div
            title={value}
            className="text-metadata-value font-semibold truncate"
          >
            {value}
          </div>
        ) : (
          value
        )}
      </HorizontalContainer>
    )
  },
)

Metadata.displayName = 'Metadata'
