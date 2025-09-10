import React from 'react'
import { HorizontalContainer } from '../HorizontalContainer/HorizontalContainer'
import { cn } from '@/utils'

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
          'justify-between gap-2 items-center whitespace-nowrap h-auto',
          className,
        )}
        {...props}
      >
        {typeof label === 'string' ? (
          <div className="text-metadata-label">{label}</div>
        ) : (
          label
        )}
        {typeof value === 'string' ? (
          <div className="text-metadata-value font-semibold">{value}</div>
        ) : (
          value
        )}
      </HorizontalContainer>
    )
  },
)

Metadata.displayName = 'Metadata'
