import { cn } from '@sqlmesh-common/utils'

export interface TaglineProps {
  className?: string
  children?: React.ReactNode
}

export function Tagline({ className, children, ...props }: TaglineProps) {
  return (
    <div
      data-component="Tagline"
      className={cn('text-typography-tagline text-xs truncate', className)}
      {...props}
    >
      {children}
    </div>
  )
}
