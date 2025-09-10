import { cn } from '@/utils'

export function Tagline({
  className,
  children,
  ...props
}: {
  className?: string
  children?: React.ReactNode
}) {
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
