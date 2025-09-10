import { cn } from '@/utils'

export function Text({
  className,
  children,
  ...props
}: {
  className?: string
  children?: React.ReactNode
}) {
  return (
    <div
      data-component="Text"
      className={cn('whitespace-wrap text-prose text-sm mb-1', className)}
      {...props}
    >
      {children}
    </div>
  )
}
