import { cn } from '@sqlmesh-common/utils'

export interface TextProps {
  className?: string
  children?: React.ReactNode
}

export function Text({ className, children, ...props }: TextProps) {
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
