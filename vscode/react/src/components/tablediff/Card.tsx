import { type ReactNode } from 'react'
import { twColors, twMerge } from './tailwind-utils'

interface CardProps {
  children: ReactNode
  className?: string
}

export function Card({ children, className }: CardProps) {
  return (
    <div
      className={twMerge(
        'rounded-xl shadow-sm border overflow-hidden',
        twColors.bgEditor,
        twColors.borderNeutral100,
        className,
      )}
    >
      {children}
    </div>
  )
}

interface CardHeaderProps {
  children: ReactNode
  className?: string
}

export function CardHeader({ children, className }: CardHeaderProps) {
  return (
    <div
      className={twMerge(
        'px-6 py-4 border-b',
        twColors.bgNeutral10,
        twColors.borderNeutral100,
        className,
      )}
    >
      {children}
    </div>
  )
}

interface CardContentProps {
  children: ReactNode
  className?: string
}

export function CardContent({ children, className }: CardContentProps) {
  return <div className={twMerge('px-6 py-4', className)}>{children}</div>
}
