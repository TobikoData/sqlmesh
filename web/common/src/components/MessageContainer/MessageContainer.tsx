import { cn } from '@/utils'
import { LoadingContainer } from '../LoadingContainer/LoadingContainer'
import { HorizontalContainer } from '../HorizontalContainer/HorizontalContainer'

export default function MessageContainer({
  children,
  className,
  wrap = false,
  isLoading = false,
}: {
  children: React.ReactNode
  className?: string
  wrap?: boolean
  isLoading?: boolean
}) {
  return (
    <HorizontalContainer
      className={cn(
        'h-auto justify-center items-center p-4 bg-message-lucid rounded-2xl',
        className,
      )}
    >
      {isLoading ? (
        <LoadingContainer
          isLoading={isLoading}
          className={cn(
            'w-full overflow-hidden',
            wrap ? 'whitespace-normal' : 'truncate',
            className,
          )}
        >
          {children}
        </LoadingContainer>
      ) : (
        children
      )}
    </HorizontalContainer>
  )
}
