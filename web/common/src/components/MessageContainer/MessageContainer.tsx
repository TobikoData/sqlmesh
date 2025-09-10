import { cn } from '@/utils'
import { LoadingContainer } from '../LoadingContainer/LoadingContainer'
import { HorizontalContainer } from '../HorizontalContainer/HorizontalContainer'

export interface MessageContainerProps {
  children: React.ReactNode
  className?: string
  wrap?: boolean
  isLoading?: boolean
}

export function MessageContainer({
  children,
  className,
  wrap = false,
  isLoading = false,
}: MessageContainerProps) {
  return (
    <HorizontalContainer
      data-component="MessageContainer"
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
