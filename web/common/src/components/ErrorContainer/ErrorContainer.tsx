import { Button } from '../Button/Button'
import type { Callback } from '@/types'
import { HorizontalContainer } from '../HorizontalContainer/HorizontalContainer'
import { VerticalContainer } from '../VerticalContainer/VerticalContainer'
import { cn } from '@/utils'
import { Badge } from '../Badge/Badge'

export function ErrorContainer({
  title,
  errorCode,
  errorMessage,
  errorDetail,
  tryAgain,
  navigate,
  className,
}: {
  title?: string
  errorCode?: string
  errorMessage?: string
  errorDetail?: string
  navigate?: Callback
  tryAgain?: Callback
  className?: string
}) {
  return (
    <HorizontalContainer
      role="alert"
      className={cn('p-4 items-center justify-center', className)}
    >
      <VerticalContainer className="gap-2 bg-error-lucid text-error p-4 rounded-2xl">
        <VerticalContainer
          scroll
          className="gap-2 p-2"
        >
          <HorizontalContainer className="gap-2 h-auto items-center">
            {errorCode && (
              <Badge
                size="2xs"
                className="bg-error text-light font-bold shrink-0"
              >
                {errorCode}
              </Badge>
            )}
            <b className="shrink-0">{title}</b>
            <p
              title={errorMessage}
              className="truncate"
            >
              {errorMessage}
            </p>
          </HorizontalContainer>
          <pre className="text-xs whitespace-pre-wrap">{errorDetail}</pre>
        </VerticalContainer>
        <HorizontalContainer className="gap-2 h-auto p-2">
          {tryAgain && (
            <Button
              variant="secondary"
              size="xs"
              onClick={tryAgain}
            >
              Try again
            </Button>
          )}
          {navigate && (
            <Button
              variant="secondary"
              size="xs"
              onClick={navigate}
            >
              Go back
            </Button>
          )}
        </HorizontalContainer>
      </VerticalContainer>
    </HorizontalContainer>
  )
}
