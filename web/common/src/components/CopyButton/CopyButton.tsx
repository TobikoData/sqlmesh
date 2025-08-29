import React, { useState } from 'react'

import { Button, type ButtonProps } from '@/components/Button/Button'
import { cn } from '@/utils'

type TimerID = ReturnType<typeof setTimeout>

export interface CopyButtonProps extends Omit<ButtonProps, 'children'> {
  text: string
  delay?: number
  children: (copied: boolean) => React.ReactNode
}

export const CopyButton = React.forwardRef<HTMLButtonElement, CopyButtonProps>(
  (
    {
      text,
      title = 'Copy to clipboard',
      variant = 'secondary',
      size = 'xs',
      delay = 2000,
      disabled = false,
      className,
      children,
      onClick,
      ...props
    },
    ref,
  ) => {
    const [copied, setCopied] = useState<TimerID | null>(null)

    const copy = (e: React.MouseEvent<HTMLButtonElement>) => {
      e.preventDefault()
      e.stopPropagation()

      if (copied) {
        clearTimeout(copied)
      }

      navigator.clipboard.writeText(text).then(() => {
        setCopied(setTimeout(() => setCopied(null), delay))
      })

      onClick?.(e)
    }

    return (
      <Button
        ref={ref}
        title={title}
        size={size}
        variant={variant}
        onClick={copy}
        disabled={disabled || !!copied}
        {...props}
        className={cn(className, copied && 'pointer-events-none')}
      >
        {children(copied != null)}
      </Button>
    )
  },
)
CopyButton.displayName = 'CopyButton'
