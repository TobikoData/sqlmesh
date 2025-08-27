import React, { useState } from 'react'

import { Button, type ButtonProps } from '@/components/Button/Button'
import { EnumButtonVariant } from '@/components/Button/help'
import { EnumSize } from '@/types/enums'
import { cn, notNil } from '@/utils'
import type { Optional, TimerID } from '@/types'

export interface ClipboardCopyProps extends Omit<ButtonProps, 'children'> {
  text: string
  delay?: number
  children: (copied: boolean) => React.ReactNode
}

export const ClipboardCopy = React.forwardRef<
  HTMLButtonElement,
  ClipboardCopyProps
>(
  (
    {
      text,
      title = 'Copy to clipboard',
      variant = EnumButtonVariant.Transparent,
      size = EnumSize.XS,
      delay = 2000,
      disabled = false,
      className,
      children,
      onClick,
      ...props
    },
    ref,
  ) => {
    const [copied, setCopied] = useState<Optional<TimerID>>(undefined)

    const copy = (e: React.MouseEvent<HTMLButtonElement>) => {
      e.preventDefault()
      e.stopPropagation()

      if (copied) {
        clearTimeout(copied)
      }

      navigator.clipboard.writeText(text).then(() => {
        setCopied(setTimeout(() => setCopied(undefined), delay))
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
        {children(notNil(copied))}
      </Button>
    )
  },
)

ClipboardCopy.displayName = 'ClipboardCopy'
