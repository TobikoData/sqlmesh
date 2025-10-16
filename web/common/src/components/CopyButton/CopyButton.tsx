import React from 'react'

import {
  Button,
  type ButtonProps,
} from '@sqlmesh-common/components/Button/Button'
import { useCopyClipboard } from '@sqlmesh-common/hooks/useCopyClipboard'

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
      children,
      onClick,
      ...props
    },
    ref,
  ) => {
    const [copyToClipboard, isCopied] = useCopyClipboard(delay)

    return (
      <Button
        ref={ref}
        data-component="CopyButton"
        title={title}
        size={size}
        variant={variant}
        onClick={e => {
          e.stopPropagation()
          copyToClipboard(text)
          onClick?.(e)
        }}
        disabled={disabled || !!isCopied}
        {...props}
      >
        {children(isCopied != null)}
      </Button>
    )
  },
)
CopyButton.displayName = 'CopyButton'
