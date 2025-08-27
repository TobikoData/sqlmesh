import { Slot } from '@radix-ui/react-slot'
import type { VariantProps } from 'class-variance-authority'
import React from 'react'

import { cn } from '@/utils'
import { buttonVariants } from './help'
import type { ButtonType } from './help'

import './Button.css'

const DEFAULT_BUTTON_TYPE = 'button'

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean
  type?: ButtonType
}

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, type, asChild = false, ...props }, ref) => {
    const Comp = asChild ? Slot : DEFAULT_BUTTON_TYPE
    return (
      <Comp
        type={type}
        className={cn(buttonVariants({ variant, size, className }))}
        ref={ref}
        {...props}
      />
    )
  },
)
Button.displayName = 'Button'

export { Button }
