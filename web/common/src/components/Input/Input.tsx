import * as React from 'react'
import { cn } from '@sqlmesh-common/utils'
import type { Size } from '@sqlmesh-common/types'
import { cva } from 'class-variance-authority'

import './Input.css'

export interface InputProps extends React.ComponentProps<'input'> {
  inputSize?: Size
}

export const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className, type = 'text', readOnly, inputSize = 's', ...props }, ref) => {
    return (
      <input
        type={type}
        className={cn(
          inputVariants({ size: inputSize }),
          'border items-center border-input-border bg-input-background text-input-foreground transition-colors placeholder:text-input-placeholder',
          'file:border-0 file:h-fit file:bg-background-translucid file:rounded-sm file:flex-col file:mt-0.5',
          type === 'file' &&
            'bg-input-background-translucid border-[transparent] pl-1',
          className,
        )}
        ref={ref}
        readOnly={readOnly}
        {...props}
      />
    )
  },
)

const size: Record<Size, string> = {
  '2xs': 'h-5 px-2 text-2xs rounded-2xs',
  xs: 'h-6 px-2 text-2xs rounded-xs',
  s: 'h-7 px-3 text-xs rounded-sm',
  m: 'h-8 px-4 rounded-md',
  l: 'h-9 px-4 rounded-lg',
  xl: 'h-10 px-4 rounded-xl',
  '2xl': 'h-11 px-6 rounded-2xl',
}

const inputVariants = cva(
  'inline-flex whitespace-nowrap leading-none ring-offset-light transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-focused focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-60',
  {
    variants: {
      size,
    },
    defaultVariants: {
      size: 's',
    },
  },
)
