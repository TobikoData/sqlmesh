import { cva } from 'class-variance-authority'

import { EnumSize } from '@/types/enums'

export const EnumButtonType = {
  Button: 'button',
  Submit: 'submit',
  Reset: 'reset',
} as const

export type ButtonType = (typeof EnumButtonType)[keyof typeof EnumButtonType]

export const EnumButtonVariant = {
  Primary: 'primary',
  Secondary: 'secondary',
  Alternative: 'alternative',
  Destructive: 'destructive',
  Danger: 'danger',
  Transparent: 'transparent',
} as const

export type ButtonVariant =
  (typeof EnumButtonVariant)[keyof typeof EnumButtonVariant]

export const buttonVariants = cva(
  'inline-flex items-center w-fit justify-center gap-1 whitespace-nowrap leading-none font-semibold ring-offset-light transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-focused focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:shrink-0 border border-[transparent]',
  {
    variants: {
      variant: {
        [EnumButtonVariant.Primary]:
          'bg-button-primary-background text-button-primary-foreground hover:bg-button-primary-hover active:bg-button-primary-active',
        [EnumButtonVariant.Secondary]:
          'bg-button-secondary-background text-button-secondary-foreground hover:bg-button-secondary-hover active:bg-button-secondary-active',
        [EnumButtonVariant.Alternative]:
          'bg-button-alternative-background text-button-alternative-foreground border-neutral-200 hover:bg-button-alternative-hover active:bg-button-alternative-active',
        [EnumButtonVariant.Destructive]:
          'bg-button-destructive-background text-button-destructive-foreground hover:bg-button-destructive-hover active:bg-button-destructive-active',
        [EnumButtonVariant.Danger]:
          'bg-button-danger-background text-button-danger-foreground hover:bg-button-danger-hover active:bg-button-danger-active',
        [EnumButtonVariant.Transparent]:
          'bg-button-transparent-background text-button-transparent-foreground hover:bg-button-transparent-hover active:bg-button-transparent-active',
      },
      size: {
        [EnumSize.XXS]: 'h-5 px-2 text-2xs leading-none rounded-2xs',
        [EnumSize.XS]: 'h-6 px-2 text-2xs rounded-xs',
        [EnumSize.S]: 'h-7 px-3 text-xs rounded-sm',
        [EnumSize.M]: 'h-8 px-4 rounded-md',
        [EnumSize.L]: 'h-9 px-4 rounded-lg',
        [EnumSize.XL]: 'h-10 px-4 rounded-xl',
        [EnumSize.XXL]: 'h-11 px-6 rounded-2xl',
      },
    },
    defaultVariants: {
      variant: EnumButtonVariant.Primary,
      size: EnumSize.S,
    },
  },
)
