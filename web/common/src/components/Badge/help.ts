import { cva } from 'class-variance-authority'

import { EnumShape, EnumSize } from '@/types/enums'

export const badgeVariants = cva(
  'bg-badge-background text-badge-foreground font-mono inline-flex align-middle items-center justify-center gap-2 leading-none whitespace-nowrap font-semibold',
  {
    variants: {
      size: {
        [EnumSize.XXS]: 'h-5 px-2 text-2xs leading-none rounded-2xs',
        [EnumSize.XS]: 'h-6 px-2 text-2xs rounded-xs',
        [EnumSize.S]: 'h-7 px-3 text-xs rounded-sm',
        [EnumSize.M]: 'h-8 px-4 rounded-md',
        [EnumSize.L]: 'h-9 px-4 rounded-lg',
        [EnumSize.XL]: 'h-10 px-4 rounded-xl',
        [EnumSize.XXL]: 'h-11 px-6 rounded-2xl',
      },
      shape: {
        [EnumShape.Square]: 'rounded-none',
        [EnumShape.Round]: 'rounded-inherit',
        [EnumShape.Pill]: 'rounded-full',
      },
    },
    defaultVariants: {
      size: EnumSize.S,
      shape: EnumShape.Round,
    },
  },
)
