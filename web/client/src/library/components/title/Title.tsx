import clsx from 'clsx'
import { EnumSize, EnumVariant, type Variant, type Size } from '~/types/enum'

export default function Title({
  as = 'p',
  size = EnumSize.sm,
  variant = EnumVariant.Info,
  text,
  className,
}: {
  text: string
  as?: 'p' | 'span' | 'small' | 'h1' | 'h2' | 'h3' | 'h4' | 'h5' | 'h6'
  size?: Size
  variant?: Variant
  className?: string
}): JSX.Element {
  const Tag = as
  return (
    <Tag
      className={clsx(
        'font-bold whitespace-nowrap',
        variant === EnumVariant.Primary &&
          'text-primary-600 dark:text-primary-400',
        variant === EnumVariant.Secondary &&
          'text-secondary-600 dark:text-secondary-400',
        variant === EnumVariant.Success &&
          'text-success-600 dark:text-success-400',
        variant === EnumVariant.Warning &&
          'text-warning-600 dark:text-warning-400',
        variant === EnumVariant.Danger &&
          'text-danger-600 dark:text-danger-400',
        variant === EnumVariant.Info &&
          'text-neutral-600 dark:text-neutral-400',
        size === EnumSize.xs && 'text-xs',
        size === EnumSize.sm && 'text-md',
        size === EnumSize.md && 'text-lg',
        size === EnumSize.lg && 'text-2xl',
        size === EnumSize.xl && 'text-4xl',
        className,
      )}
    >
      {text}
    </Tag>
  )
}
