import React from 'react'
import Spinner from '@components/logo/Spinner'
import clsx from 'clsx'
import { EnumSize, EnumVariant, type Variant, type Size } from '~/types/enum'
import { isNotNil } from '@utils/index'
import Title from '@components/title/Title'

export default function Loading({
  hasSpinner = false,
  size = EnumSize.sm,
  variant = EnumVariant.Info,
  text,
  children,
  className,
}: {
  children?: React.ReactNode
  text?: string
  size?: Size
  variant?: Variant
  hasSpinner?: boolean
  className?: string
}): JSX.Element {
  return (
    <span
      className={clsx(
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
        className,
      )}
    >
      <span className="flex items-center w-full">
        {hasSpinner && (
          <Spinner
            variant={variant}
            className={clsx(
              size === EnumSize.xs && 'w-2 h-2 mr-2',
              size === EnumSize.sm && 'w-3 h-3 mr-2',
              size === EnumSize.md && 'w-4 h-4 mr-2',
              size === EnumSize.lg && 'w-5 h-5 mr-4',
              size === EnumSize.xl && 'w-6 h-6 mr-4',
            )}
          />
        )}
        {isNotNil(text) ? (
          <Title
            size={size}
            variant={variant}
            text={text}
          />
        ) : (
          children
        )}
      </span>
    </span>
  )
}
