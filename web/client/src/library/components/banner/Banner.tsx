import clsx from 'clsx'
import React from 'react'
import { EnumVariant, type Variant } from '~/types/enum'

function Banner({
  variant = EnumVariant.Info,
  hasBorder = false,
  isFull = false,
  isCenter = false,
  children,
  className,
}: {
  children: React.ReactNode
  variant?: Variant
  isCenter?: boolean
  isFull?: boolean
  hasBorder?: boolean
  className?: string
}): JSX.Element {
  return (
    <div
      className={clsx(
        'w-full text-sm overflow-hidden p-4 rounded-lg',
        isFull && 'h-full',
        isCenter && 'flex justify-center items-center',
        hasBorder && 'border-2',
        variant === EnumVariant.Primary &&
          'bg-primary-10 border-primary-400 text-primary-600 dark:text-primary-400',
        variant === EnumVariant.Secondary &&
          'bg-secondary-10 border-secondary-400 text-secondary-600 dark:text-secondary-400',
        variant === EnumVariant.Success &&
          'bg-success-10 border-success-400 text-success-600 dark:text-success-400',
        variant === EnumVariant.Warning &&
          'bg-warning-10 border-warning-400 text-warning-600 dark:text-warning-400',
        variant === EnumVariant.Danger &&
          'bg-danger-10 border-danger-400 text-danger-600 dark:text-danger-400',
        variant === EnumVariant.Info &&
          'bg-neutral-5 border-neutral-400 text-neutral-600 dark:text-neutral-400',
        className,
      )}
    >
      {children}
    </div>
  )
}

function Headline({
  children,
  className,
}: {
  children: React.ReactNode
  className?: string
}): JSX.Element {
  return (
    <h4 className={clsx('mb-2 font-bold text-lg', className)}>{children}</h4>
  )
}

function Description({
  children,
  className,
}: {
  children: React.ReactNode
  className?: string
}): JSX.Element {
  return <p className={clsx('text-prose', className)}>{children}</p>
}

Banner.Headline = Headline
Banner.Description = Description

export default Banner
