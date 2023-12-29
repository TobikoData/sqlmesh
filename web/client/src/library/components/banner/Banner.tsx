import { Divider as BaseDivider } from '@components/divider/Divider'
import clsx from 'clsx'
import React from 'react'
import { EnumSize, EnumVariant, type Size, type Variant } from '~/types/enum'

function Banner({
  variant = EnumVariant.Neutral,
  size = EnumSize.md,
  isFull = false,
  isCenter = false,
  hasBackground = true,
  hasBackgroundOnHover = false,
  hasBorder = false,
  children,
  className,
}: {
  children: React.ReactNode
  variant?: Variant
  isCenter?: boolean
  isFull?: boolean
  hasBackground?: boolean
  hasBackgroundOnHover?: boolean
  hasBorder?: boolean
  size?: Size
  hasSpinner?: boolean
  className?: string
}): JSX.Element {
  return (
    <div
      className={clsx(
        'w-full text-sm px-4 overflow-hidden rounded-lg',
        size === EnumSize.sm && 'py-2',
        size === EnumSize.md && 'py-4',
        size === EnumSize.lg && 'py-6',
        isFull && 'h-full',
        isCenter && 'justify-center items-center',
        hasBorder && 'border-2',
        getVariant(variant, hasBackground, hasBackgroundOnHover),
        className,
      )}
    >
      {children}
    </div>
  )
}

function Divider({ className }: { className?: string }): JSX.Element {
  return <BaseDivider className={clsx('mx-4 w-full', className)} />
}

function Label({
  children,
  className,
}: {
  children: React.ReactNode
  className?: string
}): JSX.Element {
  return (
    <h4
      className={clsx(
        'font-bold text-sm whitespace-nowrap text-left',
        className,
      )}
    >
      {children}
    </h4>
  )
}

function Description({
  children,
  className,
}: {
  children: React.ReactNode
  className?: string
}): JSX.Element {
  return <p className={clsx('text-neutral-600', className)}>{children}</p>
}

Banner.Label = Label
Banner.Description = Description
Banner.Divider = Divider

export default Banner

function getVariant(
  variant: Variant,
  hasBackground: boolean = true,
  hasBackgroundOnHover: boolean = false,
): string[] {
  switch (variant) {
    case EnumVariant.Primary:
      return [
        hasBackgroundOnHover ? 'hover:bg-primary-5' : '',
        hasBackground ? 'bg-primary-5' : '',
        'text-primary-600 dark:text-primary-400',
      ]
    case EnumVariant.Success:
      return [
        hasBackgroundOnHover ? 'hover:bg-success-5' : '',
        hasBackground ? 'bg-success-5' : '',
        'text-success-600 dark:text-success-400',
      ]
    case EnumVariant.Warning:
      return [
        hasBackgroundOnHover ? 'hover:bg-warning-5' : '',
        hasBackground ? 'bg-warning-5' : '',
        'text-warning-600 dark:text-warning-400',
      ]
    case EnumVariant.Danger:
      return [
        hasBackgroundOnHover ? 'hover:bg-danger-5' : '',
        hasBackground ? 'bg-danger-5' : '',
        'text-danger-600 dark:text-danger-400',
      ]
    case EnumVariant.Info:
      return [
        hasBackgroundOnHover ? 'hover:bg-info-5' : '',
        hasBackground ? 'bg-info-5' : '',
        'text-info-600 dark:text-info-400',
      ]
    case EnumVariant.Neutral:
      return [
        hasBackgroundOnHover ? 'hover:bg-neutral-5' : '',
        hasBackground ? 'bg-neutral-5' : '',
        'text-neutral-600 dark:text-neutral-400',
      ]
    default:
      return ['bg-transparent', 'text-neutral-600 dark:text-neutral-400']
  }
}
