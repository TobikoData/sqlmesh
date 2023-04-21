import React from 'react'
import Spinner from '@components/logo/Spinner'
import clsx from 'clsx'

export default function Loading({
  hasSpinner = false,
  children,
  className,
}: {
  children: React.ReactNode
  hasSpinner?: boolean
  className?: string
}): JSX.Element {
  return (
    <span className={clsx(className)}>
      <span className="flex items-center w-full">
        {hasSpinner && <Spinner className="w-4 h-4 mr-2" />}
        {children}
      </span>
    </span>
  )
}
