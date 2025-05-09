import React from 'react'
import Spinner from '@/components/logo/Spinner'
import clsx from 'clsx'
import Loading from './Loading'

export default function LoadingSegment({
  children,
  className,
}: {
  className?: string
  children?: React.ReactNode
}): JSX.Element {
  return (
    <div
      className={clsx(
        'flex justify-center items-center w-full h-full',
        className,
      )}
    >
      <Loading className="inline-block">
        <Spinner className="w-3 h-3 border border-neutral-10 mr-4" />
        <h3 className="text-md">{children}</h3>
      </Loading>
    </div>
  )
}
