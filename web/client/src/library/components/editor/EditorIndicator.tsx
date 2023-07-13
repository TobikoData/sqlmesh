import clsx from 'clsx'
import React from 'react'
import { isFalse, isNil, isNotNil, isTrue } from '~/utils'

const EditorIndicator = function EditorIndicator({
  text,
  children,
  className,
}: {
  className?: string
  children: React.ReactNode
  text: string
}): JSX.Element {
  return (
    <small className={clsx('font-bold whitespace-nowrap text-xs', className)}>
      {isNotNil(text) && <span>{text}:&nbsp;</span>}
      {children}
    </small>
  )
}

function IndicatorText({
  text,
  className,
}: {
  text: string
  className?: string
}): JSX.Element {
  return <span className={clsx('font-normal', className)}>{text}</span>
}

function IndicatorLight({
  ok,
  className,
}: {
  ok?: boolean
  className?: string
}): JSX.Element {
  return (
    <span
      className={clsx(
        'inline-block w-2 h-2 rounded-full',
        isNil(ok) && 'bg-warning-500',
        isTrue(ok) && 'bg-success-500',
        isFalse(ok) && 'bg-danger-500',
        className,
      )}
    ></span>
  )
}

EditorIndicator.Text = IndicatorText
EditorIndicator.Light = IndicatorLight

export default EditorIndicator
