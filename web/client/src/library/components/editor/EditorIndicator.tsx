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
      {isNotNil<string>(text) && <span>{text}:&nbsp;</span>}
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

function IndicatorSelector<T = any>({
  value,
  className,
  onChange,
  options,
  children,
}: {
  value?: string
  className?: string
  options: T[]
  onChange: (e: React.ChangeEvent<HTMLSelectElement>) => void
  children: (option: T) => JSX.Element
}): JSX.Element {
  return (
    <select
      className={clsx(
        'text-xs m-0 px-1 py-[0.125rem] bg-neutral-10 rounded',
        className,
      )}
      value={value}
      onChange={onChange}
    >
      {options.map(children)}
    </select>
  )
}

function IndicatorSelectorOption({
  value,
  className,
  children,
}: {
  className?: string
  value: string
  children: React.ReactNode
}): JSX.Element {
  return (
    <option
      className={clsx(className)}
      value={value}
    >
      {children}
    </option>
  )
}

EditorIndicator.Text = IndicatorText
EditorIndicator.Light = IndicatorLight
EditorIndicator.Selector = IndicatorSelector
EditorIndicator.SelectorOption = IndicatorSelectorOption

export default EditorIndicator
