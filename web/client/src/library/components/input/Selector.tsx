import React from 'react'
import {
  ChevronUpDownIcon,
  ChevronUpIcon,
  ChevronDownIcon,
  CheckIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import * as Select from '@radix-ui/react-select'
import { EnumSize, type Size } from '~/types/enum'

export interface PropsSelector {
  list: Array<{ text: string; value: string }>
  onChange: (value: string) => void
  size?: Size
  name?: string
  value?: string
  disabled?: boolean
  required?: boolean
  autoFocus?: boolean
  className?: string
}

export default React.forwardRef<HTMLButtonElement, PropsSelector>(
  function Selector(
    {
      list = [],
      required = false,
      disabled = false,
      autoFocus = false,
      size = EnumSize.md,
      name,
      value = 'default',
      className,
      onChange,
    }: PropsSelector,
    ref?: React.Ref<HTMLButtonElement>,
  ): JSX.Element {
    const item = list.find(i => i.value === value) ??
      list[0] ?? { text: '', value }

    disabled = disabled || list.length < 2

    return (
      <Select.Root
        name={name}
        value={item.value}
        disabled={disabled}
        required={required}
        onValueChange={onChange}
      >
        <Select.Trigger
          ref={ref}
          className={clsx(
            className,
            disabled && 'opacity-50 cursor-not-allowed',
          )}
          autoFocus={autoFocus}
        >
          <Select.Value />
          <Select.Icon className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-1">
            <ChevronUpDownIcon
              className="w-4"
              aria-hidden="true"
            />
          </Select.Icon>
        </Select.Trigger>
        <Select.Portal className="z-50">
          <Select.Content
            className={clsx(
              'overflow-auto rounded-md bg-theme p-1 shadow-2xl border-2 border-neutral-200 dark:border-neutral-700 ring-opacity-5 focus:outline-none',
              size === EnumSize.sm && 'text-xs',
              size === EnumSize.md && 'text-sm',
              size === EnumSize.lg && 'text-sm',
            )}
          >
            <Select.ScrollUpButton className="flex items-center justify-center h-3 cursor-default">
              <ChevronUpIcon
                className="w-4"
                aria-hidden="true"
              />
            </Select.ScrollUpButton>
            <Select.Viewport className="p-1">
              {list.map(({ text, value }) => (
                <SelectItem
                  key={value}
                  value={value}
                >
                  {text}
                </SelectItem>
              ))}
            </Select.Viewport>
            <Select.ScrollDownButton className="flex items-center justify-center h-3 cursor-default">
              <ChevronDownIcon
                className="w-4"
                aria-hidden="true"
              />
            </Select.ScrollDownButton>
          </Select.Content>
        </Select.Portal>
      </Select.Root>
    )
  },
)

function SelectItem({
  disabled = false,
  value,
  children,
  className,
}: {
  value: string
  children: React.ReactNode
  disabled?: boolean
  className?: string
}): JSX.Element {
  return (
    <Select.Item
      value={value}
      className={clsx(
        'leading-none rounded-md flex items-center h-[25px] pr-[35px] pl-[25px] relative select-none data-[disabled]:pointer-events-none data-[highlighted]:outline-none data-[highlighted]:bg-brand-500 data-[highlighted]:text-brand-100',
        disabled && 'opacity-50 cursor-not-allowed',
        className,
      )}
    >
      <Select.ItemText>{children}</Select.ItemText>
      <Select.ItemIndicator className="absolute left-0 w-4 inline-flex items-center justify-center">
        <CheckIcon />
      </Select.ItemIndicator>
    </Select.Item>
  )
}
