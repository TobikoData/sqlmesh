import Fuse, { type IFuseOptions } from 'fuse.js'
import React from 'react'

import { Input } from '../extra/input'
import { VerticalContainer } from '../VerticalContainer/VerticalContainer'
import { HorizontalContainer } from '../HorizontalContainer/HorizontalContainer'
import { Badge } from '../Badge/Badge'
import { cn } from '@/utils'
import MessageContainer from '../MessageContainer/MessageContainer'

export function FilterableList<
  TItem extends Record<string, any> = Record<string, any>,
>({
  items,
  disabled,
  placeholder,
  autoFocus,
  filterOptions,
  className,
  children,
  beforeInput,
  afterInput,
}: {
  items: TItem[]
  filterOptions?: IFuseOptions<any>
  disabled?: boolean
  placeholder?: string
  autoFocus?: boolean
  className?: string
  children: (options: TItem[], resetSearch: () => void) => React.ReactNode
  beforeInput?: (options: TItem[]) => React.ReactNode
  afterInput?: (options: TItem[]) => React.ReactNode
}) {
  const [search, setSearch] = React.useState('')

  const fuse = new Fuse(items, filterOptions)

  const filteredItems = search
    ? fuse.search(search).map(result => result.item)
    : items

  const resetSearch = React.useCallback(() => {
    setSearch('')
  }, [])

  return (
    <VerticalContainer
      data-component="FilterableList"
      className={cn('p-1', className)}
    >
      <HorizontalContainer className="shrink-0 gap-2 h-auto items-center">
        {beforeInput?.(filteredItems)}
        <Input
          type="search"
          disabled={disabled}
          placeholder={placeholder}
          autoFocus={autoFocus}
          value={search}
          onInput={(e: React.ChangeEvent<HTMLInputElement>) =>
            setSearch(e.target.value)
          }
          className={cn(
            'w-full md:text-xs text-xs h-6 text-prose px-2 rounded-sm',
            disabled && 'bg-neutral-100',
          )}
        />
        {afterInput?.(filteredItems)}
      </HorizontalContainer>
      {children(filteredItems, resetSearch)}
    </VerticalContainer>
  )
}

function Counter({
  itemsLength,
  filteredItemsLength,
  className,
}: {
  itemsLength: number
  filteredItemsLength: number
  className?: string
}) {
  return (
    <Badge
      size="2xs"
      className={cn('flex items-center gap-1', className)}
    >
      {itemsLength !== filteredItemsLength && (
        <>
          <span>{filteredItemsLength}</span>
          <span>/</span>
        </>
      )}
      <span>{itemsLength}</span>
    </Badge>
  )
}

function EmptyMessage({
  message = 'No Results Found',
}: {
  message?: React.ReactNode
}) {
  return (
    <MessageContainer className="w-full rounded-sm text-xs mt-2">
      {message}
    </MessageContainer>
  )
}

FilterableList.Counter = Counter
FilterableList.EmptyMessage = EmptyMessage
