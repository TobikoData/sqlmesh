import Fuse, { type IFuseOptions } from 'fuse.js'
import React from 'react'

import { VerticalContainer } from '../VerticalContainer/VerticalContainer'
import { HorizontalContainer } from '../HorizontalContainer/HorizontalContainer'
import { Badge } from '../Badge/Badge'
import { cn } from '@sqlmesh-common/utils'
import { MessageContainer } from '../MessageContainer/MessageContainer'
import { Input } from '../Input/Input'

import './FilterableList.css'

export interface FilterableListProps<TItem> {
  items: TItem[]
  filterOptions?: IFuseOptions<TItem>
  disabled?: boolean
  placeholder?: string
  autoFocus?: boolean
  className?: string
  children: (options: TItem[], resetSearch: () => void) => React.ReactNode
}

export function FilterableList<TItem>({
  items,
  disabled,
  placeholder,
  autoFocus,
  filterOptions,
  className,
  children,
}: FilterableListProps<TItem>) {
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
      <HorizontalContainer className="shrink-0 gap-2 h-auto items-center overflow-visible">
        <Input
          type="search"
          disabled={disabled}
          placeholder={placeholder}
          autoFocus={autoFocus}
          value={search}
          onInput={(e: React.ChangeEvent<HTMLInputElement>) =>
            setSearch(e.target.value)
          }
          inputSize="xs"
          className="FilterableList__Input w-full"
          onClick={(e: React.MouseEvent<HTMLInputElement>) => {
            e.stopPropagation()
          }}
        />
        <Counter
          itemsLength={items.length}
          filteredItemsLength={filteredItems.length}
        />
      </HorizontalContainer>
      {filteredItems.length > 0 ? (
        children(filteredItems, resetSearch)
      ) : (
        <EmptyMessage message="No results found" />
      )}
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
      className={cn(
        'flex items-center gap-1 h-full bg-filterable-list-counter-background text-filterable-list-counter-foreground',
        className,
      )}
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
    <MessageContainer className="w-full h-full rounded-sm text-xs mt-2">
      {message}
    </MessageContainer>
  )
}
