import Input from '@components/input/Input'
import { useVirtualizer } from '@tanstack/react-virtual'
import { isArrayEmpty, isNil, isNotNil, isOnScreen } from '@utils/index'
import clsx from 'clsx'
import { useEffect, useMemo, useRef, useState } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import { EnumSize, EnumVariant, type Variant } from '~/types/enum'

interface ListItem<
  TListItem extends Record<string, any> = Record<string, any>,
> {
  id: string
  to: string
  name: string
  item: TListItem
  description?: string
  text?: string
  disabled?: boolean
}

export default function SourceList<
  TItem extends Record<string, any> = Record<string, string>,
  TType extends Record<string, string> = Record<string, string>,
>({
  listItem,
  items = [],
  types,
  by = 'id',
  byName,
  byDescription,
  to,
  disabled = false,
  className,
}: {
  listItem: (listItem: ListItem<TItem>) => React.ReactNode
  by: string
  to: string
  items?: TItem[]
  types?: TType
  byName?: string
  disabled?: boolean
  byDescription?: string
  className?: string
}): JSX.Element {
  const [filter, setFilter] = useState('')

  const { pathname } = useLocation()

  const scrollableAreaRef = useRef<HTMLDivElement>(null)

  const filtered =
    filter === ''
      ? items
      : items.filter(item => {
          const id = item[by] ?? ''
          const description = String(
            isNil(byDescription) ? '' : item?.[byDescription] ?? '',
          )
          const name = String(isNil(byName) ? '' : item?.[byName] ?? '')
          const type = String(types?.[id] ?? '')

          return (
            name.includes(filter) ||
            description.includes(filter) ||
            type.includes(filter)
          )
        })

  const activeItemIndex = useMemo((): number => {
    return filtered.findIndex(filteredItem => {
      return `${to}/${filteredItem[by]}` === pathname
    })
  }, [filtered])

  const rowVirtualizer = useVirtualizer({
    count: filtered.length,
    getScrollElement: () => scrollableAreaRef.current,
    estimateSize: () => 28,
  })

  useEffect(() => {
    console.log({ activeItemIndex })
    if (activeItemIndex > -1) {
      rowVirtualizer.scrollToIndex(activeItemIndex, {
        align: 'center',
        behavior: 'smooth',
      })
    }
  }, [activeItemIndex])

  return (
    <div className={clsx('flex flex-col w-full h-full', className)}>
      <div
        className="p-2 h-full overflow-auto hover:scrollbar scrollbar--horizontal scrollbar--vertical"
        ref={scrollableAreaRef}
      >
        <div
          className="relative"
          style={{ height: `${rowVirtualizer.getTotalSize()}px` }}
        >
          {isArrayEmpty(filtered) && (
            <li
              key="not-found"
              className="p-2"
              onClick={() => {
                setFilter('')
              }}
            >
              No Results Found
            </li>
          )}
          {rowVirtualizer.getVirtualItems().map(virtualItem => {
            const id = (filtered[virtualItem.index] as Record<string, string>)[
              by
            ]!
            const name = isNil(byName)
              ? ''
              : (filtered[virtualItem.index] as Record<string, string>)?.[
                  byName
                ] ?? ''
            const description = isNil(byDescription)
              ? undefined
              : (filtered[virtualItem.index] as Record<string, string>)?.[
                  byDescription
                ] ?? undefined

            return (
              <div
                key={virtualItem.key}
                className={clsx(
                  'text-sm font-normal absolute top-0 left-0 w-full',
                  disabled && 'cursor-not-allowed',
                )}
                style={{
                  height: `${virtualItem.size}px`,
                  transform: `translateY(${virtualItem.start}px)`,
                }}
                tabIndex={id === filter ? -1 : 0}
              >
                {listItem?.({
                  id,
                  to: `${to}/${id}`,
                  name,
                  description,
                  text: (types as Record<string, string>)?.[id],
                  disabled,
                  item: filtered[virtualItem.index]!,
                })}
              </div>
            )
          })}
        </div>
      </div>
      <div className="p-2 w-full flex justify-between">
        <Input
          className="w-full !m-0"
          size={EnumSize.sm}
        >
          {({ className }) => (
            <Input.Textfield
              className={clsx(className, 'w-full')}
              value={filter}
              placeholder="Filter items"
              onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                setFilter(e.target.value)
              }}
            />
          )}
        </Input>
        <div className="ml-1 px-3 bg-primary-10 text-primary-500 rounded-full text-xs flex items-center">
          {filtered.length}
        </div>
      </div>
    </div>
  )
}

export function SourceListItem({
  name,
  description,
  to,
  text,
  variant,
  disabled = false,
  handleDelete,
}: {
  name: string
  description?: string
  to: string
  variant?: Variant
  disabled?: boolean
  text?: string
  handleDelete?: () => void
}): JSX.Element {
  function handleKeyUp(e: React.KeyboardEvent<HTMLAnchorElement>): void {
    if (e.key === 'Delete' || e.key === 'Backspace') {
      e.preventDefault()
      e.stopPropagation()

      handleDelete?.()
    }
  }

  return (
    <NavLink
      onKeyUp={handleKeyUp}
      to={to}
      className={({ isActive }) =>
        clsx(
          'block overflow-hidden px-2 py-1 rounded-md w-full text-sm font-semibold',
          disabled && 'opacity-50 pointer-events-none',
          isActive
            ? variant === EnumVariant.Primary
              ? 'text-primary-500 bg-primary-10'
              : variant === EnumVariant.Danger
                ? 'text-danger-500 bg-danger-5'
                : 'text-neutral-500 bg-neutral-10'
            : 'hover:bg-neutral-10 text-neutral-400 dark:text-neutral-300',
        )
      }
    >
      <div className="flex items-center overflow-hidden whitespace-nowrap overflow-ellipsis">
        {name}
        {isNotNil(text) && (
          <span className="flex items-center ml-2 px-2 h-4 rounded-md text-[0.5rem] bg-neutral-10 dark:text-neutral-200 text-neutral-700 font-bold">
            {text}
          </span>
        )}
      </div>
      {isNotNil(description) && (
        <p className="text-xs overflow-hidden whitespace-nowrap overflow-ellipsis">
          {description}
        </p>
      )}
    </NavLink>
  )
}
