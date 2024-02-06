import Input from '@components/input/Input'
import { type Virtualizer, useVirtualizer } from '@tanstack/react-virtual'
import { isArrayEmpty, isNil, isNotNil } from '@utils/index'
import clsx from 'clsx'
import { useEffect, useMemo, useRef, useState } from 'react'
import { NavLink } from 'react-router-dom'
import { EnumSize, EnumVariant, type Variant } from '~/types/enum'
import { Button } from '../button/Button'

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
  isActive,
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
  isActive?: (id: string) => boolean
  byName?: string
  disabled?: boolean
  byDescription?: string
  className?: string
}): JSX.Element {
  const [filter, setFilter] = useState('')

  const scrollableAreaRef = useRef<HTMLDivElement>(null)

  const getActiveIndex = (itemsList: TItem[]): number =>
    itemsList.findIndex(item => isNotNil(isActive) && isActive(item[by]))

  const [activeItemIndex, filtered] = useMemo(() => {
    if (filter === '') {
      return [getActiveIndex(items), items]
    }
    let activeIndex = -1
    const filteredList: TItem[] = []
    items.forEach((item, index) => {
      const id = item[by] ?? ''
      const description = String(
        isNil(byDescription) ? '' : item?.[byDescription] ?? '',
      )
      const name = String(isNil(byName) ? '' : item?.[byName] ?? '')
      const type = String(types?.[id] ?? '')
      if (
        name.includes(filter) ||
        description.includes(filter) ||
        type.includes(filter)
      ) {
        filteredList.push(item)
        if (isNotNil(isActive) && isActive(item[by])) {
          activeIndex = index
        }
      }
    })
    // const filteredList = items.filter(item => {
    //   const id = item[by] ?? ''
    //   const description = String(
    //     isNil(byDescription) ? '' : item?.[byDescription] ?? '',
    //   )
    //   const name = String(isNil(byName) ? '' : item?.[byName] ?? '')
    //   const type = String(types?.[id] ?? '')

    //   return (
    //     name.includes(filter) ||
    //     description.includes(filter) ||
    //     type.includes(filter)
    //   )
    // })
    return [activeIndex, filteredList]
  }, [items, filter])

  console.log({ activeItemIndex, filtered })

  const rowVirtualizer = useVirtualizer({
    count: filtered.length,
    getScrollElement: () => scrollableAreaRef.current,
    estimateSize: () => 28,
  })

  const scrollToItem = ({
    itemIndex,
    isSmoothScroll = true,
  }: {
    itemIndex: number
    isSmoothScroll?: boolean
  }): void => {
    rowVirtualizer.scrollToIndex(itemIndex, {
      align: 'center',
      behavior: isSmoothScroll ? 'smooth' : 'auto',
    })
  }

  const isOutsideVisibleRange = ({
    itemIndex,
    range,
  }: {
    itemIndex: number
    range: Virtualizer<HTMLDivElement, Element>['range']
  }): boolean =>
    isNotNil(range) &&
    (range.startIndex > itemIndex || range?.endIndex < itemIndex)

  /**
   * The return button should appear when the
   * active item is available in the list (not
   * filtered out) and it is not in the visible
   * range of the virtualized list
   */
  const shouldShowReturnButton =
    activeItemIndex > -1 &&
    isOutsideVisibleRange({
      range: rowVirtualizer.range,
      itemIndex: activeItemIndex,
    })

  // scroll to the active item when the activeItemIndex changes
  useEffect(() => {
    if (
      activeItemIndex > -1 &&
      isOutsideVisibleRange({
        range: rowVirtualizer.range,
        itemIndex: activeItemIndex,
      })
    ) {
      scrollToItem({ itemIndex: activeItemIndex, isSmoothScroll: false })
    }
  }, [activeItemIndex])

  return (
    <div className={clsx('flex flex-col w-full h-full relative', className)}>
      {shouldShowReturnButton && (
        <Button
          className="absolute left-[50%] translate-x-[-50%] top-0 z-10 text-ellipsis !block overflow-hidden no-wrap max-w-[90%] opacity-50 hover:opacity-100"
          onClick={() => scrollToItem({ itemIndex: activeItemIndex })}
          size="sm"
          variant="neutral"
        >
          Scroll to selected
        </Button>
      )}
      <div
        className="p-2 h-full overflow-auto hover:scrollbar scrollbar--horizontal scrollbar--vertical"
        ref={scrollableAreaRef}
      >
        <ul
          className="relative"
          style={{ height: `${rowVirtualizer.getTotalSize()}px` }}
        >
          {isArrayEmpty(filtered) && (
            <li
              key="not-found"
              className="p-2"
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
              <li
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
              </li>
            )
          })}
        </ul>
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
              type="search"
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
