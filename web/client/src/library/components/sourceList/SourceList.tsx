import Input from '@components/input/Input'
import { type Virtualizer, useVirtualizer } from '@tanstack/react-virtual'
import { isArrayEmpty, isNil, isNotNil, isStringEmptyOrNil } from '@utils/index'
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
  items = [],
  keyId = 'id',
  keyName = '',
  keyDescription = '',
  disabled = false,
  to,
  types,
  className,
  isActive,
  listItem,
}: {
  keyId: string
  to: string
  items?: TItem[]
  types?: TType
  keyName?: string
  keyDescription?: string
  disabled?: boolean
  className?: string
  isActive?: (id: string) => boolean
  listItem: (listItem: ListItem<TItem>) => React.ReactNode
}): JSX.Element {
  const [filter, setFilter] = useState('')

  const scrollableAreaRef = useRef<HTMLDivElement>(null)

  const [activeItemIndex, filtered] = useMemo(() => {
    let activeIndex = -1
    const filteredList: TItem[] = []

    items.forEach((item, index) => {
      const id = ensureString(item[keyId])
      const description = ensureString(item[keyDescription])
      const name = ensureString(item[keyName])
      const type = ensureString(types?.[id])

      if (
        name.includes(filter) ||
        description.includes(filter) ||
        type.includes(filter)
      ) {
        filteredList.push(item)
      }

      if (isNotNil(isActive) && isActive(item[keyId])) {
        activeIndex = index
      }
    })

    return [activeIndex, filteredList]
  }, [items, filter, isActive])

  const rowVirtualizer = useVirtualizer({
    count: filtered.length,
    getScrollElement: () => scrollableAreaRef.current,
    estimateSize: () => 32 + (keyDescription.length > 0 ? 16 : 0),
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
    isStringEmptyOrNil(filter) &&
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

  const rows = rowVirtualizer.getVirtualItems()
  const totalSize = rowVirtualizer.getTotalSize()

  return (
    <div
      className={clsx(
        'flex flex-col w-full h-full text-sm text-neutral-600 dark:text-neutral-300',
        className,
      )}
    >
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
      <div className="mt-2 w-full h-full relative">
        {shouldShowReturnButton && (
          <Button
            className="absolute left-[50%] translate-x-[-50%] -top-2 z-10 text-ellipsis !block overflow-hidden no-wrap max-w-[90%] !border-neutral-20 shadow-md !bg-theme !hover:bg-theme text-neutral-500 dark:text-neutral-300 !focus:ring-2 !focus:ring-theme-500 !focus:ring-offset-2 !focus:ring-offset-theme-50 !focus:ring-opacity-50 !focus:outline-none !focus:ring-offset-transparent !focus:ring-offset-0 !focus:ring"
            onClick={() => scrollToItem({ itemIndex: activeItemIndex })}
            size={EnumSize.sm}
            variant={EnumVariant.Secondary}
          >
            Scroll to selected
          </Button>
        )}
        <div
          ref={scrollableAreaRef}
          className="w-full h-full relative overflow-hidden overflow-y-auto hover:scrollbar scrollbar--horizontal scrollbar--vertical pt-2"
          style={{ contain: 'strict' }}
        >
          <div
            className="relative w-full"
            style={{
              height: totalSize > 0 ? `${totalSize}px` : '100%',
            }}
          >
            <ul
              className="w-full absolute top-0 left-0 px-2"
              style={{ transform: `translateY(${rows[0]?.start ?? 0}px)` }}
            >
              {isArrayEmpty(filtered) && (
                <li
                  key="not-found"
                  className="px-2 py-0.5 text-center whitespace-nowrap overflow-ellipsis overflow-hidden"
                >
                  {filter.length > 0 ? 'No Results Found' : 'Empty List'}
                </li>
              )}
              {rows.map(virtualItem => {
                const item = filtered[virtualItem.index]!
                const id = ensureString(item[keyId])
                const description = ensureString(item[keyDescription])
                const name = ensureString(item[keyName])
                const text = ensureString(types?.[id])

                return (
                  <li
                    key={virtualItem.key}
                    data-index={virtualItem.index}
                    ref={rowVirtualizer.measureElement}
                    className={clsx(
                      'font-normal w-full',
                      disabled && 'cursor-not-allowed',
                    )}
                    tabIndex={id === filter ? -1 : 0}
                  >
                    {listItem?.({
                      id,
                      to: `${to}/${id}`,
                      name,
                      description,
                      text,
                      disabled,
                      item: filtered[virtualItem.index]!,
                    })}
                  </li>
                )
              })}
            </ul>
          </div>
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
          'block overflow-hidden px-2 py-1.5 rounded-md w-full font-semibold',
          disabled && 'opacity-50 pointer-events-none',
          isActive
            ? variant === EnumVariant.Primary
              ? 'text-primary-500 bg-primary-10'
              : variant === EnumVariant.Danger
              ? 'text-danger-500 bg-danger-5'
              : 'text-neutral-600 dark:text-neutral-100 bg-neutral-10'
            : 'hover:bg-neutral-5 text-neutral-500 dark:text-neutral-400',
        )
      }
    >
      <div className="flex items-center">
        <span className="whitespace-nowrap overflow-ellipsis overflow-hidden min-w-10">
          {name}
        </span>
        {isNotNil(text) && (
          <span className=" ml-2 px-2 rounded-md leading-0 text-[0.5rem] bg-neutral-10 text-neutral-700 dark:text-neutral-200">
            {text}
          </span>
        )}
      </div>
      {isNotNil(description) && (
        <p className="text-xs overflow-hidden whitespace-nowrap overflow-ellipsis text-neutral-300 dark:text-neutral-500">
          {description}
        </p>
      )}
    </NavLink>
  )
}

function ensureString(value?: string | number): string {
  return isNil(value) ? '' : String(value)
}
