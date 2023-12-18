import React, { Fragment, useMemo, useRef, useState } from 'react'
import Input from '@components/input/Input'
import { isArrayEmpty, isNil, isNotNil, truncate } from '@utils/index'
import { EnumSize, type Size } from '~/types/enum'
import { EMPTY_STRING, filterListBy, highlightMatch } from './help'
import { useNavigate } from 'react-router-dom'
import clsx from 'clsx'
import { Popover, Transition } from '@headlessui/react'
import { useClickAway } from '@uidotdev/usehooks'

interface PropsSearchListInput {
  value: string
  placeholder?: string
  className?: string
  size?: Size
  autoFocus?: boolean
  onInput?: (e: React.ChangeEvent<HTMLInputElement>) => void
  onKeyDown?: (e: React.KeyboardEvent<HTMLInputElement>) => void
}

const SearchListInput = React.forwardRef<
  HTMLInputElement,
  PropsSearchListInput
>(function SearchListInput(
  {
    value,
    placeholder,
    size = EnumSize.md,
    autoFocus = false,
    onInput,
    onKeyDown,
    className,
  },
  ref: React.Ref<HTMLInputElement>,
): JSX.Element {
  return (
    <Input
      className={className}
      size={size}
    >
      {({ className }) => (
        <Input.Textfield
          ref={ref}
          className={clsx(className, 'w-full')}
          autoFocus={autoFocus}
          placeholder={placeholder}
          value={value}
          onInput={onInput}
          onKeyDown={onKeyDown}
        />
      )}
    </Input>
  )
})

export default function SearchList<
  T extends Record<string, any> = Record<string, any>,
>({
  list,
  size = EnumSize.sm,
  searchBy,
  displayBy,
  descriptionBy,
  onSelect,
  to,
  placeholder = 'Search',
  autoFocus = false,
  isFullWidth = false,
  showIndex = true,
  className,
}: {
  list: T[]
  searchBy: string
  displayBy: string
  descriptionBy?: string
  placeholder?: string
  onSelect?: (item: T) => void
  autoFocus?: boolean
  showIndex?: boolean
  to?: (item: T) => string
  size?: Size
  isFullWidth?: boolean
  className?: string
}): JSX.Element {
  const navigate = useNavigate()

  const elList = useRef<HTMLDivElement>(null)
  const elTrigger = useRef<HTMLButtonElement>(null)

  const indices: Array<[T, string]> = useMemo(
    () => list.map(item => [item, item[searchBy]]),
    [list],
  )

  const [search, setSearch] = useState<string>(EMPTY_STRING)
  const [activeIndex, setActiveIndex] = useState(0)

  const ref = useClickAway(() => {
    setSearch(EMPTY_STRING)
  })

  const showSearchResults = search !== EMPTY_STRING
  const found = filterListBy<T>(indices, search)

  function hideList(): void {
    setActiveIndex(0)
    setSearch(EMPTY_STRING)
    elTrigger.current?.focus()
  }

  function selectListItem(): void {
    const item = found[activeIndex]?.[0]

    if (isNil(item)) return

    if (isNil(to)) {
      onSelect?.(item)
    } else {
      navigate(to(item))
    }

    hideList()
  }

  return (
    <div
      className={clsx('px-2 py-1 relative', className)}
      ref={ref}
      onKeyDown={(e: React.KeyboardEvent) => {
        if (e.key === 'Escape') {
          hideList()
        }
      }}
    >
      <Popover className="relative flex">
        <Popover.Button
          ref={elTrigger}
          as={SearchListInput}
          className="w-full !m-0"
          size={size}
          value={search}
          placeholder={placeholder}
          onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
            console.log(e, e.target.value)
            setSearch(e.target.value.trim())
          }}
          onKeyDown={(e: React.KeyboardEvent) => {
            if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
              elList.current?.focus()
            }
          }}
          autoFocus={autoFocus}
        />
        <Transition
          show={showSearchResults}
          as={Fragment}
          enter="transition ease-out duration-200"
          enterFrom="opacity-0 translate-y-1"
          enterTo="opacity-100 translate-y-0"
          leave="transition ease-in duration-150"
          leaveFrom="opacity-100 translate-y-0"
          leaveTo="opacity-0 translate-y-1"
        >
          <Popover.Panel
            static
            focus
            className={clsx(
              'absolute z-10 transform cursor-pointer rounded-lg bg-theme border-2 border-neutral-200',
              'p-2 bg-theme dark:bg-theme-lighter overflow-auto hover:scrollbar scrollbar--vertical scrollbar--horizontal shadow-2xl',
              size === EnumSize.sm && 'mt-7 max-h-[30vh]',
              size === EnumSize.md && 'mt-9 max-h-[40vh]',
              size === EnumSize.lg && 'mt-12 max-h-[50vh]',
              isFullWidth ? 'w-full' : 'w-full max-w-[20rem]',
            )}
            ref={elList}
            onKeyDown={(e: React.KeyboardEvent) => {
              if (e.key === 'ArrowUp') {
                activeIndex > 0 && setActiveIndex(activeIndex - 1)
              }

              if (e.key === 'ArrowDown') {
                activeIndex < found.length - 1 &&
                  setActiveIndex(activeIndex + 1)
              }

              if (e.key === 'Enter') {
                e.preventDefault()

                selectListItem()
              }
            }}
            onMouseOver={(e: React.MouseEvent) => {
              e.stopPropagation()

              const elListItem = (e.target as HTMLElement).closest(
                '[role="menuitem"]',
              ) as HTMLElement

              if (isNil(elListItem)) return

              const index = Number(elListItem.dataset.index)

              setActiveIndex(Number(index))
            }}
          >
            {isArrayEmpty(found) && showSearchResults ? (
              <div
                key="not-found"
                className={clsx(
                  size === EnumSize.sm && 'p-1',
                  size === EnumSize.md && 'p-2',
                  size === EnumSize.lg && 'p-3',
                )}
              >
                No Results Found
              </div>
            ) : (
              found.map(([item, index], idx) => (
                <div
                  key={item[displayBy]}
                  role="menuitem"
                  data-index={idx}
                  className={clsx(
                    'cursor-pointer rounded-lg',
                    activeIndex === idx && 'bg-neutral-5',
                  )}
                >
                  <SearchResult<T>
                    item={item}
                    index={index}
                    search={search}
                    displayBy={displayBy}
                    descriptionBy={descriptionBy}
                    size={size}
                    showIndex={showIndex}
                    onClick={(e: React.MouseEvent) => {
                      e.stopPropagation()
                      e.preventDefault()

                      selectListItem()
                    }}
                  />
                </div>
              ))
            )}
          </Popover.Panel>
        </Transition>
      </Popover>
    </div>
  )
}

function SearchResult<T extends Record<string, any> = Record<string, any>>({
  item,
  index,
  search,
  displayBy,
  descriptionBy,
  size,
  showIndex = true,
  onClick,
}: {
  item: T
  index: string
  search: string
  size: Size
  displayBy: string
  descriptionBy?: string
  showIndex?: boolean
  onClick?: (e: React.MouseEvent) => void
}): JSX.Element {
  return (
    <div
      onClick={onClick}
      className={clsx(
        'font-normal w-full overflow-hidden whitespace-nowrap overflow-ellipsis px-2',
        size === EnumSize.sm && 'text-sm py-1',
        size === EnumSize.md && 'text-md py-2',
        size === EnumSize.lg && 'text-lg py-3',
      )}
    >
      {showIndex ? (
        <>
          <span
            title={item[displayBy]}
            className="font-bold"
          >
            {truncate(item[displayBy], 50, 20)}
          </span>
          <small
            className="block text-neutral-600 italic overflow-hidden whitespace-nowrap overflow-ellipsis"
            dangerouslySetInnerHTML={{
              __html: highlightMatch(index, search),
            }}
          ></small>
        </>
      ) : (
        <span
          className="font-bold"
          dangerouslySetInnerHTML={{
            __html: highlightMatch(item[displayBy], search),
          }}
        ></span>
      )}
      {isNotNil(descriptionBy) && (
        <small className="block text-neutral-600 italic overflow-hidden whitespace-nowrap overflow-ellipsis">
          {item[descriptionBy]}
        </small>
      )}
    </div>
  )
}
