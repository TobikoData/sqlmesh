import React, { useMemo, useState } from 'react'
import Input from '@components/input/Input'
import { isFalse, isArrayEmpty, isArrayNotEmpty } from '@utils/index'
import { EnumSize, type Size } from '~/types/enum'
import { EMPTY_STRING, filterListBy, highlightMatch } from './help'
import { Link } from 'react-router-dom'

export default function SearchList<
  T extends Record<string, any> = Record<string, any>,
>({
  list,
  size = EnumSize.sm,
  searchBy,
  displayBy,
  onSelect,
  to,
  autoFocus = false,
}: {
  list: T[]
  searchBy: string
  displayBy: string
  onSelect?: (item: T) => void
  autoFocus?: boolean
  to?: (item: T) => string
  size?: Size
}): JSX.Element {
  const indices: Array<[T, string]> = useMemo(
    () => list.map(item => [item, item[searchBy]]),
    [list],
  )

  const [search, setSearch] = useState<string>(EMPTY_STRING)

  const showSearchResults = search !== EMPTY_STRING && search.length > 1
  const found = isFalse(showSearchResults)
    ? []
    : filterListBy<T>(indices, search)

  return (
    <div
      className="p-2 relative"
      onKeyDown={(e: React.KeyboardEvent) => {
        if (e.key === 'Escape') {
          setSearch(EMPTY_STRING)
        }
      }}
    >
      <Input
        className="w-full !m-0"
        size={size}
        value={search}
        placeholder="Search"
        onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
          setSearch(e.target.value.trim())
        }}
        autoFocus={autoFocus}
      />
      {showSearchResults && (
        <ul className="p-2 bg-theme dark:bg-theme-lighter absolute w-full z-10 mt-2 rounded-lg max-h-[25vh] overflow-auto scrollbar scrollbar--vertical scrollbar--horizontal shadow-2xl">
          {isArrayEmpty(found) && (
            <li
              key="not-found"
              className="p-2"
              onClick={(e: React.MouseEvent) => {
                e.stopPropagation()

                setSearch(EMPTY_STRING)
              }}
            >
              No Results Found
            </li>
          )}
          {isArrayNotEmpty(found) &&
            found.map(([item, index]) => (
              <li
                key={item.name}
                className="p-2 cursor-pointer hover:bg-primary-10 rounded-lg"
              >
                {to == null ? (
                  <SearchResult<T>
                    item={item}
                    index={index}
                    search={search}
                    displayBy={displayBy}
                    onClick={(e: React.MouseEvent) => {
                      e.stopPropagation()

                      onSelect?.(item)
                      setSearch(EMPTY_STRING)
                    }}
                  />
                ) : (
                  <Link
                    to={to(item)}
                    onClick={(e: React.MouseEvent) => {
                      e.stopPropagation()

                      setSearch(EMPTY_STRING)
                    }}
                    className="text-md font-normal mb-1 w-full"
                  >
                    <SearchResult<T>
                      item={item}
                      index={index}
                      search={search}
                      displayBy={displayBy}
                    />
                  </Link>
                )}
              </li>
            ))}
        </ul>
      )}
    </div>
  )
}

function SearchResult<T extends Record<string, any> = Record<string, any>>({
  item,
  index,
  search,
  displayBy,
  onClick,
}: {
  item: T
  index: string
  search: string
  displayBy: string
  onClick?: (e: React.MouseEvent) => void
}): JSX.Element {
  return (
    <div
      onClick={onClick}
      className="text-md font-normal w-full"
    >
      <span className="font-bold">{item[displayBy]}</span>
      <small
        className="block text-neutral-600 italic"
        dangerouslySetInnerHTML={{
          __html: highlightMatch(index, search),
        }}
      />
    </div>
  )
}
