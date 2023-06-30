import React, { useMemo, useState } from 'react'
import Input from '@components/input/Input'
import { isFalse, isArrayEmpty, isArrayNotEmpty } from '@utils/index'
import { EnumSize, type Size } from '~/types/enum'

const EMPTY_STRING = ''

export default function SearchList<
  T extends Record<string, any> = Record<string, any>,
>({
  list,
  size = EnumSize.sm,
  searchBy,
  displayBy,
  onSelect,
  autoFocus = false,
}: {
  list: T[]
  searchBy: string
  displayBy: string
  onSelect: (item: T) => void
  autoFocus?: boolean
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
    : filterListBySearch<T>(indices, search)

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
                onClick={(e: React.MouseEvent) => {
                  e.stopPropagation()

                  onSelect(item)
                  setSearch(EMPTY_STRING)
                }}
              >
                <div className="text-md font-normal w-full">
                  <span className="font-bold">{item[displayBy]}</span>
                  <small
                    className="block text-neutral-600 italic"
                    dangerouslySetInnerHTML={{
                      __html: highlightMatch(index, search),
                    }}
                  ></small>
                </div>
              </li>
            ))}
        </ul>
      )}
    </div>
  )
}

function highlightMatch(source: string, match: string): string {
  return source.replaceAll(
    match,
    `<b class="inline-block text-brand-500 bg-brand-10">${match}</b>`,
  )
}

function filterListBySearch<
  T extends Record<string, any> = Record<string, any>,
>(indices: Array<[T, string]> = [], search: string): Array<[T, string]> {
  const DISPLAYED_MATCH_LENGTH = 40

  return indices.reduce((acc: Array<[T, string]>, [model, index]) => {
    const idx = index.indexOf(search.toLocaleLowerCase())

    if (idx > -1) {
      const min = Math.max(0, idx - DISPLAYED_MATCH_LENGTH)
      const max = Math.min(
        index.length - 1,
        idx + search.length + DISPLAYED_MATCH_LENGTH,
      )

      acc.push([
        model,
        (min > 0 ? '... ' : EMPTY_STRING) +
          index.slice(min, max) +
          (max < index.length ? ' ...' : EMPTY_STRING),
      ])
    }

    return acc
  }, [])
}
