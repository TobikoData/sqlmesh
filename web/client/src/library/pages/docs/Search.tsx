import { type Model } from '@api/client'
import Input from '@components/input/Input'
import { isFalse, isArrayEmpty, isArrayNotEmpty } from '@utils/index'
import { useMemo } from 'react'
import { Link } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import { EnumSize } from '~/types/enum'

export default function Search({
  models,
  search,
  setSearch,
}: {
  models: Map<string, Model>
  search: string
  setSearch: (search: string) => void
}): JSX.Element {
  const indices = useMemo(() => createIndices(models), [models])
  const showSearchResults = search !== '' && search.length > 1
  const found = isFalse(showSearchResults)
    ? []
    : filterModelsBySearch(indices, search)

  return (
    <div className="p-2 relative">
      <Input
        className="w-full !m-0"
        size={EnumSize.md}
        value={search}
        placeholder="Search"
        onInput={e => {
          setSearch(e.target.value.trim())
        }}
        autoFocus
      />
      {showSearchResults && (
        <ul className="p-4 bg-theme-lighter absolute z-10 w-full top-16 left-0 right-0 rounded-lg max-h-[25vh] overflow-auto scrollbar scrollbar--vertical scrollbar--horizontal">
          {isArrayEmpty(found) && (
            <li
              key="not-found"
              className="p-2"
              onClick={() => {
                setSearch('')
              }}
            >
              No Results Found
            </li>
          )}
          {isArrayNotEmpty(found) &&
            found.map(([model, index]) => (
              <Link
                key={model.name}
                to={`${EnumRoutes.IdeDocsModels}?model=${model.name}`}
                className="text-md font-normal mb-1 w-full"
              >
                <li className="p-2 cursor-pointer hover:bg-secondary-10">
                  <span>{model.name}</span>
                  <small className="block text-neutral-600 p-2 italic">
                    {highlightMatch(index, search)}
                  </small>
                </li>
              </Link>
            ))}
        </ul>
      )}
    </div>
  )
}

function highlightMatch(source: string, match: string): JSX.Element {
  return (
    <>
      {source.split(match).reduce((acc: JSX.Element[], part, idx, arr) => {
        acc.push(<>{part}</>)

        if (idx > 0 || idx % 2 !== 0 || idx === arr.length) return acc

        acc.push(
          <span className="inline-block text-brand-500 bg-brand-10">
            {match}
          </span>,
        )

        return acc
      }, [])}
    </>
  )
}

function filterModelsBySearch(
  indices: Array<[Model, string]> = [],
  search: string,
): Array<[Model, string]> {
  return indices.reduce((acc: Array<[Model, string]>, [model, index]) => {
    const idx = index.indexOf(search.toLocaleLowerCase())

    if (idx > -1) {
      const SIZE = 40
      const min = Math.max(0, idx - SIZE)
      const max = Math.min(index.length - 1, idx + search.length + SIZE)

      acc.push([
        model,
        (min > 0 ? '... ' : '') +
          index.slice(min, max) +
          (max < index.length ? ' ...' : ''),
      ])
    }

    return acc
  }, [])
}

function createIndices(models: Map<string, Model>): Array<[Model, string]> {
  const indices: Array<[Model, string]> = []

  models.forEach((value, key) => {
    if (value.path === key) return

    const index = Object.entries(value).reduce((acc, [k, v]) => {
      if (k === 'sql') return acc

      if (k === 'details' && v != null) {
        return acc + ' ' + Object.values(v).join(' ')
      }

      if (k === 'columns' && v != null) {
        return (
          acc +
          ' ' +
          (v as Array<Record<string, unknown>>)
            .map(column => String(Object.values(column).join(' ')))
            .join(' ')
        )
      }

      return acc + ' ' + String(v ?? '')
    }, '')

    indices.push([value, index.toLocaleLowerCase() + ' ' + key])
  })

  return indices
}
