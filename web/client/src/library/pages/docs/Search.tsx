import Input from '@components/input/Input'
import { ModelSQLMeshModel } from '@models/sqlmesh-model'
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
  models: ModelSQLMeshModel[]
  search: string
  setSearch: (search: string) => void
}): JSX.Element {
  const indices: Array<[ModelSQLMeshModel, string]> = useMemo(
    () => models.map(model => [model, model.index]),
    [models],
  )
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
        <ul className="p-4 bg-theme dark:bg-theme-lighter absolute z-10 w-full top-16 left-0 right-0 rounded-lg max-h-[25vh] overflow-auto scrollbar scrollbar--vertical scrollbar--horizontal shadow-2xl">
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
              <li
                key={model.name}
                className="p-2 cursor-pointer hover:bg-primary-10 rounded-lg"
              >
                <Link
                  to={`${
                    EnumRoutes.IdeDocsModels
                  }/${ModelSQLMeshModel.encodeName(model.name)}`}
                  className="text-md font-normal mb-1 w-full"
                >
                  <span className="font-bold">{model.name}</span>
                  <small
                    className="block text-neutral-600 p-2 italic"
                    dangerouslySetInnerHTML={{
                      __html: highlightMatch(index, search),
                    }}
                  ></small>
                </Link>
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

function filterModelsBySearch(
  indices: Array<[ModelSQLMeshModel, string]> = [],
  search: string,
): Array<[ModelSQLMeshModel, string]> {
  const DISPLAYED_MATCH_LENGTH = 40

  return indices.reduce(
    (acc: Array<[ModelSQLMeshModel, string]>, [model, index]) => {
      const idx = index.indexOf(search.toLocaleLowerCase())

      if (idx > -1) {
        const min = Math.max(0, idx - DISPLAYED_MATCH_LENGTH)
        const max = Math.min(
          index.length - 1,
          idx + search.length + DISPLAYED_MATCH_LENGTH,
        )

        acc.push([
          model,
          (min > 0 ? '... ' : '') +
            index.slice(min, max) +
            (max < index.length ? ' ...' : ''),
        ])
      }

      return acc
    },
    [],
  )
}
