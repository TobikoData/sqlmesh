import ContainerPage from '@components/container/ContainerPage'
import { useStoreContext } from '@context/context'
import clsx from 'clsx'
import { Link, Outlet, useLocation } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import Content from './Content'
import Input from '@components/input/Input'
import { useEffect, useMemo, useState } from 'react'
import { EnumSize } from '~/types/enum'
import { type Model } from '@api/client'
import { isArrayEmpty, isArrayNotEmpty, isFalse } from '@utils/index'

const Docs = function Docs(): JSX.Element {
  const models = useStoreContext(s => s.models)
  const location = useLocation()
  const searchParams = new URLSearchParams(location.search)
  const modelName = searchParams.get('model')
  const indecies = useMemo(() => createIndecies(models), [models])

  const [search, setSearch] = useState('')
  const [filter, setFilter] = useState('')

  const showSearchResults = search !== '' && search.length > 1
  const found = isFalse(showSearchResults)
    ? []
    : filterModelsBySearch(indecies, search)

  useEffect(() => {
    setFilter('')
    setSearch('')
  }, [location.search])

  const isActivePageDocs = location.pathname === EnumRoutes.IdeDocs

  return (
    <ContainerPage>
      <div className="p-4 flex flex-col w-full h-full overflow-hidden">
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
                  No results found
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
                        {index
                          .split(search)
                          .reduce((acc: JSX.Element[], part, idx, arr) => {
                            acc.push(<>{part}</>)

                            if (idx > 0 || idx % 2 !== 0 || idx === arr.length)
                              return acc

                            acc.push(
                              <span className="inline-block text-brand-500">
                                {search}
                              </span>,
                            )

                            return acc
                          }, [])}
                      </small>
                    </li>
                  </Link>
                ))}
            </ul>
          )}
        </div>
        <div className="p-4 flex w-full h-full overflow-hidden">
          <div className="p-4 min-w-[24rem]">
            <div className="px-2 w-full">
              <Input
                className="w-full !m-0"
                size={EnumSize.sm}
                value={filter}
                placeholder="Filter models"
                onInput={e => {
                  setFilter(e.target.value)
                }}
              />
            </div>
            <ul className="m-4">
              {Array.from(new Set(models.values()))
                .filter(model =>
                  filter === '' ? true : model.name.includes(filter),
                )
                .map(model => (
                  <li
                    key={model.name}
                    className={clsx('text-md font-normal mb-1 w-full')}
                  >
                    <Link
                      to={`${EnumRoutes.IdeDocsModels}?model=${model.name}`}
                    >
                      <div
                        className={clsx(
                          'py-1 px-4 rounded-md w-full',
                          modelName === model.name
                            ? 'text-primary-500 bg-primary-10'
                            : 'text-neutral-500 dark:text-neutral-100',
                        )}
                      >
                        {model.name}
                      </div>
                    </Link>
                  </li>
                ))}
            </ul>
          </div>
          <div className="py-4 w-full">
            {isActivePageDocs ? (
              <div className="p-4">
                <h1 className="text-2xl font-bold">
                  Welcome to the documentation
                </h1>
                <p className="text-lg mt-4">
                  Here you can find all the information about the models and
                  their fields.
                </p>
              </div>
            ) : (
              <Outlet />
            )}
          </div>
        </div>
      </div>
    </ContainerPage>
  )
}

Docs.Content = Content

export default Docs

function filterModelsBySearch(
  indecies: Array<[Model, string]> = [],
  search: string,
): Array<[Model, string]> {
  return indecies.reduce((acc: Array<[Model, string]>, [model, index]) => {
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

function createIndecies(models: Map<string, Model>): Array<[Model, string]> {
  const indecies: Array<[Model, string]> = []

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

    indecies.push([value, index.toLocaleLowerCase() + ' ' + key])
  })

  return indecies
}
