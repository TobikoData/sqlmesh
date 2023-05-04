import { type Model } from '@api/client'
import Input from '@components/input/Input'
import { isArrayEmpty, isArrayNotEmpty } from '@utils/index'
import clsx from 'clsx'
import { Link } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import { EnumSize } from '~/types/enum'

export default function SourceList({
  models,
  filter,
  activeModel,
  setFilter,
}: {
  activeModel?: string
  models: Map<string, Model>
  filter: string
  setFilter: (filter: string) => void
}): JSX.Element {
  const modelsFiltered = Array.from(new Set(models.values())).filter(model =>
    filter === '' ? true : model.name.includes(filter),
  )
  return (
    <div>
      <div className="px-2 w-full flex justify-between">
        <Input
          className="w-full !m-0"
          size={EnumSize.sm}
          value={filter}
          placeholder="Filter models"
          onInput={e => {
            setFilter(e.target.value)
          }}
        />
        <div className="ml-3 px-3 bg-primary-10 text-primary-500 rounded-full text-xs flex items-center">
          {modelsFiltered.length}
        </div>
      </div>
      <ul className="p-2 overflow-auto scrollbar scrollbar--horizontal scrollbar--vertical">
        {isArrayEmpty(modelsFiltered) && (
          <li
            key="not-found"
            className="p-2"
            onClick={() => {
              setFilter('')
            }}
          >
            No results found
          </li>
        )}
        {isArrayNotEmpty(modelsFiltered) &&
          modelsFiltered.map(model => (
            <li
              key={model.name}
              className={clsx('text-sm font-normal w-full')}
            >
              <Link
                to={`${EnumRoutes.IdeDocsModels}?model=${model.name}`}
                state={{ model }}
              >
                <div
                  className={clsx(
                    'py-1 px-4 rounded-md w-full hover:bg-primary-10',
                    activeModel === model.name
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
  )
}
