import Input from '@components/input/Input'
import { ModelSQLMeshModel } from '@models/sqlmesh-model'
import { isArrayEmpty, isArrayNotEmpty } from '@utils/index'
import clsx from 'clsx'
import { NavLink } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import { EnumSize } from '~/types/enum'

export default function SourceList({
  models,
  filter,
  setFilter,
}: {
  models: Map<string, ModelSQLMeshModel>
  filter: string
  setFilter: (filter: string) => void
}): JSX.Element {
  const modelsFiltered = Array.from(new Set(models.values())).filter(model =>
    filter === '' ? true : model.name.includes(filter),
  )
  return (
    <div className="flex flex-col w-full h-full">
      <div className="px-2 w-full flex justify-between">
        <Input
          className="w-full !m-0"
          size={EnumSize.sm}
        >
          {({ className }) => (
            <Input.Textfield
              className={clsx(className, 'w-full')}
              value={filter}
              placeholder="Filter models"
              onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                setFilter(e.target.value)
              }}
            />
          )}
        </Input>
        <div className="ml-3 px-3 bg-primary-10 text-primary-500 rounded-full text-xs flex items-center">
          {modelsFiltered.length}
        </div>
      </div>
      <ul className="p-2 overflow-auto hover:scrollbar scrollbar--horizontal scrollbar--vertical">
        {isArrayEmpty(modelsFiltered) && (
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
        {isArrayNotEmpty(modelsFiltered) &&
          modelsFiltered.map(model => (
            <li
              key={model.name}
              className={clsx('text-sm font-normal')}
            >
              <NavLink
                to={`${EnumRoutes.IdeDocsModels}/${ModelSQLMeshModel.encodeName(
                  model.name,
                )}`}
                className={({ isActive }) =>
                  clsx(
                    'block px-2 overflow-hidden whitespace-nowrap overflow-ellipsis py-1 rounded-md w-full hover:bg-primary-10',
                    isActive
                      ? 'text-primary-500 bg-primary-10'
                      : 'text-neutral-500 dark:text-neutral-100',
                  )
                }
              >
                {model.name}
                <span
                  title={
                    model.type === 'python'
                      ? 'Column lineage disabled for Python models'
                      : 'SQL Model'
                  }
                  className="inline-block ml-2 bg-primary-10 px-2 rounded-md text-[0.65rem]"
                >
                  {model.type === 'python' && 'Python'}
                  {model.type === 'sql' && 'SQL'}
                  {model.type === 'seed' && 'Seed'}
                </span>
              </NavLink>
            </li>
          ))}
      </ul>
    </div>
  )
}
