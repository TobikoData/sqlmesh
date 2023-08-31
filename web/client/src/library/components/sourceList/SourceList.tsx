import Input from '@components/input/Input'
import { ModelSQLMeshModel } from '@models/sqlmesh-model'
import { isArrayEmpty, isArrayNotEmpty, isNil, isNotNil } from '@utils/index'
import clsx from 'clsx'
import { useState } from 'react'
import { NavLink } from 'react-router-dom'
import { EnumSize, EnumVariant, type Variant } from '~/types/enum'

export default function SourceList<
  TItem = Record<string, string>,
  TType = Record<string, string>,
>({
  items = [],
  types,
  by = 'id',
  byName,
  byDescription,
  to,
  variant = EnumVariant.Neutral,
  className,
}: {
  by: string
  to: string
  items?: TItem[]
  types?: TType
  byName?: string
  byDescription?: string
  variant?: Variant
  className?: string
}): JSX.Element {
  const [filter, setFilter] = useState('')

  const filtered =
    filter === ''
      ? items
      : items.filter(item => {
          const id = (item as Record<string, string>)[by] ?? ''
          const description = isNil(byDescription)
            ? ''
            : (item as Record<string, string>)?.[byDescription] ?? ''
          const name = isNil(byName)
            ? ''
            : (item as Record<string, string>)?.[byName] ?? ''
          const type = (types as Record<string, string>)?.[id] ?? ''

          return (
            name.includes(filter) ||
            description.includes(filter) ||
            type.includes(filter)
          )
        })

  return (
    <div className={clsx('flex flex-col w-full h-full py-2', className)}>
      <ul className="px-2 h-full overflow-auto hover:scrollbar scrollbar--horizontal scrollbar--vertical">
        {isArrayEmpty(filtered) && (
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
        {isArrayNotEmpty(filtered) &&
          filtered.map(item => {
            const id = (item as Record<string, string>)[by]!
            const name = isNil(byName)
              ? ''
              : (item as Record<string, string>)?.[byName] ?? ''
            const description = isNil(byDescription)
              ? undefined
              : (item as Record<string, string>)?.[byDescription] ?? undefined
            return (
              <li
                key={id}
                className={clsx('text-sm font-normal')}
              >
                <NavLink
                  to={`${to}/${ModelSQLMeshModel.encodeName(id)}`}
                  className={({ isActive }) =>
                    clsx(
                      'block overflow-hidden px-2 py-1 rounded-md w-full text-sm font-bold',
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
                    {isNotNil((types as Record<string, string>)?.[id]) && (
                      <span className="flex items-center ml-2 px-2 h-4 rounded-md text-[0.5rem] bg-neutral-10 dark:text-neutral-200 text-neutral-700 font-bold">
                        {(types as Record<string, string>)[id]}
                      </span>
                    )}
                  </div>
                  {isNotNil(description) && (
                    <p className="text-xs overflow-hidden whitespace-nowrap overflow-ellipsis">
                      {description}
                    </p>
                  )}
                </NavLink>
              </li>
            )
          })}
      </ul>
      <div className="px-2 w-full flex justify-between">
        <Input
          className="w-full !m-0"
          size={EnumSize.sm}
        >
          {({ className }) => (
            <Input.Textfield
              className={clsx(className, 'w-full')}
              value={filter}
              placeholder="Filter items"
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
