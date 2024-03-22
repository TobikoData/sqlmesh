import React from 'react'
import { Disclosure } from '@headlessui/react'
import { MinusCircleIcon, PlusCircleIcon } from '@heroicons/react/24/solid'
import {
  isArrayNotEmpty,
  isFalse,
  isString,
  isTrue,
  toDateFormat,
  truncate,
} from '@utils/index'
import clsx from 'clsx'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import ModelColumns from '@components/graph/ModelColumns'

const Documentation = function Documentation({
  model,
  withModel = true,
  withDescription = true,
  withColumns = true,
  withCode = true,
  withQuery = true,
}: {
  model: ModelSQLMeshModel
  withCode?: boolean
  withQuery?: boolean
  withModel?: boolean
  withDescription?: boolean
  withColumns?: boolean
}): JSX.Element {
  return (
    <Container className="pt-2">
      {withModel && (
        <Section
          headline="Model"
          defaultOpen={true}
        >
          <ul className="w-full">
            <DetailsItem
              name="Path"
              value={model.path}
            />
            <DetailsItem
              name="Name"
              title={model.displayName}
              value={truncate(model.displayName, 50, 25)}
            />
            <DetailsItem
              name="Dialect"
              value={model.dialect}
            />
            <DetailsItem
              name="Type"
              value={model.type}
            />
            {Object.entries(model.details ?? {}).map(([key, value]) => (
              <DetailsItem
                key={key}
                name={key.replaceAll('_', ' ')}
                value={value}
                isCapitalize={true}
              />
            ))}
          </ul>
        </Section>
      )}
      {withDescription && (
        <Section
          headline="Description"
          defaultOpen={true}
        >
          {model.description ?? 'No description'}
        </Section>
      )}
      {withColumns && (
        <Section
          headline="Columns"
          defaultOpen={true}
        >
          <ModelColumns
            nodeId={model.fqn}
            columns={model.columns}
            disabled={isFalse(model.isModelSQL)}
            withHandles={false}
            withSource={false}
            withDescription={true}
            limit={10}
          />
        </Section>
      )}
    </Container>
  )
}

function Headline({ headline }: { headline: string }): JSX.Element {
  return (
    <div
      className="text-md font-bold whitespace-nowrap w-full"
      id={headline}
    >
      <h3 className="py-2">{headline}</h3>
    </div>
  )
}

function NotFound(): JSX.Element {
  return (
    <Container className="font-bold whitespace-nowrap">
      <div className="flex items-center justify-center w-full h-full">
        Documentation Not Found
      </div>
    </Container>
  )
}

function Container({
  children,
  className,
}: {
  className?: string
  children: React.ReactNode
}): JSX.Element {
  return (
    <div className={clsx('w-full h-full rounded-xl', className)}>
      <div className="w-full h-full overflow-auto scrollbar scrollbar--vertical scrollbar--horizontal">
        {children}
      </div>
    </div>
  )
}

function Section({
  children,
  className,
  headline,
  defaultOpen = false,
}: {
  headline: string
  children: React.ReactNode
  className?: string
  defaultOpen?: boolean
}): JSX.Element {
  return (
    <div className="px-1 text-neutral-500 dark:text-neutral-400">
      <Disclosure defaultOpen={defaultOpen}>
        {({ open }) => (
          <>
            <Disclosure.Button
              className={clsx(
                'flex items-center justify-between rounded-lg text-left w-full hover:bg-neutral-5 px-3 mb-1 overflow-hidden',
                className,
              )}
            >
              <Headline headline={headline} />
              <div>
                {open ? (
                  <MinusCircleIcon className="w-4 text-neutral-50" />
                ) : (
                  <PlusCircleIcon className="w-4 text-neutral-50" />
                )}
              </div>
            </Disclosure.Button>
            <Disclosure.Panel className="pb-2 px-4 text-xs overflow-hidden">
              {children}
            </Disclosure.Panel>
          </>
        )}
      </Disclosure>
    </div>
  )
}

function DetailsItem<TValue = Record<string, Primitive>>({
  className,
  name,
  value,
  isHighlighted = false,
  isCapitalize = false,
  children,
  title,
}: {
  name: string
  title?: string
  value: Primitive | TValue[]
  className?: string
  isHighlighted?: boolean
  isCapitalize?: boolean
  children?: React.ReactNode
}): JSX.Element {
  return (
    <li
      className={clsx(
        'w-full border-b last:border-b-0 border-neutral-10 py-1 mb-1 text-neutral-500 dark:text-neutral-400',
        className,
      )}
    >
      {isArrayNotEmpty<TValue>(value) ? (
        <>
          <strong
            title={title}
            className="mr-2 text-xs capitalize"
          >
            {name}
          </strong>
          {value.map((item, idx) => (
            <ul
              key={idx}
              className="w-full flex flex-col whitespace-nowrap p-1 m-1 rounded-md overflow-hidden"
            >
              {Object.entries<Primitive>(item as Record<string, Primitive>).map(
                ([key, val]) => (
                  <li
                    key={key}
                    className="flex items-center justify-between text-xs border-b border-neutral-10 last:border-b-0 py-1"
                  >
                    <strong className="mr-2">{key}:</strong>
                    <p className="text-xs">{getValue(val)}</p>
                  </li>
                ),
              )}
            </ul>
          ))}
        </>
      ) : (
        <div className="flex justify-between text-xs whitespace-nowrap">
          <strong
            className={clsx(
              'mr-2',
              isCapitalize && 'capitalize',
              isHighlighted && 'text-brand-500',
            )}
          >
            {name}
          </strong>
          <p className="text-xs rounded text-neutral-500 dark:text-neutral-400">
            {getValue(value)}
          </p>
        </div>
      )}
      <p className="text-xs ">{children}</p>
    </li>
  )
}

Documentation.NotFound = NotFound
Documentation.Container = Container

export default Documentation

function getValue(value: Primitive): string {
  const maybeDate = new Date(value as string)
  const isDate = isString(value) && !isNaN(maybeDate.getTime())
  const isBoolean = typeof value === 'boolean'

  if (isBoolean && isTrue(value)) return 'True'
  if (isBoolean && isFalse(value)) return 'False'
  if (isDate) return toDateFormat(maybeDate)

  return String(value)
}
