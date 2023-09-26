import React from 'react'
import { Disclosure, Tab } from '@headlessui/react'
import { MinusCircleIcon, PlusCircleIcon } from '@heroicons/react/24/solid'
import { EnumFileExtensions } from '@models/file'
import {
  isArrayNotEmpty,
  isFalse,
  isNil,
  isString,
  isTrue,
  toDateFormat,
} from '@utils/index'
import clsx from 'clsx'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { ModelColumns } from '@components/graph/Graph'
import { useLineageFlow } from '@components/graph/context'
import TabList from '@components/tab/Tab'
import { useSQLMeshModelExtensions } from '@components/editor/hooks'
import {
  CodeEditorDefault,
  CodeEditorRemoteFile,
} from '@components/editor/EditorCode'

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
  const { handleClickModel } = useLineageFlow()

  const modelExtensions = useSQLMeshModelExtensions(model.path, model => {
    handleClickModel?.(model.name)
  })

  return (
    <Container>
      {withModel && (
        <Section
          headline="Model"
          defaultOpen={true}
        >
          <ul className="px-2 w-full">
            <DetailsItem
              name="Path"
              value={model.path.split('/').slice(0, -1).join('/')}
            />
            <DetailsItem
              name="Name"
              value={model.name}
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
          {isNil(model.description) ? 'No description' : model.description}
        </Section>
      )}
      {withColumns && (
        <Section headline="Columns">
          <ModelColumns
            className="max-h-[15rem]"
            nodeId={model.name}
            columns={model.columns}
            disabled={model?.type === 'python'}
            withHandles={false}
            withSource={false}
            withDescription={true}
            limit={10}
          />
        </Section>
      )}
      {(withCode || withQuery) && (
        <Section headline="SQL">
          <CodeEditorRemoteFile path={model.path}>
            {({ file }) => (
              <Tab.Group defaultIndex={withQuery ? 1 : 0}>
                <TabList
                  list={
                    [
                      withCode && 'Source Code',
                      withQuery && 'Compiled Query',
                    ].filter(Boolean) as string[]
                  }
                  className="!justify-center"
                />

                <Tab.Panels className="h-full w-full overflow-hidden bg-neutral-10 mt-4 rounded-lg">
                  {withCode && (
                    <Tab.Panel
                      unmount={false}
                      className={clsx(
                        'flex flex-col w-full h-full relative px-2 overflow-hidden p-2',
                        'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
                      )}
                    >
                      <CodeEditorDefault
                        content={file.content}
                        type={file.extension}
                        extensions={modelExtensions}
                        className="text-xs"
                      />
                    </Tab.Panel>
                  )}
                  {withQuery && (
                    <Tab.Panel
                      unmount={false}
                      className="w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2"
                    >
                      <CodeEditorDefault
                        type={EnumFileExtensions.SQL}
                        content={model.sql ?? ''}
                        extensions={modelExtensions}
                        className="text-xs"
                      />
                    </Tab.Panel>
                  )}
                </Tab.Panels>
              </Tab.Group>
            )}
          </CodeEditorRemoteFile>
        </Section>
      )}
    </Container>
  )
}

function Headline({ headline }: { headline: string }): JSX.Element {
  return (
    <div
      className="text-lg font-bold whitespace-nowrap w-full"
      id={headline}
    >
      <h3 className="py-2">{headline}</h3>
    </div>
  )
}

function NotFound(): JSX.Element {
  return (
    <Container className="font-bold whitespace-nowrap w-full h-full">
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
      <div className="w-full h-full py-4 rounded-xl">
        <div className="w-full h-full overflow-auto hover:scrollbar scrollbar--vertical scrollbar--horizontal">
          {children}
        </div>
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
    <div className="px-4">
      <Disclosure defaultOpen={defaultOpen}>
        {({ open }) => (
          <>
            <Disclosure.Button
              className={clsx(
                'flex items-center justify-between rounded-lg text-left w-full bg-neutral-10 px-3 mb-2',
                className,
              )}
            >
              <Headline headline={headline} />
              <div>
                {open ? (
                  <MinusCircleIcon className="h-6 w-6 text-neutral-50" />
                ) : (
                  <PlusCircleIcon className="h-6 w-6 text-neutral-50" />
                )}
              </div>
            </Disclosure.Button>
            <Disclosure.Panel className="pb-2 overflow-hidden">
              <div className="px-4 mb-2 text-xs">{children}</div>
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
}: {
  name: string
  value: Primitive | TValue[]
  className?: string
  isHighlighted?: boolean
  isCapitalize?: boolean
  children?: React.ReactNode
}): JSX.Element {
  return (
    <li
      className={clsx('w-full border-b border-primary-10 py-1 mb-1', className)}
    >
      {isArrayNotEmpty<TValue>(value) ? (
        <>
          <strong className="mr-2 text-xs capitalize">{name}</strong>
          {value.map((item, idx) => (
            <span
              className="w-full items-center flex ml-2 mb-1"
              key={idx}
            >
              <ul className="w-full flex ml-3 whitespace-nowrap">
                {Object.entries<Primitive>(
                  item as Record<string, Primitive>,
                ).map(([key, val]) => (
                  <li
                    key={key}
                    className="flex"
                  >
                    <div className="flex text-xs">
                      <strong className="mr-2">{key}:</strong>
                      <p className="text-xs rounded text-neutral-500 dark:text-neutral-400">
                        {getValue(val)}
                      </p>
                      <span className="px-2 text-neutral-20">|</span>
                    </div>
                  </li>
                ))}
              </ul>
            </span>
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
