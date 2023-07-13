import React from 'react'
import { Disclosure, Tab } from '@headlessui/react'
import { MinusCircleIcon, PlusCircleIcon } from '@heroicons/react/24/solid'
import { EnumFileExtensions } from '@models/file'
import { isString, isTrue, toDateFormat } from '@utils/index'
import clsx from 'clsx'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { ModelColumns } from '@components/graph/Graph'
import { useLineageFlow } from '@components/graph/context'
import TabList from '@components/tab/Tab'
import CodeEditor from '@components/editor/EditorCode'
import { useSQLMeshModelExtensions } from '@components/editor/hooks'

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
          {model.description == null ? 'No description' : model.description}
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
            limit={10}
          />
        </Section>
      )}
      {(withCode || withQuery) && (
        <Section headline="SQL">
          <CodeEditor.RemoteFile path={model.path}>
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
                      <CodeEditor.Default
                        content={file.content}
                        type={file.extension}
                      >
                        {({ extensions, content }) => (
                          <CodeEditor
                            extensions={extensions.concat(modelExtensions)}
                            content={content}
                            className="text-xs"
                          />
                        )}
                      </CodeEditor.Default>
                    </Tab.Panel>
                  )}
                  {withQuery && (
                    <Tab.Panel
                      unmount={false}
                      className="w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2"
                    >
                      <CodeEditor.Default
                        type={EnumFileExtensions.SQL}
                        content={model.sql ?? ''}
                      >
                        {({ extensions, content }) => (
                          <CodeEditor
                            extensions={extensions.concat(modelExtensions)}
                            content={content}
                            className="text-xs"
                          />
                        )}
                      </CodeEditor.Default>
                    </Tab.Panel>
                  )}
                </Tab.Panels>
              </Tab.Group>
            )}
          </CodeEditor.RemoteFile>
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
              <div className="px-2">{children}</div>
            </Disclosure.Panel>
          </>
        )}
      </Disclosure>
    </div>
  )
}

function DetailsItem({
  className,
  name,
  value,
  isHighlighted = false,
  isCapitalize = false,
  children,
}: {
  name: string
  value: string | boolean | number
  className?: string
  isHighlighted?: boolean
  isCapitalize?: boolean
  children?: React.ReactNode
}): JSX.Element {
  const maybeDate = new Date(value as string)
  const isDate = isString(value) && !isNaN(maybeDate.getTime())
  const isBoolean = typeof value === 'boolean'
  return (
    <li
      className={clsx('w-full border-b border-primary-10 py-1 mb-1', className)}
    >
      <div className="flex justify-between text-xs">
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
          {isBoolean
            ? isTrue(value)
              ? 'True'
              : 'False'
            : isDate
            ? toDateFormat(maybeDate)
            : value}
        </p>
      </div>
      <p className="text-xs ">{children}</p>
    </li>
  )
}

Documentation.NotFound = NotFound
Documentation.Container = Container

export default Documentation
