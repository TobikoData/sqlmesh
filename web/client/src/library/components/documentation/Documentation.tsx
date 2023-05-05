import { type Model } from '@api/client'
import CodeEditor, {
  useSQLMeshModelExtensions,
} from '@components/editor/EditorCode'
import { useStoreEditor } from '@context/editor'
import { useStoreLineage } from '@context/lineage'
import { Disclosure, Tab } from '@headlessui/react'
import { MinusCircleIcon, PlusCircleIcon } from '@heroicons/react/24/solid'
import { EnumFileExtensions } from '@models/file'
import { isFalse, isString, isTrue, toDateFormat } from '@utils/index'
import clsx from 'clsx'
import React, { useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { EnumRoutes } from '~/routes'

const Documantation = function Documantation({
  model,
  withModel = true,
  withDescription = true,
  withColumns = true,
  withCode = true,
  withQuery = true,
}: {
  model: Model
  withCode?: boolean
  withQuery?: boolean
  withModel?: boolean
  withDescription?: boolean
  withColumns?: boolean
}): JSX.Element {
  const navigate = useNavigate()
  const lineage = useStoreEditor(s => s.previewLineage)

  const setColumns = useStoreLineage(s => s.setColumns)
  const clearActiveEdges = useStoreLineage(s => s.clearActiveEdges)

  const modelExtensions = useSQLMeshModelExtensions(
    model.path,
    lineage,
    model => {
      navigate(`${EnumRoutes.IdeDocsModels}?model=${model.name}`, {
        state: { model },
      })
    },
  )

  useEffect(() => {
    setColumns(undefined)
    clearActiveEdges()
  }, [model])

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
            {Object.entries(model.details).map(([key, value]) => (
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
          <ul className="px-2 w-full">
            {model.columns.map(column => (
              <DetailsItem
                key={column.name}
                name={column.name}
                value={column.type}
                isHighlighted={true}
              >
                {column.description}
              </DetailsItem>
            ))}
          </ul>
        </Section>
      )}
      {(withCode || withQuery) && (
        <Section headline="SQL">
          <CodeEditor.RemoteFile path={model.path}>
            {({ file }) => (
              <>
                <Tab.Group defaultIndex={withQuery ? 1 : 0}>
                  <Tab.List className="w-full whitespace-nowrap px-2 pt-3 flex justify-center items-center">
                    <Tab
                      disabled={isFalse(withCode)}
                      className={({ selected }) =>
                        clsx(
                          'inline-block text-sm font-medium px-3 py-1 mr-2 last-child:mr-0 rounded-md relative',
                          selected
                            ? 'bg-secondary-500 text-secondary-100 cursor-default'
                            : 'bg-secondary-10 ',
                          isFalse(withCode)
                            ? 'opacity-50 cursor-not-allowed'
                            : 'cursor-pointer',
                        )
                      }
                    >
                      Source Code
                    </Tab>
                    <Tab
                      disabled={isFalse(withQuery)}
                      className={({ selected }) =>
                        clsx(
                          'inline-block text-sm font-medium px-3 py-1 mr-2 last-child:mr-0 rounded-md relative',
                          selected
                            ? 'bg-secondary-500 text-secondary-100 cursor-default'
                            : 'bg-secondary-10 ',
                          isFalse(withQuery)
                            ? 'opacity-50 cursor-not-allowed'
                            : 'cursor-pointer',
                        )
                      }
                    >
                      Compiled Query
                    </Tab>
                  </Tab.List>
                  <Tab.Panels className="h-full w-full overflow-hidden p-2 bg-neutral-10 mt-4 rounded-lg">
                    <Tab.Panel
                      className={clsx(
                        'flex flex-col w-full h-full pt-4 relative px-2 overflow-hidden',
                        'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
                      )}
                    >
                      <CodeEditor.SQLMeshDialect content={file.content}>
                        {({ extensions, content }) => (
                          <CodeEditor
                            extensions={extensions.concat(modelExtensions)}
                            content={content}
                          />
                        )}
                      </CodeEditor.SQLMeshDialect>
                    </Tab.Panel>
                    <Tab.Panel
                      className={clsx(
                        'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2',
                      )}
                    >
                      <CodeEditor.Default
                        type={EnumFileExtensions.SQL}
                        content={model.sql ?? ''}
                      >
                        {({ extensions, content }) => (
                          <CodeEditor
                            extensions={extensions.concat(modelExtensions)}
                            content={content}
                          />
                        )}
                      </CodeEditor.Default>
                    </Tab.Panel>
                  </Tab.Panels>
                </Tab.Group>
              </>
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
    <Container className="text-sm font-bold whitespace-nowrap w-full h-full">
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
        <div className="w-full h-full overflow-auto scrollbar scrollbar--vertical scrollbar--horizontal">
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
                'flex items-center justify-between rounded-lg text-left text-sm w-full bg-neutral-10 px-3 mb-2',
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
            <Disclosure.Panel className="px-4 pb-2 text-sm">
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

Documantation.NotFound = NotFound
Documantation.Container = Container

export default Documantation
