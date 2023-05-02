import { type Model } from '@api/client'
import { CodeEditorDocsReadOnly } from '@components/editor/EditorCode'
import { useStoreLineage } from '@context/lineage'
import { Tab } from '@headlessui/react'
import { isString, toDateFormat, isTrue } from '@utils/index'
import clsx from 'clsx'
import React, { useEffect } from 'react'

const Documantation = function Documantation({
  model,
  withCode = true,
  withQuery = true,
}: {
  model: Model
  withCode?: boolean
  withQuery?: boolean
}): JSX.Element {
  const setColumns = useStoreLineage(s => s.setColumns)
  const clearActiveEdges = useStoreLineage(s => s.clearActiveEdges)

  useEffect(() => {
    setColumns(undefined)
    clearActiveEdges()
  }, [model])

  return (
    <Container>
      <Section headline="Model">
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
              name={key}
              value={value}
            />
          ))}
        </ul>
      </Section>
      <Section headline="Description">
        {model.description == null ? 'No description' : model.description}
      </Section>
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
      {(withCode || withQuery) && (
        <Section headline="SQL">
          <Tab.Group>
            <Tab.List className="w-full whitespace-nowrap px-2 pt-3 flex justigy-between items-center">
              {withCode && (
                <Tab
                  className={({ selected }) =>
                    clsx(
                      'inline-block text-sm font-medium px-3 py-1 mr-2 last-child:mr-0 rounded-md relative',
                      selected
                        ? 'bg-neutral-500 text-neutral-100 cursor-default'
                        : 'bg-neutral-10 cursor-pointer',
                    )
                  }
                >
                  Code
                </Tab>
              )}
              {withQuery && (
                <Tab
                  className={({ selected }) =>
                    clsx(
                      'inline-block text-sm font-medium px-3 py-1 mr-2 last-child:mr-0 rounded-md relative',
                      selected
                        ? 'bg-neutral-500 text-neutral-100 cursor-default'
                        : 'bg-neutral-10 cursor-pointer',
                    )
                  }
                >
                  Query
                </Tab>
              )}
            </Tab.List>
            <Tab.Panels className="h-full w-full overflow-hidden">
              {withCode && (
                <Tab.Panel
                  className={clsx(
                    'w-full h-full pt-4 relative px-2',
                    'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
                  )}
                >
                  <CodeEditorDocsReadOnly model={model} />
                </Tab.Panel>
              )}
              {withQuery && (
                <Tab.Panel
                  className={clsx(
                    'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2',
                  )}
                >
                  <pre className="w-full h-full p-4 bg-primary-10 rounded-lg overflow-auto scrollbar scrollbar--horizontal scrollbar--vertical text-xs">
                    <code>{model.sql}</code>
                  </pre>
                </Tab.Panel>
              )}
            </Tab.Panels>
          </Tab.Group>
        </Section>
      )}
    </Container>
  )
}

function Headline({ headline }: { headline: string }): JSX.Element {
  return (
    <div
      className="text-lg font-bold whitespace-nowrap"
      id={headline}
    >
      <h3 className="mt-3 mb-1">{headline}</h3>
    </div>
  )
}

function NotFound(): JSX.Element {
  return (
    <Container className="text-sm font-bold whitespace-nowrap">
      Documentation Not Found
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
      <div className="bg-neutral-10 w-full h-full py-8 px-8 rounded-xl">
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
}: {
  headline: string
  className?: string
  children: React.ReactNode
}): JSX.Element {
  return (
    <div className={clsx('mb-5', className)}>
      <Headline headline={headline} />
      <div className="px-2">{children}</div>
    </div>
  )
}

function DetailsItem({
  className,
  name,
  value,
  isHighlighted = false,
  children,
}: {
  name: string
  value: string | boolean | number
  className?: string
  isHighlighted?: boolean
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
          className={clsx('mr-2 capitalize', isHighlighted && 'text-brand-500')}
        >
          {name.replaceAll('_', ' ')}
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
