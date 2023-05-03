import { type Model } from '@api/client'
import { CodeEditorDocsReadOnly } from '@components/editor/EditorCode'
import SplitPane from '@components/splitPane/SplitPane'
import { useStoreLineage } from '@context/lineage'
import { Disclosure } from '@headlessui/react'
import { MinusCircleIcon, PlusCircleIcon } from '@heroicons/react/24/solid'
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
              name={key}
              value={value}
            />
          ))}
        </ul>
      </Section>
      <Section
        headline="Description"
        defaultOpen={true}
      >
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
          <SplitPane
            className="flex w-full h-full overflow-hidden"
            sizes={[50, 50]}
            minSize={0}
            snapOffset={0}
          >
            {withCode && (
              <CodeEditorDocsReadOnly
                model={model}
                className="w-full h-full relative overflow-auto scrollbar scrollbar--horizontal scrollbar--vertical text-xs pr-2"
              />
            )}
            {withQuery && (
              <div className=" pl-2">
                <pre className="flex w-full h-full bg-primary-10 overflow-auto scrollbar scrollbar--horizontal scrollbar--vertical text-xs p-4">
                  <code>{model.sql}</code>
                </pre>
              </div>
            )}
          </SplitPane>
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
