import { type Model } from '@api/client'
import clsx from 'clsx'
import React from 'react'

const Documantation = function Documantation({
  model,
}: {
  model: Model
}): JSX.Element {
  return (
    <Container>
      <div className="mb-5">
        <Headline headline="Model" />
        <div className="px-2">
          <ul className="px-2 w-full">
            <li className=" w-full mb-2 pb-1 border-b border-primary-30">
              <div className="flex  justify-between mb-2">
                <h3 className="mr-2">Name</h3>
                <p className="mt-1 px-2 py-1 bg-secondary-10 text-secondary-500 dark:text-primary-500 dark:bg-primary-10 text-xs rounded">
                  {model.name}
                </p>
              </div>
              {/* <div className='p-4 bg-neutral-10 font-normal rounded-md mb-2'>
                <p className='text-xs mb-2'>
                  name specifies the name of the model. This name represents the production view name that the model outputs, so it generally takes the form of schema.view_name. The name of a model must be unique in a SQLMesh project.
                </p>
                <p className='text-xs mb-2'>
                  When models are used in non-production environments, SQLMesh automatically prefixes the names. For example, consider a model named sushi.customers. In production its view is named sushi.customers, and in dev its view is named dev__sushi.customers.
                </p>
                <p className='text-xs mb-2'>
                  Name is required and must be unique.
                </p>
              </div> */}
            </li>
            <li className=" w-full  mb-2 pb-1 border-b border-primary-30">
              <div className="flex justify-between mb-2">
                <div className="mr-2">Kind</div>
                <p className="font-normal text-sm">incremental_by_time_range</p>
              </div>
              {/* <div className='p-4 bg-neutral-10 opacity-80 font-normal rounded-md mb-2'>
                <p className='text-xs mb-2'>
                  Kind specifies what kind a model is. A model&lsquo;s kind determines how it is computed and stored. The default kind is VIEW, which means a view is created and your query is run each time that view is accessed.
                </p>
              </div> */}
            </li>
          </ul>
        </div>
      </div>
      <div className="mb-5">
        <Headline headline="Columns" />
        <ul className="px-2 w-full">
          {model.columns.map(column => (
            <li
              key={column.name}
              className="flex w-full justify-between mb-2 pb-1 border-b border-primary-30"
            >
              <div className={clsx('mr-3')}>
                <span className="inline font-bold text-brand-400 text-sm">
                  {column.name}
                </span>
                <span className="block font-normal text-xs">
                  {column.description}
                </span>
              </div>
              <div className="text-neutral-300 dark:text-neutral-400 font-normal">
                {column.type}
              </div>
            </li>
          ))}
        </ul>
      </div>
    </Container>
  )
}

function Headline({ headline }: { headline: string }): JSX.Element {
  return (
    <div className="text-lg font-bold whitespace-nowrap">
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
    <div className={clsx('w-full h-full p-2 rounded-xl', className)}>
      <div className="bg-primary-10 w-full h-full p-4 rounded-xl">
        <div className="w-full h-full overflow-auto scrollbar scrollbar--vertical scrollbar--horizontal">
          {children}
        </div>
      </div>
    </div>
  )
}

Documantation.NotFound = NotFound
Documantation.Container = Container

export default Documantation
