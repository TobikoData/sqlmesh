import React, { useState } from 'react'
import { Tab } from '@headlessui/react'
import clsx from 'clsx'

export default function Example() {
  let [categories] = useState({
    Table: [],
    // DAG: [],
    'Query Preview': [],
  })

  return (
    <div className="h-full w-full bg-gray-800/5 sm:px-0 ">
      <Tab.Group>
        <Tab.List className="w-full whitespace-nowrap px-4 rounded-xl bg-blue-900/20 p-1 ">
          <div className="w-full overflow-hidden overflow-x-auto">
            {Object.keys(categories).map((category) => (
              <Tab
                key={category}
                className={({ selected }) =>
                  clsx(
                    'inline-block rounded-lg p-2.5 text-sm font-medium leading-5 text-blue-700',
                    'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
                    selected
                      ? 'bg-white shadow'
                      : 'text-blue-100 hover:bg-white/[0.12] hover:text-white'
                  )
                }
              >
                {category}
              </Tab>
            ))}
          </div>
        </Tab.List>
        <Tab.Panels className="mt-2">
          <Tab.Panel
            className={clsx(
              'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2'
            )}
          >
            Table
          </Tab.Panel>
          <Tab.Panel
            className={clsx(
              'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2'
            )}
          >
            DAG
          </Tab.Panel>
          <Tab.Panel
            className={clsx(
              'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2'
            )}
          >
            Query Preview
          </Tab.Panel>
        </Tab.Panels>
      </Tab.Group>
    </div>
  )
}
