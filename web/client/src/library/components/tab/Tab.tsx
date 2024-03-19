import { Tab } from '@headlessui/react'
import clsx from 'clsx'

export default function TabList({
  list,
  children = [],
  className,
  disabled = false,
}: {
  list: string[]
  disabled?: boolean
  children?: React.ReactNode
  className?: string
}): JSX.Element {
  return (
    <Tab.List
      className={clsx(
        'w-full whitespace-nowrap px-2 flex justify-center items-center',
        disabled && 'cursor-not-allowed',
      )}
    >
      <div
        className={clsx(
          'flex w-full overflow-hidden overflow-x-auto py-1 hover:scrollbar scrollbar--horizontal',
          className,
          disabled && 'opacity-50 pointer-events-none',
        )}
      >
        <div className="flex p-1 items-center bg-secondary-10 dark:bg-primary-10 cursor-pointer rounded-full overflow-hidden">
          {list.map(item => (
            <Tab
              key={item}
              className={({ selected }) =>
                clsx(
                  'text-xs px-2 py-0.5 mr-2 last:mr-0 rounded-full relative align-middle',
                  selected
                    ? 'bg-secondary-500 text-secondary-100 cursor-default font-bold'
                    : 'cursor-pointer font-medium text-secondary-400 dark:text-secondary-400',
                )
              }
            >
              {item}
            </Tab>
          ))}
        </div>
      </div>
      {children}
    </Tab.List>
  )
}
