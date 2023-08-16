import { Fragment, useEffect, useState } from 'react'
import { Listbox, Transition } from '@headlessui/react'
import { ChevronDownIcon, CheckIcon } from '@heroicons/react/24/solid'
import { isFalse } from '@utils/index'
import clsx from 'clsx'

export default function ListboxShow({
  options,
  value = [],
}: {
  options: Record<
    string,
    | Optional<React.Dispatch<React.SetStateAction<boolean>>>
    | ((value: boolean) => void)
  >
  value: string[]
}): JSX.Element {
  const [selected, setSelected] = useState<string[]>([])

  useEffect(() => {
    setSelected(value)
  }, [value])

  return (
    <Listbox
      value={selected}
      onChange={value => {
        setSelected(value)

        for (const key in options) {
          options[key]?.(value.includes(key))
        }
      }}
      multiple
    >
      <div className="relative flex">
        <Listbox.Button className="flex items-center relative w-full cursor-default bg-primary-10 text-xs rounded-full text-primary-500 py-1 px-2 text-center focus:outline-none focus-visible:border-accent-500 focus-visible:ring-2 focus-visible:ring-light focus-visible:ring-opacity-75 focus-visible:ring-offset-2 focus-visible:ring-offset-brand-300">
          <span className="block truncate">Show</span>
          <ChevronDownIcon
            className="ml-2 h-4 w-4"
            aria-hidden="true"
          />
        </Listbox.Button>
        <Transition
          as={Fragment}
          leave="transition ease-in duration-100"
          leaveFrom="opacity-100"
          leaveTo="opacity-0"
        >
          <Listbox.Options className="absolute top-8 right-0 z-50 max-h-60 min-w-16 overflow-auto rounded-md bg-theme py-2 shadow-lg ring-2 ring-primary-10 ring-opacity-5 focus:outline-none sm:text-sm">
            {Object.keys(options)
              .filter(key => options[key])
              .map(key => (
                <Listbox.Option
                  key={key}
                  className={({ active, disabled }) =>
                    clsx(
                      'relative cursor-default select-none py-1 pl-10 pr-4',
                      disabled ? 'opacity-50 cursor-not-allowed' : '',
                      active
                        ? 'bg-primary-10 text-primary-500'
                        : 'text-neutral-700 dark:text-neutral-300',
                    )
                  }
                  value={key}
                  disabled={isFalse(Boolean(options[key]))}
                >
                  {({ selected }) => (
                    <>
                      <span>{key}</span>
                      <span
                        className={clsx(
                          'absolute inset-y-0 left-0 flex items-center pl-3 text-primary-500',
                          selected ? 'block' : 'hidden',
                        )}
                      >
                        <CheckIcon
                          className="h-4 w-4"
                          aria-hidden="true"
                        />
                      </span>
                    </>
                  )}
                </Listbox.Option>
              ))}
          </Listbox.Options>
        </Transition>
      </div>
    </Listbox>
  )
}
