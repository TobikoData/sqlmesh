import Input from '@components/input/Input'
import { Listbox, Transition } from '@headlessui/react'
import { ChevronUpDownIcon, CheckIcon } from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { Fragment } from 'react'

export default function Selector({
  label,
  info,
  item,
  list = [],
  disabled = false,
  onChange,
  className,
}: {
  list: Array<{ text: string; value: string }>
  item: { text: string; value: string }
  onChange: (value: { text: string; value: string }) => void
  disabled?: boolean
  label?: string
  info?: string
  className?: string
}): JSX.Element {
  return (
    <div
      className={clsx(
        'inline-block relative m-1',
        disabled && 'opacity-50 cursor-not-allowed',
        className,
      )}
    >
      {label != null && <Input.Label>{label}</Input.Label>}
      <Listbox
        disabled={disabled}
        value={item.value}
        onChange={value => {
          onChange(list.find(i => value === i.value)!)
        }}
      >
        <div className="relative mt-1">
          <Listbox.Button
            className={clsx(
              'relative w-full cursor-default rounded-lg bg-theme py-2 pl-3 pr-10 text-left border-2 border-secondary-100  dark:border-primary-10 focus:outline-none focus-visible:border-indigo-500 focus-visible:ring-2 focus-visible:ring-light focus-visible:ring-opacity-75 focus-visible:ring-offset-2 focus-visible:ring-offset-brand-300 sm:text-sm',
              disabled && 'opacity-50 cursor-not-allowed',
            )}
          >
            <span className="block truncate">{item.text}</span>
            <span className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-2">
              <ChevronUpDownIcon
                className="h-5 w-5 text-gray-400"
                aria-hidden="true"
              />
            </span>
          </Listbox.Button>
          <Transition
            as={Fragment}
            leave="transition ease-in duration-100"
            leaveFrom="opacity-100"
            leaveTo="opacity-0"
          >
            <Listbox.Options className="absolute mt-1 z-50 max-h-60 w-full overflow-auto rounded-md bg-theme py-1 text-base shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none sm:text-sm">
              {list.map(({ text, value }) => (
                <Listbox.Option
                  key={value}
                  className={({ active }) =>
                    `relative cursor-default select-none py-2 pl-10 pr-4 ${
                      active ? 'bg-amber-100 text-amber-900' : 'text-gray-900'
                    }`
                  }
                  value={value}
                >
                  {({ selected }) => (
                    <>
                      <span
                        className={`block truncate ${
                          selected ? 'font-medium' : 'font-normal'
                        }`}
                      >
                        {text}
                      </span>
                      {selected ? (
                        <span className="absolute inset-y-0 left-0 flex items-center pl-3 text-amber-600">
                          <CheckIcon
                            className="h-5 w-5"
                            aria-hidden="true"
                          />
                        </span>
                      ) : null}
                    </>
                  )}
                </Listbox.Option>
              ))}
            </Listbox.Options>
          </Transition>
        </div>
      </Listbox>
      {info != null && <Input.Info>{info}</Input.Info>}
    </div>
  )
}
