import { Fragment, useState } from 'react'
import { Menu, Transition } from '@headlessui/react'
import { CogIcon } from '@heroicons/react/24/solid'
import { CheckIcon } from '@heroicons/react/24/outline'
import clsx from 'clsx'

interface SettingsControlProps {
  settings: {
    showColumns: boolean
  }
  onSettingChange: (setting: string, value: boolean) => void
}

export function SettingsControl({
  settings,
  onSettingChange,
}: SettingsControlProps): JSX.Element {
  return (
    <Menu as="div" className="relative">
      <Menu.Button className="react-flow__controls-button" title="Settings">
        <CogIcon className="h-3 w-3" aria-hidden="true" />
      </Menu.Button>
      <Transition
        as={Fragment}
        enter="transition ease-out duration-100"
        enterFrom="transform opacity-0 scale-95"
        enterTo="transform opacity-100 scale-100"
        leave="transition ease-in duration-75"
        leaveFrom="transform opacity-100 scale-100"
        leaveTo="transform opacity-0 scale-95"
      >
        <Menu.Items className="absolute bottom-full mb-2 right-0 w-56 origin-bottom-right divide-y divide-neutral-100 dark:divide-neutral-800 rounded-md bg-theme shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none z-50">
          <div className="px-1 py-1">
            <Menu.Item>
              {({ active }) => (
                <button
                  className={clsx(
                    'group flex w-full items-center rounded-md px-2 py-2 text-sm',
                    active
                      ? 'bg-primary-10 text-primary-500'
                      : 'text-neutral-700 dark:text-neutral-300'
                  )}
                  onClick={() => onSettingChange('showColumns', !settings.showColumns)}
                >
                  <span className="flex-1 text-left">Show Columns</span>
                  {settings.showColumns && (
                    <CheckIcon className="h-4 w-4 text-primary-500" aria-hidden="true" />
                  )}
                </button>
              )}
            </Menu.Item>
          </div>
        </Menu.Items>
      </Transition>
    </Menu>
  )
}