import { Menu, Transition } from '@headlessui/react'
import { PlayIcon } from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { Fragment } from 'react'
import { EnumSize } from '../../../types/enum'
import { ButtonMenu } from '../button/Button'

export function DropdownPlan({
  onSelect,
}: {
  onSelect: (item: { text: string; value: string }) => void
}): JSX.Element {
  return (
    <Menu
      as="div"
      className="relative inline-block text-left"
    >
      <ButtonMenu size={EnumSize.sm}>
        <span className="inline-block mr-3">Run Plan</span>
        <PlayIcon className="w-4 h-4 text-gray-100" />
      </ButtonMenu>

      <Transition
        as={Fragment}
        enter="transition ease-out duration-100"
        enterFrom="transform opacity-0 scale-95"
        enterTo="transform opacity-100 scale-100"
        leave="transition ease-in duration-75"
        leaveFrom="transform opacity-100 scale-100"
        leaveTo="transform opacity-0 scale-95"
      >
        <Menu.Items
          className={
            'absolute bsolute right-0 py-1 mt-2 w-56 origin-top-right divide-y divide-gray-100 rounded-md bg-white shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none'
          }
        >
          {[
            { text: 'Production Plan', value: 'production' },
            { text: 'Development Plan', value: 'development' },
          ].map(item => (
            <Menu.Item
              key={item.value}
              as={Fragment}
            >
              {({ active }) => (
                <p
                  onClick={() => {
                    onSelect(item)
                  }}
                  className={clsx(
                    `whitespace-nowrap text-gray-600 px-2 py-1 cursor-pointer hover:bg-secondary-100`,
                    active && 'bg-blue-500',
                  )}
                >
                  {item.text}
                </p>
              )}
            </Menu.Item>
          ))}
        </Menu.Items>
      </Transition>
    </Menu>
  )
}
