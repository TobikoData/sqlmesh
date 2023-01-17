import { Menu, Transition } from '@headlessui/react'
import { PlayIcon, PlusCircleIcon, EllipsisVerticalIcon  } from '@heroicons/react/24/solid'
import { Fragment } from 'react'


export function DropdownPlan() {
  return (
    <Menu as="div" className="relative inline-block text-left">
      <Menu.Button className='bg-secondary-500 text-gray-100 h-6 whitespace-nowrap rounded flex justify-center items-center mr-2 pl-2 pr-1'>
        <small className='inline-block mr-2'>Run Plan</small>
        <PlayIcon className='w-4 h-4 text-gray-100' />
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
        <Menu.Items className={'absolute bsolute right-0 mt-2 w-56 origin-top-right divide-y divide-gray-100 rounded-md bg-white shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none'}>
          <Menu.Item>
            {({ active }) => (
              <p
                className={`${active && 'bg-blue-500'} whitespace-nowrap`}
              >
                Production Plan
              </p>
            )}
          </Menu.Item>
          <Menu.Item>
            {({ active }) => (
              <p
                className={`${active && 'bg-blue-500'} whitespace-nowrap`}
              >
                Development Plan
              </p>
            )}
          </Menu.Item>          
      </Menu.Items> 
      </Transition>

    </Menu>
  )
}

export function DropdownAudits() {
  return (
    <Menu as="div" className="relative inline-block text-left">
      <Menu.Button className='bg-gray-500 text-gray-100 h-6 whitespace-nowrap rounded flex justify-center items-center mr-2 pl-2 pr-1'>
        <small className='inline-block mr-2'>Run Audits</small>
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
        <Menu.Items className={'absolute bsolute right-0 mt-2 w-56 origin-top-right divide-y divide-gray-100 rounded-md bg-white shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none'}>
          <Menu.Item>
              {({ active }) => (
                <p
                  className={`${active && 'bg-blue-500'} whitespace-nowrap`}
                >
                  Production Audits
                </p>
              )}
            </Menu.Item>
            <Menu.Item>
              {({ active }) => (
                <p
                  className={`${active && 'bg-blue-500'} whitespace-nowrap`}
                >
                  Development Audits
                </p>
              )}
            </Menu.Item>        
      </Menu.Items> 
      </Transition>

    </Menu>
  )
}

export function DropdownActions() {
  return (
    <Menu as="div" className="relative inline-block text-left">
      <Menu.Button className='bg-gray-100 h-6 w-6 whitespace-nowrap rounded flex justify-center items-center'>
        <EllipsisVerticalIcon className='w-5 h-5 text-secondary-500' />
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
        <Menu.Items className={'absolute bsolute right-0 mt-2 w-56 origin-top-right divide-y divide-gray-100 rounded-md bg-white shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none'}>
          <Menu.Item>
            {({ active }) => (
              <p
                className={`${active && 'bg-blue-500'} whitespace-nowrap flex`}
              >
                <PlusCircleIcon  className='w-4 h-4 text-gray-100' />
                Add Envoirment
              </p>
            )}
          </Menu.Item>      
      </Menu.Items> 
      </Transition>

    </Menu>
  )
}