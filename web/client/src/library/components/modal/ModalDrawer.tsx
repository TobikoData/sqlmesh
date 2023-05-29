import { Transition, Dialog } from '@headlessui/react'
import { Fragment } from 'react'

export default function ModalDrawer({
  show,
  children,
  afterLeave,
  onClose = () => undefined,
}: {
  show: boolean
  children: React.ReactNode
  onClose?: () => void
  afterLeave?: () => void
}): JSX.Element {
  return (
    <Transition
      appear
      show={show}
      as={Fragment}
      afterLeave={afterLeave}
    >
      <Dialog
        as="div"
        className="relative z-[100] w-full h-full "
        onClose={onClose}
      >
        <Transition.Child
          as={Fragment}
          enter="ease-out duration-300"
          enterFrom="bg-overlay opacity-0"
          enterTo="bg-overlay opacity-80"
          leave="ease-in duration-200"
          leaveFrom="bg-overlay opacity-80"
          leaveTo="bg-overlay opacity-0"
        >
          <div className="fixed inset-0" />
        </Transition.Child>

        <div className="w-full h-full fixed inset-0">
          <Transition.Child
            as={Fragment}
            enter="ease-out duration-300"
            enterFrom="translate-x-[100%]"
            enterTo="translate-x-0"
            leave="ease-in duration-200"
            leaveFrom="translate-x-0"
            leaveTo="translate-x-[100%]"
          >
            {children}
          </Transition.Child>
        </div>
      </Dialog>
    </Transition>
  )
}
