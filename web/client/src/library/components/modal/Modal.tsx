import { Transition, Dialog } from '@headlessui/react'
import { Fragment } from 'react'

interface PropsModal extends React.HTMLAttributes<HTMLElement> {
  show: boolean
  onClose: () => void
}

export default function Modal({
  show,
  children,
  onClose = () => undefined,
}: PropsModal): JSX.Element {
  return (
    <Transition
      appear
      show={show}
    >
      <Dialog
        as="div"
        className="relative z-[100]"
        onClose={onClose}
      >
        <Transition.Child
          as={Fragment}
          enter="ease-out duration-300"
          enterFrom="opacity-0"
          enterTo="opacity-100"
          leave="ease-in duration-200"
          leaveFrom="opacity-100"
          leaveTo="opacity-0"
        >
          <div className="fixed inset-0 bg-black bg-opacity-25" />
        </Transition.Child>

        <div className="fixed inset-0 overflow-y-auto">
          <div className="flex min-h-full items-center justify-center p-4 text-center">
            <Transition.Child
              as={Fragment}
              enter="ease-out duration-300"
              enterFrom="opacity-0 scale-95"
              enterTo="opacity-100 scale-100"
              leave="ease-in duration-200"
              leaveFrom="opacity-100 scale-100"
              leaveTo="opacity-0 scale-95"
            >
              {children}
            </Transition.Child>
          </div>
        </div>
      </Dialog>
    </Transition>
  )
}
