import { Dialog } from '@headlessui/react'
import Modal from './Modal'

interface PropsModalConfirmation extends React.HTMLAttributes<HTMLElement> {
  show: boolean
  headline?: string
  tagline?: string
  description?: string
  onClose: () => void
}

export interface Confirmation {
  action?: () => void
  cancel?: () => void
  headline?: string
  description?: string
  tagline?: string
  yesText: string
  noText: string
}

export interface WithConfirmation {
  setConfirmation: (confirmation?: Confirmation) => void
}

export default function ModalConfirmation({
  show,
  headline,
  tagline,
  description,
  children,
  onClose,
}: PropsModalConfirmation): JSX.Element {
  return (
    <Modal
      show={show}
      onClose={onClose}
    >
      <Dialog.Panel className="w-[30rem] transform overflow-hidden rounded-xl bg-white text-left align-middle shadow-xl transition-all">
        <div className="py-4 px-5 mt-2 mb-5">
          <h2 className="font-black text-xl mb-2">{headline}</h2>
          <h4>{tagline}</h4>
          <p>{description}</p>
        </div>
        <div className="flex justify-end items-center py-3 px-3">
          {children}
        </div>
      </Dialog.Panel>
    </Modal>
  )
}
