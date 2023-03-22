import { Dialog } from '@headlessui/react'
import Modal from './Modal'

interface PropsModalConfirmation extends React.HTMLAttributes<HTMLElement> {
  show: boolean
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

function ModalConfirmation({
  show,
  children,
  onClose,
}: PropsModalConfirmation): JSX.Element {
  return (
    <Modal
      show={show}
      onClose={onClose}
    >
      <Dialog.Panel className="w-[30rem] transform rounded-xl bg-theme text-left align-middle shadow-xl transition-all">
        {children}
      </Dialog.Panel>
    </Modal>
  )
}

function ModalConfirmationMain({
  children,
}: React.HTMLAttributes<HTMLElement>): JSX.Element {
  return <div className="py-4 px-5 m-2">{children}</div>
}

function ModalConfirmationHeadline({
  children,
}: React.HTMLAttributes<HTMLElement>): JSX.Element {
  return <h2 className="font-bold text-xl mb-2">{children}</h2>
}

function ModalConfirmationTagline({
  children,
}: React.HTMLAttributes<HTMLElement>): JSX.Element {
  return <h4 className="font-bold">{children}</h4>
}

function ModalConfirmationDescription({
  children,
}: React.HTMLAttributes<HTMLElement>): JSX.Element {
  return <p className="text-sm">{children}</p>
}

function ModalConfirmationActions({
  children,
}: React.HTMLAttributes<HTMLElement>): JSX.Element {
  return (
    <div className="flex justify-end items-center py-3 px-3">{children}</div>
  )
}

ModalConfirmation.Headline = ModalConfirmationHeadline
ModalConfirmation.Tagline = ModalConfirmationTagline
ModalConfirmation.Description = ModalConfirmationDescription
ModalConfirmation.Actions = ModalConfirmationActions
ModalConfirmation.Main = ModalConfirmationMain

export default ModalConfirmation
