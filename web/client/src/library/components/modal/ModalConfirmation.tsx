import { Dialog } from '@headlessui/react'
import Modal from './Modal';

export default function ModalConfirmation({ show, headline, tagline, description, children }: any) {
  return (
    <Modal
      show={show}
    >
      <Dialog.Panel
        className="w-[30rem] transform overflow-hidden rounded-xl bg-white text-left align-middle shadow-xl transition-all"
      >
        <div className='py-4 px-5 mt-2 mb-5'>
          <h2 className='font-black text-xl mb-2'>{headline}</h2>
          <h4>{tagline}</h4>
          <p>{description}</p>
        </div>
        <div className='flex justify-end items-center py-3 px-3'>
          {children}
        </div>
      </Dialog.Panel>
    </Modal>
  );
}