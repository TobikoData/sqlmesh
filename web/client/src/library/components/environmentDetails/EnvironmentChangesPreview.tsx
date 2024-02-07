import clsx from 'clsx'
import PlanChangePreview from '@components/plan/PlanChangePreview'
import {
  EnumPlanChangeType,
  type PlanChangeType,
} from '@components/plan/context'
import { Transition, Popover } from '@headlessui/react'
import { type ModelSQLMeshChangeDisplay } from '@models/sqlmesh-change-display'
import { Fragment, useState } from 'react'

export default function EnvironmentChangesPreview({
  headline,
  type,
  changes,
  className,
}: {
  headline?: string
  type: PlanChangeType
  changes: ModelSQLMeshChangeDisplay[]
  className?: string
}): JSX.Element {
  const [isShowing, setIsShowing] = useState(false)

  return (
    <Popover
      onMouseEnter={() => {
        setIsShowing(true)
      }}
      onMouseLeave={() => {
        setIsShowing(false)
      }}
      className="relative flex"
    >
      {() => (
        <>
          <span
            className={clsx(
              'transition-all flex min-w-[1rem] h-4 text-center mx-0.5 px-1 rounded-full text-[10px] font-black text-neutral-100 cursor-default border border-inherit',
              type === EnumPlanChangeType.Add &&
                'bg-success-500 border-success-500',
              type === EnumPlanChangeType.Remove &&
                'bg-danger-500 border-danger-500',
              type === EnumPlanChangeType.Direct &&
                'bg-secondary-500 border-secondary-500',
              type === EnumPlanChangeType.Indirect &&
                'bg-warning-500 border-warning-500',
              type === EnumPlanChangeType.Metadata &&
                'bg-neutral-400 border-neutral-400',
              type === EnumPlanChangeType.Default &&
                'bg-neutral-600 border-neutral-600',
              className,
            )}
          >
            {changes.length}
          </span>
          <Transition
            show={isShowing}
            as={Fragment}
            enter="transition ease-out duration-200"
            enterFrom="opacity-0 translate-y-1"
            enterTo="opacity-100 translate-y-0"
            leave="transition ease-in duration-150"
            leaveFrom="opacity-100 translate-y-0"
            leaveTo="opacity-0 translate-y-1"
          >
            <Popover.Panel className="absolute top-6 right-0 z-10 transform flex p-2 bg-theme-lighter shadow-xl focus:ring-2 ring-opacity-5 rounded-lg ">
              <PlanChangePreview
                headline={headline}
                type={type}
                className="w-full h-full max-w-[50vw] max-h-[40vh] overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical"
              >
                <PlanChangePreview.Default
                  type={type}
                  changes={changes}
                />
              </PlanChangePreview>
            </Popover.Panel>
          </Transition>
        </>
      )}
    </Popover>
  )
}
