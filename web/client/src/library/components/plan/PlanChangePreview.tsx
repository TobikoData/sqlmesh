import { Disclosure } from '@headlessui/react'
import {
  MinusCircleIcon,
  PlusCircleIcon,
  PlusIcon,
  MinusIcon,
  ArrowPathRoundedSquareIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { type ModelsDiffDirectItem } from '~/api/client'
import { Divider } from '../divider/Divider'
import { EnumPlanChangeType, type PlanChangeType } from './context'

interface PropsPlanChangePreview extends React.HTMLAttributes<HTMLElement> {
  headline?: string
  type: PlanChangeType
}

function PlanChangePreview({
  children,
  headline,
  type,
  className,
}: PropsPlanChangePreview): JSX.Element {
  return (
    <div
      className={clsx(
        'flex flex-col rounded-md p-4 mx-2',
        type === EnumPlanChangeType.Add && 'bg-success-100',
        type === EnumPlanChangeType.Remove && 'bg-danger-100',
        type === EnumPlanChangeType.Direct && 'bg-secondary-100',
        type === EnumPlanChangeType.Indirect && 'bg-warning-100',
        type === 'metadata' && 'bg-gray-100',
        className,
      )}
    >
      {headline != null && (
        <h4
          className={clsx(
            `mb-2 font-bold whitespace-nowrap`,
            type === EnumPlanChangeType.Add && 'text-success-700',
            type === EnumPlanChangeType.Remove && 'text-danger-700',
            type === EnumPlanChangeType.Direct && 'text-secondary-500',
            type === EnumPlanChangeType.Indirect && 'text-warning-700',
            type === EnumPlanChangeType.Metadata && 'text-gray-900',
          )}
        >
          {headline}
        </h4>
      )}
      <ul>{children}</ul>
    </div>
  )
}

function PlanChangePreviewDefault({
  changes = [],
  type,
}: {
  type: PlanChangeType
  changes?: string[]
}): JSX.Element {
  return (
    <>
      {changes.map(change => (
        <li
          key={change}
          className={clsx(
            'flex items-center px-1 leading-5',
            type === EnumPlanChangeType.Add && 'text-success-700',
            type === EnumPlanChangeType.Remove && 'text-danger-700',
            type === EnumPlanChangeType.Direct && 'text-secondary-500',
            type === EnumPlanChangeType.Indirect && 'text-warning-700',
            type === EnumPlanChangeType.Metadata && 'text-gray-900',
          )}
        >
          {type === EnumPlanChangeType.Add ? (
            <PlusIcon className="h-3 mr-2" />
          ) : type === EnumPlanChangeType.Remove ? (
            <MinusIcon className="h-3 mr-2" />
          ) : (
            <ArrowPathRoundedSquareIcon className="h-4 mr-2" />
          )}
          <small>{change}</small>
        </li>
      ))}
    </>
  )
}

function PlanChangePreviewDirect({
  changes = [],
}: {
  changes?: ModelsDiffDirectItem[]
}): JSX.Element {
  return (
    <>
      {changes.map(change => (
        <li
          key={change.model_name}
          className="text-secondary-500"
        >
          <PlanChangePreviewDiff change={change} />
        </li>
      ))}
    </>
  )
}

function PlanChangePreviewDiff({
  change,
}: {
  change: ModelsDiffDirectItem
}): JSX.Element {
  return (
    <Disclosure>
      {({ open }) => (
        <>
          <Disclosure.Button className="flex items-center w-full justify-between rounded-lg text-left">
            <div className="flex items-center ">
              <ArrowPathRoundedSquareIcon className="h-4 mr-2" />
              <small className="inline-block text-sm">
                {change.model_name}
              </small>
            </div>
            <Divider className="mx-4" />
            {(() => {
              const Tag = open ? MinusCircleIcon : PlusCircleIcon

              return (
                <Tag className="max-h-[1rem] min-w-[1rem] text-secondary-200" />
              )
            })()}
          </Disclosure.Button>
          <Disclosure.Panel className="text-sm text-secondary-100">
            <pre className="my-4 bg-secondary-900 rounded-lg p-4">
              {change.diff?.split('\n').map((line: string, idx: number) => (
                <p
                  key={`${line}-${idx}`}
                  className={clsx(
                    line.startsWith('+') && 'text-success-500',
                    line.startsWith('-') && 'text-danger-500',
                    line.startsWith('@@') && 'text-secondary-300 my-5',
                  )}
                >
                  {line}
                </p>
              ))}
            </pre>
          </Disclosure.Panel>
        </>
      )}
    </Disclosure>
  )
}

PlanChangePreview.Default = PlanChangePreviewDefault
PlanChangePreview.Diff = PlanChangePreviewDiff
PlanChangePreview.Direct = PlanChangePreviewDirect

export default PlanChangePreview
