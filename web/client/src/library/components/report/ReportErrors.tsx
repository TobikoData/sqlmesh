import { Disclosure, Popover, Transition } from '@headlessui/react'
import pluralize from 'pluralize'
import clsx from 'clsx'
import { useState, Fragment } from 'react'
import { useIDE, type ErrorIDE, type ErrorKey } from '../../pages/ide/context'
import { isNotNil, toDate, toDateFormat } from '@utils/index'
import { MinusCircleIcon, PlusCircleIcon } from '@heroicons/react/24/solid'
import { Divider } from '@components/divider/Divider'
import { useStoreContext } from '@context/context'

export default function ReportErrors(): JSX.Element {
  const { errors } = useIDE()

  const [isShow, setIsShow] = useState(false)

  const hasError = errors.size > 0

  return (
    <Popover
      onMouseEnter={() => {
        setIsShow(true)
      }}
      onMouseLeave={() => {
        setIsShow(false)
      }}
      className="flex"
    >
      {() => (
        <>
          <span
            className={clsx(
              'block ml-1 px-2 first-child:ml-0 rounded-full border whitespace-nowrap dark:text-neutral-100 text-xs text-center',
              hasError
                ? 'border-danger-500 text-danger-100 bg-danger-500 cursor-pointer'
                : 'border-neutral-500 cursor-default text-neutral-700',
            )}
          >
            {hasError ? (
              <span>
                {errors.size} {pluralize('Error', errors.size)}
              </span>
            ) : (
              <span>No Errors</span>
            )}
          </span>
          <Transition
            show={isShow && hasError}
            as={Fragment}
            enter="transition ease-out duration-200"
            enterFrom="opacity-0 translate-y-1"
            enterTo="opacity-100 translate-y-0"
            leave="transition ease-in duration-150"
            leaveFrom="opacity-100 translate-y-0"
            leaveTo="opacity-0 translate-y-1"
          >
            <Popover.Panel
              className={clsx(
                'absolute rounded-md top-20 right-2 z-[1000] bg-light p-2 transform overflow-hidden text-danger-700 shadow-2xl',
                'w-[90vw] max-h-[80vh]',
              )}
            >
              <ul
                className={clsx(
                  'w-full bg-danger-10 py-4 pl-3 pr-1 rounded-md overflow-auto hover:scrollbar scrollbar--vertical scrollbar--horizontal',
                  'max-w-[90vw] max-h-[80vh]',
                )}
              >
                {Array.from(errors.entries())
                  .reverse()
                  .map(([scope, error]) => (
                    <li
                      key={`${scope}-${error.message}`}
                      className="mb-10"
                    >
                      <DisplayError
                        scope={scope}
                        error={error}
                      />
                    </li>
                  ))}
              </ul>
            </Popover.Panel>
          </Transition>
        </>
      )}
    </Popover>
  )
}

export function DisplayError({
  error,
  scope,
}: {
  scope: ErrorKey
  error: ErrorIDE
}): JSX.Element {
  const version = useStoreContext(s => s.version)

  return (
    <div>
      <div className="flex w-full">
        <div className="w-[20rem]">
          <div className="flex aline-center mb-2">
            <small className="block mr-3">
              <small className="py-0.5 px-2 bg-danger-500 text-danger-100 rounded-md text-xs">
                {error.status ?? 'UI'}
              </small>
            </small>
            <small className="block">{scope}</small>
          </div>
          <div className={clsx('text-sm px-2')}>
            <small className="block text-xs font-mono">
              {toDateFormat(
                toDate(error.timestamp),
                'yyyy-mm-dd hh-mm-ss',
                false,
              )}
            </small>
            <p>{error.message}</p>
          </div>
        </div>
        <div className="w-full px-4">
          {isNotNil(version) && (
            <small className="block">
              <b>Version</b>: {version}
            </small>
          )}
          {error.type != null && (
            <small className="block">
              <b>Type</b>: {error.type}
            </small>
          )}
          {error.origin != null && (
            <small className="block mb-2">
              <b>Origin</b>: {error.origin}
            </small>
          )}
          {error.trigger != null && (
            <small className="block mb-2">
              <b>Trigger</b>: {error.trigger}
            </small>
          )}
          <Divider className="!border-danger-10" />
          <p className="text-sm my-4">{error.description}</p>
        </div>
      </div>
      {error.traceback != null && (
        <>
          <div className="'w-full mb-4 mt-6">
            <Disclosure>
              {({ open }) => (
                <>
                  <Disclosure.Button
                    className={clsx(
                      'flex items-center justify-between rounded-lg text-left w-full bg-neutral-10 px-3 mb-2',
                    )}
                  >
                    <div className="text-lg font-bold whitespace-nowrap w-full">
                      <h3 className="py-2">Traceback</h3>
                    </div>
                    <div>
                      {open ? (
                        <MinusCircleIcon className="h-6 w-6 text-danger-500" />
                      ) : (
                        <PlusCircleIcon className="h-6 w-6 text-danger-500" />
                      )}
                    </div>
                  </Disclosure.Button>
                  <Disclosure.Panel className="px-2 pb-2 overflow-hidden">
                    <pre className="font-mono w-full bg-dark-lighter text-danger-500 rounded-lg p-4 overflow-auto hover:scrollbar scrollbar--vertical scrollbar--horizontal max-h-[35vh] text-sm">
                      <code>{error.traceback ?? error.message}</code>
                    </pre>
                  </Disclosure.Panel>
                </>
              )}
            </Disclosure>
          </div>
          <Divider className="!border-danger-10" />
        </>
      )}
    </div>
  )
}
