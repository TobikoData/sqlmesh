import { Disclosure, Popover, Transition } from '@headlessui/react'
import pluralize from 'pluralize'
import clsx from 'clsx'
import React, { useState, Fragment, useEffect } from 'react'
import {
  useNotificationCenter,
  type ErrorIDE,
  type ErrorKey,
} from '../../pages/root/context/notificationCenter'
import { isNil, isNotNil, toDate, toDateFormat } from '@utils/index'
import { MinusCircleIcon, PlusCircleIcon } from '@heroicons/react/24/solid'
import { Divider } from '@components/divider/Divider'
import { useStoreContext } from '@context/context'
import { Button } from '@components/button/Button'
import { EnumSize, EnumVariant } from '~/types/enum'
import SplitPane from '@components/splitPane/SplitPane'

export default function ReportErrors(): JSX.Element {
  const { errors, clearErrors } = useNotificationCenter()

  const [isShow, setIsShow] = useState(false)

  useEffect(() => {
    if (errors.size < 1) {
      setIsShow(false)
    }
  }, [errors])

  const hasError = errors.size > 0

  return (
    <Popover
      onMouseEnter={() => {
        setIsShow(hasError)
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
              'block ml-1 px-3 py-1 first-child:ml-0 rounded-full whitespace-nowrap font-bold text-xs text-center',
              hasError
                ? 'text-danger-500 bg-danger-10 dark:bg-danger-20 cursor-pointer'
                : 'bg-neutral-5 dark:bg-neutral-20 cursor-default text-neutral-500 dark:text-neutral-300',
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
            show={isShow}
            as={Fragment}
            enter="transition ease-out duration-200"
            enterFrom="opacity-0 translate-y-1"
            enterTo="opacity-100 translate-y-0"
            leave="transition ease-in duration-150"
            leaveFrom="opacity-100 translate-y-0"
            leaveTo="opacity-0 translate-y-1"
          >
            <Popover.Panel className="absolute top-10 right-2 z-[1000] flex flex-col rounded-md bg-light transform text-danger-700 shadow-2xl w-[90vw] max-h-[80vh]">
              <div className="flex justify-end mx-1 mt-2">
                <Button
                  size={EnumSize.sm}
                  variant={EnumVariant.Neutral}
                  onClick={() => clearErrors()}
                >
                  Clear
                </Button>
              </div>
              <ul className="w-full h-full p-2 overflow-auto hover:scrollbar scrollbar--vertical scrollbar--horizontal">
                {Array.from(errors)
                  .reverse()
                  .map(error => (
                    <li
                      key={error.id}
                      className="bg-danger-10 mb-4 last:m-0 p-2 rounded-md"
                    >
                      <DisplayError
                        scope={error.key}
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
  withSplitPane = false,
}: {
  scope: ErrorKey
  error: ErrorIDE
  withSplitPane?: boolean
}): JSX.Element {
  const { removeError } = useNotificationCenter()

  const version = useStoreContext(s => s.version)

  return (
    <div className="flex flex-col w-full h-full overflow-hidden">
      {isNil(error.traceback) ? (
        <DisplayErrorDetails
          version={version}
          scope={scope}
          error={error}
        />
      ) : withSplitPane ? (
        <SplitPane
          direction="vertical"
          sizes={[50, 50]}
          minSize={100}
          snapOffset={0}
          className="flex flex-col w-full h-full overflow-hidden"
        >
          <DisplayErrorDetails
            version={version}
            scope={scope}
            error={error}
          />
          {isNotNil(error.traceback) && (
            <Disclosure defaultOpen={true}>
              {({ open }) => (
                <div className="flex flex-col w-full h-full overflow-hidden">
                  <Disclosure.Button className="flex items-center justify-between rounded-lg text-left w-full bg-neutral-10 px-3 my-2">
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
                  <Disclosure.Panel className="flex w-full h-full px-2 mb-2 overflow-hidden">
                    <pre className="font-mono w-full h-full bg-dark-lighter text-danger-500 rounded-lg p-4 overflow-scroll hover:scrollbar scrollbar--vertical scrollbar--horizontal text-sm">
                      <code>{error.traceback ?? error.message}</code>
                    </pre>
                  </Disclosure.Panel>
                </div>
              )}
            </Disclosure>
          )}
        </SplitPane>
      ) : (
        <>
          <DisplayErrorDetails
            version={version}
            scope={scope}
            error={error}
          />
          {isNotNil(error.traceback) && (
            <Disclosure defaultOpen={true}>
              {({ open }) => (
                <div className="w-full h-full overflow-hidden">
                  <Disclosure.Button className="flex items-center justify-between rounded-lg text-left w-full bg-neutral-10 px-3 my-2">
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
                  <Disclosure.Panel className="flex w-full h-full px-2 mb-2 overflow-hidden">
                    <pre className="font-mono w-full h-full bg-dark-lighter text-danger-500 rounded-lg p-4 overflow-scroll hover:scrollbar scrollbar--vertical scrollbar--horizontal text-sm">
                      <code>{error.traceback ?? error.message}</code>
                    </pre>
                  </Disclosure.Panel>
                </div>
              )}
            </Disclosure>
          )}
        </>
      )}
      <div className="flex justify-end mt-2">
        <Button
          variant={EnumVariant.Neutral}
          size={EnumSize.sm}
          onClick={(e: React.MouseEvent<HTMLButtonElement>) => {
            e.stopPropagation()

            removeError(error)
          }}
        >
          Dismiss
        </Button>
      </div>
    </div>
  )
}

function DisplayErrorDetails({
  error,
  scope,
  version,
}: {
  scope: ErrorKey
  error: ErrorIDE
  version?: string
}): JSX.Element {
  return (
    <div className="flex w-full overflow-hidden mb-2">
      <div className="w-[20rem] h-full">
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
      <div className="flex flex-col w-full px-4">
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
        <Divider />
        <div className="pt-4 h-full overflow-scroll hover:scrollbar scrollbar--vertical scrollbar--horizontal">
          <p className="flex text-sm ">{error.description}</p>
        </div>
      </div>
    </div>
  )
}
