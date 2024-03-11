import clsx from 'clsx'
import {
  ChevronDownIcon,
  MinusCircleIcon,
  StarIcon,
  CheckCircleIcon,
} from '@heroicons/react/20/solid'
import { Divider } from '@components/divider/Divider'
import { useStoreContext } from '@context/context'
import {
  EnumErrorKey,
  useNotificationCenter,
} from '../../pages/root/context/notificationCenter'
import { apiDeleteEnvironment } from '@api/index'
import { EnumSize, EnumVariant } from '~/types/enum'
import { isFalse } from '@utils/index'
import {
  type ButtonSize,
  makeButton,
  Button,
  EnumButtonFormat,
} from '@components/button/Button'
import { Menu, Transition } from '@headlessui/react'
import { type ModelEnvironment } from '@models/environment'
import { Fragment, type MouseEvent } from 'react'
import { AddEnvironment } from './AddEnvironment'

export function SelectEnvironment({
  disabled = false,
  className,
  showAddEnvironment = true,
  size = EnumSize.sm,
  onSelect,
}: {
  className?: string
  size?: ButtonSize
  disabled?: boolean
  showAddEnvironment?: boolean
  onSelect?: () => void
}): JSX.Element {
  const { addError } = useNotificationCenter()

  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)
  const setEnvironment = useStoreContext(s => s.setEnvironment)
  const removeLocalEnvironment = useStoreContext(s => s.removeLocalEnvironment)

  const ButtonMenu = makeButton<HTMLDivElement>(Menu.Button)

  function handleSelectEnvironment(e: MouseEvent, env: ModelEnvironment): void {
    e.stopPropagation()

    setEnvironment(env)

    onSelect?.()
  }

  function handleRemoveLocalEnvironment(
    e: MouseEvent,
    env: ModelEnvironment,
  ): void {
    e.stopPropagation()

    removeLocalEnvironment(env)
  }

  function handleDeleteEnvironment(e: MouseEvent, env: ModelEnvironment): void {
    e.stopPropagation()

    apiDeleteEnvironment(env.name)
      .then(() => {
        removeLocalEnvironment(env)
      })
      .catch(error => {
        addError(EnumErrorKey.Environments, error)
      })
  }

  return (
    <Menu
      as="div"
      className={clsx('flex relative', className)}
    >
      {({ close }) => (
        <>
          <ButtonMenu
            variant={EnumVariant.Info}
            size={size}
            disabled={disabled}
            className="flex justify-between w-full mx-0 pr-1"
          >
            <span
              className={clsx(
                'block overflow-hidden truncate',
                (environment.isLocal || disabled) && 'text-neutral-500',
                environment.isRemote
                  ? 'text-primary-600 dark:text-primary-300'
                  : 'text-neutral-600 dark:text-neutral-200',
              )}
            >
              <span className="text-xs text-neutral-300 dark:text-neutral-500 mr-1">
                Environment:
              </span>
              <span>{environment.name}</span>
            </span>
            <span className="pointer-events-none inset-y-0 right-0 flex items-center pl-1">
              <ChevronDownIcon
                className={clsx(
                  'w-4',
                  disabled
                    ? 'text-neutral-400 dark:text-neutral-200'
                    : 'text-neutral-800 dark:text-neutral-200',
                )}
                aria-hidden="true"
              />
            </span>
          </ButtonMenu>
          <Transition
            as={Fragment}
            leave="transition ease-in duration-100"
            leaveFrom="opacity-100"
            leaveTo="opacity-0"
          >
            <div className="!mx-0 absolute top-10 right-0 min-w-[16rem] max-w-[100%] overflow-hidden shadow-2xl bg-theme border-0 border-neutral-100 dark:border-neutral-800 rounded-md flex flex-col z-50">
              <Menu.Items className="mx-0 overflow-auto max-h-80 hover:scrollbar scrollbar--vertical">
                {Array.from(environments).map(env => (
                  <Menu.Item
                    key={env.name}
                    disabled={env === environment}
                  >
                    {({ active }) => (
                      <div
                        onClick={e => handleSelectEnvironment(e, env)}
                        className={clsx(
                          'flex justify-between items-center pl-2 pr-1 py-1 overflow-auto',
                          active
                            ? 'text-neutral-600 bg-neutral-10'
                            : 'text-neutral-400',
                          env === environment
                            ? 'cursor-default bg-neutral-5'
                            : 'cursor-pointer',
                        )}
                      >
                        <div className="flex items-start">
                          <CheckCircleIcon
                            className={clsx(
                              'w-4 h-4 mt-[6px]',
                              env === environment
                                ? 'opacity-100 text-primary-500'
                                : 'opacity-0',
                            )}
                          />
                          <span className="block">
                            <span className="flex items-baseline">
                              <span
                                className={clsx(
                                  'block truncate ml-2',
                                  env.isRemote && 'text-primary-500',
                                )}
                              >
                                {env.name}
                              </span>
                              <small className="block ml-2">({env.type})</small>
                            </span>
                            {env.isProd && (
                              <span className="flex ml-2">
                                <small className="text-xs text-neutral-500">
                                  Production Environment
                                </small>
                              </span>
                            )}
                            {env.isDefault && (
                              <span className="flex ml-2">
                                <small className="text-xs text-neutral-500">
                                  Default Environment
                                </small>
                              </span>
                            )}
                          </span>
                        </div>
                        <div className="flex items-center">
                          {env.isPinned && (
                            <StarIcon
                              title="Pinned"
                              className="w-4 text-primary-500 dark:text-primary-100 mx-1"
                            />
                          )}
                          {isFalse(env.isPinned) &&
                            env.isLocal &&
                            env !== environment && (
                              <Button
                                className="!m-0 !px-1 bg-transparent hover:bg-transparent border-none"
                                size={EnumSize.xs}
                                format={EnumButtonFormat.Ghost}
                                variant={EnumVariant.Neutral}
                                onClick={e =>
                                  handleRemoveLocalEnvironment(e, env)
                                }
                              >
                                <MinusCircleIcon className="w-4 text-neutral-500 dark:text-neutral-100" />
                              </Button>
                            )}
                          {isFalse(env.isPinned) &&
                            env.isRemote &&
                            env !== environment && (
                              <Button
                                className="!px-2 !my-0"
                                size={EnumSize.xs}
                                variant={EnumVariant.Danger}
                                onClick={e => handleDeleteEnvironment(e, env)}
                              >
                                Delete
                              </Button>
                            )}
                        </div>
                      </div>
                    )}
                  </Menu.Item>
                ))}
                {showAddEnvironment && (
                  <>
                    <Divider />
                    <AddEnvironment
                      onAdd={close}
                      className="p-2 pb-2"
                    />
                  </>
                )}
              </Menu.Items>
            </div>
          </Transition>
        </>
      )}
    </Menu>
  )
}
