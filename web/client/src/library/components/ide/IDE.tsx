import { Button, ButtonMenu } from '../button/Button'
import { Divider } from '../divider/Divider'
import { Editor } from '../editor/Editor'
import { FolderTree } from '../folderTree/FolderTree'
import { Fragment, useEffect, MouseEvent, useState, lazy } from 'react'
import clsx from 'clsx'
import { ChevronDownIcon, CheckCircleIcon } from '@heroicons/react/24/solid'
import { EnumSize } from '../../../types/enum'
import {
  Transition,
  Dialog,
  Popover,
  RadioGroup,
  Menu,
} from '@headlessui/react'
import { useApiPlan, useApiFiles, useApiEnvironments } from '../../../api'
import fetchAPI from '../../../api/instance'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
} from '../../../context/plan'
import { useChannel } from '../../../api/channels'
import SplitPane from '../splitPane/SplitPane'
import useLocalStorage from '~/hooks/useLocalStorage'
import Modal from '../modal/Modal'
import { isArrayEmpty, isStringEmptyOrNil } from '~/utils'
import Input from '../input/Input'
import {
  Environment,
  getDefaultEnvironments,
  useStoreContext,
} from '~/context/context'
import { useStoreFileTree } from '~/context/fileTree'

const Plan = lazy(async () => await import('../plan/Plan'))
const Graph = lazy(async () => await import('../graph/Graph'))
const Spinner = lazy(async () => await import('../logo/Spinner'))
const Tasks = lazy(async () => await import('../plan/Tasks'))

const CUSTOM = 'custom'
const PROFILE = 'profile'

interface Profile {
  environment: string
  environments: Environment[]
}

export function IDE(): JSX.Element {
  const [profile, setProfile] = useLocalStorage<Profile>(PROFILE)

  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const mostRecentPlan = useStorePlan(s => s.lastPlan ?? s.activePlan)
  const setPlanState = useStorePlan(s => s.setState)
  const setPlanAction = useStorePlan(s => s.setAction)
  const setActivePlan = useStorePlan(s => s.setActivePlan)
  const setLastPlan = useStorePlan(s => s.setLastPlan)
  const updateTasks = useStorePlan(s => s.updateTasks)

  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)
  const setEnvironment = useStoreContext(s => s.setEnvironment)
  const setEnvironments = useStoreContext(s => s.setEnvironments)

  const activeFileId = useStoreFileTree(s => s.activeFileId)
  const openedFiles = useStoreFileTree(s => s.openedFiles)

  const [isGraphOpen, setIsGraphOpen] = useState(false)

  const [subscribe, getChannel, unsubscribe] = useChannel(
    '/api/tasks',
    updateTasks,
  )

  const { data: project } = useApiFiles()
  const { data: contextEnvironments } = useApiEnvironments()
  const { refetch, data: plan } = useApiPlan(environment)

  useEffect(() => {
    if (getChannel() == null) {
      subscribe()
    }
  }, [])

  useEffect(() => {
    if (contextEnvironments == null || environments == null) return

    const newEnvironments = structuredClone(environments)

    Object.keys(contextEnvironments).forEach(envName => {
      const environment = newEnvironments.find(env => env.name === envName)

      if (environment == null) {
        newEnvironments.push({ name: envName, type: 'local' })
      } else {
        // Still holds reference to object in newEnvironments
        environment.type = 'remote'
      }
    })

    newEnvironments.sort(env => (env.type === 'remote' ? -1 : 1))

    setEnvironments(newEnvironments)
  }, [contextEnvironments])

  useEffect(() => {
    if (environment == null) return

    void refetch()
  }, [environment])

  useEffect(() => {
    setEnvironment(profile?.environment)

    if (profile?.environments == null) return

    const newEnvironments = environments.filter(env => env.type === 'remote')

    profile.environments.forEach(profileEnv => {
      const foundIndex = newEnvironments.findIndex(
        env => env.name === profileEnv.name,
      )

      if (foundIndex < 0) {
        newEnvironments.push(profileEnv)
      }
    })

    if (isArrayEmpty(newEnvironments)) {
      newEnvironments.push(...getDefaultEnvironments())
    }

    newEnvironments.sort(env => (env.type === 'remote' ? -1 : 1))

    setEnvironments(newEnvironments)
  }, [profile])

  useEffect(() => {
    if (
      (planState === EnumPlanState.Finished &&
        planAction === EnumPlanAction.None) ||
      openedFiles.get(activeFileId) == null
    ) {
      void refetch()
    }
  }, [planState, openedFiles])

  function closePlan(): void {
    setPlanAction(EnumPlanAction.Closing)

    void refetch()
  }

  function cancelPlan(): void {
    if (planAction === EnumPlanAction.Applying) {
      setPlanState(EnumPlanState.Canceling)
    }

    if (planAction !== EnumPlanAction.None) {
      setPlanAction(EnumPlanAction.Canceling)
    }

    fetchAPI({ url: '/api/plan/cancel', method: 'post' }).catch(console.error)

    setPlanState(EnumPlanState.Cancelled)
    setLastPlan(mostRecentPlan)

    getChannel()?.close()
    unsubscribe()

    if (planAction !== EnumPlanAction.None) {
      startPlan()
    }
  }

  function startPlan(): void {
    setActivePlan(undefined)
    setPlanState(EnumPlanState.Init)
    setPlanAction(EnumPlanAction.Opening)
  }

  function showGraph(): void {
    setIsGraphOpen(true)
  }

  function closeGraph(): void {
    setIsGraphOpen(false)
  }

  return (
    <>
      <div className="w-full flex justify-between items-center min-h-[2rem] z-50">
        <div className="px-3 flex items-center whitespace-nowrap">
          <h3 className="font-bold">
            <span className="inline-block text-secondary-500">/</span>{' '}
            {project?.name}
          </h3>
        </div>

        <div className="px-3 flex items-center min-w-[10rem] justify-end">
          <Button
            className="mr-4"
            variant="alternative"
            size={EnumSize.sm}
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              showGraph()
            }}
          >
            Graph
          </Button>
          {environment != null && (
            <RunPlan
              startPlan={startPlan}
              setProfile={setProfile}
              changes={plan?.changes}
            />
          )}
          {mostRecentPlan != null && (
            <Popover className="relative flex">
              {() => (
                <>
                  <Popover.Button
                    className={clsx(
                      'inline-block ml-1 px-2 py-[3px] rounded-[4px] text-xs font-bold',
                      planState === EnumPlanState.Finished &&
                        'bg-success-500 text-white',
                      planState === EnumPlanState.Failed &&
                        'bg-danger-500 text-white',
                      planState === EnumPlanState.Applying &&
                        'bg-secondary-500 text-white',
                      planState !== EnumPlanState.Finished &&
                        planState !== EnumPlanState.Failed &&
                        planState !== EnumPlanState.Applying &&
                        'bg-gray-100 text-gray-500',
                    )}
                  >
                    {mostRecentPlan == null ? 0 : 1}
                  </Popover.Button>
                  <Transition
                    as={Fragment}
                    enter="transition ease-out duration-200"
                    enterFrom="opacity-0 translate-y-1"
                    enterTo="opacity-100 translate-y-0"
                    leave="transition ease-in duration-150"
                    leaveFrom="opacity-100 translate-y-0"
                    leaveTo="opacity-0 translate-y-1"
                  >
                    <Popover.Panel className="absolute right-1 z-10 mt-8 transform">
                      <div className="overflow-hidden rounded-lg shadow-lg ring-1 ring-black ring-opacity-5">
                        <Tasks
                          environment={plan.environment}
                          tasks={plan.tasks}
                          updated_at={plan.updated_at}
                        />
                        <div className="my-4 px-4">
                          {planState === EnumPlanState.Applying && (
                            <Button
                              size="sm"
                              variant="danger"
                              className="mx-0"
                              onClick={(e: MouseEvent) => {
                                e.stopPropagation()

                                cancelPlan()
                              }}
                            >
                              Cancel
                            </Button>
                          )}
                        </div>
                      </div>
                    </Popover.Panel>
                  </Transition>
                </>
              )}
            </Popover>
          )}
        </div>
      </div>
      <Divider />
      <SplitPane
        sizes={[20, 80]}
        className="flex w-full h-full overflow-hidden"
      >
        <FolderTree project={project} />
        <Editor />
      </SplitPane>
      <Divider />
      <div className="px-2 py-1 text-xs">Version: 0.0.1</div>
      <ModalSetPlan
        show={isStringEmptyOrNil(environment)}
        setProfile={setProfile}
      />
      <Transition
        appear
        show={
          (planAction !== EnumPlanAction.None &&
            planAction !== EnumPlanAction.Closing) ||
          isGraphOpen
        }
        as={Fragment}
        afterLeave={() => {
          setPlanAction(EnumPlanAction.None)
        }}
      >
        <Dialog
          as="div"
          className="relative z-[100]"
          onClose={() => undefined}
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
                <Dialog.Panel className="w-full transform overflow-hidden rounded-2xl bg-white text-left align-middle shadow-xl transition-all">
                  {isGraphOpen && <Graph closeGraph={closeGraph} />}
                  {planAction !== EnumPlanAction.None &&
                    planAction !== EnumPlanAction.Closing && (
                      <Plan
                        onClose={closePlan}
                        onCancel={cancelPlan}
                      />
                    )}
                </Dialog.Panel>
              </Transition.Child>
            </div>
          </div>
        </Dialog>
      </Transition>
    </>
  )
}

function ModalSetPlan({
  show = true,
  setProfile,
}: {
  show: boolean
  setProfile: (value: Partial<Profile>) => void
}): JSX.Element {
  const environments = useStoreContext(s => s.environments)

  const [selected, setSelected] = useState(CUSTOM)
  const [customValue, setCustomValue] = useState('')

  const first3environments = environments.slice(0, 3)

  return (
    <Modal
      show={show}
      onClose={() => undefined}
    >
      <Dialog.Panel className="w-[60%] transform overflow-hidden rounded-xl bg-white text-left align-middle shadow-xl transition-all">
        <form className="h-full p-10">
          <fieldset className="mb-4">
            <h2 className="whitespace-nowrap text-xl font-bold mb-1 text-gray-900">
              Select Environment
            </h2>
            <p>
              Plan a migration of the current context&apos;s models with the
              given environment
            </p>
            <div className="flex flex-col w-full mt-3 py-1">
              <RadioGroup
                className="rounded-lg w-full flex flex-wrap"
                value={selected}
                name="environment"
                onChange={(value: string) => {
                  setSelected(value)
                }}
              >
                {first3environments.map(environment => (
                  <div
                    key={environment.name}
                    className="w-[50%] py-2 odd:pr-2 even:pl-2"
                  >
                    <RadioGroup.Option
                      value={environment.name}
                      className={({ active, checked }) =>
                        clsx(
                          'relative flex cursor-pointer rounded-md px-3 py-3 focus:outline-none border-2 border-secondary-100',
                          active &&
                            'ring-4 ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100',
                          checked
                            ? 'border-secondary-500 bg-secondary-100'
                            : 'bg-secondary-100 text-gray-900',
                        )
                      }
                    >
                      {() => (
                        <RadioGroup.Label
                          as="p"
                          className={clsx(
                            'flex items-center',
                            environment.type === 'remote'
                              ? 'text-secondary-500'
                              : 'text-gray-700',
                          )}
                        >
                          {environment.name}
                          <small className="block ml-2 text-gray-400">
                            ({environment.type})
                          </small>
                        </RadioGroup.Label>
                      )}
                    </RadioGroup.Option>
                  </div>
                ))}
                <div className="w-[50%] py-2 pl-2">
                  <RadioGroup.Option
                    value={CUSTOM}
                    className={({ active, checked }) =>
                      clsx(
                        'relative flex cursor-pointer rounded-md focus:outline-none border-2 border-secondary-100 pointer-events-auto',
                        active &&
                          'ring-4 ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100',
                        checked
                          ? 'border-secondary-500 bg-secondary-100'
                          : 'bg-secondary-100 text-gray-900',
                      )
                    }
                  >
                    <input
                      type="text"
                      name="environment"
                      className="bg-gray-100 px-3 py-3 rounded-md w-full m-0  focus:outline-none"
                      placeholder="Environment Name"
                      onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                        e.stopPropagation()

                        setCustomValue(e.target.value)
                      }}
                      value={customValue}
                    />
                  </RadioGroup.Option>
                </div>
              </RadioGroup>
              <small className="block text-xs mt-1 px-3 text-gray-500">
                Please select from suggested environments or provide custom name
              </small>
            </div>
          </fieldset>
          <fieldset>
            <div className="flex justify-end">
              <Button
                variant="primary"
                className="mx-0"
                disabled={
                  selected === CUSTOM && isStringEmptyOrNil(customValue)
                }
                onClick={(e: MouseEvent) => {
                  e.stopPropagation()
                  e.preventDefault()

                  const newEnvironment =
                    selected === CUSTOM
                      ? customValue.toLowerCase()
                      : selected.toLowerCase()
                  const hasEnvironment = environments.some(
                    env => env.name === newEnvironment,
                  )
                  const newEnvironments: Environment[] = hasEnvironment
                    ? environments
                    : [...environments, { name: newEnvironment, type: 'local' }]

                  setProfile({
                    environment: newEnvironment,
                    environments: newEnvironments.filter(
                      env => env.type === 'local',
                    ),
                  })
                }}
              >
                Apply
              </Button>
            </div>
          </fieldset>
        </form>
      </Dialog.Panel>
    </Modal>
  )
}

function RunPlan({
  startPlan,
  setProfile,
  changes,
}: {
  startPlan: () => void
  setProfile: (value: Partial<Profile>) => void
  changes: any
}): JSX.Element {
  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)

  const environments = useStoreContext(s => s.environments)
  const environment = useStoreContext(s => s.environment)

  const [selected, setSelected] = useState<string>()
  const [customEnvironment, setCustomEnvironment] = useState<string>('')

  useEffect(() => {
    setSelected(environment)
  }, [environment])

  return (
    <div className="flex items-center relative border my-1 rounded-md">
      <div>
        <Button
          className="rounded-none rounded-l-md border-r border-secondary-200 mx-0 my-0"
          disabled={
            planAction !== EnumPlanAction.None ||
            planState === EnumPlanState.Applying ||
            planState === EnumPlanState.Canceling
          }
          variant="primary"
          size={EnumSize.sm}
          onClick={(e: MouseEvent) => {
            e.stopPropagation()

            startPlan()
          }}
        >
          {planState === EnumPlanState.Applying ||
            (planState === EnumPlanState.Canceling && (
              <Spinner className="w-3 h-3 mr-1" />
            ))}
          <span className="inline-block">
            {planState === EnumPlanState.Applying
              ? 'Applying Plan...'
              : planState === EnumPlanState.Canceling
              ? 'Canceling Plan...'
              : planAction !== EnumPlanAction.None
              ? 'Setting Plan...'
              : 'Run Plan'}
          </span>
        </Button>
      </div>
      <div>
        <Menu>
          {({ open }) => (
            <>
              <ButtonMenu
                variant="primary"
                size={EnumSize.sm}
                disabled={
                  planAction !== EnumPlanAction.None ||
                  planState === EnumPlanState.Applying ||
                  planState === EnumPlanState.Canceling
                }
                className="flex rounded-none rounded-r-md border-l border-secondary-200 mx-0 my-0 py-[0.25rem]"
              >
                <span className="block overflow-hidden truncate text-gray-900">
                  {selected}
                </span>
                <span className="pointer-events-none inset-y-0 right-0 flex items-center pl-2 text-gray-900">
                  <ChevronDownIcon
                    className="h-4 w-4"
                    aria-hidden="true"
                  />
                </span>

                <span className="flex ml-1">
                  {changes == null && (
                    <span
                      title="Latest"
                      className="block h-4 ml-1 px-2 first-child:ml-0 rounded-full bg-gray-200 text-gray-900 p-[0.125rem] text-xs leading-[0.75rem] text-center"
                    >
                      latest
                    </span>
                  )}
                  {changes?.added?.length > 0 && (
                    <span
                      title="Models Added"
                      className="block w-6 h-4 ml-1 first-child:ml-0 rounded-full bg-success-500 p-[0.125rem] text-xs font-black leading-[0.75rem] text-white text-center"
                    >
                      {changes.added.length}
                    </span>
                  )}
                  {changes?.modified?.direct.length > 0 && (
                    <span
                      title="Models Modified Directly"
                      className="block w-6 h-4 ml-1 first-child:ml-0 rounded-full bg-secondary-500 p-[0.125rem] text-xs font-black leading-[0.75rem] text-white text-center"
                    >
                      {changes.modified.direct.length}
                    </span>
                  )}
                  {changes?.modified?.indirect.length > 0 && (
                    <span
                      title="Models Modified Indirectly"
                      className="block w-6 h-4 ml-1 first-child:ml-0 rounded-full bg-warning-500 p-[0.125rem] text-xs font-black leading-[0.75rem] text-white text-center"
                    >
                      {changes.modified.indirect.length}
                    </span>
                  )}
                  {changes?.removed?.length > 0 && (
                    <span
                      title="Models Removed"
                      className="block w-6 h-4 ml-1 first-child:ml-0 rounded-full bg-danged-500 p-[0.125rem] text-xs font-black leading-[0.75rem] text-white text-center"
                    >
                      {changes.removed.length}
                    </span>
                  )}
                </span>
              </ButtonMenu>
              <Transition
                as={Fragment}
                leave="transition ease-in duration-100"
                leaveFrom="opacity-100"
                leaveTo="opacity-0"
              >
                <Menu.Items className="absolute mt-3 right-0 max-h-60 overflow-auto rounded-md bg-white py-1 text-base shadow-lg">
                  {environments.map(env => (
                    <Menu.Item key={env.name}>
                      {({ active }) => (
                        <div
                          onClick={(e: MouseEvent) => {
                            e.stopPropagation()

                            setProfile({
                              environment: env.name,
                            })
                          }}
                          className={clsx(
                            'flex justify-between items-center px-4 py-1 text-gray-900 cursor-pointer',
                            active && 'bg-secondary-100',
                            env.name === selected &&
                              'pointer-events-none cursor-default',
                          )}
                        >
                          <div className="flex items-center">
                            <CheckCircleIcon
                              className={clsx(
                                'w-5 h-5 text-secondary-500',
                                active && 'opacity-10',
                                env.name !== selected && 'opacity-0',
                              )}
                            />
                            <span
                              className={clsx(
                                'block truncate ml-2',
                                env.type === 'remote'
                                  ? 'text-secondary-500'
                                  : 'text-gray-700',
                              )}
                            >
                              {env.name}
                            </span>
                            <small className="block ml-2 text-gray-400">
                              ({env.type})
                            </small>
                          </div>
                          {env.type === 'local' && env.name !== selected && (
                            <Button
                              className="my-0 mx-0"
                              size={EnumSize.xs}
                              variant="alternative"
                              onClick={(e: MouseEvent) => {
                                e.stopPropagation()

                                const newEnvironments = environments.filter(
                                  ({ name, type }) =>
                                    name !== env.name && type === 'local',
                                )

                                setProfile({
                                  environments: newEnvironments,
                                })
                              }}
                            >
                              -
                            </Button>
                          )}
                        </div>
                      )}
                    </Menu.Item>
                  ))}
                  <Menu.Item
                    as="div"
                    disabled={true}
                  >
                    <div className="flex w-full items-end px-2 py-2">
                      <Input
                        className="my-0 mx-0 mr-4 min-w-[10rem]"
                        size={EnumSize.sm}
                        placeholder="Environment"
                        value={customEnvironment}
                        onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                          e.stopPropagation()

                          setCustomEnvironment(e.target.value?.toLowerCase())
                        }}
                      />
                      <Button
                        className="my-0 mx-0"
                        size={EnumSize.sm}
                        disabled={isStringEmptyOrNil(customEnvironment)}
                        onClick={(e: MouseEvent) => {
                          e.stopPropagation()

                          const hasEnvironment = environments.some(
                            env => env.name === customEnvironment,
                          )
                          const newEnvironments: Environment[] = hasEnvironment
                            ? environments
                            : [
                                ...environments,
                                { name: customEnvironment, type: 'local' },
                              ]

                          setCustomEnvironment('')

                          setProfile({
                            environments: newEnvironments.filter(
                              env => env.type === 'local',
                            ),
                          })
                        }}
                      >
                        Add
                      </Button>
                    </div>
                  </Menu.Item>
                </Menu.Items>
              </Transition>
            </>
          )}
        </Menu>
      </div>
    </div>
  )
}
