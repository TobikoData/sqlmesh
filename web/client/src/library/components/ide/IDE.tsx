import { Button, ButtonMenu } from '../button/Button'
import { Divider } from '../divider/Divider'
import { Editor } from '../editor/Editor'
import FolderTree from '../folderTree/FolderTree'
import { Fragment, useEffect, MouseEvent, useState, lazy } from 'react'
import clsx from 'clsx'
import { ChevronDownIcon, CheckCircleIcon } from '@heroicons/react/24/solid'
import { EnumSize } from '../../../types/enum'
import { Transition, Dialog, Popover, Menu } from '@headlessui/react'
import {
  useApiPlanRun,
  useApiFiles,
  useApiEnvironments,
  apiCancelPlanApplyAndRun,
} from '../../../api'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
  PlanProgress,
} from '../../../context/plan'
import { useChannelEvents } from '../../../api/channels'
import SplitPane from '../splitPane/SplitPane'
import {
  includes,
  isArrayEmpty,
  isArrayNotEmpty,
  isFalse,
  isStringEmptyOrNil,
  isTrue,
} from '~/utils'
import Input from '../input/Input'
import {
  EnumRelativeLocation,
  EnvironmentShort,
  EnvironmentName,
  useStoreContext,
} from '~/context/context'
import Spinner from '../logo/Spinner'
import Modal from '../modal/Modal'
import PlanProvider from '../plan/context'
import { useQueryClient } from '@tanstack/react-query'

const Plan = lazy(async () => await import('../plan/Plan'))
const Graph = lazy(async () => await import('../graph/Graph'))
const Tasks = lazy(async () => await import('../tasksProgress/TasksProgress'))

export interface Profile {
  environment: string
  environments: EnvironmentShort[]
}

export function IDE(): JSX.Element {
  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)
  const initialStartDate = useStoreContext(s => s.initialStartDate)
  const initialEndDate = useStoreContext(s => s.initialEndDate)
  const addRemoteEnvironments = useStoreContext(s => s.addRemoteEnvironments)

  const activePlan = useStorePlan(s => s.activePlan)
  const setPlanAction = useStorePlan(s => s.setAction)
  const updateTasks = useStorePlan(s => s.updateTasks)

  const [isGraphOpen, setIsGraphOpen] = useState(false)
  const [isPlanOpen, setIsPlanOpen] = useState(false)
  const [isClosingModal, setIsClosingModal] = useState(false)

  const [subscribe] = useChannelEvents(updateTasks)

  const { data: project } = useApiFiles()

  useEffect(() => {
    void getEnvironments()
    const unsubscribe = subscribe('tasks')

    return () => {
      unsubscribe?.()
    }
  }, [])

  const { refetch: getEnvironments, data: contextEnvironemnts } =
    useApiEnvironments()

  useEffect(() => {
    if (
      contextEnvironemnts == null ||
      isArrayEmpty(Object.keys(contextEnvironemnts))
    )
      return

    addRemoteEnvironments(Object.values(contextEnvironemnts))
  }, [contextEnvironemnts])

  function showGraph(): void {
    setIsGraphOpen(true)
  }

  function closeGraph(): void {
    setIsGraphOpen(false)
  }

  function showRunPlan(): void {
    setIsPlanOpen(true)
  }

  function closeRunPlan(): void {
    setIsClosingModal(true)
    setIsPlanOpen(false)
  }

  const activeEnvironemnt = environments.find(e => e.name === environment)

  return (
    <>
      <div className="w-full flex justify-between items-center min-h-[2rem] z-50">
        <div className="px-3 flex items-center whitespace-nowrap">
          <h3 className="font-bold">
            <span className="inline-block text-secondary-500">/</span>
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
              environment={environment}
              showRunPlan={showRunPlan}
            />
          )}
          {environment != null && activePlan != null && (
            <ActivePlan
              environment={environment}
              plan={activePlan}
            />
          )}
        </div>
      </div>
      <Divider />
      {environment != null ? (
        <SplitPane
          sizes={[20, 80]}
          minSize={[160]}
          maxSize={[320]}
          snapOffset={0}
          className="flex w-full h-full overflow-hidden"
        >
          <FolderTree project={project} />
          <Editor environment={environment} />
        </SplitPane>
      ) : (
        <div className="flex w-full h-full bg-secondary-900"></div>
      )}
      <Divider />
      <div className="px-2 py-1 text-xs">Version: 0.0.1</div>
      <Modal show={isGraphOpen}>
        <Dialog.Panel className="w-full transform overflow-hidden rounded-2xl bg-white text-left align-middle shadow-xl transition-all">
          <Graph closeGraph={closeGraph} />
        </Dialog.Panel>
      </Modal>
      {environment != null && (
        <Modal
          show={isPlanOpen}
          afterLeave={() => {
            setPlanAction(EnumPlanAction.None)
            setIsClosingModal(false)
          }}
        >
          <Dialog.Panel className="w-full transform overflow-hidden rounded-2xl bg-white text-left align-middle shadow-xl transition-all">
            <PlanProvider>
              <Plan
                environment={environment}
                isInitialPlanRun={
                  activeEnvironemnt?.isInitial == null ||
                  isTrue(activeEnvironemnt?.isInitial)
                }
                disabled={isClosingModal}
                initialStartDate={initialStartDate}
                initialEndDate={initialEndDate}
                onClose={closeRunPlan}
              />
            </PlanProvider>
          </Dialog.Panel>
        </Modal>
      )}
    </>
  )
}

function ActivePlan({
  environment,
  plan,
}: {
  environment: EnvironmentName
  plan: PlanProgress
}): JSX.Element {
  const client = useQueryClient()

  const planState = useStorePlan(s => s.state)
  const setPlanState = useStorePlan(s => s.setState)
  const setPlanAction = useStorePlan(s => s.setAction)

  function cancel(): void {
    setPlanState(EnumPlanState.Cancelling)
    setPlanAction(EnumPlanAction.Cancelling)

    void apiCancelPlanApplyAndRun(client, environment)
      .catch(console.error)
      .finally(() => {
        setPlanAction(EnumPlanAction.None)
        setPlanState(EnumPlanState.Cancelled)
      })
  }

  return (
    <Popover className="relative flex">
      {() => (
        <>
          <Popover.Button
            className={clsx(
              'inline-block ml-1 px-2 py-[3px] rounded-[4px] text-xs font-bold',
              planState === EnumPlanState.Finished &&
                'bg-success-500 text-white',
              planState === EnumPlanState.Failed && 'bg-danger-500 text-white',
              planState === EnumPlanState.Applying &&
                'bg-secondary-500 text-white',
              planState !== EnumPlanState.Finished &&
                planState !== EnumPlanState.Failed &&
                planState !== EnumPlanState.Applying &&
                'bg-gray-100 text-gray-500',
            )}
          >
            {plan == null ? 0 : 1}
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
                  environment={environment}
                  tasks={plan.tasks}
                  updated_at={plan.updated_at}
                  headline="Most Recent Plan"
                  showBatches={plan.type !== 'logical'}
                  showLogicalUpdate={plan.type === 'logical'}
                  planState={planState}
                />
                <div className="my-4 px-4">
                  {planState === EnumPlanState.Applying && (
                    <Button
                      size="sm"
                      variant="danger"
                      className="mx-0"
                      onClick={(e: MouseEvent) => {
                        e.stopPropagation()

                        cancel()
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
  )
}

function RunPlan({
  showRunPlan,
  environment,
}: {
  showRunPlan: () => void
  environment: EnvironmentName
}): JSX.Element {
  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const setPlanState = useStorePlan(s => s.setState)
  const setPlanAction = useStorePlan(s => s.setAction)
  const setActivePlan = useStorePlan(s => s.setActivePlan)

  const environments = useStoreContext(s => s.environments)
  const setInitialDates = useStoreContext(s => s.setInitialDates)
  const isExistingEnvironment = useStoreContext(s => s.isExistingEnvironment)
  const setEnvironment = useStoreContext(s => s.setEnvironment)
  const addLocalEnvironments = useStoreContext(s => s.addLocalEnvironments)
  const removeLocalEnvironments = useStoreContext(
    s => s.removeLocalEnvironments,
  )

  const [customEnvironment, setCustomEnvironment] = useState<string>('')

  const { refetch: getEnvironments } = useApiEnvironments()
  const { refetch: planRun, data: plan, isLoading } = useApiPlanRun(environment)

  useEffect(() => {
    void planRun()
  }, [environment])

  useEffect(() => {
    if (planState === EnumPlanState.Finished) {
      void planRun()
      void getEnvironments()
    }
  }, [planState])

  useEffect(() => {
    setInitialDates(plan?.start, plan?.end)
  }, [plan])

  function startPlan(): void {
    setActivePlan(undefined)
    setPlanState(EnumPlanState.Init)
    setPlanAction(EnumPlanAction.Run)
    showRunPlan()
  }

  const hasChanges =
    isArrayNotEmpty(plan?.changes?.added) ||
    isArrayNotEmpty(plan?.changes?.removed) ||
    isArrayNotEmpty(plan?.changes?.modified?.direct) ||
    isArrayNotEmpty(plan?.changes?.modified?.indirect) ||
    isArrayNotEmpty(plan?.changes?.modified?.metadata)

  return (
    <div
      className={clsx(
        'flex items-center relative border my-1 rounded-md',
        environment == null &&
          'opacity-50 pointer-events-none cursor-not-allowed',
      )}
    >
      <div>
        <Button
          className="rounded-none rounded-l-md border-r border-secondary-200 mx-0 my-0"
          disabled={
            isLoading ||
            planAction !== EnumPlanAction.None ||
            planState === EnumPlanState.Applying ||
            planState === EnumPlanState.Running ||
            planState === EnumPlanState.Cancelling
          }
          variant="primary"
          size={EnumSize.sm}
          onClick={(e: MouseEvent) => {
            e.stopPropagation()

            startPlan()
          }}
        >
          {includes(
            [
              EnumPlanState.Applying,
              EnumPlanState.Running,
              EnumPlanState.Cancelling,
            ],
            planState,
          ) && <Spinner className="w-3 h-3 mr-1" />}
          <span className="inline-block">
            {planState === EnumPlanState.Running
              ? 'Running Plan...'
              : planState === EnumPlanState.Applying
              ? 'Applying Plan...'
              : planState === EnumPlanState.Cancelling
              ? 'Cancelling Plan...'
              : planAction !== EnumPlanAction.None
              ? 'Setting Plan...'
              : 'Run Plan'}
          </span>
        </Button>
      </div>
      {environment != null && (
        <div>
          <Menu>
            {() => (
              <>
                <ButtonMenu
                  variant="primary"
                  size={EnumSize.sm}
                  disabled={
                    isLoading ||
                    planAction !== EnumPlanAction.None ||
                    planState === EnumPlanState.Applying ||
                    planState === EnumPlanState.Cancelling
                  }
                  className="flex rounded-none rounded-r-md border-l border-secondary-200 mx-0 my-0 py-[0.25rem]"
                >
                  <span className="block overflow-hidden truncate text-gray-900">
                    {environment}
                  </span>
                  <span className="pointer-events-none inset-y-0 right-0 flex items-center pl-2 text-gray-900">
                    <ChevronDownIcon
                      className="h-4 w-4"
                      aria-hidden="true"
                    />
                  </span>
                  <span className="flex ml-1">
                    {isLoading && (
                      <span className="flex items-center ml-2">
                        <Spinner className="w-3 h-3 mr-1" />
                        <span className="inline-block ">Checking...</span>
                      </span>
                    )}
                    {isFalse(hasChanges) && isFalse(isLoading) && (
                      <span
                        title="Latest"
                        className="block h-4 ml-1 px-2 first-child:ml-0 rounded-full bg-gray-200 text-gray-900 p-[0.125rem] text-xs leading-[0.75rem] text-center"
                      >
                        latest
                      </span>
                    )}
                    {isArrayNotEmpty(plan?.changes?.added) && (
                      <span
                        title="Models Added"
                        className="block w-6 h-4 ml-1 first-child:ml-0 rounded-full bg-success-500 p-[0.125rem] text-xs font-black leading-[0.75rem] text-white text-center"
                      >
                        {plan?.changes?.added.length}
                      </span>
                    )}
                    {isArrayNotEmpty(plan?.changes?.modified?.direct) && (
                      <span
                        title="Models Modified Directly"
                        className="block w-6 h-4 ml-1 first-child:ml-0 rounded-full bg-secondary-500 p-[0.125rem] text-xs font-black leading-[0.75rem] text-white text-center"
                      >
                        {plan?.changes?.modified.direct.length}
                      </span>
                    )}
                    {isArrayNotEmpty(plan?.changes?.modified?.indirect) && (
                      <span
                        title="Models Modified Indirectly"
                        className="block w-6 h-4 ml-1 first-child:ml-0 rounded-full bg-warning-500 p-[0.125rem] text-xs font-black leading-[0.75rem] text-white text-center"
                      >
                        {plan?.changes?.modified.indirect.length}
                      </span>
                    )}
                    {isArrayNotEmpty(plan?.changes?.removed) && (
                      <span
                        title="Models Removed"
                        className="block w-6 h-4 ml-1 first-child:ml-0 rounded-full bg-danged-500 p-[0.125rem] text-xs font-black leading-[0.75rem] text-white text-center"
                      >
                        {plan?.changes?.removed.length}
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
                  <div className="absolute right-0 overflow-hidden mt-2 shadow-lg bg-white rounded-md flex flex-col">
                    <Menu.Items className="overflow-auto max-h-80  py-2 scrollbar scrollbar--vertical">
                      {environments.map(env => (
                        <Menu.Item key={env.name}>
                          {({ active }) => (
                            <div
                              onClick={(e: MouseEvent) => {
                                e.stopPropagation()

                                setEnvironment(env.name)
                              }}
                              className={clsx(
                                'flex justify-between items-center px-4 py-1 text-gray-900 cursor-pointer overflow-auto',
                                active && 'bg-secondary-100',
                                env.name === environment &&
                                  'pointer-events-none cursor-default',
                              )}
                            >
                              <div className="flex items-center">
                                <CheckCircleIcon
                                  className={clsx(
                                    'w-5 h-5 text-secondary-500',
                                    active && 'opacity-10',
                                    env.name !== environment && 'opacity-0',
                                  )}
                                />
                                <span
                                  className={clsx(
                                    'block truncate ml-2',
                                    env.type === EnumRelativeLocation.Remote
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
                              {env.type === EnumRelativeLocation.Local &&
                                env.name !== environment && (
                                  <Button
                                    className="my-0 mx-0"
                                    size={EnumSize.xs}
                                    variant="alternative"
                                    onClick={(e: MouseEvent) => {
                                      e.stopPropagation()

                                      removeLocalEnvironments([env.name])
                                    }}
                                  >
                                    -
                                  </Button>
                                )}
                            </div>
                          )}
                        </Menu.Item>
                      ))}
                      <Divider />
                      <div className="flex w-full items-end px-2 pt-2">
                        <Input
                          className="my-0 mx-0 mr-4 min-w-[10rem]"
                          size={EnumSize.sm}
                          placeholder="Environment"
                          value={customEnvironment}
                          onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                            e.stopPropagation()

                            setCustomEnvironment(e.target.value)
                          }}
                        />
                        <Button
                          className="my-0 mx-0"
                          size={EnumSize.sm}
                          disabled={
                            isStringEmptyOrNil(customEnvironment) ||
                            isExistingEnvironment(customEnvironment)
                          }
                          onClick={(e: MouseEvent) => {
                            e.stopPropagation()

                            setCustomEnvironment('')

                            addLocalEnvironments([customEnvironment])
                          }}
                        >
                          Add
                        </Button>
                      </div>
                    </Menu.Items>
                    <Divider />
                  </div>
                </Transition>
              </>
            )}
          </Menu>
        </div>
      )}
    </div>
  )
}
