import { Button } from '../button/Button'
import { Divider } from '../divider/Divider'
import { Editor } from '../editor/Editor'
import { FolderTree } from '../folderTree/FolderTree'
import Tabs from '../tabs/Tabs'
import { Fragment, useEffect, MouseEvent } from 'react'
import clsx from 'clsx'
import { PlayIcon } from '@heroicons/react/24/solid'
import { EnumSize } from '../../../types/enum'
import { Transition, Dialog, Popover } from '@headlessui/react'
import { useApiFiles } from '../../../api'
import { Plan } from '../plan/Plan'
import { EnumPlanState, EnumPlanAction, useStorePlan } from '../../../context/plan'
import { Progress } from '../progress/Progress'
import Spinner from '../logo/Spinner'
import { useChannel } from '../../../api/channels'
import fetchAPI from '../../../api/instance'

export function IDE(): JSX.Element {
  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const setPlanState = useStorePlan(s => s.setState)
  const setPlanAction = useStorePlan(s => s.setAction)
  const setActivePlan = useStorePlan(s => s.setActivePlan)
  const setLastPlan = useStorePlan(s => s.setLastPlan)
  const plan = useStorePlan(s => s.lastPlan ?? s.activePlan)
  const setEnvironment = useStorePlan(s => s.setEnvironment)
  const updateTasks = useStorePlan(s => s.updateTasks)

  const [subscribe, getChannel, unsubscribe] = useChannel('/api/tasks', updateTasks)

  const { data: project } = useApiFiles()

  useEffect(() => {
    if (getChannel() == null) {
      subscribe()
    }
  }, [])

  function closePlan(): void {
    setPlanAction(EnumPlanAction.Closing)
  }

  function cancelPlan(): void {
    setPlanState(EnumPlanState.Canceling)

    if (planAction !== EnumPlanAction.None) {
      setPlanAction(EnumPlanAction.Canceling)
    }

    fetchAPI({ url: '/api/plan/cancel', method: 'post' }).catch(console.error)

    setPlanState(EnumPlanState.Cancelled)
    setLastPlan(plan)

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
    setEnvironment(undefined)
  }

  return (
    <>
      <div className="w-full flex justify-between items-center min-h-[2rem] z-50">
        <div className="px-3 flex items-center whitespace-nowrap">
          <h3 className="font-bold">
            <span className="inline-block text-secondary-500">/</span> {project?.name}
          </h3>
        </div>

        <div className="flex w-full justify-center">
          <ul className="flex w-full items-center justify-center">
            {['Editor', 'Graph', 'Audits', 'Tests'].map((name, i) => (
              <li key={name}>
                <div
                  className={clsx(
                    'mx-2 text-sm opacity-85 flex',
                    name === 'Editor' &&
                      'font-bold opacity-100 border-b-2 border-secondary-500 text-secondary-500 cursor-default',
                    ['Audits', 'Graph', 'Tests'].includes(name) && 'opacity-25 cursor-not-allowed'
                  )}
                >
                  {i > 0 && (
                    <Divider
                      orientation="vertical"
                      className="h-3 mx-2"
                    />
                  )}
                  {name}
                </div>
              </li>
            ))}
          </ul>
        </div>

        <div className="px-3 flex items-center min-w-[10rem] justify-end">
          <Button
            disabled={
              planAction !== EnumPlanAction.None ||
              planState === EnumPlanState.Applying ||
              planState === EnumPlanState.Canceling
            }
            variant="primary"
            size={EnumSize.sm}
            onClick={e => {
              e.stopPropagation()

              startPlan()
            }}
            className="min-w-[6rem] justify-between"
          >
            {planState === EnumPlanState.Applying ||
              (planState === EnumPlanState.Canceling && <Spinner className="w-3 h-3 mr-1" />)}
            <span className="inline-block mr-3 min-w-20">
              {planState === EnumPlanState.Applying
                ? 'Applying Plan...'
                : planState === EnumPlanState.Canceling
                ? 'Canceling Plan...'
                : planAction !== EnumPlanAction.None
                ? 'Setting Plan...'
                : 'Run Plan'}
            </span>
            <PlayIcon className="w-[1rem] h-[1rem] text-inherit" />
          </Button>
          {plan != null && (
            <Popover className="relative flex">
              {() => (
                <>
                  <Popover.Button
                    className={clsx(
                      'inline-block ml-1 px-2 py-[3px] rounded-[4px] text-xs font-bold',
                      planState === EnumPlanState.Finished && 'bg-success-500 text-white',
                      planState === EnumPlanState.Failed && 'bg-danger-500 text-white',
                      planState === EnumPlanState.Applying && 'bg-secondary-500 text-white',
                      planState !== EnumPlanState.Finished &&
                        planState !== EnumPlanState.Failed &&
                        planState !== EnumPlanState.Applying &&
                        'bg-gray-100 text-gray-500'
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
                    <Popover.Panel className="absolute right-1 z-10 mt-3 transform">
                      <div className="overflow-hidden rounded-lg shadow-lg ring-1 ring-black ring-opacity-5">
                        <div className="relative grid gap-8 py-3 bg-white">
                          <div
                            key={plan.environment}
                            className="mx-4"
                          >
                            <div className="flex justify-between items-baseline">
                              <small className="block whitespace-nowrap text-sm font-medium text-gray-900">
                                Environemnt: {plan.environment}
                              </small>
                              <small className="block whitespace-nowrap text-xs font-medium text-gray-900">
                                {
                                  Object.values(plan.tasks).filter(
                                    (t: any) => t.completed === t.total
                                  ).length
                                }{' '}
                                of {Object.values(plan.tasks).length}
                              </small>
                            </div>
                            <Progress
                              progress={Math.ceil(
                                (Object.values(plan.tasks).filter(
                                  (t: any) => t.completed === t.total
                                ).length /
                                  Object.values(plan.tasks).length) *
                                  100
                              )}
                            />
                            <div className="my-4 px-4 py-2 bg-secondary-100 rounded-lg">
                              {Object.entries(plan.tasks).map(([model_name, task]: any) => (
                                <div key={model_name}>
                                  <div className="flex justify-between items-baselin">
                                    <small className="text-xs block whitespace-nowrap font-medium text-gray-900 mr-6">
                                      {model_name}
                                    </small>
                                    <small className="block whitespace-nowrap text-xs font-medium text-gray-900">
                                      {task.completed} of {task.total}
                                    </small>
                                  </div>
                                  <Progress
                                    progress={Math.ceil((task.completed / task.total) * 100)}
                                  />
                                </div>
                              ))}
                            </div>
                            <div className="flex justify-end items-center px-2">
                              <div className="w-full">
                                <small className="text-xs">
                                  <b>Last Update:</b> {new Date(plan.updated_at).toDateString()}
                                </small>
                              </div>
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
      <div className="flex w-full h-full overflow-hidden">
        <div className="w-[16rem] overflow-hidden overflow-y-auto">
          <FolderTree project={project} />
        </div>
        <Divider orientation="vertical" />
        <div className={clsx('h-full w-full flex flex-col overflow-hidden')}>
          <div className="w-full h-full flex overflow-hidden">
            <Editor />
          </div>
          <Divider />
          <div className="w-full min-h-[10rem] overflow-auto">
            <Tabs />
          </div>
        </div>
      </div>
      <Divider />
      <div className="p-1">ide footer</div>
      <Transition
        appear
        show={planAction !== EnumPlanAction.None && planAction !== EnumPlanAction.Closing}
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
                  <Plan
                    onClose={closePlan}
                    onCancel={cancelPlan}
                  />
                </Dialog.Panel>
              </Transition.Child>
            </div>
          </div>
        </Dialog>
      </Transition>
    </>
  )
}
