import { Button } from '../button/Button'
import { Divider } from '../divider/Divider'
import { Editor } from '../editor/Editor'
import { FolderTree } from '../folderTree/FolderTree'
import { Fragment, useEffect, MouseEvent, useState, lazy } from 'react'
import clsx from 'clsx'
import { PlayIcon } from '@heroicons/react/24/solid'
import { EnumSize } from '../../../types/enum'
import { Transition, Dialog, Popover } from '@headlessui/react'
import { useApiFiles } from '../../../api'
import fetchAPI from '../../../api/instance'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
} from '../../../context/plan'
import { useChannel } from '../../../api/channels'
import SplitPane from '../splitPane/SplitPane'

const Plan = lazy(async () => await import('../plan/Plan'))
const Graph = lazy(async () => await import('../graph/Graph'))
const Spinner = lazy(async () => await import('../logo/Spinner'))
const Tasks = lazy(async () => await import('../plan/Tasks'))

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

  const [isGraphOpen, setIsGraphOpen] = useState(false)

  const [subscribe, getChannel, unsubscribe] = useChannel(
    '/api/tasks',
    updateTasks,
  )

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
    if (planAction === EnumPlanAction.Applying) {
      setPlanState(EnumPlanState.Canceling)
    }

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
            variant="alternative"
            size={EnumSize.sm}
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              showGraph()
            }}
          >
            Graph
          </Button>
          <Button
            className="min-w-[6rem] justify-between"
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
