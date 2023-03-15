import { Popover, Transition } from '@headlessui/react'
import { useQueryClient } from '@tanstack/react-query'
import clsx from 'clsx'
import { lazy, Fragment, MouseEvent } from 'react'
import { apiCancelPlanApplyAndRun } from '~/api'
import { useStoreContext } from '~/context/context'
import {
  PlanProgress,
  useStorePlan,
  EnumPlanState,
  EnumPlanAction,
} from '~/context/plan'
import { Button } from '../button/Button'

const TasksProgress = lazy(
  async () => await import('../tasksProgress/TasksProgress'),
)

export default function ActivePlan({
  plan,
}: {
  plan: PlanProgress
}): JSX.Element {
  const client = useQueryClient()

  const environment = useStoreContext(s => s.environment)

  const planState = useStorePlan(s => s.state)
  const setPlanState = useStorePlan(s => s.setState)
  const setPlanAction = useStorePlan(s => s.setAction)

  function cancel(): void {
    setPlanState(EnumPlanState.Cancelling)
    setPlanAction(EnumPlanAction.Cancelling)

    void apiCancelPlanApplyAndRun(client, environment.name)
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
                <TasksProgress
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
