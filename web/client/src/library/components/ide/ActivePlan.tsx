import { Popover, Transition } from '@headlessui/react'
import { useQueryClient } from '@tanstack/react-query'
import clsx from 'clsx'
import { lazy, Fragment, type MouseEvent } from 'react'
import { apiCancelPlanApply } from '~/api'
import { useStoreContext } from '~/context/context'
import {
  type PlanProgress,
  type PlanState,
  useStorePlan,
  EnumPlanState,
  EnumPlanAction,
  EnumPlanApplyType,
} from '~/context/plan'
import { EnumSize, EnumVariant } from '~/types/enum'
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

    apiCancelPlanApply(client)
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
              getTriggerBgColor(planState),
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
              <div className="overflow-hidden rounded-lg bg-theme shadow-lg ring-1 ring-black ring-opacity-5">
                <TasksProgress
                  environment={environment}
                  tasks={plan.tasks}
                  updated_at={plan.updated_at}
                  headline="Most Recent Plan"
                  showBatches={plan.type !== EnumPlanApplyType.Virtual}
                  showVirtualUpdate={plan.type === EnumPlanApplyType.Virtual}
                  planState={planState}
                />
                <div className="my-4 px-4">
                  {planState === EnumPlanState.Applying && (
                    <Button
                      size={EnumSize.sm}
                      variant={EnumVariant.Danger}
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

function getTriggerBgColor(planState: PlanState): string {
  if (planState === EnumPlanState.Finished) return 'bg-success-500 text-light'
  if (planState === EnumPlanState.Failed) return 'bg-danger-500 text-light'
  if (planState === EnumPlanState.Applying) return 'bg-secondary-500 text-light'

  return 'bg-neutral-100 text-neutral-500'
}
