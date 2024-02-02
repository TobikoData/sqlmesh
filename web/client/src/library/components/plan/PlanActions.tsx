import { type MouseEvent } from 'react'
import useActiveFocus from '~/hooks/useActiveFocus'
import { EnumSize, EnumVariant } from '~/types/enum'
import { isFalse } from '~/utils'
import { Button } from '../button/Button'
import { EnumPlanAction, ModelPlanAction } from '@models/plan-action'
import { useStorePlan } from '@context/plan'
import { useStoreContext } from '@context/context'
import { AddEnvironment, SelectEnvironemnt } from '~/library/pages/root/Page'
import { Transition } from '@headlessui/react'

export default function PlanActions({
  run,
  apply,
  cancel,
  reset,
}: {
  apply: () => void
  run: () => void
  cancel: () => void
  reset: () => void
}): JSX.Element {
  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)
  const addConfirmation = useStoreContext(s => s.addConfirmation)
  const setShowConfirmation = useStoreContext(s => s.setShowConfirmation)

  const planAction = useStorePlan(s => s.planAction)
  const planCancel = useStorePlan(s => s.planCancel)

  const setFocus = useActiveFocus<HTMLButtonElement>()

  function handleReset(e: MouseEvent): void {
    e.stopPropagation()

    reset()
  }

  function handleCancel(e: MouseEvent): void {
    e.stopPropagation()

    cancel()
  }

  function handleApply(e: MouseEvent): void {
    e.stopPropagation()

    if (environment.isProd && isFalse(environment.isInitial)) {
      addConfirmation({
        headline: 'Applying Plan Directly On Prod Environment!',
        description: `Are you sure you want to apply your changes directly on prod? Safer choice will be to select or add new environment first.`,
        yesText: `Yes, Run ${environment.name}`,
        noText: 'No, Cancel',
        action() {
          apply()
        },
        children: (
          <div className="mt-5 pt-4">
            <h4 className="mb-2">{`${
              environments.size > 1 ? 'Select or ' : ''
            }Add Environment`}</h4>
            <div className="flex items-center relative">
              {environments.size > 1 && (
                <SelectEnvironemnt
                  className="mr-2"
                  showAddEnvironment={false}
                  onSelect={() => {
                    setShowConfirmation(false)
                  }}
                  size={EnumSize.md}
                />
              )}
              <AddEnvironment
                className="w-full"
                size={EnumSize.md}
                onAdd={() => {
                  setShowConfirmation(false)
                }}
              />
            </div>
          </div>
        ),
      })
    } else {
      apply()
    }
  }

  function handleRun(e: MouseEvent): void {
    e.stopPropagation()

    run()
  }

  const showPlanAction =
    isFalse(planAction.isCancelling) &&
    isFalse(planAction.isDone) &&
    (planAction.isProcessing ? isFalse(planCancel.isSuccessed) : true)

  return (
    <>
      <div className="flex justify-between pt-2 pl-4 pr-2 pb-2">
        <div className="flex w-full items-center">
          <Transition
            appear
            show={showPlanAction}
            enter="transition ease duration-300 transform"
            enterFrom="opacity-0 scale-95"
            enterTo="opacity-100 scale-100"
            leave="transition ease duration-300 transform"
            leaveFrom="opacity-100 scale-100"
            leaveTo="opacity-0 scale-95"
            className="trasition-all duration-300 ease-in-out"
          >
            {showPlanAction && (
              <Button
                disabled={
                  planAction.isProcessing ||
                  planCancel.isSuccessed ||
                  planAction.isDone
                }
                onClick={
                  planAction.isRun
                    ? handleRun
                    : planCancel.isSuccessed
                    ? undefined
                    : handleApply
                }
                ref={setFocus}
                variant={
                  planCancel.isSuccessed
                    ? EnumVariant.Danger
                    : EnumVariant.Primary
                }
                autoFocus
                className="trasition-all duration-300 ease-in-out"
              >
                <span>
                  {ModelPlanAction.getActionDisplayName(
                    planAction,
                    planCancel.isSuccessed
                      ? []
                      : [
                          EnumPlanAction.RunningTask,
                          EnumPlanAction.Running,
                          EnumPlanAction.Run,
                          EnumPlanAction.Applying,
                          EnumPlanAction.ApplyBackfill,
                          EnumPlanAction.ApplyVirtual,
                          EnumPlanAction.ApplyChangesAndBackfill,
                          EnumPlanAction.ApplyMetadata,
                        ],
                    planCancel.isSuccessed ? 'Canceled' : 'Done',
                  )}
                </span>
              </Button>
            )}
          </Transition>
        </div>
        <div className="flex items-center">
          <Transition
            appear
            show={planAction.isProcessing || isFalse(planAction.isRun)}
            enter="transition ease duration-300 transform"
            enterFrom="opacity-0 scale-95"
            enterTo="opacity-100 scale-100"
            leave="transition ease duration-300 transform"
            leaveFrom="opacity-100 scale-100"
            leaveTo="opacity-0 scale-95"
            className="trasition-all duration-300 ease-in-out"
          >
            <Button
              onClick={
                planAction.isRunning || planAction.isApplying
                  ? handleCancel
                  : handleReset
              }
              variant={
                planAction.isProcessing
                  ? EnumVariant.Danger
                  : EnumVariant.Neutral
              }
              disabled={
                planAction.isCancelling ||
                (planAction.isProcessing && planCancel.isSuccessed)
              }
            >
              {ModelPlanAction.getActionDisplayName(
                planAction,
                planAction.isProcessing && isFalse(planCancel.isSuccessed)
                  ? [EnumPlanAction.Cancelling]
                  : [],
                planAction.isProcessing
                  ? planCancel.isSuccessed
                    ? 'Finishing Cancellation...'
                    : 'Cancel'
                  : 'Start Over',
              )}
            </Button>
          </Transition>
        </div>
      </div>
    </>
  )
}
