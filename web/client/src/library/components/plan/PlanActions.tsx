import { type MouseEvent } from 'react'
import useActiveFocus from '~/hooks/useActiveFocus'
import { EnumSize, EnumVariant } from '~/types/enum'
import { isFalse, isNil } from '~/utils'
import { Button } from '../button/Button'
import { EnumPlanAction, ModelPlanAction } from '@models/plan-action'
import { useStorePlan } from '@context/plan'
import { useStoreContext } from '@context/context'
import { Transition } from '@headlessui/react'
import { useNavigate } from 'react-router'
import { SelectEnvironment } from '@components/environmentDetails/SelectEnvironment'
import { AddEnvironment } from '@components/environmentDetails/AddEnvironment'
import { usePlan } from './context'

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
  const navigate = useNavigate()
  const { change_categorization } = usePlan()

  const modules = useStoreContext(s => s.modules)
  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)
  const addConfirmation = useStoreContext(s => s.addConfirmation)
  const setShowConfirmation = useStoreContext(s => s.setShowConfirmation)

  const planAction = useStorePlan(s => s.planAction)
  const planOverview = useStorePlan(s => s.planOverview)
  const planApply = useStorePlan(s => s.planApply)
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

  function handleGoBack(e: MouseEvent): void {
    e.stopPropagation()

    navigate(-1)
  }

  function handleApply(e: MouseEvent): void {
    e.stopPropagation()

    const isProd = environment.isProd && isFalse(environment.isInitial)
    const hasUncategorized = Array.from(change_categorization.values()).some(
      c => isNil(c.category),
    )

    if (isProd) {
      addConfirmation({
        headline: 'Applying Plan Directly On Prod Environment!',
        tagline: 'Safer choice will be to select or add new environment first.',
        description:
          'Are you sure you want to apply your changes directly on prod?',
        yesText: `Yes, Run ${environment.name}`,
        noText: 'No, Cancel',
        action: apply,
        details: hasUncategorized
          ? [
              'ATTENTION!',
              '[Breaking Change] category will be applied to all uncategorized changes',
            ]
          : undefined,
        children: (
          <div className="mt-5 pt-4">
            <h4 className="mb-2">{`${
              environments.size > 1 ? 'Select or ' : ''
            }Add Environment`}</h4>
            <div className="flex items-center relative">
              {environments.size > 1 && (
                <SelectEnvironment
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
      if (hasUncategorized) {
        addConfirmation({
          headline: 'Some changes are missing categorization!',
          description: 'Are you sure you want to proceed?',
          details: [
            '[Breaking Change] category will be applied to all uncategorized changes',
          ],
          yesText: 'Yes, Apply',
          noText: 'No, Cancel',
          action: apply,
        })
      } else {
        apply()
      }
    }
  }

  function handleRun(e: MouseEvent): void {
    e.stopPropagation()

    run()
  }

  const isFailedOrCanceled =
    planOverview.isFailed || planApply.isFailed || planCancel.isSuccessful
  const showPlanActionButton =
    isFalse(planAction.isCancelling) &&
    isFalse(planAction.isDone) &&
    (planAction.isProcessing ? isFalse(planCancel.isSuccessful) : true) &&
    isFalse(isFailedOrCanceled)
  const showCancelButton =
    planAction.isApplying ||
    planCancel.isCancelling ||
    (planApply.isRunning && planOverview.isFinished)
  const showResetButton =
    isFailedOrCanceled ||
    (isFalse(planAction.isProcessing) &&
      isFalse(planAction.isRun) &&
      isFalse(planAction.isDone))
  const showBackButton = modules.showHistoryNavigation

  return (
    <>
      <div className="flex justify-between pt-2 pl-4 pr-2 pb-2">
        <div className="flex w-full items-center">
          <Transition
            appear
            show={showPlanActionButton}
            enter="transition ease duration-300 transform"
            enterFrom="opacity-0 scale-95"
            enterTo="opacity-100 scale-100"
            leave="transition ease duration-300 transform"
            leaveFrom="opacity-100 scale-100"
            leaveTo="opacity-0 scale-95"
            className="trasition-all duration-300 ease-in-out"
            as="div"
          >
            {showPlanActionButton && (
              <Button
                disabled={
                  planAction.isProcessing ||
                  planCancel.isSuccessful ||
                  planAction.isDone
                }
                onClick={
                  planAction.isRun
                    ? handleRun
                    : planCancel.isSuccessful
                      ? undefined
                      : handleApply
                }
                ref={setFocus}
                variant={
                  planCancel.isSuccessful
                    ? EnumVariant.Danger
                    : EnumVariant.Primary
                }
                autoFocus
                className="trasition-all duration-300 ease-in-out"
              >
                <span>
                  {ModelPlanAction.getActionDisplayName(
                    planAction,
                    planCancel.isSuccessful
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
                    planCancel.isSuccessful ? 'Canceled' : 'Done',
                  )}
                </span>
              </Button>
            )}
          </Transition>
        </div>
        <div className="flex items-center">
          {showCancelButton && (
            <Button
              onClick={handleCancel}
              variant={EnumVariant.Danger}
              disabled={
                planAction.isCancelling ||
                (planAction.isProcessing && planCancel.isSuccessful)
              }
            >
              {ModelPlanAction.getActionDisplayName(
                planAction,
                planAction.isProcessing && isFalse(planCancel.isSuccessful)
                  ? [EnumPlanAction.Cancelling]
                  : [],
                planCancel.isSuccessful
                  ? 'Finishing Cancellation...'
                  : 'Cancel',
              )}
            </Button>
          )}
          {showResetButton && (
            <Button
              onClick={handleReset}
              variant={EnumVariant.Info}
              disabled={
                planAction.isCancelling ||
                (planAction.isProcessing && planCancel.isSuccessful)
              }
            >
              Start Over
            </Button>
          )}
          {showBackButton && (
            <Button
              onClick={handleGoBack}
              variant={EnumVariant.Info}
            >
              Go Back
            </Button>
          )}
        </div>
      </div>
    </>
  )
}
