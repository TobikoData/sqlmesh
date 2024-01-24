import { type MouseEvent } from 'react'
import useActiveFocus from '~/hooks/useActiveFocus'
import { EnumSize, EnumVariant } from '~/types/enum'
import { includes, isFalse } from '~/utils'
import { Button } from '../button/Button'
import { EnumPlanAction, ModelPlanAction } from '@models/plan-action'
import { useStorePlan } from '@context/plan'
import { useStoreContext } from '@context/context'
import { AddEnvironment, SelectEnvironemnt } from '~/library/pages/root/Page'

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

  return (
    <>
      <div className="flex justify-between px-4 pb-2">
        <div className="flex w-full items-center">
          {(planAction.isRun || planAction.isRunning) && (
            <Button
              disabled={planAction.isRunning}
              onClick={handleRun}
              ref={setFocus}
              variant={EnumVariant.Primary}
              autoFocus
            >
              <span>
                {ModelPlanAction.getActionDisplayName(planAction, [
                  EnumPlanAction.RunningTask,
                  EnumPlanAction.Running,
                  EnumPlanAction.Run,
                ])}
              </span>
            </Button>
          )}
          {(planAction.isApply || planAction.isApplying) && (
            <Button
              onClick={handleApply}
              disabled={planAction.isApplying}
              ref={setFocus}
              variant={EnumVariant.Primary}
            >
              {ModelPlanAction.getActionDisplayName(
                planAction,
                [
                  EnumPlanAction.Applying,
                  EnumPlanAction.ApplyBackfill,
                  EnumPlanAction.ApplyVirtual,
                  EnumPlanAction.ApplyChangesAndBackfill,
                  EnumPlanAction.ApplyMetadata,
                ],
                'Apply',
              )}
            </Button>
          )}
          {planAction.isProcessing && (
            <Button
              onClick={handleCancel}
              variant={EnumVariant.Danger}
              className="justify-self-end"
              disabled={planAction.isCancelling}
            >
              {ModelPlanAction.getActionDisplayName(
                planAction,
                [EnumPlanAction.Cancelling],
                'Cancel',
              )}
            </Button>
          )}
        </div>
        <div className="flex items-center">
          {[planAction.isProcessing, planAction.isRun].every(isFalse) && (
            <Button
              onClick={handleReset}
              variant={EnumVariant.Neutral}
              disabled={includes(
                [
                  EnumPlanAction.Running,
                  EnumPlanAction.Applying,
                  EnumPlanAction.Cancelling,
                ],
                planAction.value,
              )}
            >
              {ModelPlanAction.getActionDisplayName(
                planAction,
                [],
                'Start Over',
              )}
            </Button>
          )}
        </div>
      </div>
    </>
  )
}
