import { type MouseEvent } from 'react'
import useActiveFocus from '~/hooks/useActiveFocus'
import { EnumVariant } from '~/types/enum'
import { includes, isFalse } from '~/utils'
import { Button } from '../button/Button'
import PlanActionsDescription from './PlanActionsDescription'
import { EnumPlanAction, ModelPlanAction } from '@models/plan-action'

export default function PlanActions({
  run,
  apply,
  cancel,
  reset,
  close,
  planAction,
}: {
  apply: () => void
  run: () => void
  cancel: () => void
  close: () => void
  reset: () => void
  planAction: ModelPlanAction
}): JSX.Element {
  const setFocus = useActiveFocus<HTMLButtonElement>()

  function handleClose(e: MouseEvent): void {
    e.stopPropagation()

    close()
  }

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

    apply()
  }

  function handleRun(e: MouseEvent): void {
    e.stopPropagation()

    run()
  }

  return (
    <>
      {isFalse(planAction.isDone) && <PlanActionsDescription />}
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
          <Button
            onClick={handleClose}
            variant={
              planAction.isDone ? EnumVariant.Primary : EnumVariant.Neutral
            }
            ref={
              planAction.isDone || planAction.isApplying ? setFocus : undefined
            }
          >
            {ModelPlanAction.getActionDisplayName(
              planAction,
              [EnumPlanAction.Done],
              'Close',
            )}
          </Button>
        </div>
      </div>
    </>
  )
}
