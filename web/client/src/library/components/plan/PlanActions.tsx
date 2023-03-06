import { MouseEvent } from 'react'
import { EnvironmentName } from '~/context/context'
import { PlanAction, EnumPlanAction } from '~/context/plan'
import useActiveFocus from '~/hooks/useActiveFocus'
import { includes, isFalse } from '~/utils'
import { Button } from '../button/Button'
import { getActionName } from './help'

interface PropsPlanActions {
  environment: EnvironmentName
  planAction: PlanAction
  shouldApplyWithBackfill: boolean
  disabled: boolean
  apply: () => void
  run: () => void
  cancel: () => void
  close: () => void
  reset: () => void
}

export default function PlanActions({
  environment,
  planAction,
  shouldApplyWithBackfill,
  disabled,
  run,
  apply,
  cancel,
  close,
  reset,
}: PropsPlanActions): JSX.Element {
  const setFocus = useActiveFocus<HTMLButtonElement>()

  const isRun = planAction === EnumPlanAction.Run
  const isDone = planAction === EnumPlanAction.Done
  const isCanceling = planAction === EnumPlanAction.Cancelling
  const isApply = planAction === EnumPlanAction.Apply
  const isApplying = planAction === EnumPlanAction.Applying
  const isRunning = planAction === EnumPlanAction.Running
  const isProcessing = isRunning || isApplying || isCanceling

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
    <div className="flex justify-between px-4 py-2">
      <div className="flex w-full items-center">
        {(isRun || isRunning) && (
          <>
            <Button
              disabled={isRunning || disabled}
              onClick={handleRun}
              autoFocus
              ref={setFocus}
            >
              {getActionName(planAction, [
                EnumPlanAction.Running,
                EnumPlanAction.Run,
              ])}
            </Button>
            {isRun && (
              <p className="ml-2 text-gray-600">
                <small>Plan for</small>
                <b className="text-secondary-500 font-bold mx-1">
                  {environment}
                </b>
                <small>Environment</small>
              </p>
            )}
          </>
        )}

        {(isApply || isApplying) && (
          <Button
            onClick={handleApply}
            disabled={isApplying || disabled}
            ref={setFocus}
          >
            {getActionName(
              planAction,
              [EnumPlanAction.Applying],
              shouldApplyWithBackfill ? 'Apply And Backfill' : 'Apply',
            )}
          </Button>
        )}

        {isProcessing && (
          <Button
            onClick={handleCancel}
            variant="danger"
            className="justify-self-end"
            disabled={isCanceling || disabled}
          >
            {getActionName(planAction, [EnumPlanAction.Cancelling], 'Cancel')}
          </Button>
        )}
      </div>
      <div className="flex items-center">
        {isFalse(isProcessing) && isFalse(isRun) && isFalse(disabled) && (
          <Button
            onClick={handleReset}
            variant="alternative"
            disabled={
              includes(
                [
                  EnumPlanAction.Resetting,
                  EnumPlanAction.Running,
                  EnumPlanAction.Applying,
                  EnumPlanAction.Cancelling,
                ],
                planAction,
              ) || disabled
            }
          >
            {getActionName(
              planAction,
              [EnumPlanAction.Resetting],
              'Start Over',
            )}
          </Button>
        )}
        <Button
          onClick={handleClose}
          variant={isDone ? 'secondary' : 'alternative'}
          disabled={
            includes(
              [
                EnumPlanAction.Running,
                EnumPlanAction.Resetting,
                EnumPlanAction.Cancelling,
              ],
              planAction,
            ) || disabled
          }
          ref={isDone || isApplying ? setFocus : undefined}
        >
          {getActionName(planAction, [EnumPlanAction.Done], 'Close')}
        </Button>
      </div>
    </div>
  )
}
