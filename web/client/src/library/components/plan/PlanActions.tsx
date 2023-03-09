import { MouseEvent } from 'react'
import { EnvironmentName } from '~/context/context'
import { PlanAction, EnumPlanAction } from '~/context/plan'
import useActiveFocus from '~/hooks/useActiveFocus'
import { includes, isFalse, isStringEmptyOrNil } from '~/utils'
import { Button } from '../button/Button'
import { EnumCategoryType, usePlan } from './context'
import { getActionName } from './help'

interface PropsPlanActions {
  planAction: PlanAction
  disabled: boolean
  environment: EnvironmentName
  apply: () => void
  run: () => void
  cancel: () => void
  close: () => void
  reset: () => void
}

export default function PlanActions({
  planAction,
  disabled,
  environment,
  run,
  apply,
  cancel,
  close,
  reset,
}: PropsPlanActions): JSX.Element {
  const {
    start,
    end,
    hasBackfills,
    change_category,
    skip_tests,
    auto_apply,
    skip_backfill,
    no_gaps,
    no_auto_categorization,
    forward_only,
    from,
    restate_models,
  } = usePlan()

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

  const shouldApplyWithBackfill =
    hasBackfills &&
    change_category?.id !== EnumCategoryType.NoChange &&
    isFalse(skip_backfill)

  return (
    <div className="flex justify-between px-4 py-2">
      <div className="flex w-full items-center">
        {(isRun || isRunning) && (
          <Button
            disabled={isRunning || disabled}
            onClick={handleRun}
            autoFocus
            ref={setFocus}
          >
            <span>
              {getActionName(planAction, [
                EnumPlanAction.Running,
                EnumPlanAction.Run,
              ])}
            </span>
            {skip_tests && (
              <span className="inline-block ml-1">And Skip Test</span>
            )}
            {auto_apply && (
              <span className="inline-block ml-1">And Auto Apply</span>
            )}
          </Button>
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

        {(isRun || isRunning || isApply || isApplying) && (
          <p className="ml-2 text-gray-600 text-xs max-w-sm">
            <span>Plan for</span>
            <b className="text-secondary-500 font-bold mx-1">{environment}</b>
            <span className="inline-block mr-1">environment</span>
            {
              <span className="inline-block mr-1">
                from{' '}
                <b>
                  {isFalse(isStringEmptyOrNil(start))
                    ? start
                    : 'the begining of history'}
                </b>
              </span>
            }
            {
              <span className="inline-block mr-1">
                till <b>{isFalse(isStringEmptyOrNil(start)) ? end : 'today'}</b>
              </span>
            }
            {isFalse(isStringEmptyOrNil(from)) && (
              <span className="inline-block mr-1">
                based on <b>{from}</b> environment
              </span>
            )}
            {no_gaps && (
              <span className="inline-block mr-1">
                with <b>No Gaps</b>
              </span>
            )}
            {skip_backfill && (
              <span className="inline-block mr-1">
                without <b>Backfills</b>
              </span>
            )}
            {forward_only && (
              <span className="inline-block mr-1">
                consider as a <b>Breaking Change</b>
              </span>
            )}
            {no_auto_categorization && (
              <span className="inline-block mr-1">
                also set <b>Change Category</b> manually
              </span>
            )}
            {isFalse(isStringEmptyOrNil(from)) && (
              <span className="inline-block mr-1">
                and restate folowing models <b>{restate_models}</b>
              </span>
            )}
          </p>
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
