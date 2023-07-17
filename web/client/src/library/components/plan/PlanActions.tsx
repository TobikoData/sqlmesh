import { type MouseEvent } from 'react'
import { useStoreContext } from '~/context/context'
import { type PlanAction, EnumPlanAction } from '~/context/plan'
import useActiveFocus from '~/hooks/useActiveFocus'
import { EnumVariant } from '~/types/enum'
import { includes, isFalse, isStringEmptyOrNil } from '~/utils'
import { Button } from '../button/Button'
import { usePlan } from './context'
import { getActionName } from './help'

interface PropsPlanActions {
  planAction: PlanAction
  disabled: boolean
  apply: () => void
  run: () => void
  cancel: () => void
  close: () => void
  reset: () => void
}

export default function PlanActions({
  planAction,
  disabled,
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
    skip_tests,
    auto_apply,
    skip_backfill,
    no_gaps,
    no_auto_categorization,
    forward_only,
    restate_models,
    include_unmodified,
    hasVirtualUpdate,
  } = usePlan()

  const environment = useStoreContext(s => s.environment)

  const setFocus = useActiveFocus<HTMLButtonElement>()

  const isNone = planAction === EnumPlanAction.None
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

  const shouldApplyWithBackfill = hasBackfills && isFalse(skip_backfill)

  return (
    <div className="flex justify-between px-4 py-2">
      <div className="flex w-full items-center">
        {(isRun || isRunning) && (
          <Button
            disabled={isRunning || disabled}
            onClick={handleRun}
            autoFocus
            ref={setFocus}
            variant={EnumVariant.Primary}
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
            variant={EnumVariant.Primary}
          >
            {getActionName(
              planAction,
              [EnumPlanAction.Applying],
              shouldApplyWithBackfill
                ? 'Apply And Backfill'
                : hasVirtualUpdate
                ? 'Apply Virtual Update'
                : 'Apply',
            )}
          </Button>
        )}
        {isProcessing && (
          <Button
            onClick={handleCancel}
            variant={EnumVariant.Danger}
            className="justify-self-end"
            disabled={isCanceling || disabled}
          >
            {getActionName(planAction, [EnumPlanAction.Cancelling], 'Cancel')}
          </Button>
        )}

        {(isRun || isRunning || isApply || isApplying) && (
          <p className="ml-2 text-xs max-w-sm">
            <span>Plan for</span>
            <b className="text-primary-500 font-bold mx-1">
              {environment.name}
            </b>
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
            {isFalse(isStringEmptyOrNil(restate_models)) && (
              <span className="inline-block mr-1">
                and restate folowing models <b>{restate_models}</b>
              </span>
            )}
            {include_unmodified && (
              <span className="inline-block mr-1">
                with views for all models
              </span>
            )}
          </p>
        )}
      </div>
      <div className="flex items-center">
        {(isNone ||
          [
            isProcessing,
            isRun,
            disabled,
            environment.isInitial && environment.isDefault,
            isDone,
          ].every(isFalse)) && (
          <Button
            onClick={handleReset}
            variant={EnumVariant.Neutral}
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
          variant={isDone ? EnumVariant.Primary : EnumVariant.Neutral}
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
