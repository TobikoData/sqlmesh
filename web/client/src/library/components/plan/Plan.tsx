import { Button } from '../button/Button'
import { useApiPlan, useApiContextCancel } from '../../../api'
import { useEffect, MouseEvent, useMemo } from 'react'
import PlanWizard from './PlanWizard'
import { useQueryClient } from '@tanstack/react-query'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
} from '../../../context/plan'
import fetchAPI from '../../../api/instance'
import {
  includes,
  isArrayNotEmpty,
  isFalse,
  isStringEmptyOrNil,
  toDate,
  toDateFormat,
} from '../../../utils'
import { useChannel } from '../../../api/channels'
import { getActionName, isModified } from './help'
import { Divider } from '../divider/Divider'
import { EnvironmentName, useStoreContext } from '~/context/context'

export default function Plan({
  environment,
  onClose,
  onCancel,
}: {
  environment: EnvironmentName
  onClose: () => void
  onCancel: () => void
}): JSX.Element {
  const client = useQueryClient()

  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const backfill_start = useStorePlan(s => s.backfill_start)
  const backfill_end = useStorePlan(s => s.backfill_end)
  const setPlanAction = useStorePlan(s => s.setAction)
  const setPlanState = useStorePlan(s => s.setState)
  const setCategory = useStorePlan(s => s.setCategory)
  const setWithBackfill = useStorePlan(s => s.setWithBackfill)
  const setBackfills = useStorePlan(s => s.setBackfills)
  const setBackfillDate = useStorePlan(s => s.setBackfillDate)
  const updateTasks = useStorePlan(s => s.updateTasks)
  const resetPlanOptions = useStorePlan(s => s.resetPlanOptions)

  const { refetch, data } = useApiPlan(environment)
  const [subscribe] = useChannel('/api/tasks', updateTasks)

  const plan = useMemo(() => {
    return [
      data?.environment == null,
      planAction === EnumPlanAction.Run,
      planAction === EnumPlanAction.Done,
    ].some(Boolean)
      ? undefined
      : data
  }, [data, planAction])
  const changes = useMemo(() => plan?.changes, [plan])
  const hasChanges = useMemo(
    () =>
      [
        isModified(changes?.modified),
        isArrayNotEmpty(changes?.added),
        isArrayNotEmpty(changes?.removed),
      ].some(Boolean),
    [changes],
  )

  useEffect(() => {
    if (planState === EnumPlanState.Applying) {
      setPlanAction(EnumPlanAction.Applying)
    }

    if (planState === EnumPlanState.Cancelling) {
      setPlanAction(EnumPlanAction.Cancelling)
    }

    if (includes([EnumPlanState.Finished, EnumPlanState.Failed], planState)) {
      setPlanAction(EnumPlanAction.Done)
    }
  }, [planState])

  useEffect(() => {
    if (plan == null) return

    setBackfills(plan.backfills)

    if (isArrayNotEmpty(plan.backfills) || hasChanges) {
      setPlanAction(EnumPlanAction.Apply)
    } else {
      setPlanAction(EnumPlanAction.Done)
    }

    setBackfillDate('start', toDateFormat(toDate(plan.start)))
    setBackfillDate('end', toDateFormat(toDate(plan.end)))
  }, [plan])

  function cleanUp(): void {
    void useApiContextCancel(client)

    setCategory(undefined)
    setWithBackfill(false)
    setBackfills()
    resetPlanOptions()
  }

  function reset(): void {
    setPlanAction(EnumPlanAction.Resetting)

    cleanUp()

    if (
      planState !== EnumPlanState.Applying &&
      planState !== EnumPlanState.Cancelling
    ) {
      setPlanAction(EnumPlanAction.Run)
    }
  }

  function cancel(): void {
    onCancel()

    reset()
  }

  async function apply<T extends { ok: boolean }>(): Promise<void> {
    setPlanState(EnumPlanState.Applying)

    try {
      const data: T = await fetchAPI<T, { start?: string; end?: string }>({
        url: '/api/commands/apply',
        method: 'post',
        data: {
          start: backfill_start,
          end: backfill_end,
        },
        params: {
          environment,
        },
      })

      if (data.ok) {
        subscribe()
      }
    } catch (error) {
      reset()
    }
  }

  function close(): void {
    cleanUp()

    onClose()
  }

  function run(): void {
    setPlanAction(EnumPlanAction.Running)

    void refetch()
  }

  return (
    <div className="flex flex-col w-full max-h-[90vh] overflow-hidden">
      <div className="h-full w-full py-4 px-6">
        <h4 className="text-xl">
          <span className="font-bold">Target Environment is</span>
          <b className="ml-2 px-2 py-1 font-sm rounded-md bg-secondary-500 text-secondary-100">
            {environment}
          </b>
        </h4>
      </div>
      <Divider />
      <div className="flex flex-col w-full h-full overflow-hidden overflow-y-auto p-4 scrollbar scrollbar--vertical">
        <PlanWizard
          environment={environment}
          changes={changes}
        />
      </div>
      <Divider />
      <PlanActions
        apply={apply}
        run={run}
        cancel={cancel}
        close={close}
        reset={reset}
      />
    </div>
  )
}

interface PropsPlanActions {
  apply: () => Promise<void>
  run: () => void
  cancel: () => void
  close: () => void
  reset: () => void
}

function PlanActions({
  run,
  apply,
  cancel,
  close,
  reset,
}: PropsPlanActions): JSX.Element {
  const withBackfill = useStorePlan(s => s.withBackfill)
  const planAction = useStorePlan(s => s.action)

  const environment = useStoreContext(s => s.environment)

  const isRunning =
    planAction === EnumPlanAction.Run || planAction === EnumPlanAction.Running
  const isApplying =
    planAction === EnumPlanAction.Apply ||
    planAction === EnumPlanAction.Applying
  const isProcessing = includes(
    [
      EnumPlanAction.Running,
      EnumPlanAction.Applying,
      EnumPlanAction.Cancelling,
    ],
    planAction,
  )

  return (
    <div className="flex justify-between px-4 py-2">
      <div className="flex w-full items-center">
        {isRunning && (
          <>
            <Button
              type="submit"
              disabled={
                planAction === EnumPlanAction.Running ||
                isStringEmptyOrNil(environment)
              }
              onClick={(e: MouseEvent) => {
                e.stopPropagation()

                run()
              }}
            >
              {getActionName(planAction, [
                EnumPlanAction.Running,
                EnumPlanAction.Run,
              ])}
            </Button>
            {planAction === EnumPlanAction.Run && (
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

        {isApplying && (
          <Button
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              void apply()
            }}
            disabled={planAction === EnumPlanAction.Applying}
          >
            {getActionName(
              planAction,
              [EnumPlanAction.Applying],
              withBackfill ? 'Apply And Backfill' : 'Apply',
            )}
          </Button>
        )}
        {isProcessing && (
          <Button
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              cancel()
            }}
            variant="danger"
            className="justify-self-end"
            disabled={planAction === EnumPlanAction.Cancelling}
          >
            {getActionName(planAction, [EnumPlanAction.Cancelling], 'Cancel')}
          </Button>
        )}
      </div>
      <div className="flex items-center">
        {isFalse(isProcessing) && isFalse(isRunning) && (
          <Button
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              reset()
            }}
            variant="alternative"
            className="justify-self-end"
            disabled={includes(
              [
                EnumPlanAction.Resetting,
                EnumPlanAction.Applying,
                EnumPlanAction.Cancelling,
                EnumPlanAction.Closing,
              ],
              planAction,
            )}
          >
            {getActionName(
              planAction,
              [EnumPlanAction.Resetting],
              'Start Over',
            )}
          </Button>
        )}
        <Button
          onClick={(e: MouseEvent) => {
            e.preventDefault()

            close()
          }}
          variant={
            planAction === EnumPlanAction.Done ? 'secondary' : 'alternative'
          }
          className="justify-self-end"
          disabled={includes(
            [
              EnumPlanAction.Closing,
              EnumPlanAction.Resetting,
              EnumPlanAction.Cancelling,
            ],
            planAction,
          )}
        >
          {getActionName(
            planAction,
            [EnumPlanAction.Closing, EnumPlanAction.Done],
            'Close',
          )}
        </Button>
      </div>
    </div>
  )
}
