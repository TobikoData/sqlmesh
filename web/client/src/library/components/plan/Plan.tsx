import { Button } from '../button/Button'
import { useApiContext, useApiPlan, useApiContextCancel } from '../../../api'
import { useEffect, MouseEvent } from 'react'
import { PlanWizard } from './PlanWizard'
import { useQueryClient } from '@tanstack/react-query'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
} from '../../../context/plan'
import fetchAPI from '../../../api/instance'
import {
  includes,
  isArrayEmpty,
  isFalse,
  isStringEmptyOrNil,
} from '../../../utils'
import { useChannel } from '../../../api/channels'
import { getActionName } from './help'
import { Divider } from '../divider/Divider'
import { useStoreContext } from '~/context/context'

export default function Plan({
  onClose,
  onCancel,
}: {
  onClose: () => void
  onCancel: () => void
}): JSX.Element {
  const client = useQueryClient()

  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const setPlanAction = useStorePlan(s => s.setAction)
  const setPlanState = useStorePlan(s => s.setState)
  // const setActivePlan = useStorePlan(s => s.setActivePlan)
  const setCategory = useStorePlan(s => s.setCategory)

  const setWithBackfill = useStorePlan(s => s.setWithBackfill)
  const setBackfills = useStorePlan(s => s.setBackfills)
  const updateTasks = useStorePlan(s => s.updateTasks)
  const backfill_start = useStorePlan(s => s.backfill_start)
  const backfill_end = useStorePlan(s => s.backfill_end)
  const resetPlanOptions = useStorePlan(s => s.resetPlanOptions)

  const prod = useStoreContext(s => s.prod)
  const environment = useStoreContext(s => s.environment)
  const setEnvironment = useStoreContext(s => s.setEnvironment)
  const environments = useStoreContext(s => s.environments)
  const setEnvironments = useStoreContext(s => s.setEnvironments)

  const [subscribe] = useChannel('/api/tasks', updateTasks)
  const { refetch, data: contextPlan } = useApiPlan(environment)
  const { data: context } = useApiContext()

  useEffect(() => {
    setPlanAction(EnumPlanAction.Run)
  }, [])

  useEffect(() => {
    if (contextPlan == null || contextPlan.environment == null) return

    setEnvironment(contextPlan.environment)

    environments.forEach(env => {
      if (env.name === contextPlan.environment) {
        env.type = 'remote'
      }
    })

    setEnvironments([...environments])
  }, [contextPlan])

  useEffect(() => {
    if (environment != null) return

    if (
      includes(
        [EnumPlanAction.None, EnumPlanAction.Opening, EnumPlanAction.Resetting],
        planAction,
      )
    ) {
      setPlanAction(EnumPlanAction.Run)
    }
  }, [environment])

  useEffect(() => {
    if (planState === EnumPlanState.Applying) {
      setPlanAction(EnumPlanAction.Applying)
    }

    if (planState === EnumPlanState.Canceling) {
      setPlanAction(EnumPlanAction.Canceling)
    }

    if (
      planState === EnumPlanState.Finished ||
      planState === EnumPlanState.Failed
    ) {
      setPlanAction(EnumPlanAction.Done)
    }
  }, [planState])

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
      planState !== EnumPlanState.Canceling
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
        url: '/api/apply',
        method: 'post',
        data: {
          start: backfill_start,
          end: backfill_end,
        },
        params: {
          environment: environment ?? prod.name,
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
    <div className="flex w-full max-h-[90vh]">
      <div className="flex flex-col overflow-hidden w-full">
        {isArrayEmpty(context?.models) ? (
          <div className="flex items-center justify-center w-full h-full">
            <h2 className="text-2xl font-black text-gray-700">
              No Models Found
            </h2>
          </div>
        ) : (
          <div className="flex flex-col w-full h-full overflow-hidden overflow-y-auto p-4">
            <PlanWizard />
          </div>
        )}
        <Divider />
        <PlanActions
          apply={apply}
          run={run}
          cancel={cancel}
          close={close}
          reset={reset}
        />
      </div>
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
    [EnumPlanAction.Running, EnumPlanAction.Applying, EnumPlanAction.Canceling],
    planAction,
  )

  return (
    <div className="flex justify-between px-4 py-2 ">
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
            disabled={planAction === EnumPlanAction.Canceling}
          >
            {getActionName(planAction, [EnumPlanAction.Canceling], 'Cancel')}
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
                EnumPlanAction.Canceling,
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
              EnumPlanAction.Canceling,
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
