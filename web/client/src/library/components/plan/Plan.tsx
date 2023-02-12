import { Button } from '../button/Button'
import { useApiContext, useApiContextByEnvironment, useApiContextCancel } from '../../../api'
import { useEffect, MouseEvent } from 'react'
import { PlanSidebar } from './PlanSidebar'
import { PlanWizard } from './PlanWizard'
import { useQueryClient } from '@tanstack/react-query'
import { Divider } from '../divider/Divider'
import { EnumPlanState, EnumPlanAction, useStorePlan } from '../../../context/plan'
import fetchAPI from '../../../api/instance'
import { includes, isArrayEmpty } from '../../../utils'
import { useChannel } from '../../../api/channels'
import { getActionName } from './help'

export function Plan({
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
  const environment = useStorePlan(s => s.environment)
  const setActivePlan = useStorePlan(s => s.setActivePlan)
  const setEnvironment = useStorePlan(s => s.setEnvironment)
  const setCategory = useStorePlan(s => s.setCategory)
  const withBackfill = useStorePlan(s => s.withBackfill)
  const setWithBackfill = useStorePlan(s => s.setWithBackfill)
  const setBackfills = useStorePlan(s => s.setBackfills)
  const updateTasks = useStorePlan(s => s.updateTasks)
  const backfill_start = useStorePlan(s => s.backfill_start)
  const backfill_end = useStorePlan(s => s.backfill_end)

  const [subscribe] = useChannel('/api/tasks', updateTasks)

  const { refetch } = useApiContextByEnvironment(environment)
  const { data: context } = useApiContext()

  useEffect(() => {
    setPlanAction(EnumPlanAction.Run)
  }, [])

  useEffect(() => {
    if (environment != null) {
      if (includes([EnumPlanAction.Run, EnumPlanAction.Running], planAction)) {
        void refetch()
      }
    } else {
      if (
        includes(
          [EnumPlanAction.None, EnumPlanAction.Opening, EnumPlanAction.Resetting],
          planAction
        )
      ) {
        setPlanAction(EnumPlanAction.Run)
      }
    }
  }, [environment])

  useEffect(() => {
    if (planState === EnumPlanState.Applying) {
      setPlanAction(EnumPlanAction.Applying)
    }

    if (planState === EnumPlanState.Canceling) {
      setPlanAction(EnumPlanAction.Canceling)
    }

    if (planState === EnumPlanState.Finished || planState === EnumPlanState.Failed) {
      setPlanAction(EnumPlanAction.Done)
    }
  }, [planState])

  function cleanUp(): void {
    void useApiContextCancel(client)

    setEnvironment(undefined)
    setCategory(undefined)
    setWithBackfill(false)
    setBackfills()
    setActivePlan(undefined)
  }

  function cancel(): void {
    onCancel()

    reset()
  }

  async function apply<T extends { ok: boolean }>(): Promise<void> {
    setPlanState(EnumPlanState.Applying)

    const url = `/api/apply?environment=${environment == null ? '' : String(environment)}`

    try {
      const data: T = await fetchAPI<T, { start?: string; end?: string }>({
        url,
        method: 'post',
        data: {
          start: backfill_start,
          end: backfill_end,
        },
      })

      if (data.ok) {
        subscribe()
      }
    } catch (error) {
      reset()
    }
  }

  function reset(): void {
    setPlanAction(EnumPlanAction.Resetting)

    cleanUp()

    if (planState !== EnumPlanState.Applying && planState !== EnumPlanState.Canceling) {
      setActivePlan(undefined)
    }
  }

  function close(): void {
    cleanUp()

    onClose()
  }

  const isRunning = includes(
    [EnumPlanAction.Running, EnumPlanAction.Applying, EnumPlanAction.Canceling],
    planAction
  )

  return (
    <div className="flex items-start w-full h-[75vh] overflow-hidden">
      <PlanSidebar context={context} />
      <div className="flex flex-col w-full h-full overflow-hidden">
        {isArrayEmpty(context?.models) ? (
          <div className="flex items-center justify-center w-full h-full">
            <h2 className="text-2xl font-black text-gray-700">No Models Found</h2>
          </div>
        ) : (
          <div className="flex flex-col w-full h-full overflow-hidden overflow-y-auto p-4">
            <PlanWizard id="contextEnvironment" />
          </div>
        )}
        <Divider className="h-2" />
        <div className="flex justify-between px-4 py-2 ">
          <div className="flex w-full">
            {(planAction === EnumPlanAction.Run || planAction === EnumPlanAction.Running) && (
              <Button
                type="submit"
                form="contextEnvironment"
                disabled={planAction === EnumPlanAction.Running}
              >
                {getActionName(planAction, [EnumPlanAction.Running, EnumPlanAction.Run])}
              </Button>
            )}

            {(planAction === EnumPlanAction.Apply || planAction === EnumPlanAction.Applying) && (
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
                  withBackfill === true ? 'Apply And Backfill' : 'Apply'
                )}
              </Button>
            )}
            {isRunning && (
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
            {environment != null && !isRunning && (
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
                  planAction
                )}
              >
                {getActionName(planAction, [EnumPlanAction.Resetting], 'Start Over')}
              </Button>
            )}
            <Button
              onClick={(e: MouseEvent) => {
                e.preventDefault()

                close()
              }}
              variant={planAction === EnumPlanAction.Done ? 'secondary' : 'alternative'}
              className="justify-self-end"
              disabled={includes(
                [EnumPlanAction.Closing, EnumPlanAction.Resetting, EnumPlanAction.Canceling],
                planAction
              )}
            >
              {getActionName(planAction, [EnumPlanAction.Closing, EnumPlanAction.Done], 'Close')}
            </Button>
          </div>
        </div>
      </div>
    </div>
  )
}
