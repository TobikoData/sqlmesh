import { Button } from '../button/Button'
import { useApiContext, useApiContextByEnvironment, useApiContextCancel } from '../../../api'
import { useEffect } from 'react'
import { PlanSidebar } from './PlanSidebar'
import { PlanWizard } from './PlanWizard'
import { useQueryClient } from '@tanstack/react-query'
import { Divider } from '../divider/Divider'
import { EnumPlanState, PlanState, useStorePlan } from '../../../context/plan'
import fetchAPI from '../../../api/instance'
import { delay, isArrayEmpty, isNil, isNotNil } from '../../../utils'
import { useChannel } from '../../../api/channels'


export function Plan({
  onClose,
  onCancel,
}: {
  onClose: any,
  onCancel: any,
}) {
  const client = useQueryClient()

  const planState = useStorePlan((s: any) => s.state)
  const planAction = useStorePlan((s: any) => s.action)
  const setPlanAction = useStorePlan((s: any) => s.setAction)
  const setPlanState = useStorePlan((s: any) => s.setState)
  const environment = useStorePlan((s: any) => s.environment)
  const setEnvironment = useStorePlan((s: any) => s.setEnvironment)
  const setCategory = useStorePlan((s: any) => s.setCategory)
  const withBackfill = useStorePlan((s: any) => s.withBackfill)
  const setWithBackfill = useStorePlan((s: any) => s.setWithBackfill)
  const setBackfills = useStorePlan((s: any) => s.setBackfills)
  const updateTasks = useStorePlan((s: any) => s.updateTasks)

  const [subscribe] = useChannel('/api/tasks', updateTasks)

  const { refetch } = useApiContextByEnvironment(environment)
  const { data: context } = useApiContext()

  useEffect(() => {
    setPlanAction(EnumPlanState.Run)
  }, [])

  useEffect(() => {
    if (isNotNil(environment)) {
      refetch()
    }

    if (isNil(environment)) {
      setPlanAction(EnumPlanState.Run)
    }
  }, [environment])

  useEffect(() => {
    if (planState === EnumPlanState.Applying) {
      setPlanAction(EnumPlanState.Applying)
    }

    if (planState === EnumPlanState.Canceling) {
      setPlanAction(EnumPlanState.Canceling)
    }

    if (planState === EnumPlanState.Finished || planState === EnumPlanState.Failed) {
      setPlanAction(EnumPlanState.Done)
      refetch()
    }
  }, [planState])

  async function apply() {
    setPlanState(EnumPlanState.Applying)

    try {
      const data: any = await fetchAPI({ url: `/api/apply?environment=${environment}`, method: 'post' })

      if (data.ok) {
        subscribe()
      }
    } catch (error) {
      console.log(error)

      reset()
    }
  }

  function cleanUp() {
    useApiContextCancel(client)
    setEnvironment(null)
    setCategory(null)
    setWithBackfill(false)
    setBackfills([])
  }

  async function reset() {
    setPlanAction(EnumPlanState.Resetting)

    await delay(500)

    cleanUp()
  }

  function cancel() {
    onCancel()

    reset()
  }

  function close() {
    reset()

    onClose()
  }

  return (
    <div className="flex items-start w-full h-[75vh] overflow-hidden">
      <PlanSidebar context={context} />
      <div className="flex flex-col w-full h-full overflow-hidden">
        {isArrayEmpty(context?.models) ? (
          <div className='flex items-center justify-center w-full h-full'>
            <h2 className='text-2xl font-black text-gray-700'>No Models Found</h2>
          </div>
        ) : (
          <div className="flex flex-col w-full h-full overflow-hidden overflow-y-auto p-4">
            <PlanWizard id="contextEnvironment" />
          </div>
        )}
        <Divider className='h-2' />
        <div className='flex justify-between px-4 py-2 '>
          <div className='flex w-full'>
            {(planAction === EnumPlanState.Run || planAction === EnumPlanState.Running) && (
              <Button type="submit" form='contextEnvironment' disabled={planAction === EnumPlanState.Running}>
                {getActionName(planAction, [EnumPlanState.Running, EnumPlanState.Run])}
              </Button>
            )}

            {(planAction === EnumPlanState.Apply || planAction === EnumPlanState.Applying) && (
              <Button onClick={() => apply()} disabled={planAction === EnumPlanState.Applying}>
                {getActionName(planAction, [EnumPlanState.Applying], withBackfill ? 'Apply And Backfill' : 'Apply')}
              </Button>
            )}
            {(planAction === EnumPlanState.Applying || planAction === EnumPlanState.Canceling) && (
              <Button
                onClick={() => cancel()}
                variant="danger"
                className='justify-self-end'
                disabled={planAction === EnumPlanState.Canceling}
              >
                {getActionName(planAction, [EnumPlanState.Canceling], 'Cancel')}
              </Button>
            )}
          </div>
          <div className='flex items-center'>
            {environment != null && (
              <Button
                onClick={() => reset()}
                variant="alternative"
                className='justify-self-end'
                disabled={planAction === EnumPlanState.Resetting || planAction === EnumPlanState.Applying || planAction === EnumPlanState.Canceling || planAction === EnumPlanState.Closing}
              >
                {getActionName(planAction, [EnumPlanState.Resetting], 'Reset')}
              </Button>
            )}
            <Button
              onClick={() => close()}
              variant={planAction === EnumPlanState.Done ? 'secondary' : 'alternative'}
              className='justify-self-end'
              disabled={planAction === EnumPlanState.Closing || planAction === EnumPlanState.Resetting || planAction === EnumPlanState.Canceling}
            >
              {getActionName(planAction, [EnumPlanState.Closing, EnumPlanState.Done], 'Close')}
            </Button>
          </div>
        </div>
      </div>
    </div>
  )
}

function getActionName(action: PlanState, options: Array<string> = [], fallback: string = 'Start'): string {
  let name = fallback;

  if (action === EnumPlanState.Done) {
    name = 'Done'
  }

  if (action === EnumPlanState.Running) {
    name = 'Running...'
  }

  if (action === EnumPlanState.Applying) {
    name = 'Applying...'
  }

  if (action === EnumPlanState.Canceling) {
    name = 'Canceling...'
  }

  if (action === EnumPlanState.Resetting) {
    name = 'Resetting...'
  }

  if (action === EnumPlanState.Closing) {
    name = 'Closing...'
  }

  if (action === EnumPlanState.Run) {
    name = 'Run'
  }

  if (action === EnumPlanState.Apply) {
    name = 'Apply'
  }

  return options.includes(action) ? name : fallback
}