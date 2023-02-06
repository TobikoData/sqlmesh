import { Button } from '../button/Button'
import { useApiContext, useApiContextByEnvironment, useApiContextCancel } from '../../../api'
import { useEffect } from 'react'
import { PlanSidebar } from './PlanSidebar'
import { PlanWizard } from './PlanWizard'
import { useQueryClient } from '@tanstack/react-query'
import { Divider } from '../divider/Divider'
import { EnumPlanState, useStorePlan } from '../../../context/plan'
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
                {planAction === EnumPlanState.Running ? 'Running...' : 'Run'}
              </Button>
            )}

            {(planAction === EnumPlanState.Apply || planAction === EnumPlanState.Applying) && (
              <Button onClick={() => apply()} disabled={planAction === EnumPlanState.Applying}>
                {planAction === EnumPlanState.Applying ? 'Applying...' : withBackfill ? 'Apply And Backfill' : 'Apply'}
              </Button>
            )}

            {planAction === EnumPlanState.Done && (
              <Button
                onClick={() => close()}
              >
                Done
              </Button>
            )}
            {(planAction === EnumPlanState.Applying || planAction === EnumPlanState.Canceling) && (
              <Button
                onClick={() => cancel()}
                variant="danger"
                className='justify-self-end'
                disabled={planAction === EnumPlanState.Canceling}
              >
                {planAction === EnumPlanState.Canceling ? 'Canceling...' : 'Cancel'}
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
                {planAction === EnumPlanState.Resetting ? 'Resetting...' : 'Reset'}
              </Button>
            )}
            <Button
              onClick={() => close()}
              variant="alternative"
              className='justify-self-end'
              disabled={planAction === EnumPlanState.Closing || planAction === EnumPlanState.Resetting || planAction === EnumPlanState.Canceling}
            >
              {planAction === EnumPlanState.Closing ? 'Closing...' : 'Close'}
            </Button>
          </div>
        </div>
      </div>
    </div>
  )
}

