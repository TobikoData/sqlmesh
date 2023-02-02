import { Button } from '../button/Button'
import { useApiContext, useApiContextCancel } from '../../../api'
import { useEffect, useState } from 'react'
import { PlanSidebar } from './PlanSidebar'
import { PlanWizard } from './PlanWizard'
import { useQueryClient } from '@tanstack/react-query'
import { Divider } from '../divider/Divider'
import { applyPlan } from '../../../api/endpoints'
import { EnumPlanState, type PlanState, useStorePlan } from '../../../context/plan'


export function Plan({ onClose, onCancel, onApply }: { onClose: any, onCancel: any, onApply: any }) {
  const client = useQueryClient()

  const planState = useStorePlan((s: any) => s.state)
  const planAction = useStorePlan((s: any) => s.action)
  const setPlanAction = useStorePlan((s: any) => s.setAction)
  const environment = useStorePlan((s: any) => s.environment)
  const setEnvironment = useStorePlan((s: any) => s.setEnvironment)
  const withBackfill = useStorePlan((s: any) => s.withBackfill)

  const { data: context } = useApiContext()

  useEffect(() => {
    if (environment == null) {
      setPlanAction(EnumPlanState.Run)
    }
  }, [environment])

  function apply() {
    onApply()
  }

  function cleanUp() {
    useApiContextCancel(client)
    setEnvironment(null)
  }

  function reset() {
    setPlanAction(EnumPlanState.Resetting)

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
        {planState === EnumPlanState.Init ? (
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
                {planState === EnumPlanState.Resetting ? 'Resetting...' : 'Reset'}
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

