import { Button } from '../button/Button'
import { useApiContext, useApiContextCancel } from '../../../api'
import { useEffect, useState } from 'react'
import { PlanSidebar } from './PlanSidebar'
import { PlanWizard } from './PlanWizard'
import { useQueryClient } from '@tanstack/react-query'
import { Divider } from '../divider/Divider'
import { applyPlan } from '../../../api/endpoints'

export const EnumStatePlan = {
  Empty: 'empty',
  Run: 'run',
  Running: 'running',
  Backfill: 'backfill',
  Backfilling: 'backfilling',
  Apply: 'apply',
  Applying: 'applying',
  Done: 'done',
  Resetting: 'resetting',
  Canceling: 'canceling',
  Closing: 'closing',
} as const;

export type StatePlan = typeof EnumStatePlan[keyof typeof EnumStatePlan]

export function Plan({ onClose }: { onClose: any }) {
  const client = useQueryClient()

  const [environment, setEnvironment] = useState<string>()
  const [planState, setPlanState] = useState<StatePlan>(EnumStatePlan.Empty)
  const [withModified, setWithModified] = useState(false)
  const [backfillStatus, setBackfillStatus] = useState<boolean>()
  const { data: context, refetch } = useApiContext()

  async function backfill() {
    setPlanState(EnumStatePlan.Backfilling)

    setBackfillStatus(false)

    const done = await applyPlan({ params: { environment } })

    if (done) {
      setBackfillStatus(true)

      if (withModified) {
        setPlanState(EnumStatePlan.Apply)
      } else {
        setPlanState(EnumStatePlan.Done)
      }
    }
  }

  function apply() {
    setPlanState(EnumStatePlan.Applying)

    setTimeout(() => {
      setPlanState(EnumStatePlan.Done)
    }, 3000)
  }

  useEffect(() => {
    if (context?.models?.length === 0) {
      setPlanState(EnumStatePlan.Empty)
    } else {
      setPlanState(EnumStatePlan.Run)
    }
  }, [context])


  async function reset() {
    setPlanState(EnumStatePlan.Resetting)

    await useApiContextCancel(client)
    await delay(2000)
    await refetch()

    setPlanState(EnumStatePlan.Run)
  }

  async function cancel() {
    setPlanState(EnumStatePlan.Canceling)

    await useApiContextCancel(client)
    await delay(1000)
  }

  async function close() {
    setPlanState(EnumStatePlan.Closing)

    await useApiContextCancel(client)
    await delay(1000)

    onClose()
  }

  return (
    <div className="flex items-start w-full h-[75vh] overflow-hidden">
      <PlanSidebar context={context} />
      <div className="flex flex-col w-full h-full overflow-hidden">
        {planState === EnumStatePlan.Empty ? (
          <div className='flex items-center justify-center w-full h-full'>
            <h2 className='text-2xl font-black text-gray-700'>No Models Found</h2>
          </div>
        ) : (
          <div className="flex flex-col w-full h-full overflow-hidden overflow-y-auto p-4">
            <PlanWizard
              id="contextEnvironment"
              backfillStatus={backfillStatus}
              setPlanState={setPlanState}
              setWithModified={setWithModified}
              planState={planState}
              setEnvironment={setEnvironment}
              environment={environment}
            />
          </div>
        )}
        <Divider className='h-2' />
        <div className='flex justify-between px-4 py-2 '>
          <div className='flex w-full'>
            {(planState === EnumStatePlan.Run || planState === EnumStatePlan.Running) && (
              <Button type="submit" form='contextEnvironment' disabled={planState === EnumStatePlan.Running}>
                {planState === EnumStatePlan.Running ? 'Running' : 'Run'}
              </Button>
            )}

            {(planState === EnumStatePlan.Backfill || planState === EnumStatePlan.Backfilling) && (
              <Button onClick={backfill} disabled={planState === EnumStatePlan.Backfilling}>
                {planState === EnumStatePlan.Backfilling ? 'Backfilling' : 'Apply and Backfill'}
              </Button>
            )}

            {(planState === EnumStatePlan.Apply || planState === EnumStatePlan.Applying) && (
              <Button onClick={() => apply()} disabled={planState === EnumStatePlan.Applying}>
                {planState === EnumStatePlan.Applying ? 'Applying' : 'Apply'}
              </Button>
            )}

            {planState === EnumStatePlan.Done && (
              <Button
                onClick={() => close()}
              >
                Done
              </Button>
            )}
            {[EnumStatePlan.Applying, EnumStatePlan.Running, EnumStatePlan.Backfilling].includes(planState as Subset<StatePlan, typeof EnumStatePlan.Backfilling | typeof EnumStatePlan.Running | typeof EnumStatePlan.Applying>) && (
              <Button
                onClick={() => cancel()}
                variant="danger"
                className='justify-self-end'
                disabled={planState === EnumStatePlan.Canceling || planState === EnumStatePlan.Resetting}
              >
                {planState === EnumStatePlan.Canceling ? 'Canceling' : 'Cancel'}
              </Button>
            )}
          </div>
          <div className='flex items-center'>
            {![EnumStatePlan.Empty, EnumStatePlan.Canceling, EnumStatePlan.Closing].includes(planState as Subset<StatePlan, typeof EnumStatePlan.Empty | typeof EnumStatePlan.Canceling>) && (
              <Button
                onClick={() => reset()}
                variant="alternative"
                className='justify-self-end'
                disabled={planState === EnumStatePlan.Resetting}
              >
                {planState === EnumStatePlan.Resetting ? 'Resetting' : 'Reset'}
              </Button>
            )}
            <Button
              onClick={() => close()}
              variant="alternative"
              className='justify-self-end'
              disabled={planState === EnumStatePlan.Closing || planState === EnumStatePlan.Resetting}
            >
              {planState === EnumStatePlan.Closing ? 'Closing' : 'Close'}
            </Button>
          </div>
        </div>
      </div>
    </div>
  )
}

async function delay(time: number = 1000): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, time)
  })
}