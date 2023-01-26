import { Button } from '../button/Button'
import { useApiContext } from '../../../api'
import { useEffect, useState } from 'react'
import { PlanSidebar } from './PlanSidebar'
import { PlanWizard } from './PlanWizard'

export function Plan({ onCancel: cancel }: { onCancel: any }) {

  const [shouldShowNext, setShouldShowNext] = useState<boolean>(false)
  const [shouldShowApply, setShouldShowApply] = useState<boolean>(false)
  const [shouldShowBackfill, setShouldShowBackfill] = useState(false)
  const [backfillStatus, setBackfillStatus] = useState<boolean>()
  const [backfillTasks, setBackfillTasks] = useState<Array<[string, number]>>([])
  const { data: context } = useApiContext()

  function backfill() {
    context?.models && setBackfillTasks(context?.models.map(name => [name, Math.round(Math.random() * 40)]))

    setTimeout(() => {
      setBackfillStatus(false)
    }, 200)

    setTimeout(() => {
      setBackfillStatus(true)
    }, 2000)

    setShouldShowBackfill(false)
  }

  return (
    <div className="flex items-start w-full h-[75vh] overflow-hidden">
      <PlanSidebar context={context} />
      <div className="flex flex-col w-full h-full overflow-hidden">
        <div className="flex flex-col w-full h-full overflow-hidden overflow-y-auto p-4">
          <PlanWizard
            id="contextEnvironment"
            setShouldShowApply={setShouldShowApply}
            setShouldShowNext={setShouldShowNext}
            setShouldShowBackfill={setShouldShowBackfill}
            shouldShowBackfill={shouldShowBackfill}
            backfillTasks={backfillTasks}
            backfillStatus={backfillStatus}
          />
        </div>
        <div className='flex justify-between px-4 py-2 bg-secondary-100'>
          <div className='w-full'>
            {shouldShowBackfill && (
              <Button onClick={backfill}>
                Backfill
              </Button>
            )}
            {shouldShowNext && (
              <Button type="submit" form='contextEnvironment'>
                Next
              </Button>
            )}
            {shouldShowApply && (
              <Button
                onClick={() => console.log('Running plan')}
              >
                Apply Plan
              </Button>
            )}
            {(!shouldShowApply && !shouldShowNext && !shouldShowBackfill && backfillStatus) && (
              <Button
                onClick={() => cancel(false)}
              >
                Done
              </Button>
            )}
          </div>
          {!(!shouldShowApply && !shouldShowNext && !shouldShowBackfill && backfillStatus) && (
            <div className='flex items-center'>
              <Button
                onClick={() => cancel(false)}
                variant="alternative"
                className='justify-self-end'
              >
                Reset
              </Button>
              <Button
                onClick={() => cancel(false)}
                variant="danger"
                className='justify-self-end'
              >
                Cancel
              </Button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
