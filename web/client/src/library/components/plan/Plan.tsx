
import { Button } from '../button/Button'
import { useApiContext } from '../../../api'
import { useState } from 'react'
import { PlanSidebar } from './PlanSidebar'
import { PlanWizard } from './PlanWizard'

export function Plan({ onCancel: cancel }: { onCancel: any }) {

  const [shouldShowNext, setShouldShowNext] = useState<boolean>(false)
  const [shouldShowApply, setShouldShowApply] = useState<boolean>(false)
  const { data: context } = useApiContext()

  return (
    <div className="flex items-start w-full h-[40rem] overflow-hidden">
      <PlanSidebar context={context} />
      <div className="flex flex-col w-full h-full overflow-hidden">
        <div className="flex flex-col w-full h-full overflow-hidden overflow-y-auto p-4">
          <PlanWizard id="contextEnvironment" setShouldShowApply={setShouldShowApply} setShouldShowNext={setShouldShowNext} />
        </div>
        <div className='flex justify-between px-4 py-2 bg-secondary-100'>
          <div className='w-full'>
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
          </div>
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
      </div>
    </div>
  )
}
