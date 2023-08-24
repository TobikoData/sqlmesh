import PlanProvider from '@components/plan/context'
import Page from '../root/Page'
import Plan from '@components/plan/Plan'
import { isTrue } from '@utils/index'
import { useStoreContext } from '@context/context'
import { useState } from 'react'

export default function PagePlan(): JSX.Element {
  const environment = useStoreContext(s => s.environment)
  const initialStartDate = useStoreContext(s => s.initialStartDate)
  const initialEndDate = useStoreContext(s => s.initialEndDate)

  const [isClosingModal, setIsClosingModal] = useState(false)

  function closeModal(): void {
    setIsClosingModal(true)
  }

  return (
    <Page
      sidebar={<div></div>}
      content={
        <PlanProvider>
          <Plan
            environment={environment}
            isInitialPlanRun={
              environment?.isDefault == null || isTrue(environment?.isDefault)
            }
            disabled={isClosingModal}
            initialStartDate={initialStartDate}
            initialEndDate={initialEndDate}
            onClose={closeModal}
          />
        </PlanProvider>
      }
    />
  )
}
