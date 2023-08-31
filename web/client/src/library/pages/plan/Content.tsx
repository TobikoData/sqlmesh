import { useParams } from 'react-router-dom'
import { isNil, isTrue } from '@utils/index'
import PlanProvider from '@components/plan/context'
import { useStoreContext } from '@context/context'
import { useState } from 'react'
import Plan from '@components/plan/Plan'
import { EnumRoutes } from '~/routes'
import NotFound from '../root/NotFound'

export default function Content(): JSX.Element {
  const { environmentName } = useParams()

  const environments = useStoreContext(s => s.environments)
  const initialStartDate = useStoreContext(s => s.initialStartDate)
  const initialEndDate = useStoreContext(s => s.initialEndDate)

  const [isClosingModal, setIsClosingModal] = useState(false)

  function closeModal(): void {
    setIsClosingModal(true)
  }

  const environment = isNil(environmentName)
    ? undefined
    : Array.from(environments).find(
        environment => environment.name === environmentName,
      )

  return (
    <PlanProvider>
      {isNil(environment) ? (
        <NotFound
          link={EnumRoutes.Plan}
          descritpion={
            isNil(environmentName)
              ? undefined
              : `Model ${environmentName} Does Not Exist`
          }
          message="Back To Docs"
        />
      ) : (
        <Plan
          key={environment.name}
          environment={environment}
          isInitialPlanRun={
            environment?.isDefault == null || isTrue(environment?.isDefault)
          }
          disabled={isClosingModal}
          initialStartDate={initialStartDate}
          initialEndDate={initialEndDate}
          onClose={closeModal}
        />
      )}
    </PlanProvider>
  )
}
