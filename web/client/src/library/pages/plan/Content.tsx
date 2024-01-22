import { useParams } from 'react-router-dom'
import { isNil, isNotNil } from '@utils/index'
import PlanProvider from '@components/plan/context'
import { useStoreContext } from '@context/context'
import Plan from '@components/plan/Plan'
import { EnumRoutes } from '~/routes'
import NotFound from '../root/NotFound'
import { useEffect } from 'react'

export default function Content(): JSX.Element {
  const { environmentName } = useParams()

  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)
  const setEnvironment = useStoreContext(s => s.setEnvironment)

  useEffect(() => {
    const found = Array.from(environments).find(
      environment => environment.name === environmentName,
    )

    if (isNotNil(found) && environment.name !== found.name) {
      setEnvironment(found)
    }
  }, [environmentName, environments])

  return (
    <PlanProvider>
      {isNil(environment) ? (
        <NotFound
          link={EnumRoutes.Plan}
          description={
            isNil(environmentName)
              ? undefined
              : `Environment ${environmentName} Does Not Exist`
          }
          message="Back To Docs"
        />
      ) : (
        <Plan />
      )}
    </PlanProvider>
  )
}
