import { useParams } from 'react-router-dom'
import { isNil } from '@utils/index'
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
    if (environment.isInitialProd || environmentName === environment.name)
      return

    const found = Array.from(environments).find(
      env => env.name === environmentName,
    )

    if (isNil(found)) return

    setEnvironment(found)
  }, [environmentName])

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
