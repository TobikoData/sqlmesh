import Page from '../root/Page'
import { useStoreContext } from '@context/context'
import SourceList from '@components/sourceList/SourceList'
import { EnumRoutes } from '~/routes'
import { Outlet, useLocation, useNavigate } from 'react-router-dom'
import { useEffect } from 'react'

export default function PagePlan(): JSX.Element {
  const navigaete = useNavigate()
  const location = useLocation()

  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)

  useEffect(() => {
    if (
      location.pathname === EnumRoutes.Plan ||
      location.pathname === `${EnumRoutes.Plan}/environments`
    ) {
      navigaete(`${EnumRoutes.Plan}/environments/${environment.name}`)
    }
  }, [location])

  useEffect(() => {
    navigaete(`${EnumRoutes.Plan}/environments/${environment.name}`)
  }, [environment])

  return (
    <Page
      sidebar={
        <SourceList
          by="name"
          byName="name"
          to={`${EnumRoutes.Plan}/environments`}
          items={Array.from(environments)}
        />
      }
      content={<Outlet />}
    />
  )
}
