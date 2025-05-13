import Page from '../root/Page'
import { useStoreContext } from '@context/context'
import { EnumRoutes } from '~/routes'
import { Outlet, useLocation, useNavigate } from 'react-router'
import { useEffect } from 'react'
import { useStorePlan } from '@context/plan'
import { isNotNil } from '@utils/index'

export default function PagePlan(): JSX.Element {
  const navigate = useNavigate()
  const location = useLocation()

  const environment = useStoreContext(s => s.environment)

  const planApply = useStorePlan(s => s.planApply)

  useEffect(() => {
    if (planApply.isRunning && isNotNil(planApply.environment)) {
      const pathname = `${EnumRoutes.Plan}/environments/${planApply.environment}`

      if (location.pathname !== pathname) {
        navigate(pathname, { replace: true })
      }
    } else if (
      location.pathname === EnumRoutes.Plan ||
      location.pathname === `${EnumRoutes.Plan}/environments`
    ) {
      navigate(`${EnumRoutes.Plan}/environments/${environment.name}`, {
        replace: true,
      })
    }
  }, [location, planApply.isRunning])

  useEffect(() => {
    navigate(`${EnumRoutes.Plan}/environments/${environment.name}`, {
      replace: true,
    })
  }, [environment])

  return <Page content={<Outlet />} />
}
