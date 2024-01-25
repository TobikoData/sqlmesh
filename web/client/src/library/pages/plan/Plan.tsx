import Page from '../root/Page'
import { useStoreContext } from '@context/context'
import SourceList, { SourceListItem } from '@components/sourceList/SourceList'
import { EnumRoutes } from '~/routes'
import { Outlet, useLocation, useNavigate } from 'react-router-dom'
import { useEffect } from 'react'
import { useStorePlan } from '@context/plan'
import { isFalse, isNotNil } from '@utils/index'
import { Modules } from '@api/client'

export default function PagePlan(): JSX.Element {
  const navigate = useNavigate()
  const location = useLocation()

  const modules = useStoreContext(s => s.modules)
  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)

  const planAction = useStorePlan(s => s.planAction)
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

  return (
    <Page
      sidebar={
        <SourceList
          by="name"
          byName="name"
          to={`${EnumRoutes.Plan}/environments`}
          items={Array.from(environments)}
          disabled={
            isFalse(modules.includes(Modules.plans)) ||
            planAction.isProcessing ||
            environment.isInitialProd
          }
          listItem={({
            to,
            name,
            description,
            text,
            disabled = false,
            item,
          }) => (
            <SourceListItem
              to={to}
              name={name}
              text={text}
              description={description}
              disabled={disabled}
            />
          )}
        />
      }
      content={<Outlet />}
    />
  )
}
