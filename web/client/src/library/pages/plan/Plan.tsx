import Page from '../root/Page'
import { useStoreContext } from '@context/context'
import SourceList from '@components/sourceList/SourceList'
import { EnumRoutes } from '~/routes'
import { Outlet } from 'react-router-dom'

export default function PagePlan(): JSX.Element {
  const environments = useStoreContext(s => s.environments)

  console.log(Array.from(environments))

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
