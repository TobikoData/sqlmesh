import { useIDE } from '../ide/context'
import Page from '../root/Page'
import SourceList from '@components/sourceList/SourceList'
import { EnumRoutes } from '~/routes'
import { Outlet } from 'react-router-dom'
import { EnumVariant } from '~/types/enum'

export default function PageErrors(): JSX.Element {
  const { errors } = useIDE()

  return (
    <Page
      sidebar={
        <SourceList
          by="id"
          byName="key"
          byDescription="message"
          variant={EnumVariant.Danger}
          to={EnumRoutes.Errors}
          items={Array.from(errors)}
          types={Array.from(errors).reduce(
            (acc: Record<string, string>, it) =>
              Object.assign(acc, { [it.id]: it.status }),
            {},
          )}
        />
      }
      content={<Outlet />}
    />
  )
}
