import { Outlet, useLocation } from 'react-router-dom'
import Page from '../root/Page'
import { useStoreProject } from '@context/project'
import SourceList from '@components/sourceList/SourceList'
import SourceListItem from '@components/sourceList/SourceListItem'
import { EnumSize, EnumVariant } from '~/types/enum'
import { EnumRoutes } from '~/routes'
import { Button } from '@components/button/Button'
import { Divider } from '@components/divider/Divider'
import { type ModelFile } from '@models/file'

export default function PageTests(): JSX.Element {
  const { pathname } = useLocation()
  const files = useStoreProject(s => s.files)

  const items = Array.from(files.values()).filter(it =>
    it.path.endsWith('tests'),
  )

  return (
    <Page
      sidebar={
        <div className="flex flex-col w-full h-full">
          <SourceList<ModelFile>
            keyId="basename"
            keyName="basename"
            to={EnumRoutes.Tests}
            items={items}
            isActive={id => `${EnumRoutes.Tests}/${id}` === pathname}
            listItem={({ to, name, description, text, disabled = false }) => (
              <SourceListItem
                to={to}
                name={name}
                text={text}
                description={description}
                disabled={disabled}
              />
            )}
          />
          <Divider />
          <div className="py-1 px-1 flex justify-end">
            <Button
              size={EnumSize.sm}
              variant={EnumVariant.Neutral}
            >
              Run All
            </Button>
          </div>
        </div>
      }
      content={<Outlet />}
    />
  )
}
