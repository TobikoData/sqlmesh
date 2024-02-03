import { Outlet } from 'react-router-dom'
import Page from '../root/Page'
import { useStoreProject } from '@context/project'
import SourceList, { SourceListItem } from '@components/sourceList/SourceList'
import { EnumSize, EnumVariant } from '~/types/enum'
import { EnumRoutes } from '~/routes'
import { Button } from '@components/button/Button'
import { Divider } from '@components/divider/Divider'
import { useMemo } from 'react'

export default function PageTests(): JSX.Element {
  const files = useStoreProject(s => s.files)

  const items = Array.from(files.values()).filter(it =>
    it.path.endsWith('tests'),
  )

  const activeItemIndex = useMemo((): number => {
    return items.findIndex(item => {
      return `${EnumRoutes.Tests}/${item.basename}` === location.pathname
    })
  }, [location.pathname, items])

  return (
    <Page
      sidebar={
        <div className="flex flex-col w-full h-full">
          <SourceList
            by="basename"
            byName="basename"
            to={EnumRoutes.Tests}
            items={items}
            activeItemIndex={activeItemIndex}
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
