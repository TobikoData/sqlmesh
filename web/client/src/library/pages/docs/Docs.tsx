import { Outlet, useLocation } from 'react-router-dom'
import { useEffect, useMemo, useState } from 'react'
import { isArrayNotEmpty } from '@utils/index'
import { useStoreContext } from '@context/context'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import Container from '@components/container/Container'
import SplitPane from '@components/splitPane/SplitPane'
import SourceList from './SourceList'
import SearchList from '@components/search/SearchList'
import { EnumSize } from '~/types/enum'
import { EnumRoutes } from '~/routes'

export default function PageDocs(): JSX.Element {
  const location = useLocation()

  const models = useStoreContext(s => s.models)

  const [filter, setFilter] = useState('')

  const filtered = useMemo(() => Array.from(new Set(models.values())), [models])

  useEffect(() => {
    setFilter('')
  }, [location.pathname])

  return (
    <Container.Page>
      {models.size === 0 ? (
        <div className="p-4 flex flex-col w-full h-full overflow-hidden">
          <div className="flex justify-center items-center w-full h-full">
            <div className="center">
              <h3 className="text-lg mb-4">
                Still Waiting For Models To Load...
              </h3>
              <p>It should take few more seconds.</p>
              <p>
                If it takes too long, probably, there is a problem with API
                response.
              </p>
              <p>Check if the server is running and refresh.</p>
            </div>
          </div>
        </div>
      ) : (
        <div className="p-4 flex flex-col w-full h-full overflow-hidden">
          {isArrayNotEmpty(filtered) && (
            <SearchList<ModelSQLMeshModel>
              list={filtered}
              size={EnumSize.lg}
              searchBy="index"
              displayBy="displayName"
              to={model => `${EnumRoutes.IdeDocsModels}/${model.name}`}
              isFullWidth={true}
            />
          )}
          <SplitPane
            className="flex w-full h-full overflow-hidden mt-8"
            sizes={[25, 75]}
            minSize={0}
            snapOffset={0}
          >
            <div className="py-4 w-full">
              {models.size > 0 && (
                <SourceList
                  models={models}
                  filter={filter}
                  setFilter={setFilter}
                />
              )}
            </div>
            <div className="w-full">
              <Outlet />
            </div>
          </SplitPane>
        </div>
      )}
    </Container.Page>
  )
}
