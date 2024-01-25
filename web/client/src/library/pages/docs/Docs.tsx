import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import { useEffect, useMemo } from 'react'
import { isArrayNotEmpty, isNil } from '@utils/index'
import { useStoreContext } from '@context/context'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import Container from '@components/container/Container'
import SearchList from '@components/search/SearchList'
import { EnumSize } from '~/types/enum'
import { EnumRoutes } from '~/routes'
import Page from '../root/Page'
import SourceList, { SourceListItem } from '@components/sourceList/SourceList'
import { type LineageNodeModelType } from '@components/graph/Graph'
import { getModelNodeTypeTitle } from '@components/graph/help'

export default function PageDocs(): JSX.Element {
  const location = useLocation()
  const navigate = useNavigate()
  const { modelName } = useParams()

  const models = useStoreContext(s => s.models)
  const lastSelectedModel = useStoreContext(s => s.lastSelectedModel)
  const setLastSelectedModel = useStoreContext(s => s.setLastSelectedModel)

  const filtered = useMemo(() => Array.from(new Set(models.values())), [models])

  useEffect(() => {
    if (models.size === 0) return

    const model = isNil(modelName) ? lastSelectedModel : models.get(modelName)

    setLastSelectedModel(model)

    navigate(
      isNil(model)
        ? `${EnumRoutes.IdeDocsModels}`
        : `${EnumRoutes.IdeDocsModels}/${model.name}`,
      { replace: true },
    )
  }, [models])

  const list = Array.from(new Set(models.values()))

  return (
    <Page
      sidebar={
        <SourceList
          key={location.pathname}
          by="displayName"
          byName="displayName"
          to={EnumRoutes.IdeDocsModels}
          items={list}
          types={list.reduce(
            (acc: Record<string, string>, it) =>
              Object.assign(acc, {
                [it.name]: getModelNodeTypeTitle(
                  it.type as LineageNodeModelType,
                ),
              }),
            {},
          )}
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
      }
      content={
        <Container.Page>
          {models.size === 0 ? (
            <div className="p-4 flex flex-col w-full h-full overflow-hidden">
              <div className="flex justify-center items-center w-full h-full">
                <div className="center">
                  <h3 className="text-lg mb-4">
                    Still Waiting For Models To Load...
                  </h3>
                  <p>It usually takes few seconds.</p>
                </div>
              </div>
            </div>
          ) : (
            <div className="flex flex-col w-full h-full overflow-hidden">
              {isArrayNotEmpty(filtered) && (
                <SearchList<ModelSQLMeshModel>
                  list={filtered}
                  size={EnumSize.lg}
                  searchBy="index"
                  displayBy="displayName"
                  to={model => `${EnumRoutes.IdeDocsModels}/${model.name}`}
                  direction="top"
                  className="my-2"
                  isFullWidth
                />
              )}
              <Outlet />
            </div>
          )}
        </Container.Page>
      }
    />
  )
}
