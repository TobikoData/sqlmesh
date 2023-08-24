import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import { useEffect } from 'react'
import { isArrayNotEmpty, isNil } from '@utils/index'
import { useStoreContext } from '@context/context'
import { ModelSQLMeshModel } from '@models/sqlmesh-model'
import Container from '@components/container/Container'
import SearchList from '@components/search/SearchList'
import { EnumSize, EnumVariant } from '~/types/enum'
import { EnumRoutes } from '~/routes'
import Page from '../root/Page'
import SourceList from '@components/sourceList/SourceList'
import { type LineageNodeModelType } from '@components/graph/Graph'
import { getModelNodeTypeTitle } from '@components/graph/help'

export default function PageDocs(): JSX.Element {
  const location = useLocation()
  const navigate = useNavigate()
  const { modelName } = useParams()

  const models = useStoreContext(s => s.models)
  const lastActiveModel = useStoreContext(s => s.lastActiveModel)

  useEffect(() => {
    const model = isNil(modelName)
      ? undefined
      : models.get(ModelSQLMeshModel.decodeName(modelName))

    let route

    if (isNil(model)) {
      if (isNil(lastActiveModel)) return

      route = `${EnumRoutes.IdeDocsModels}/${ModelSQLMeshModel.encodeName(
        lastActiveModel.name,
      )}`
    } else {
      route = `${EnumRoutes.IdeDocsModels}/${ModelSQLMeshModel.encodeName(
        model.name,
      )}`
    }

    navigate(route)
  }, [])

  const filtered = Array.from(models.entries()).reduce(
    (acc: ModelSQLMeshModel[], [key, model]) => {
      if (model.name === key) return acc
      if (
        isNil(modelName) ||
        model.name !== ModelSQLMeshModel.decodeName(modelName)
      ) {
        acc.push(model)
      }

      return acc
    },
    [],
  )

  return (
    <Page
      sidebar={
        <SourceList
          key={location.pathname}
          by="name"
          byName="name"
          variant={EnumVariant.Primary}
          to={EnumRoutes.IdeDocsModels}
          items={Array.from(new Set(models.values()))}
          types={Array.from(new Set(models.values())).reduce(
            (acc: Record<string, string>, it) =>
              Object.assign(acc, {
                [it.name]: getModelNodeTypeTitle(
                  it.type as LineageNodeModelType,
                ),
              }),
            {},
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
            <div className="flex flex-col w-full h-full overflow-hidden">
              {isArrayNotEmpty(filtered) && (
                <SearchList<ModelSQLMeshModel>
                  list={filtered}
                  size={EnumSize.lg}
                  searchBy="index"
                  displayBy="name"
                  to={model =>
                    `${EnumRoutes.IdeDocsModels}/${ModelSQLMeshModel.encodeName(
                      model.name,
                    )}`
                  }
                  direction="top"
                  className="my-2"
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
