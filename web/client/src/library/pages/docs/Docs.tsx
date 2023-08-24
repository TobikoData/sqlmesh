import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import { useEffect, useState } from 'react'
import { isArrayNotEmpty, isNil, isNotNil } from '@utils/index'
import { useStoreContext } from '@context/context'
import { ModelSQLMeshModel } from '@models/sqlmesh-model'
import Container from '@components/container/Container'
import SourceList from './SourceList'
import SearchList from '@components/search/SearchList'
import { EnumSize } from '~/types/enum'
import { EnumRoutes } from '~/routes'
import Page from '../root/Page'

export default function PageDocs(): JSX.Element {
  const location = useLocation()
  const navigate = useNavigate()
  const { modelName } = useParams()

  const models = useStoreContext(s => s.models)
  const lastActiveModel = useStoreContext(s => s.lastActiveModel)

  const model = isNil(modelName)
    ? undefined
    : models.get(ModelSQLMeshModel.decodeName(modelName))

  const [filter, setFilter] = useState('')

  useEffect(() => {
    if (isNil(model) && isNotNil(lastActiveModel)) {
      navigate(
        EnumRoutes.IdeDocsModels +
          '/' +
          ModelSQLMeshModel.encodeName(lastActiveModel.name),
      )
    }
  }, [model])

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

  useEffect(() => {
    setFilter('')
  }, [location.pathname])

  return (
    <Page
      sidebar={
        <SourceList
          models={models}
          filter={filter}
          setFilter={setFilter}
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
