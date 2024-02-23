import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import { useEffect, useMemo } from 'react'
import { isArrayNotEmpty, isNil, isNotNil } from '@utils/index'
import { useStoreContext } from '@context/context'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import Container from '@components/container/Container'
import SearchList from '@components/search/SearchList'
import { EnumSize } from '~/types/enum'
import { EnumRoutes, type Routes } from '~/routes'
import Page from '../root/Page'
import SourceList, { SourceListItem } from '@components/sourceList/SourceList'
import { type LineageNodeModelType } from '@components/graph/Graph'
import { getModelNodeTypeTitle } from '@components/graph/help'
import { Divider } from '@components/divider/Divider'
import NotFound from '../root/NotFound'
import { useStoreProject } from '@context/project'
import LoadingSegment from '@components/loading/LoadingSegment'
import { useApiModels } from '@api/index'

export default function PageModels({
  route = EnumRoutes.Home,
}: {
  route: Routes
}): JSX.Element {
  const { pathname } = useLocation()
  const { modelName } = useParams()
  const navigate = useNavigate()

  const models = useStoreContext(s => s.models)
  const lastSelectedModel = useStoreContext(s => s.lastSelectedModel)
  const setLastSelectedModel = useStoreContext(s => s.setLastSelectedModel)

  const files = useStoreProject(s => s.files)
  const setSelectedFile = useStoreProject(s => s.setSelectedFile)

  const list = useMemo(() => Array.from(new Set(models.values())), [models])

  const { isFetching: isFetchingModels } = useApiModels()

  const to = `${route}/models`
  const model =
    isNil(modelName) || modelName === lastSelectedModel?.name
      ? lastSelectedModel
      : models.get(encodeURI(modelName))

  useEffect(() => {
    if (isNil(model)) return

    const file = files.get(model.path)

    if (isNotNil(file)) {
      setSelectedFile(file)
    }

    setLastSelectedModel(model)

    navigate(`${to}/${model.name}`, { replace: true })
  }, [files, model, to])

  const isNotFound = isNil(model) && isNotNil(modelName)

  return isFetchingModels ? (
    <LoadingSegment>Loading Models...</LoadingSegment>
  ) : (
    <Page
      sidebar={
        <SourceList<ModelSQLMeshModel>
          keyId="displayName"
          keyName="displayName"
          to={to}
          items={list}
          isActive={id => `${to}/${id}` === pathname}
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
          <div className="flex flex-col w-full h-full overflow-hidden">
            {isArrayNotEmpty(list) && (
              <SearchList<ModelSQLMeshModel>
                list={list}
                size={EnumSize.lg}
                searchBy="index"
                displayBy="displayName"
                to={model => `${to}/${model.name}`}
                direction="top"
                className="my-2"
                isFullWidth
              />
            )}
            <Divider />
            {isNotFound ? (
              <NotFound
                link={to}
                message="Go Back"
                description={`Model "${modelName}" not found.`}
              />
            ) : (
              <Outlet />
            )}
          </div>
        </Container.Page>
      }
    />
  )
}
