import { Outlet, useLocation, useNavigate, useParams } from 'react-router'
import { useEffect, useMemo } from 'react'
import {
  isArrayEmpty,
  isArrayNotEmpty,
  isFalse,
  isNil,
  isNotNil,
} from '@utils/index'
import { useStoreContext } from '@context/context'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import Container from '@components/container/Container'
import SearchList from '@components/search/SearchList'
import { EnumSize } from '~/types/enum'
import { EnumRoutes, type Routes } from '~/routes'
import Page from '../root/Page'
import { getModelNodeTypeTitle } from '@components/graph/help'
import { Divider } from '@components/divider/Divider'
import NotFound from '../root/NotFound'
import { useStoreProject } from '@context/project'
import LoadingSegment from '@components/loading/LoadingSegment'
import { useApiModels } from '@api/index'
import {
  TBKResizeObserver,
  TBKBadge,
  TBKSourceList,
  TBKSourceListItem,
  TBKSourceListSection,
  TBKModelName,
} from '~/utils/additional-components'
import { ModelName } from '~/utils/tbk-components'

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

  const isNotFound =
    isNil(model) && isNotNil(modelName) && isFalse(isFetchingModels)

  return (
    <Page
      sidebar={
        isArrayEmpty(list) ? undefined : (
          <TBKResizeObserver update-selector="tbk-model-name">
            <TBKSourceList
              selectable
              onChange={(e: CustomEvent) => navigate(e.detail.value)}
              className="pt-3 px-2"
            >
              {Object.entries(
                ModelName.categorize(list) as Record<
                  string,
                  ModelSQLMeshModel[]
                >,
              ).map(([namespace, models]) => (
                <TBKSourceListSection
                  key={namespace}
                  headline={namespace}
                  open
                >
                  {models.map((model: ModelSQLMeshModel) => {
                    return (
                      <TBKSourceListItem
                        key={model.name}
                        size="xs"
                        hide-icon
                        active={pathname === `${to}/${model.name}`}
                        value={`${to}/${model.name}`}
                        search-value={model.displayName}
                        compact
                      >
                        <TBKModelName
                          hide-schema
                          hide-catalog
                          hide-tooltip
                          text={model.displayName}
                        >
                          <TBKBadge
                            size="2xs"
                            slot="after"
                          >
                            {getModelNodeTypeTitle(model.type)}
                          </TBKBadge>
                        </TBKModelName>
                      </TBKSourceListItem>
                    )
                  })}
                </TBKSourceListSection>
              ))}
            </TBKSourceList>
          </TBKResizeObserver>
        )
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
                className="p-2"
                isFullWidth
                disabled={isFetchingModels}
              />
            )}
            <Divider />
            {isNotFound ? (
              <NotFound
                link={to}
                message="Go Back"
                description={`Model "${modelName}" not found.`}
              />
            ) : isFetchingModels ? (
              <LoadingSegment>Loading Model page...</LoadingSegment>
            ) : (
              <Outlet />
            )}
          </div>
        </Container.Page>
      }
    />
  )
}
