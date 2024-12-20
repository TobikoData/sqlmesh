import Documentation from '@components/documentation/Documentation'
import ModelLineage from '@components/graph/ModelLineage'
import { useStoreContext } from '@context/context'
import { useNavigate, useParams } from 'react-router-dom'
import NotFound from '../root/NotFound'
import { EnumRoutes } from '~/routes'
import LineageFlowProvider from '@components/graph/context'
import { type ErrorIDE } from '../root/context/notificationCenter'
import { isNil, isNotNil } from '@utils/index'
import {
  CodeEditorRemoteFile,
  CodeEditorDefault,
} from '@components/editor/EditorCode'
import { EnumFileExtensions } from '@models/file'
import { useSQLMeshModelExtensions } from '@components/editor/hooks'
import { useEffect } from 'react'
import { useApiModel } from '@api/index'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import {
  TBKSplitPane,
  TBKTab,
  TBKTabs,
  TBKTabPanel,
} from '@utils/additional-components'

export default function PageDataCatalog(): JSX.Element {
  const { modelName = '' } = useParams()

  const navigate = useNavigate()

  const models = useStoreContext(s => s.models)
  const lastSelectedModel = useStoreContext(s => s.lastSelectedModel)
  const setLastSelectedModel = useStoreContext(s => s.setLastSelectedModel)

  const {
    refetch: getModel,
    cancel: cancelRequestModel,
    isFetching: isFetchingModel,
  } = useApiModel(modelName)

  const model =
    isNil(modelName) || modelName === lastSelectedModel?.name
      ? lastSelectedModel
      : models.get(encodeURI(modelName))

  const modelExtensions = useSQLMeshModelExtensions(model?.path, model => {
    handleClickModel?.(model.name)
  })

  useEffect(() => {
    if (isNil(model)) return

    void getModel().then(({ data }) => {
      model.update(data)

      setLastSelectedModel(model)
    })

    return () => {
      cancelRequestModel()
    }
  }, [modelName, model?.hash, model?.path, model?.name])

  function handleClickModel(modelName: string): void {
    const model = models.get(modelName)

    if (isNil(model)) return

    navigate(`${EnumRoutes.DataCatalogModels}/${model.name}`)
  }

  function handleError(error: ErrorIDE): void {
    console.log(error?.message)
  }

  return (
    <div className="flex overflow-auto w-full h-full">
      {isNil(model) ? (
        <NotFound
          link={EnumRoutes.DataCatalog}
          description={
            isNil(modelName) ? undefined : `Model ${modelName} Does Not Exist`
          }
          message="Back To Data Catalog"
        />
      ) : (
        <LineageFlowProvider
          showColumns={true}
          handleClickModel={handleClickModel}
          handleError={handleError}
        >
          <TBKSplitPane
            vertical
            className="w-full h-full overflow-hidden"
            position="60"
          >
            <div slot="divider"></div>
            <div
              slot="start"
              className="flex w-full"
            >
              <TBKSplitPane
                className="w-full h-full overflow-hidden"
                position="60"
              >
                <div slot="divider"></div>
                <div
                  slot="end"
                  className="flex flex-col h-full overflow-hidden"
                >
                  <Documentation model={model} />
                </div>
                <div
                  slot="start"
                  className="flex w-full h-full overflow-hidden pl-2"
                >
                  <TBKTabs
                    size="2xs"
                    variant="primary"
                  >
                    <TBKTab
                      slot="nav"
                      panel="source"
                    >
                      Source Code
                    </TBKTab>
                    {model.isModelSQL && (
                      <TBKTab
                        slot="nav"
                        panel="compiled"
                      >
                        Compiled Query
                      </TBKTab>
                    )}
                    <TBKTabPanel name="source">
                      {isFetchingModel ? (
                        <div className="flex justify-center items-center w-full h-full">
                          <Loading className="inline-block">
                            <Spinner className="w-5 h-5 border border-neutral-10 mr-4" />
                            <h3 className="text-xl">Waiting for Model...</h3>
                          </Loading>
                        </div>
                      ) : isNil(model.definition) && isNotNil(model.path) ? (
                        <CodeEditorRemoteFile path={model.path}>
                          {({ file }) => (
                            <CodeEditorDefault
                              content={file.content}
                              type={file.extension}
                              extensions={modelExtensions}
                            />
                          )}
                        </CodeEditorRemoteFile>
                      ) : (
                        <>
                          {isNil(model.definition) ? (
                            <div className="flex justify-center items-center w-full h-full">
                              <h3 className="text-xl">Definition Not Found</h3>
                            </div>
                          ) : (
                            <CodeEditorDefault
                              content={model.definition}
                              type={
                                model.isModelPython
                                  ? EnumFileExtensions.PY
                                  : EnumFileExtensions.SQL
                              }
                              extensions={modelExtensions}
                            />
                          )}
                        </>
                      )}
                    </TBKTabPanel>
                    {model.isModelSQL && (
                      <TBKTabPanel name="compiled">
                        <CodeEditorDefault
                          type={EnumFileExtensions.SQL}
                          content={model.sql ?? ''}
                          extensions={modelExtensions}
                        />
                      </TBKTabPanel>
                    )}
                  </TBKTabs>
                </div>
              </TBKSplitPane>
            </div>
            <div
              slot="end"
              className="flex w-full relative"
            >
              <ModelLineage model={model} />
            </div>
          </TBKSplitPane>
        </LineageFlowProvider>
      )}
    </div>
  )
}
