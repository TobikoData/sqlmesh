import Documentation from '@components/documentation/Documentation'
import ModelLineage from '@components/graph/ModelLineage'
import SplitPane from '@components/splitPane/SplitPane'
import { useStoreContext } from '@context/context'
import { useNavigate, useParams } from 'react-router-dom'
import NotFound from '../root/NotFound'
import { EnumRoutes } from '~/routes'
import LineageFlowProvider from '@components/graph/context'
import { type ErrorIDE } from '../root/context/notificationCenter'
import { isFalse, isNil, isNotNil } from '@utils/index'
import {
  CodeEditorRemoteFile,
  CodeEditorDefault,
} from '@components/editor/EditorCode'
import TabList from '@components/tab/Tab'
import { Tab } from '@headlessui/react'
import { EnumFileExtensions } from '@models/file'
import { useSQLMeshModelExtensions } from '@components/editor/hooks'
import { useEffect, useState, type MouseEvent } from 'react'
import { Button } from '@components/button/Button'
import {
  ArrowsPointingOutIcon,
  ArrowsPointingInIcon,
} from '@heroicons/react/20/solid'
import clsx from 'clsx'
import { EnumSize, EnumVariant } from '~/types/enum'
import { useApiModel } from '@api/index'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'

export default function PageDocs(): JSX.Element {
  const { modelName = '' } = useParams()

  const navigate = useNavigate()

  const models = useStoreContext(s => s.models)
  const lastSelectedModel = useStoreContext(s => s.lastSelectedModel)
  const setLastSelectedModel = useStoreContext(s => s.setLastSelectedModel)

  const [splitPaneH, setSplitPaneH] = useState([65, 35])
  const [splitPaneV, setSplitPaneV] = useState([50, 50])
  const [fullscreenLineage, setFullscreenLineage] = useState(false)
  const [fullscreenQuery, setFullscreenQuery] = useState(false)

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

    navigate(EnumRoutes.DocsModels + '/' + model.name)
  }

  function handleError(error: ErrorIDE): void {
    console.log(error?.message)
  }

  return (
    <div className="flex overflow-auto w-full h-full">
      {isNil(model) ? (
        <NotFound
          link={EnumRoutes.Docs}
          description={
            isNil(modelName) ? undefined : `Model ${modelName} Does Not Exist`
          }
          message="Back To Docs"
        />
      ) : (
        <LineageFlowProvider
          showColumns={true}
          handleClickModel={handleClickModel}
          handleError={handleError}
        >
          <SplitPane
            className="flex h-full w-full"
            sizes={fullscreenLineage || fullscreenQuery ? [100, 0] : splitPaneV}
            minSize={0}
            snapOffset={0}
            onDragEnd={sizes => setSplitPaneV(sizes)}
          >
            <div className="flex flex-col h-full">
              <SplitPane
                direction="vertical"
                sizes={
                  fullscreenLineage
                    ? [0, 100]
                    : fullscreenQuery
                    ? [100, 0]
                    : splitPaneH
                }
                minSize={0}
                snapOffset={0}
                className="flex flex-col w-full h-full overflow-hidden"
                onDragEnd={sizes => setSplitPaneH(sizes)}
              >
                <div className="flex flex-col h-full relative overflow-hidden">
                  <Button
                    className={clsx(
                      'absolute top-0 right-1 h-8 w-8 !px-0 !bg-light !text-neutral-500 shadow-xl z-10',
                    )}
                    variant={EnumVariant.Info}
                    size={EnumSize.sm}
                    onClick={(e: MouseEvent) => {
                      e.stopPropagation()

                      setFullscreenQuery(oldValue => isFalse(oldValue))
                    }}
                  >
                    {fullscreenLineage ? (
                      <ArrowsPointingInIcon className="w-4 h-4" />
                    ) : (
                      <ArrowsPointingOutIcon className="w-4 h-4" />
                    )}
                  </Button>
                  <Tab.Group>
                    <TabList
                      list={
                        [
                          'Source Code',
                          model.isModelSQL && 'Compiled Query',
                        ].filter(Boolean) as string[]
                      }
                      disabled={isFetchingModel}
                      className="justify-center"
                    />
                    <Tab.Panels className="h-full w-full overflow-hidden text-xs">
                      <Tab.Panel
                        unmount={false}
                        className="w-full h-full"
                      >
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
                                <h3 className="text-xl">
                                  Definition Not Found
                                </h3>
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
                      </Tab.Panel>
                      {model.isModelSQL && (
                        <Tab.Panel
                          unmount={false}
                          className="w-full h-full"
                        >
                          <CodeEditorDefault
                            type={EnumFileExtensions.SQL}
                            content={model.sql ?? ''}
                            extensions={modelExtensions}
                          />
                        </Tab.Panel>
                      )}
                    </Tab.Panels>
                  </Tab.Group>
                </div>
                <div className="flex flex-col h-full relative overflow-hidden">
                  <Button
                    className={clsx(
                      'absolute top-9 right-1 h-8 w-8 !px-0 !bg-light !text-neutral-500 shadow-xl z-10',
                    )}
                    variant={EnumVariant.Info}
                    size={EnumSize.sm}
                    onClick={(e: MouseEvent) => {
                      e.stopPropagation()

                      setFullscreenLineage(oldValue => isFalse(oldValue))
                    }}
                  >
                    {fullscreenLineage ? (
                      <ArrowsPointingInIcon className="w-4 h-4" />
                    ) : (
                      <ArrowsPointingOutIcon className="w-4 h-4" />
                    )}
                  </Button>
                  <ModelLineage model={model} />
                </div>
              </SplitPane>
            </div>
            <div className="flex flex-col h-full">
              <Documentation model={model} />
            </div>
          </SplitPane>
        </LineageFlowProvider>
      )}
    </div>
  )
}
