import Documentation from '@components/documentation/Documentation'
import ModelLineage from '@components/graph/ModelLineage'
import SplitPane from '@components/splitPane/SplitPane'
import { useStoreContext } from '@context/context'
import { useNavigate, useParams } from 'react-router-dom'
import NotFound from '../root/NotFound'
import { EnumRoutes } from '~/routes'
import LineageFlowProvider from '@components/graph/context'
import { type ErrorIDE } from '../ide/context'
import { isNil, isNotNil } from '@utils/index'
import { useEffect } from 'react'
import { useStoreProject } from '@context/project'
import {
  CodeEditorRemoteFile,
  CodeEditorDefault,
} from '@components/editor/EditorCode'
import TabList from '@components/tab/Tab'
import { Tab } from '@headlessui/react'
import { EnumFileExtensions } from '@models/file'
import { useSQLMeshModelExtensions } from '@components/editor/hooks'

export default function Content(): JSX.Element {
  const { modelName } = useParams()
  const navigate = useNavigate()

  const models = useStoreContext(s => s.models)
  const lastSelectedModel = useStoreContext(s => s.lastSelectedModel)
  const setLastSelectedModel = useStoreContext(s => s.setLastSelectedModel)

  const files = useStoreProject(s => s.files)
  const setSelectedFile = useStoreProject(s => s.setSelectedFile)

  const model = isNil(modelName)
    ? lastSelectedModel
    : models.get(encodeURI(modelName))

  useEffect(() => {
    if (isNotNil(model)) {
      const file = files.get(model.path)

      if (isNil(file)) return

      setSelectedFile(file)
    }

    setLastSelectedModel(model)
  }, [model, model?.sql])

  const modelExtensions = isNil(model)
    ? []
    : useSQLMeshModelExtensions(model.path, model => {
        handleClickModel?.(model.name)
      })

  function handleClickModel(modelName: string): void {
    const model = models.get(modelName)

    if (isNil(model)) return

    navigate(EnumRoutes.IdeDocsModels + '/' + model.name)
  }

  function handleError(error: ErrorIDE): void {
    console.log(error?.message)
  }

  return (
    <div className="flex overflow-auto w-full h-full">
      {isNil(model) ? (
        <NotFound
          link={EnumRoutes.IdeDocs}
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
            sizes={[65, 35]}
            minSize={0}
            snapOffset={0}
          >
            <div className="flex flex-col h-full">
              <SplitPane
                direction="vertical"
                sizes={[50, 50]}
                minSize={100}
                snapOffset={0}
                className="flex flex-col w-full h-full overflow-hidden"
              >
                <div className="flex flex-col h-full">
                  <CodeEditorRemoteFile
                    key={model.path}
                    path={model.path}
                  >
                    {({ file }) => (
                      <Tab.Group>
                        <TabList
                          list={
                            [
                              'Source Code',
                              model.isModelSQL && 'Compiled Query',
                            ].filter(Boolean) as string[]
                          }
                          className="!justify-center"
                        />
                        <Tab.Panels className="h-full w-full overflow-hidden text-xs">
                          <Tab.Panel
                            unmount={false}
                            className="w-full h-full"
                          >
                            <CodeEditorDefault
                              content={file.content}
                              type={file.extension}
                              extensions={modelExtensions}
                            />
                          </Tab.Panel>
                          {model.isModelSQL && (
                            <Tab.Panel className="w-full h-full">
                              <CodeEditorDefault
                                type={EnumFileExtensions.SQL}
                                content={model.sql ?? ''}
                                extensions={modelExtensions}
                              />
                            </Tab.Panel>
                          )}
                        </Tab.Panels>
                      </Tab.Group>
                    )}
                  </CodeEditorRemoteFile>
                </div>
                <div className="flex flex-col h-full px-2">
                  <ModelLineage model={model} />
                </div>
              </SplitPane>
            </div>
            <div className="flex flex-col h-full">
              <Documentation
                model={model}
                withQuery={model.isModelSQL}
              />
            </div>
          </SplitPane>
        </LineageFlowProvider>
      )}
    </div>
  )
}
