import { Suspense, lazy, useCallback, useEffect } from 'react'
import { useStoreContext } from '@context/context'
import { useStoreProject } from '@context/project'
import { EnumErrorKey, useIDE, type ErrorIDE } from '../ide/context'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import Page from '../root/Page'
import { isNil } from '@utils/index'

const FileExplorer = lazy(() => import('@components/fileExplorer/FileExplorer'))
const FileExplorerProvider = lazy(
  () => import('@components/fileExplorer/context'),
)
const Editor = lazy(() => import('@components/editor/Editor'))
const LineageFlowProvider = lazy(() => import('@components/graph/context'))

export default function PageEditor(): JSX.Element {
  const { addError } = useIDE()

  const models = useStoreContext(s => s.models)
  const lastSelectedModel = useStoreContext(s => s.lastSelectedModel)
  const setLastSelectedModel = useStoreContext(s => s.setLastSelectedModel)

  const files = useStoreProject(s => s.files)
  const selectedFile = useStoreProject(s => s.selectedFile)
  const setSelectedFile = useStoreProject(s => s.setSelectedFile)

  const handleClickModel = useCallback(
    function handleClickModel(modelName: string): void {
      const model = models.get(modelName)

      if (isNil(model)) return

      setLastSelectedModel(model)
    },
    [files, models],
  )

  const handleError = useCallback(function handleError(error: ErrorIDE): void {
    addError(EnumErrorKey.ColumnLineage, error)
  }, [])

  useEffect(() => {
    if (isNil(lastSelectedModel)) return

    const file = files.get(lastSelectedModel.path)

    if (isNil(file)) return

    setSelectedFile(file)
  }, [lastSelectedModel])

  useEffect(() => {
    if (isNil(selectedFile)) return

    setLastSelectedModel(models.get(selectedFile.path))
  }, [selectedFile])

  return (
    <Page
      sidebar={
        <Suspense
          fallback={
            <div className="flex justify-center items-center w-full h-full">
              <Loading className="inline-block">
                <Spinner className="w-3 h-3 border border-neutral-10 mr-4" />
                <h3 className="text-md">Getting Files...</h3>
              </Loading>
            </div>
          }
        >
          <FileExplorerProvider>
            <FileExplorer />
          </FileExplorerProvider>
        </Suspense>
      }
      content={
        <Suspense
          fallback={
            <div className="flex justify-center items-center w-full h-full">
              <Loading className="inline-block">
                <Spinner className="w-3 h-3 border border-neutral-10 mr-4" />
                <h3 className="text-md">Getting Editor Ready...</h3>
              </Loading>
            </div>
          }
        >
          <LineageFlowProvider
            showColumns={true}
            handleClickModel={handleClickModel}
            handleError={handleError}
          >
            <Editor />
          </LineageFlowProvider>
        </Suspense>
      }
    />
  )
}
