import { Suspense, lazy, useCallback } from 'react'
import { useStoreContext } from '@context/context'
import { useStoreProject } from '@context/project'
import { EnumErrorKey, useIDE, type ErrorIDE } from '../ide/context'
import SplitPane from '@components/splitPane/SplitPane'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'

const FileExplorer = lazy(() => import('@components/fileExplorer/FileExplorer'))
const FileExplorerProvider = lazy(
  () => import('@components/fileExplorer/context'),
)
const Editor = lazy(() => import('@components/editor/Editor'))
const LineageFlowProvider = lazy(() => import('@components/graph/context'))

export default function PageEditor(): JSX.Element {
  const { addError } = useIDE()
  const models = useStoreContext(s => s.models)

  const files = useStoreProject(s => s.files)
  const setSelectedFile = useStoreProject(s => s.setSelectedFile)

  const handleClickModel = useCallback(
    function handleClickModel(modelName: string): void {
      const model = models.get(modelName)

      if (model == null) return

      setSelectedFile(files.get(model.path))
    },
    [files, models],
  )

  const handleError = useCallback(function handleError(error: ErrorIDE): void {
    addError(EnumErrorKey.ColumnLineage, error)
  }, [])

  return (
    <SplitPane
      sizes={[20, 80]}
      minSize={[8, 8]}
      snapOffset={0}
      className="flex w-full h-full overflow-hidden"
    >
      <div className="h-full">
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
      </div>
      <div className="h-full">
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
            handleClickModel={handleClickModel}
            handleError={handleError}
          >
            <Editor />
          </LineageFlowProvider>
        </Suspense>
      </div>
    </SplitPane>
  )
}
