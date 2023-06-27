import SplitPane from '@components/splitPane/SplitPane'
import { useStoreContext } from '@context/context'
import { useStoreFileExplorer } from '@context/fileTree'
import LineageFlowProvider from '@components/graph/context'
import { useStoreEditor } from '@context/editor'
import { EnumErrorKey, type ErrorIDE } from '../ide/context'
import { Suspense, lazy, useCallback } from 'react'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'

const FileExplorer = lazy(() => import('@components/fileExplorer/FileExplorer'))
const Editor = lazy(() => import('@components/editor/Editor'))

export default function PageEditor(): JSX.Element {
  const models = useStoreContext(s => s.models)

  const files = useStoreFileExplorer(s => s.files)
  const setSelected = useStoreFileExplorer(s => s.setSelected)

  const setPreviewConsole = useStoreEditor(s => s.setPreviewConsole)

  const handleClickModel = useCallback(
    function handleClickModel(modelName: string): void {
      const model = models.get(modelName)

      if (model == null) return

      setSelected(files.get(model.path))
    },
    [files, models],
  )

  const handleError = useCallback(function handleError(error: ErrorIDE): void {
    setPreviewConsole([EnumErrorKey.ColumnLineage, error])
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
          <FileExplorer />
        </Suspense>
      </div>
      <div className="h-full">
        <LineageFlowProvider
          handleClickModel={handleClickModel}
          handleError={handleError}
        >
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
            <Editor />
          </Suspense>
        </LineageFlowProvider>
      </div>
    </SplitPane>
  )
}
