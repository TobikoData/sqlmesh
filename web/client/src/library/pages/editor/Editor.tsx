import { Suspense, lazy, useCallback } from 'react'
import { useStoreContext } from '@context/context'
import { useStoreProject } from '@context/project'
import {
  EnumErrorKey,
  useNotificationCenter,
  type ErrorIDE,
} from '../root/context/notificationCenter'
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
  const { addError } = useNotificationCenter()

  const modules = useStoreContext(s => s.modules)
  const models = useStoreContext(s => s.models)
  const setLastSelectedModel = useStoreContext(s => s.setLastSelectedModel)

  const files = useStoreProject(s => s.files)

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

  return (
    <Page
      sidebar={
        modules.hasFiles ? (
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
        ) : undefined
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
