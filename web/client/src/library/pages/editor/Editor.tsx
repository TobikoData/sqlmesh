import FileTree from '@components/fileTree/FileTree'
import SplitPane from '@components/splitPane/SplitPane'
import { useStoreContext } from '@context/context'
import Editor from '@components/editor/Editor'
import { useStoreFileTree } from '@context/fileTree'
import LineageFlowProvider from '@components/graph/context'
import { useStoreEditor } from '@context/editor'
import { EnumErrorKey, type ErrorIDE } from '../ide/context'

export default function PageEditor(): JSX.Element {
  const models = useStoreContext(s => s.models)
  const environment = useStoreContext(s => s.environment)

  const files = useStoreFileTree(s => s.files)
  const selectFile = useStoreFileTree(s => s.selectFile)
  const project = useStoreFileTree(s => s.project)

  const setPreviewConsole = useStoreEditor(s => s.setPreviewConsole)

  function handleClickModel(modelName: string): void {
    const model = models.get(modelName)

    if (model == null) return

    selectFile(files.get(model.path))
  }

  function handleError(error: ErrorIDE): void {
    setPreviewConsole([EnumErrorKey.ColumnLineage, error])
  }

  return (
    <>
      {environment != null && (
        <SplitPane
          sizes={[20, 80]}
          minSize={[160]}
          snapOffset={0}
          className="flex w-full h-full overflow-hidden"
        >
          <FileTree project={project} />
          <LineageFlowProvider
            handleClickModel={handleClickModel}
            handleError={handleError}
          >
            <Editor />
          </LineageFlowProvider>
        </SplitPane>
      )}
    </>
  )
}
