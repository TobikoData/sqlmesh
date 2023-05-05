import Documentation from '@components/documentation/Documentation'
import { ModelLineage } from '@components/graph/ModelLineage'
import SplitPane from '@components/splitPane/SplitPane'
import { useStoreContext } from '@context/context'
import { useNavigate, useParams } from 'react-router-dom'
import NotFound from '../root/NotFound'
import { EnumRoutes } from '~/routes'
import { ModelSQLMeshModel } from '@models/sqlmesh-model'
import LineageFlowProvider from '@components/graph/context'

export default function Content(): JSX.Element {
  const { modelName } = useParams()
  const navigate = useNavigate()

  const models = useStoreContext(s => s.models)
  const model =
    modelName == null
      ? undefined
      : models.get(ModelSQLMeshModel.decodeName(modelName))

  function handleClickModel(modelName: string): void {
    const model = models.get(modelName)

    if (model == null) return

    navigate(
      EnumRoutes.IdeDocsModels + '/' + ModelSQLMeshModel.encodeName(model.name),
    )
  }

  return (
    <div className="flex overflow-auto w-full h-full">
      {model == null ? (
        <NotFound
          link={EnumRoutes.IdeDocs}
          message="Back to docs"
        />
      ) : (
        <LineageFlowProvider handleClickModel={handleClickModel}>
          <SplitPane
            className="flex h-full w-full"
            sizes={[50, 50]}
            minSize={0}
            snapOffset={0}
          >
            <div className="flex flex-col h-full bg-theme-darker dark:bg-theme-lighter round">
              <Documentation
                model={model}
                withQuery={model.type !== 'python'}
              />
            </div>
            <div className="flex flex-col h-full px-2">
              <ModelLineage
                model={model}
                fingerprint={model.id as string}
              />
            </div>
          </SplitPane>
        </LineageFlowProvider>
      )}
    </div>
  )
}
