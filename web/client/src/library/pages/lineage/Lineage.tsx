import { useStoreContext } from '@context/context'
import { useNavigate, useParams } from 'react-router'
import { isNil } from '@utils/index'
import LineageFlowProvider from '@components/graph/context'
import ModelLineage from '@components/graph/ModelLineage'
import { EnumRoutes } from '~/routes'
import { type ErrorIDE } from '../root/context/notificationCenter'

export default function PageLineage(): JSX.Element {
  const navigate = useNavigate()
  const { modelName } = useParams()

  const models = useStoreContext(s => s.models)
  const lastSelectedModel = useStoreContext(s => s.lastSelectedModel)

  const model =
    isNil(modelName) || modelName === lastSelectedModel?.name
      ? lastSelectedModel
      : models.get(encodeURI(modelName))

  function handleClickModel(modelName: string): void {
    const model = models.get(modelName)

    if (isNil(model)) return

    navigate(EnumRoutes.LineageModels + '/' + model.name)
  }

  function handleError(error: ErrorIDE): void {
    console.log(error?.message)
  }

  return (
    <div className="flex overflow-hidden w-full h-full">
      <LineageFlowProvider
        showColumns={true}
        handleClickModel={handleClickModel}
        handleError={handleError}
      >
        <ModelLineage model={model!} />
      </LineageFlowProvider>
    </div>
  )
}
