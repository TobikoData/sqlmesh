import { useParams } from 'react-router-dom'
import LineageFlowProvider from '@/components/graph/context'
import ModelLineage from '@/components/graph/ModelLineage'

export default function PageLineage(): JSX.Element {
  const { modelName } = useParams()

  const models: Model[] = [] 
  const lastSelectedModel = 'temp'
  const 

  return (
    <div className="flex overflow-hidden w-full h-full">
      <LineageFlowProvider
        showColumns={true}
        handleClickModel={() => console.log('click')}
        handleError={() => console.log('error')}
      >
        <ModelLineage model={model!} />
      </LineageFlowProvider>
    </div>
  )
}
