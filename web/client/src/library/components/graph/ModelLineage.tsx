import { useApiModelLineage } from '@api/index'
import { useEffect } from 'react'
import { ModelColumnLineage } from './Graph'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { useLineageFlow } from './context'
import { mergeLineageWithModels } from './help'
import { ReactFlowProvider } from 'reactflow'

export default function ModelLineage({
  model,
  fingerprint,
  highlightedNodes,
  className,
}: {
  model: ModelSQLMeshModel
  fingerprint: string | ID
  highlightedNodes?: Record<string, string[]>
  className?: string
}): JSX.Element {
  const { setActiveEdges, setConnections, setLineage, handleError } =
    useLineageFlow()

  const { refetch: getModelLineage } = useApiModelLineage(model.name)

  useEffect(() => {
    setActiveEdges(new Map())
    setConnections(new Map())

    void getModelLineage()
      .then(({ data }) => {
        setLineage(() =>
          data == null ? undefined : mergeLineageWithModels({}, data),
        )
      })
      .catch(error => {
        handleError?.(error)
      })
  }, [fingerprint])

  useEffect(() => {
    setActiveEdges(new Map())
    setConnections(new Map())
  }, [model.name])

  return (
    <ReactFlowProvider>
      <ModelColumnLineage
        model={model}
        highlightedNodes={highlightedNodes}
        className={className}
      />
    </ReactFlowProvider>
  )
}
