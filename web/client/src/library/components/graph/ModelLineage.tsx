import { useApiModelLineage } from '@api/index'
import { debounceAsync } from '@utils/index'
import { useCallback, useEffect } from 'react'
import { ModelColumnLineage } from './Graph'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { useLineageFlow } from './context'
import { mergeLineageWithModels } from './help'

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
  const { setActiveEdges, setConnections, setLineage, models, handleError } =
    useLineageFlow()

  const { refetch: getModelLineage } = useApiModelLineage(model.name)

  const debouncedGetModelLineage = useCallback(
    debounceAsync(getModelLineage, 500),
    [model.name, models],
  )

  useEffect(() => {
    setActiveEdges(new Map())
    setConnections(new Map())

    void debouncedGetModelLineage()
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
    <ModelColumnLineage
      model={model}
      highlightedNodes={highlightedNodes}
      className={className}
    />
  )
}
