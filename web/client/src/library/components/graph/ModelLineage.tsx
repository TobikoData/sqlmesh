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
  const { clearActiveEdges, setLineage, models } = useLineageFlow()

  const { data: dataLineage, refetch: getModelLineage } = useApiModelLineage(
    model.name,
  )

  const debouncedGetModelLineage = useCallback(
    debounceAsync(getModelLineage, 500),
    [model.name, models],
  )

  useEffect(() => {
    void debouncedGetModelLineage()
  }, [fingerprint])

  useEffect(() => {
    if (dataLineage == null) {
      setLineage(undefined)
    } else {
      setLineage(lineage =>
        mergeLineageWithModels(structuredClone(lineage), dataLineage),
      )
    }
  }, [dataLineage])

  useEffect(() => {
    clearActiveEdges()
  }, [model.name])

  return (
    <ModelColumnLineage
      model={model}
      highlightedNodes={highlightedNodes}
      className={className}
    />
  )
}
