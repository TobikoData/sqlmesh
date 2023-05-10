import { useApiModelLineage } from '@api/index'
import { debounceAsync } from '@utils/index'
import { memo, useCallback, useEffect } from 'react'
import { ModelColumnLineage } from './Graph'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { mergeLineage, useLineageFlow } from './context'
import { type Lineage } from '@context/editor'

const ModelLineage = memo(function ModelLineage({
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
  const { clearActiveEdges, models, lineage, setLineage } = useLineageFlow()

  const { data: dataLineage, refetch: getModelLineage } = useApiModelLineage(
    model.name,
  )

  const debouncedGetModelLineage = useCallback(
    debounceAsync(getModelLineage, 500),
    [model.name, fingerprint],
  )

  useEffect(() => {
    void debouncedGetModelLineage()
  }, [debouncedGetModelLineage])

  useEffect(() => {
    if (dataLineage == null) {
      setLineage(undefined)
    } else {
      const lineageModels = Object.keys(dataLineage).reduce(
        (acc: Record<string, Lineage>, key) => {
          if (models.has(key)) {
            acc[key] = {
              models: (dataLineage[key] ?? []).filter(name => models.has(name)),
              columns: lineage?.[key]?.columns ?? undefined,
            }
          }

          return acc
        },
        {},
      )

      setLineage(mergeLineage(models, lineageModels))
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
})

export default ModelLineage
