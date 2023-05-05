import { useApiModelLineage } from '@api/index'
import { debounceAsync } from '@utils/index'
import { useCallback, useEffect } from 'react'
import { ModelColumnLineage } from './Graph'
import { ModelSQLMeshModel } from '@models/sqlmesh-model'
import { useLineageFlow } from './context'
import { type Lineage } from '@context/editor'

export function ModelLineage({
  model,
  fingerprint,
}: {
  model: ModelSQLMeshModel
  fingerprint: string
}): JSX.Element {
  const { clearActiveEdges, refreshModels, models } = useLineageFlow()

  const { data: lineage, refetch: getModelLineage } = useApiModelLineage(
    model.name,
  )

  const debouncedGetModelLineage = useCallback(
    debounceAsync(getModelLineage, 1000, true),
    [model, fingerprint],
  )

  useEffect(() => {
    void debouncedGetModelLineage()
  }, [debouncedGetModelLineage])

  useEffect(() => {
    if (lineage == null) {
      model.update({ lineage: undefined })
    } else {
      model.update({
        lineage: ModelSQLMeshModel.mergeLineage(
          models,
          Object.keys(lineage).reduce((acc: Record<string, Lineage>, key) => {
            acc[key] = {
              models: lineage[key] ?? [],
              columns: model.lineage?.[key]?.columns ?? undefined,
            }

            return acc
          }, {}),
        ),
      })
    }

    refreshModels()
  }, [lineage])

  useEffect(() => {
    clearActiveEdges()
  }, [model])

  return <ModelColumnLineage model={model} />
}
