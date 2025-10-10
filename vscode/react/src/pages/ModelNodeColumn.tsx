import React from 'react'

import { FactoryColumn } from '@sqlmesh-common/components/Lineage'
import { cn } from '@sqlmesh-common/utils'
import {
  useModelLineage,
  type ModelColumnID,
  type ModelNodeId,
  type ModelColumnName,
  type BrandedColumnLevelLineageAdjacencyList,
  type ModelColumnRightHandleId,
  type ModelColumnLeftHandleId,
} from './ModelLineageContext'
import { useApiColumnLineage } from '@/api/index'
import type { ModelFQN } from '@/domain/models'

const ModelColumn = FactoryColumn<
  ModelFQN,
  ModelColumnName,
  ModelNodeId,
  ModelColumnID,
  ModelColumnLeftHandleId,
  ModelColumnRightHandleId,
  BrandedColumnLevelLineageAdjacencyList
>(useModelLineage)

export const ModelNodeColumn = React.memo(function ModelNodeColumn({
  id,
  nodeId,
  modelName,
  name,
  description,
  type,
  className,
}: {
  id: ModelColumnID
  nodeId: ModelNodeId
  modelName: ModelFQN
  name: ModelColumnName
  type: string
  description?: string | null
  className?: string
}) {
  const { selectedColumns, setColumnLevelLineage, setFetchingColumns } =
    useModelLineage()

  const isSelectedColumn = selectedColumns.has(id)

  const {
    refetch: getColumnLineage,
    isFetching: isColumnLineageFetching,
    error: columnLineageError,
  } = useApiColumnLineage(nodeId, name, { models_only: true })

  const [columnLineageData, setColumnLineageData] = React.useState<
    BrandedColumnLevelLineageAdjacencyList | undefined
  >(undefined)

  const toggleSelectedColumn = React.useCallback(async () => {
    if (isSelectedColumn) {
      setColumnLevelLineage(prev => {
        prev.delete(id)
        return new Map(prev)
      })
    } else {
      if (columnLineageData == null) {
        setTimeout(() => {
          setFetchingColumns(prev => new Set(prev.add(id)))
        })

        const { data } = (await getColumnLineage()) as {
          data: BrandedColumnLevelLineageAdjacencyList | undefined
        }

        setColumnLineageData(data)

        setFetchingColumns(prev => {
          prev.delete(id)
          return new Set(prev)
        })

        if (data != null) {
          setColumnLevelLineage(prev => new Map(prev).set(id, data))
        }
      }
    }
  }, [
    isSelectedColumn,
    id,
    setColumnLevelLineage,
    columnLineageData,
    setFetchingColumns,
    getColumnLineage,
  ])

  return (
    <ModelColumn
      id={id}
      nodeId={nodeId}
      modelName={modelName}
      name={name}
      type={type}
      description={description}
      className={cn(
        'ModelNodeColumn cursor-pointer',
        isSelectedColumn && 'bg-lineage-model-column-active-background',
        className,
      )}
      data={columnLineageData}
      isFetching={isColumnLineageFetching}
      error={columnLineageError}
      onClick={toggleSelectedColumn}
      renderError={error => <div>Error: {error.message}</div>}
    />
  )
})
