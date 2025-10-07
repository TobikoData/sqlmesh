import React from 'react'

import { FactoryColumn } from '@tobikodata/sqlmesh-common/lineage'

import {
  useModelLineage,
  type ModelColumnID,
  type ModelNodeId,
  type ColumnName,
  type BrandedColumnLevelLineageAdjacencyList,
} from './ModelLineageContext'
import { cn } from '@tobikodata/sqlmesh-common'
import { useApiColumnLineage } from '@/api/index'
import type { ModelFQN } from '@/domain/models'

const ModelColumn = FactoryColumn<
  ModelFQN,
  ColumnName,
  ModelNodeId,
  ModelColumnID
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
  name: ColumnName
  type: string
  description?: string | null
  className?: string
}) {
  const {
    selectedColumns,
    setColumnLevelLineage,
    setFetchingColumns,
    setSelectedNodeId,
  } = useModelLineage()

  const isSelectedColumn = selectedColumns.has(id)

  const {
    refetch: getColumnLineage,
    isFetching: isColumnLineageFetching,
    error: columnLineageError,
  } = useApiColumnLineage(nodeId, name, { models_only: true })

  const [columnLineageData, setColumnLineageData] = React.useState<
    BrandedColumnLevelLineageAdjacencyList<ModelFQN, ColumnName> | undefined
  >(undefined)

  const toggleSelectedColumn = React.useCallback(async () => {
    if (isSelectedColumn) {
      setColumnLevelLineage(prev => {
        prev.delete(id)
        return new Map(prev)
      })
    } else {
      setSelectedNodeId(nodeId)

      if (columnLineageData == null) {
        setTimeout(() => {
          setFetchingColumns(prev => new Set(prev.add(id)))
        })

        const { data } = (await getColumnLineage()) as {
          data:
            | BrandedColumnLevelLineageAdjacencyList<ModelFQN, ColumnName>
            | undefined
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
        'ModelNodeColumn',
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
