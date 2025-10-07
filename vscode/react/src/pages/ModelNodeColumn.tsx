import React from 'react'

import {
  FactoryColumn,
  type ColumnLevelLineageAdjacencyList,
} from '@tobikodata/sqlmesh-common/lineage'

import {
  useModelLineage,
  type ModelColumnID,
  type ModelNodeId,
  type ColumnName,
} from './ModelLineageContext'
import { cn } from '@tobikodata/sqlmesh-common'
import type { ModelName } from '@/domain/models'
import { useApiColumnLineage } from '@/api/index'

const ModelColumn = FactoryColumn<
  ModelName,
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
  modelName: ModelName
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
    data: columnLineageData,
    refetch: getColumnLineage,
    isFetching: isColumnLineageFetching,
    error: columnLineageError,
  } = useApiColumnLineage(nodeId, name, { models_only: true })

  async function toggleSelectedColumn() {
    if (isSelectedColumn) {
      setColumnLevelLineage(prev => {
        prev.delete(id)
        return new Map(prev)
      })
    } else {
      setSelectedNodeId(nodeId)

      let columnLevelLineage =
        columnLineageData as ColumnLevelLineageAdjacencyList<
          ModelName,
          ColumnName
        >

      if (columnLineageData == null) {
        setTimeout(() => {
          setFetchingColumns(prev => new Set(prev.add(id)))
        })

        const { data } = await getColumnLineage()

        columnLevelLineage = data as ColumnLevelLineageAdjacencyList<
          ModelName,
          ColumnName
        >

        setFetchingColumns(prev => {
          prev.delete(id)
          return new Set(prev)
        })
      }

      if (columnLevelLineage != null) {
        setColumnLevelLineage(prev => new Map(prev).set(id, columnLevelLineage))
      }
    }
  }

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
      data={
        columnLineageData as ColumnLevelLineageAdjacencyList<
          ModelName,
          ColumnName
        >
      }
      isFetching={isColumnLineageFetching}
      error={columnLineageError as Error | null}
      onClick={toggleSelectedColumn}
      renderError={error => <div>Error: {error.message}</div>}
    />
  )
})
