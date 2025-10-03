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
import type { ModelName } from '@/domain/models'

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
  columnLineageData,
}: {
  id: ModelColumnID
  nodeId: ModelNodeId
  modelName: ModelName
  name: ColumnName
  type: string
  description?: string | null
  className?: string
  columnLineageData?: ColumnLevelLineageAdjacencyList<ModelName, ColumnName>
}) {
  const { selectedColumns, setColumnLevelLineage } = useModelLineage()

  const isSelectedColumn = selectedColumns.has(id)

  async function toggleSelectedColumn() {
    if (isSelectedColumn) {
      setColumnLevelLineage(prev => {
        prev.delete(id)
        return new Map(prev)
      })
    } else {
      if (columnLineageData != null) {
        setColumnLevelLineage(prev => new Map(prev).set(id, columnLineageData))
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
      className={className}
      data={columnLineageData}
      error={null}
      isFetching={false}
      onClick={toggleSelectedColumn}
      onCancel={() => console.log('cancel')}
      renderError={error => <div>Error: {error.message}</div>}
      renderExpression={expression => <div>{expression}</div>}
      renderSource={source => <div>{source}</div>}
    />
  )
})
