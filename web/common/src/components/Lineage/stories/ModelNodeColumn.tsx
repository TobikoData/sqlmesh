import React from 'react'

import { FactoryColumn } from '../LineageColumnLevel/FactoryColumn'

import {
  useModelLineage,
  type ModelColumnID,
  type ModelName,
  type ModelNodeId,
  type ColumnName,
  type BrandedColumnLevelLineageAdjacencyList,
  type LeftPortHandleId,
  type RightPortHandleId,
} from './ModelLineageContext'

const ModelColumn = FactoryColumn<
  ModelName,
  ColumnName,
  ModelNodeId,
  ModelColumnID,
  LeftPortHandleId,
  RightPortHandleId,
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
  columnLineageData,
}: {
  id: ModelColumnID
  nodeId: ModelNodeId
  modelName: ModelName
  name: ColumnName
  type: string
  description?: string | null
  className?: string
  columnLineageData?: BrandedColumnLevelLineageAdjacencyList
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
