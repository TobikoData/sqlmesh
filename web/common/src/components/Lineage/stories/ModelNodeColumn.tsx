import React from 'react'

import { type ColumnLevelLineageAdjacencyList } from '../LineageColumnLevel/ColumnLevelLineageContext'
import { FactoryColumn } from '../LineageColumnLevel/FactoryColumn'
import {
  type AdjacencyListColumnKey,
  type AdjacencyListKey,
  type NodeId,
  type PortId,
} from '../utils'
import { useModelLineage } from './ModelLineageContext'

const ModelColumn = FactoryColumn(useModelLineage)

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
  id: PortId
  nodeId: NodeId
  modelName: AdjacencyListKey
  name: AdjacencyListColumnKey
  type: string
  description?: string | null
  className?: string
  columnLineageData?: ColumnLevelLineageAdjacencyList
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
    />
  )
})
