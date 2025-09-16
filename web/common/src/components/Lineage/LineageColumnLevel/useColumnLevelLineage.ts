import merge from 'deepmerge'
import React from 'react'

import { type PortId } from '../utils'
import { type ColumnLevelLineageAdjacencyList } from './ColumnLevelLineageContext'
import {
  getAdjacencyListKeysFromColumnLineage,
  getConnectedColumnsIDs,
} from './help'

export function useColumnLevelLineage(
  columnLevelLineage: Map<PortId, ColumnLevelLineageAdjacencyList>,
) {
  const adjacencyListColumnLevel = React.useMemo(() => {
    return merge.all(Array.from(columnLevelLineage.values()), {
      arrayMerge: (dest, source) => Array.from(new Set([...dest, ...source])),
    }) as ColumnLevelLineageAdjacencyList
  }, [columnLevelLineage])

  const selectedColumns = React.useMemo(() => {
    return getConnectedColumnsIDs(adjacencyListColumnLevel)
  }, [adjacencyListColumnLevel])

  const adjacencyListKeysColumnLevel = React.useMemo(() => {
    return adjacencyListColumnLevel != null
      ? getAdjacencyListKeysFromColumnLineage(adjacencyListColumnLevel)
      : []
  }, [adjacencyListColumnLevel])

  return {
    adjacencyListColumnLevel,
    selectedColumns,
    adjacencyListKeysColumnLevel,
  }
}
