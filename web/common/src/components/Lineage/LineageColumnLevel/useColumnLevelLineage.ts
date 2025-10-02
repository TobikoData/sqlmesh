import merge from 'deepmerge'
import React from 'react'

import { type PortId } from '../utils'
import { type ColumnLevelLineageAdjacencyList } from './ColumnLevelLineageContext'
import {
  getAdjacencyListKeysFromColumnLineage,
  getConnectedColumnsIDs,
} from './help'

export function useColumnLevelLineage<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
  TColumnID extends string = PortId,
>(
  columnLevelLineage: Map<
    TColumnID,
    ColumnLevelLineageAdjacencyList<TAdjacencyListKey, TAdjacencyListColumnKey>
  >,
) {
  const adjacencyListColumnLevel = React.useMemo(() => {
    return merge.all(Array.from(columnLevelLineage.values()), {
      arrayMerge: (dest, source) => Array.from(new Set([...dest, ...source])),
    }) as ColumnLevelLineageAdjacencyList<
      TAdjacencyListKey,
      TAdjacencyListColumnKey
    >
  }, [columnLevelLineage])

  const selectedColumns = React.useMemo(() => {
    return getConnectedColumnsIDs<
      TAdjacencyListKey,
      TAdjacencyListColumnKey,
      TColumnID
    >(adjacencyListColumnLevel)
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
