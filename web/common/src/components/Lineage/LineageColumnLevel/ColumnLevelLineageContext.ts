import React from 'react'

import {
  type AdjacencyListColumnKey,
  type AdjacencyListKey,
  type PortId,
} from '../utils'

export type LineageColumn = {
  source?: string | null
  expression?: string | null
  models: Record<string, string[]>
}

export type ColumnLevelModelConnections = Record<
  AdjacencyListKey,
  AdjacencyListKey[]
>
export type ColumnLevelDetails = Omit<LineageColumn, 'models'> & {
  models: ColumnLevelModelConnections
}
export type ColumnLevelConnections = Record<
  AdjacencyListColumnKey,
  ColumnLevelDetails
>
export type ColumnLevelLineageAdjacencyList = Record<
  AdjacencyListKey,
  ColumnLevelConnections
>

export type ColumnLevelLineageContextValue = {
  adjacencyListColumnLevel: ColumnLevelLineageAdjacencyList
  selectedColumns: Set<PortId>
  columnLevelLineage: Map<PortId, ColumnLevelLineageAdjacencyList>
  setColumnLevelLineage: React.Dispatch<
    React.SetStateAction<Map<PortId, ColumnLevelLineageAdjacencyList>>
  >
  showColumns: boolean
  setShowColumns: React.Dispatch<React.SetStateAction<boolean>>
  fetchingColumns: Set<PortId>
  setFetchingColumns: React.Dispatch<React.SetStateAction<Set<PortId>>>
}

export const initial = {
  adjacencyListColumnLevel: {},
  selectedColumns: new Set<PortId>(),
  columnLevelLineage: new Map<PortId, ColumnLevelLineageAdjacencyList>(),
  setColumnLevelLineage: () => {},
  showColumns: false,
  setShowColumns: () => {},
  fetchingColumns: new Set<PortId>(),
  setFetchingColumns: () => {},
}

export type ColumnLevelLineageContextHook = () => ColumnLevelLineageContextValue
