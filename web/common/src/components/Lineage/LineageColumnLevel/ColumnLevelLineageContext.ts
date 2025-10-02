import React from 'react'

import { type PortId } from '../utils'

export type LineageColumn = {
  source?: string | null
  expression?: string | null
  models: Record<string, string[]>
}

export type ColumnLevelModelConnections<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
> = Record<TAdjacencyListKey, TAdjacencyListColumnKey[]>
export type ColumnLevelDetails<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
> = Omit<LineageColumn, 'models'> & {
  models: ColumnLevelModelConnections<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  >
}
export type ColumnLevelConnections<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
> = Record<
  TAdjacencyListColumnKey,
  ColumnLevelDetails<TAdjacencyListKey, TAdjacencyListColumnKey>
>
export type ColumnLevelLineageAdjacencyList<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
> = Record<
  TAdjacencyListKey,
  ColumnLevelConnections<TAdjacencyListKey, TAdjacencyListColumnKey>
>

export type ColumnLevelLineageContextValue<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
  TColumnID extends string = PortId,
> = {
  adjacencyListColumnLevel: ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  >
  selectedColumns: Set<TColumnID>
  columnLevelLineage: Map<
    TColumnID,
    ColumnLevelLineageAdjacencyList<TAdjacencyListKey, TAdjacencyListColumnKey>
  >
  setColumnLevelLineage: React.Dispatch<
    React.SetStateAction<
      Map<
        TColumnID,
        ColumnLevelLineageAdjacencyList<
          TAdjacencyListKey,
          TAdjacencyListColumnKey
        >
      >
    >
  >
  showColumns: boolean
  setShowColumns: React.Dispatch<React.SetStateAction<boolean>>
  fetchingColumns: Set<TColumnID>
  setFetchingColumns: React.Dispatch<React.SetStateAction<Set<TColumnID>>>
}

export function getColumnLevelLineageContextInitial<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
  TColumnID extends string = PortId,
>() {
  return {
    adjacencyListColumnLevel: {},
    columnLevelLineage: new Map<
      TColumnID,
      ColumnLevelLineageAdjacencyList<
        TAdjacencyListKey,
        TAdjacencyListColumnKey
      >
    >(),
    setColumnLevelLineage: () => {},
    showColumns: false,
    setShowColumns: () => {},
    selectedColumns: new Set<TColumnID>(),
    fetchingColumns: new Set<TColumnID>(),
    setFetchingColumns: () => {},
  } as const
}

export type ColumnLevelLineageContextHook<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
  TColumnID extends string = PortId,
> = () => ColumnLevelLineageContextValue<
  TAdjacencyListKey,
  TAdjacencyListColumnKey,
  TColumnID
>
