import React from 'react'

import { type PortId } from '../utils'

export type ColumnLevelLineageAdjacencyList<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
> = {
  [K in TAdjacencyListKey]: {
    [C in TAdjacencyListColumnKey]: {
      source?: string | null
      expression?: string | null
      models: Record<TAdjacencyListKey, TAdjacencyListColumnKey[]>
    }
  }
}

export type ColumnLevelLineageContextValue<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
  TColumnID extends string = PortId,
  TColumnLevelLineageAdjacencyList extends ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  > = ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  >,
> = {
  adjacencyListColumnLevel: TColumnLevelLineageAdjacencyList
  selectedColumns: Set<TColumnID>
  columnLevelLineage: Map<TColumnID, TColumnLevelLineageAdjacencyList>
  setColumnLevelLineage: React.Dispatch<
    React.SetStateAction<Map<TColumnID, TColumnLevelLineageAdjacencyList>>
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
  TColumnLevelLineageAdjacencyList extends ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  > = ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  >,
>() {
  return {
    adjacencyListColumnLevel: {} as TColumnLevelLineageAdjacencyList,
    columnLevelLineage: new Map<TColumnID, TColumnLevelLineageAdjacencyList>(),
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
  TColumnLevelLineageAdjacencyList extends ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  > = ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  >,
> = () => ColumnLevelLineageContextValue<
  TAdjacencyListKey,
  TAdjacencyListColumnKey,
  TColumnID,
  TColumnLevelLineageAdjacencyList
>
