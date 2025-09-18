import React from 'react'

import { toPortID } from '../utils'
import { type PortId } from '../utils'

export interface Column {
  data_type: string
  description?: string | null
}

export function useColumns<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
  TColumn extends Column,
  TColumnID extends string = PortId,
>(
  selectedPorts: Set<TColumnID>,
  adjacencyListKey: TAdjacencyListKey,
  rawColumns?: Record<TAdjacencyListColumnKey, TColumn>,
) {
  const columnNames = React.useMemo(() => {
    return new Set<TColumnID>(
      Object.keys(rawColumns ?? {}).map(column =>
        toPortID(adjacencyListKey, column),
      ),
    )
  }, [rawColumns, adjacencyListKey])

  const [selectedColumns, columns] = React.useMemo(() => {
    const selected = []
    const output = []

    for (const [column, info] of Object.entries(rawColumns ?? {}) as [
      TAdjacencyListColumnKey,
      TColumn,
    ][]) {
      const columnId = toPortID<TColumnID>(adjacencyListKey, column)
      const nodeColumn = {
        name: column,
        ...info,
        id: columnId,
      }

      if (selectedPorts.has(columnId)) {
        selected.push(nodeColumn)
      } else {
        output.push(nodeColumn)
      }
    }
    return [selected, output]
  }, [rawColumns, adjacencyListKey, selectedPorts])

  return {
    columns,
    columnNames,
    selectedColumns,
  }
}
