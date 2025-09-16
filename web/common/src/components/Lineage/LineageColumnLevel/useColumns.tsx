import React from 'react'

import { toPortID } from '../utils'
import {
  type AdjacencyListColumnKey,
  type AdjacencyListKey,
  type PortId,
} from '../utils'

export interface Column {
  data_type: string
  description?: string | null
}

export function useColumns(
  selectedPorts: Set<PortId>,
  adjacencyListKey: AdjacencyListKey,
  rawColumns: Record<AdjacencyListColumnKey, Column> = {},
) {
  const columnNames = React.useMemo(() => {
    return new Set<PortId>(
      Object.keys(rawColumns ?? {}).map(column =>
        toPortID(adjacencyListKey, column),
      ),
    )
  }, [rawColumns, adjacencyListKey])

  const [selectedColumns, columns] = React.useMemo(() => {
    const selected = []
    const output = []

    for (const [column, info] of Object.entries(rawColumns) as [
      AdjacencyListColumnKey,
      Column,
    ][]) {
      const columnId = toPortID(adjacencyListKey, column)
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
