import { toEdgeID, toNodeID, toPortID } from '../utils'
import {
  type AdjacencyListColumnKey,
  type AdjacencyListKey,
  type EdgeId,
  type LineageEdge,
  type LineageEdgeData,
  type NodeId,
  type PortId,
  type TransformEdgeFn,
} from '../utils'
import {
  type ColumnLevelConnections,
  type ColumnLevelDetails,
  type ColumnLevelLineageAdjacencyList,
} from './ColumnLevelLineageContext'

export const MAX_COLUMNS_TO_DISPLAY = 5

export function getAdjacencyListKeysFromColumnLineage(
  columnLineage: ColumnLevelLineageAdjacencyList,
) {
  const adjacencyListKeys = new Set<AdjacencyListKey>()

  const targets = Object.entries(columnLineage) as [
    AdjacencyListKey,
    ColumnLevelConnections,
  ][]

  for (const [sourceModelName, targetColumns] of targets) {
    adjacencyListKeys.add(sourceModelName)

    const targetConnections = Object.entries(targetColumns) as [
      AdjacencyListColumnKey,
      ColumnLevelDetails,
    ][]

    for (const [, { models: sourceModels }] of targetConnections) {
      for (const targetModelName of Object.keys(
        sourceModels,
      ) as AdjacencyListKey[]) {
        adjacencyListKeys.add(targetModelName)
      }
    }
  }

  return Array.from(adjacencyListKeys)
}

export function getEdgesFromColumnLineage<
  TEdgeData extends LineageEdgeData = LineageEdgeData,
>({
  columnLineage = {},
  transformEdge,
}: {
  columnLineage: ColumnLevelLineageAdjacencyList
  transformEdge: TransformEdgeFn<TEdgeData>
}) {
  const edges: LineageEdge<TEdgeData>[] = []
  const modelLevelEdgeIDs = new Map<EdgeId, [NodeId, NodeId]>()
  const targets = Object.entries(columnLineage || {}) as [
    AdjacencyListKey,
    ColumnLevelConnections,
  ][]

  for (const [targetModelName, targetColumns] of targets) {
    const targetConnections = Object.entries(targetColumns) as [
      AdjacencyListColumnKey,
      ColumnLevelDetails,
    ][]

    const targetNodeId = toNodeID(targetModelName)

    for (const [
      targetColumnName,
      { models: sourceModels },
    ] of targetConnections) {
      const sources = Object.entries(sourceModels) as [
        AdjacencyListKey,
        AdjacencyListKey[],
      ][]

      for (const [sourceModelName, sourceColumns] of sources) {
        const sourceNodeId = toNodeID(sourceModelName)

        modelLevelEdgeIDs.set(toEdgeID(sourceModelName, targetModelName), [
          sourceNodeId,
          targetNodeId,
        ])

        sourceColumns.forEach(sourceColumnName => {
          const edgeId = toEdgeID(
            sourceModelName,
            sourceColumnName,
            targetModelName,
            targetColumnName,
          )
          const sourceColumnId = toPortID(sourceModelName, sourceColumnName)
          const targetColumnId = toPortID(targetModelName, targetColumnName)

          edges.push(
            transformEdge(
              'port',
              edgeId,
              sourceNodeId,
              targetNodeId,
              sourceColumnId,
              targetColumnId,
            ),
          )
        })
      }
    }
  }

  Array.from(modelLevelEdgeIDs.entries()).forEach(
    ([edgeId, [sourceNodeId, targetNodeId]]) => {
      edges.push(transformEdge('edge', edgeId, sourceNodeId, targetNodeId))
    },
  )
  return edges
}

export function getConnectedColumnsIDs(
  adjacencyList: ColumnLevelLineageAdjacencyList,
) {
  const connectedColumns = new Set<PortId>()
  const targets = Object.entries(adjacencyList) as [
    AdjacencyListKey,
    ColumnLevelConnections,
  ][]

  for (const [sourceModelName, targetColumns] of targets) {
    const targetConnections = Object.entries(targetColumns) as [
      AdjacencyListColumnKey,
      ColumnLevelDetails,
    ][]

    for (const [
      sourceColumnName,
      { models: sourceModels },
    ] of targetConnections) {
      connectedColumns.add(toPortID(sourceModelName, sourceColumnName))

      const sources = Object.entries(sourceModels) as [
        AdjacencyListKey,
        AdjacencyListKey[],
      ][]

      for (const [targetModelName, sourceColumns] of sources) {
        sourceColumns.forEach(sourceColumnName => {
          connectedColumns.add(toPortID(targetModelName, sourceColumnName))
        })
      }
    }
  }
  return connectedColumns
}

export function calculateNodeColumnsCount(columnsCount: number = 0) {
  return Math.min(columnsCount, MAX_COLUMNS_TO_DISPLAY)
}

export function calculateSelectedColumnsHeight(
  selectedColumnsCount: number = 0,
) {
  const selectedColumnsTopSeparatorHeight = 1
  const selectedColumnSeparatorHeight = 1
  const selectedColumnHeight = 24 // tailwind h-6
  const selectedColumnsSeparators =
    selectedColumnsCount > 1 ? selectedColumnsCount - 1 : 0

  return [
    selectedColumnsCount > 0 ? selectedColumnsTopSeparatorHeight : 0,
    selectedColumnsCount * selectedColumnHeight,
    selectedColumnsCount > 0
      ? selectedColumnsSeparators * selectedColumnSeparatorHeight
      : 0,
  ].reduce((acc, h) => acc + h, 0)
}

export function calculateColumnsHeight({
  columnsCount = 0,
  hasColumnsFilter = true,
}: {
  columnsCount: number
  hasColumnsFilter?: boolean
}) {
  const hasColumns = columnsCount > 0
  const columnHeight = 24 // tailwind h-6
  const columnsTopSeparator = 1
  const columnSeparator = 1
  const columnsContainerPadding = 4
  const columnsPadding = 4
  const columnsFilterHeight = hasColumnsFilter && hasColumns ? columnHeight : 0
  const columnsSeparators = columnsCount > 1 ? columnsCount - 1 : 0

  return [
    hasColumns ? columnsSeparators * columnSeparator : 0,
    columnsCount * columnHeight,
    hasColumns ? columnsPadding * 2 : 0,
    hasColumns ? columnsContainerPadding * 2 : 0,
    hasColumns ? columnsFilterHeight : 0,
    hasColumns ? columnsTopSeparator : 0,
  ].reduce((acc, height) => acc + height, 0)
}
