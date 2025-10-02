import {
  toEdgeID,
  toNodeID,
  toPortID,
  type LineageEdge,
  type LineageEdgeData,
  type EdgeId,
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

export function getAdjacencyListKeysFromColumnLineage<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
>(
  columnLineage: ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  >,
) {
  const adjacencyListKeys = new Set<TAdjacencyListKey>()

  const targets = Object.entries(columnLineage) as [
    TAdjacencyListKey,
    ColumnLevelConnections<TAdjacencyListKey, TAdjacencyListColumnKey>,
  ][]

  for (const [sourceModelName, targetColumns] of targets) {
    adjacencyListKeys.add(sourceModelName)

    const targetConnections = Object.entries(targetColumns) as [
      TAdjacencyListColumnKey,
      ColumnLevelDetails<TAdjacencyListKey, TAdjacencyListColumnKey>,
    ][]

    for (const [, { models: sourceModels }] of targetConnections) {
      for (const targetModelName of Object.keys(
        sourceModels,
      ) as TAdjacencyListKey[]) {
        adjacencyListKeys.add(targetModelName)
      }
    }
  }

  return Array.from(adjacencyListKeys)
}

export function getEdgesFromColumnLineage<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TEdgeID extends string = EdgeId,
  TNodeID extends string = NodeId,
  TPortID extends string = PortId,
>({
  columnLineage,
  transformEdge,
}: {
  columnLineage: ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  >
  transformEdge: TransformEdgeFn<TEdgeData, TNodeID, TEdgeID, TPortID>
}) {
  const edges: LineageEdge<TEdgeData, TNodeID, TEdgeID, TPortID>[] = []
  const modelLevelEdgeIDs = new Map<TEdgeID, [TNodeID, TNodeID]>()
  const targets = Object.entries(columnLineage || {}) as [
    TAdjacencyListKey,
    ColumnLevelConnections<TAdjacencyListKey, TAdjacencyListColumnKey>,
  ][]

  for (const [targetModelName, targetColumns] of targets) {
    const targetConnections = Object.entries(targetColumns) as [
      TAdjacencyListColumnKey,
      ColumnLevelDetails<TAdjacencyListKey, TAdjacencyListColumnKey>,
    ][]

    const targetNodeId = toNodeID<TNodeID>(targetModelName)

    for (const [
      targetColumnName,
      { models: sourceModels },
    ] of targetConnections) {
      const sources = Object.entries(sourceModels) as [
        TAdjacencyListKey,
        TAdjacencyListColumnKey[],
      ][]

      for (const [sourceModelName, sourceColumns] of sources) {
        const sourceNodeId = toNodeID<TNodeID>(sourceModelName)

        modelLevelEdgeIDs.set(
          toEdgeID<TEdgeID>(sourceModelName, targetModelName),
          [sourceNodeId, targetNodeId],
        )

        sourceColumns.forEach(sourceColumnName => {
          const edgeId = toEdgeID<TEdgeID>(
            sourceModelName,
            sourceColumnName,
            targetModelName,
            targetColumnName,
          )
          const sourceColumnId = toPortID<TPortID>(
            sourceModelName,
            sourceColumnName,
          )
          const targetColumnId = toPortID<TPortID>(
            targetModelName,
            targetColumnName,
          )

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

export function getConnectedColumnsIDs<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
  TColumnID extends string = PortId,
>(
  adjacencyList: ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  >,
) {
  const connectedColumns = new Set<TColumnID>()
  const targets = Object.entries(adjacencyList) as [
    TAdjacencyListKey,
    ColumnLevelConnections<TAdjacencyListKey, TAdjacencyListColumnKey>,
  ][]

  for (const [sourceModelName, targetColumns] of targets) {
    const targetConnections = Object.entries(targetColumns) as [
      TAdjacencyListColumnKey,
      ColumnLevelDetails<TAdjacencyListKey, TAdjacencyListColumnKey>,
    ][]

    for (const [
      sourceColumnName,
      { models: sourceModels },
    ] of targetConnections) {
      connectedColumns.add(toPortID(sourceModelName, sourceColumnName))

      const sources = Object.entries(sourceModels) as [
        TAdjacencyListKey,
        TAdjacencyListColumnKey[],
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
