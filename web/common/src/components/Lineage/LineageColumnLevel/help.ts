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
import { type ColumnLevelLineageAdjacencyList } from './ColumnLevelLineageContext'

export const MAX_COLUMNS_TO_DISPLAY = 5

export function getAdjacencyListKeysFromColumnLineage<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
  TColumnLevelLineageAdjacencyList extends ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  > = ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  >,
>(columnLineage: TColumnLevelLineageAdjacencyList) {
  const adjacencyListKeys = new Set<TAdjacencyListKey>()

  const targets = Object.entries(columnLineage) as [
    TAdjacencyListKey,
    TColumnLevelLineageAdjacencyList[TAdjacencyListKey],
  ][]

  for (const [sourceModelName, targetColumns] of targets) {
    adjacencyListKeys.add(sourceModelName)

    const targetConnections = Object.entries(targetColumns) as [
      TAdjacencyListColumnKey,
      TColumnLevelLineageAdjacencyList[TAdjacencyListKey][TAdjacencyListColumnKey],
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
  TSourceID extends string = NodeId,
  TTargetID extends string = NodeId,
  TSourceHandleID extends string = PortId,
  TTargetHandleID extends string = PortId,
  TColumnLevelLineageAdjacencyList extends ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  > = ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  >,
>({
  columnLineage,
  transformEdge,
}: {
  columnLineage: TColumnLevelLineageAdjacencyList
  transformEdge: TransformEdgeFn<
    TEdgeData,
    TEdgeID,
    TSourceID,
    TTargetID,
    TSourceHandleID,
    TTargetHandleID
  >
}) {
  const edges: LineageEdge<
    TEdgeData,
    TEdgeID,
    TSourceID,
    TTargetID,
    TSourceHandleID,
    TTargetHandleID
  >[] = []
  const modelLevelEdgeIDs = new Map<TEdgeID, [TSourceID, TTargetID]>()
  const targets = Object.entries(columnLineage || {}) as [
    TAdjacencyListKey,
    TColumnLevelLineageAdjacencyList[TAdjacencyListKey],
  ][]

  for (const [targetModelName, targetColumns] of targets) {
    const targetConnections = Object.entries(targetColumns) as [
      TAdjacencyListColumnKey,
      TColumnLevelLineageAdjacencyList[TAdjacencyListKey][TAdjacencyListColumnKey],
    ][]

    const targetNodeId = toNodeID<TTargetID>(targetModelName)

    for (const [
      targetColumnName,
      { models: sourceModels },
    ] of targetConnections) {
      const sources = Object.entries(sourceModels) as [
        TAdjacencyListKey,
        TAdjacencyListColumnKey[],
      ][]

      for (const [sourceModelName, sourceColumns] of sources) {
        const sourceNodeId = toNodeID<TSourceID>(sourceModelName)

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
          const sourceColumnId = toPortID<TSourceHandleID>(
            sourceModelName,
            sourceColumnName,
          )
          const targetColumnId = toPortID<TTargetHandleID>(
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
  TColumnLevelLineageAdjacencyList extends ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  > = ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  >,
>(adjacencyList: TColumnLevelLineageAdjacencyList) {
  const connectedColumns = new Set<TColumnID>()
  const targets = Object.entries(adjacencyList) as [
    TAdjacencyListKey,
    TColumnLevelLineageAdjacencyList[TAdjacencyListKey],
  ][]

  for (const [sourceModelName, targetColumns] of targets) {
    const targetConnections = Object.entries(targetColumns) as [
      TAdjacencyListColumnKey,
      TColumnLevelLineageAdjacencyList[TAdjacencyListKey][TAdjacencyListColumnKey],
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
  const columnSeparator = 0
  const columnsContainerPadding = 4
  const columnsPadding = 0
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
