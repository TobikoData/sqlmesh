import { Position } from '@xyflow/react'

import {
  DEFAULT_NODE_HEIGHT,
  DEFAULT_NODE_WIDTH,
  type EdgeId,
  type LineageAdjacencyList,
  type LineageDetails,
  type LineageEdge,
  type LineageEdgeData,
  type LineageNode,
  type LineageNodeData,
  type LineageNodesMap,
  type NodeId,
  type PortId,
  toEdgeID,
  toNodeID,
  type TransformEdgeFn,
  type TransformNodeFn,
} from './utils'

export function getOnlySelectedNodes<
  TNodeData extends LineageNodeData = LineageNodeData,
  TNodeID extends string = NodeId,
>(nodeMaps: LineageNodesMap<TNodeData, TNodeID>, selectedNodes: Set<TNodeID>) {
  return (
    Object.values(nodeMaps) satisfies LineageNode<TNodeData, TNodeID>[]
  ).reduce(
    (acc, node) =>
      selectedNodes.has(node.id) ? { ...acc, [node.id]: node } : acc,
    {} as LineageNodesMap<TNodeData, TNodeID>,
  )
}

export function getTransformedNodes<
  TAdjacencyListKey extends string,
  TDetailsNode,
  TNodeData extends LineageNodeData = LineageNodeData,
  TNodeID extends string = NodeId,
>(
  adjacencyListKeys: TAdjacencyListKey[],
  lineageDetails: LineageDetails<TAdjacencyListKey, TDetailsNode>,
  transformNode: TransformNodeFn<TDetailsNode, TNodeData, TNodeID>,
  allNodesMap?: LineageNodesMap<TNodeData, TNodeID>,
): LineageNodesMap<TNodeData, TNodeID> {
  const nodesCount = adjacencyListKeys.length
  const nodesMap: LineageNodesMap<TNodeData, TNodeID> = Object.create(null)

  for (let i = 0; i < nodesCount; i++) {
    const adjacencyListKey = adjacencyListKeys[i]
    const nodeId = toNodeID<TNodeID>(adjacencyListKey)
    nodesMap[nodeId] =
      allNodesMap?.[nodeId] ||
      transformNode(nodeId, lineageDetails[adjacencyListKey])
  }

  return nodesMap
}

export function getTransformedModelEdgesSourceTargets<
  TAdjacencyListKey extends string,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TEdgeID extends string = EdgeId,
  TSourceID extends string = NodeId,
  TTargetID extends string = NodeId,
  TSourceHandleID extends string = PortId,
  TTargetHandleID extends string = PortId,
>(
  adjacencyListKeys: TAdjacencyListKey[],
  lineageAdjacencyList: LineageAdjacencyList<TAdjacencyListKey>,
  transformEdge: TransformEdgeFn<
    TEdgeData,
    TEdgeID,
    TSourceID,
    TTargetID,
    TSourceHandleID,
    TTargetHandleID
  >,
) {
  const nodesCount = adjacencyListKeys.length

  if (nodesCount === 0) return []

  const edges = []

  for (let i = 0; i < nodesCount; i++) {
    const sourceAdjacencyListKey = adjacencyListKeys[i]
    const sourceNodeId = toNodeID<TSourceID>(sourceAdjacencyListKey)
    const targets = lineageAdjacencyList[sourceAdjacencyListKey]
    const targetsCount = targets?.length || 0

    if (targets == null || targetsCount < 1) continue

    for (let j = 0; j < targetsCount; j++) {
      const targetAdjacencyListKey = targets[j]

      if (!(targetAdjacencyListKey in lineageAdjacencyList)) continue

      const edgeId = toEdgeID<TEdgeID>(
        sourceAdjacencyListKey,
        targetAdjacencyListKey,
      )
      const targetNodeId = toNodeID<TTargetID>(targetAdjacencyListKey)

      edges.push(transformEdge('edge', edgeId, sourceNodeId, targetNodeId))
    }
  }

  return edges
}

export function getTransformedModelEdgesTargetSources<
  TAdjacencyListKey extends string,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TEdgeID extends string = EdgeId,
  TSourceID extends string = NodeId,
  TTargetID extends string = NodeId,
  TSourceHandleID extends string = PortId,
  TTargetHandleID extends string = PortId,
>(
  adjacencyListKeys: TAdjacencyListKey[],
  lineageAdjacencyList: LineageAdjacencyList<TAdjacencyListKey>,
  transformEdge: TransformEdgeFn<
    TEdgeData,
    TEdgeID,
    TSourceID,
    TTargetID,
    TSourceHandleID,
    TTargetHandleID
  >,
) {
  const nodesCount = adjacencyListKeys.length

  if (nodesCount === 0) return []

  const edges = []

  for (let i = 0; i < nodesCount; i++) {
    const targetAdjacencyListKey = adjacencyListKeys[i]
    const targetNodeId = toNodeID<TTargetID>(targetAdjacencyListKey)
    const sources = lineageAdjacencyList[targetAdjacencyListKey]
    const sourcesCount = sources?.length || 0

    if (sources == null || sourcesCount < 1) continue

    for (let j = 0; j < sourcesCount; j++) {
      const sourceAdjacencyListKey = sources[j]

      if (!(sourceAdjacencyListKey in lineageAdjacencyList)) continue

      const edgeId = toEdgeID<TEdgeID>(
        sourceAdjacencyListKey,
        targetAdjacencyListKey,
      )
      const sourceNodeId = toNodeID<TSourceID>(sourceAdjacencyListKey)

      edges.push(transformEdge('edge', edgeId, sourceNodeId, targetNodeId))
    }
  }

  return edges
}

export function createNode<
  TNodeData extends LineageNodeData = LineageNodeData,
  TNodeID extends string = NodeId,
>(type: string, nodeId: TNodeID, data: TNodeData) {
  return {
    id: nodeId,
    sourcePosition: Position.Right,
    targetPosition: Position.Left,
    width: DEFAULT_NODE_WIDTH,
    height: DEFAULT_NODE_HEIGHT,
    data,
    type,
    hidden: false,
    position: { x: 0, y: 0 },
    zIndex: 10,
  }
}

export function calculateNodeBaseHeight({
  includeNodeFooterHeight = false,
  includeCeilingHeight = false,
  includeFloorHeight = false,
}: {
  includeNodeFooterHeight?: boolean
  includeCeilingHeight?: boolean
  includeFloorHeight?: boolean
}) {
  const border = 2
  const footerHeight = 20 // tailwind h-5
  const base = 28 // tailwind h-7
  const ceilingHeight = 20 // tailwind h-5
  const floorHeight = 20 // tailwind h-5

  const ceilingGap = 4
  const floorGap = 4

  return [
    border * 2,
    base,
    includeNodeFooterHeight ? footerHeight : 0,
    includeCeilingHeight ? ceilingHeight + ceilingGap : 0,
    includeFloorHeight ? floorHeight + floorGap : 0,
  ].reduce((acc, h) => acc + h, 0)
}

export function calculateNodeDetailsHeight({
  nodeDetailsCount = 0,
}: {
  nodeDetailsCount?: number
}) {
  const nodeOptionHeight = 24 // tailwind h-6

  const nodeOptionsSeparator = 1
  const nodeOptionsSeparators = nodeDetailsCount > 1 ? nodeDetailsCount - 1 : 0

  return [
    nodeOptionsSeparators * nodeOptionsSeparator,
    nodeDetailsCount * nodeOptionHeight,
  ].reduce((acc, h) => acc + h, 0)
}

export function createEdge<
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TEdgeID extends string = EdgeId,
  TSourceID extends string = NodeId,
  TTargetID extends string = NodeId,
  TSourceHandleID extends string = PortId,
  TTargetHandleID extends string = PortId,
>(
  type: string,
  edgeId: TEdgeID,
  sourceId: TSourceID,
  targetId: TTargetID,
  sourceHandleId?: TSourceHandleID,
  targetHandleId?: TTargetHandleID,
  data?: TEdgeData,
): LineageEdge<
  TEdgeData,
  TEdgeID,
  TSourceID,
  TTargetID,
  TSourceHandleID,
  TTargetHandleID
> {
  return {
    id: edgeId,
    source: sourceId,
    target: targetId,
    type,
    sourceHandle: sourceHandleId ? sourceHandleId : undefined,
    targetHandle: targetHandleId ? targetHandleId : undefined,
    data,
    zIndex: 1,
  }
}
