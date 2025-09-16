import { Position } from '@xyflow/react'

import {
  type AdjacencyListKey,
  DEFAULT_NODE_HEIGHT,
  DEFAULT_NODE_WIDTH,
  type EdgeId,
  type LineageAdjacencyList,
  type LineageAdjacencyListNode,
  type LineageDetails,
  type LineageEdge,
  type LineageEdgeData,
  type LineageNodeData,
  type LineageNodesMap,
  type NodeId,
  type PortId,
  type TransformEdgeFn,
  type TransformNodeFn,
} from './utils'

export function getOnlySelectedNodes<
  TNodeData extends LineageNodeData = LineageNodeData,
>(nodeMaps: LineageNodesMap<TNodeData>, selectedNodes: Set<NodeId>) {
  return Object.values(nodeMaps).reduce(
    (acc, node) =>
      selectedNodes.has(node.id) ? { ...acc, [node.id]: node } : acc,
    {} as LineageNodesMap<TNodeData>,
  )
}

export function getTransformedNodes<
  TDetailsNode,
  TNodeData extends LineageNodeData = LineageNodeData,
>(
  adjacencyListKeys: AdjacencyListKey[] = [],
  lineageDetails: LineageDetails<TDetailsNode> = {},
  transformNode: TransformNodeFn<TDetailsNode, TNodeData>,
) {
  const nodesCount = adjacencyListKeys.length

  if (nodesCount === 0) return {}

  const nodesMap: LineageNodesMap<TNodeData> = Object.create(null)

  for (let i = 0; i < nodesCount; i++) {
    const nodeId = adjacencyListKeys[i]
    const encodedNodeId = toNodeID(nodeId)
    nodesMap[encodedNodeId] = transformNode(
      encodedNodeId,
      lineageDetails[nodeId],
    )
  }

  return nodesMap
}

export function getTransformedModelEdges<
  TAdjacencyListNode extends
    LineageAdjacencyListNode = LineageAdjacencyListNode,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
>(
  adjacencyListKeys: AdjacencyListKey[],
  lineageAdjacencyList: LineageAdjacencyList<TAdjacencyListNode>,
  transformEdge: TransformEdgeFn<TEdgeData>,
) {
  const nodesCount = adjacencyListKeys.length

  if (nodesCount === 0) return []

  const edges = []

  for (let i = 0; i < nodesCount; i++) {
    const adjacencyListKey = adjacencyListKeys[i]
    const nodeId = toNodeID(adjacencyListKey)
    const targets = lineageAdjacencyList[adjacencyListKey]
    const targetsCount = targets?.length || 0

    if (targets == null || targetsCount < 1) continue

    for (let j = 0; j < targetsCount; j++) {
      const target = targets[j]?.name

      if (!(target in lineageAdjacencyList)) continue

      const edgeId = toEdgeID(adjacencyListKey, target)

      edges.push(transformEdge(edgeId, nodeId, toNodeID(target)))
    }
  }

  return edges
}

export function createNode<TNodeData extends LineageNodeData = LineageNodeData>(
  nodeId: NodeId,
  type: string = 'node',
  data: TNodeData,
) {
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
  hasNodeFooter = false,
  hasCeiling = false,
  hasFloor = false,
  nodeOptionsCount = 0,
}: {
  hasNodeFooter?: boolean
  hasCeiling?: boolean
  hasFloor?: boolean
  nodeOptionsCount?: number
}) {
  const border = 2
  const footerHeight = 20 // tailwind h-5
  const base = 28 // tailwind h-7
  const ceilingHeight = 20 // tailwind h-5
  const floorHeight = 20 // tailwind h-5
  const nodeOptionHeight = 24 // tailwind h-6

  const nodeOptionsSeparator = 1
  const nodeOptionsSeparators = nodeOptionsCount > 1 ? nodeOptionsCount - 1 : 0

  const ceilingGap = 4
  const floorGap = 4

  return [
    border * 2,
    base,
    hasNodeFooter ? footerHeight : 0,

    hasCeiling ? ceilingHeight + ceilingGap : 0,
    hasFloor ? floorHeight + floorGap : 0,

    nodeOptionsSeparators * nodeOptionsSeparator,
    nodeOptionsCount * nodeOptionHeight,
  ].reduce((acc, h) => acc + h, 0)
}

export function createEdge<TEdgeData extends LineageEdgeData = LineageEdgeData>(
  edgeId: EdgeId,
  sourceId: NodeId,
  targetId: NodeId,
  type: string = 'edge',
  sourceHandleId?: PortId,
  targetHandleId?: PortId,
  data?: TEdgeData,
): LineageEdge<TEdgeData> {
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

export function toID<TReturn extends string>(...args: string[]) {
  return args.join('.') as TReturn
}

export function toNodeID(...args: string[]) {
  return encodeURI(toID(...args)) as NodeId
}

export function toEdgeID(...args: string[]) {
  return encodeURI(toID(...args)) as EdgeId
}

export function toPortID(...args: string[]) {
  return encodeURI(toID(...args)) as PortId
}
