import type { Branded } from '@/types'
import { type Edge, type Node } from '@xyflow/react'

export type NodeId = Branded<string, 'NodeId'>
export type EdgeId = Branded<string, 'EdgeId'>
export type PortId = Branded<string, 'PortId'>
export type AdjacencyListKey = Branded<string, 'AdjacencyListKey'>
export type AdjacencyListColumnKey = Branded<string, 'AdjacencyListColumnKey'>

export type LineageAdjacencyListNode = {
  name: AdjacencyListKey
  [key: string]: unknown
}
export type LineageNodeData = Record<string, unknown>
export type LineageEdgeData = Record<string, unknown>

export type LineageAdjacencyList<
  TAdjacencyListNode = LineageAdjacencyListNode,
> = Record<AdjacencyListKey, TAdjacencyListNode[]>
export type LineageDetails<TValue> = Record<AdjacencyListKey, TValue>

export type LineageNodesMap<TNodeData extends LineageNodeData> = Record<
  NodeId,
  LineageNode<TNodeData>
>
export interface LineageNode<TNodeData extends LineageNodeData>
  extends Node<TNodeData> {
  id: NodeId
}

export interface LineageEdge<TEdgeData extends LineageEdgeData>
  extends Edge<TEdgeData> {
  id: EdgeId
  source: NodeId
  target: NodeId
  sourceHandle?: PortId
  targetHandle?: PortId
}

export type LayoutedGraph<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
> = {
  edges: LineageEdge<TEdgeData>[]
  nodesMap: LineageNodesMap<TNodeData>
}

export type PathType = 'bezier' | 'smoothstep' | 'step' | 'straight'

export const DEFAULT_NODE_HEIGHT = 32
export const DEFAULT_NODE_WIDTH = 400
export const DEFAULT_ZOOM = 0.85
export const MIN_ZOOM = 0.01
export const MAX_ZOOM = 1.75
export const ZOOM_TRESHOLD = 0.75
export const NODES_TRESHOLD = 200
export const NODES_TRESHOLD_ZOOM = 0.1

export type TransformNodeFn<
  TData,
  TNodeData extends LineageNodeData = LineageNodeData,
> = (nodeId: NodeId, data: TData) => LineageNode<TNodeData>

export type TransformEdgeFn<
  TEdgeData extends LineageEdgeData = LineageEdgeData,
> = (
  edgeId: EdgeId,
  sourceId: NodeId,
  targetId: NodeId,
  sourceColumnId?: PortId,
  targetColumnId?: PortId,
) => LineageEdge<TEdgeData>
