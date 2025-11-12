import type { Branded } from '@sqlmesh-common/types'
import { type Edge, type Node } from '@xyflow/react'

export type NodeId = Branded<string, 'NodeId'>
export type EdgeId = Branded<string, 'EdgeId'>
export type PortId = Branded<string, 'PortId'>
export type HandleId = Branded<string, 'HandleId'>
export type PortHandleId = Branded<string, 'PortHandleId'>

export type LineageNodeData = Record<string, unknown>
export type LineageEdgeData = Record<string, unknown>

export type LineageAdjacencyList<TAdjacencyListKey extends string = string> =
  Record<TAdjacencyListKey, TAdjacencyListKey[]>

export type LineageDetails<TAdjacencyListKey extends string, TValue> = Record<
  TAdjacencyListKey,
  TValue
>

export type LineageNodesMap<
  TNodeData extends LineageNodeData,
  TNodeID extends string = NodeId,
> = Record<TNodeID, LineageNode<TNodeData, TNodeID>>
export interface LineageNode<
  TNodeData extends LineageNodeData,
  TNodeID extends string = NodeId,
> extends Node<TNodeData> {
  id: TNodeID
}

export interface LineageEdge<
  TEdgeData extends LineageEdgeData,
  TEdgeID extends string = EdgeId,
  TSourceID extends string = NodeId,
  TTargetID extends string = NodeId,
  TSourceHandleID extends string = PortId,
  TTargetHandleID extends string = PortId,
> extends Edge<TEdgeData> {
  id: TEdgeID
  source: TSourceID
  target: TTargetID
  sourceHandle?: TSourceHandleID
  targetHandle?: TTargetHandleID
}

export type LayoutedGraph<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TEdgeID extends string = EdgeId,
  TSourceID extends string = NodeId,
  TTargetID extends string = NodeId,
  TSourceHandleID extends string = PortId,
  TTargetHandleID extends string = PortId,
> = {
  edges: LineageEdge<
    TEdgeData,
    TEdgeID,
    TSourceID,
    TTargetID,
    TSourceHandleID,
    TTargetHandleID
  >[]
  nodesMap: LineageNodesMap<TNodeData>
}

export type PathType = 'bezier' | 'smoothstep' | 'step' | 'straight'
export type TransformNodeFn<
  TData,
  TNodeData extends LineageNodeData = LineageNodeData,
  TNodeID extends string = NodeId,
> = (nodeId: TNodeID, data: TData) => LineageNode<TNodeData, TNodeID>

export type TransformEdgeFn<
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TEdgeID extends string = EdgeId,
  TSourceID extends string = NodeId,
  TTargetID extends string = NodeId,
  TSourceHandleID extends string = PortId,
  TTargetHandleID extends string = PortId,
> = (
  edgeType: string,
  edgeId: TEdgeID,
  sourceId: TSourceID,
  targetId: TTargetID,
  sourceHandleId?: TSourceHandleID,
  targetHandleId?: TTargetHandleID,
) => LineageEdge<
  TEdgeData,
  TEdgeID,
  TSourceID,
  TTargetID,
  TSourceHandleID,
  TTargetHandleID
>

export const DEFAULT_NODE_HEIGHT = 32
export const DEFAULT_NODE_WIDTH = 300
export const DEFAULT_ZOOM = 0.85
export const MIN_ZOOM = 0.01
export const MAX_ZOOM = 1.75
export const ZOOM_THRESHOLD = 0.5
export const NODES_TRESHOLD = 200
export const NODES_TRESHOLD_ZOOM = 0.1

// ID generated from toInternalID is meant to be used only internally to identify nodes, edges and ports within the graph
// Do not rely on the ID to be a valid URL, or anythjin outside of the graph
export function toInternalID<TReturn extends string>(
  ...args: string[]
): TReturn {
  return encodeURI(args.filter(Boolean).join('.')) as TReturn
}

export function toNodeID<TNodeID extends string = NodeId>(
  ...args: string[]
): TNodeID {
  return toInternalID<TNodeID>(...args)
}

export function toEdgeID<TEdgeID extends string = EdgeId>(
  ...args: string[]
): TEdgeID {
  return toInternalID<TEdgeID>(...args)
}

export function toPortID<TPortId extends string = PortId>(
  ...args: string[]
): TPortId {
  return toInternalID<TPortId>(...args)
}
