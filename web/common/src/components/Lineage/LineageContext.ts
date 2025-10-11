import React from 'react'

import {
  type EdgeId,
  type LineageEdge,
  type LineageEdgeData,
  type LineageNode,
  type LineageNodeData,
  type LineageNodesMap,
  type NodeId,
  type PortId,
  ZOOM_THRESHOLD,
} from './utils'

export interface LineageContextValue<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TNodeID extends string = NodeId,
  TEdgeID extends string = EdgeId,
  TSourceID extends string = TNodeID,
  TTargetID extends string = TNodeID,
  TSourceHandleID extends string = PortId,
  TTargetHandleID extends string = PortId,
> {
  // Node selection
  showOnlySelectedNodes: boolean
  setShowOnlySelectedNodes: React.Dispatch<React.SetStateAction<boolean>>
  selectedNodes: Set<TNodeID>
  setSelectedNodes: React.Dispatch<React.SetStateAction<Set<TNodeID>>>
  selectedEdges: Set<TEdgeID>
  setSelectedEdges: React.Dispatch<React.SetStateAction<Set<TEdgeID>>>
  selectedNodeId: TNodeID | null
  setSelectedNodeId: React.Dispatch<React.SetStateAction<TNodeID | null>>

  // Layout
  zoom: number
  setZoom: React.Dispatch<React.SetStateAction<number>>

  // Nodes and Edges
  edges: LineageEdge<
    TEdgeData,
    TEdgeID,
    TSourceID,
    TTargetID,
    TSourceHandleID,
    TTargetHandleID
  >[]
  setEdges: React.Dispatch<
    React.SetStateAction<
      LineageEdge<
        TEdgeData,
        TEdgeID,
        TSourceID,
        TTargetID,
        TSourceHandleID,
        TTargetHandleID
      >[]
    >
  >
  nodes: LineageNode<TNodeData, TNodeID>[]
  nodesMap: LineageNodesMap<TNodeData, TNodeID>
  setNodesMap: React.Dispatch<
    React.SetStateAction<LineageNodesMap<TNodeData, TNodeID>>
  >
  currentNodeId: TNodeID | null
  selectedNode: LineageNode<TNodeData, TNodeID> | null
}

export function getInitial<
  TNodeID extends string = NodeId,
  TEdgeID extends string = EdgeId,
>() {
  return {
    showOnlySelectedNodes: false,
    setShowOnlySelectedNodes: () => {},
    selectedNodes: new Set<TNodeID>(),
    setSelectedNodes: () => {},
    selectedEdges: new Set<TEdgeID>(),
    setSelectedEdges: () => {},
    setSelectedNodeId: () => {},
    zoom: ZOOM_THRESHOLD,
    setZoom: () => {},
    edges: [],
    setEdges: () => {},
    nodes: [],
    nodesMap: {},
    setNodesMap: () => {},
    selectedNodeId: null,
    selectedNode: null,
    currentNodeId: null,
  }
}

export type LineageContextHook<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TNodeID extends string = NodeId,
  TEdgeID extends string = EdgeId,
  TSourceID extends string = TNodeID,
  TTargetID extends string = TNodeID,
  TSourceHandleID extends string = PortId,
  TTargetHandleID extends string = PortId,
> = () => LineageContextValue<
  TNodeData,
  TEdgeData,
  TNodeID,
  TEdgeID,
  TSourceID,
  TTargetID,
  TSourceHandleID,
  TTargetHandleID
>

export function createLineageContext<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TNodeID extends string = NodeId,
  TEdgeID extends string = EdgeId,
  TSourceID extends string = TNodeID,
  TTargetID extends string = TNodeID,
  TSourceHandleID extends string = PortId,
  TTargetHandleID extends string = PortId,
  TLineageContextValue extends LineageContextValue<
    TNodeData,
    TEdgeData,
    TNodeID,
    TEdgeID,
    TSourceID,
    TTargetID,
    TSourceHandleID,
    TTargetHandleID
  > = LineageContextValue<
    TNodeData,
    TEdgeData,
    TNodeID,
    TEdgeID,
    TSourceID,
    TTargetID,
    TSourceHandleID,
    TTargetHandleID
  >,
>(initial: TLineageContextValue) {
  const LineageContext = React.createContext(initial)

  return {
    Provider: LineageContext.Provider,
    useLineage: () => React.useContext(LineageContext),
  }
}
