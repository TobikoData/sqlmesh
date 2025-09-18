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
  ZOOM_TRESHOLD,
} from './utils'

export interface LineageContextValue<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TNodeID extends string = NodeId,
  TEdgeID extends string = EdgeId,
  TPortID extends string = PortId,
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
  isBuildingLayout: boolean
  setIsBuildingLayout: React.Dispatch<React.SetStateAction<boolean>>
  zoom: number
  setZoom: React.Dispatch<React.SetStateAction<number>>

  // Nodes and Edges
  edges: LineageEdge<TEdgeData, TNodeID, TEdgeID, TPortID>[]
  setEdges: React.Dispatch<
    React.SetStateAction<LineageEdge<TEdgeData, TNodeID, TEdgeID, TPortID>[]>
  >
  nodes: LineageNode<TNodeData, TNodeID>[]
  nodesMap: LineageNodesMap<TNodeData, TNodeID>
  setNodesMap: React.Dispatch<React.SetStateAction<LineageNodesMap<TNodeData>>>
  currentNode: LineageNode<TNodeData, TNodeID> | null
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
    selectedNodeId: null,
    setSelectedNodeId: () => {},
    zoom: ZOOM_TRESHOLD,
    setZoom: () => {},
    edges: [],
    setEdges: () => {},
    nodes: [],
    nodesMap: {},
    setNodesMap: () => {},
    isBuildingLayout: false,
    setIsBuildingLayout: () => {},
    currentNode: null,
  }
}

export type LineageContextHook<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TNodeID extends string = NodeId,
  TEdgeID extends string = EdgeId,
  TPortID extends string = PortId,
> = () => LineageContextValue<TNodeData, TEdgeData, TNodeID, TEdgeID, TPortID>

export function createLineageContext<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TNodeID extends string = NodeId,
  TEdgeID extends string = EdgeId,
  TPortID extends string = PortId,
  TLineageContextValue extends LineageContextValue<
    TNodeData,
    TEdgeData,
    TNodeID,
    TEdgeID,
    TPortID
  > = LineageContextValue<TNodeData, TEdgeData, TNodeID, TEdgeID, TPortID>,
>(initial: TLineageContextValue) {
  const LineageContext = React.createContext(initial)

  return {
    Provider: LineageContext.Provider,
    useLineage: () => React.useContext(LineageContext),
  }
}
