import React from 'react'

import {
  type EdgeId,
  type LineageEdge,
  type LineageEdgeData,
  type LineageNode,
  type LineageNodeData,
  type LineageNodesMap,
  type NodeId,
  ZOOM_TRESHOLD,
} from './utils'

export interface LineageContextValue<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
> {
  // Node selection
  showOnlySelectedNodes: boolean
  setShowOnlySelectedNodes: React.Dispatch<React.SetStateAction<boolean>>
  selectedNodes: Set<NodeId>
  setSelectedNodes: React.Dispatch<React.SetStateAction<Set<NodeId>>>
  selectedEdges: Set<EdgeId>
  setSelectedEdges: React.Dispatch<React.SetStateAction<Set<EdgeId>>>
  selectedNodeId: NodeId | null
  setSelectedNodeId: React.Dispatch<React.SetStateAction<NodeId | null>>

  // Layout
  isBuildingLayout: boolean
  setIsBuildingLayout: React.Dispatch<React.SetStateAction<boolean>>
  zoom: number
  setZoom: React.Dispatch<React.SetStateAction<number>>

  // Nodes and Edges
  edges: LineageEdge<TEdgeData>[]
  setEdges: React.Dispatch<React.SetStateAction<LineageEdge<TEdgeData>[]>>
  nodes: LineageNode<TNodeData>[]
  nodesMap: LineageNodesMap<TNodeData>
  setNodesMap: React.Dispatch<React.SetStateAction<LineageNodesMap<TNodeData>>>
  currentNode: LineageNode<TNodeData> | null
}

export const initial = {
  showOnlySelectedNodes: false,
  setShowOnlySelectedNodes: () => {},
  selectedNodes: new Set() as Set<NodeId>,
  setSelectedNodes: () => {},
  selectedEdges: new Set() as Set<EdgeId>,
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

export type LineageContextHook<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
> = () => LineageContextValue<TNodeData, TEdgeData>

export function createLineageContext<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TLineageContextValue extends LineageContextValue<
    TNodeData,
    TEdgeData
  > = LineageContextValue<TNodeData, TEdgeData>,
>(initial: TLineageContextValue) {
  const LineageContext = React.createContext(initial)

  return {
    Provider: LineageContext.Provider,
    useLineage: () => React.useContext(LineageContext),
  }
}
