import {
  type EdgeTypes,
  type NodeTypes,
  ReactFlowProvider,
  type SetCenter,
} from '@xyflow/react'

import React from 'react'

import { type LineageContextHook } from './LineageContext'

import {
  type LineageEdgeData,
  type LineageNode,
  type LineageNodeData,
  type NodeId,
  type EdgeId,
  type PortId,
} from './utils'

import { LineageLayoutBase } from './LineageLayoutBase'
import { LineageLayoutContainer } from './LineageLayoutContainer'

export function LineageLayout<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TNodeID extends string = NodeId,
  TEdgeID extends string = EdgeId,
  TSourceID extends string = TNodeID,
  TTargetID extends string = TNodeID,
  TSourceHandleID extends string = PortId,
  TTargetHandleID extends string = PortId,
>({
  nodeTypes,
  edgeTypes,
  className,
  controls,
  nodesDraggable,
  nodesConnectable,
  isBuildingLayout,
  useLineage,
  onNodeClick,
  onNodeDoubleClick,
  showControlOnlySelectedNodes,
  showControlZoomToCurrentNode,
  showControlZoomToSelectedNode,
}: {
  useLineage: LineageContextHook<
    TNodeData,
    TEdgeData,
    TNodeID,
    TEdgeID,
    TSourceID,
    TTargetID,
    TSourceHandleID,
    TTargetHandleID
  >
  isBuildingLayout?: boolean
  nodeTypes?: NodeTypes
  edgeTypes?: EdgeTypes
  className?: string
  nodesDraggable?: boolean
  nodesConnectable?: boolean
  showControlOnlySelectedNodes?: boolean
  showControlZoomToCurrentNode?: boolean
  showControlZoomToSelectedNode?: boolean
  controls?:
    | React.ReactNode
    | (({ setCenter }: { setCenter: SetCenter }) => React.ReactNode)
  onNodeClick?: (
    event: React.MouseEvent<Element, MouseEvent>,
    node: LineageNode<TNodeData, TNodeID>,
  ) => void
  onNodeDoubleClick?: (
    event: React.MouseEvent<Element, MouseEvent>,
    node: LineageNode<TNodeData, TNodeID>,
  ) => void
}) {
  return (
    <ReactFlowProvider>
      <LineageLayoutContainer
        isBuildingLayout={isBuildingLayout}
        className={className}
      >
        <LineageLayoutBase
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          nodesDraggable={nodesDraggable}
          nodesConnectable={nodesConnectable}
          controls={controls}
          useLineage={useLineage}
          onNodeClick={onNodeClick}
          onNodeDoubleClick={onNodeDoubleClick}
          showControlOnlySelectedNodes={showControlOnlySelectedNodes}
          showControlZoomToCurrentNode={showControlZoomToCurrentNode}
          showControlZoomToSelectedNode={showControlZoomToSelectedNode}
        />
      </LineageLayoutContainer>
    </ReactFlowProvider>
  )
}
