import {
  Background,
  BackgroundVariant,
  Controls,
  type EdgeTypes,
  type NodeTypes,
  ReactFlow,
  type SetCenter,
  getConnectedEdges,
  getIncomers,
  getOutgoers,
  useReactFlow,
  useViewport,
} from '@xyflow/react'

import '@xyflow/react/dist/style.css'
import './Lineage.css'

import { CircuitBoard, LocateFixed, RotateCcw } from 'lucide-react'
import React from 'react'

import { type LineageContextHook } from './LineageContext'
import { LineageControlButton } from './LineageControlButton'
import { LineageControlIcon } from './LineageControlIcon'
import {
  DEFAULT_ZOOM,
  type LineageEdge,
  type LineageEdgeData,
  type LineageNode,
  type LineageNodeData,
  MAX_ZOOM,
  MIN_ZOOM,
  NODES_TRESHOLD,
  NODES_TRESHOLD_ZOOM,
  type NodeId,
  type EdgeId,
  type PortId,
  ZOOM_THRESHOLD,
} from './utils'

import '@xyflow/react/dist/style.css'
import './Lineage.css'
import { cn } from '@sqlmesh-common/utils'

export function LineageLayoutBase<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TEdgeID extends string = EdgeId,
  TNodeID extends string = NodeId,
  TSourceID extends string = TNodeID,
  TTargetID extends string = TNodeID,
  TSourceHandleID extends string = PortId,
  TTargetHandleID extends string = PortId,
>({
  nodeTypes,
  edgeTypes,
  className,
  controls,
  useLineage,
  onNodeClick,
  onNodeDoubleClick,
  showControlOnlySelectedNodes = true,
  showControlZoomToSelectedNode = true,
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
  nodeTypes?: NodeTypes
  edgeTypes?: EdgeTypes
  className?: string
  showControlOnlySelectedNodes?: boolean
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
  const { zoom: viewportZoom } = useViewport()
  const { setCenter } = useReactFlow()

  const {
    zoom,
    nodes,
    edges,
    selectedNode,
    showOnlySelectedNodes,
    selectedNodeId,
    setZoom,
    setShowOnlySelectedNodes,
    setSelectedNodes,
    setSelectedEdges,
  } = useLineage()

  const updateZoom = React.useMemo(() => debounce(setZoom, 200), [setZoom])

  const zoomToSelectedNode = React.useCallback(
    (zoom: number = DEFAULT_ZOOM) => {
      if (selectedNode) {
        setCenter(selectedNode.position.x, selectedNode.position.y, {
          zoom,
          duration: 0,
        })
      }
    },
    [selectedNode?.position.x, selectedNode?.position.y],
  )

  const getAllIncomers = React.useCallback(
    (
      node: LineageNode<TNodeData, TNodeID>,
      visited: Set<TNodeID> = new Set(),
    ): LineageNode<TNodeData, TNodeID>[] => {
      if (visited.has(node.id)) return []

      visited.add(node.id)

      return Array.from(
        new Set<LineageNode<TNodeData, TNodeID>>([
          node,
          ...getIncomers(node, nodes, edges)
            .map(n => getAllIncomers(n, visited))
            .flat(),
        ]),
      )
    },
    [nodes, edges],
  )

  const getAllOutgoers = React.useCallback(
    (
      node: LineageNode<TNodeData, TNodeID>,
      visited: Set<TNodeID> = new Set(),
    ): LineageNode<TNodeData, TNodeID>[] => {
      if (visited.has(node.id)) return []

      visited.add(node.id)

      return Array.from(
        new Set<LineageNode<TNodeData, TNodeID>>([
          node,
          ...getOutgoers(node, nodes, edges)
            .map(n => getAllOutgoers(n, visited))
            .flat(),
        ]),
      )
    },
    [nodes, edges],
  )

  const connectedNodes = React.useMemo(() => {
    if (selectedNode == null) return []

    const all = [
      ...getAllIncomers(selectedNode),
      ...getAllOutgoers(selectedNode),
    ]

    return all
  }, [selectedNode, getAllIncomers, getAllOutgoers])

  const connectedEdges = React.useMemo(() => {
    return getConnectedEdges<
      LineageNode<TNodeData, TNodeID>,
      LineageEdge<
        TEdgeData,
        TEdgeID,
        TSourceID,
        TTargetID,
        TSourceHandleID,
        TTargetHandleID
      >
    >(connectedNodes, edges)
  }, [connectedNodes, edges])

  React.useEffect(() => {
    if (selectedNodeId == null) {
      setShowOnlySelectedNodes(false)
      setSelectedNodes(new Set())
      setSelectedEdges(new Set())
    }
  }, [selectedNodeId, selectedNode])

  React.useLayoutEffect(() => {
    const selectedNodes = new Set<TNodeID>(connectedNodes.map(node => node.id))
    const selectedEdges = new Set(
      connectedEdges.reduce((acc, edge) => {
        if (
          [edge.source, edge.target].every(id =>
            selectedNodes.has(id as unknown as TNodeID),
          )
        ) {
          edge.zIndex = 2
          acc.add(edge.id)
        } else {
          edge.zIndex = 1
        }
        return acc
      }, new Set<TEdgeID>()),
    )

    setSelectedNodes(selectedNodes)
    setSelectedEdges(selectedEdges)
  }, [connectedNodes, connectedEdges])

  React.useLayoutEffect(() => {
    zoomToSelectedNode()
  }, [zoomToSelectedNode])

  React.useEffect(() => {
    updateZoom(viewportZoom)
  }, [updateZoom, viewportZoom])

  return (
    <ReactFlow<
      LineageNode<TNodeData, TNodeID>,
      LineageEdge<
        TEdgeData,
        TEdgeID,
        TSourceID,
        TTargetID,
        TSourceHandleID,
        TTargetHandleID
      >
    >
      className={cn('shrink-0', className)}
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      edgeTypes={edgeTypes}
      zoomOnDoubleClick={false}
      panOnScroll={true}
      zoomOnScroll={true}
      minZoom={nodes.length > NODES_TRESHOLD ? NODES_TRESHOLD_ZOOM : MIN_ZOOM}
      maxZoom={MAX_ZOOM}
      fitView={false}
      onlyRenderVisibleElements
      onNodeClick={onNodeClick}
      onNodeDoubleClick={onNodeDoubleClick}
    >
      {zoom > ZOOM_THRESHOLD && (
        <Background
          id="1"
          gap={10}
          color="var(--color-lineage-grid-dot)"
          variant={BackgroundVariant.Dots}
        />
      )}
      <Controls
        showInteractive={false}
        showFitView={false}
        position="top-right"
        className="m-1 border-2 border-lineage-control-border rounded-sm overflow-hidden"
      >
        {selectedNodeId && (
          <>
            {showControlOnlySelectedNodes && (
              <LineageControlButton
                text={
                  showOnlySelectedNodes
                    ? 'Rebuild with all nodes'
                    : 'Only selected nodes'
                }
                onClick={() => setShowOnlySelectedNodes(!showOnlySelectedNodes)}
              >
                <LineageControlIcon
                  Icon={showOnlySelectedNodes ? RotateCcw : CircuitBoard}
                />
              </LineageControlButton>
            )}
            {showControlZoomToSelectedNode && (
              <LineageControlButton
                text="Zoom to selected node"
                onClick={() => zoomToSelectedNode(DEFAULT_ZOOM)}
              >
                <LineageControlIcon Icon={LocateFixed} />
              </LineageControlButton>
            )}
          </>
        )}
        {controls && typeof controls === 'function'
          ? controls({ setCenter })
          : controls}
      </Controls>
    </ReactFlow>
  )
}

function debounce<T extends CallableFunction>(func: T, wait: number) {
  let timeout: NodeJS.Timeout
  return (...args: unknown[]) => {
    clearTimeout(timeout)
    timeout = setTimeout(() => func(...args), wait)
  }
}
