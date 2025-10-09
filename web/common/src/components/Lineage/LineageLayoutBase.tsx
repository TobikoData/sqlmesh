import {
  Background,
  BackgroundVariant,
  Controls,
  type EdgeChange,
  type EdgeTypes,
  type NodeChange,
  type NodeTypes,
  ReactFlow,
  type SetCenter,
  getConnectedEdges,
  getIncomers,
  getOutgoers,
  useReactFlow,
  useViewport,
  applyNodeChanges,
  applyEdgeChanges,
} from '@xyflow/react'

import '@xyflow/react/dist/style.css'
import './Lineage.css'

import { CircuitBoard, Crosshair, LocateFixed, RotateCcw } from 'lucide-react'
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
  nodesDraggable = false,
  nodesConnectable = false,
  useLineage,
  onNodeClick,
  onNodeDoubleClick,
  showControlOnlySelectedNodes = true,
  showControlZoomToCurrentNode = true,
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
  nodesDraggable?: boolean
  nodesConnectable?: boolean
  nodeTypes?: NodeTypes
  edgeTypes?: EdgeTypes
  className?: string
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
  const { zoom: viewportZoom } = useViewport()
  const { setCenter } = useReactFlow()

  const {
    currentNode,
    zoom,
    nodes: initialNodes,
    edges,
    setEdges,
    selectedNode,
    showOnlySelectedNodes,
    selectedNodeId,
    setZoom,
    setSelectedNodeId,
    setShowOnlySelectedNodes,
    setSelectedNodes,
    setSelectedEdges,
  } = useLineage()

  const [nodes, setNodes] =
    React.useState<LineageNode<TNodeData, TNodeID>[]>(initialNodes)

  const onNodesChange = React.useCallback(
    (changes: NodeChange<LineageNode<TNodeData, TNodeID>>[]) => {
      setNodes(applyNodeChanges(changes, nodes))
    },
    [nodes],
  )

  const onEdgesChange = React.useCallback(
    (
      changes: EdgeChange<
        LineageEdge<
          TEdgeData,
          TEdgeID,
          TSourceID,
          TTargetID,
          TSourceHandleID,
          TTargetHandleID
        >
      >[],
    ) => {
      setEdges(
        applyEdgeChanges<
          LineageEdge<
            TEdgeData,
            TEdgeID,
            TSourceID,
            TTargetID,
            TSourceHandleID,
            TTargetHandleID
          >
        >(changes, edges),
      )
    },
    [edges],
  )

  const updateZoom = React.useMemo(() => debounce(setZoom, 200), [setZoom])

  const zoomToCurrentNode = React.useCallback(
    (zoom: number = DEFAULT_ZOOM) => {
      if (currentNode) {
        setCenter(currentNode.position.x, currentNode.position.y, {
          zoom,
          duration: 0,
        })
      }
    },
    [currentNode?.position.x, currentNode?.position.y],
  )

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
          ...getIncomers(node, initialNodes, edges)
            .map(n => getAllIncomers(n, visited))
            .flat(),
        ]),
      )
    },
    [initialNodes, edges],
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
          ...getOutgoers(node, initialNodes, edges)
            .map(n => getAllOutgoers(n, visited))
            .flat(),
        ]),
      )
    },
    [initialNodes, edges],
  )

  const connectedNodes = React.useMemo(() => {
    if (selectedNode == null) return []

    const all = [
      ...getAllIncomers(selectedNode),
      ...getAllOutgoers(selectedNode),
    ]

    if (currentNode) {
      all.push(currentNode)
    }

    return all
  }, [selectedNode, currentNode, getAllIncomers, getAllOutgoers])

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
    setNodes(initialNodes)
  }, [initialNodes])

  React.useEffect(() => {
    if (selectedNodeId == null) {
      setShowOnlySelectedNodes(false)
      setSelectedNodes(new Set())
      setSelectedEdges(new Set())
    } else {
      if (selectedNode == null) {
        setSelectedNodeId(null)
      }
    }
  }, [selectedNodeId, selectedNode])

  React.useEffect(() => {
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

  React.useEffect(() => {
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
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      nodesDraggable={nodesDraggable}
      nodesConnectable={nodesConnectable}
      zoomOnDoubleClick={false}
      panOnScroll={true}
      zoomOnScroll={true}
      minZoom={nodes.length > NODES_TRESHOLD ? NODES_TRESHOLD_ZOOM : MIN_ZOOM}
      maxZoom={MAX_ZOOM}
      fitView={false}
      nodeOrigin={[0.5, 0.5]}
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
        {currentNode && showControlZoomToCurrentNode && (
          <LineageControlButton
            text="Zoom to current node"
            onClick={() => zoomToCurrentNode(DEFAULT_ZOOM)}
          >
            <LineageControlIcon Icon={Crosshair} />
          </LineageControlButton>
        )}
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
