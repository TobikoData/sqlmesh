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
import { cn } from '@/utils'

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
    edges: initialEdges,
    nodesMap,
    showOnlySelectedNodes,
    selectedNodeId,
    setZoom,
    setSelectedNodeId,
    setShowOnlySelectedNodes,
    setSelectedNodes,
    setSelectedEdges,
  } = useLineage()

  const [nodes, setNodes] = React.useState<LineageNode<TNodeData, TNodeID>[]>(
    [],
  )
  const [edges, setEdges] = React.useState<
    LineageEdge<
      TEdgeData,
      TEdgeID,
      TSourceID,
      TTargetID,
      TSourceHandleID,
      TTargetHandleID
    >[]
  >([])

  const onNodesChange = React.useCallback(
    (changes: NodeChange<LineageNode<TNodeData, TNodeID>>[]) => {
      setNodes(
        applyNodeChanges<LineageNode<TNodeData, TNodeID>>(changes, nodes),
      )
    },
    [nodes, setNodes],
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
    [edges, setEdges],
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
    [currentNode, setCenter],
  )

  const zoomToSelectedNode = React.useCallback(
    (zoom: number = DEFAULT_ZOOM) => {
      const node = selectedNodeId ? nodesMap[selectedNodeId] : null
      if (node) {
        setCenter(node.position.x, node.position.y, {
          zoom,
          duration: 0,
        })
      }
    },
    [nodesMap, selectedNodeId, setCenter],
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

  React.useEffect(() => {
    setNodes(initialNodes)
  }, [initialNodes])

  React.useEffect(() => {
    setEdges(initialEdges)
  }, [initialEdges])

  React.useEffect(() => {
    if (selectedNodeId == null) {
      setShowOnlySelectedNodes(false)
      setSelectedNodes(new Set())
      setSelectedEdges(new Set())

      return
    }

    const node = selectedNodeId ? nodesMap[selectedNodeId] : null

    if (node == null) {
      setSelectedNodeId(null)
      return
    }

    const incomers = getAllIncomers(node)
    const outgoers = getAllOutgoers(node)
    const connectedNodes = [...incomers, ...outgoers]

    if (currentNode) {
      connectedNodes.push(currentNode)
    }

    const connectedEdges = getConnectedEdges<
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
  }, [
    currentNode,
    selectedNodeId,
    setSelectedNodes,
    setSelectedEdges,
    getAllIncomers,
    getAllOutgoers,
    setShowOnlySelectedNodes,
    setSelectedNodeId,
  ])

  React.useEffect(() => {
    if (selectedNodeId) {
      zoomToSelectedNode(zoom)
    } else {
      zoomToCurrentNode(zoom)
    }
  }, [zoomToCurrentNode, zoomToSelectedNode])

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
        {currentNode && (
          <LineageControlButton
            text="Zoom to current node"
            onClick={() => zoomToCurrentNode(DEFAULT_ZOOM)}
          >
            <LineageControlIcon Icon={Crosshair} />
          </LineageControlButton>
        )}
        {selectedNodeId && (
          <>
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
            <LineageControlButton
              text="Zoom to selected node"
              onClick={() => zoomToSelectedNode(DEFAULT_ZOOM)}
            >
              <LineageControlIcon Icon={LocateFixed} />
            </LineageControlButton>
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
