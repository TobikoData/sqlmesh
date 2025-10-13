import React from 'react'

import { Focus, Rows2, Rows3 } from 'lucide-react'

import {
  type LineageNode,
  type LineageEdge,
  type LineageNodesMap,
  MAX_COLUMNS_TO_DISPLAY,
  ZOOM_THRESHOLD,
  FactoryEdgeWithGradient,
  EdgeWithGradient,
  useColumnLevelLineage,
  createNode,
  toPortID,
  calculateNodeBaseHeight,
  calculateNodeDetailsHeight,
  calculateSelectedColumnsHeight,
  buildLayout,
  calculateColumnsHeight,
  calculateNodeColumnsCount,
  createEdge,
  getEdgesFromColumnLineage,
  getOnlySelectedNodes,
  getTransformedModelEdgesTargetSources,
  getTransformedNodes,
  toNodeID,
  LineageControlButton,
  LineageControlIcon,
  LineageLayout,
  type LineageAdjacencyList,
  type LineageDetails,
  type ColumnLevelLineageAdjacencyList,
} from '@sqlmesh-common/components/Lineage'

import {
  type ModelColumnName,
  type EdgeData,
  type ModelColumnID,
  type ModelColumnLeftHandleId,
  type ModelColumnRightHandleId,
  type ModelEdgeId,
  type ModelLineageNodeDetails,
  type ModelNodeId,
  type NodeData,
  ModelLineageContext,
} from './ModelLineageContext'
import { ModelNode } from './ModelNode'
import { useModelLineage } from './ModelLineageContext'
import type { ModelFQN } from '@/domain/models'
import { NODE_TYPE_COLOR_VAR } from './help'

const nodeTypes = {
  node: ModelNode,
}
const edgeTypes = {
  edge: FactoryEdgeWithGradient(useModelLineage),
  port: EdgeWithGradient,
}

export const ModelLineage = ({
  selectedModelName,
  adjacencyList,
  lineageDetails,
  className,
  onNodeClick,
}: {
  adjacencyList: LineageAdjacencyList<ModelFQN>
  lineageDetails: LineageDetails<ModelFQN, ModelLineageNodeDetails>
  selectedModelName?: ModelFQN
  className?: string
  onNodeClick?: (
    event: React.MouseEvent<Element, MouseEvent>,
    node: LineageNode<NodeData, ModelNodeId>,
  ) => void
}) => {
  const currentNodeId = selectedModelName
    ? toNodeID<ModelNodeId>(selectedModelName)
    : null

  // Store all nodes to keep track of the position and to reuse nodes
  const allNodesMap = React.useRef<LineageNodesMap<NodeData, ModelNodeId>>({})

  const [zoom, setZoom] = React.useState(ZOOM_THRESHOLD)
  const [isBuildingLayout, setIsBuildingLayout] = React.useState(false)
  const [edges, setEdges] = React.useState<
    LineageEdge<
      EdgeData,
      ModelEdgeId,
      ModelNodeId,
      ModelNodeId,
      ModelColumnRightHandleId,
      ModelColumnLeftHandleId
    >[]
  >([])
  const [nodesMap, setNodesMap] = React.useState<
    LineageNodesMap<NodeData, ModelNodeId>
  >({})
  const [showOnlySelectedNodes, setShowOnlySelectedNodes] =
    React.useState(false)
  const [selectedNodes, setSelectedNodes] = React.useState<Set<ModelNodeId>>(
    new Set(),
  )
  const [selectedEdges, setSelectedEdges] = React.useState<Set<ModelEdgeId>>(
    new Set(),
  )
  const [selectedNodeId, setSelectedNodeId] =
    React.useState<ModelNodeId | null>(currentNodeId)

  const [showColumns, setShowColumns] = React.useState(false)
  const [columnLevelLineage, setColumnLevelLineage] = React.useState<
    Map<
      ModelColumnID,
      ColumnLevelLineageAdjacencyList<ModelFQN, ModelColumnName>
    >
  >(new Map())
  const [fetchingColumns, setFetchingColumns] = React.useState<
    Set<ModelColumnID>
  >(new Set())

  const {
    adjacencyListColumnLevel,
    selectedColumns,
    adjacencyListKeysColumnLevel,
  } = useColumnLevelLineage<
    ModelFQN,
    ModelColumnName,
    ModelColumnID,
    ColumnLevelLineageAdjacencyList<ModelFQN, ModelColumnName>
  >(columnLevelLineage)

  const adjacencyListKeys = React.useMemo(() => {
    let keys: ModelFQN[] = []

    if (adjacencyListKeysColumnLevel.length > 0) {
      keys = adjacencyListKeysColumnLevel
    } else {
      keys = Object.keys(adjacencyList) as ModelFQN[]
    }

    return keys
  }, [adjacencyListKeysColumnLevel, adjacencyList])

  const transformNode = React.useCallback(
    (nodeId: ModelNodeId, detail: ModelLineageNodeDetails) => {
      const columns = detail.columns

      const node = createNode<NodeData, ModelNodeId>('node', nodeId, {
        name: detail.name,
        displayName: detail.display_name,
        model_type: detail.model_type,
        identifier: detail.identifier,
        kind: detail.kind,
        cron: detail.cron,
        owner: detail.owner,
        dialect: detail.dialect,
        version: detail.version,
        tags: detail.tags || [],
        columns,
      })

      const selectedColumnsCount = new Set(
        Object.keys(columns ?? {}).map(k => toPortID(detail.name, k)),
      ).intersection(selectedColumns).size
      // We are trying to project the node hight so we are including the ceiling and floor heights
      const nodeBaseHeight = calculateNodeBaseHeight({
        includeNodeFooterHeight: false,
        includeCeilingHeight: true,
        includeFloorHeight: false,
      })
      const nodeDetailsHeight = calculateNodeDetailsHeight({
        nodeDetailsCount: 0,
      })
      const selectedColumnsHeight =
        calculateSelectedColumnsHeight(selectedColumnsCount)

      const columnsHeight = calculateColumnsHeight({
        columnsCount: calculateNodeColumnsCount(
          Object.keys(columns ?? {}).length,
        ),
        hasColumnsFilter:
          Object.keys(columns ?? {}).length > MAX_COLUMNS_TO_DISPLAY,
      })

      node.height =
        nodeBaseHeight +
        nodeDetailsHeight +
        selectedColumnsHeight +
        columnsHeight

      return node
    },
    [selectedColumns],
  )

  const transformedNodesMap = React.useMemo(() => {
    return getTransformedNodes<
      ModelFQN,
      ModelLineageNodeDetails,
      NodeData,
      ModelNodeId
    >(adjacencyListKeys, lineageDetails, transformNode, allNodesMap.current)
  }, [adjacencyListKeys, lineageDetails, transformNode])

  const transformEdge = React.useCallback(
    (
      edgeType: string,
      edgeId: ModelEdgeId,
      sourceId: ModelNodeId,
      targetId: ModelNodeId,
      sourceHandleId?: ModelColumnRightHandleId,
      targetHandleId?: ModelColumnLeftHandleId,
    ) => {
      const sourceNode = transformedNodesMap[sourceId]
      const targetNode = transformedNodesMap[targetId]
      const data: EdgeData = {}

      if (sourceHandleId) {
        data.startColor = 'var(--color-lineage-node-port-edge-source)'
      } else {
        if (sourceNode?.data?.model_type) {
          data.startColor = NODE_TYPE_COLOR_VAR[sourceNode.data.model_type]
        }
      }

      if (targetHandleId) {
        data.endColor = 'var(--color-lineage-node-port-edge-target)'
      } else {
        if (targetNode?.data?.model_type) {
          data.endColor = NODE_TYPE_COLOR_VAR[targetNode.data.model_type]
        }
      }

      if (sourceHandleId && targetHandleId) {
        data.strokeWidth = 2
      }

      return createEdge<
        EdgeData,
        ModelEdgeId,
        ModelNodeId,
        ModelNodeId,
        ModelColumnRightHandleId,
        ModelColumnLeftHandleId
      >(
        edgeType,
        edgeId,
        sourceId,
        targetId,
        sourceHandleId,
        targetHandleId,
        data,
      )
    },
    [transformedNodesMap],
  )

  const edgesColumnLevel = React.useMemo(
    () =>
      getEdgesFromColumnLineage<
        ModelFQN,
        ModelColumnName,
        EdgeData,
        ModelEdgeId,
        ModelNodeId,
        ModelNodeId,
        ModelColumnRightHandleId,
        ModelColumnLeftHandleId,
        ColumnLevelLineageAdjacencyList<ModelFQN, ModelColumnName>
      >({
        columnLineage: adjacencyListColumnLevel,
        transformEdge,
      }),
    [adjacencyListColumnLevel, transformEdge],
  )

  const transformedEdges = React.useMemo(() => {
    return edgesColumnLevel.length > 0
      ? edgesColumnLevel
      : getTransformedModelEdgesTargetSources<
          ModelFQN,
          EdgeData,
          ModelEdgeId,
          ModelNodeId,
          ModelNodeId,
          ModelColumnRightHandleId,
          ModelColumnLeftHandleId
        >(adjacencyListKeys, adjacencyList, transformEdge)
  }, [adjacencyListKeys, adjacencyList, transformEdge, edgesColumnLevel])

  const calculateLayout = React.useCallback(
    (
      eds: LineageEdge<
        EdgeData,
        ModelEdgeId,
        ModelNodeId,
        ModelNodeId,
        ModelColumnRightHandleId,
        ModelColumnLeftHandleId
      >[],
      nds: LineageNodesMap<NodeData, ModelNodeId>,
      buildingLayoutId: NodeJS.Timeout,
    ) => {
      clearTimeout(buildingLayoutId)

      const layoutNodesMap = buildLayout<
        NodeData,
        EdgeData,
        ModelEdgeId,
        ModelNodeId,
        ModelNodeId,
        ModelColumnRightHandleId,
        ModelColumnLeftHandleId
      >({ edges: eds, nodesMap: nds })

      allNodesMap.current = { ...allNodesMap.current, ...layoutNodesMap }

      setEdges(eds)
      setNodesMap(layoutNodesMap)
      setIsBuildingLayout(false)
    },
    [allNodesMap.current],
  )

  const nodes = React.useMemo(() => {
    return Object.values(nodesMap)
  }, [nodesMap])

  const selectedNode = selectedNodeId
    ? allNodesMap.current[selectedNodeId]
    : null

  const handleReset = React.useCallback(() => {
    setShowColumns(false)
    setEdges([])
    setNodesMap({})
    setShowOnlySelectedNodes(false)
    setSelectedNodes(new Set())
    setSelectedEdges(new Set())
    setSelectedNodeId(currentNodeId)
    setColumnLevelLineage(new Map())
  }, [currentNodeId])

  React.useEffect(() => {
    const buildingLayoutId = setTimeout(() => {
      setIsBuildingLayout(true)
    }, 500)

    if (showOnlySelectedNodes) {
      const onlySelectedNodesMap = getOnlySelectedNodes<NodeData, ModelNodeId>(
        transformedNodesMap,
        selectedNodes,
      )
      const onlySelectedEdges = transformedEdges.filter(edge =>
        selectedEdges.has(edge.id as ModelEdgeId),
      )
      calculateLayout(onlySelectedEdges, onlySelectedNodesMap, buildingLayoutId)
    } else {
      calculateLayout(transformedEdges, transformedNodesMap, buildingLayoutId)
    }
  }, [showOnlySelectedNodes, transformedEdges, transformedNodesMap])

  React.useEffect(() => {
    handleReset()
  }, [handleReset])

  function toggleColumns() {
    setShowColumns(prev => !prev)
  }

  return (
    <ModelLineageContext.Provider
      value={{
        showColumns,
        fetchingColumns,
        adjacencyListColumnLevel,
        selectedColumns,
        columnLevelLineage,
        showOnlySelectedNodes,
        selectedNodes,
        selectedEdges,
        selectedNodeId,
        selectedNode,
        currentNodeId,
        zoom,
        edges,
        nodes,
        nodesMap,
        setFetchingColumns,
        setColumnLevelLineage,
        setShowColumns,
        setShowOnlySelectedNodes,
        setSelectedNodes,
        setSelectedEdges,
        setSelectedNodeId,
        setZoom,
        setEdges,
        setNodesMap,
      }}
    >
      <LineageLayout<
        NodeData,
        EdgeData,
        ModelNodeId,
        ModelEdgeId,
        ModelNodeId,
        ModelNodeId,
        ModelColumnRightHandleId,
        ModelColumnLeftHandleId
      >
        useLineage={useModelLineage}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        className={className}
        onNodeClick={onNodeClick}
        isBuildingLayout={isBuildingLayout}
        showControlOnlySelectedNodes={selectedColumns.size === 0}
        controls={
          <>
            <LineageControlButton
              text={showColumns ? 'Hide columns' : `Show columns`}
              onClick={() => toggleColumns()}
              disabled={isBuildingLayout}
            >
              {showColumns ? (
                <LineageControlIcon Icon={Rows2} />
              ) : (
                <LineageControlIcon Icon={Rows3} />
              )}
            </LineageControlButton>
            <LineageControlButton
              text="Reset"
              onClick={() => handleReset()}
              disabled={isBuildingLayout}
            >
              <LineageControlIcon Icon={Focus} />
            </LineageControlButton>
          </>
        }
      />
    </ModelLineageContext.Provider>
  )
}
