import React from 'react'

import { Focus, Rows2, Rows3 } from 'lucide-react'
import {
  FactoryEdgeWithGradient,
  EdgeWithGradient,
  ZOOM_THRESHOLD,
  type LineageEdge,
  type LineageNodesMap,
  type ColumnLevelLineageAdjacencyList,
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
  MAX_COLUMNS_TO_DISPLAY,
  toNodeID,
  LineageLayout,
  type LineageNode,
  LineageControlButton,
  LineageControlIcon,
} from '@tobikodata/sqlmesh-common/lineage'

import {
  type BrandedLineageAdjacencyList,
  type BrandedLineageDetails,
  type ColumnName,
  type EdgeData,
  type ModelColumnID,
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
  adjacencyList: BrandedLineageAdjacencyList<ModelFQN>
  lineageDetails: BrandedLineageDetails<ModelFQN, ModelLineageNodeDetails>
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

  const [zoom, setZoom] = React.useState(ZOOM_THRESHOLD)
  const [isBuildingLayout, setIsBuildingLayout] = React.useState(false)
  const [edges, setEdges] = React.useState<
    LineageEdge<EdgeData, ModelNodeId, ModelEdgeId, ModelColumnID>[]
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
    React.useState<ModelNodeId | null>(null)

  const [showColumns, setShowColumns] = React.useState(false)
  const [columnLevelLineage, setColumnLevelLineage] = React.useState<
    Map<ModelColumnID, ColumnLevelLineageAdjacencyList<ModelFQN, ColumnName>>
  >(new Map())
  const [fetchingColumns, setFetchingColumns] = React.useState<
    Set<ModelColumnID>
  >(new Set())

  const {
    adjacencyListColumnLevel,
    selectedColumns,
    adjacencyListKeysColumnLevel,
  } = useColumnLevelLineage<ModelFQN, ColumnName, ModelColumnID>(
    columnLevelLineage,
  )

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

      const node = createNode('node', nodeId, {
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
        includeFloorHeight: true,
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
    >(adjacencyListKeys, lineageDetails, transformNode)
  }, [adjacencyListKeys, lineageDetails, transformNode])

  const transformEdge = React.useCallback(
    (
      edgeType: string,
      edgeId: ModelEdgeId,
      sourceId: ModelNodeId,
      targetId: ModelNodeId,
      sourceHandleId?: ModelColumnID,
      targetHandleId?: ModelColumnID,
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

      return createEdge<EdgeData, ModelNodeId, ModelEdgeId, ModelColumnID>(
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
        ColumnName,
        EdgeData,
        ModelEdgeId,
        ModelNodeId,
        ModelColumnID
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
          ModelNodeId,
          ModelEdgeId,
          ModelColumnID
        >(adjacencyListKeys, adjacencyList, transformEdge)
  }, [adjacencyListKeys, adjacencyList, transformEdge, edgesColumnLevel])

  const calculateLayout = React.useCallback(
    (
      eds: LineageEdge<EdgeData, ModelNodeId, ModelEdgeId, ModelColumnID>[],
      nds: LineageNodesMap<NodeData>,
    ) => {
      const { edges, nodesMap } = buildLayout<
        NodeData,
        EdgeData,
        ModelNodeId,
        ModelEdgeId,
        ModelColumnID
      >({ edges: eds, nodesMap: nds })
      setEdges(edges)
      setNodesMap(nodesMap)
      setIsBuildingLayout(false)
    },
    [edges, nodesMap, setEdges, setNodesMap, setIsBuildingLayout],
  )

  const nodes = React.useMemo(() => {
    return Object.values(nodesMap)
  }, [nodesMap])

  const currentNode = React.useMemo(() => {
    return currentNodeId ? nodesMap[currentNodeId] : null
  }, [currentNodeId, nodesMap])

  const handleReset = React.useCallback(() => {
    setShowColumns(false)
    setEdges([])
    setNodesMap({})
    setShowOnlySelectedNodes(false)
    setSelectedNodes(new Set())
    setSelectedEdges(new Set())
    setSelectedNodeId(null)
    setColumnLevelLineage(new Map())
  }, [])

  React.useEffect(() => {
    setIsBuildingLayout(true)

    if (showOnlySelectedNodes) {
      const onlySelectedNodesMap = getOnlySelectedNodes<NodeData, ModelNodeId>(
        transformedNodesMap,
        selectedNodes,
      )
      const onlySelectedEdges = transformedEdges.filter(edge =>
        selectedEdges.has(edge.id as ModelEdgeId),
      )
      calculateLayout(onlySelectedEdges, onlySelectedNodesMap)
    } else {
      calculateLayout(transformedEdges, transformedNodesMap)
    }
  }, [showOnlySelectedNodes, transformedEdges, transformedNodesMap])

  // currentNodeId is passed from the parent component
  // we it change we need to reset the selectedNodeId
  React.useEffect(() => {
    if (currentNodeId) {
      setSelectedNodeId(currentNodeId)
    }
  }, [currentNodeId])

  // When the selectedColumns is empty it measn we dont have any selected columns
  // so we need to set the selectedNodeId back to the currentNode.id
  // where currentNode derived from currentNodeId if present in nodesMap
  // if the currentNode is null we need to set the selectedNodeId to null
  React.useEffect(() => {
    if (selectedColumns.size === 0 && selectedNodeId != currentNode?.id) {
      setSelectedNodeId(currentNode?.id || null)
    }
  }, [selectedColumns, currentNode?.id])

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
        zoom,
        edges,
        nodes,
        nodesMap,
        currentNode,
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
        ModelColumnID
      >
        useLineage={useModelLineage}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        className={className}
        onNodeClick={onNodeClick}
        isBuildingLayout={isBuildingLayout}
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
