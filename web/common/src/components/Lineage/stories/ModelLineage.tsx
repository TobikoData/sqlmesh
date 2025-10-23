import { Focus, Rows2, Rows3 } from 'lucide-react'
import React from 'react'

import {
  MAX_COLUMNS_TO_DISPLAY,
  calculateColumnsHeight,
  calculateNodeColumnsCount,
  calculateSelectedColumnsHeight,
  getEdgesFromColumnLineage,
} from '../LineageColumnLevel/help'
import { useColumnLevelLineage } from '../LineageColumnLevel/useColumnLevelLineage'
import { LineageLayout } from '../LineageLayout'
import { FactoryEdgeWithGradient } from '../edge/FactoryEdgeWithGradient'
import {
  calculateNodeBaseHeight,
  calculateNodeDetailsHeight,
  createEdge,
  createNode,
  getOnlySelectedNodes,
  getTransformedModelEdgesSourceTargets,
  getTransformedNodes,
} from '../help'
import {
  type LineageEdge,
  type LineageNodesMap,
  toNodeID,
  toPortID,
  ZOOM_THRESHOLD,
} from '../utils'
import {
  type EdgeData,
  ModelLineageContext,
  type ModelLineageNodeDetails,
  type ModelName,
  type ColumnName,
  type NodeData,
  useModelLineage,
  type ModelNodeId,
  type ModelColumnID,
  type ModelEdgeId,
  type NodeType,
  type BrandedLineageAdjacencyList,
  type BrandedLineageDetails,
  type BrandedColumnLevelLineageAdjacencyList,
  type ModelColumn,
  type ModelDisplayName,
  type LeftHandleId,
  type RightHandleId,
  type LeftPortHandleId,
  type RightPortHandleId,
} from './ModelLineageContext'
import { ModelNode } from './ModelNode'
import { getNodeTypeColorVar } from './help'
import { EdgeWithGradient } from '../edge/EdgeWithGradient'
import { cleanupLayoutWorker, getLayoutedGraph } from '../layout/help'
import { LineageControlButton } from '../LineageControlButton'
import { LineageControlIcon } from '../LineageControlIcon'
import type { BrandedRecord } from '@sqlmesh-common/types'

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
}: {
  adjacencyList: BrandedLineageAdjacencyList
  lineageDetails: BrandedLineageDetails
  selectedModelName?: ModelName
  className?: string
}) => {
  const currentNodeId = selectedModelName
    ? toNodeID<ModelNodeId>(selectedModelName)
    : null

  const [zoom, setZoom] = React.useState(ZOOM_THRESHOLD)
  const [isBuildingLayout, setIsBuildingLayout] = React.useState(false)
  const [edges, setEdges] = React.useState<
    LineageEdge<
      EdgeData,
      ModelEdgeId,
      LeftHandleId,
      RightHandleId,
      LeftPortHandleId,
      RightPortHandleId
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
    Map<ModelColumnID, BrandedColumnLevelLineageAdjacencyList>
  >(new Map())
  const [fetchingColumns, setFetchingColumns] = React.useState<
    Set<ModelColumnID>
  >(new Set())

  const {
    adjacencyListColumnLevel,
    selectedColumns,
    adjacencyListKeysColumnLevel,
  } = useColumnLevelLineage<
    ModelName,
    ColumnName,
    ModelColumnID,
    BrandedColumnLevelLineageAdjacencyList
  >(columnLevelLineage)

  const adjacencyListKeys = React.useMemo(() => {
    let keys: ModelName[] = []

    if (adjacencyListKeysColumnLevel.length > 0) {
      keys = adjacencyListKeysColumnLevel
    } else {
      keys = Object.keys(adjacencyList) as ModelName[]
    }

    return keys
  }, [adjacencyListKeysColumnLevel, adjacencyList])

  const transformNode = React.useCallback(
    (nodeId: ModelNodeId, detail: ModelLineageNodeDetails) => {
      const columns = detail.columns

      const node = createNode<NodeData, ModelNodeId>('node', nodeId, {
        name: detail.name as ModelName,
        displayName: detail.display_name as ModelDisplayName,
        identifier: detail.identifier,
        model_type: detail.model_type as NodeType,
        kind: detail.kind!,
        cron: detail.cron,
        owner: detail.owner!,
        dialect: detail.dialect,
        version: detail.version,
        tags: detail.tags || [],
        columns: columns as BrandedRecord<ColumnName, ModelColumn>,
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
      ModelName,
      ModelLineageNodeDetails,
      NodeData,
      ModelNodeId
    >(adjacencyListKeys, lineageDetails, transformNode)
  }, [adjacencyListKeys, lineageDetails, transformNode])

  const transformEdge = React.useCallback(
    (
      edgeType: string,
      edgeId: ModelEdgeId,
      sourceId: LeftHandleId,
      targetId: RightHandleId,
      sourceHandleId?: LeftPortHandleId,
      targetHandleId?: RightPortHandleId,
    ) => {
      const sourceNode = transformedNodesMap[sourceId]
      const targetNode = transformedNodesMap[targetId]
      const data: EdgeData = {}

      if (sourceHandleId) {
        data.startColor = 'var(--color-lineage-node-port-edge-source)'
      } else {
        if (sourceNode?.data?.model_type) {
          data.startColor = getNodeTypeColorVar(
            sourceNode.data.model_type as NodeType,
          )
        }
      }

      if (targetHandleId) {
        data.endColor = 'var(--color-lineage-node-port-edge-target)'
      } else {
        if (targetNode?.data?.model_type) {
          data.endColor = getNodeTypeColorVar(
            targetNode.data.model_type as NodeType,
          )
        }
      }

      if (sourceHandleId && targetHandleId) {
        data.strokeWidth = 2
      }

      return createEdge<
        EdgeData,
        ModelEdgeId,
        LeftHandleId,
        RightHandleId,
        LeftPortHandleId,
        RightPortHandleId
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
        ModelName,
        ColumnName,
        EdgeData,
        ModelEdgeId,
        LeftHandleId,
        RightHandleId,
        LeftPortHandleId,
        RightPortHandleId,
        BrandedColumnLevelLineageAdjacencyList
      >({
        columnLineage: adjacencyListColumnLevel,
        transformEdge,
      }),
    [adjacencyListColumnLevel, transformEdge],
  )

  const transformedEdges = React.useMemo(() => {
    return edgesColumnLevel.length > 0
      ? edgesColumnLevel
      : getTransformedModelEdgesSourceTargets<
          ModelName,
          EdgeData,
          ModelEdgeId,
          LeftHandleId,
          RightHandleId,
          LeftPortHandleId,
          RightPortHandleId
        >(adjacencyListKeys, adjacencyList, transformEdge)
  }, [adjacencyListKeys, adjacencyList, transformEdge, edgesColumnLevel])

  const calculateLayout = React.useCallback(
    (
      eds: LineageEdge<
        EdgeData,
        ModelEdgeId,
        LeftHandleId,
        RightHandleId,
        LeftPortHandleId,
        RightPortHandleId
      >[],
      nds: LineageNodesMap<NodeData, ModelNodeId>,
    ) =>
      getLayoutedGraph(
        eds,
        nds,
        new URL('./dagreLayout.worker.ts', import.meta.url),
      )
        .then(({ edges, nodesMap }) => {
          setEdges(edges)
          setNodesMap(nodesMap)
        })
        .catch(error => {
          console.error('Layout processing failed:', error)

          setEdges([])
          setNodesMap({})
        })
        .finally(() => {
          setIsBuildingLayout(false)
        }),
    [setEdges, setNodesMap, setIsBuildingLayout],
  )

  const nodes = React.useMemo(() => {
    return Object.values(nodesMap)
  }, [nodesMap])

  const selectedNode = selectedNodeId ? nodesMap[selectedNodeId] : null

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
        selectedEdges.has(edge.id),
      )
      calculateLayout(onlySelectedEdges, onlySelectedNodesMap)
    } else {
      calculateLayout(transformedEdges, transformedNodesMap)
    }
  }, [showOnlySelectedNodes, transformedEdges, transformedNodesMap])

  React.useEffect(() => {
    setSelectedNodeId(currentNodeId)
  }, [currentNodeId])

  // Cleanup worker on unmount
  React.useEffect(() => () => cleanupLayoutWorker(), [])

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
        LeftHandleId,
        RightHandleId,
        LeftPortHandleId,
        RightPortHandleId
      >
        isBuildingLayout={isBuildingLayout}
        useLineage={useModelLineage}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        className={className}
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
