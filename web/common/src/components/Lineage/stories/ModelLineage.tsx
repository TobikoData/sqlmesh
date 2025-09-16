import { ReactFlowProvider } from '@xyflow/react'

import '@xyflow/react/dist/style.css'

import { debounce } from 'lodash'
import { Focus, Rows2, Rows3 } from 'lucide-react'
import React from 'react'

import { type ColumnLevelLineageAdjacencyList } from '../LineageColumnLevel/ColumnLevelLineageContext'
import {
  MAX_COLUMNS_TO_DISPLAY,
  calculateColumnsHeight,
  calculateNodeColumnsCount,
  calculateSelectedColumnsHeight,
  getEdgesFromColumnLineage,
} from '../LineageColumnLevel/help'
import { useColumnLevelLineage } from '../LineageColumnLevel/useColumnLevelLineage'
import { type Column } from '../LineageColumnLevel/useColumns'
import { LineageControlButton } from '../LineageControlButton'
import { LineageControlIcon } from '../LineageControlIcon'
import { LineageLayout } from '../LineageLayout'
import { FactoryEdgeWithGradient } from '../edge/FactoryEdgeWithGradient'
import {
  calculateNodeBaseHeight,
  createEdge,
  createNode,
  getOnlySelectedNodes,
  getTransformedModelEdges,
  getTransformedNodes,
  toNodeID,
  toPortID,
} from '../help'
import {
  cleanupLayoutWorker,
  getLayoutedGraph as getDagreLayoutedGraph,
} from '../layout/dagreLayout'
import {
  type AdjacencyListColumnKey,
  type AdjacencyListKey,
  type EdgeId,
  type LineageEdge,
  type LineageNodesMap,
  type NodeId,
  type PortId,
  ZOOM_TRESHOLD,
} from '../utils'
import {
  type AdjacencyListNode,
  type EdgeData,
  ModelLineageContext,
  type ModelLineageNodeDetails,
  type NodeData,
  type NodeType,
  useModelLineage,
} from './ModelLineageContext'
import { ModelNode } from './ModelNode'
import { getNodeTypeColorVar } from './help'

const nodeTypes = {
  node: ModelNode,
}
const edgeTypes = {
  gradient: FactoryEdgeWithGradient(useModelLineage),
}

export const ModelLineage = ({
  selectedModelName,
  artifactAdjacencyList,
  artifactDetails,
  className,
}: {
  artifactAdjacencyList: Record<AdjacencyListKey, AdjacencyListNode[]>
  artifactDetails: Record<AdjacencyListKey, ModelLineageNodeDetails>
  selectedModelName?: string
  className?: string
}) => {
  const [zoom, setZoom] = React.useState(ZOOM_TRESHOLD)
  const [isBuildingLayout, setIsBuildingLayout] = React.useState(false)
  const [edges, setEdges] = React.useState<LineageEdge<EdgeData>[]>([])
  const [nodesMap, setNodesMap] = React.useState<LineageNodesMap<NodeData>>({})
  const [showOnlySelectedNodes, setShowOnlySelectedNodes] =
    React.useState(false)
  const [selectedNodes, setSelectedNodes] = React.useState<Set<NodeId>>(
    new Set(),
  )
  const [selectedEdges, setSelectedEdges] = React.useState<Set<EdgeId>>(
    new Set(),
  )
  const [selectedNodeId, setSelectedNodeId] = React.useState<NodeId | null>(
    null,
  )

  const [showColumns, setShowColumns] = React.useState(false)
  const [columnLevelLineage, setColumnLevelLineage] = React.useState<
    Map<PortId, ColumnLevelLineageAdjacencyList>
  >(new Map())
  const [fetchingColumns, setFetchingColumns] = React.useState<Set<PortId>>(
    new Set(),
  )

  const {
    adjacencyListColumnLevel,
    selectedColumns,
    adjacencyListKeysColumnLevel,
  } = useColumnLevelLineage(columnLevelLineage)

  const adjacencyListKeys = React.useMemo(() => {
    let keys: AdjacencyListKey[] = []

    if (adjacencyListKeysColumnLevel.length > 0) {
      keys = adjacencyListKeysColumnLevel
    } else {
      keys = Object.keys(artifactAdjacencyList) as AdjacencyListKey[]
    }

    return keys
  }, [adjacencyListKeysColumnLevel, artifactAdjacencyList])

  const transformNode = React.useCallback(
    (nodeId: NodeId, detail: ModelLineageNodeDetails) => {
      const columns: Record<AdjacencyListColumnKey, Column> =
        detail.columns || {}

      const node = createNode(nodeId, 'node', {
        name: detail.name as AdjacencyListKey,
        identifier: detail.identifier,
        model_type: detail.model_type as NodeType,
        kind: detail.kind!,
        cron: detail.cron,
        displayName: detail.display_name,
        owner: detail.owner!,
        dialect: detail.dialect,
        version: detail.version,
        tags: detail.tags || [],
        columns,
      })

      const hasNodeFooter = false
      const selectedColumnsCount = new Set(
        Object.keys(columns).map(k => toPortID(detail.name, k)),
      ).intersection(selectedColumns).size
      const hasColumnsFilter =
        Object.keys(columns).length > MAX_COLUMNS_TO_DISPLAY

      const baseNodeHeight = calculateNodeBaseHeight({
        hasNodeFooter,
        hasCeiling: true,
        hasFloor: false,
        nodeOptionsCount: 0,
      })
      const selectedColumnsHeight =
        calculateSelectedColumnsHeight(selectedColumnsCount)

      const columnsHeight = calculateColumnsHeight({
        columnsCount: calculateNodeColumnsCount(Object.keys(columns).length),
        hasColumnsFilter,
      })

      node.height = baseNodeHeight + selectedColumnsHeight + columnsHeight

      return node
    },
    [selectedColumns],
  )

  const transformedNodesMap = React.useMemo(() => {
    return getTransformedNodes<ModelLineageNodeDetails, NodeData>(
      adjacencyListKeys,
      artifactDetails,
      transformNode,
    )
  }, [adjacencyListKeys, artifactDetails, transformNode])

  const transformEdge = React.useCallback(
    (
      edgeId: EdgeId,
      sourceId: NodeId,
      targetId: NodeId,
      sourceHandleId?: PortId,
      targetHandleId?: PortId,
    ) => {
      const sourceNode = transformedNodesMap[sourceId]
      const targetNode = transformedNodesMap[targetId]
      const data: EdgeData = {}

      if (sourceNode?.data?.model_type) {
        data.startColor = getNodeTypeColorVar(
          sourceNode.data.model_type as NodeType,
        )
      }

      if (targetNode?.data?.model_type) {
        data.endColor = getNodeTypeColorVar(
          targetNode.data.model_type as NodeType,
        )
      }

      return createEdge<EdgeData>(
        edgeId,
        sourceId,
        targetId,
        'gradient',
        sourceHandleId,
        targetHandleId,
        data,
      )
    },
    [transformedNodesMap],
  )

  const edgesColumnLevel = React.useMemo(
    () =>
      getEdgesFromColumnLineage({
        columnLineage: adjacencyListColumnLevel,
        transformEdge,
      }),
    [adjacencyListColumnLevel, transformEdge],
  )

  const transformedEdges = React.useMemo(() => {
    return edgesColumnLevel.length > 0
      ? edgesColumnLevel
      : getTransformedModelEdges<AdjacencyListNode, EdgeData>(
          adjacencyListKeys,
          artifactAdjacencyList,
          transformEdge,
        )
  }, [
    adjacencyListKeys,
    artifactAdjacencyList,
    transformEdge,
    edgesColumnLevel,
  ])

  const calculateLayout = React.useMemo(() => {
    return debounce(
      (eds: LineageEdge<EdgeData>[], nds: LineageNodesMap<NodeData>) =>
        getDagreLayoutedGraph(eds, nds)
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
            setTimeout(() => {
              setIsBuildingLayout(false)
            })
          }),
      1000,
    )
  }, [])

  const nodes = React.useMemo(() => {
    return Object.values(nodesMap)
  }, [nodesMap])

  const currentNode = React.useMemo(() => {
    return selectedModelName
      ? nodesMap[toNodeID(selectedModelName as string)]
      : null
  }, [selectedModelName, nodesMap])

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
      const onlySelectedNodesMap = getOnlySelectedNodes<NodeData>(
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
  }, [
    calculateLayout,
    showOnlySelectedNodes,
    transformedEdges,
    transformedNodesMap,
  ])

  React.useEffect(() => {
    const currentNodeId = selectedModelName
      ? toNodeID(selectedModelName)
      : undefined

    if (currentNodeId && currentNodeId in nodesMap) {
      setSelectedNodeId(currentNodeId)
    } else {
      handleReset()
    }
  }, [handleReset, selectedModelName])

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
        isBuildingLayout,
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
        setIsBuildingLayout,
        setZoom,
        setEdges,
        setNodesMap,
      }}
    >
      <ReactFlowProvider>
        <LineageLayout<NodeData, EdgeData>
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
      </ReactFlowProvider>
    </ModelLineageContext.Provider>
  )
}
