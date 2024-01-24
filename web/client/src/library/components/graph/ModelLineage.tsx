import { useApiModelLineage } from '@api/index'
import { useEffect, useMemo, useState } from 'react'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { type HighlightedNodes, useLineageFlow } from './context'
import ReactFlow, {
  Controls,
  Background,
  BackgroundVariant,
  type EdgeChange,
  applyEdgeChanges,
  applyNodeChanges,
  type NodeChange,
  useReactFlow,
  type Edge,
  type Node,
  Panel,
  ReactFlowProvider,
} from 'reactflow'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { createLineageWorker } from '~/workers'
import { isArrayEmpty, isFalse, isNil, isNotNil, truncate } from '@utils/index'
import ListboxShow from '@components/listbox/ListboxShow'
import SearchList from '@components/search/SearchList'
import clsx from 'clsx'
import { EnumSize } from '~/types/enum'
import { EnumLineageNodeModelType } from './Graph'
import ModelNode from './ModelNode'
import {
  getNodeMap,
  getEdges,
  getLineageIndex,
  getActiveNodes,
  getUpdatedNodes,
  getUpdatedEdges,
  createGraphLayout,
  getModelAncestors,
} from './help'

const WITH_COLUMNS_LIMIT = 30

export default function ModelLineage({
  model,
  highlightedNodes,
}: {
  model: ModelSQLMeshModel
  highlightedNodes?: HighlightedNodes
}): JSX.Element {
  const {
    setActiveEdges,
    setConnections,
    setLineage,
    handleError,
    setSelectedNodes,
    setMainNode,
    setWithColumns,
    setHighlightedNodes,
    setNodeConnections,
  } = useLineageFlow()

  const {
    refetch: getModelLineage,
    isFetching,
    cancel,
  } = useApiModelLineage(model.name)

  const [isMegringModels, setIsMergingModels] = useState(false)

  useEffect(() => {
    const lineageWorker = createLineageWorker()

    lineageWorker.addEventListener('message', handleLineageWorkerMessage)

    getModelLineage()
      .then(({ data }) => {
        if (isNil(data)) return

        const modelsCount = Object.keys(data).length

        setWithColumns(modelsCount <= WITH_COLUMNS_LIMIT)

        lineageWorker.postMessage({
          topic: 'lineage',
          payload: {
            currentLineage: {},
            newLineage: data,
            mainNode: model.fqn,
          },
        })

        setIsMergingModels(true)
      })
      .catch(error => {
        handleError?.(error)
      })
      .finally(() => {
        setActiveEdges(new Map())
        setConnections(new Map())
        setSelectedNodes(new Set())
        setMainNode(model.fqn)
      })

    return () => {
      void cancel?.()

      lineageWorker.removeEventListener('message', handleLineageWorkerMessage)
      lineageWorker.terminate()

      setLineage({})
      setNodeConnections({})
      setMainNode(undefined)
      setHighlightedNodes({})
      setWithColumns(false)
    }
  }, [model])

  useEffect(() => {
    setHighlightedNodes(highlightedNodes ?? {})
  }, [highlightedNodes])

  function handleLineageWorkerMessage(e: MessageEvent): void {
    if (e.data.topic === 'lineage') {
      setLineage(e.data.payload.lineage)
      setNodeConnections(e.data.payload.nodesConnections)
      setIsMergingModels(false)
    }
    if (e.data.topic === 'error') {
      handleError?.(e.data.error)
      setIsMergingModels(false)
    }
  }

  return (
    <div className="relative h-full w-full">
      {(isFetching || isMegringModels) && (
        <div className="absolute top-0 left-0 z-10 w-full h-full bg-theme flex justify-center items-center">
          <Loading className="inline-block">
            <Spinner className="w-3 h-3 border border-neutral-10 mr-4" />
            <h3 className="text-md">
              {isFetching ? "Loading Model's Lineage..." : "Merging Model's..."}
            </h3>
          </Loading>
        </div>
      )}
      <ReactFlowProvider>
        <ModelColumnLineage />
      </ReactFlowProvider>
    </div>
  )
}

function ModelColumnLineage(): JSX.Element {
  const {
    withColumns,
    models,
    lineage,
    mainNode,
    selectedEdges,
    selectedNodes,
    withConnected,
    withImpacted,
    withSecondary,
    hasBackground,
    activeEdges,
    connectedNodes,
    connections,
    handleError,
    setActiveNodes,
  } = useLineageFlow()

  const { setCenter } = useReactFlow()

  const [isBuildingLayout, setIsBuildingLayout] = useState(false)

  const nodeTypes = useMemo(() => ({ model: ModelNode }), [])
  const nodesMap = useMemo(
    () =>
      getNodeMap({
        lineage,
        models,
        withColumns,
      }),
    [lineage, models, withColumns],
  )
  const allEdges = useMemo(() => getEdges(lineage), [lineage])
  const lineageIndex = useMemo(() => getLineageIndex(lineage), [lineage])

  const [nodes, setNodes] = useState<Node[]>([])
  const [edges, setEdges] = useState<Edge[]>([])

  useEffect(() => {
    if (isArrayEmpty(allEdges) || isNil(mainNode)) return

    setIsBuildingLayout(true)

    const newActiveNodes = getActiveNodes(
      allEdges,
      activeEdges,
      selectedEdges,
      nodesMap,
    )
    const newNodes = getUpdatedNodes(
      Object.values(nodesMap),
      newActiveNodes,
      mainNode,
      connectedNodes,
      selectedNodes,
      connections,
      withConnected,
      withImpacted,
      withSecondary,
    )
    const newEdges = getUpdatedEdges(
      allEdges,
      connections,
      activeEdges,
      newActiveNodes,
      selectedEdges,
      selectedNodes,
      connectedNodes,
      withConnected,
      withImpacted,
      withSecondary,
    )
    const createLayout = createGraphLayout({
      nodesMap,
      nodes: newNodes,
      edges: newEdges,
    })

    createLayout
      .create()
      .then(layout => {
        setEdges(layout.edges)
        setNodes(layout.nodes)
      })
      .catch(error => {
        handleError?.(error)
        setEdges([])
        setNodes([])
      })
      .finally(() => {
        const node = isNil(mainNode) ? undefined : nodesMap[mainNode]

        if (isNotNil(node)) {
          setCenter(node.position.x, node.position.y, {
            zoom: 0.5,
            duration: 0,
          })

          setTimeout(() => {
            setIsBuildingLayout(false)
          }, 100)
        }
      })

    return () => {
      createLayout.terminate()

      setEdges([])
      setNodes([])
    }
  }, [lineageIndex, withColumns])

  useEffect(() => {
    if (isNil(mainNode) || isArrayEmpty(nodes)) return

    const newActiveNodes = getActiveNodes(
      allEdges,
      activeEdges,
      selectedEdges,
      nodesMap,
    )
    const newNodes = getUpdatedNodes(
      nodes,
      newActiveNodes,
      mainNode,
      connectedNodes,
      selectedNodes,
      connections,
      withConnected,
      withImpacted,
      withSecondary,
    )

    const newEdges = getUpdatedEdges(
      allEdges,
      connections,
      activeEdges,
      newActiveNodes,
      selectedEdges,
      selectedNodes,
      connectedNodes,
      withConnected,
      withImpacted,
      withSecondary,
    )

    setEdges(newEdges)
    setNodes(newNodes)
    setActiveNodes(newActiveNodes)
  }, [
    connections,
    nodesMap,
    allEdges,
    activeEdges,
    selectedNodes,
    selectedEdges,
    connectedNodes,
    withConnected,
    withImpacted,
    withSecondary,
    withColumns,
    mainNode,
  ])

  function onNodesChange(changes: NodeChange[]): void {
    setNodes(applyNodeChanges(changes, nodes))
  }

  function onEdgesChange(changes: EdgeChange[]): void {
    setEdges(applyEdgeChanges(changes, edges))
  }

  return (
    <>
      {isBuildingLayout && (
        <div className="absolute top-0 left-0 z-10 bg-theme flex justify-center items-center w-full h-full">
          <Loading className="inline-block">
            <Spinner className="w-3 h-3 border border-neutral-10 mr-4" />
            <h3 className="text-md">Building Lineage...</h3>
          </Loading>
        </div>
      )}
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeOrigin={[0.5, 0.5]}
        minZoom={0.05}
        maxZoom={1.5}
        snapGrid={[16, 16]}
        snapToGrid
      >
        <Panel
          position="top-right"
          className="bg-theme !m-0 w-full"
        >
          <GraphControls nodes={nodes} />
        </Panel>
        <Controls className="bg-light p-1 rounded-md !border-none !shadow-lg" />
        <Background
          variant={BackgroundVariant.Cross}
          gap={32}
          size={4}
          className={clsx(hasBackground ? 'opacity-100' : 'opacity-0')}
        />
      </ReactFlow>
    </>
  )
}

function GraphControls({ nodes = [] }: { nodes: Node[] }): JSX.Element {
  const {
    models,
    withColumns,
    lineage,
    mainNode,
    selectedNodes,
    withConnected,
    withImpacted,
    withSecondary,
    hasBackground,
    activeNodes,
    connectedNodes,
    highlightedNodes,
    setSelectedNodes,
    setWithColumns,
    setWithConnected,
    setWithImpacted,
    setWithSecondary,
    setHasBackground,
  } = useLineageFlow()

  const currentModels = useMemo(() => {
    if (isNil(mainNode) || isNil(lineage)) return []

    const ancestors = getModelAncestors(lineage, mainNode)

    return Object.keys(lineage)
      .map(model => ({
        name: model,
        displayName: models.get(model)?.displayName ?? decodeURI(model),
        description: `${
          ancestors.includes(model) ? 'Upstream' : 'Downstream'
        } | ${connectedNodes.has(model) ? 'Directly' : 'Indirectly'} Connected`,
      }))
      .filter(Boolean) as Array<{ name: string; description: string }>
  }, [lineage, mainNode])

  const model = isNil(mainNode) ? undefined : models.get(mainNode)
  const countSelected = selectedNodes.size
  const countImpact = connectedNodes.size - 1
  const countSecondary = nodes.filter(n =>
    isFalse(connectedNodes.has(n.id)),
  ).length
  const countActive =
    activeNodes.size > 0 ? activeNodes.size : connectedNodes.size
  const countHidden = nodes.filter(n => n.hidden).length
  const countVisible = nodes.filter(n => isFalse(n.hidden)).length
  const countDataSources = nodes.filter(
    n =>
      isFalse(n.hidden) &&
      (n.data.type === EnumLineageNodeModelType.external ||
        n.data.type === EnumLineageNodeModelType.seed),
  ).length
  const countCTEs = nodes.filter(
    n => isFalse(n.hidden) && n.data.type === EnumLineageNodeModelType.cte,
  ).length
  const highlightedNodeModels = useMemo(
    () => Object.values(highlightedNodes ?? {}).flat(),
    [highlightedNodes],
  )

  function handleSelect(model: { name: string; description: string }): void {
    if (highlightedNodeModels.includes(model.name) || mainNode === model.name)
      return

    setSelectedNodes(current => {
      if (current.has(model.name)) {
        current.delete(model.name)
      } else {
        current.add(model.name)
      }

      return new Set(current)
    })
  }

  return (
    <div className="pl-2 flex items-center text-xs text-neutral-400">
      <div className="contents">
        {isNotNil(model) && (
          <span
            title={model.displayName}
            className="mr-2 w-full min-w-[10rem] whitespace-nowrap text-ellipsis overflow-hidden"
          >
            <b>Model:</b> {truncate(model.displayName, 50, 25)}
          </span>
        )}
        {isNotNil(highlightedNodes) ?? (
          <span className="mr-2 whitespace-nowrap">
            <b>Highlighted:</b> {Object.keys(highlightedNodes ?? {}).length}
          </span>
        )}
        {countSelected > 0 && (
          <span className="mr-2 whitespace-nowrap">
            <b>Selected:</b> {countSelected}
          </span>
        )}
        {withImpacted && countSelected === 0 && countImpact > 0 && (
          <span className="mr-2 whitespace-nowrap">
            <b>Impact:</b> {countImpact}
          </span>
        )}
        {withSecondary && countSelected === 0 && countSecondary > 0 && (
          <span className="mr-2 whitespace-nowrap">
            <b>Secondary:</b> {countSecondary}
          </span>
        )}
        <span className="mr-2 whitespace-nowrap">
          <b>Active:</b> {countActive}
        </span>
        {countVisible > 0 && countVisible !== countActive && (
          <span className="mr-2 whitespace-nowrap">
            <b>Visible:</b> {countVisible}
          </span>
        )}
        {countHidden > 0 && (
          <span className="mr-2 whitespace-nowrap">
            <b>Hidden:</b> {countHidden}
          </span>
        )}
        {countDataSources > 0 && (
          <span className="mr-2 whitespace-nowrap">
            <b>Data Sources</b>: {countDataSources}
          </span>
        )}
        {countCTEs > 0 && (
          <span className="mr-2 whitespace-nowrap">
            <b>CTEs:</b> {countCTEs}
          </span>
        )}
      </div>
      <div className="flex w-full justify-end items-center">
        <SearchList<{ name: string; description: string }>
          list={currentModels}
          placeholder="Find"
          searchBy="displayName"
          displayBy="displayName"
          descriptionBy="description"
          showIndex={false}
          size={EnumSize.sm}
          onSelect={handleSelect}
          className="w-full min-w-[15rem] max-w-[20rem]"
          isFullWidth={true}
        />
        <ListboxShow
          options={{
            Background: setHasBackground,
            Columns:
              activeNodes.size > 0 && selectedNodes.size === 0
                ? undefined
                : setWithColumns,
            Connected: activeNodes.size > 0 ? undefined : setWithConnected,
            Impact: activeNodes.size > 0 ? undefined : setWithImpacted,
            Secondary: activeNodes.size > 0 ? undefined : setWithSecondary,
          }}
          value={
            [
              withColumns && 'Columns',
              hasBackground && 'Background',
              withConnected && 'Connected',
              withImpacted && 'Impact',
              withSecondary && 'Secondary',
            ].filter(Boolean) as string[]
          }
        />
      </div>
    </div>
  )
}
