import { useApiModelLineage, useApiModels } from '@/api/index'
import { useEffect, useMemo, useRef, useState } from 'react'
import { type ModelSQLMeshModel } from '@/domain/sqlmesh-model'
import { type HighlightedNodes, useLineageFlow } from './context'
import { ChevronDownIcon } from '@heroicons/react/24/solid'
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
import Loading from '@/components/loading/Loading'
import Spinner from '@/components/logo/Spinner'
import { createLineageWorker } from '@/workers/index'
import { isArrayEmpty, isFalse, isNil, isNotNil } from '@/utils/index'
import ListboxShow from '@/components/listbox/ListboxShow'
import clsx from 'clsx'
import ModelNode from './ModelNode'
import {
  getEdges,
  getLineageIndex,
  getActiveNodes,
  getUpdatedNodes,
  getUpdatedEdges,
  createGraphLayout,
} from './help'
import { Popover } from '@headlessui/react'
import ModelLineageDetails from './ModelLineageDetails'
import { Divider } from '@/components/divider/Divider'
import { type ModelLineageApiLineageModelNameGet200 } from '@/api/client'
import './Graph.css'

const WITH_COLUMNS_LIMIT = 30

export function ModelLineage({
  model,
  highlightedNodes,
}: {
  model: ModelSQLMeshModel
  highlightedNodes?: HighlightedNodes
}): JSX.Element {
  const {
    setActiveNodes,
    setActiveEdges,
    setConnections,
    setLineage,
    handleError,
    setSelectedNodes,
    setMainNode,
    setWithColumns,
    setHighlightedNodes,
    setNodeConnections,
    setLineageCache,
    setUnknownModels,
    models,
    unknownModels,
    setWithSecondary,
    setWithConnected,
    setWithImpacted,
  } = useLineageFlow()

  useEffect(() => {
    setWithColumns(true)
    setWithSecondary(true)
    setWithConnected(true)
    setWithImpacted(true)
  }, [setWithSecondary])

  const { refetch: getModelLineage, isFetching: isFetchingModelLineage } =
    useApiModelLineage(model.name)
  const { isFetching: isFetchingModels } = useApiModels()

  const [isMergingModels, setIsMergingModels] = useState(false)
  const [modelLineage, setModelLineage] = useState<
    ModelLineageApiLineageModelNameGet200 | undefined
  >(undefined)

  useEffect(() => {
    const lineageWorker = new createLineageWorker()

    lineageWorker.addEventListener('message', handleLineageWorkerMessage)

    getModelLineage()
      .then(({ data }) => {
        setModelLineage(data)
        if (isNil(data)) return

        setIsMergingModels(true)

        lineageWorker.postMessage({
          topic: 'lineage',
          payload: {
            currentLineage: {},
            newLineage: data,
            mainNode: model.fqn,
          },
        })
      })
      .catch(error => {
        handleError?.(error)
      })
      .finally(() => {
        setActiveNodes(new Set())
        setActiveEdges(new Map())
        setConnections(new Map())
        setSelectedNodes(new Set())
        setLineageCache(undefined)
        setMainNode(model.fqn)
      })

    return () => {
      lineageWorker.removeEventListener('message', handleLineageWorkerMessage)
      lineageWorker.terminate()

      setLineage({})
      setNodeConnections({})
      setMainNode(undefined)
      setHighlightedNodes({})
    }
  }, [model.name, model.hash])

  useEffect(() => {
    Object.keys(modelLineage ?? {}).forEach(modelName => {
      modelName = encodeURI(modelName)

      if (isFalse(modelName in models) && isFalse(modelName in unknownModels)) {
        unknownModels.add(modelName)
      }
    })

    setUnknownModels(new Set(unknownModels))
  }, [modelLineage, models])

  useEffect(() => {
    setHighlightedNodes(highlightedNodes ?? {})
  }, [highlightedNodes])

  function handleLineageWorkerMessage(e: MessageEvent): void {
    if (e.data.topic === 'lineage') {
      setIsMergingModels(false)
      setNodeConnections(e.data.payload.nodesConnections)
      setLineage(e.data.payload.lineage)

      if (
        Object.values(e.data.payload?.lineage ?? {}).length > WITH_COLUMNS_LIMIT
      ) {
        setWithColumns(false)
      }
    }

    if (e.data.topic === 'error') {
      handleError?.(e.data.error)
      setIsMergingModels(false)
    }
  }

  const isFetching =
    isFetchingModelLineage || isFetchingModels || isMergingModels

  return (
    <div className="relative h-full w-full overflow-hidden">
      {isFetching && (
        <div className="absolute top-0 left-0 z-10 flex justify-center items-center w-full h-full">
          <span className="absolute w-full h-full z-10 bg-transparent-20 backdrop-blur-lg"></span>
          <Loading className="inline-block z-10">
            <Spinner className="w-3 h-3 border border-neutral-10 mr-4" />
            <h3 className="text-md whitespace-nowrap">
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
    nodesMap,
    showControls,
    handleError,
    setActiveNodes,
  } = useLineageFlow()

  const { setCenter } = useReactFlow()

  const [isBuildingLayout, setIsBuildingLayout] = useState(false)

  const nodeTypes = useMemo(() => ({ model: ModelNode }), [])

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
        }

        setTimeout(() => {
          setIsBuildingLayout(false)
        }, 100)
      })

    return () => {
      createLayout.terminate()

      setEdges([])
      setNodes([])
    }
  }, [activeEdges, nodesMap, lineageIndex])

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
        <div className="absolute top-0 left-0 z-10 flex justify-center items-center w-full h-full">
          <span className="absolute w-full h-full z-10 bg-transparent-20 backdrop-blur-lg"></span>
          <Loading className="inline-block z-10">
            <Spinner className="w-3 h-3 border border-neutral-10 mr-4" />
            <h3 className="text-md whitespace-nowrap">Building Lineage...</h3>
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
        {showControls && (
          <Panel
            position="top-right"
            className="bg-theme !m-0 w-full !z-10"
          >
            <GraphControls nodes={nodes} />
            <Divider />
          </Panel>
        )}
        <Controls className="bg-light p-1 rounded-md !border-none !shadow-lg" />
        <Background
          variant={BackgroundVariant.Cross}
          gap={32}
          size={4}
          className={clsx(
            hasBackground
              ? 'opacity-100 stroke-neutral-200 dark:stroke-neutral-800'
              : 'opacity-0',
          )}
        />
      </ReactFlow>
    </>
  )
}

function GraphControls({ nodes = [] }: { nodes: Node[] }): JSX.Element {
  const {
    withColumns,
    // mainNode,
    selectedNodes,
    withConnected,
    withImpacted,
    withSecondary,
    hasBackground,
    activeNodes,
    // highlightedNodes,
    // setSelectedNodes,
    setWithColumns,
    setWithConnected,
    setWithImpacted,
    setWithSecondary,
    setHasBackground,
  } = useLineageFlow()

  useEffect(() => {
    setWithColumns(true)
    setWithSecondary(true)
    setWithConnected(true)
    setWithImpacted(true)
  }, [setWithSecondary])

  const lineageInfoTrigger = useRef<HTMLButtonElement>(null)

  // const highlightedNodeModels = useMemo(
  //   () => Object.values(highlightedNodes ?? {}).flat(),
  //   [highlightedNodes],
  // )
  // function handleSelect(model: { name: string; description: string }): void {
  //   if (highlightedNodeModels.includes(model.name) || mainNode === model.name)
  //     return

  //   setSelectedNodes(current => {
  //     if (current.has(model.name)) {
  //       current.delete(model.name)
  //     } else {
  //       current.add(model.name)
  //     }

  //     return new Set(current)
  //   })
  // }

  return (
    <div className="px-2 flex items-center text-xs text-neutral-400 @container">
      <div className="contents">
        <Popover
          className="flex @lg:hidden bg-none border-none"
          aria-label="Show lineage node details"
        >
          <Popover.Button
            ref={lineageInfoTrigger}
            className="flex items-center relative w-full cursor-pointer bg-primary-10 text-xs rounded-full text-primary-500 py-1 px-3 text-center focus:outline-none focus-visible:border-accent-500 focus-visible:ring-2 focus-visible:ring-light focus-visible:ring-opacity-75 focus-visible:ring-offset-2 focus-visible:ring-offset-brand-300 border-1 border-transparent"
          >
            Details
            <ChevronDownIcon
              className="ml-2 h-4 w-4"
              aria-hidden="true"
            />
          </Popover.Button>
          <Popover.Panel className="absolute left-2 right-2 flex-col z-50 mt-8 transform flex px-4 py-3 bg-theme-lighter shadow-xl focus:ring-2 ring-opacity-5 rounded-lg">
            <ModelLineageDetails nodes={nodes} />
          </Popover.Panel>
        </Popover>
        <div className="hidden @lg:contents w-full">
          <ModelLineageDetails nodes={nodes} />
        </div>
      </div>
      <div className="flex w-full justify-end items-center">
        {/* <ModelLineageSearch handleSelect={handleSelect} /> */}
        <ListboxShow
          options={{
            Background: setHasBackground,
            Columns:
              activeNodes.size > 0 && selectedNodes.size === 0
                ? undefined
                : setWithColumns,
            Connected: activeNodes.size > 0 ? undefined : setWithConnected,
            'Upstream/Downstream':
              activeNodes.size > 0 ? undefined : setWithImpacted,
            All: activeNodes.size > 0 ? undefined : setWithSecondary,
          }}
          value={
            [
              withColumns && 'Columns',
              hasBackground && 'Background',
              withConnected && 'Connected',
              withImpacted && 'Upstream/Downstream',
              withSecondary && 'All',
            ].filter(Boolean) as string[]
          }
        />
      </div>
    </div>
  )
}
