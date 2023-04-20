import React, {
  type MouseEvent,
  useEffect,
  useMemo,
  useState,
  useCallback,
} from 'react'
import ReactFlow, {
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  Panel,
  Handle,
  Position,
  BackgroundVariant,
  type NodeProps,
  type Edge,
  type Node,
} from 'reactflow'
import { Button } from '../button/Button'
import { useApiDag } from '../../../api'
import 'reactflow/dist/base.css'
import { getNodesAndEdges, createGraphLayout, createGraph } from './help'
import { isFalse, isNil } from '../../../utils'
import { EnumSize, EnumVariant } from '~/types/enum'
import { useStoreContext } from '@context/context'
import { ArrowRightCircleIcon } from '@heroicons/react/24/solid'
import { useStoreLineage } from '@context/lineage'
import clsx from 'clsx'
import { type Column } from '@api/client'

export default function Graph({ closeGraph }: any): JSX.Element {
  const models = useStoreContext(s => s.models)

  const setColumns = useStoreLineage(s => s.setColumns)
  const activeEdges = useStoreLineage(s => s.activeEdges)
  const hasActiveEdge = useStoreLineage(s => s.hasActiveEdge)

  const { data } = useApiDag()

  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])

  const [graph, setGraph] = useState<{ nodes: Node[]; edges: Edge[] }>()

  const nodeTypes = useMemo(() => ({ model: ModelNode }), [ModelNode])
  const nodesAndEdges = useMemo(
    () => (data == null ? undefined : getNodesAndEdges({ data, models })),
    [data, models],
  )
  const lineage = useMemo(
    () =>
      nodesAndEdges == null
        ? undefined
        : createGraph({
            nodesMap: nodesAndEdges.nodesMap,
            edges: nodesAndEdges.edges,
          }),
    [nodesAndEdges],
  )

  useEffect(() => {
    setColumns(nodesAndEdges?.columns)
  }, [nodesAndEdges?.columns])

  useEffect(() => {
    if (isNil(data)) return

    let active = true

    void load()

    return () => {
      active = false
    }

    async function load(): Promise<void> {
      setGraph(undefined)

      if (data == null || nodesAndEdges == null || lineage == null) return

      const graph = await createGraphLayout({
        data,
        lineage,
        ...nodesAndEdges,
      })

      if (isFalse(active)) return

      setGraph(graph)
    }
  }, [data, nodesAndEdges, lineage])

  useEffect(() => {
    if (graph == null) return

    setNodes(graph.nodes)
    setEdges(toggleEdge(graph.edges))
  }, [graph])

  useEffect(() => {
    setEdges(toggleEdge)
  }, [activeEdges])

  function toggleEdge(edges: Edge[] = []): Edge[] {
    return edges.map(edge => {
      if (edge.sourceHandle != null && edge.targetHandle != null) {
        edge.hidden =
          isFalse(hasActiveEdge(edge.sourceHandle)) &&
          isFalse(hasActiveEdge(edge.targetHandle))
      } else {
        edge.hidden = false
      }

      return edge
    })
  }

  return (
    <div className="px-2 py-1 w-full h-[90vh]">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeOrigin={[0.5, 0.5]}
        nodeTypes={nodeTypes}
        fitView
      >
        <Panel
          position="top-right"
          className="flex"
        >
          <Button
            size={EnumSize.sm}
            variant={EnumVariant.Neutral}
            className="mx-0 ml-4"
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              closeGraph()
            }}
          >
            Close
          </Button>
        </Panel>
        <Controls className="!bg-primary-20 p-2 rounded-md !border-none !shadow-none" />
        <Background
          variant={BackgroundVariant.Dots}
          gap={16}
          size={2}
        />
      </ReactFlow>
    </div>
  )
}

function ModelNode({
  id,
  data,
  sourcePosition,
  targetPosition,
}: NodeProps): JSX.Element {
  const COLUMS_LIMIT_DEFAULT = 5
  const COLUMS_LIMIT_COLLAPSED = 2

  const models = useStoreContext(s => s.models)

  const activeEdges = useStoreLineage(s => s.activeEdges)
  const hasActiveEdge = useStoreLineage(s => s.hasActiveEdge)
  const addActiveEdges = useStoreLineage(s => s.addActiveEdges)
  const removeActiveEdges = useStoreLineage(s => s.removeActiveEdges)

  const columns = models.get(data.label)?.columns ?? []

  const [showColumns, setShowColumns] = useState(
    columns.length <= COLUMS_LIMIT_DEFAULT,
  )
  const toggleEdgeById = useCallback(
    function toggleEdgeById(
      isActive: boolean,
      edgeId: string,
      from: string[] = [],
      type: string,
    ): void {
      const edges = from.map(id => `${type}_${id}`)

      if (isActive) {
        removeActiveEdges([edgeId].concat(edges))
      } else {
        addActiveEdges([edgeId].concat(edges))
      }
    },
    [removeActiveEdges, addActiveEdges],
  )
  const [columnsVisible = [], columnHidden = []] = useMemo(() => {
    const visible: Column[] = []
    const rest: Column[] = []
    const hidden: Column[] = []

    if (showColumns) return [columns, []]

    columns.forEach(column => {
      const sourceId = `source_${id}_${column.name}`
      const targetId = `target_${id}_${column.name}`

      if (hasActiveEdge(sourceId) || hasActiveEdge(targetId)) {
        visible.push(column)
      } else {
        rest.push(column)
      }
    })

    rest.forEach(column => {
      if (visible.length < COLUMS_LIMIT_COLLAPSED) {
        visible.push(column)
      } else {
        hidden.push(column)
      }
    })

    return [visible, hidden]
  }, [columns, showColumns, activeEdges])

  return (
    <div
      className={clsx(
        'text-xs border-1 border-secondary-20 font-semibold text-secondary-500 dark:text-primary-100 rounded-md shadow-lg dark:border-primary-10',
      )}
    >
      <div className="drag-handle">
        <ModelNodeHandles
          id={id}
          className="rounded-t-lg bg-secondary-100 dark:bg-primary-900 py-2"
          sourcePosition={sourcePosition}
          targetPosition={targetPosition}
          isLeading={true}
        >
          {data.label}
        </ModelNodeHandles>
      </div>
      <ModelColumns
        className={clsx(
          columns.length <= COLUMS_LIMIT_DEFAULT && 'rounded-b-lg',
        )}
      >
        {columnsVisible.map(column => (
          <ModelColumn
            key={column.name}
            id={id}
            column={column}
            sourcePosition={sourcePosition}
            targetPosition={targetPosition}
            toggleEdgeById={toggleEdgeById}
          />
        ))}
        {columnHidden.map(column => (
          <ModelColumn
            className={clsx('invisible h-0')}
            key={column.name}
            id={id}
            column={column}
            sourcePosition={sourcePosition}
            targetPosition={targetPosition}
            toggleEdgeById={toggleEdgeById}
          />
        ))}
      </ModelColumns>
      {columns.length > COLUMS_LIMIT_DEFAULT && (
        <div className="flex px-3 py-2 bg-theme-lighter rounded-b-lg cursor-default">
          <Button
            className="w-full"
            size={EnumSize.xs}
            variant={EnumVariant.Neutral}
            onClick={() => {
              setShowColumns(prev => !prev)
            }}
          >
            {showColumns ? 'Hide' : `Show ${columnHidden.length} More`}
          </Button>
        </div>
      )}
    </div>
  )
}

function ModelColumns({
  children,
  className,
}: {
  children: React.ReactNode
  className?: string
}): JSX.Element {
  return (
    <ul
      className={clsx(
        'w-full py-2 bg-theme-lighter opacity-90 overflow-hidden cursor-default',
        className,
      )}
    >
      {children}
    </ul>
  )
}

function ModelColumn({
  id,
  column,
  sourcePosition,
  targetPosition,
  toggleEdgeById,
  className,
}: {
  className?: string
  id: string
  column: Column
  sourcePosition?: Position
  targetPosition?: Position
  toggleEdgeById: (
    isActive: boolean,
    edgeId: string,
    from: string[] | undefined,
    type: string,
  ) => void
}): JSX.Element {
  const columnId = `${id}_${column.name}`
  const sourceId = `source_${columnId}`
  const targetId = `target_${columnId}`

  const columns = useStoreLineage(s => s.columns)

  const [isActive, setIsActive] = useState(false)

  return (
    <li
      key={column.name}
      className={clsx(
        isActive && 'bg-secondary-10 dark:bg-primary-900',
        className,
      )}
      onClick={() => {
        setIsActive(!isActive)

        toggleEdgeById(isActive, sourceId, columns?.[columnId]?.ins, 'source')
        toggleEdgeById(isActive, targetId, columns?.[columnId]?.outs, 'target')
      }}
    >
      <ModelNodeHandles
        id={columnId}
        sourcePosition={sourcePosition}
        targetPosition={targetPosition}
      >
        <div className="flex w-full justify-between">
          <div className="mr-3 text-neutral-600 dark:text-neutral-100">
            {column.name}
          </div>
          <div className="text-neutral-400 dark:text-neutral-300">
            {column.type}
          </div>
        </div>
      </ModelNodeHandles>
    </li>
  )
}

function ModelNodeHandles({
  id,
  sourcePosition,
  targetPosition,
  children,
  className,
  isLeading = false,
}: {
  sourcePosition?: Position
  targetPosition?: Position
  children: React.ReactNode
  id?: string
  isLeading?: boolean
  className?: string
}): JSX.Element {
  return (
    <div
      className={clsx(
        'flex w-full !relative px-3 py-1 items-center',
        isFalse(isLeading) && 'hover:bg-secondary-10 dark:hover:bg-primary-10',
        className,
      )}
    >
      {targetPosition === Position.Right && (
        <Handle
          type="target"
          id={id != null ? `target_${id}` : undefined}
          position={Position.Right}
          isConnectable={false}
          className="w-2 h-2 rounded-full !bg-secondary-500 dark:!bg-primary-500"
        />
      )}
      {children}
      {sourcePosition === Position.Left && (
        <Handle
          type="source"
          id={id != null ? `source_${id}` : undefined}
          position={Position.Left}
          isConnectable={false}
          className={clsx(
            isLeading
              ? '!bg-transparent -ml-2 dark:text-primary-500'
              : 'w-2 h-2 rounded-full !bg-secondary-500 dark:!bg-primary-500',
          )}
        >
          {isLeading && <ArrowRightCircleIcon className="w-5" />}
        </Handle>
      )}
    </div>
  )
}
