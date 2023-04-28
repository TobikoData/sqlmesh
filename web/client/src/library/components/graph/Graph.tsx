import React, {
  memo,
  type MouseEvent,
  useEffect,
  useMemo,
  useState,
  useCallback,
} from 'react'
import ReactFlow, {
  Controls,
  Background,
  Panel,
  Handle,
  Position,
  BackgroundVariant,
  type NodeProps,
  type Edge,
} from 'reactflow'
import { Button } from '../button/Button'
import 'reactflow/dist/base.css'
import {
  getNodesAndEdges,
  createGraphLayout,
  toNodeOrEdgeId,
  type GraphNodeData,
} from './help'
import { debounceAsync, isArrayNotEmpty, isFalse, isTrue } from '../../../utils'
import { EnumSize, EnumVariant } from '~/types/enum'
import { useStoreContext } from '@context/context'
import { ArrowRightCircleIcon } from '@heroicons/react/24/solid'
import { useStoreLineage, useStoreReactFlow } from '@context/lineage'
import clsx from 'clsx'
import { type Column } from '@api/client'
import { useStoreFileTree } from '@context/fileTree'
import { useApiColumnLineage } from '@api/index'
import { useStoreEditor, type Lineage } from '@context/editor'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import './Graph.css'

const Flow = memo(function Flow({
  lineage,
  closeGraph,
  highlightedNodes = [],
}: {
  lineage: Record<string, Lineage>
  closeGraph?: () => void
  highlightedNodes?: string[]
}): JSX.Element {
  const models = useStoreContext(s => s.models)

  const nodes = useStoreReactFlow(s => s.nodes)
  const edges = useStoreReactFlow(s => s.edges)
  const setNodes = useStoreReactFlow(s => s.setNodes)
  const setEdges = useStoreReactFlow(s => s.setEdges)
  const onNodesChange = useStoreReactFlow(s => s.onNodesChange)
  const onEdgesChange = useStoreReactFlow(s => s.onEdgesChange)
  const onConnect = useStoreReactFlow(s => s.onConnect)

  const setColumns = useStoreLineage(s => s.setColumns)
  const activeEdges = useStoreLineage(s => s.activeEdges)
  const hasActiveEdge = useStoreLineage(s => s.hasActiveEdge)

  const [key, setKey] = useState('default')

  const nodeTypes = useMemo(() => ({ model: ModelNode }), [ModelNode])

  useEffect(() => {
    const nodesAndEdges = getNodesAndEdges({
      lineage,
      highlightedNodes,
      models,
      nodes,
      edges,
    })

    void load()

    async function load(): Promise<void> {
      const layout = await createGraphLayout(nodesAndEdges)

      setNodes(layout.nodes)
      setEdges(toggleEdge(layout.edges))
      setColumns(nodesAndEdges.columns)
      setKey(nodesAndEdges.key)
    }
  }, [lineage, highlightedNodes, models])

  useEffect(() => {
    setEdges(toggleEdge(edges))
  }, [activeEdges])

  useEffect(() => {
    nodes.forEach(node => {
      node.position.x = 0
      node.position.y = 0
    })
  }, [highlightedNodes])

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
    <div className="px-2 py-1 w-full h-full">
      <ReactFlow
        key={key}
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onConnect={onConnect}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeOrigin={[0.5, 0.5]}
        fitView
      >
        {closeGraph != null && (
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
        )}
        <Controls className="bg-light p-1 rounded-md !border-none !shadow-lg" />
        <Background
          variant={BackgroundVariant.Dots}
          gap={16}
          size={2}
        />
      </ReactFlow>
    </div>
  )
})

export default Flow

function ModelNode({
  id,
  data,
  sourcePosition,
  targetPosition,
}: NodeProps & { data: GraphNodeData }): JSX.Element {
  const COLUMS_LIMIT_DEFAULT = 5
  const COLUMS_LIMIT_COLLAPSED = 2

  const models = useStoreContext(s => s.models)

  const files = useStoreFileTree(s => s.files)
  const selectFile = useStoreFileTree(s => s.selectFile)

  const activeEdges = useStoreLineage(s => s.activeEdges)
  const addActiveEdges = useStoreLineage(s => s.addActiveEdges)
  const removeActiveEdges = useStoreLineage(s => s.removeActiveEdges)

  const model = models.get(data.label)
  const file = model != null ? files.get(model.path) : undefined
  const columns = model != null ? model.columns : []

  const [showColumns, setShowColumns] = useState(
    columns.length <= COLUMS_LIMIT_DEFAULT,
  )
  const toggleEdgeById = useCallback(
    function toggleEdgeById(
      action: 'add' | 'remove',
      edgeIds: [string, string],
      connections: { ins: string[]; outs: string[] } = { ins: [], outs: [] },
    ): void {
      const edges = [
        connections.ins.map(id => toNodeOrEdgeId('source', id)),
        connections.outs.map(id => toNodeOrEdgeId('target', id)),
      ]
        .flat()
        .concat(edgeIds)

      if (action === 'remove') {
        removeActiveEdges(edges)
      }

      if (action === 'add') {
        addActiveEdges(edges)
      }
    },
    [removeActiveEdges, addActiveEdges, activeEdges],
  )
  const [columnsVisible = [], columnHidden = []] = useMemo(() => {
    const visible: Column[] = []
    const hidden: Column[] = []

    if (showColumns) return [columns, []]

    columns.forEach(column => {
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
        'text-xs font-semibold text-secondary-500 dark:text-primary-100 rounded-xl shadow-lg relative z-1',
        isTrue(data.isHighlighted) && 'border-4 border-brand-500',
      )}
    >
      <div className="drag-handle">
        <ModelNodeHandles
          key={id}
          id={id}
          className="rounded-t-lg bg-secondary-100 dark:bg-primary-900 py-2"
          sourcePosition={sourcePosition}
          targetPosition={targetPosition}
          isLeading={true}
        >
          {file != null && (
            <span
              title={
                file.isSQLMeshModelPython
                  ? 'Column lineage disabled for Python models'
                  : 'SQL Model'
              }
              className="inline-block mr-2 bg-primary-30 px-2 rounded-[0.25rem] text-[0.5rem]"
            >
              {file.isSQLMeshModelPython && 'Python'}
              {file.isSQLMeshModelSQL && 'SQL'}
            </span>
          )}
          <span
            className={clsx(
              'inline-block',
              isTrue(data.isInteractive) && 'cursor-pointer hover:underline',
            )}
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              const model = models.get(id)

              if (isFalse(data.isInteractive) || model == null) return

              const file = files.get(model.path)

              if (file == null) return

              selectFile(file)
            }}
          >
            <span>{data.label}</span>
          </span>
        </ModelNodeHandles>
      </div>
      {file != null && (
        <div
          className={clsx(
            'w-full py-2 bg-theme-lighter cursor-default',
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
              disabled={file.isSQLMeshModelPython}
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
              disabled={file.isSQLMeshModelPython}
            />
          ))}
        </div>
      )}
      {columns.length > COLUMS_LIMIT_DEFAULT && (
        <div className="flex px-3 py-2 bg-theme-lighter rounded-b-lg cursor-default">
          <Button
            className="w-full"
            size={EnumSize.xs}
            variant={EnumVariant.Neutral}
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

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

function ModelColumn({
  id,
  column,
  sourcePosition,
  targetPosition,
  toggleEdgeById,
  className,
  disabled = false,
}: {
  className?: string
  id: string
  column: Column
  sourcePosition?: Position
  targetPosition?: Position
  disabled?: boolean
  toggleEdgeById: (
    type: 'add' | 'remove',
    edgeIds: [string, string],
    connections?: { ins: string[]; outs: string[] },
  ) => void
}): JSX.Element {
  const {
    refetch: getColumnLineage,
    isFetching,
    isError,
  } = useApiColumnLineage(id, column.name)

  const debouncedGetColumnLineage = useCallback(
    debounceAsync(getColumnLineage, 1000, true),
    [getColumnLineage],
  )

  const models = useStoreContext(s => s.models)
  const activeEdges = useStoreLineage(s => s.activeEdges)
  const hasActiveEdge = useStoreLineage(s => s.hasActiveEdge)
  const columns = useStoreLineage(s => s.columns)

  const columnId = toNodeOrEdgeId(id, column.name)
  const sourceId = toNodeOrEdgeId('source', columnId)
  const targetId = toNodeOrEdgeId('target', columnId)

  const previewLineage = useStoreEditor(s => s.previewLineage)
  const setPreviewLineage = useStoreEditor(s => s.setPreviewLineage)

  const [isActive, setIsActive] = useState(
    hasActiveEdge(sourceId) || hasActiveEdge(targetId),
  )

  useEffect(() => {
    setIsActive(hasActiveEdge(sourceId) || hasActiveEdge(targetId))
  }, [activeEdges])

  const hasTarget = isArrayNotEmpty(columns?.[columnId]?.outs)
  const hasSource = isArrayNotEmpty(columns?.[columnId]?.ins)

  return (
    <div
      key={column.name}
      className={clsx(
        isActive && 'bg-secondary-10 dark:bg-primary-900',
        className,
      )}
      onClick={() => {
        if (disabled) return

        if (isFalse(isActive)) {
          void debouncedGetColumnLineage().then(({ data: columnLineage }) => {
            const upstreamModels = columnLineage?.[id]?.[column.name]?.models
            if (upstreamModels != null) {
              const columns = Object.entries(upstreamModels)
                .map(([id, columns]) =>
                  columns.map(column => toNodeOrEdgeId(id, column)),
                )
                .flat()

              toggleEdgeById('add', [sourceId, targetId], {
                ins: columns,
                outs: [],
              })
              setPreviewLineage(models, previewLineage, columnLineage)
            }
          })
        } else {
          toggleEdgeById('remove', [sourceId, targetId], columns?.[columnId])
        }
      }}
    >
      <ModelNodeHandles
        id={columnId}
        targetPosition={targetPosition}
        sourcePosition={sourcePosition}
        hasTarget={hasTarget}
        hasSource={hasSource}
        disabled={disabled}
      >
        <div className="flex w-full items-center">
          <div className="flex items-center">
            {isFetching && (
              <Loading className="inline-block mr-2">
                <Spinner className="w-3 h-3 border border-neutral-10" />
              </Loading>
            )}
          </div>
          <div
            className={clsx(
              'w-full mr-3 flex justify-between',
              disabled && 'opacity-50 cursor-not-allowed',
            )}
          >
            <span
              className={clsx(
                'mr-3',
                isError ? 'text-danger-500' : 'text-neutral-400',
              )}
            >
              {column.name}
            </span>
            <div className="text-neutral-400 dark:text-neutral-300">
              {column.type}
            </div>
          </div>
        </div>
      </ModelNodeHandles>
    </div>
  )
}

function ModelNodeHandles({
  id,
  sourcePosition,
  targetPosition,
  children,
  className,
  isLeading = false,
  hasTarget = true,
  hasSource = true,
  disabled = false,
}: {
  id: string
  sourcePosition?: Position
  targetPosition?: Position
  children: React.ReactNode
  isLeading?: boolean
  className?: string
  hasTarget?: boolean
  hasSource?: boolean
  disabled?: boolean
}): JSX.Element {
  return (
    <div
      className={clsx(
        'flex w-full !relative px-3 py-1 items-center',
        isFalse(isLeading) &&
          isFalse(disabled) &&
          'hover:bg-secondary-10 dark:hover:bg-primary-10',
        className,
      )}
    >
      {targetPosition === Position.Right && (
        <Handle
          type="target"
          id={toNodeOrEdgeId('target', id)}
          position={Position.Right}
          isConnectable={false}
          className={clsx(
            'w-2 h-2 rounded-full !bg-secondary-500 dark:!bg-primary-500',
            hasTarget ? 'visible' : 'invisible',
          )}
        />
      )}
      {children}
      {sourcePosition === Position.Left && (
        <Handle
          type="source"
          id={toNodeOrEdgeId('source', id)}
          position={Position.Left}
          isConnectable={false}
          className={clsx(
            isLeading
              ? '!bg-transparent -ml-2 dark:text-primary-500'
              : 'w-2 h-2 rounded-full !bg-secondary-500 dark:!bg-primary-500',
            hasSource ? 'visible' : 'invisible',
          )}
        >
          {isLeading && (
            <ArrowRightCircleIcon className="w-5 bg-theme rounded-full" />
          )}
        </Handle>
      )}
    </div>
  )
}
