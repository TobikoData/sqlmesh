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
  toggleLineageColumn,
} from './help'
import { debounceAsync, isArrayNotEmpty, isFalse, isTrue } from '../../../utils'
import { EnumSize, EnumVariant } from '~/types/enum'
import { ArrowRightCircleIcon } from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { type Column } from '@api/client'
import { useApiColumnLineage } from '@api/index'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import './Graph.css'
import { ModelSQLMeshModel } from '@models/sqlmesh-model'
import { useLineageFlow } from './context'

const KEY_DEFAULT = 'default'

export function ModelColumnLineage({
  model,
}: {
  model: ModelSQLMeshModel
}): JSX.Element {
  const {
    nodes,
    edges,
    setNodes,
    setEdges,
    activeEdges,
    setActiveColumns,
    hasActiveEdge,
    onConnect,
    onNodesChange,
    onEdgesChange,
    models,
  } = useLineageFlow()

  const [key, setKey] = useState(KEY_DEFAULT)

  const nodeTypes = useMemo(() => ({ model: ModelNode }), [ModelNode])

  useEffect(() => {
    const nodesAndEdges = getNodesAndEdges({
      lineage: model.lineage,
      highlightedNodes: [model.name],
      models,
      nodes,
      edges,
    })

    void load()

    async function load(): Promise<void> {
      const layout = await createGraphLayout(nodesAndEdges)

      setNodes(layout.nodes)
      setEdges(toggleEdge(layout.edges))
      setActiveColumns(nodesAndEdges.activeColumns)
      setKey(nodesAndEdges.key)
    }
  }, [model, models])

  useEffect(() => {
    setEdges(toggleEdge(edges))
  }, [activeEdges])

  useEffect(() => {
    nodes.forEach(node => {
      node.position.x = 0
      node.position.y = 0
    })
  }, [model])

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
        nodes={key === KEY_DEFAULT ? [] : nodes}
        edges={key === KEY_DEFAULT ? [] : edges}
        nodeTypes={nodeTypes}
        onConnect={onConnect}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeOrigin={[0.5, 0.5]}
        fitView
      >
        <Controls className="bg-light p-1 rounded-md !border-none !shadow-lg" />
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
}: NodeProps & { data: GraphNodeData }): JSX.Element {
  const COLUMS_LIMIT_DEFAULT = 5
  const COLUMS_LIMIT_COLLAPSED = 2

  const { models, activeEdges, handleClickModel } = useLineageFlow()

  const model = models.get(data.label)
  const columns = model != null ? model.columns : []

  const [showColumns, setShowColumns] = useState(
    columns.length <= COLUMS_LIMIT_DEFAULT,
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
          {model != null && (
            <span
              title={
                model.type === 'python'
                  ? 'Column lineage disabled for Python models'
                  : 'SQL Model'
              }
              className="inline-block mr-2 bg-primary-30 px-2 rounded-[0.25rem] text-[0.5rem]"
            >
              {model.type === 'python' && 'Python'}
              {model.type === 'sql' && 'SQL'}
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

              handleClickModel?.(model.name)
            }}
          >
            <span>{data.label}</span>
          </span>
        </ModelNodeHandles>
      </div>
      {model != null && (
        <div
          className={clsx(
            'w-full py-2 bg-theme-lighter cursor-default',
            columns.length <= COLUMS_LIMIT_DEFAULT && 'rounded-b-lg',
          )}
        >
          {columnsVisible.map(column => (
            <ModelColumn
              key={column.name}
              model={model}
              column={column}
              disabled={model.type === 'python'}
            >
              {({ column, columnId, hasTarget, hasSource, disabled }) => (
                <ModelNodeHandles
                  id={columnId}
                  targetPosition={targetPosition}
                  sourcePosition={sourcePosition}
                  hasTarget={hasTarget}
                  hasSource={hasSource}
                  disabled={disabled}
                >
                  <ModelColumn.Display
                    columnName={column.name}
                    columnType={column.type}
                    disabled={disabled}
                  />
                </ModelNodeHandles>
              )}
            </ModelColumn>
          ))}
          {columnHidden.map(column => (
            <ModelColumn
              className={clsx('invisible h-0')}
              key={column.name}
              model={model}
              column={column}
              disabled={model.type === 'python'}
            >
              {({ column, columnId, hasTarget, hasSource, disabled }) => (
                <ModelNodeHandles
                  id={columnId}
                  targetPosition={targetPosition}
                  sourcePosition={sourcePosition}
                  hasTarget={hasTarget}
                  hasSource={hasSource}
                  disabled={disabled}
                >
                  <ModelColumn.Display
                    columnName={column.name}
                    columnType={column.type}
                    disabled={disabled}
                  />
                </ModelNodeHandles>
              )}
            </ModelColumn>
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

export function ModelColumn({
  model,
  column,
  className,
  disabled = false,
  children,
}: {
  model: ModelSQLMeshModel
  column: Column
  sourcePosition?: Position
  targetPosition?: Position
  disabled?: boolean
  className?: string
  children: ({
    column,
    isFetching,
    isError,
    columnId,
    hasTarget,
    hasSource,
    disabled,
  }: {
    column: Column
    isFetching: boolean
    isError: boolean
    columnId: string
    hasTarget: boolean
    hasSource: boolean
    disabled: boolean
  }) => JSX.Element
}): JSX.Element {
  const {
    models,
    refreshModels,
    activeEdges,
    hasActiveEdge,
    activeColumns,
    removeActiveEdges,
    addActiveEdges,
    manuallySelectedColumn,
    setManuallySelectedColumn,
  } = useLineageFlow()

  const columnId = toNodeOrEdgeId(model.name, column.name)
  const sourceId = toNodeOrEdgeId('source', columnId)
  const targetId = toNodeOrEdgeId('target', columnId)

  const {
    refetch: getColumnLineage,
    isFetching,
    isError,
  } = useApiColumnLineage(model.name, column.name)

  const debouncedGetColumnLineage = useCallback(
    debounceAsync(getColumnLineage, 1000, true),
    [getColumnLineage],
  )

  const toggleEdgeById = useCallback(
    function toggleEdgeById(
      action: 'add' | 'remove',
      connections: { ins: string[]; outs: string[] } = { ins: [], outs: [] },
    ): void {
      toggleLineageColumn(
        action,
        model.name,
        column.name,
        connections,
        removeActiveEdges,
        addActiveEdges,
      )
    },
    [removeActiveEdges, addActiveEdges, activeEdges],
  )

  const [isActive, setIsActive] = useState(
    hasActiveEdge(sourceId) || hasActiveEdge(targetId),
  )

  useEffect(() => {
    setIsActive(hasActiveEdge(sourceId) || hasActiveEdge(targetId))
  }, [activeEdges])

  useEffect(() => {
    if (manuallySelectedColumn == null) return

    const [selectedModel, selectedColumn] = manuallySelectedColumn

    if (selectedModel == null || selectedColumn == null) return

    if (
      selectedModel.name !== model.name ||
      selectedColumn.name !== column.name
    )
      return

    toggleColumnLineage()
    setManuallySelectedColumn(undefined)
  }, [manuallySelectedColumn])

  const hasTarget = isArrayNotEmpty(activeColumns.get(columnId)?.outs)
  const hasSource = isArrayNotEmpty(activeColumns.get(columnId)?.ins)

  function toggleColumnLineage(): void {
    if (disabled) return

    if (isFalse(isActive)) {
      void debouncedGetColumnLineage().then(({ data: columnLineage }) => {
        const upstreamModels =
          columnLineage?.[model.name]?.[column.name]?.models
        if (upstreamModels != null) {
          const columns = Object.entries(upstreamModels)
            .map(([id, columns]) =>
              columns.map(column => toNodeOrEdgeId(id, column)),
            )
            .flat()

          toggleEdgeById('add', {
            ins: columns,
            outs: [],
          })

          model.update({
            lineage: ModelSQLMeshModel.mergeLineage(
              models,
              model.lineage,
              columnLineage,
            ),
          })

          refreshModels()
        }
      })
    } else {
      toggleEdgeById('remove', activeColumns.get(columnId))
    }
  }

  return (
    <div
      key={column.name}
      className={clsx(
        isActive && 'bg-secondary-10 dark:bg-primary-900',
        className,
      )}
      onClick={toggleColumnLineage}
    >
      {children({
        hasTarget,
        hasSource,
        column,
        isFetching,
        isError,
        columnId,
        disabled,
      })}
    </div>
  )
}

ModelColumn.Display = function Display({
  columnName,
  columnType,
  columnDescription,
  isFetching = false,
  isError = false,
  disabled = false,
  isHighlighted = false,
  className,
}: {
  columnName: string
  columnType: string
  columnDescription?: string
  isFetching?: boolean
  isError?: boolean
  disabled?: boolean
  isHighlighted?: boolean
  className?: string
}): JSX.Element {
  return (
    <div
      className={clsx(
        'flex w-full items-center',
        disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer',
        className,
      )}
    >
      {isFetching && (
        <Loading className="inline-block mr-2">
          <Spinner className="w-3 h-3 border border-neutral-10" />
        </Loading>
      )}
      <div className={clsx('flex w-full items-center')}>
        <div className="w-full flex justify-between items-center">
          <span
            className={clsx(
              isError && 'text-danger-500',
              isHighlighted && 'text-brand-500',
            )}
          >
            {columnName}
          </span>
          <span className="text-neutral-400 dark:text-neutral-300">
            {columnType}
          </span>
        </div>
        <p className="text-neutral-600 dark:text-neutral-400 mt-1">
          {columnDescription}
        </p>
      </div>
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
