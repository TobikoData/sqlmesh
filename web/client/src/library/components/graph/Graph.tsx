import React, {
  type MouseEvent,
  useEffect,
  useMemo,
  useState,
  useCallback,
  memo,
} from 'react'
import ReactFlow, {
  Controls,
  Background,
  Handle,
  Position,
  BackgroundVariant,
  type NodeProps,
  type EdgeChange,
  applyEdgeChanges,
  applyNodeChanges,
  type NodeChange,
  type Edge,
  type Node,
  useUpdateNodeInternals,
} from 'reactflow'
import { Button } from '../button/Button'
import 'reactflow/dist/base.css'
import {
  getNodesAndEdges,
  createGraphLayout,
  toNodeOrEdgeId,
  type GraphNodeData,
  mergeLineage,
} from './help'
import {
  debounceAsync,
  debounceSync,
  isArrayEmpty,
  isArrayNotEmpty,
  isFalse,
  isTrue,
} from '../../../utils'
import { EnumSize, EnumVariant } from '~/types/enum'
import { ArrowRightCircleIcon } from '@heroicons/react/24/solid'
import clsx from 'clsx'
import {
  type Column,
  columnLineageApiLineageModelNameColumnNameGet,
  type ColumnLineageApiLineageModelNameColumnNameGet200,
} from '@api/client'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import './Graph.css'
import {
  type InitialSQLMeshModel,
  type ModelSQLMeshModel,
} from '@models/sqlmesh-model'
import { useLineageFlow } from './context'
import { useQueryClient } from '@tanstack/react-query'
import Input from '@components/input/Input'
import { type ResponseWithDetail } from '@api/instance'
import { Divider } from '@components/divider/Divider'

const ModelColumnDisplay = memo(function ModelColumnDisplay({
  columnName,
  columnType,
  columnDescription,
  className,
}: {
  columnName: string
  columnType: string
  columnDescription?: string
  disabled?: boolean
  className?: string
}): JSX.Element {
  return (
    <div className={clsx('w-full items-center', className)}>
      <div className="w-full flex justify-between items-center">
        <span>{columnName}</span>
        <span className="inline-block text-neutral-400 dark:text-neutral-300 ml-4">
          {columnType}
        </span>
      </div>
      {columnDescription != null && (
        <p className="text-neutral-600 dark:text-neutral-400 mt-1">
          {columnDescription}
        </p>
      )}
    </div>
  )
})

const ModelNodeHandles = memo(function ModelNodeHandles({
  nodeId,
  id,
  hasLeft = false,
  hasRight = false,
  disabled = false,
  children,
  className,
}: {
  nodeId: string
  id: string
  children: React.ReactNode
  className?: string
  hasLeft?: boolean
  hasRight?: boolean
  disabled?: boolean
}): JSX.Element {
  const updateNodeInternals = useUpdateNodeInternals()

  useEffect(() => {
    updateNodeInternals(nodeId)
  }, [hasLeft, hasRight])

  return (
    <div
      className={clsx(
        'flex w-full !relative px-3 py-1 items-center',
        isFalse(disabled) && 'hover:bg-secondary-10 dark:hover:bg-primary-10',
        className,
      )}
    >
      {hasRight && (
        <Handle
          type="target"
          id={toNodeOrEdgeId('right', id)}
          position={Position.Right}
          isConnectable={false}
          className={clsx(
            'w-2 h-2 rounded-full !bg-secondary-500 dark:!bg-primary-500',
          )}
        />
      )}
      {children}
      {hasLeft && (
        <Handle
          type="source"
          id={toNodeOrEdgeId('left', id)}
          position={Position.Left}
          isConnectable={false}
          className={clsx(
            'w-2 h-2 rounded-full !bg-secondary-500 dark:!bg-primary-500',
          )}
        ></Handle>
      )}
    </div>
  )
})

const ModelNodeHeaderHandles = memo(function ModelNodeHeaderHandles({
  id,
  className,
  hasLeft = false,
  hasRight = false,
  label,
  type,
  handleClick,
}: {
  id: string
  label: string
  type?: string
  hasLeft?: boolean
  hasRight?: boolean
  count?: number
  className?: string
  handleClick?: (e: MouseEvent) => void
}): JSX.Element {
  return (
    <div
      className={clsx(
        'flex w-full !relative px-3 py-1 items-center',
        className,
      )}
    >
      {hasRight && (
        <Handle
          type="target"
          id={toNodeOrEdgeId('right', id)}
          position={Position.Right}
          isConnectable={false}
          className={clsx(
            'w-2 h-2 rounded-full !bg-secondary-500 dark:!bg-primary-500',
          )}
        />
      )}
      <span className="inline-block w-full">
        {type != null && (
          <span
            title={
              type === 'python'
                ? 'Column lineage disabled for Python models'
                : 'SQL Model'
            }
            className="inline-block mr-2 bg-primary-30 px-2 rounded-[0.25rem] text-[0.5rem]"
          >
            {type === 'python' && 'Python'}
            {type === 'sql' && 'SQL'}
            {type === 'seed' && 'Seed'}
          </span>
        )}
        <span
          className={clsx(
            'inline-block',
            handleClick != null && 'cursor-pointer hover:underline',
          )}
          onClick={handleClick}
        >
          {label}
        </span>
      </span>
      {hasLeft && (
        <Handle
          type="source"
          id={toNodeOrEdgeId('left', id)}
          position={Position.Left}
          isConnectable={false}
          className={clsx('!bg-transparent -ml-2 dark:text-primary-500')}
        >
          <ArrowRightCircleIcon className="w-5 bg-theme rounded-full" />
        </Handle>
      )}
    </div>
  )
})

const ModelColumn = memo(function ModelColumn({
  id,
  nodeId,
  column,
  className,
  disabled = false,
  isActive = false,
  hasLeft = false,
  hasRight = false,
  getColumnLineage,
  handleError,
  updateColumnLineage,
  removeEdges,
  selectManually,
  withHandles = false,
}: {
  id: string
  nodeId: string
  column: Column
  disabled?: boolean
  isActive?: boolean
  hasLeft?: boolean
  hasRight?: boolean
  withHandles?: boolean
  getColumnLineage: (
    columnName: string,
  ) => Promise<
    ColumnLineageApiLineageModelNameColumnNameGet200 & ResponseWithDetail
  >
  updateColumnLineage: (
    lineage: ColumnLineageApiLineageModelNameColumnNameGet200,
  ) => void
  removeEdges: (columnId: string) => void
  handleError?: (error: Error) => void
  selectManually?: React.Dispatch<
    React.SetStateAction<
      [ModelSQLMeshModel<InitialSQLMeshModel>, Column] | undefined
    >
  >
  className?: string
}): JSX.Element {
  const debouncedGetColumnLineage = useCallback(
    debounceAsync(getColumnLineage, 1000, true),
    [getColumnLineage],
  )

  const [isFetching, setIsFetching] = useState(false)
  const [isError, setIsError] = useState(false)
  const [isEmpty, setIsEmpty] = useState(false)

  useEffect(() => {
    if (selectManually == null) return

    toggleColumnLineage()
    selectManually(undefined)
  }, [selectManually])

  function toggleColumnLineage(): void {
    if (disabled) return

    if (isActive) {
      removeEdges(id)
    } else {
      setIsFetching(true)
      setIsError(false)
      setIsEmpty(false)

      debouncedGetColumnLineage(column.name)
        .then(data => {
          const models =
            data as ColumnLineageApiLineageModelNameColumnNameGet200

          setIsEmpty(() => {
            for (const modelName in models) {
              const model = models[modelName]

              if (model == null) continue

              for (const columnName in model) {
                const lineage = model[columnName]

                if (lineage == null) continue

                return Object.keys(lineage.models ?? {}).length === 0
              }
            }

            return false
          })

          updateColumnLineage(data)
        })
        .catch(error => {
          setIsError(true)
          handleError?.(error as Error)
        })
        .finally(() => {
          setIsFetching(false)
        })
    }
  }

  return (
    <div
      className={clsx(
        isActive && 'bg-secondary-10 dark:bg-primary-900',
        withHandles ? 'p-0' : 'py-1 px-2 rounded-md mb-1',
        className,
      )}
      onClick={debounceSync(toggleColumnLineage, 500)}
    >
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
        {withHandles ? (
          <ModelNodeHandles
            id={id}
            nodeId={nodeId}
            hasLeft={hasLeft}
            hasRight={hasRight}
            disabled={disabled}
          >
            <ModelColumnDisplay
              columnName={column.name}
              columnType={column.type}
              disabled={disabled}
              className={clsx(
                isError && 'text-danger-500',
                isEmpty && 'text-neutral-400 dark:text-neutral-600',
              )}
            />
          </ModelNodeHandles>
        ) : (
          <ModelColumnDisplay
            columnName={column.name}
            columnType={column.type}
            disabled={disabled}
            className={clsx(
              isError && 'text-danger-500',
              isEmpty && 'text-neutral-400 dark:text-neutral-600',
            )}
          />
        )}
      </div>
    </div>
  )
})

const ModelColumns = memo(function ModelColumns({
  nodeId,
  columns,
  disabled,
  className,
  limit = 5,
  withHandles = false,
}: {
  nodeId: string
  columns: Column[]
  disabled?: boolean
  className?: string
  limit?: number
  withHandles?: boolean
}): JSX.Element {
  const queryClient = useQueryClient()

  const {
    models,
    connections,
    isActiveColumn,
    setConnections,
    manuallySelectedColumn,
    setManuallySelectedColumn,
    handleError,
    setLineage,
    removeActiveEdges,
    hasActiveEdge,
    addActiveEdges,
  } = useLineageFlow()

  const [filter, setFilter] = useState('')
  const [showColumns, setShowColumns] = useState(columns.length <= limit)

  const [columnsSelected = [], columnsRest = []] = useMemo(() => {
    const active: Column[] = []
    const rest: Column[] = []

    columns.forEach(column => {
      if (isActiveColumn(nodeId, column.name)) {
        active.push(column)
      } else {
        if (showColumns) {
          rest.push(column)
        } else if (active.length + rest.length < limit) {
          rest.push(column)
        }
      }
    })

    return [active, rest]
  }, [nodeId, columns, showColumns, isActiveColumn, hasActiveEdge])

  const getColumnLineage = useCallback(
    async function getColumnLineage(
      columnName: string,
    ): Promise<
      ColumnLineageApiLineageModelNameColumnNameGet200 & ResponseWithDetail
    > {
      return await queryClient.fetchQuery({
        queryKey: [`/api/lineage`, nodeId, columnName],
        queryFn: async ({ signal }) =>
          await columnLineageApiLineageModelNameColumnNameGet(
            nodeId,
            columnName,
            {
              signal,
            },
          ),
        cacheTime: 0,
      })
    },
    [nodeId],
  )

  const updateColumnLineage = useCallback(
    function updateColumnLineage(
      columnLineage: ColumnLineageApiLineageModelNameColumnNameGet200,
    ): void {
      setLineage(lineage => mergeLineage(models, lineage, columnLineage))
      setConnections(connections => {
        for (const modelName in columnLineage) {
          const model = columnLineage[modelName]

          if (model == null) continue

          for (const columnName in model) {
            const column = model[columnName]

            if (column?.models == null) continue

            const connectionSource = connections.get(
              toNodeOrEdgeId(modelName, columnName),
            ) ?? {
              left: [],
              right: [],
            }

            Object.entries(column.models).forEach(([id, columns]) => {
              columns.forEach(column => {
                const connectionTarget = connections.get(
                  toNodeOrEdgeId(id, column),
                ) ?? {
                  left: [],
                  right: [],
                }

                connectionTarget.right = Array.from(
                  new Set(
                    connectionTarget.right.concat(
                      toNodeOrEdgeId(modelName, columnName),
                    ),
                  ),
                )
                connectionSource.left = Array.from(
                  new Set(
                    connectionSource.left.concat(toNodeOrEdgeId(id, column)),
                  ),
                )

                connections.set(toNodeOrEdgeId(id, column), connectionTarget)
                connections.set(
                  toNodeOrEdgeId(modelName, columnName),
                  connectionSource,
                )
              })
            })

            const modelColumnConnectionsLeft = (
              connections.get(toNodeOrEdgeId(modelName, columnName))?.left ?? []
            ).map(id => toNodeOrEdgeId('right', id))
            const modelColumnConnectionsRight = (
              connections.get(toNodeOrEdgeId(modelName, columnName))?.right ??
              []
            ).map(id => toNodeOrEdgeId('left', id))

            addActiveEdges(
              modelColumnConnectionsLeft.concat(modelColumnConnectionsRight),
            )
          }
        }

        return new Map(connections)
      })
    },
    [models, addActiveEdges, setConnections],
  )

  const isSelectManually = useCallback(
    function isSelectManually(columnName: string): boolean {
      if (manuallySelectedColumn == null) return false

      const [selectedModel, selectedColumn] = manuallySelectedColumn

      if (selectedModel == null || selectedColumn == null) return false

      return selectedModel.name === nodeId && selectedColumn.name === columnName
    },
    [nodeId, manuallySelectedColumn],
  )

  const removeEdges = useCallback(
    function removeEdges(columnId: string): void {
      removeActiveEdges(
        [columnId, walk(columnId, 'left'), walk(columnId, 'right')].flat(),
      )

      function walk(id: string, side: 'left' | 'right'): string[] {
        const edges = connections.get(id)?.[side] ?? []

        return [id, edges.map(edge => walk(edge, side))].flat(
          Infinity,
        ) as string[]
      }
    },
    [removeActiveEdges, connections],
  )

  return (
    <>
      {isArrayNotEmpty(columnsSelected) && (
        <div
          className={clsx(
            'overflow-hidden overflow-y-auto scrollbar scrollbar--vertical-md',
            withHandles ? 'w-full bg-theme-lighter cursor-default' : '',
            className,
          )}
        >
          {columnsSelected.map(column => (
            <ModelColumn
              key={toNodeOrEdgeId(nodeId, column.name)}
              id={toNodeOrEdgeId(nodeId, column.name)}
              nodeId={nodeId}
              column={column}
              disabled={disabled}
              handleError={handleError}
              getColumnLineage={getColumnLineage}
              updateColumnLineage={updateColumnLineage}
              removeEdges={removeEdges}
              isActive={true}
              hasLeft={isArrayNotEmpty(
                connections.get(toNodeOrEdgeId(nodeId, column.name))?.left,
              )}
              hasRight={isArrayNotEmpty(
                connections.get(toNodeOrEdgeId(nodeId, column.name))?.right,
              )}
              selectManually={
                isSelectManually(column.name)
                  ? setManuallySelectedColumn
                  : undefined
              }
              withHandles={withHandles}
            />
          ))}
        </div>
      )}
      {columnsRest.length > 20 && (
        <div className="p-1 w-full flex justify-between bg-theme">
          <Input
            className="w-full !m-0"
            size={EnumSize.sm}
            value={filter}
            placeholder="Filter models"
            onInput={e => {
              setFilter(e.target.value)
            }}
          />
        </div>
      )}
      <div
        className={clsx(
          'overflow-hidden overflow-y-auto scrollbar scrollbar--vertical-md py-2',
          withHandles ? 'w-full bg-theme-lighter cursor-default' : '',
          className,
        )}
      >
        {columnsRest.map(column => (
          <ModelColumn
            key={toNodeOrEdgeId(nodeId, column.name)}
            id={toNodeOrEdgeId(nodeId, column.name)}
            nodeId={nodeId}
            column={column}
            disabled={disabled}
            handleError={handleError}
            getColumnLineage={getColumnLineage}
            updateColumnLineage={updateColumnLineage}
            removeEdges={removeEdges}
            isActive={false}
            hasLeft={false}
            hasRight={false}
            selectManually={
              isSelectManually(column.name)
                ? setManuallySelectedColumn
                : undefined
            }
            className={clsx(
              filter === '' ||
                (showColumns ? column.name.includes(filter) : true)
                ? 'opacity-100'
                : 'opacity-0 h-0 overflow-hidden',
            )}
            withHandles={withHandles}
          />
        ))}
      </div>
      <Divider className="border-primary-500" />
      {columns.length > limit && (
        <div className="py-2 flex justify-center bg-theme-lighter">
          <Button
            size={EnumSize.xs}
            variant={EnumVariant.Neutral}
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              setShowColumns(prev => !prev)
            }}
          >
            {showColumns
              ? 'Hide'
              : `Show ${
                  columns.length - columnsSelected.length - columnsRest.length
                } More`}
          </Button>
        </div>
      )}
    </>
  )
})

function ModelColumnLineage({
  model,
  highlightedNodes,
  className,
}: {
  model: ModelSQLMeshModel
  highlightedNodes?: Record<string, string[]>
  className?: string
}): JSX.Element {
  const { withColumns, hasActiveEdge, models, lineage } = useLineageFlow()

  const [nodes, setNodes] = useState<Node[]>([])
  const [edges, setEdges] = useState<Edge[]>([])
  const [isBuildingLayout, setIsBuildingLayout] = useState(true)

  const nodeTypes = useMemo(() => ({ model: ModelNode }), [])

  const toggleEdge = useCallback(
    function toggleEdge(edges: Edge[] = []): Edge[] {
      return edges.map(edge => {
        if (edge.sourceHandle == null && edge.targetHandle == null) {
          edge.hidden = false
        } else {
          edge.hidden = isFalse(
            hasActiveEdge(edge.sourceHandle) &&
              hasActiveEdge(edge.targetHandle),
          )
        }

        return edge
      })
    },
    [hasActiveEdge],
  )

  useEffect(() => {
    setIsBuildingLayout(isArrayEmpty(nodes) || isArrayEmpty(edges))

    const highlightedNodesDefault = {
      'border-4 border-brand-500': [model.name],
    }

    void load()

    async function load(): Promise<void> {
      const nodesAndEdges = getNodesAndEdges({
        lineage,
        highlightedNodes: highlightedNodes ?? highlightedNodesDefault,
        models,
        nodes,
        edges,
        model,
        withColumns,
      })

      void createGraphLayout(nodesAndEdges).then(layout => {
        setNodes(layout.nodes)
        setEdges(toggleEdge(layout.edges))
        setIsBuildingLayout(
          isArrayEmpty(layout.nodes) || isArrayEmpty(layout.edges),
        )
      })
    }
  }, [model.name, models, highlightedNodes, lineage])

  useEffect(() => {
    setEdges(toggleEdge(edges))
  }, [toggleEdge])

  function onNodesChange(changes: NodeChange[]): void {
    setNodes(applyNodeChanges(changes, nodes))
  }

  function onEdgesChange(changes: EdgeChange[]): void {
    setEdges(applyEdgeChanges(changes, edges))
  }

  return (
    <div className={clsx('px-2 py-1 w-full h-full', className)}>
      {isBuildingLayout ? (
        <div>Building Lineage...</div>
      ) : (
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
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
      )}
    </div>
  )
}

function ModelNode({
  id,
  data,
  sourcePosition,
  targetPosition,
}: NodeProps & { data: GraphNodeData }): JSX.Element {
  const { models, withColumns, handleClickModel } = useLineageFlow()

  const { model, columns } = useMemo(() => {
    const model = models.get(id)

    return {
      model,
      columns: model?.columns ?? [],
    }
  }, [id, models])

  const handleClick = useCallback(
    (e: MouseEvent) => {
      e.stopPropagation()

      handleClickModel?.(id)
    },
    [handleClickModel, id, data.isInteractive],
  )

  const highlighted = Object.keys(data.highlightedNodes ?? {}).find(key =>
    data.highlightedNodes[key].includes(data.label),
  )
  const splat = data.highlightedNodes?.['*']
  const isInteractive = isTrue(data.isInteractive) && handleClickModel != null

  return (
    <div
      className={clsx(
        'text-xs font-semibold text-secondary-500 dark:text-primary-100 rounded-xl shadow-lg relative z-1',
        highlighted == null ? splat : highlighted,
      )}
    >
      <div className="drag-handle">
        <ModelNodeHeaderHandles
          id={id}
          type={model?.type}
          label={data.label}
          className={clsx(
            'bg-secondary-100 dark:bg-primary-900 py-2',
            withColumns ? 'rounded-t-lg' : 'rounded-lg',
          )}
          hasLeft={sourcePosition === Position.Left}
          hasRight={targetPosition === Position.Right}
          handleClick={isInteractive ? handleClick : undefined}
        />
      </div>
      {withColumns && isArrayNotEmpty(columns) && (
        <>
          <ModelColumns
            className="max-h-[15rem]"
            nodeId={id}
            columns={columns}
            disabled={model?.type === 'python'}
            withHandles={true}
          />
          <div className="rounded-b-lg bg-secondary-100 dark:bg-primary-900 py-1"></div>
        </>
      )}
    </div>
  )
}

export { ModelColumnLineage, ModelColumns }
