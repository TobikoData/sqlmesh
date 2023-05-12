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
      <div className={clsx('w-full items-center')}>
        <div className="w-full flex justify-between items-center">
          <span
            className={clsx(
              isError && 'text-danger-500',
              isHighlighted && 'text-brand-500',
            )}
          >
            {columnName}
          </span>
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
    </div>
  )
})

const ModelNodeHandles = memo(function ModelNodeHandles({
  scope,
  id,
  hasTarget = false,
  hasSource = false,
  disabled = false,
  children,
  className,
}: {
  scope: string
  id: string
  children: React.ReactNode
  className?: string
  hasTarget?: boolean
  hasSource?: boolean
  disabled?: boolean
}): JSX.Element {
  const updateNodeInternals = useUpdateNodeInternals()

  useEffect(() => {
    updateNodeInternals(scope)
  }, [hasTarget, hasSource])

  return (
    <div
      className={clsx(
        'flex w-full !relative px-3 py-1 items-center',
        isFalse(disabled) && 'hover:bg-secondary-10 dark:hover:bg-primary-10',
        className,
      )}
    >
      {hasSource && (
        <Handle
          type="target"
          id={toNodeOrEdgeId('target', id)}
          position={Position.Right}
          isConnectable={false}
          className={clsx(
            'w-2 h-2 rounded-full !bg-secondary-500 dark:!bg-primary-500',
          )}
        />
      )}
      {children}
      {hasTarget && (
        <Handle
          type="source"
          id={toNodeOrEdgeId('source', id)}
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
  hasTarget = true,
  hasSource = true,
  count = 0,
  label,
  type,
  handleClick,
}: {
  id: string
  label: string
  type?: string
  hasTarget?: boolean
  hasSource?: boolean
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
      {hasTarget && (
        <Handle
          type="target"
          id={toNodeOrEdgeId('target', id)}
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
      <span className="inline-block bg-primary-30 px-2 rounded-[0.25rem] text-[0.5rem] ml-3">
        {count}
      </span>
      {hasSource && (
        <Handle
          type="source"
          id={toNodeOrEdgeId('source', id)}
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
  scope,
  column,
  className,
  disabled = false,
  isActive = false,
  connections = { ins: [], outs: [] },
  getColumnLineage,
  handleError,
  updateColumnLineage,
  removeEdges,
  selectManually,
}: {
  id: string
  scope: string
  column: Column
  disabled?: boolean
  isActive?: boolean
  getColumnLineage: (
    columnName: string,
  ) => Promise<
    ColumnLineageApiLineageModelNameColumnNameGet200 & ResponseWithDetail
  >
  updateColumnLineage: (
    lineage: ColumnLineageApiLineageModelNameColumnNameGet200,
  ) => void
  removeEdges: (
    columnId: string,
    connections: { ins: string[]; outs: string[] },
  ) => void
  connections?: { ins: string[]; outs: string[] }
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

  useEffect(() => {
    if (selectManually == null) return

    toggleColumnLineage()
    selectManually(undefined)
  }, [selectManually])

  function toggleColumnLineage(): void {
    if (disabled) return

    if (isActive) {
      removeEdges(id, connections)
    } else {
      setIsFetching(true)
      setIsError(false)

      debouncedGetColumnLineage(column.name)
        .then(updateColumnLineage)
        .catch(error => {
          setIsError(true)
          handleError?.(error as Error)
        })
        .finally(() => {
          setIsFetching(false)
        })
    }
  }

  console.log('ModelColumn')

  return (
    <div
      className={clsx(
        isActive && 'bg-secondary-10 dark:bg-primary-900',
        className,
      )}
      onClick={debounceSync(toggleColumnLineage, 500)}
    >
      <ModelNodeHandles
        id={id}
        scope={scope}
        hasTarget={isActive}
        hasSource={isActive}
        disabled={disabled}
      >
        <ModelColumnDisplay
          columnName={column.name}
          columnType={column.type}
          disabled={disabled}
          isError={isError}
          isFetching={isFetching}
        />
      </ModelNodeHandles>
    </div>
  )
})

const ModelColumns = memo(function ModelColumns({
  scope,
  columns,
  disabled,
  className,
  limit = 5,
}: {
  scope: string
  columns: Column[]
  disabled?: boolean
  className?: string
  limit?: number
}): JSX.Element {
  const queryClient = useQueryClient()

  const {
    models,
    isActiveColumn,
    // activeEdges,
    activeColumns,
    manuallySelectedColumn,
    setManuallySelectedColumn,
    handleError,
    setLineage,
    toggleLineageColumn,
    hasActiveEdge,
    addActiveEdges,
  } = useLineageFlow()

  const [filter, setFilter] = useState('')
  const [showColumns, setShowColumns] = useState(columns.length <= limit)

  const [columnsSelected = [], columnsRest = []] = useMemo(() => {
    const active: Column[] = []
    const rest: Column[] = []

    columns.forEach(column => {
      if (isActiveColumn(scope, column.name)) {
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
  }, [scope, columns, showColumns, isActiveColumn, hasActiveEdge])

  const getColumnLineage = useCallback(
    async function getColumnLineage(
      columnName: string,
    ): Promise<
      ColumnLineageApiLineageModelNameColumnNameGet200 & ResponseWithDetail
    > {
      return await queryClient.fetchQuery({
        queryKey: [`/api/lineage`, scope, columnName],
        queryFn: async ({ signal }) =>
          await columnLineageApiLineageModelNameColumnNameGet(
            scope,
            columnName,
            {
              signal,
            },
          ),
        cacheTime: 0,
      })
    },
    [scope],
  )

  const updateColumnLineage = useCallback(
    function updateColumnLineage(
      columnLineage: ColumnLineageApiLineageModelNameColumnNameGet200,
    ): void {
      for (const modelName in columnLineage) {
        const model = columnLineage[modelName]

        if (model == null) continue

        for (const columnName in model) {
          const column = model[columnName]

          if (column?.models == null) continue

          const sources = Object.entries(column.models)

          const columns = new Set(
            sources
              .map(([id, columns]) =>
                columns.map(column => toNodeOrEdgeId('source', id, column)),
              )
              .flat(),
          )

          const output = Array.from(columns)

          if (sources.length > 0) {
            output.push(toNodeOrEdgeId('target', modelName, columnName))
          }

          addActiveEdges(output)

          setLineage(lineage => mergeLineage(models, lineage, columnLineage))
        }
      }
    },
    [models, addActiveEdges],
  )

  const isSelectManually = useCallback(
    function isSelectManually(columnName: string): boolean {
      if (manuallySelectedColumn == null) return false

      const [selectedModel, selectedColumn] = manuallySelectedColumn

      if (selectedModel == null || selectedColumn == null) return false

      return selectedModel.name === scope && selectedColumn.name === columnName
    },
    [scope, manuallySelectedColumn],
  )

  const removeEdges = useCallback(
    function removeEdges(
      columnId: string,
      connections: { ins: string[]; outs: string[] },
    ): void {
      console.log('removeEdges', columnId, connections)
      toggleLineageColumn('remove', columnId, connections)
    },
    [toggleLineageColumn],
  )

  return (
    <>
      {isArrayNotEmpty(columnsSelected) && (
        <div
          className={clsx(
            'overflow-hidden overflow-y-auto scrollbar scrollbar--vertical-md',
            'w-full py-2 bg-theme-lighter cursor-default',
            className,
          )}
        >
          {columnsSelected.map(column => (
            <ModelColumn
              key={toNodeOrEdgeId(scope, column.name)}
              id={toNodeOrEdgeId(scope, column.name)}
              scope={scope}
              column={column}
              disabled={disabled}
              handleError={handleError}
              getColumnLineage={getColumnLineage}
              updateColumnLineage={updateColumnLineage}
              removeEdges={removeEdges}
              isActive={true}
              selectManually={
                isSelectManually(column.name)
                  ? setManuallySelectedColumn
                  : undefined
              }
              connections={activeColumns.get(
                toNodeOrEdgeId(scope, column.name),
              )}
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
          'overflow-hidden overflow-y-auto scrollbar scrollbar--vertical-md',
          'w-full py-2 bg-theme-lighter cursor-default',
          className,
        )}
      >
        {columnsRest.map(column => (
          <ModelColumn
            key={toNodeOrEdgeId(scope, column.name)}
            id={toNodeOrEdgeId(scope, column.name)}
            scope={scope}
            column={column}
            disabled={disabled}
            handleError={handleError}
            getColumnLineage={getColumnLineage}
            updateColumnLineage={updateColumnLineage}
            removeEdges={removeEdges}
            isActive={false}
            selectManually={
              isSelectManually(column.name)
                ? setManuallySelectedColumn
                : undefined
            }
            connections={activeColumns.get(toNodeOrEdgeId(scope, column.name))}
            className={clsx(
              filter === '' ||
                (showColumns ? column.name.includes(filter) : true)
                ? 'opacity-100'
                : 'opacity-0 h-0 overflow-hidden',
            )}
          />
        ))}
      </div>
      <Divider className="border-primary-500" />
      {columns.length > limit && (
        <>
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
        </>
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
  const {
    withColumns,
    activeEdges,
    setActiveColumns,
    hasActiveEdge,
    models,
    lineage,
  } = useLineageFlow()

  const [nodes, setNodes] = useState<Node[]>([])
  const [edges, setEdges] = useState<Edge[]>([])
  const [isBuildingLayout, setIsBuildingLayout] = useState(true)

  const nodeTypes = useMemo(() => ({ model: ModelNode }), [])

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

      setActiveColumns(nodesAndEdges.activeColumns)

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
  }, [activeEdges])

  function onNodesChange(changes: NodeChange[]): void {
    setNodes(applyNodeChanges(changes, nodes))
  }

  function onEdgesChange(changes: EdgeChange[]): void {
    setEdges(applyEdgeChanges(changes, edges))
  }

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
          count={columns.length}
          className={clsx(
            'bg-secondary-100 dark:bg-primary-900 py-2',
            withColumns ? 'rounded-t-lg' : 'rounded-lg',
          )}
          hasSource={sourcePosition === Position.Left}
          hasTarget={targetPosition === Position.Right}
          handleClick={isInteractive ? handleClick : undefined}
        />
      </div>
      {withColumns && isArrayNotEmpty(columns) && (
        <>
          <ModelColumns
            className="max-h-[15rem]"
            scope={id}
            columns={columns}
            disabled={model?.type === 'python'}
          />
          <div className="rounded-b-lg bg-secondary-100 dark:bg-primary-900 py-1"></div>
        </>
      )}
    </div>
  )
}

export { ModelColumnLineage, ModelColumn }
