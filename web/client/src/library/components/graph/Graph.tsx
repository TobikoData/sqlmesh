import React, {
  type MouseEvent,
  useEffect,
  useMemo,
  useState,
  useCallback,
  memo,
  Fragment,
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
  mergeLineageWithColumns,
  hasNoModels,
  mergeConnections,
} from './help'
import {
  debounceAsync,
  debounceSync,
  isArrayEmpty,
  isArrayNotEmpty,
  isFalse,
  isNil,
  isTrue,
} from '../../../utils'
import { EnumSide, EnumSize, EnumVariant, type Side } from '~/types/enum'
import {
  ArrowRightCircleIcon,
  InformationCircleIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import {
  type Column,
  columnLineageApiLineageModelNameColumnNameGet,
  type ColumnLineageApiLineageModelNameColumnNameGet200,
  type LineageColumn,
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
import { type ErrorIDE } from '~/library/pages/ide/context'
import { Popover, Transition } from '@headlessui/react'
import CodeEditor from '@components/editor/EditorCode'
import { EnumFileExtensions } from '@models/file'
import { useSQLMeshModelExtensions } from '@components/editor/hooks'

const ModelColumnDisplay = memo(function ModelColumnDisplay({
  columnName,
  columnType,
  columnDescription,
  className,
  expression,
  source,
  disabled = false,
}: {
  columnName: string
  columnType: string
  columnDescription?: string
  source?: string
  expression?: string
  disabled?: boolean
  className?: string
}): JSX.Element {
  const { handleClickModel } = useLineageFlow()

  const modelExtensions = useSQLMeshModelExtensions(undefined, model => {
    handleClickModel?.(model.name)
  })

  const [isShowing, setIsShowing] = useState(false)

  return (
    <div className={clsx('flex w-full items-center relative', className)}>
      {source != null && (
        <Popover
          onMouseEnter={() => {
            setIsShowing(true)
          }}
          onMouseLeave={() => {
            setIsShowing(false)
          }}
          onClick={e => {
            e.stopPropagation()
          }}
          className="relative flex"
        >
          {() => (
            <>
              <InformationCircleIcon className="text-secondary-500 dark:text-primary-500 inline-block mr-3 w-4 h-4" />
              <Transition
                show={isShowing}
                as={Fragment}
                enter="transition ease-out duration-200"
                enterFrom="opacity-0 translate-y-1"
                enterTo="opacity-100 translate-y-0"
                leave="transition ease-in duration-150"
                leaveFrom="opacity-100 translate-y-0"
                leaveTo="opacity-0 translate-y-1"
              >
                <Popover.Panel className="fixed bottom-0 left-10 z-10 transform cursor-pointer rounded-lg bg-theme border-4 border-primary-20">
                  <CodeEditor.Default
                    content={source}
                    type={EnumFileExtensions.SQL}
                    className="scrollbar--vertical-md scrollbar--horizontal-md overflow-auto !h-[25vh] !max-w-[30rem]"
                  >
                    {({ extensions, content }) => (
                      <CodeEditor
                        extensions={extensions.concat(modelExtensions)}
                        content={content}
                        className="text-xs pr-2"
                      />
                    )}
                  </CodeEditor.Default>
                </Popover.Panel>
              </Transition>
            </>
          )}
        </Popover>
      )}
      <div
        className={clsx(
          'w-full flex justify-between items-center',
          disabled && 'opacity-50',
        )}
      >
        <span>{columnName}</span>
        <span className="inline-block text-neutral-500 dark:text-neutral-300 ml-4">
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
    // TODO: This is a hack to fix the issue where the handles are not rendered yet
    setTimeout(() => {
      updateNodeInternals(nodeId)
    }, 100)
  }, [hasLeft, hasRight])

  return (
    <div
      className={clsx(
        'flex w-full !relative px-3 py-1 items-center',
        isFalse(disabled) && 'hover:bg-secondary-10 dark:hover:bg-primary-10',
        className,
      )}
    >
      {hasLeft && (
        <Handle
          type="target"
          id={toNodeOrEdgeId(EnumSide.Left, id)}
          position={Position.Left}
          isConnectable={false}
          className={clsx(
            'w-2 h-2 rounded-full !bg-secondary-500 dark:!bg-primary-500',
          )}
        />
      )}
      {children}
      {hasRight && (
        <Handle
          type="source"
          id={toNodeOrEdgeId(EnumSide.Right, id)}
          position={Position.Right}
          isConnectable={false}
          className={clsx(
            'w-2 h-2 rounded-full !bg-secondary-500 dark:!bg-primary-500',
          )}
        />
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
      {hasLeft && (
        <Handle
          type="target"
          id={toNodeOrEdgeId(EnumSide.Left, id)}
          position={Position.Left}
          isConnectable={false}
          className={clsx('!bg-transparent -ml-2 dark:text-primary-500')}
        >
          <ArrowRightCircleIcon className="w-5 bg-theme rounded-full" />
        </Handle>
      )}
      <span className="inline-block w-full">
        {type != null && (
          <span
            title={
              type === 'python'
                ? 'Column lineage disabled for Python models'
                : type === 'cte'
                ? 'CTE'
                : 'SQL Query'
            }
            className="inline-block mr-2 bg-primary-30 px-2 rounded-[0.25rem] text-[0.5rem]"
          >
            {type === 'python' && 'Python'}
            {type === 'sql' && 'SQL'}
            {type === 'seed' && 'Seed'}
            {type === 'cte' && 'CTE'}
            {type === 'external' && 'External'}
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
      {hasRight && (
        <Handle
          type="source"
          id={toNodeOrEdgeId(EnumSide.Right, id)}
          position={Position.Right}
          isConnectable={false}
          className={clsx(
            'w-2 h-2 rounded-full !bg-secondary-500 dark:!bg-primary-500',
          )}
        />
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
  source,
  expression,
}: {
  id: string
  nodeId: string
  column: Column
  disabled?: boolean
  isActive?: boolean
  hasLeft?: boolean
  hasRight?: boolean
  withHandles?: boolean
  source?: string
  expression?: string
  getColumnLineage: (
    columnName: string,
  ) => Promise<
    ColumnLineageApiLineageModelNameColumnNameGet200 & ResponseWithDetail
  >
  updateColumnLineage: (
    lineage: ColumnLineageApiLineageModelNameColumnNameGet200,
  ) => void
  removeEdges: (columnId: string) => void
  handleError?: (error: ErrorIDE) => void
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
          setIsEmpty(hasNoModels(data))
          updateColumnLineage(data)
        })
        .catch(error => {
          setIsError(true)
          handleError?.(error)
        })
        .finally(() => {
          setIsFetching(false)
        })
    }
  }

  return (
    <div
      className={clsx(
        isActive
          ? 'bg-secondary-10 dark:bg-primary-900 text-secondary-500 dark:text-neutral-100'
          : 'text-neutral-500 dark:text-neutral-100',
        withHandles ? 'p-0' : 'py-1 px-2 rounded-md mb-1',
        className,
      )}
      onClick={debounceSync(toggleColumnLineage, 500)}
    >
      <div
        className={clsx(
          'flex w-full items-center',
          disabled ? 'cursor-not-allowed' : 'cursor-pointer',
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
              expression={expression}
              source={source}
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
            source={source}
            expression={expression}
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
  withSource = false,
}: {
  nodeId: string
  columns: Column[]
  disabled?: boolean
  className?: string
  limit?: number
  withHandles?: boolean
  withSource?: boolean
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
    lineage,
    setShouldRecalculate,
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
      columnLineage: Record<string, Record<string, LineageColumn>> = {},
    ): void {
      setShouldRecalculate(
        Object.keys(columnLineage).some(modelName => !models.has(modelName)),
      )
      setLineage(lineage =>
        mergeLineageWithColumns(structuredClone(lineage), columnLineage),
      )
      setConnections(connections =>
        mergeConnections(
          structuredClone(connections),
          columnLineage,
          addActiveEdges,
        ),
      )
    },
    [addActiveEdges, setConnections],
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
      const visited = new Set<string>()

      removeActiveEdges(
        [
          columnId,
          walk(columnId, EnumSide.Left),
          walk(columnId, EnumSide.Right),
        ].flat(),
      )

      function walk(id: string, side: Side): string[] {
        if (visited.has(id)) return []

        const edges = connections.get(id)?.[side] ?? []

        visited.add(id)

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
            'overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical-md',
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
              source={
                withSource
                  ? lineage?.[nodeId]?.columns?.[column.name]?.source
                  : undefined
              }
              expression={
                withSource
                  ? lineage?.[nodeId]?.columns?.[column.name]?.expression
                  : undefined
              }
            />
          ))}
        </div>
      )}
      {columnsRest.length > 20 && (
        <div className="p-1 w-full flex justify-between bg-theme">
          <Input
            className="w-full !m-0"
            size={EnumSize.sm}
          >
            {({ className }) => (
              <Input.Textfield
                className={clsx(className, 'w-full')}
                value={filter}
                placeholder="Filter models"
                onInput={e => {
                  setFilter(e.target.value)
                }}
              />
            )}
          </Input>
        </div>
      )}
      <div
        className={clsx(
          'overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical-md py-2',
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
            source={
              withSource
                ? lineage?.[nodeId]?.columns?.[column.name]?.source
                : undefined
            }
            expression={
              withSource
                ? lineage?.[nodeId]?.columns?.[column.name]?.expression
                : undefined
            }
          />
        ))}
      </div>
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
  const {
    withColumns,
    hasActiveEdge,
    models,
    lineage,
    shouldRecalculate,
    handleError,
  } = useLineageFlow()

  const [nodes, setNodes] = useState<Node[]>([])
  const [edges, setEdges] = useState<Edge[]>([])
  const [isBuildingLayout, setIsBuildingLayout] = useState(true)
  const [isEmpty, setIsEmpty] = useState(true)

  const nodeTypes = useMemo(() => ({ model: ModelNode }), [])

  const nodesAndEdges = useMemo(() => {
    const highlightedNodesDefault = {
      'border-4 border-brand-500': [model.name],
    }

    return getNodesAndEdges({
      lineage,
      nodes: shouldRecalculate ? [] : nodes,
      edges: shouldRecalculate ? [] : edges,
      highlightedNodes: highlightedNodes ?? highlightedNodesDefault,
      models,
      model,
      withColumns,
    })
  }, [lineage, models, model, withColumns, shouldRecalculate])

  useEffect(() => {
    setIsBuildingLayout(isArrayEmpty(nodes) || isArrayEmpty(edges))
    setIsEmpty(isArrayEmpty(nodesAndEdges.nodes))

    if (isArrayEmpty(nodesAndEdges.nodes)) return

    void createGraphLayout(nodesAndEdges)
      .then(layout => {
        toggleEdgeAndNodes(layout.edges, layout.nodes)
        setIsEmpty(isArrayEmpty(layout.nodes))
      })
      .catch(error => {
        handleError?.(error)
      })
      .finally(() => {
        setIsBuildingLayout(false)
      })
  }, [nodesAndEdges])

  useEffect(() => {
    toggleEdgeAndNodes(edges, nodes)
  }, [hasActiveEdge])

  function toggleEdgeAndNodes(edges: Edge[] = [], nodes: Node[] = []): void {
    const visibility = new Map<string, boolean>()

    const newEdges = edges.map(edge => {
      if (edge.sourceHandle == null && edge.targetHandle == null) {
        edge.hidden = false
      } else {
        edge.hidden = isFalse(
          hasActiveEdge(edge.sourceHandle) && hasActiveEdge(edge.targetHandle),
        )
      }

      const isTableSource =
        nodesAndEdges.nodesMap[edge.source]?.data.type === 'cte'
      const isTableTarget =
        nodesAndEdges.nodesMap[edge.target]?.data.type === 'cte'

      if (isTableSource) {
        if (isFalse(visibility.has(edge.source))) {
          visibility.set(edge.source, true)
        }

        visibility.set(
          edge.source,
          isFalse(visibility.has(edge.source)) ? false : edge.hidden,
        )
      }

      if (isTableTarget) {
        if (isFalse(visibility.has(edge.target))) {
          visibility.set(edge.target, true)
        }

        visibility.set(
          edge.target,
          isFalse(visibility.get(edge.target)) ? false : edge.hidden,
        )
      }

      return edge
    })

    setEdges(newEdges)
    setNodes(() =>
      nodes.map(node => {
        if (node.data.type === 'cte') {
          node.hidden = visibility.get(node.id)
        }

        return node
      }),
    )
  }

  function onNodesChange(changes: NodeChange[]): void {
    setNodes(applyNodeChanges(changes, nodes))
  }

  function onEdgesChange(changes: EdgeChange[]): void {
    setEdges(applyEdgeChanges(changes, edges))
  }

  return (
    <div className={clsx('px-2 py-1 w-full h-full', className)}>
      {isBuildingLayout ? (
        <div className="flex justify-center items-center w-full h-full">
          <Loading className="inline-block">
            <Spinner className="w-3 h-3 border border-neutral-10 mr-4" />
            <h3 className="text-md">Building Lineage...</h3>
          </Loading>
        </div>
      ) : isEmpty ? (
        <div className="flex justify-center items-center w-full h-full">
          <Loading className="inline-block">
            <h3 className="text-md">Empty</h3>
          </Loading>
        </div>
      ) : (
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          nodeOrigin={[0.5, 0.5]}
          minZoom={0.1}
          maxZoom={1.5}
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
  const {
    models,
    withColumns,
    handleClickModel,
    lineage = {},
  } = useLineageFlow()

  const { model, columns } = useMemo(() => {
    const model = models.get(id)
    const columns = model?.columns ?? []

    Object.keys(lineage[id]?.columns ?? {}).forEach((column: string) => {
      const found = columns.find(({ name }) => name === column)

      if (isNil(found)) {
        columns.push({ name: column, type: 'UNKNOWN' })
      }
    })

    return {
      model,
      columns,
    }
  }, [id, models, lineage])

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
  const isCTE = data.type === 'cte'
  const showColumns = withColumns && isArrayNotEmpty(columns)
  const type = isCTE ? 'cte' : model?.type

  return (
    <div
      className={clsx(
        'text-xs font-semibold rounded-xl shadow-lg relative z-1',
        highlighted == null ? splat : highlighted,
        isCTE ? 'text-neutral-100' : 'text-secondary-500 dark:text-primary-100',
      )}
    >
      <div className="drag-handle">
        <ModelNodeHeaderHandles
          id={id}
          type={type}
          label={data.label}
          className={clsx(
            'py-2',
            showColumns ? 'rounded-t-lg' : 'rounded-lg',
            isCTE ? 'bg-accent-500' : 'bg-secondary-100 dark:bg-primary-900',
          )}
          hasLeft={targetPosition === Position.Left}
          hasRight={sourcePosition === Position.Right}
          handleClick={isInteractive ? handleClick : undefined}
        />
      </div>
      {showColumns && isArrayNotEmpty(columns) && (
        <>
          <ModelColumns
            className="max-h-[15rem]"
            nodeId={id}
            columns={columns}
            disabled={model?.type === 'python' || data.type !== 'model'}
            withHandles={true}
            withSource={true}
          />
          <div
            className={clsx(
              'rounded-b-lg py-1',
              isCTE ? 'bg-accent-500' : 'bg-secondary-100 dark:bg-primary-900',
            )}
          ></div>
        </>
      )}
    </div>
  )
}

export { ModelColumnLineage, ModelColumns }
