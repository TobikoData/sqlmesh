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
  type EdgeChange,
  applyEdgeChanges,
  applyNodeChanges,
  type NodeChange,
  type Edge,
  type Node,
  useUpdateNodeInternals,
  useReactFlow,
  Panel,
} from 'reactflow'
import { Button } from '../button/Button'
import 'reactflow/dist/base.css'
import {
  getNodesAndEdges,
  createGraphLayout,
  toNodeOrEdgeId,
  mergeLineageWithColumns,
  hasNoModels,
  mergeConnections,
} from './help'
import {
  debounceSync,
  isArrayEmpty,
  isArrayNotEmpty,
  isFalse,
  isNil,
  isNotNil,
} from '../../../utils'
import { EnumSide, EnumSize, EnumVariant, type Side } from '~/types/enum'
import {
  ArrowRightCircleIcon,
  CheckIcon,
  ChevronDownIcon,
} from '@heroicons/react/24/solid'
import {
  InformationCircleIcon,
  ClockIcon,
  ExclamationCircleIcon,
} from '@heroicons/react/24/outline'
import clsx from 'clsx'
import {
  type Column,
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
import Input from '@components/input/Input'
import { Listbox, Popover, Transition } from '@headlessui/react'
import { CodeEditorDefault } from '@components/editor/EditorCode'
import { EnumFileExtensions } from '@models/file'
import { useSQLMeshModelExtensions } from '@components/editor/hooks'
import { useApiColumnLineage } from '@api/index'
import ModelNode from './ModelNode'

const ModelColumnDisplay = memo(function ModelColumnDisplay({
  columnName,
  columnType,
  columnDescription,
  className,
  source,
  disabled = false,
}: {
  columnName: string
  columnType: string
  columnDescription?: string
  source?: string
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
              <InformationCircleIcon
                onClick={(e: React.MouseEvent<SVGSVGElement>) => {
                  e.stopPropagation()

                  setIsShowing(true)
                }}
                className={clsx(
                  'inline-block mr-3 w-4 h-4',
                  isShowing
                    ? 'text-brand-500'
                    : 'text-neutral-400 dark:text-neutral-100',
                )}
              />
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
                  <CodeEditorDefault
                    content={source}
                    type={EnumFileExtensions.SQL}
                    className="scrollbar--vertical-md scrollbar--horizontal-md overflow-auto !h-[25vh] !max-w-[30rem] text-xs pr-2"
                    extensions={modelExtensions}
                  />
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
        <span className="inline-block text-neutral-500 dark:text-neutral-300 ml-2">
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
  isSelected = false,
  isDraggable = false,
  label,
  type,
  handleClick,
  handleSelect,
}: {
  id: string
  label: string
  type?: string
  hasLeft?: boolean
  hasRight?: boolean
  count?: number
  className?: string
  isSelected?: boolean
  isDraggable?: boolean
  handleClick?: (e: MouseEvent) => void
  handleSelect?: (e: MouseEvent) => void
}): JSX.Element {
  return (
    <div className={clsx('flex w-full !relative items-center', className)}>
      {hasLeft && (
        <Handle
          type="target"
          id={toNodeOrEdgeId(EnumSide.Left, id)}
          position={Position.Left}
          isConnectable={false}
          className="!bg-transparent -ml-2 dark:text-primary-500"
        >
          <ArrowRightCircleIcon className="w-5 bg-theme rounded-full" />
        </Handle>
      )}
      {isNotNil(handleSelect) && (
        <span
          onClick={handleSelect}
          className={clsx(
            'mr-3 ml-4 flex justify-center items-center min-w-[1rem] h-4 rounded-full cursor-pointer',
            isSelected
              ? 'border-2 border-success-500'
              : 'border-2 border-neutral-400',
          )}
        >
          <span
            className={clsx(
              'flex w-2 h-2 rounded-full',
              isSelected ? 'bg-success-500' : 'bg-neutral-30',
            )}
          ></span>
        </span>
      )}
      <span
        className={clsx(
          'flex w-full overflow-hidden px-3 py-2 bg-neutral-10',
          isDraggable && 'drag-handle',
        )}
      >
        {type != null && (
          <span
            title={
              type === 'python'
                ? 'Column lineage disabled for Python models'
                : type === 'cte'
                  ? 'CTE'
                  : 'SQL Query'
            }
            className="inline-block mr-2 bg-light text-secondary-900 px-2 rounded-[0.25rem] text-[0.5rem]"
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
            'inline-block whitespace-nowrap overflow-hidden overflow-ellipsis',
            isNotNil(handleClick) && 'cursor-pointer hover:underline',
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
          className="!bg-transparent -mr-2 dark:text-primary-500"
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
  updateColumnLineage,
  removeEdges,
  selectManually,
  withHandles = false,
  source,
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
  updateColumnLineage: (
    lineage: ColumnLineageApiLineageModelNameColumnNameGet200,
  ) => void
  removeEdges: (columnId: string) => void
  selectManually?: React.Dispatch<
    React.SetStateAction<
      [ModelSQLMeshModel<InitialSQLMeshModel>, Column] | undefined
    >
  >
  className?: string
}): JSX.Element {
  const [isEmpty, setIsEmpty] = useState(false)

  const {
    refetch: getColumnLineage,
    isFetching,
    isError,
    isTimeout,
  } = useApiColumnLineage(nodeId, column.name)

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
      setIsEmpty(false)

      void getColumnLineage().then(({ data }) => {
        if (isNil(data)) return

        setIsEmpty(hasNoModels(data))
        updateColumnLineage(data)
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
        {withHandles ? (
          <ModelNodeHandles
            id={id}
            nodeId={nodeId}
            hasLeft={hasLeft}
            hasRight={hasRight}
            disabled={disabled}
          >
            <ColumnLoading
              isFetching={isFetching}
              isError={isError}
              isTimeout={isTimeout}
            />
            <ModelColumnDisplay
              columnName={column.name}
              columnType={column.type}
              disabled={disabled}
              source={source}
              className={clsx(
                isError && 'text-danger-500',
                isTimeout && 'text-warning-500',
                isEmpty && 'text-neutral-400 dark:text-neutral-600',
              )}
            />
          </ModelNodeHandles>
        ) : (
          <>
            <ColumnLoading
              isFetching={isFetching}
              isError={isError}
              isTimeout={isTimeout}
            />
            <ModelColumnDisplay
              columnName={column.name}
              columnType={column.type}
              disabled={disabled}
              source={source}
              className={clsx(
                isError && 'text-danger-500',
                isTimeout && 'text-warning-500',
                isEmpty && 'text-neutral-400 dark:text-neutral-600',
              )}
            />
          </>
        )}
      </div>
    </div>
  )
})

function ColumnLoading({
  isFetching = false,
  isError = false,
  isTimeout = false,
}: {
  isFetching: boolean
  isError: boolean
  isTimeout: boolean
}): JSX.Element {
  return (
    <>
      {isFetching && (
        <Loading className="inline-block mr-1">
          <Spinner className="w-3 h-3 border border-neutral-10" />
        </Loading>
      )}
      {isTimeout && isFalse(isFetching) && (
        <ClockIcon className="w-4 h-4 text-warning-500 mr-1" />
      )}
      {isError && isFalse(isFetching) && (
        <ExclamationCircleIcon className="w-4 h-4 text-danger-500 mr-1" />
      )}
    </>
  )
}

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
  const {
    connections,
    isActiveColumn,
    setConnections,
    manuallySelectedColumn,
    setManuallySelectedColumn,
    setLineage,
    removeActiveEdges,
    hasActiveEdge,
    addActiveEdges,
    lineage,
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

  const updateColumnLineage = useCallback(
    function updateColumnLineage(
      columnLineage: Record<string, Record<string, LineageColumn>> = {},
    ): void {
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
              : `Show ${columns.length - columnsSelected.length - columnsRest.length
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
    setWithColumns,
    hasActiveEdge,
    models,
    lineage,
    handleError,
    setMainNode,
    mainNode,
    selectedEdges,
    nodes,
    setNodes,
    edges,
    setEdges,
    withConnected,
    connectedNodes,
    setWithConnected,
  } = useLineageFlow()
  const { setCenter } = useReactFlow()

  const [isEmpty, setIsEmpty] = useState(true)
  const [isBuildingLayout, setIsBuildingLayout] = useState(true)
  const [hasBackground, setHasBackground] = useState(true)

  const nodeTypes = useMemo(() => ({ model: ModelNode }), [])

  const nodesAndEdges = useMemo(() => {
    const highlightedNodesDefault = {
      'border-4 border-brand-500': [model.name],
    }

    return getNodesAndEdges({
      lineage,
      highlightedNodes: highlightedNodes ?? highlightedNodesDefault,
      models,
      model,
      withColumns,
    })
  }, [lineage, models, model, withColumns, highlightedNodes])

  const toggleEdgeAndNodes = useCallback(
    function toggleEdgeAndNodes(
      edges: Edge[] = [],
      nodes: Node[] = [],
    ): {
      edges: Edge[]
      nodes: Node[]
    } {
      const visibility = new Map<string, boolean>()

      return {
        edges: edges.map(edge => {
          if (isNil(edge.sourceHandle) && isNil(edge.targetHandle)) {
            // Edge between models
            edge.hidden = false
          } else {
            // Edge between columns
            edge.hidden = isFalse(
              hasActiveEdge(edge.sourceHandle) &&
              hasActiveEdge(edge.targetHandle),
            )
          }

          let stroke = 'var(--color-graph-edge-main)'
          let strokeWidth = 2

          const isConnectedSource = connectedNodes.has(edge.source)
          const isConnectedTarget = connectedNodes.has(edge.target)

          if (
            selectedEdges.has(edge.id) ||
            (withConnected && isConnectedSource && isConnectedTarget)
          ) {
            strokeWidth = 5
            stroke = 'var(--color-graph-edge-selected)'
            edge.zIndex = 10
          } else {
            if (hasActiveEdge(edge.sourceHandle)) {
              stroke = 'var(--color-graph-edge-secondary)'
            } else if (hasActiveEdge(edge.targetHandle)) {
              stroke = 'var(--color-graph-edge-secondary)'
            } else if (isConnectedSource && isConnectedTarget) {
              strokeWidth = 2
              stroke = 'var(--color-graph-edge-direct)'
              edge.zIndex = 10
            }
          }

          edge.style = {
            ...edge.style,
            stroke,
            strokeWidth,
          }

          const isSourceTypeCTE =
            nodesAndEdges.nodesMap[edge.source]?.data.type === 'cte'
          const isTargetTypeCTE =
            nodesAndEdges.nodesMap[edge.target]?.data.type === 'cte'

          if (isSourceTypeCTE) {
            if (isFalse(visibility.has(edge.source))) {
              visibility.set(edge.source, true)
            }

            visibility.set(
              edge.source,
              isFalse(visibility.has(edge.source)) ? false : edge.hidden,
            )
          }

          if (isTargetTypeCTE) {
            if (isFalse(visibility.has(edge.target))) {
              visibility.set(edge.target, true)
            }

            visibility.set(
              edge.target,
              isFalse(visibility.get(edge.target)) ? false : edge.hidden,
            )
          }

          return edge
        }),
        nodes: nodes.map(node => {
          node.hidden = node.data.type === 'cte' && visibility.get(node.id)

          return node
        }),
      }
    },
    [nodesAndEdges, selectedEdges, mainNode, withConnected],
  )

  useEffect(() => {
    setMainNode(model.name)
  }, [model.name])

  useEffect(() => {
    setIsEmpty(isArrayEmpty(nodesAndEdges.nodes))

    if (isArrayEmpty(nodesAndEdges.nodes)) return

    void createGraphLayout(nodesAndEdges)
      .then(layout => {
        const { edges, nodes } = toggleEdgeAndNodes(layout.edges, layout.nodes)

        setIsEmpty(isArrayEmpty(layout.nodes))
        setEdges(edges)
        setNodes(nodes)
      })
      .catch(error => {
        handleError?.(error)
      })
      .finally(() => {
        const node = nodesAndEdges.nodesMap[model.name]

        setIsBuildingLayout(false)

        if (isNotNil(node)) {
          setCenter(node.position.x, node.position.y, {
            zoom: 0.5,
            duration: 1000,
          })
        }
      })
  }, [nodesAndEdges])

  useEffect(() => {
    const { edges: newEdges, nodes: newNodes } = toggleEdgeAndNodes(
      edges,
      nodes,
    )

    setEdges(newEdges)
    setNodes(newNodes)
  }, [hasActiveEdge, toggleEdgeAndNodes])

  function onNodesChange(changes: NodeChange[]): void {
    setNodes(applyNodeChanges(changes, nodes))
  }

  function onEdgesChange(changes: EdgeChange[]): void {
    setEdges(applyEdgeChanges(changes, edges))
  }

  return (
    <div className={clsx('px-1 w-full h-full relative', className)}>
      {isEmpty && isFalse(isBuildingLayout) ? (
        <div className="flex justify-center items-center w-full h-full">
          <Loading className="inline-block">
            <h3 className="text-md">Empty</h3>
          </Loading>
        </div>
      ) : (
        <>
          {isBuildingLayout && (
            <div className="absolute top-0 left-0 z-[1000] bg-theme flex justify-center items-center w-full h-full">
              <Loading className="inline-block">
                <Spinner className="w-3 h-3 border border-neutral-10 mr-4" />
                <h3 className="text-md">Building Lineage...</h3>
              </Loading>
            </div>
          )}
          <div className="flex justify-end"></div>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            nodeOrigin={[0.5, 0.5]}
            minZoom={0.1}
            maxZoom={1.5}
            snapGrid={[16, 16]}
            snapToGrid
          >
            <Panel
              position="top-right"
              className="!mx-0 !my-1"
            >
              <GraphOptions
                options={{
                  Background: setHasBackground,
                  Columns: setWithColumns,
                  Connected: setWithConnected,
                }}
                value={
                  [
                    withColumns && 'Columns',
                    hasBackground && 'Background',
                    withConnected && 'Connected',
                  ].filter(Boolean) as string[]
                }
              />
            </Panel>
            <Controls className="bg-light p-1 rounded-md !border-none !shadow-lg" />
            <Background
              variant={BackgroundVariant.Dots}
              gap={32}
              size={4}
              className={clsx(hasBackground ? 'opacity-100' : 'opacity-0')}
            />
          </ReactFlow>
        </>
      )}
    </div>
  )
}

export { ModelColumnLineage, ModelColumns, ModelNodeHeaderHandles }

function GraphOptions({
  options,
  value = [],
}: {
  options: Record<string, React.Dispatch<React.SetStateAction<boolean>>>
  value: string[]
}): JSX.Element {
  const [selected, setSelected] = useState(value)

  return (
    <Listbox
      value={selected}
      onChange={value => {
        setSelected(value)

        for (const key in options) {
          options[key]?.(value.includes(key))
        }
      }}
      multiple
    >
      <div className="relative m-1 flex">
        <Listbox.Button className="flex items-center relative w-full cursor-default bg-primary-10 text-xs rounded-full text-primary-500 py-1 px-2 text-center focus:outline-none focus-visible:border-accent-500 focus-visible:ring-2 focus-visible:ring-light focus-visible:ring-opacity-75 focus-visible:ring-offset-2 focus-visible:ring-offset-brand-300">
          <span className="block truncate">Show</span>
          <ChevronDownIcon
            className="ml-2 h-4 w-4"
            aria-hidden="true"
          />
        </Listbox.Button>
        <Transition
          as={Fragment}
          leave="transition ease-in duration-100"
          leaveFrom="opacity-100"
          leaveTo="opacity-0"
        >
          <Listbox.Options className="absolute top-8 right-0 z-50 max-h-60 min-w-16 overflow-auto rounded-md bg-theme py-2 shadow-lg ring-2 ring-primary-10 ring-opacity-5 focus:outline-none sm:text-sm">
            {Object.keys(options).map(key => (
              <Listbox.Option
                key={key}
                className={({ active }) =>
                  `relative cursor-default select-none py-1 pl-10 pr-4 ${active
                    ? 'bg-primary-10 text-primary-500'
                    : 'text-neutral-700 dark:text-neutral-300'
                  }`
                }
                value={key}
              >
                {({ selected }) => (
                  <>
                    <span>{key}</span>
                    <span
                      className={clsx(
                        'absolute inset-y-0 left-0 flex items-center pl-3 text-primary-500',
                        selected ? 'block' : 'hidden',
                      )}
                    >
                      <CheckIcon
                        className="h-4 w-4"
                        aria-hidden="true"
                      />
                    </span>
                  </>
                )}
              </Listbox.Option>
            ))}
          </Listbox.Options>
        </Transition>
      </div>
    </Listbox>
  )
}
