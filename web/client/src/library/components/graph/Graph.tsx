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
  useUpdateNodeInternals,
  useReactFlow,
  Panel,
  type Edge,
  type Node,
} from 'reactflow'
import { Button } from '../button/Button'
import 'reactflow/dist/base.css'
import {
  getEdges,
  createGraphLayout,
  toNodeOrEdgeId,
  mergeLineageWithColumns,
  mergeConnections,
  getNodeMap,
  getLineageIndex,
  getActiveNodes,
  getUpdatedEdges,
  getUpdatedNodes,
  getModelNodeTypeTitle,
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
import { NoSymbolIcon, ArrowRightCircleIcon } from '@heroicons/react/24/solid'
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
import { Popover, Transition } from '@headlessui/react'
import { CodeEditorDefault } from '@components/editor/EditorCode'
import { EnumFileExtensions } from '@models/file'
import { useSQLMeshModelExtensions } from '@components/editor/hooks'
import { useApiColumnLineage } from '@api/index'
import ModelNode from './ModelNode'
import ListboxShow from '@components/listbox/ListboxShow'

export const EnumLineageNodeModelType = {
  python: 'python',
  sql: 'sql',
  cte: 'cte',
  seed: 'seed',
  external: 'external',
  unknown: 'unknown',
} as const

export type LineageNodeModelType = KeyOf<typeof EnumLineageNodeModelType>

const ModelColumnDisplay = memo(function ModelColumnDisplay({
  columnName,
  columnType,
  columnDescription,
  className,
  source,
  disabled = false,
  withDescription = true,
}: {
  columnName: string
  columnType: string
  columnDescription?: string
  source?: string
  disabled?: boolean
  withDescription?: boolean
  className?: string
}): JSX.Element {
  const { handleClickModel } = useLineageFlow()

  const modelExtensions = useSQLMeshModelExtensions(undefined, model => {
    handleClickModel?.(model.name)
  })

  const [isShowing, setIsShowing] = useState(false)

  return (
    <div className={clsx('flex w-full items-center relative', className)}>
      {isNotNil(source) && (
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
                    className="scrollbar--vertical-md scrollbar--horizontal-md overflow-auto !max-w-[30rem] !h-[25vh] text-xs"
                    extensions={modelExtensions}
                  />
                </Popover.Panel>
              </Transition>
            </>
          )}
        </Popover>
      )}
      <div className="w-full">
        <div className="w-full flex justify-between items-center">
          <span
            title="No column level lineage for Python models"
            className={clsx('flex items-center', disabled && 'opacity-50')}
          >
            {disabled && <NoSymbolIcon className="w-3 h-3 mr-2" />}
            <b>{columnName}</b>
          </span>
          <span className="inline-block text-neutral-400 dark:text-neutral-300 ml-2">
            {columnType}
          </span>
        </div>
        {isNotNil(columnDescription) && withDescription && (
          <p className="block text-neutral-600 dark:text-neutral-300 mt-2">
            {columnDescription}
          </p>
        )}
      </div>
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
  count,
  handleClick,
  handleSelect,
}: {
  id: string
  label: string
  type?: LineageNodeModelType
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
          className="!bg-transparent -ml-2 text-neutral-100 border border-secondary-500 rounded-full overflow-hidden "
        >
          <ArrowRightCircleIcon className="w-5 bg-secondary-500 dark:bg-primary-900 text-secondary-100" />
        </Handle>
      )}
      <div className="w-full flex items-center">
        {isNotNil(handleSelect) && (
          <span
            onClick={handleSelect}
            className={clsx(
              'ml-5 w-4 h-4 rounded-full cursor-pointer p-0.5',
              isSelected
                ? 'border-2 border-secondary-500 dark:border-primary-500'
                : 'border-2 border-neutral-500 dark:border-neutral-200',
            )}
          >
            <span
              className={clsx(
                'flex w-2 h-2 rounded-full',
                isSelected
                  ? 'bg-secondary-500 dark:bg-primary-500'
                  : 'bg-neutral-30',
              )}
            ></span>
          </span>
        )}
        <span
          className={clsx(
            'flex w-full overflow-hidden px-3 py-2',
            isDraggable && 'drag-handle',
          )}
        >
          {isNotNil(type) && (
            <span className="inline-block mr-2 bg-light text-secondary-900 px-2 rounded-[0.25rem] text-[0.5rem]">
              {getModelNodeTypeTitle(type)}
            </span>
          )}
          <span
            title={label}
            className={clsx(
              'inline-block whitespace-nowrap overflow-hidden overflow-ellipsis pr-2',
              isNotNil(handleClick) && 'cursor-pointer hover:underline',
            )}
            onClick={handleClick}
          >
            {label}
          </span>
          <span className="flex justify-between mx-2 px-2 rounded-full bg-neutral-10">
            {count}
          </span>
        </span>
      </div>
      {hasRight && (
        <Handle
          type="source"
          id={toNodeOrEdgeId(EnumSide.Right, id)}
          position={Position.Right}
          isConnectable={false}
          className="!bg-transparent -mr-2 text-neutral-100 border border-secondary-500 rounded-full overflow-hidden"
        >
          <ArrowRightCircleIcon className="w-5 bg-secondary-500 dark:bg-primary-900 text-secondary-100" />
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
  withDescription = true,
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
  withDescription?: boolean
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

        updateColumnLineage(data)
      })
    }
  }

  return (
    <div
      className={clsx(
        isActive
          ? 'bg-secondary-10 dark:bg-primary-900 text-secondary-500 dark:text-neutral-100'
          : 'text-neutral-600 dark:text-neutral-100',
        withHandles ? 'p-0' : 'py-1 px-2 rounded-md mb-1',
        className,
      )}
      onClick={debounceSync(toggleColumnLineage, 500, true)}
    >
      <div
        className={clsx(
          'flex w-full items-center',
          disabled ? 'cursor-not-allowed' : 'cursor-pointer',
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
              columnDescription={column.description}
              disabled={disabled}
              withDescription={withDescription}
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
              columnDescription={column.description}
              disabled={disabled}
              withDescription={withDescription}
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

const ModelColumns = memo(function ModelColumns({
  nodeId,
  columns,
  disabled,
  className,
  limit = 5,
  withHandles = false,
  withSource = false,
  withDescription = true,
}: {
  nodeId: string
  columns: Column[]
  disabled?: boolean
  className?: string
  limit?: number
  withHandles?: boolean
  withSource?: boolean
  withDescription?: boolean
}): JSX.Element {
  const {
    connections,
    isActiveColumn,
    setConnections,
    manuallySelectedColumn,
    setManuallySelectedColumn,
    setLineage,
    removeActiveEdges,
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
  }, [nodeId, columns, showColumns, isActiveColumn])

  const updateColumnLineage = useCallback(
    function updateColumnLineage(
      columnLineage: Record<string, Record<string, LineageColumn>> = {},
    ): void {
      const { connections: newConnections, activeEdges } = mergeConnections(
        structuredClone(connections),
        columnLineage,
      )
      const mergedLineage = mergeLineageWithColumns(
        structuredClone(lineage),
        columnLineage,
      )

      setLineage(mergedLineage)
      setConnections(newConnections)
      addActiveEdges(activeEdges)
    },
    [connections, lineage, addActiveEdges],
  )

  const isSelectManually = useCallback(
    function isSelectManually(columnName: string): boolean {
      if (isNil(manuallySelectedColumn)) return false

      const [selectedModel, selectedColumn] = manuallySelectedColumn

      if (isNil(selectedModel) || isNil(selectedColumn)) return false

      return selectedModel.name === nodeId && selectedColumn.name === columnName
    },
    [nodeId, manuallySelectedColumn],
  )

  const removeEdges = useCallback(
    function removeEdges(columnId: string): void {
      const visited = new Set<string>()

      removeActiveEdges(
        walk(columnId, EnumSide.Left).concat(walk(columnId, EnumSide.Right)),
      )

      function walk(id: string, side: Side): Array<[string, string]> {
        if (visited.has(id)) return []

        const edges = connections.get(id)?.[side] ?? []

        connections.delete(id)

        visited.add(id)

        setConnections(connections)

        return edges
          .map(edge =>
            [
              side === EnumSide.Left
                ? [
                    toNodeOrEdgeId(EnumSide.Left, id),
                    toNodeOrEdgeId(EnumSide.Right, edge),
                  ]
                : [
                    toNodeOrEdgeId(EnumSide.Left, edge),
                    toNodeOrEdgeId(EnumSide.Right, id),
                  ],
            ].concat(walk(edge, side)),
          )
          .flat() as Array<[string, string]>
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
              withDescription={withDescription}
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
          columnsSelected.length > 0 && 'pt-1 border-t border-neutral-10',
          withHandles ? 'w-full bg-theme-lighter cursor-default' : '',
          className,
        )}
      >
        {columnsRest.map((column, idx) => (
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
              'border-t border-neutral-10 first:border-0',
              filter === '' ||
                (showColumns ? column.name.includes(filter) : true)
                ? 'opacity-100'
                : 'opacity-0 h-0 overflow-hidden',
            )}
            withHandles={withHandles}
            withDescription={withDescription}
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
            className="px-1"
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

export { ModelColumnLineage, ModelColumns, ModelNodeHeaderHandles }

function ModelColumnLineage({
  className,
}: {
  className?: string
}): JSX.Element {
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
    activeNodes,
    activeEdges,
    connectedNodes,
    connections,
    highlightedNodes,
    setWithColumns,
    handleError,
    setWithConnected,
    setActiveNodes,
    setWithImpacted,
    setWithSecondary,
    setHasBackground,
  } = useLineageFlow()
  const { setCenter } = useReactFlow()

  const [isBuildingLayout, setIsBuildingLayout] = useState(true)

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
    const WITH_COLUMNS_LIMIT = 30

    if (isArrayEmpty(allEdges) || isNil(mainNode)) return

    if (activeEdges.size > 0) {
      setIsBuildingLayout(true)
    }

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
      withColumns,
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
    setTimeout(() => {
      void createGraphLayout({
        nodesMap,
        nodes: newNodes,
        edges: newEdges,
      })
        .then(layout => {
          setEdges(layout.edges)
          setNodes(layout.nodes)
        })
        .catch(error => {
          handleError?.(error)
        })
        .finally(() => {
          const node = nodesMap[mainNode]

          setActiveNodes(newActiveNodes)

          if (isNotNil(node)) {
            setIsBuildingLayout(false)
            setCenter(node.position.x, node.position.y, {
              zoom: 0.5,
              duration: 0,
            })
          }

          if (
            nodes.length !== newNodes.length &&
            newNodes.length > WITH_COLUMNS_LIMIT
          ) {
            setWithColumns(false)
          }
        })
    }, 100)
  }, [lineageIndex, withColumns, activeEdges, selectedEdges])

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
      withColumns,
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
      (models.get(n.id)?.type === EnumLineageNodeModelType.external ||
        models.get(n.id)?.type === EnumLineageNodeModelType.seed),
  ).length
  const countCTEs = nodes.filter(
    n =>
      isFalse(n.hidden) &&
      models.get(n.id)?.type === EnumLineageNodeModelType.cte,
  ).length

  return (
    <div className={clsx('px-1 w-full h-full relative', className)}>
      {isBuildingLayout && (
        <div className="absolute top-0 left-0 z-[1000] bg-theme flex justify-center items-center w-full h-full">
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
        minZoom={0.1}
        maxZoom={1.5}
        snapGrid={[16, 16]}
        snapToGrid
      >
        <Panel
          position="top-right"
          className="bg-theme !m-0 w-full opacity-90"
        >
          <div className="pl-2 flex items-center text-xs text-neutral-400 pb-2">
            <div className="flex w-full whitespace-nowrap overflow-auto hover:scrollbar scrollbar--horizontal">
              <span className="mr-2">
                <b>Model:</b> {mainNode}
              </span>
              {isNotNil(highlightedNodes) ?? (
                <span className="mr-2">
                  <b>Highlighted:</b>{' '}
                  {Object.keys(highlightedNodes ?? {}).length}
                </span>
              )}
              {countSelected > 0 && (
                <span className="mr-2">
                  <b>Selected:</b> {countSelected}
                </span>
              )}
              {withImpacted && countSelected === 0 && countImpact > 0 && (
                <span className="mr-2">
                  <b>Impact:</b> {countImpact}
                </span>
              )}
              {withSecondary && countSelected === 0 && countSecondary > 0 && (
                <span className="mr-2">
                  <b>Secondary:</b> {countSecondary}
                </span>
              )}
              <span className="mr-2">
                <b>Active:</b> {countActive}
              </span>
              {countVisible > 0 && countVisible !== countActive && (
                <span className="mr-2">
                  <b>Visible:</b> {countVisible}
                </span>
              )}
              {countHidden > 0 && (
                <span className="mr-2">
                  <b>Hidden:</b> {countHidden}
                </span>
              )}
              {countDataSources > 0 && (
                <span className="mr-2">
                  <b>Data Sources</b>: {countDataSources}
                </span>
              )}
              {countCTEs > 0 && (
                <span className="mr-2">
                  <b>CTEs:</b> {countCTEs}
                </span>
              )}
            </div>
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
        </Panel>
        <Controls className="bg-light p-1 rounded-md !border-none !shadow-lg" />
        <Background
          variant={BackgroundVariant.Dots}
          gap={32}
          size={4}
          className={clsx(hasBackground ? 'opacity-100' : 'opacity-0')}
        />
      </ReactFlow>
    </div>
  )
}

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
