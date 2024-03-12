import React, {
  type MouseEvent,
  useEffect,
  useMemo,
  useState,
  useCallback,
  memo,
  Fragment,
  useRef,
} from 'react'
import { Handle, Position, useUpdateNodeInternals } from 'reactflow'
import { Button } from '../button/Button'
import 'reactflow/dist/base.css'
import {
  mergeLineageWithColumns,
  mergeConnections,
  getModelNodeTypeTitle,
} from './help'
import {
  debounceSync,
  isArrayNotEmpty,
  isFalse,
  isNil,
  isNotNil,
  toID,
  truncate,
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
  type ColumnDescription,
  type Column,
  type ColumnLineageApiLineageModelNameColumnNameGet200,
  type LineageColumn,
  type LineageColumnSource,
  ModelType,
  type LineageColumnExpression,
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
import { type Lineage } from '@context/editor'

export const EnumLineageNodeModelType = {
  ...ModelType,
  cte: 'cte',
  unknown: 'unknown',
} as const

export type LineageNodeModelType = KeyOf<typeof EnumLineageNodeModelType>

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
    <div className={clsx('flex w-full relative items-center', className)}>
      {hasLeft && (
        <Handle
          type="target"
          id={toID(EnumSide.Left, id)}
          position={Position.Left}
          isConnectable={false}
          className="-ml-2 border rounded-full overflow-hidden border-current"
        >
          <ArrowRightCircleIcon className="w-5 text-light dark:text-dark-lighter" />
        </Handle>
      )}
      <div
        className={clsx(
          'w-full flex items-center',
          hasLeft ? 'pl-3' : 'pl-1',
          hasRight ? 'pr-3' : 'pr-1',
        )}
      >
        {isNotNil(handleSelect) && (
          <span
            onClick={handleSelect}
            className="mx-2 w-4 h-4 rounded-full cursor-pointer p-0.5 border-2 border-current"
          >
            <span
              className={clsx(
                'flex w-2 h-2 rounded-full',
                isSelected ? 'bg-current' : 'bg-neutral-10',
              )}
            ></span>
          </span>
        )}
        <span
          className={clsx(
            'flex w-full overflow-hidden py-2',
            isDraggable && 'drag-handle',
          )}
        >
          {isNotNil(type) && (
            <span className="inline-block ml-1 mr-2 px-1 rounded-[0.25rem] text-[0.5rem] bg-neutral-10">
              {getModelNodeTypeTitle(type)}
            </span>
          )}
          <span
            title={decodeURI(label)}
            className={clsx(
              'inline-block whitespace-nowrap overflow-hidden overflow-ellipsis pr-2 font-black',
              isNotNil(handleClick) && 'cursor-pointer hover:underline',
            )}
            onClick={handleClick}
          >
            {truncate(decodeURI(label), 50, 20)}
          </span>
          {isNotNil(count) && (
            <span className="flex justify-between ml-2 mr-1 px-2 rounded-full bg-neutral-10">
              {count}
            </span>
          )}
        </span>
      </div>
      {hasRight && (
        <Handle
          type="source"
          id={toID(EnumSide.Right, id)}
          position={Position.Right}
          isConnectable={false}
          className="-mr-2 border rounded-full overflow-hidden border-current"
        >
          <ArrowRightCircleIcon className="w-5 text-light dark:text-dark-lighter" />
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
  isEmpty = false,
  updateColumnLineage,
  removeEdges,
  selectManually,
  withHandles = false,
  withDescription = true,
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
  isEmpty?: boolean
  withHandles?: boolean
  source?: LineageColumnSource
  expression?: LineageColumnExpression
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
  const {
    refetch: getColumnLineage,
    isFetching,
    isError,
    isTimeout,
  } = useApiColumnLineage(nodeId, column.name)

  useEffect(() => {
    if (isNil(selectManually)) return

    toggleColumnLineage()
    selectManually(undefined)
  }, [selectManually])

  function toggleColumnLineage(): void {
    if (disabled) return

    if (isActive) {
      removeEdges(id)
    } else {
      void getColumnLineage().then(({ data }) =>
        updateColumnLineage(data ?? {}),
      )
    }
  }

  const showHandles = withHandles && (hasLeft || hasRight)

  return (
    <div
      className={clsx(
        'hover:bg-neutral-10',
        isActive && 'bg-neutral-5',
        showHandles ? 'px-0 py-0.5' : 'px-2 py-0.5',
        className,
      )}
      onClick={debounceSync(toggleColumnLineage, 500, true)}
    >
      <div className="flex w-full items-center relative">
        {showHandles ? (
          <ModelNodeHandles
            id={id}
            nodeId={nodeId}
            hasLeft={hasLeft}
            hasRight={hasRight}
            disabled={disabled}
            className="px-2"
          >
            {isNotNil(source) && (
              <ColumnSource
                source={source}
                expression={expression}
              />
            )}
            <ColumnStatus
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
              className={clsx(
                isError
                  ? 'text-danger-500'
                  : isTimeout
                  ? 'text-warning-500'
                  : isEmpty
                  ? 'text-neutral-400 dark:text-neutral-600'
                  : 'text-prose',
              )}
            />
          </ModelNodeHandles>
        ) : (
          <>
            <ColumnStatus
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
              className={clsx(
                isError
                  ? 'text-danger-500'
                  : isTimeout
                  ? 'text-warning-500'
                  : isEmpty
                  ? 'text-neutral-400 dark:text-neutral-600'
                  : 'text-prose',
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
    mainNode,
    connections,
    isActiveColumn,
    setConnections,
    manuallySelectedColumn,
    setManuallySelectedColumn,
    setLineage,
    removeActiveEdges,
    addActiveEdges,
    lineage,
    lineageCache,
    setLineageCache,
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

  function updateColumnLineage(
    columnLineage: Record<string, Record<string, LineageColumn>> = {},
  ): void {
    let newLineageCache = lineageCache
    let currentConnections
    let currentLineage

    if (isNil(lineageCache)) {
      const mainNodeLineage = isNil(mainNode)
        ? undefined
        : lineage[mainNode] ?? lineageCache?.[mainNode]

      newLineageCache = lineage
      currentConnections = new Map()
      currentLineage =
        isNil(mainNode) || isNil(mainNodeLineage)
          ? {}
          : { [mainNode]: { models: [] } }
    } else {
      currentConnections = connections
      currentLineage = structuredClone(lineage)
    }

    const { connections: newConnections, activeEdges } = mergeConnections(
      currentConnections,
      columnLineage,
    )

    if (newConnections.size === 0 && activeEdges.length === 0) {
      currentLineage = structuredClone(lineage)
      newLineageCache = undefined
    } else {
      setConnections(newConnections)
      addActiveEdges(activeEdges)
    }

    const mergedLineage = mergeLineageWithColumns(currentLineage, columnLineage)

    setLineageCache(newLineageCache)
    setLineage(mergedLineage)
  }

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

      if (connections.size === 0 && isNotNil(lineageCache)) {
        setLineage(lineageCache)
        setLineageCache(undefined)
      }

      setConnections(connections)

      function walk(id: string, side: Side): Array<[string, string]> {
        if (visited.has(id)) return []

        const edges = connections.get(id)?.[side] ?? []

        connections.delete(id)

        visited.add(id)

        return edges
          .map(edge =>
            [
              side === EnumSide.Left
                ? [toID(EnumSide.Left, id), toID(EnumSide.Right, edge)]
                : [toID(EnumSide.Left, edge), toID(EnumSide.Right, id)],
            ].concat(walk(edge, side)),
          )
          .flat() as Array<[string, string]>
      }
    },
    [removeActiveEdges, connections],
  )

  return (
    <div className={clsx('overflow-hidden', className)}>
      {isArrayNotEmpty(columnsSelected) && (
        <div
          className={clsx(
            'overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical-md',
            withHandles ? 'w-full bg-theme-lighter cursor-default' : '',
          )}
        >
          {columnsSelected.map(column => (
            <ModelColumn
              key={toID(nodeId, column.name)}
              id={toID(nodeId, column.name)}
              nodeId={nodeId}
              column={column}
              disabled={disabled}
              updateColumnLineage={updateColumnLineage}
              removeEdges={removeEdges}
              isActive={true}
              hasLeft={isArrayNotEmpty(
                connections.get(toID(nodeId, column.name))?.left,
              )}
              hasRight={isArrayNotEmpty(
                connections.get(toID(nodeId, column.name))?.right,
              )}
              selectManually={
                isSelectManually(column.name)
                  ? setManuallySelectedColumn
                  : undefined
              }
              withHandles={withHandles}
              withDescription={withDescription}
              expression={
                withSource
                  ? getColumnFromLineage(lineage, nodeId, column.name)
                      ?.expression
                  : undefined
              }
              source={
                withSource
                  ? getColumnFromLineage(lineage, nodeId, column.name)?.source
                  : undefined
              }
              isEmpty={
                isNotNil(getColumnFromLineage(lineage, nodeId, column.name)) &&
                Object.keys(
                  getColumnFromLineage(lineage, nodeId, column.name)?.models ??
                    {},
                ).length === 0
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
      {columnsRest.length > 0 && (
        <div
          className={clsx(
            'overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical-md pb-1',
            columnsSelected.length > 0 && 'border-t border-neutral-10',
            withHandles ? 'w-full bg-theme-lighter cursor-default' : '',
          )}
        >
          {columnsRest.map(column => (
            <ModelColumn
              key={toID(nodeId, column.name)}
              id={toID(nodeId, column.name)}
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
              expression={
                withSource
                  ? getColumnFromLineage(lineage, nodeId, column.name)
                      ?.expression
                  : undefined
              }
              source={
                withSource
                  ? getColumnFromLineage(lineage, nodeId, column.name)?.source
                  : undefined
              }
              isEmpty={
                isNotNil(getColumnFromLineage(lineage, nodeId, column.name)) &&
                Object.keys(
                  getColumnFromLineage(lineage, nodeId, column.name)?.models ??
                    {},
                ).length === 0
              }
            />
          ))}
        </div>
      )}
      {columns.length > limit && (
        <div className="py-1 flex justify-center bg-theme-lighter">
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
    </div>
  )
})

export { ModelColumns, ModelNodeHeaderHandles }

function ModelNodeHandles({
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
    <div className={clsx('flex w-full items-center', className)}>
      {hasLeft && (
        <Handle
          type="target"
          id={toID(EnumSide.Left, id)}
          position={Position.Left}
          isConnectable={false}
          className="w-2 h-2 rounded-full"
        />
      )}
      {children}
      {hasRight && (
        <Handle
          type="source"
          id={toID(EnumSide.Right, id)}
          position={Position.Right}
          isConnectable={false}
          className="w-2 h-2 rounded-full"
        />
      )}
    </div>
  )
}

function ModelColumnDisplay({
  columnName,
  columnType,
  columnDescription,
  className,
  disabled = false,
  withDescription = true,
}: {
  columnName: string
  columnType: string
  columnDescription?: ColumnDescription
  disabled?: boolean
  withDescription?: boolean
  className?: string
}): JSX.Element {
  return (
    <div
      className={clsx(
        'block w-full',
        disabled ? 'cursor-not-allowed' : 'cursor-pointer',
        className,
      )}
    >
      <div className="w-full flex justify-between items-center">
        <span
          title={decodeURI(columnName)}
          className={clsx('flex items-center', disabled && 'opacity-50')}
        >
          {disabled && (
            <NoSymbolIcon
              title="No column level lineage for Python models"
              className="w-3 h-3 mr-2"
            />
          )}
          {truncate(decodeURI(columnName), 50, 20)}
        </span>
        <span className="inline-block ml-2 text-[0.5rem] font-black opacity-60">
          {columnType}
        </span>
      </div>
      {isNotNil(columnDescription) && withDescription && (
        <p className="block opacity-60 text-[0.65rem]">{columnDescription}</p>
      )}
    </div>
  )
}

function ColumnStatus({
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

function ColumnSource({
  source,
  expression,
}: {
  source: LineageColumnSource
  expression?: LineageColumnExpression
}): JSX.Element {
  const elSourceContainer = useRef<HTMLDivElement>(null)
  const { handleClickModel } = useLineageFlow()

  const modelExtensions = useSQLMeshModelExtensions(undefined, model => {
    handleClickModel?.(model.name)
  })

  const [isShowing, setIsShowing] = useState(false)

  useEffect(() => {
    if (isShowing) {
      scrollToExpression()
    }
  }, [isShowing, expression])

  function scrollToExpression(): void {
    // NOTE: This is a hack to scroll to the expression
    // and should be replaced with a code mirror extension
    setTimeout(() => {
      const lines = Array.from(
        elSourceContainer.current?.querySelector('[role="textbox"].cm-content')
          ?.children ?? [],
      )

      for (const node of lines) {
        if (node.textContent?.trim() === expression) {
          node.scrollIntoView({
            behavior: 'smooth',
            block: 'center',
            inline: 'nearest',
          })
          setTimeout(() => node.classList.add('sqlmesh-expression'), 500)
          return
        }
      }
    }, 300)
  }

  return (
    <Popover
      onMouseLeave={() => {
        setIsShowing(false)
      }}
      onClick={e => {
        e.stopPropagation()
      }}
      className="flex"
    >
      <InformationCircleIcon
        onClick={(e: React.MouseEvent<SVGSVGElement>) => {
          e.stopPropagation()

          setIsShowing(true)
        }}
        className={clsx(
          'inline-block mr-3 w-4 h-4',
          isShowing ? 'text-inherit' : 'text-prose',
        )}
      />
      <Transition
        show={isShowing}
        as={Fragment}
        enter="transition ease-out duration-200"
        enterFrom="opacity-0 -translate-y-[40%]"
        enterTo="opacity-100 -translate-y-[50%]"
        leave="transition ease-in duration-150"
        leaveFrom="opacity-100 -translate-y-[50%]"
        leaveTo="opacity-0 -translate-y-[40%]"
      >
        <Popover.Panel
          ref={elSourceContainer}
          className="fixed -translate-x-[100%] -translate-y-[50%] z-10 content transform cursor-pointer rounded-lg bg-theme border-4 border-neutral-200 dark:border-neutral-600"
        >
          <CodeEditorDefault
            content={source as string}
            type={EnumFileExtensions.SQL}
            className="w-full h-full text-xs rounded-lg scrollbar scrollbar--vertical scrollbar--horizontal overflow-auto max-w-[40rem] h-[25rem]"
            extensions={modelExtensions}
          />
        </Popover.Panel>
      </Transition>
    </Popover>
  )
}

function getColumnFromLineage(
  lineage: Record<string, Lineage>,
  nodeId: string,
  columnName: string,
): Optional<LineageColumn> {
  return lineage?.[nodeId]?.columns?.[encodeURI(columnName)]
}
