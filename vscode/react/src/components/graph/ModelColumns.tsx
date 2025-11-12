import React, { useEffect, useMemo, useCallback } from 'react'
import { Handle, Position, useUpdateNodeInternals } from 'reactflow'
import 'reactflow/dist/base.css'
import { mergeLineageWithColumns, mergeConnections } from './help'
import {
  debounceSync,
  isArrayNotEmpty,
  isFalse,
  isNil,
  isNotNil,
  truncate,
} from '@/utils/index'
import { toID, type PartialColumnHandleId, type Side } from './types'
import { NoSymbolIcon } from '@heroicons/react/24/solid'
import { ClockIcon, ExclamationCircleIcon } from '@heroicons/react/24/outline'
import clsx from 'clsx'
import {
  type ColumnDescription,
  type ColumnLineageApiLineageModelNameColumnNameGet200,
  type LineageColumn,
} from '@/api/client'
import Loading from '@/components/loading/Loading'
import Spinner from '@/components/logo/Spinner'
import './Graph.css'
import {
  type InitialSQLMeshModel,
  type ModelSQLMeshModel,
} from '@/domain/sqlmesh-model'
import { useLineageFlow } from './context'
import { useApiColumnLineage } from '@/api/index'
import SourceList from '@/components/sourceList/SourceList'
import type { Lineage } from '@/domain/lineage'
import type { Column, ColumnName } from '@/domain/column'
import type { ModelEncodedFQN } from '@/domain/models'

export function ModelColumns({
  nodeId,
  columns,
  disabled,
  className,
  limit,
  withHandles = false,
  withDescription = true,
  maxHeight = '50vh',
}: {
  nodeId: ModelEncodedFQN
  columns: Column[]
  disabled?: boolean
  className?: string
  limit: number
  withHandles?: boolean
  withDescription?: boolean
  maxHeight?: string
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

  const [columnsSelected = [], columnsRest = []] = useMemo(() => {
    const active: Column[] = []
    const rest: Column[] = []

    columns.forEach(column => {
      if (isActiveColumn(nodeId, column.name)) {
        active.push(column)
      } else {
        rest.push(column)
      }
    })

    return [active, rest]
  }, [nodeId, columns, isActiveColumn])

  function updateColumnLineage(
    columnLineage: Record<string, Record<string, LineageColumn>> = {},
  ): void {
    let newLineageCache = lineageCache
    let currentConnections
    let currentLineage

    if (isNil(lineageCache)) {
      const mainNodeLineage = isNil(mainNode)
        ? undefined
        : (lineage[mainNode] ?? lineageCache?.[mainNode])

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
    function isSelectManually(columnName: ColumnName): boolean {
      if (isNil(manuallySelectedColumn)) return false

      const [selectedModel, selectedColumn] = manuallySelectedColumn

      if (isNil(selectedModel) || isNil(selectedColumn)) return false

      return selectedModel.fqn === nodeId && selectedColumn.name === columnName
    },
    [nodeId, manuallySelectedColumn],
  )

  const removeEdges = useCallback(
    function removeEdges(columnId: PartialColumnHandleId): void {
      const visited = new Set<string>()

      removeActiveEdges(walk(columnId, 'left').concat(walk(columnId, 'right')))

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
              side === 'left'
                ? [toID('left', id), toID('right', edge)]
                : [toID('left', edge), toID('right', id)],
            ].concat(walk(edge, side)),
          )
          .flat() as Array<[PartialColumnHandleId, PartialColumnHandleId]>
      }
    },
    [removeActiveEdges, connections],
  )

  return (
    <div className={clsx('overflow-hidden', className)}>
      {isArrayNotEmpty(columnsSelected) && (
        <div
          className={clsx(
            'w-full h-full overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical border-b border-t border-neutral-10',
            withHandles && 'w-full cursor-default',
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
              isEmpty={
                isNotNil(getColumnFromLineage(lineage, nodeId, column.name)) &&
                Object.keys(
                  getColumnFromLineage(lineage, nodeId, column.name)?.models ??
                    {},
                ).length === 0
              }
              className="border-t border-neutral-10 first:border-0"
            />
          ))}
        </div>
      )}
      {columnsRest.length <= limit && (
        <div
          className={clsx(
            'w-full h-full overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical py-2',
            withHandles && 'w-full cursor-default',
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
              withHandles={withHandles}
              withDescription={withDescription}
              isEmpty={
                isNotNil(getColumnFromLineage(lineage, nodeId, column.name)) &&
                Object.keys(
                  getColumnFromLineage(lineage, nodeId, column.name)?.models ??
                    {},
                ).length === 0
              }
              className="border-t border-neutral-10 first:border-0 rounded-md"
            />
          ))}
        </div>
      )}
      {columnsRest.length > limit && (
        <div
          className={clsx(
            'w-full h-full overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical pb-2',
            withHandles && 'w-full cursor-default',
          )}
          style={{ height: maxHeight }}
        >
          <SourceList<Column>
            keyId="name"
            keyName="name"
            items={columnsRest}
            withCounter={false}
            withFilter={columnsRest.length > limit}
            disabled={disabled}
            listItem={({ disabled, item }) => (
              <ModelColumn
                key={toID(nodeId, item.name)}
                id={toID(nodeId, item.name)}
                nodeId={nodeId}
                column={item}
                disabled={disabled}
                updateColumnLineage={updateColumnLineage}
                removeEdges={removeEdges}
                isActive={false}
                hasLeft={false}
                hasRight={false}
                selectManually={
                  isSelectManually(item.name)
                    ? setManuallySelectedColumn
                    : undefined
                }
                withHandles={withHandles}
                withDescription={withDescription}
                isEmpty={
                  isNotNil(getColumnFromLineage(lineage, nodeId, item.name)) &&
                  Object.keys(
                    getColumnFromLineage(lineage, nodeId, item.name)?.models ??
                      {},
                  ).length === 0
                }
                className="border-t border-neutral-10 first:border-0 rounded-md"
              />
            )}
          />
        </div>
      )}
    </div>
  )
}

function ModelColumn({
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
}: {
  id: PartialColumnHandleId
  nodeId: ModelEncodedFQN
  column: Column
  disabled?: boolean
  isActive?: boolean
  hasLeft?: boolean
  hasRight?: boolean
  isEmpty?: boolean
  withHandles?: boolean
  withDescription?: boolean
  updateColumnLineage: (
    lineage: ColumnLineageApiLineageModelNameColumnNameGet200,
  ) => void
  removeEdges: (columnId: PartialColumnHandleId) => void
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
  } = useApiColumnLineage(nodeId, column.name, { models_only: true })

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
        'transition-colors duration-200 cursor-pointer',
        disabled ? 'cursor-not-allowed' : 'hover:bg-neutral-5',
        isActive && 'bg-neutral-5',
        showHandles ? 'px-0 py-0.5' : 'px-2 py-0.5',
        className,
      )}
      onClick={debounceSync(toggleColumnLineage, 500, true)}
    >
      <div className="flex w-full items-center relative">
        {showHandles ? (
          <ColumnHandles
            id={id}
            nodeId={nodeId}
            hasLeft={hasLeft}
            hasRight={hasRight}
            disabled={disabled}
            className="px-2"
          >
            <ColumnStatus
              isFetching={isFetching}
              isError={isError}
              isTimeout={false}
            />
            <ColumnDisplay
              columnName={column.name}
              columnType={column.type}
              columnDescription={column.description}
              disabled={disabled}
              withDescription={withDescription}
              className={clsx(
                isError
                  ? 'text-danger-500'
                  : false
                    ? 'text-warning-500'
                    : isEmpty
                      ? 'text-neutral-400 dark:text-neutral-600'
                      : 'text-prose',
              )}
            />
          </ColumnHandles>
        ) : (
          <>
            <ColumnStatus
              isFetching={isFetching}
              isError={isError}
              isTimeout={false}
            />
            <ColumnDisplay
              columnName={column.name}
              columnType={column.type}
              columnDescription={column.description}
              disabled={disabled}
              withDescription={withDescription}
              className={clsx(
                isError
                  ? 'text-danger-500'
                  : false
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
}

function ColumnHandles({
  nodeId,
  id,
  hasLeft = false,
  hasRight = false,
  disabled = false,
  children,
  className,
}: {
  nodeId: ModelEncodedFQN
  id: PartialColumnHandleId
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
        'flex w-full items-center',
        disabled ? 'cursor-not-allowed' : 'inherit',
        className,
      )}
    >
      {hasLeft && (
        <Handle
          type="target"
          id={toID('left', id)}
          position={Position.Left}
          isConnectable={false}
          className="w-2 h-2 rounded-full"
        />
      )}
      {children}
      {hasRight && (
        <Handle
          type="source"
          id={toID('right', id)}
          position={Position.Right}
          isConnectable={false}
          className="w-2 h-2 rounded-full"
        />
      )}
    </div>
  )
}

function ColumnDisplay({
  columnName,
  columnType,
  columnDescription,
  className,
  disabled = false,
  withDescription = true,
}: {
  columnName: ColumnName
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
        disabled ? 'cursor-not-allowed' : 'inherit',
        className,
      )}
    >
      <div className="w-full flex justify-between items-center">
        <span
          title={columnName}
          className={clsx('flex items-center', disabled && 'opacity-50')}
        >
          {disabled && (
            <NoSymbolIcon
              title="No column level lineage for Python models"
              className="w-3 h-3 mr-2"
            />
          )}
          {truncate(columnName, 50, 20)}
        </span>
        <span
          title={columnType}
          className="inline-block ml-2 text-[0.6rem] font-black opacity-60"
        >
          {truncate(columnType, 20, 10)}
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

function getColumnFromLineage(
  lineage: Record<string, Lineage>,
  nodeId: string,
  columnName: string,
): LineageColumn | undefined {
  return lineage?.[nodeId]?.columns?.[columnName as ColumnName]
}
