import {
  AlertCircle,
  CircleOff,
  FileCode,
  FileMinus,
  Workflow,
} from 'lucide-react'
import React from 'react'

import { cn } from '@sqlmesh-common/utils'
import { NodeBadge } from '../node/NodeBadge'
import { NodePort } from '../node/NodePort'
import { type NodeId, type PortHandleId, type PortId } from '../utils'
import {
  type ColumnLevelLineageAdjacencyList,
  type ColumnLevelLineageContextHook,
} from './ColumnLevelLineageContext'
import { Tooltip } from '@sqlmesh-common/components/Tooltip/Tooltip'
import { Metadata } from '@sqlmesh-common/components/Metadata/Metadata'
import { HorizontalContainer } from '@sqlmesh-common/components/HorizontalContainer/HorizontalContainer'
import { Information } from '@sqlmesh-common/components/Typography/Information'
import { LoadingContainer } from '@sqlmesh-common/components/LoadingContainer/LoadingContainer'

import './FactoryColumn.css'

export function FactoryColumn<
  TAdjacencyListKey extends string,
  TAdjacencyListColumnKey extends string,
  TNodeID extends string = NodeId,
  TColumnID extends string = PortId,
  TLeftPortHandleId extends string = PortHandleId,
  TRightPortHandleId extends string = PortHandleId,
  TColumnLevelLineageAdjacencyList extends ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  > = ColumnLevelLineageAdjacencyList<
    TAdjacencyListKey,
    TAdjacencyListColumnKey
  >,
>(
  useLineage: ColumnLevelLineageContextHook<
    TAdjacencyListKey,
    TAdjacencyListColumnKey,
    TColumnID,
    TColumnLevelLineageAdjacencyList
  >,
) {
  return React.memo(function FactoryColumn({
    id,
    nodeId,
    modelName,
    name,
    description,
    type,
    className,
    data,
    isFetching = false,
    error,
    renderError,
    renderExpression,
    renderSource,
    onClick,
    onCancel,
  }: {
    id: TColumnID
    nodeId: TNodeID
    modelName: TAdjacencyListKey
    name: TAdjacencyListColumnKey
    type: string
    description?: string | null
    className?: string
    data?: TColumnLevelLineageAdjacencyList
    isFetching?: boolean
    error?: Error | null
    renderError?: (error: Error) => React.ReactNode
    renderExpression?: (expression: string) => React.ReactNode
    renderSource?: (
      source: string,
      expression?: string | null,
    ) => React.ReactNode
    onClick?: () => void
    onCancel?: () => void
  }) {
    const { selectedColumns, adjacencyListColumnLevel, columnLevelLineage } =
      useLineage()

    const column = adjacencyListColumnLevel?.[modelName]?.[name]
    const currentColumnLineage = columnLevelLineage.get(id)
    const isSelectedColumn = selectedColumns.has(id)
    const isTriggeredColumn =
      column != null && currentColumnLineage != null && isSelectedColumn

    // Column that has no upstream connections
    const isSourceColumn = React.useMemo(() => {
      if (data == null) return false

      const models = Object.values(data)

      console.assert(
        data[modelName],
        `Model: ${modelName} not found in column lineage data`,
      )
      console.assert(
        data[modelName][name],
        `Column: ${name} for model: ${modelName} not found in column lineage data`,
      )

      const columns = Object.values(data[modelName])

      if (models.length > 1 || columns.length > 1) return false

      const columnModels = data[modelName][name].models

      return Object.keys(columnModels).length === 0
    }, [data, modelName, name])

    const isDisabledColumn = isSourceColumn && !isSelectedColumn

    function renderColumnStates() {
      if (isFetching) return <></>
      if (error && renderError)
        return (
          <Tooltip
            trigger={
              <AlertCircle
                size={16}
                className="text-lineage-model-column-error-icon"
              />
            }
            side="left"
            sideOffset={20}
            delayDuration={0}
            className="bg-lineage-model-column-error-background p-0"
          >
            {renderError(error)}
          </Tooltip>
        )

      return (
        <>
          {isSourceColumn ? (
            <CircleOff
              size={16}
              className="text-lineage-model-column-icon shrink-0"
            />
          ) : (
            <Workflow
              size={16}
              className={cn(
                'shrink-0',
                data
                  ? 'text-lineage-model-column-icon-active'
                  : 'text-lineage-model-column-icon',
              )}
            />
          )}
          {column?.source && renderSource && (
            <Tooltip
              trigger={
                <FileCode
                  size={16}
                  className="text-lineage-model-column-icon hover:text-lineage-model-column-icon-active"
                />
              }
              side="left"
              sideOffset={20}
              className="p-0 min-w-[30rem] max-w-xl bg-lineage-model-column-source-background"
              delayDuration={0}
            >
              {renderSource(column.source, column.expression)}
            </Tooltip>
          )}
          {column?.expression && renderExpression && (
            <Tooltip
              trigger={
                <FileMinus
                  size={16}
                  className="text-lineage-model-column-icon hover:text-lineage-model-column-icon-active"
                />
              }
              side="left"
              sideOffset={20}
              className="p-0 min-w-[30rem] max-w-xl bg-lineage-model-column-expression-background"
              delayDuration={0}
            >
              {renderExpression(column.expression)}
            </Tooltip>
          )}
        </>
      )
    }

    function renderColumn() {
      return (
        <Metadata
          data-component="FactoryColumn"
          onClick={handleSelectColumn}
          label={
            <LoadingContainer
              isLoading={isFetching}
              message="cancel"
            >
              <HorizontalContainer
                className={cn('gap-2 items-center', isFetching && 'opacity-50')}
              >
                {renderColumnStates()}
                {description ? (
                  <Information
                    className="FactoryColumn__Information"
                    info={description}
                  >
                    <DisplayColumName
                      name={name}
                      className={cn(
                        isTriggeredColumn &&
                          'text-lineage-model-column-active-foreground',
                      )}
                    />
                  </Information>
                ) : (
                  <DisplayColumName
                    name={name}
                    className={cn(
                      isTriggeredColumn &&
                        'text-lineage-model-column-active-foreground',
                    )}
                  />
                )}
              </HorizontalContainer>
            </LoadingContainer>
          }
          value={
            <NodeBadge className="FactoryColumn__NodeBadge">{type}</NodeBadge>
          }
          className={cn(
            'FactoryColumn__Metadata relative overflow-visible',
            isDisabledColumn && 'cursor-not-allowed',
            className,
          )}
        />
      )
    }

    function handleSelectColumn(e: React.MouseEvent<HTMLDivElement>) {
      e.stopPropagation()
      e.preventDefault()

      if (isFetching) {
        onCancel?.()
      } else if ((isSelectedColumn || isSourceColumn) && !isTriggeredColumn) {
        return
      } else {
        onClick?.()
      }
    }

    return isSelectedColumn ? (
      <NodePort<TColumnID, TNodeID, TLeftPortHandleId, TRightPortHandleId>
        id={id}
        nodeId={nodeId}
        className={cn(
          'border-t border-lineage-divider first:border-t-0',
          isTriggeredColumn && 'bg-lineage-model-column-active-background',
        )}
      >
        {renderColumn()}
      </NodePort>
    ) : (
      renderColumn()
    )
  })
}

function DisplayColumName({
  name,
  className,
}: {
  name: string
  className?: string
}) {
  return (
    <span
      title={name}
      className={cn(
        'text-xs font-mono font-semibold w-full truncate',
        className,
      )}
    >
      {name}
    </span>
  )
}
