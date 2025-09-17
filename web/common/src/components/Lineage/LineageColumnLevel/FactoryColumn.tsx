import {
  AlertCircle,
  CircleOff,
  FileCode,
  FileMinus,
  Workflow,
} from 'lucide-react'
import React from 'react'

import { cn } from '@/utils'
import { NodeBadge } from '../node/NodeBadge'
import { NodePort } from '../node/NodePort'
import {
  type AdjacencyListColumnKey,
  type AdjacencyListKey,
  type NodeId,
  type PortId,
} from '../utils'
import {
  type ColumnLevelLineageAdjacencyList,
  type ColumnLevelLineageContextHook,
} from './ColumnLevelLineageContext'
import { Tooltip } from '@/components/Tooltip/Tooltip'
import { Metadata } from '@/components/Metadata/Metadata'
import { HorizontalContainer } from '@/components/HorizontalContainer/HorizontalContainer'
import { Information } from '@/components/Typography/Information'
import { LoadingContainer } from '@/components/LoadingContainer/LoadingContainer'

export function FactoryColumn(useLineage: ColumnLevelLineageContextHook) {
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
    id: PortId
    nodeId: NodeId
    modelName: AdjacencyListKey
    name: AdjacencyListColumnKey
    type: string
    description?: string | null
    className?: string
    data?: ColumnLevelLineageAdjacencyList
    isFetching?: boolean
    error?: Error | null
    renderError?: (error: Error) => React.ReactNode
    renderExpression?: (expression: string) => React.ReactNode
    renderSource?: (source: string) => React.ReactNode
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
              {renderSource(column.source)}
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
          data-component="ModelColumn"
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
                  <Information info={description}>
                    <DisplayColumName name={name} />
                  </Information>
                ) : (
                  <DisplayColumName name={name} />
                )}
              </HorizontalContainer>
            </LoadingContainer>
          }
          value={<NodeBadge>{type}</NodeBadge>}
          className={cn(
            'relative overflow-visible group p-0',
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
      <NodePort
        id={id}
        nodeId={nodeId}
        className={cn(
          'border-t border-lineage-divider first:border-t-0 px-2',
          isTriggeredColumn && 'bg-lineage-model-column-active',
        )}
      >
        {renderColumn()}
      </NodePort>
    ) : (
      renderColumn()
    )
  })
}

function DisplayColumName({ name }: { name: string }) {
  return (
    <span
      title={name}
      className="text-xs font-mono font-semibold w-full truncate"
    >
      {name}
    </span>
  )
}
