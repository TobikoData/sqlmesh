import { type Node, type NodeProps } from '@xyflow/react'
import cronstrue from 'cronstrue'
import React from 'react'

import { cn } from '@/utils'
import { HorizontalContainer } from '../../HorizontalContainer/HorizontalContainer'
import { VerticalContainer } from '../../VerticalContainer/VerticalContainer'
import {
  MAX_COLUMNS_TO_DISPLAY,
  calculateColumnsHeight,
  calculateNodeColumnsCount,
  calculateSelectedColumnsHeight,
} from '../LineageColumnLevel/help'
import { useColumns, type Column } from '../LineageColumnLevel/useColumns'
import { calculateNodeBaseHeight } from '../help'
import { NodeAppendix } from '../node/NodeAppendix'
import { NodeBadge } from '../node/NodeBadge'
import { NodeBase } from '../node/NodeBase'
import { NodeContainer } from '../node/NodeContainer'
import { NodeDivider } from '../node/NodeDivider'
import { NodeHandleIcon } from '../node/NodeHandleIcon'
import { NodeHandles } from '../node/NodeHandles'
import { NodeHeader } from '../node/NodeHeader'
import { NodePorts } from '../node/NodePorts'
import { useNodeMetadata } from '../node/useNodeMetadata'
import { type NodeId, ZOOM_TRESHOLD } from '../utils'
import {
  type NodeData,
  type NodeType,
  useModelLineage,
} from './ModelLineageContext'
import { ModelNodeColumn } from './ModelNodeColumn'
import {
  getNodeTypeBorderColor,
  getNodeTypeColor,
  getNodeTypeTextColor,
} from './help'
import { Tooltip } from '@/components/Tooltip/Tooltip'
import type { ColumnLevelLineageAdjacencyList } from '../LineageColumnLevel/ColumnLevelLineageContext'
import { ModelName } from '@/components/ModelName/ModelName'
import { Metadata } from '@/components/Metadata/Metadata'

export const ModelNode = React.memo(function ModelNode({
  id,
  data,
  ...props
}: NodeProps<Node<NodeData>>) {
  const {
    selectedColumns,
    zoom,
    currentNode,
    selectedNodeId,
    selectedNodes,
    showColumns,
    fetchingColumns,
    setSelectedNodeId,
  } = useModelLineage()

  const [showNodeColumns, setShowNodeColumns] = React.useState(showColumns)
  const [isHovered, setIsHovered] = React.useState(false)

  const nodeId = id as NodeId

  const {
    leftId,
    rightId,
    isSelected, // if selected from inside the lineage and node is selcted
    isActive, // if selected from inside the lineage and node is not selected but in path
  } = useNodeMetadata(nodeId, currentNode, selectedNodeId, selectedNodes)

  const {
    columns,
    selectedColumns: modelSelectedColumns,
    columnNames,
  } = useColumns(selectedColumns, data.name, data.columns)

  const hasSelectedColumns = selectedColumns.intersection(columnNames).size > 0
  const hasFetchingColumns = fetchingColumns.intersection(columnNames).size > 0

  React.useEffect(() => {
    setShowNodeColumns(showColumns || isSelected)
  }, [columnNames, isSelected, showColumns])

  function toggleSelectedNode() {
    setSelectedNodeId(prev => (prev === nodeId ? null : nodeId))
  }

  const shouldShowColumns =
    showNodeColumns || hasSelectedColumns || hasFetchingColumns || isHovered
  const modelType = data.model_type.toLowerCase() as NodeType
  const hasColumnsFilter =
    shouldShowColumns && columns.length > MAX_COLUMNS_TO_DISPLAY

  const baseNodeHeight = calculateNodeBaseHeight({
    nodeOptionsCount: 0,
  })
  const selectedColumnsHeight = calculateSelectedColumnsHeight(
    modelSelectedColumns.length,
  )
  const columnsHeight = calculateColumnsHeight({
    columnsCount: shouldShowColumns
      ? calculateNodeColumnsCount(columns.length)
      : 0,
    hasColumnsFilter,
  })

  const nodeHeight = baseNodeHeight + selectedColumnsHeight + columnsHeight

  return (
    <NodeContainer
      className={cn(
        'hover:opacity-100 group',
        selectedNodeId == null || isActive || isSelected
          ? 'opacity-100'
          : 'opacity-10',
      )}
      style={{
        height: `${nodeHeight}px`,
      }}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <NodeAppendix
        position="top"
        className="bg-lineage-node-appendix-background"
      >
        <HorizontalContainer className="gap-1 items-center h-5">
          {zoom > ZOOM_TRESHOLD && (
            <>
              <NodeBadge>{data.kind.toUpperCase()}</NodeBadge>
              <Tooltip
                side="top"
                sideOffset={6}
                trigger={
                  <NodeBadge className="cursor-default whitespace-nowrap">
                    {data.cron.toUpperCase()}
                  </NodeBadge>
                }
                className="text-xs p-2 rounded-md font-semibold"
              >
                <span className="flex gap-2">
                  <NodeBadge size="2xs">UTC Time</NodeBadge>
                  {cronstrue.toString(data.cron, {
                    dayOfWeekStartIndexZero: true,
                    use24HourTimeFormat: true,
                    verbose: true,
                  })}
                </span>
              </Tooltip>
            </>
          )}
        </HorizontalContainer>
      </NodeAppendix>
      <NodeBase
        data-component="ModelNodeBase"
        id={nodeId}
        data={data}
        {...props}
        className={cn(
          'ring-offset-2 z-10',
          isSelected
            ? 'ring-2 ring-lineage-node-selected-border'
            : 'hover:ring-2 hover:ring-lineage-node-border-hover',
          getNodeTypeBorderColor(modelType),
        )}
      >
        <NodeHeader
          className="shrink-0 h-7"
          onClick={toggleSelectedNode}
        >
          <NodeHandles
            leftId={leftId}
            rightId={rightId}
            leftIcon={
              <NodeHandleIcon
                className={cn(
                  'ml-0.5',
                  getNodeTypeTextColor(modelType),
                  getNodeTypeBorderColor(modelType),
                )}
              />
            }
            rightIcon={
              <NodeHandleIcon
                className={cn(
                  'mr-0.5',
                  getNodeTypeTextColor(modelType),
                  getNodeTypeBorderColor(modelType),
                )}
              />
            }
            handleClassName="top-4"
          >
            <HorizontalContainer
              className={cn(
                'items-center px-1 w-auto shrink-0',
                leftId ? 'pl-2' : 'pl-1',
                getNodeTypeColor(modelType),
              )}
            >
              <NodeBadge
                size="2xs"
                className="bg-[transparent] text-[white]"
              >
                {modelType.toUpperCase()}
              </NodeBadge>
            </HorizontalContainer>
            <HorizontalContainer
              className={cn(
                'gap-2 items-center px-2',
                rightId ? 'pr-3' : 'pr-2',
                getNodeTypeBorderColor(modelType),
              )}
            >
              <ModelName
                hideCatalog
                showTooltip={zoom > ZOOM_TRESHOLD}
                hideIcon
                name={data.displayName}
                grayscale
                showCopy
                className="w-full text-xs overflow-hidden cursor-default truncate"
              />
            </HorizontalContainer>
          </NodeHandles>
        </NodeHeader>
        {shouldShowColumns && (
          <VerticalContainer className="border-t border-lineage-node-border">
            <VerticalContainer className="h-auto shrink-0">
              {modelSelectedColumns.map(column => (
                <ModelNodeColumn
                  key={column.id}
                  id={column.id}
                  nodeId={nodeId}
                  modelName={data.name}
                  name={column.name}
                  description={column.description}
                  type={column.data_type}
                  className="p-1 first:border-t-0 h-6"
                  columnLineageData={
                    (
                      column as Column & {
                        columnLineageData?: ColumnLevelLineageAdjacencyList
                      }
                    ).columnLineageData
                  }
                />
              ))}
            </VerticalContainer>
            {columns.length > 0 && (
              <NodePorts
                ports={columns}
                estimatedListItemHeight={24}
                isFilterable={hasColumnsFilter}
                filterOptions={{
                  keys: ['name', 'description'],
                  threshold: 0.3,
                }}
                renderPort={column => (
                  <ModelNodeColumn
                    key={column.id}
                    id={column.id}
                    nodeId={nodeId}
                    modelName={data.name}
                    name={column.name}
                    description={column.description}
                    type={column.data_type}
                    className="p-1 border-t border-lineage-divider first:border-t-0 h-6"
                    columnLineageData={
                      (
                        column as Column & {
                          columnLineageData?: ColumnLevelLineageAdjacencyList
                        }
                      ).columnLineageData
                    }
                  />
                )}
              />
            )}
          </VerticalContainer>
        )}
      </NodeBase>
    </NodeContainer>
  )
})

export function NodeDetail({
  label,
  value,
  hasDivider = true,
  className,
}: {
  label: string
  value: string
  hasDivider?: boolean
  className?: string
}) {
  return (
    <>
      {hasDivider && <NodeDivider />}
      <Metadata
        label={label}
        value={value}
        className={cn('px-2 text-xs shrink-0 h-6', className)}
      />
    </>
  )
}
