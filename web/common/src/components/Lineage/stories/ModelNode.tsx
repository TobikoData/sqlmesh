import cronstrue from 'cronstrue'
import React from 'react'

import { cn } from '@sqlmesh-common/utils'
import { HorizontalContainer } from '../../HorizontalContainer/HorizontalContainer'
import { VerticalContainer } from '../../VerticalContainer/VerticalContainer'
import {
  MAX_COLUMNS_TO_DISPLAY,
  calculateColumnsHeight,
  calculateNodeColumnsCount,
  calculateSelectedColumnsHeight,
} from '../LineageColumnLevel/help'
import { useColumns, type Column } from '../LineageColumnLevel/useColumns'
import { calculateNodeBaseHeight, calculateNodeDetailsHeight } from '../help'
import { NodeAppendix } from '../node/NodeAppendix'
import { NodeBadge } from '../node/NodeBadge'
import { NodeBase } from '../node/NodeBase'
import { NodeContainer } from '../node/NodeContainer'
import { NodeHandleIcon } from '../node/NodeHandleIcon'
import { NodeHandles } from '../node/NodeHandles'
import { NodeHeader } from '../node/NodeHeader'
import { useNodeMetadata, type NodeProps } from '../node/useNodeMetadata'
import { ZOOM_THRESHOLD } from '../utils'
import {
  type ModelName as ModelNameType,
  type ColumnName,
  type NodeData,
  useModelLineage,
  type ModelColumn,
  type ModelNodeId,
  type ModelColumnID,
  type NodeType,
} from './ModelLineageContext'
import { ModelNodeColumn } from './ModelNodeColumn'
import {
  getNodeTypeBorderColor,
  getNodeTypeColor,
  getNodeTypeTextColor,
} from './help'
import { Tooltip } from '@sqlmesh-common/components/Tooltip/Tooltip'
import type { ColumnLevelLineageAdjacencyList } from '../LineageColumnLevel/ColumnLevelLineageContext'
import { ModelName } from '@sqlmesh-common/components/ModelName/ModelName'
import { Badge } from '@sqlmesh-common/components/Badge/Badge'
import { NodePorts } from '../node/NodePorts'

export const ModelNode = React.memo(function ModelNode({
  id,
  data,
  ...props
}: NodeProps<NodeData>) {
  const {
    selectedColumns,
    zoom,
    currentNodeId,
    selectedNodeId,
    selectedNodes,
    showColumns,
    fetchingColumns,
    setSelectedNodeId,
  } = useModelLineage()

  const [showNodeColumns, setShowNodeColumns] = React.useState(showColumns)
  const [isHovered, setIsHovered] = React.useState(false)

  const nodeId = id as ModelNodeId

  const {
    leftId,
    rightId,
    isCurrent,
    isSelected, // if selected from inside the lineage and node is selcted
    isActive, // if selected from inside the lineage and node is not selected but in path
  } = useNodeMetadata(nodeId, currentNodeId, selectedNodeId, selectedNodes)

  const {
    columns,
    selectedColumns: modelSelectedColumns,
    columnNames,
  } = useColumns<ModelNameType, ColumnName, ModelColumn, ModelColumnID>(
    selectedColumns,
    data.name,
    data.columns,
  )

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
  const modelType = data.model_type?.toLowerCase() as NodeType
  const hasColumnsFilter =
    shouldShowColumns && columns.length > MAX_COLUMNS_TO_DISPLAY
  // We are not including the footer, because we need actual height to dynamically adjust node container height
  const nodeBaseHeight = calculateNodeBaseHeight({
    includeNodeFooterHeight: false,
    includeCeilingHeight: false,
    includeFloorHeight: false,
  })
  const nodeDetailsHeight =
    zoom > ZOOM_THRESHOLD
      ? calculateNodeDetailsHeight({
          nodeDetailsCount: 0,
        })
      : 0
  const selectedColumnsHeight = calculateSelectedColumnsHeight(
    modelSelectedColumns.length,
  )
  const columnsHeight =
    zoom > ZOOM_THRESHOLD && shouldShowColumns
      ? calculateColumnsHeight({
          columnsCount: calculateNodeColumnsCount(columns.length),
          hasColumnsFilter,
        })
      : 0

  // If zoom is less than ZOOM_THRESHOLD, we are making node looks bigger
  const nodeHeight =
    (zoom > ZOOM_THRESHOLD ? nodeBaseHeight : nodeBaseHeight * 2) +
    nodeDetailsHeight +
    selectedColumnsHeight +
    columnsHeight

  return (
    <NodeContainer
      className={cn(
        'hover:opacity-100 group',
        isActive || isSelected || hasSelectedColumns
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
        <HorizontalContainer className="gap-1 items-center overflow-visible h-5">
          {isCurrent && (
            <NodeBadge className="bg-lineage-node-current-background text-lineage-node-current-foreground">
              current
            </NodeBadge>
          )}
          {zoom > ZOOM_THRESHOLD && (
            <>
              <NodeBadge>{data.kind?.toUpperCase()}</NodeBadge>
              {data.cron && (
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
              )}
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
          className={cn(zoom > ZOOM_THRESHOLD ? 'shrink-0 h-7' : 'h-full')}
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
                'gap-2 items-center pl-4 pr-2',
                getNodeTypeBorderColor(modelType),
              )}
            >
              <ModelName
                showTooltip
                hideCatalog
                hideSchema={zoom <= ZOOM_THRESHOLD}
                hideIcon
                showCopy
                name={data.displayName}
                grayscale
                className={cn(
                  'w-full overflow-hidden cursor-default truncate',
                  zoom > ZOOM_THRESHOLD
                    ? ' text-xs'
                    : 'text-2xl justify-center',
                )}
              />
            </HorizontalContainer>
          </NodeHandles>
        </NodeHeader>
        {shouldShowColumns && (
          <>
            {modelSelectedColumns.length > 0 && (
              <VerticalContainer className="h-auto shrink-0 border-t border-lineage-divider">
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
                        column satisfies Column & {
                          columnLineageData?: ColumnLevelLineageAdjacencyList<
                            ModelNameType,
                            ColumnName
                          >
                        }
                      ).columnLineageData
                    }
                  />
                ))}
              </VerticalContainer>
            )}
            {columns.length > 0 && zoom > ZOOM_THRESHOLD && (
              <NodePorts<ModelColumn>
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
                        column satisfies Column & {
                          columnLineageData?: ColumnLevelLineageAdjacencyList<
                            ModelNameType,
                            ColumnName
                          >
                        }
                      ).columnLineageData
                    }
                  />
                )}
                className="border-t border-lineage-divider"
              />
            )}
          </>
        )}
      </NodeBase>
      {modelType && (
        <NodeAppendix
          position="bottom"
          className="bg-lineage-node-appendix-background"
        >
          <HorizontalContainer
            className={cn(
              'gap-1 items-center overflow-visible',
              zoom > ZOOM_THRESHOLD ? 'h-5' : 'h-8',
            )}
          >
            <Badge
              size={zoom > ZOOM_THRESHOLD ? '2xs' : 'm'}
              className={cn(
                'text-[white] font-black',
                getNodeTypeColor(modelType),
              )}
            >
              {modelType.toUpperCase()}
            </Badge>
          </HorizontalContainer>
        </NodeAppendix>
      )}
    </NodeContainer>
  )
})
