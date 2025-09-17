import { cn } from '@/utils'
import { HorizontalContainer } from '../../HorizontalContainer/HorizontalContainer'
import { NodeAppendix } from '../node/NodeAppendix'
import { NodeBase } from '../node/NodeBase'
import { NodeContainer } from '../node/NodeContainer'
import { NodeBadge } from '../node/NodeBadge'
import { Tooltip } from '../../Tooltip/Tooltip'
import cronstrue from 'cronstrue'
import { NodeHeader } from '../node/NodeHeader'
import { ModelName } from '../../ModelName/ModelName'
import { VerticalContainer } from '../../VerticalContainer/VerticalContainer'
import { NodePorts } from '../node/NodePorts'
import { NodePort } from '../node/NodePort'
import type { AdjacencyListKey, NodeId, PortId } from '../utils'
import { Metadata } from '../../Metadata/Metadata'
import { type NodeProps, type Node } from '@xyflow/react'

import '@xyflow/react/dist/style.css'
import { NodeDivider } from '../node/NodeDivider'
import { ModelLineage } from './ModelLineage'
import type {
  AdjacencyListNode,
  ModelLineageNodeDetails,
} from './ModelLineageContext'

export default {
  title: 'Components/Lineage',
}

export const LineageModel = () => {
  return (
    <div
      style={{
        width: '90vw',
        height: '90vh',
      }}
    >
      <style>{`
        :root {
          --color-lineage-divider: rgba(0, 0, 0, 0.1);
          --color-lineage-background: rgba(0, 0, 0, 0);
          --color-lineage-border: rgba(0, 0, 0, 0.1);

          --color-lineage-control-background: rgba(250, 250, 250, 1);
          --color-lineage-control-background-hover: rgba(245, 245, 245, 1);
          --color-lineage-control-icon-background: rgba(0, 0, 0, 1);
          --color-lineage-control-icon-foreground: rgba(255, 255, 255, 1);

          --color-lineage-grid-dot: rgba(0, 0, 0, 1);

          --color-lineage-node-appendix-background: transparent;
          
          --color-lineage-node-background: rgba(255, 255, 255, 1);
          --color-lineage-node-foreground: rgba(0, 0, 0, 0.75);
          --color-lineage-node-border: rgba(0, 0, 0, 0.1);
          --color-lineage-node-border-hover: rgba(0, 0, 0, 0.2);

          --color-lineage-node-selected-border: rgba(0, 120, 120, 0.5);

          --color-lineage-node-badge-background: rgba(240, 240, 240, 1);
          --color-lineage-node-badge-foreground: rgba(0, 0, 0, 1);

          --color-lineage-node-type-background-sql: rgba(0, 0, 120, 1);
          --color-lineage-node-type-foreground-sql: rgba(0, 0, 120, 1);
          --color-lineage-node-type-border-sql: rgba(0, 0, 120, 1);

          --color-lineage-node-type-background-python: rgba(120, 0, 120, 1);
          --color-lineage-node-type-foreground-python: rgba(120, 0, 120, 1);
          --color-lineage-node-type-border-python: rgba(120, 0, 120, 1);

          --color-lineage-node-type-handle-icon-background: rgba(255, 255, 255, 1);

          --color-lineage-node-port-background: rgba(70, 0, 0, 0.05);
          --color-lineage-node-port-handle-source: rgba(70, 0, 0, 1);
          --color-lineage-node-port-handle-target: rgba(170, 0, 0, 1);
          --color-lineage-node-port-edge-source: rgba(70, 0, 0, 1);
          --color-lineage-node-port-edge-target: rgba(130, 0, 0, 1);

          --color-lineage-model-column-error-background: rgba(255, 0, 0, 1);
          --color-lineage-model-column-source-background: rgba(200, 0, 0, 1);
          --color-lineage-model-column-expression-background: rgba(100, 0, 0, 1);
          --color-lineage-model-column-error-icon: rgba(255, 0, 0, 1);
          --color-lineage-model-column-active: rgba(70, 0, 0, 0.1);
          --color-lineage-model-column-icon: rgba(0, 0, 0, 1);
          --color-lineage-model-column-icon-active: rgba(0, 0, 0, 1);
        }
      `}</style>
      <ModelLineage
        selectedModelName="sqlmesh.sushi.orders"
        artifactAdjacencyList={
          {
            'sqlmesh.sushi.raw_orders': [
              {
                name: 'sqlmesh.sushi.orders',
                identifier: '123456789',
              },
            ],
            'sqlmesh.sushi.orders': [],
          } as Record<AdjacencyListKey, AdjacencyListNode[]>
        }
        artifactDetails={
          {
            'sqlmesh.sushi.raw_orders': {
              name: 'sqlmesh.sushi.raw_orders',
              display_name: 'sushi.raw_orders',
              identifier: '123456789',
              version: '123456789',
              dialect: 'bigquery',
              cron: '0 0 * * *',
              owner: 'admin',
              kind: 'INCREMENTAL_BY_TIME',
              model_type: 'python',
              tags: ['test', 'tag', 'another tag'],
              columns: {
                user_id: {
                  data_type: 'STRING',
                  description: 'node',
                },
                event_id: {
                  data_type: 'STRING',
                  description: 'node',
                },
                created_at: {
                  data_type: 'TIMESTAMP',
                  description: 'node',
                },
              },
            },
            'sqlmesh.sushi.orders': {
              name: 'sqlmesh.sushi.orders',
              display_name: 'sushi.orders',
              identifier: '123456789',
              version: '123456789',
              dialect: 'bigquery',
              cron: '0 0 * * *',
              owner: 'admin',
              kind: 'INCREMENTAL_BY_TIME',
              model_type: 'sql',
              tags: ['test', 'tag', 'another tag'],
              columns: {
                user_id: {
                  data_type: 'STRING',
                  description: 'node',
                  columnLineageData: {
                    'sqlmesh.sushi.orders': {
                      user_id: {
                        source: 'sqlmesh.sushi.raw_orders',
                        expression:
                          'select user_id from sqlmesh.sushi.raw_orders',
                        models: {
                          'sqlmesh.sushi.raw_orders': ['user_id'],
                        },
                      },
                    },
                  },
                },
                event_id: {
                  data_type: 'STRING',
                  description: 'node',
                  columnLineageData: {
                    'sqlmesh.sushi.orders': {
                      event_id: {
                        models: {
                          'sqlmesh.sushi.raw_orders': ['event_id'],
                        },
                      },
                    },
                  },
                },
                product_id: {
                  data_type: 'STRING',
                  description: 'node',
                },
                customer_id: {
                  data_type: 'STRING',
                  description: 'node',
                },
                updated_at: {
                  data_type: 'TIMESTAMP',
                  description: 'node',
                },
                deleted_at: {
                  data_type: 'TIMESTAMP',
                  description: 'node',
                },
                expired_at: {
                  data_type: 'TIMESTAMP',
                  description: 'node',
                },
                start_at: {
                  data_type: 'TIMESTAMP',
                  description: 'node',
                },
                end_at: {
                  data_type: 'TIMESTAMP',
                  description: 'node',
                },
                created_ts: {
                  data_type: 'TIMESTAMP',
                  description: 'node',
                },
              },
            },
          } as Record<AdjacencyListKey, ModelLineageNodeDetails>
        }
        className="rounded-2xl"
      />
    </div>
  )
}

type NodeData = {
  name: string
  displayName: string
  model_type: 'sql' | 'pythob'
  identifier: string
  version: string
  kind: string
  cron: string
  owner: string
  dialect: string
  columns?: Record<
    string,
    {
      data_type: string
      description: string
    }
  >
  tags: string[]
  planId?: string
  runId?: string
}

function CustomNode({ id, data, type }: NodeProps<Node<NodeData>>) {
  const nodeId = id as NodeId
  const columns = Object.entries(data.columns ?? {}).map(([key, value]) => ({
    id: key as PortId,
    name: key,
    description: value.description,
    data_type: value.data_type,
  }))
  const modelSelectedColumns = columns.filter(column =>
    ['user_id', 'event_id'].includes(column.id),
  )
  const nodeDetails = {
    showKind: true,
    showCron: true,
    showFQN: true,
    showOwner: true,
    showDialect: true,
    showTags: true,
    showVersion: true,
    showIdentifier: true,
    showModelType: true,
  }
  const modelType = 'sql'

  return (
    <NodeContainer className="opacity-100 hover:opacity-100 group">
      <NodeAppendix
        position="top"
        className="bg-lineage-node-appendix-background"
      >
        <HorizontalContainer className="gap-1 overflow-visible h-5">
          {nodeDetails.showKind && (
            <NodeBadge>{data.kind.toUpperCase()}</NodeBadge>
          )}
          {nodeDetails.showCron && (
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
        </HorizontalContainer>
      </NodeAppendix>
      <NodeBase
        data-component="ModelNodeDemo"
        id={id}
        data={data}
        className={cn(
          'overflow-hidden ring-offset-2 z-10 hover:ring-2 hover:ring-lineage-node-border-hover',
          'border-[blue]',
        )}
        selectable={false}
        draggable={false}
        deletable={false}
        zIndex={0}
        type={type}
        dragging={false}
        selected={false}
        isConnectable={false}
        positionAbsoluteX={0}
        positionAbsoluteY={0}
      >
        <NodeHeader className="shrink-0 h-7">
          {nodeDetails.showModelType && (
            <HorizontalContainer
              className={cn('items-center px-1 w-auto shrink-0', 'bg-[blue]')}
            >
              <NodeBadge
                size="2xs"
                className="bg-[transparent] text-[white]"
              >
                {modelType.toUpperCase()}
              </NodeBadge>
            </HorizontalContainer>
          )}
          <HorizontalContainer className="gap-2 items-center px-2">
            <ModelName
              hideCatalog
              showTooltip
              hideIcon
              name={data.displayName}
              grayscale
              className="h-7 w-full text-xs overflow-hidden cursor-default truncate"
            />
          </HorizontalContainer>
        </NodeHeader>
        <VerticalContainer className="border-t border-lineage-node-border">
          <VerticalContainer className="h-auto shrink-0">
            {modelSelectedColumns.map(column => (
              <CustomColumn
                key={column.id}
                id={column.id}
                nodeId={nodeId}
                name={column.name}
                type={column.data_type}
              />
            ))}
          </VerticalContainer>
          {columns.length > 0 && (
            <NodePorts
              ports={columns}
              estimatedListItemHeight={24}
              renderPort={column => (
                <CustomColumn
                  key={column.id}
                  id={column.id}
                  nodeId={nodeId}
                  name={column.name}
                  type={column.data_type}
                />
              )}
            />
          )}
        </VerticalContainer>
        <VerticalContainer className="h-auto shrink-0">
          {nodeDetails.showFQN && data.name && (
            <NodeDetail
              label="FQN"
              value={data.name}
              hasDivider
            />
          )}
          {nodeDetails.showOwner && data.owner && (
            <NodeDetail
              label="Owner"
              value={data.owner}
              hasDivider
            />
          )}
          {nodeDetails.showDialect && data.dialect && (
            <NodeDetail
              label="Dialect"
              value={data.dialect}
              hasDivider
            />
          )}
          {nodeDetails.showTags && data.tags.length > 0 && (
            <NodeDetail
              label="Tags"
              value={data.tags.join(', ')}
              hasDivider
            />
          )}
          {nodeDetails.showIdentifier && data.identifier && (
            <NodeDetail
              label="Identifier"
              value={data.identifier}
              hasDivider
            />
          )}
        </VerticalContainer>
      </NodeBase>
    </NodeContainer>
  )
}

function CustomColumn({
  id,
  nodeId,
  name,
  type,
}: {
  id: PortId
  nodeId: NodeId
  name: string
  type: string
}) {
  return (
    <NodePort
      id={id}
      nodeId={nodeId}
      className={cn('border-t border-lineage-divider first:border-t-0 px-2')}
    >
      <Metadata
        label={name}
        value={<NodeBadge>{type}</NodeBadge>}
        className="text-2xs h-6"
      />
    </NodePort>
  )
}

function NodeDetail({
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
