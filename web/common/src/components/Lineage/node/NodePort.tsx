import { useNodeConnections } from '@xyflow/react'
import React from 'react'

import { cn } from '@sqlmesh-common/utils'
import { type NodeId, type PortHandleId } from '../utils'
import { NodeHandles } from './NodeHandles'

export function NodePort<
  TPortId extends string = PortHandleId,
  TNodeID extends string = NodeId,
  TLeftPortHandleId extends string = PortHandleId,
  TRightPortHandleId extends string = PortHandleId,
>({
  id,
  nodeId,
  className,
  children,
}: {
  id: TPortId
  nodeId: TNodeID
  className?: string
  children: React.ReactNode
}) {
  const sources = useNodeConnections({
    id: nodeId,
    handleType: 'source',
    handleId: id,
  })
  const targets = useNodeConnections({
    id: nodeId,
    handleType: 'target',
    handleId: id,
  })

  const isLeftHandleId = (id: TPortId): id is TPortId & TLeftPortHandleId => {
    return id && targets.length > 0
  }

  const isRightHandleId = (id: TPortId): id is TPortId & TRightPortHandleId => {
    return id && sources.length > 0
  }

  const leftId = isLeftHandleId(id) ? id : undefined
  const rightId = isRightHandleId(id) ? id : undefined

  return (
    <NodeHandles<TLeftPortHandleId, TRightPortHandleId>
      data-component="NodePort"
      leftIcon={
        <span className="flex-shrink-0 p-1.5 rounded-full bg-lineage-node-port-handle-target"></span>
      }
      rightIcon={
        <span className="flex-shrink-0 p-1.5 rounded-full bg-lineage-node-port-handle-source"></span>
      }
      leftId={leftId}
      rightId={rightId}
      className={cn(
        'relative overflow-visible group bg-lineage-node-port-background h-auto',
        className,
      )}
      handleClassName="absolute"
    >
      {children}
    </NodeHandles>
  )
}
