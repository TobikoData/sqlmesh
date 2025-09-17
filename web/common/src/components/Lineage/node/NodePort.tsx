import { useNodeConnections, useUpdateNodeInternals } from '@xyflow/react'
import React from 'react'

import { cn } from '@/utils'
import { type NodeId, type PortId } from '../utils'
import { NodeHandles } from './NodeHandles'

export const NodePort = React.memo(function NodePort({
  id,
  nodeId,
  className,
  children,
}: {
  id: PortId
  nodeId: NodeId
  className?: string
  children: React.ReactNode
}) {
  const updateNodeInternals = useUpdateNodeInternals()

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

  const leftId = targets.length > 0 ? id : undefined
  const rightId = sources.length > 0 ? id : undefined

  React.useEffect(() => {
    if (leftId || rightId) {
      updateNodeInternals(nodeId)
    }
  }, [updateNodeInternals, nodeId, leftId, rightId])

  return (
    <NodeHandles
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
        'relative overflow-visible group p-0 bg-lineage-node-port-background h-auto',
        className,
      )}
      handleClassName="absolute"
    >
      {children}
    </NodeHandles>
  )
})
