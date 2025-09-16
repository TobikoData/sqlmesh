import { useNodeConnections } from '@xyflow/react'

import { type LineageNode, type LineageNodeData, type NodeId } from '../utils'

export function useNodeMetadata<
  TNodeData extends LineageNodeData = LineageNodeData,
>(
  nodeId: NodeId,
  currentNode: LineageNode<TNodeData> | null,
  selectedNodeId: NodeId | null,
  selectedNodes: Set<NodeId>,
) {
  const sources = useNodeConnections({
    handleType: 'source',
  })
  const targets = useNodeConnections({
    handleType: 'target',
  })

  const leftId = targets.length > 0 ? nodeId : undefined
  const rightId = sources.length > 0 ? nodeId : undefined
  const isCurrent = currentNode?.id === nodeId
  const isSelected = selectedNodeId === nodeId
  const isActive = selectedNodes.has(nodeId)

  return {
    leftId,
    rightId,
    isCurrent,
    isSelected,
    isActive,
  }
}
