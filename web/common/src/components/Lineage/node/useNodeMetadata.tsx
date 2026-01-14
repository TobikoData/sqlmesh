import {
  type Node,
  type NodeProps as ReactFlowNodeProps,
  useNodeConnections,
} from '@xyflow/react'

import { type LineageNodeData, type NodeId } from '../utils'

export type NodeProps<TNodeData extends LineageNodeData = LineageNodeData> =
  ReactFlowNodeProps<Node<TNodeData>>

export function useNodeMetadata<TNodeID extends string = NodeId>(
  nodeId: TNodeID,
  currentNodeId: TNodeID | null,
  selectedNodeId: TNodeID | null,
  selectedNodes: Set<TNodeID>,
) {
  const sources = useNodeConnections({
    id: nodeId,
    handleType: 'source',
  })
  const targets = useNodeConnections({
    id: nodeId,
    handleType: 'target',
  })

  const leftId = targets.length > 0 ? nodeId : undefined
  const rightId = sources.length > 0 ? nodeId : undefined
  const isCurrent = currentNodeId === nodeId
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
