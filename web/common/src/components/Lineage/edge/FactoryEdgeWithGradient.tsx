import React from 'react'

import { type LineageContextHook } from '../LineageContext'
import {
  type EdgeId,
  type LineageNodeData,
  type NodeId,
  type PortId,
} from '../utils'
import { EdgeWithGradient, type EdgeData } from './EdgeWithGradient'
import type { Edge, EdgeProps } from '@xyflow/react'

export function FactoryEdgeWithGradient<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends EdgeData = EdgeData,
  TNodeID extends string = NodeId,
  TEdgeID extends string = EdgeId,
  TPortID extends string = PortId,
>(
  useLineage: LineageContextHook<
    TNodeData,
    TEdgeData,
    TNodeID,
    TEdgeID,
    TPortID
  >,
) {
  return React.memo(({ data, id, ...props }: EdgeProps<Edge<TEdgeData>>) => {
    const edgeId = id as TEdgeID

    const { selectedEdges } = useLineage()

    const isActive = selectedEdges.has(edgeId)

    let startColor = 'var(--color-lineage-edge)'
    let endColor = 'var(--color-lineage-edge)'

    if (isActive && data?.startColor) {
      startColor = data?.startColor
    }

    if (isActive && data?.endColor) {
      endColor = data?.endColor
    }

    return (
      <EdgeWithGradient
        {...props}
        id={edgeId}
        data={{
          ...data,
          startColor,
          endColor,
        }}
      />
    )
  })
}
