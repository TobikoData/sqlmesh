import React from 'react'

import { type LineageContextHook } from '../LineageContext'
import {
  type EdgeId,
  type LineageEdgeData,
  type LineageNodeData,
  type PathType,
} from '../utils'
import { EdgeWithGradient } from './EdgeWithGradient'
import type { Edge, EdgeProps } from '@xyflow/react'

export interface EdgeData extends LineageEdgeData {
  startColor?: string
  endColor?: string
  strokeWidth?: number
  pathType?: PathType
}

export function FactoryEdgeWithGradient<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends EdgeData = EdgeData,
>(useLineage: LineageContextHook<TNodeData, TEdgeData>) {
  return React.memo(({ data, id, ...props }: EdgeProps<Edge<TEdgeData>>) => {
    const edgeId = id as EdgeId

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
