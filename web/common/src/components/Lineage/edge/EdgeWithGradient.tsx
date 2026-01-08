import {
  type Edge,
  type EdgeProps,
  getBezierPath,
  getSmoothStepPath,
  getStraightPath,
} from '@xyflow/react'
import React, { useId } from 'react'

import { type EdgeId, type LineageEdgeData, type PathType } from '../utils'

export interface EdgeData extends LineageEdgeData {
  startColor?: string
  endColor?: string
  strokeWidth?: number
  pathType?: PathType
}

export const EdgeWithGradient = React.memo(
  <TEdgeData extends EdgeData = EdgeData>({
    id,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    style,
    data,
    markerEnd,
  }: EdgeProps<Edge<TEdgeData>>) => {
    const edgeId = id as EdgeId

    const gradientId = useId()
    const startColor = data?.startColor || 'var(--color-lineage-edge)'
    const endColor = data?.endColor || 'var(--color-lineage-edge)'
    const pathType = data?.pathType || 'bezier'
    const strokeWidth = data?.strokeWidth || 4
    const edgePath = getEdgePath(pathType)

    function getEdgePath(pathType: PathType) {
      return {
        straight: getStraightPath({
          sourceX,
          sourceY,
          targetX,
          targetY,
        }),
        smoothstep: getSmoothStepPath({
          sourceX,
          sourceY,
          sourcePosition,
          targetX,
          targetY,
          targetPosition,
          borderRadius: 10,
        }),
        bezier: getBezierPath({
          sourceX,
          sourceY,
          sourcePosition,
          targetX,
          targetY,
          targetPosition,
        }),
        step: getSmoothStepPath({
          sourceX,
          sourceY,
          sourcePosition,
          targetX,
          targetY,
          targetPosition,
          borderRadius: 0,
        }),
      }[pathType]
    }

    return (
      <>
        <defs>
          <linearGradient
            id={gradientId}
            x1={sourceX}
            y1={sourceY}
            x2={targetX}
            y2={targetY}
            gradientUnits="userSpaceOnUse"
          >
            <stop
              offset="0%"
              stopColor={startColor}
            />
            <stop
              offset="100%"
              stopColor={endColor}
            />
          </linearGradient>
        </defs>
        <path
          id={edgeId}
          style={{
            ...style,
            stroke: `url(#${gradientId})`,
            strokeWidth,
            fill: 'none',
          }}
          className="react-flow__edge-path"
          d={edgePath[0]}
          markerEnd={markerEnd}
        />
      </>
    )
  },
)
