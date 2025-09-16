import {
  type ColumnLevelLineageAdjacencyList,
  type ColumnLevelLineageContextValue,
  initial as columnLevelLineageContextInitial,
} from '../LineageColumnLevel/ColumnLevelLineageContext'
import { type Column } from '../LineageColumnLevel/useColumns'
import {
  type LineageContextValue,
  createLineageContext,
  initial as lineageContextInitial,
} from '../LineageContext'
import {
  type AdjacencyListColumnKey,
  type AdjacencyListKey,
  type LineageEdgeData,
  type LineageNodeData,
  type PathType,
} from '../utils'

export type NodeType = 'sql' | 'python'

export type AdjacencyListNode = {
  name: AdjacencyListKey
  identifier: string
}

export interface ModelLineageNodeDetails {
  name: string
  display_name: string
  identifier: string
  version: string
  dialect: string
  cron: string
  owner?: string
  kind?: string
  model_type?: string
  tags?: string[]
  columns?: Record<
    AdjacencyListColumnKey,
    Column & { columnLineageData?: ColumnLevelLineageAdjacencyList }
  >
}

export type NodeData = {
  name: AdjacencyListKey
  displayName: string
  model_type: NodeType
  identifier: string
  version: string
  kind: string
  cron: string
  owner: string
  dialect: string
  columns?: Record<AdjacencyListColumnKey, Column>
  tags: string[]
}

export type EdgeData = {
  pathType?: PathType
  startColor?: string
  endColor?: string
  strokeWidth?: number
}

export interface ModelLineageContextValue<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
> extends ColumnLevelLineageContextValue,
    LineageContextValue<TNodeData, TEdgeData> {}

const initial = {
  ...lineageContextInitial,
  ...columnLevelLineageContextInitial,
}

export const { Provider, useLineage } = createLineageContext<
  NodeData,
  EdgeData,
  ModelLineageContextValue<NodeData, EdgeData>
>(initial)

export const ModelLineageContext = {
  Provider,
}

export const useModelLineage = useLineage
