import type { Branded } from '@/types'
import {
  type ColumnLevelLineageAdjacencyList,
  type ColumnLevelLineageContextValue,
  getColumnLevelLineageContextInitial,
} from '../LineageColumnLevel/ColumnLevelLineageContext'
import { type Column } from '../LineageColumnLevel/useColumns'
import {
  type LineageContextValue,
  createLineageContext,
  getInitial as getLineageContextInitial,
} from '../LineageContext'
import { type PathType } from '../utils'

export type ModelName = Branded<string, 'ModelName'>
export type ColumnName = Branded<string, 'ColumnName'>
export type ModelColumnID = Branded<string, 'ModelColumnID'>
export type ModelNodeId = Branded<string, 'ModelNodeId'>
export type ModelEdgeId = Branded<string, 'ModelEdgeId'>
export type ModelColumn = Column & {
  id: ModelColumnID
  name: ColumnName
  columnLineageData?: ColumnLevelLineageAdjacencyList<ModelName, ColumnName>
}

export type NodeType = 'sql' | 'python'
export type ModelLineageNodeDetails = {
  name: ModelName
  display_name: string
  identifier: string
  version: string
  dialect: string
  cron: string
  owner?: string
  kind?: string
  model_type?: string
  tags?: string[]
  columns?: Record<ColumnName, ModelColumn>
}

export type NodeData = {
  name: ModelName
  displayName: string
  model_type: NodeType
  identifier: string
  version: string
  kind: string
  cron: string
  owner: string
  dialect: string
  columns?: Record<ColumnName, ModelColumn>
  tags: string[]
}

export type EdgeData = {
  pathType?: PathType
  startColor?: string
  endColor?: string
  strokeWidth?: number
}

export type ModelLineageContextValue = ColumnLevelLineageContextValue<
  ModelName,
  ColumnName,
  ModelColumnID
> &
  LineageContextValue<
    NodeData,
    EdgeData,
    ModelNodeId,
    ModelEdgeId,
    ModelColumnID
  >

export const initial = {
  ...getLineageContextInitial<ModelNodeId, ModelEdgeId>(),
  ...getColumnLevelLineageContextInitial<
    ModelName,
    ColumnName,
    ModelColumnID
  >(),
}

export const { Provider, useLineage } = createLineageContext<
  NodeData,
  EdgeData,
  ModelNodeId,
  ModelEdgeId,
  ModelColumnID,
  ModelLineageContextValue
>(initial)

export const ModelLineageContext = {
  Provider,
}

export const useModelLineage = useLineage
