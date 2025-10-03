import type { ModelName } from '@/domain/models'
import type { Branded } from '@tobikodata/sqlmesh-common'
import {
  type Column,
  type ColumnLevelLineageAdjacencyList,
  type ColumnLevelLineageContextValue,
  type LineageContextValue,
  type PathType,
  getInitial as getLineageContextInitial,
  getColumnLevelLineageContextInitial,
  createLineageContext,
} from '@tobikodata/sqlmesh-common/lineage'

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
  model_type: string
  identifier?: string | null
  version?: string | null
  dialect?: string | null
  cron?: string | null
  owner?: string | null
  kind?: string | null
  tags?: string[]
  columns?: Record<ColumnName, Column>
}

export type NodeData = {
  name: ModelName
  displayName: string
  model_type: NodeType
  identifier?: string | null
  version?: string | null
  kind?: string | null
  cron?: string | null
  owner?: string | null
  dialect?: string | null
  tags?: string[]
  columns?: Record<ColumnName, Column>
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
