import type { ModelType } from '@/api/client'
import type { ModelFQN, ModelName } from '@/domain/models'
import type { Branded, BrandedString } from '@bus/brand'
import {
  type Column,
  type ColumnLevelLineageContextValue,
  type LineageContextValue,
  type PathType,
  getInitial as getLineageContextInitial,
  getColumnLevelLineageContextInitial,
  createLineageContext,
  type LineageAdjacencyList,
  type LineageDetails,
  type ColumnLevelLineageAdjacencyList,
} from '@tobikodata/sqlmesh-common/lineage'

export type BrandedLineageAdjacencyList<K extends BrandedString> =
  LineageAdjacencyList<K> & {
    readonly __adjacencyListKeyBrand: K
  }

export type BrandedLineageDetails<K extends BrandedString, V> = LineageDetails<
  K,
  V
> & {
  readonly __lineageDetailsKeyBrand: K
}

export type BrandedColumnLevelLineageAdjacencyList<
  K extends BrandedString,
  V extends BrandedString,
> = ColumnLevelLineageAdjacencyList<K, V> & {
  readonly __columnLevelLineageAdjacencyListKeyBrand: K
  readonly __columnLevelLineageAdjacencyListColumnKeyBrand: V
}

export type ColumnName = Branded<string, 'ColumnName'>
export type ModelColumnID = Branded<string, 'ModelColumnID'>
export type ModelNodeId = Branded<string, 'ModelNodeId'>
export type ModelEdgeId = Branded<string, 'ModelEdgeId'>
export type ModelColumn = Column & {
  id: ModelColumnID
  name: ColumnName
}

export type ModelLineageNodeDetails = {
  name: ModelFQN
  display_name: ModelName
  model_type: ModelType
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
  name: ModelFQN
  displayName: ModelName
  model_type: ModelType
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
  ModelFQN,
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
  ...getColumnLevelLineageContextInitial<ModelFQN, ColumnName, ModelColumnID>(),
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
