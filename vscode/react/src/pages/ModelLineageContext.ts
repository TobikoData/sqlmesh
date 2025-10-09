import type { ModelType } from '@/api/client'
import type { ModelFQN, ModelName } from '@/domain/models'
import type { Branded } from '@bus/brand'
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
} from '@sqlmesh-common/components/Lineage'

export type ModelColumnName = Branded<string, 'ModelColumnName'>
export type ModelColumnID = Branded<string, 'ModelColumnID'>
export type ModelNodeId = Branded<string, 'ModelNodeId'>
export type ModelEdgeId = Branded<string, 'ModelEdgeId'>
export type ModelColumn = Column & {
  id: ModelColumnID
  name: ModelColumnName
}
export type ModelColumnLeftHandleId = Branded<string, 'ModelColumnLeftHandleId'>
export type ModelColumnRightHandleId = Branded<
  string,
  'ModelColumnRightHandleId'
>

export type BrandedLineageAdjacencyList = LineageAdjacencyList<ModelFQN> & {
  readonly __adjacencyListKeyBrand: ModelFQN
}

export type BrandedLineageDetails = LineageDetails<
  ModelFQN,
  ModelLineageNodeDetails
> & {
  readonly __lineageDetailsKeyBrand: ModelFQN
}

export type BrandedModelColumns = Record<ModelColumnName, Column>

export type BrandedColumnLevelLineageAdjacencyList =
  ColumnLevelLineageAdjacencyList<ModelFQN, ModelColumnName> & {
    readonly __columnLevelLineageAdjacencyListKeyBrand: ModelFQN
    readonly __columnLevelLineageAdjacencyListColumnKeyBrand: ModelColumnName
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
  columns?: BrandedModelColumns
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
  columns?: BrandedModelColumns
}

export type EdgeData = {
  pathType?: PathType
  startColor?: string
  endColor?: string
  strokeWidth?: number
}

export type ModelLineageContextValue = ColumnLevelLineageContextValue<
  ModelFQN,
  ModelColumnName,
  ModelColumnID,
  BrandedColumnLevelLineageAdjacencyList
> &
  LineageContextValue<
    NodeData,
    EdgeData,
    ModelNodeId,
    ModelEdgeId,
    ModelNodeId,
    ModelNodeId,
    ModelColumnRightHandleId,
    ModelColumnLeftHandleId
  >

export const initial = {
  ...getLineageContextInitial<ModelNodeId, ModelEdgeId>(),
  ...getColumnLevelLineageContextInitial<
    ModelFQN,
    ModelColumnName,
    ModelColumnID,
    BrandedColumnLevelLineageAdjacencyList
  >(),
}

export const { Provider, useLineage } = createLineageContext<
  NodeData,
  EdgeData,
  ModelNodeId,
  ModelEdgeId,
  ModelNodeId,
  ModelNodeId,
  ModelColumnRightHandleId,
  ModelColumnLeftHandleId,
  ModelLineageContextValue
>(initial)

export const ModelLineageContext = {
  Provider,
}

export const useModelLineage = useLineage
