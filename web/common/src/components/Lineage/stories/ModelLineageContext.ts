import type { Branded, BrandedRecord } from '@sqlmesh-common/types'
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
import {
  type LineageAdjacencyList,
  type LineageDetails,
  type PathType,
} from '../utils'

export type ModelName = Branded<string, 'ModelName'>
export type ModelDisplayName = Branded<string, 'ModelDisplayName'>
export type ColumnName = Branded<string, 'ColumnName'>
export type ModelColumnID = Branded<string, 'ModelColumnID'>
export type ModelEdgeId = Branded<string, 'ModelEdgeId'>
export type LeftHandleId = Branded<string, 'LeftHandleId'>
export type RightHandleId = Branded<string, 'RightHandleId'>
export type ModelNodeId = LeftHandleId | RightHandleId
export type LeftPortHandleId = Branded<string, 'LeftPortHandleId'>
export type RightPortHandleId = Branded<string, 'RightPortHandleId'>

export type BrandedColumnLevelLineageAdjacencyList =
  ColumnLevelLineageAdjacencyList<ModelName, ColumnName> & {
    readonly __adjacencyListKeyBrand: ModelName
    readonly __adjacencyListColumnKeyBrand: ColumnName
  }

export type BrandedLineageAdjacencyList = LineageAdjacencyList<ModelName> & {
  readonly __adjacencyListKeyBrand: ModelName
}

export type BrandedLineageDetails = LineageDetails<
  ModelName,
  ModelLineageNodeDetails
> & {
  readonly __lineageDetailsKeyBrand: ModelName
}

export type ModelColumn = Column & {
  id: ModelColumnID
  name: ColumnName
  columnLineageData?: BrandedColumnLevelLineageAdjacencyList
}

export type NodeType = 'sql' | 'python'
export type ModelLineageNodeDetails = {
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
  columns?: BrandedRecord<ColumnName, Column>
}

export type NodeData = {
  name: ModelName
  displayName: ModelDisplayName
  model_type: NodeType
  identifier: string
  version: string
  kind: string
  cron: string
  owner: string
  dialect: string
  tags: string[]
  columns?: BrandedRecord<ColumnName, ModelColumn>
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
  ModelColumnID,
  BrandedColumnLevelLineageAdjacencyList
> &
  LineageContextValue<
    NodeData,
    EdgeData,
    ModelNodeId,
    ModelEdgeId,
    LeftHandleId,
    RightHandleId,
    LeftPortHandleId,
    RightPortHandleId
  >

export const initial = {
  ...getLineageContextInitial<ModelNodeId, ModelEdgeId>(),
  ...getColumnLevelLineageContextInitial<
    ModelName,
    ColumnName,
    ModelColumnID,
    BrandedColumnLevelLineageAdjacencyList
  >(),
}

export const { Provider, useLineage } = createLineageContext<
  NodeData,
  EdgeData,
  ModelNodeId,
  ModelEdgeId,
  LeftHandleId,
  RightHandleId,
  LeftPortHandleId,
  RightPortHandleId,
  ModelLineageContextValue
>(initial)

export const ModelLineageContext = {
  Provider,
}

export const useModelLineage = useLineage
