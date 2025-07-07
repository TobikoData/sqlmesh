import type { ColumnName } from '@/domain/column'
import type { ModelEncodedFQN } from '@/domain/models'
import type { Branded } from '@bus/brand'

export type Side = 'left' | 'right'

export type NodeId = string

export type EdgeId = string

/**
 * Partial column handle id that isn't complete yet as it's missing the left/right side
 * definition.
 */
export type PartialColumnHandleId = Branded<string, 'PartialColumnHandleId'>
export type ColumnHandleId = Branded<string, 'ColumnHandleId'>
export type ModelHandleId = Branded<string, 'ModelHandleId'>

/**
 * Converts a list of strings to a single string with a double underscore
 * Outlines with types, the type of ids that can be created.
 * @param args
 * @returns
 */
export function toID(
  leftOrRight: Side,
  modelName: ModelEncodedFQN,
  columnName: ColumnName,
): NodeId
export function toID(
  modelName: ModelEncodedFQN,
  columnName: ColumnName,
): PartialColumnHandleId
export function toID(
  leftOrRight: Side,
  partialColumnHandleId: PartialColumnHandleId,
): ColumnHandleId
export function toID(
  leftOrRight: Side,
  modelName: ModelEncodedFQN,
): ModelHandleId
export function toID(source: NodeId, target: NodeId): NodeId
export function toID(
  source: NodeId,
  target: NodeId,
  sourceHandle: string | undefined,
  targetHandle: string | undefined,
): EdgeId
export function toID(...args: Array<string | undefined>): string {
  return args.filter(Boolean).join('__')
}

export function toKeys<K extends string, V>(obj: Record<K, V>): K[] {
  return Object.keys(obj) as K[]
}

export type ModelLineage = Record<ModelEncodedFQN, ModelEncodedFQN[]>
