import type { ColumnName } from '@/domain/column'
import type { ModelEncodedFQN } from '@/domain/models'

export const EnumSide = {
  Left: 'left',
  Right: 'right',
} as const

export type Side = (typeof EnumSide)[keyof typeof EnumSide]

export type NodeId = string

export type EdgeId = string

/**
 * Converts a list of strings to a single string with a double underscore
 * Outlines with types, the type of ids that can be created.
 * @param args
 * @returns
 */
export function toID(
  leftOrRight: 'left' | 'right',
  modelName: ModelEncodedFQN,
  columnName: ColumnName,
): NodeId
export function toID(source: NodeId, target: NodeId): NodeId
export function toID(
  leftOrRight: 'left' | 'right',
  modelName: ModelEncodedFQN,
): NodeId
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
