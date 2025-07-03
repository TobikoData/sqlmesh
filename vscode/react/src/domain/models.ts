import type { Branded } from '@bus/brand'

/**
 * ModelName is a type that represents the name of a model.
 */
export type ModelName = Branded<string, 'ModelName'>

/**
 * ModelEncodedName is a type that represents the encoded name of a model.
 */
export type ModelEncodedName = Branded<string, 'ModelEncodedName'>

/**
 * ModelFQN is a type that represents the fully qualified name of a model.
 */
export type ModelFQN = Branded<string, 'ModelFQN'>

/**
 * ModelEncodedFQN is a type that represents the encoded fully qualified name of a model.
 */
export type ModelEncodedFQN = Branded<string, 'ModelEncodedFQN'>

/**
 * ModelURI is a type that represents the URI of a model.
 */
export type ModelURI = Branded<string, 'ModelURI'>

/**
 * ModelEncodedURI is a type that represents the encoded URI of a model.
 */
export type ModelEncodedURI = Branded<string, 'ModelEncodedURI'>

export function encode(fqn: ModelName): ModelEncodedName
export function encode(fqn: ModelURI): ModelEncodedURI
export function encode(fqn: ModelFQN): ModelEncodedFQN
export function encode(s: string): string {
  return encodeURI(s)
}

export function decode(fqn: ModelEncodedName): ModelName
export function decode(fqn: ModelEncodedURI): ModelURI
export function decode(fqn: ModelEncodedFQN): ModelFQN
export function decode(s: string): string {
  return decodeURI(s)
}

/**
 * ModelPath is a type that represents the path of a model.
 * A model path is relative to the project root.
 */
export type ModelPath = Branded<string, 'ModelPath'>

/**
 * ModelFullPath is a type that represents the full path of a model.
 * A model full path is a fully qualified path to a model.
 */
export type ModelFullPath = Branded<string, 'ModelFullPath'>
