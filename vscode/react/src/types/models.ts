export type Brand<T, B extends string> = T & { __brand: B }

export type ModelName = Brand<string, 'ModelName'>
export type ModelFQN = Brand<string, 'ModelFQN'>
export type ModelURI = Brand<string, 'ModelURI'>
export type ModelPath = Brand<string, 'ModelPath'>
