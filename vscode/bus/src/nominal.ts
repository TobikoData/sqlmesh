/**
 * Nominal type
 *
 * This is a nominal type that is used to create a new type that is a brand of the original type.
 * This is useful for creating interfaces that are not compatible with the original type and require
 * a type check to ensure that the type is correct.
 */
export type Nominal<K, T> = K & { __brand: T }
