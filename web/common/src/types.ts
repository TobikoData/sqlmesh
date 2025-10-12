export declare const __brand: unique symbol
export type Brand<B> = { [__brand]: B }

/**
 * Branded is a type that adds a brand to a type. It is a type that is used to
 * ensure that the type is unique and that it is not possible to mix up types
 * with the same brand.
 *
 * @example
 *
 * type UserId = Branded<string, 'UserId'>
 * type UserName = Branded<string, 'UserName'>
 *
 * const userId = '123' as UserId
 * const userName = 'John Doe' as UserName
 *
 * userId == userName -> compile error
 */
export type Branded<T, B> = T & Brand<B>

/**
 * Constraint that only accepts branded string types
 */
export type BrandedString = string & Brand<string>

/**
 * BrandedRecord is a type that creates a branded Record type with strict key checking.
 * This ensures that Record<BrandedKey1, V> is NOT assignable to Record<BrandedKey2, V>
 *
 * @example
 * type ModelFQN = Branded<string, 'ModelFQN'>
 * type ModelName = Branded<string, 'ModelName'>
 *
 * type FQNMap = BrandedRecord<ModelFQN, string>
 * type NameMap = BrandedRecord<ModelName, string>
 *
 * const fqnMap: FQNMap = {}
 * const nameMap: NameMap = fqnMap // TypeScript error!
 */
export type BrandedRecord<K extends BrandedString, V> = Record<K, V> & {
  readonly __recordKeyBrand: K
}

export type Callback<T = unknown> = (data?: T) => void

export type Size = '2xs' | 'xs' | 's' | 'm' | 'l' | 'xl' | '2xl'
export type HeadlineLevel = 1 | 2 | 3 | 4 | 5 | 6
export type Side = 'left' | 'right' | 'both'
export type LayoutDirection = 'vertical' | 'horizontal' | 'both'
export type Shape = 'square' | 'round' | 'pill'
export type Position =
  | 'top'
  | 'right'
  | 'bottom'
  | 'left'
  | 'center'
  | 'start'
  | 'end'
