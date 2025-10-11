declare const __brand: unique symbol
type Brand<B> = { [__brand]: B }

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
