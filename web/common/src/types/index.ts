export declare const __brand: unique symbol
export type Brand<B> = { [__brand]: B }
export type Branded<T, B> = T & Brand<B>

export type Nullable<T> = T | null
export type Nil = undefined | null
export type Optional<T> = T | undefined
export type Maybe<T> = T | Nil
export type EmptyString = ''
export type TimerID = ReturnType<typeof setTimeout>
