declare module '@uidotdev/usehooks'

type Subset<T, S extends T> = S
type Path = string
type ID = string | number
type KeyOf<T> = T[keyof T]
type Nil = undefined | null
type Optional<T> = T | undefined
type Maybe<T> = T | Nil
type Callback = () => void
type Primitive = string | number | boolean
