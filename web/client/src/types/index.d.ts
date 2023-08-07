declare module '@uidotdev/usehooks'

type Subset<T, S extends T> = S
type Path = string
type ID = string | number
type KeyOf<T> = T[keyof T]
type Optional<T> = T | undefined | null
type Nullable<T> = T | null
type Maybe<T> = Optional<T> | Nullable<T>
type Callback = () => void
