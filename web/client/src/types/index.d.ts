declare module '@uidotdev/usehooks'

type Subset<T, S extends T> = S
type Path = string
type ID = string | number
type KeyOf<T> = T[keyof T]
type Optional<T> = T | undefined
type Callback = () => void
