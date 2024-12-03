declare module '@uidotdev/usehooks'
declare module 'https://cdn.jsdelivr.net/pyodide/v0.23.2/full/pyodide.js'
declare module '*?worker&inline' {
  const InlineWorkerFactory: () => Worker
  export default InlineWorkerFactory
}

type Subset<T, S extends T> = S
type Path = string
type ID = string | number
type KeyOf<T> = T[keyof T]
type Nil = undefined | null
type Optional<T> = T | undefined
type Maybe<T> = T | Nil
type Callback = () => void
type Primitive = string | number | boolean
