declare module '@uidotdev/usehooks'
declare module 'https://cdn.jsdelivr.net/pyodide/v0.23.2/full/pyodide.js'
declare module '*?worker&inline' {
  const InlineWorkerFactory: () => Worker
  export default InlineWorkerFactory
declare module '~/utils/tbk-components' {
  export const Badge: any
  export const SourceList: any
  export const SourceListItem: any
  export const SourceListSection: any
  export const ModelName: any
  export const ResizeObserver: any
}
declare module '~/utils/additional-components' {
  export const TBKBadge: any
  export const TBKSourceList: any
  export const TBKSourceListItem: any
  export const TBKSourceListSection: any
  export const TBKModelName: any
  export const TBKResizeObserver: any
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
