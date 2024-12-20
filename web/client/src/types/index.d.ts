declare module '@uidotdev/usehooks'
declare module 'https://cdn.jsdelivr.net/pyodide/v0.23.2/full/pyodide.js'
declare module '*?worker&inline' {
  const InlineWorkerFactory: () => Worker
  export default InlineWorkerFactory
}
declare module '~/utils/tbk-components' {
  export const Badge: any
  export const SourceList: any
  export const SourceListItem: any
  export const SourceListSection: any
  export const ModelName: any
  export const ResizeObserver: any
  export const Metadata: any
  export const MetadataItem: any
  export const MetadataSection: any
  export const Scroll: any
  export const SplitPane: any
  export const Details: any
  export const Tabs: any
  export const Tab: any
  export const TabPanel: any
  export const Datetime: any
  export const TextBlock: any
  export const Information: any
  export const Icon: any
  export const Button: any
  export const Tooltip: any
}
declare module '~/utils/additional-components' {
  export const TBKBadge: any
  export const TBKSourceList: any
  export const TBKSourceListItem: any
  export const TBKSourceListSection: any
  export const TBKModelName: any
  export const TBKResizeObserver: any
  export const TBKMetadata: any
  export const TBKMetadataItem: any
  export const TBKMetadataSection: any
  export const TBKScroll: any
  export const TBKSplitPane: any
  export const TBKDetails: any
  export const TBKTabs: any
  export const TBKTab: any
  export const TBKTabPanel: any
  export const TBKDatetime: any
  export const TBKTextBlock: any
  export const TBKInformation: any
  export const TBKIcon: any
  export const TBKButton: any
  export const TBKTooltip: any
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
