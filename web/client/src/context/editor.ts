import { type LineageColumn } from '@api/client'
import { type TableColumn, type TableRow } from '@components/table/help'
import { uid } from '@utils/index'
import { create } from 'zustand'
import useLocalStorage from '~/hooks/useLocalStorage'
import { type ErrorKey, type ErrorIDE } from '~/library/pages/ide/context'
import { ModelFile } from '~/models'
import { sqlglotWorker } from '~/workers'

export interface Dialect {
  dialect_title: string
  dialect_name: string
}

export interface Lineage {
  models: string[]
  columns?: Record<string, LineageColumn>
}

interface EditorStore {
  storedTabsId?: ID
  storedTabsIds: ID[]
  tabs: Map<ModelFile, EditorTab>
  tab?: EditorTab
  engine: Worker
  dialects: Dialect[]
  previewQuery?: string
  previewTable?: [TableColumn[], TableRow[]]
  previewConsole?: [ErrorKey, ErrorIDE]
  previewDiff?: any
  direction: 'vertical' | 'horizontal'
  setDirection: (direction: 'vertical' | 'horizontal') => void
  selectTab: (tab?: EditorTab) => void
  replaceTab: (from: EditorTab, to: EditorTab) => void
  updateStoredTabsIds: () => void
  addTab: (tab: EditorTab) => void
  closeTab: (file: ModelFile) => void
  createTab: (file?: ModelFile) => EditorTab
  setDialects: (dialects: Dialect[]) => void
  refreshTab: () => void
  setPreviewQuery: (previewQuery?: string) => void
  setPreviewTable: (previewTable?: [TableColumn[], TableRow[]]) => void
  setPreviewConsole: (previewConsole?: [ErrorKey, ErrorIDE]) => void
  setPreviewDiff: (previewDiff?: any) => void
}

interface EditorPreview<TTable = any> {
  queryPreview?: string
  table?: TTable[]
  terminal?: string
  lineage?: Record<string, Lineage>
}

export interface EditorTab {
  id: string
  file: ModelFile
  isValid: boolean
  isSaved: boolean
  dialect?: string
  dialectOptions?: {
    keywords: string
    types: string
  }
  preview?: EditorPreview
  el?: HTMLElement
}

const [getStoredTabs, setStoredTabs] = useLocalStorage<{ ids: ID[]; id: ID }>(
  'tabs',
)

const initialFile = createLocalFile()
const initialTab: EditorTab = createTab(initialFile)
const initialTabs = new Map([[initialFile, initialTab]])

export const useStoreEditor = create<EditorStore>((set, get) => ({
  storedTabsIds: getStoredTabs()?.ids ?? [],
  storedTabsId: getStoredTabs()?.id,
  tab: initialTab,
  tabs: initialTabs,
  engine: sqlglotWorker,
  dialects: [],
  previewQuery: undefined,
  previewTable: undefined,
  previewConsole: undefined,
  previewDiff: undefined,
  direction: 'vertical',
  replaceTab(from, to) {
    const s = get()

    const tabs = Array.from(s.tabs.entries())
    const indexAt = tabs.findIndex(([file]) => file === from.file)

    tabs[indexAt] = [to.file, to]

    set(() => ({
      tabs: new Map(tabs),
    }))

    s.updateStoredTabsIds()
  },
  updateStoredTabsIds() {
    const s = get()
    const id = s.tab?.file.id
    const ids = Array.from(get().tabs.values())
      .filter(tab => tab.file.isRemote)
      .map(tab => tab.file.id)

    setStoredTabs({
      id,
      ids,
    })

    set(() => ({
      storedTabsId: id,
      storedTabsIds: ids,
    }))
  },
  refreshTab() {
    const tab = get().tab

    if (tab == null) return

    get().selectTab({ ...tab })
  },
  setDialects(dialects) {
    set(() => ({
      dialects,
    }))
  },
  selectTab(tab) {
    set(() => ({ tab }))
  },
  addTab(tab) {
    const s = get()

    if (s.tabs.has(tab.file)) {
      s.tabs.set(tab.file, tab)

      set(() => ({
        tabs: new Map(s.tabs),
      }))
    } else {
      const tabs = Array.from(s.tabs.entries())
      const indexAt = tabs.findIndex(([f]) => f === s.tab?.file)

      tabs.splice(indexAt < 0 ? tabs.length : indexAt + 1, 0, [tab.file, tab])

      set(() => ({
        tabs: new Map(tabs),
      }))
    }

    s.updateStoredTabsIds()
  },
  closeTab(file) {
    const s = get()

    s.tabs.delete(file)

    if (s.tabs.size === 0) {
      s.selectTab(undefined)
    } else if (s.tabs.size === 1) {
      const tabs = Array.from(s.tabs.values())

      s.selectTab(tabs.at(0) as EditorTab)
    } else if (file.id === s.tab?.file.id) {
      const tabs = Array.from(s.tabs.values())
      const indexAt = tabs.findIndex(tab => tab.file === file)

      s.selectTab(tabs.at(indexAt - 1) as EditorTab)
    }

    set(() => ({
      tabs: new Map(s.tabs),
    }))

    s.updateStoredTabsIds()
  },
  createTab,
  setPreviewQuery(previewQuery) {
    set(() => ({ previewQuery }))
  },
  setPreviewTable(previewTable) {
    set(() => ({ previewTable }))
  },
  setPreviewConsole(previewConsole) {
    set(() => ({ previewConsole }))
  },
  setPreviewDiff(previewDiff) {
    set(() => ({ previewDiff }))
  },
  setDirection(direction) {
    set(() => ({ direction }))
  },
}))

function createTab(file: ModelFile = createLocalFile()): EditorTab {
  return {
    id: uid(),
    file,
    isValid: true,
    isSaved: true,
  }
}

function createLocalFile(): ModelFile {
  return new ModelFile({
    name: '',
    path: '',
    content:
      '-- Create arbitrary SQL queries\n-- and execute them against different environments\n\n',
  })
}
