import { type FileContent, type LineageColumn } from '@api/client'
import { type TableColumn, type TableRow } from '@components/table/help'
import { isFalse, isNil, isNotNil, uid } from '@utils/index'
import { create } from 'zustand'
import useLocalStorage from '~/hooks/useLocalStorage'
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

export interface StoredTab {
  id?: ID
  content?: FileContent
}

interface EditorStore {
  storedTabId?: ID
  storedTabs: StoredTab[]
  tabs: Map<ModelFile, EditorTab>
  tab?: EditorTab
  engine: Worker
  dialects: Dialect[]
  previewQuery?: string
  previewTable?: [TableColumn[], TableRow[]]
  previewDiff?: any
  direction: 'vertical' | 'horizontal'
  setDirection: (direction: 'vertical' | 'horizontal') => void
  selectTab: (tab?: EditorTab) => void
  replaceTab: (from: EditorTab, to: EditorTab) => void
  updateStoredTabsIds: () => void
  inTabs: (file: ModelFile) => boolean
  addTab: (tab: EditorTab) => void
  addTabs: (tabs: EditorTab[]) => void
  closeTab: (file: ModelFile) => void
  createTab: (file?: ModelFile) => EditorTab
  setDialects: (dialects: Dialect[]) => void
  refreshTab: (tab?: EditorTab) => void
  setPreviewQuery: (previewQuery?: string) => void
  setPreviewTable: (previewTable?: [TableColumn[], TableRow[]]) => void
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
  dialect?: string
  dialectOptions?: {
    keywords: string
    types: string
  }
  preview?: EditorPreview
  el?: HTMLElement
}

const [getStoredTabs, setStoredTabs] = useLocalStorage<{
  tabs: StoredTab[]
  id?: ID
}>('tabs')

const { tabs: storedTabs = [], id: storedTabId } = getStoredTabs() ?? {}
const initialFile = createLocalFile()
const initialTab: EditorTab = createTab(initialFile)
const initialTabs = new Map(
  storedTabs.length > 0 && isNotNil(storedTabId)
    ? []
    : [[initialFile, initialTab]],
)

export const useStoreEditor = create<EditorStore>((set, get) => ({
  storedTabs,
  storedTabId,
  tab: initialTab,
  tabs: initialTabs,
  engine: sqlglotWorker,
  dialects: [],
  previewQuery: undefined,
  previewTable: undefined,
  previewDiff: undefined,
  direction: 'vertical',
  inTabs(file) {
    return get().tabs.has(file)
  },
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

    if (isNil(s.tab)) {
      setStoredTabs({
        id: undefined,
        tabs: [],
      })

      set(() => ({
        storedTabId: undefined,
        storedTabs: [],
      }))

      return
    }

    const tabs: StoredTab[] = []

    for (const tab of s.tabs.values()) {
      if (isFalse(tab.file.isChanged) && tab.file.isLocal) continue

      tabs.push({
        id: tab.file.id,
        content: tab.file.isChanged ? tab.file.content : undefined,
      })
    }

    const id =
      s.tab.file.isChanged || s.tab.file.isRemote ? s.tab.file.id : undefined

    setStoredTabs({
      id,
      tabs,
    })

    set(() => ({
      storedTabId: id,
      storedTabs: tabs,
    }))
  },
  refreshTab(tab) {
    if (isNil(tab)) return

    get().selectTab({ ...tab })
  },
  setDialects(dialects) {
    set(() => ({
      dialects,
    }))
  },
  selectTab(tab) {
    const s = get()

    set(() => ({ tab }))

    s.updateStoredTabsIds()
  },
  addTabs(tabs) {
    const s = get()

    for (const tab of tabs) {
      s.tabs.set(tab.file, tab)
    }

    set(() => ({
      tabs: new Map(s.tabs),
    }))
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

    file.removeChanges()

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
  }
}

export function createLocalFile(id?: ID): ModelFile {
  return new ModelFile({
    id,
    name: '',
    path: '',
    content:
      '-- Create arbitrary SQL queries\n-- and execute them against different environments\n\n',
  })
}
