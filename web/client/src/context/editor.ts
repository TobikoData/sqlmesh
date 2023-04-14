import { create } from 'zustand'
import useLocalStorage from '~/hooks/useLocalStorage'
import { ModelFile } from '~/models'
import { sqlglotWorker } from '~/workers'

export interface Dialect {
  dialect_title: string
  dialect_name: string
}

interface EditorStore {
  storedTabsIds: ID[]
  tabs: Map<ModelFile, EditorTab>
  tab?: EditorTab
  engine: Worker
  dialects: Dialect[]
  previewQuery?: string
  previewTable?: any[]
  previewConsole?: string
  selectTab: (tab?: EditorTab) => void
  updateStoredTabsIds: () => void
  addTab: (tab: EditorTab) => void
  closeTab: (file: ModelFile) => void
  createTab: (file?: ModelFile) => EditorTab
  setDialects: (dialects: Dialect[]) => void
  refreshTab: () => void
  setPreviewQuery: (previewQuery?: string) => void
  setPreviewTable: (previewTable?: any[]) => void
  setPreviewConsole: (previewConsole?: string) => void
}

interface EditorPreview<TTable = any> {
  queryPreview?: string
  table?: TTable[]
  terminal?: string
}

export interface EditorTab {
  file: ModelFile
  isValid: boolean
  isSaved: boolean
  dialect?: string
  dialectOptions?: {
    keywords: string
    types: string
  }
  preview?: EditorPreview
}

const [getStoredTabs, setStoredTabs] = useLocalStorage<{ ids: ID[] }>('tabs')

const initialFile = createLocalFile()
const initialTab: EditorTab = createTab(initialFile)
const initialTabs = new Map([[initialFile, initialTab]])

export const useStoreEditor = create<EditorStore>((set, get) => ({
  storedTabsIds: getStoredTabsIds(),
  tab: initialTab,
  tabs: initialTabs,
  engine: sqlglotWorker,
  dialects: [],
  previewQuery: undefined,
  previewTable: undefined,
  previewConsole: undefined,
  updateStoredTabsIds() {
    setStoredTabs({
      ids: Array.from(get().tabs.values())
        .filter(tab => tab.file.isRemote)
        .map(tab => tab.file.id),
    })

    set(() => ({
      storedTabsIds: getStoredTabsIds(),
    }))
  },
  refreshTab() {
    const tab = get().tab

    tab != null && get().selectTab({ ...tab })
  },
  setDialects(dialects) {
    set(() => ({
      dialects,
    }))
  },
  selectTab(tab) {
    set(() => ({ tab }))

    if (tab == null) return

    get().addTab(tab)
  },
  addTab(tab) {
    const s = get()

    s.tabs.set(tab.file, tab)

    const tabs = new Map(s.tabs)

    set(() => ({
      tabs,
    }))

    s.updateStoredTabsIds()
  },
  closeTab(file) {
    const s = get()

    s.tabs.delete(file)

    if (s.tabs.size === 0) {
      s.selectTab(undefined)
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
}))

function createTab(file: ModelFile = createLocalFile()): EditorTab {
  return {
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

function getStoredTabsIds(): ID[] {
  return getStoredTabs()?.ids ?? []
}
