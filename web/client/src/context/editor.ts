import { create } from 'zustand'
import useLocalStorage from '~/hooks/useLocalStorage'
import { ModelFile } from '~/models'
import { isTrue } from '~/utils'
import { sqlglotWorker } from '~/workers'

export interface Dialect {
  dialect_title: string
  dialect_name: string
}

interface EditorStore {
  storedTabsIds: ID[]
  tabs: Map<ID, EditorTab>
  tab: EditorTab
  engine: Worker
  dialects: Dialect[]
  previewQuery?: string
  previewTable?: any[]
  previewConsole?: string
  selectTab: (tab: EditorTab) => void
  addTab: (tab: EditorTab) => void
  closeTab: (id: ID) => void
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
  isInitial: boolean
  dialect?: string
  preview?: EditorPreview
}

const [getStoredTabs, setStoredTabs] = useLocalStorage<{ ids: ID[] }>('tabs')

const initialFile = createLocalFile()
const initialTab: EditorTab = createTab(initialFile, true)
const initialTabs = new Map([[initialFile.id, initialTab]])

export const useStoreEditor = create<EditorStore>((set, get) => ({
  storedTabsIds: getStoredTabsIds(),
  tab: initialTab,
  tabs: initialTabs,
  engine: sqlglotWorker,
  dialects: [],
  refreshTab() {
    get().selectTab({ ...get().tab })
  },
  setDialects(dialects) {
    set(() => ({
      dialects,
    }))
  },
  selectTab(tab) {
    set(() => ({ tab }))

    get().addTab(tab)
  },
  addTab(tab) {
    const tabs = new Map([...get().tabs, [tab.file.id, tab]])

    setStoredTabs({
      ids: Array.from(tabs.values())
        .filter(tab => tab.file.isRemote)
        .map(tab => tab.file.id),
    })

    set(() => ({
      tabsIds: getStoredTabsIds(),
      tabs,
    }))
  },
  closeTab(id) {
    const s = get()

    if (isTrue(s.tabs.get(id)?.isInitial)) return

    const tabs = Array.from(get().tabs.values())
    const indexAt = tabs.findIndex(tab => tab.file.id === id)

    s.tabs.delete(id)

    if (id === s.tab.file.id) {
      s.selectTab(tabs.at(indexAt - 1) as EditorTab)
    }

    set(() => ({
      tabs: new Map(s.tabs),
    }))
  },
  createTab,
  previewQuery: undefined,
  previewTable: undefined,
  previewConsole: undefined,
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

function createTab(
  file: ModelFile = createLocalFile(),
  isInitial = false,
): EditorTab {
  return {
    file,
    isInitial,
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
