import { create } from 'zustand'
import useLocalStorage from '~/hooks/useLocalStorage'
import { ModelFile } from '~/models'
import { sqlglotWorker } from '~/workers'

export interface Dialect {
  dialect_title: string
  dialect_name: string
}

interface EditorStore {
  tabsIds: ID[]
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
  getNextTab: () => EditorTab
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

const [getTabsIds, setTabsIds] = useLocalStorage<{ ids: ID[] }>('tabs')

const initialFile = new ModelFile()
const initialTab: EditorTab = createTab(initialFile, true)
const initialTabs = new Map([[initialFile.id, initialTab]])

export const useStoreEditor = create<EditorStore>((set, get) => ({
  tabsIds: getTabsIds()?.ids ?? [],
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

    setTabsIds({
      ids: Array.from(tabs.values())
        .filter(tab => tab.file.isRemote)
        .map(tab => tab.file.id),
    })

    set(() => ({
      tabsIds: getTabsIds()?.ids ?? [],
      tabs,
    }))
  },
  closeTab(id) {
    const s = get()

    if (id === s.tab.file.id) {
      s.selectTab(s.getNextTab())
    }

    get().tabs.delete(id)

    set(() => ({
      tabs: new Map(get().tabs),
    }))
  },
  getNextTab() {
    return get().tabs.values().next().value
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
  file: ModelFile = new ModelFile(),
  isInitial = false,
): EditorTab {
  return {
    file,
    isInitial,
    isValid: true,
    isSaved: true,
  }
}
