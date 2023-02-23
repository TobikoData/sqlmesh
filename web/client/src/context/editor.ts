import { create } from 'zustand'

interface EditorStore {
  tabQueryPreviewContent?: string
  tabTableContent?: any[]
  tabTerminalContent?: string
  setTabQueryPreviewContent: (tabQueryPreviewContent?: string) => void
  setTabTableContent: (tabTableContent?: any[]) => void
  setTabTerminalContent: (tabTerminalContent?: string) => void
}

export const useStoreEditor = create<EditorStore>((set, get) => ({
  tabQueryPreviewContent: undefined,
  tabTableContent: undefined,
  tabTerminalContent: undefined,
  setTabQueryPreviewContent: (tabQueryPreviewContent?: string) => {
    set(() => ({ tabQueryPreviewContent }))
  },
  setTabTableContent: (tabTableContent?: any[]) => {
    set(() => ({ tabTableContent }))
  },
  setTabTerminalContent: (tabTerminalContent?: string) => {
    set(() => ({ tabTerminalContent }))
  },
}))
