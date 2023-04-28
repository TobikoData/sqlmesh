import {
  type Model,
  type ColumnLineageApiLineageModelNameColumnNameGet200,
} from '@api/client'
import { uid, isObjectEmpty } from '@utils/index'
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
  columns?: Record<
    string,
    {
      source?: string
      models?: Record<string, string[]>
    }
  >
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
  previewLineage?: Record<string, Lineage>
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
  setPreviewLineage: (
    models: Map<string, Model>,
    previewLineage?: Record<string, Lineage>,
    columns?: ColumnLineageApiLineageModelNameColumnNameGet200,
  ) => void
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
  previewLineage: undefined,
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
  setPreviewLineage(models, lineage, columns) {
    const previewLineage = structuredClone(lineage)

    if (columns != null && previewLineage != null) {
      mergeLineageWithColumns(models, previewLineage, columns)
    }

    set(() => ({ previewLineage }))
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

function getStoredTabsIds(): ID[] {
  return getStoredTabs()?.ids ?? []
}

// TODO: use better merge
function mergeLineageWithColumns(
  models: Map<string, Model>,
  lineage: Record<string, Lineage>,
  columns: ColumnLineageApiLineageModelNameColumnNameGet200,
): Record<string, Lineage> {
  for (const model in columns) {
    const lineageModel = lineage[model]
    const columnsModel = columns[model]

    if (lineageModel == null || columnsModel == null) continue

    if (lineageModel.columns == null) {
      lineageModel.columns = {}
    }

    for (const columnName in columnsModel) {
      const columnsModelColumn = columnsModel[columnName]

      if (columnsModelColumn == null) continue

      const lineageModelColumn = lineageModel.columns[columnName] ?? {}

      lineageModelColumn.source = columnsModelColumn.source
      lineageModelColumn.models = {}

      lineageModel.columns[columnName] = lineageModelColumn

      if (isObjectEmpty(columnsModelColumn.models)) continue

      for (const columnModel in columnsModelColumn.models) {
        const columnsModelColumnModel = columnsModelColumn.models[columnModel]

        if (columnsModelColumnModel == null) continue

        const lineageModelColumnModel = lineageModelColumn.models[columnModel]

        if (lineageModelColumnModel == null) {
          lineageModelColumn.models[columnModel] = columnsModelColumnModel
        } else {
          lineageModelColumn.models[columnModel] = Array.from(
            new Set(lineageModelColumnModel.concat(columnsModelColumnModel)),
          )
        }
      }
    }
  }

  for (const modelName in lineage) {
    const model = models.get(modelName)
    const modelLineage = lineage[modelName]

    if (model == null || modelLineage == null) {
      delete lineage[modelName]

      continue
    }

    if (modelLineage.columns == null) continue

    if (model.columns == null) {
      delete modelLineage.columns

      continue
    }

    for (const columnName in modelLineage.columns) {
      const found = model.columns.find(c => c.name === columnName)

      if (found == null) {
        delete modelLineage.columns[columnName]

        continue
      }
    }
  }

  return lineage
}
