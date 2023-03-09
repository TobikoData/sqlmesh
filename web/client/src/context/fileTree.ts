import { create } from 'zustand'
import useLocalStorage from '~/hooks/useLocalStorage'
import { isArrayNotEmpty, isFalse } from '~/utils'
import { ModelFile } from '../models'

interface FileTreeStore {
  files: Map<ID, ModelFile>
  activeFile: ModelFile
  openedFiles: Set<ModelFile>
  setOpenedFiles: (files: Set<ModelFile>) => void
  selectFile: (file: ModelFile) => void
  getNextOpenedFile: () => ModelFile
  setFiles: (files: ModelFile[]) => void
}

const initialFile = new ModelFile()

const [getOpenedFilesIds, setOpenedFilesIds] = useLocalStorage<{ ids: ID[] }>(
  'openedFiles',
)

export const useStoreFileTree = create<FileTreeStore>((set, get) => ({
  files: new Map(),
  activeFile: initialFile,
  openedFiles: new Set([initialFile]),
  setFiles(files: ModelFile[]) {
    set(() => {
      const openedFilesIds = getOpenedFilesIds()?.ids ?? []
      const openedFiles: Set<ModelFile> = new Set([initialFile])
      const output = new Map()

      if (isArrayNotEmpty(openedFilesIds)) {
        files.forEach(file => {
          if (openedFilesIds.includes(file.id)) {
            openedFiles.add(file)
          }

          output.set(file.id, file)
        })
      }

      return {
        files: output,
        openedFiles,
      }
    })
  },
  setOpenedFiles(files: Set<ModelFile>) {
    set(() => {
      const openedFiles = new Set(files)

      setOpenedFilesIds({
        ids: Array.from(openedFiles.values())
          .filter(file => isFalse(file.isLocal))
          .map(file => file.id),
      })

      return { openedFiles }
    })
  },
  getNextOpenedFile() {
    return get().openedFiles.values().next().value
  },
  selectFile(file: ModelFile) {
    const s = get()

    if (isFalse(s.openedFiles.has(file))) {
      s.openedFiles.add(file)
    }

    set(() => ({
      activeFileId: file.id,
      activeFile: file,
    }))
  },
}))
