import { create } from 'zustand'
import useLocalStorage from '~/hooks/useLocalStorage'
import { isArrayNotEmpty, isFalse } from '~/utils'
import { ModelFile } from '../models'

interface FileTreeStore {
  files: ModelFile[]
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
  files: [],
  activeFile: initialFile,
  openedFiles: new Set([initialFile]),
  setFiles(files: ModelFile[]) {
    set(() => {
      const openedFilesIds = getOpenedFilesIds()?.ids ?? []
      const openedFiles = new Set<ModelFile>([initialFile])

      if (isArrayNotEmpty(openedFilesIds)) {
        files.forEach(file => {
          if (openedFilesIds.includes(file.id)) {
            openedFiles.add(file)
          }
        })
      }

      return {
        files,
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

