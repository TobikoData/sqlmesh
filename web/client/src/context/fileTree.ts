import { create } from 'zustand'
import { isFalse } from '~/utils'
import { ModelFile } from '../models'

interface FileTreeStore {
  activeFile: ModelFile
  openedFiles: Set<ModelFile>
  setOpenedFiles: (files: Set<ModelFile>) => void
  selectFile: (file: ModelFile) => void
  getNextOpenedFile: () => ModelFile
}

const initialFile = new ModelFile()

export const useStoreFileTree = create<FileTreeStore>((set, get) => ({
  activeFile: initialFile,
  openedFiles: new Set([initialFile]),
  setOpenedFiles(files: Set<ModelFile>) {
    set(() => ({ openedFiles: new Set(files) }))
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
