import { create } from "zustand";
import { ModelFile } from "../models";

interface FileTreeStore {
  openedFiles: Map<ID, ModelFile>
  activeFileId: ID
  setActiveFileId: (activeFileId: ID) => void
  setOpenedFiles: (files: Map<ID, ModelFile>) => void
  selectFile: (file: ModelFile) => void
}

const file = new ModelFile()

export const useStoreFileTree = create<FileTreeStore>((set, get) => ({
  openedFiles: new Map([[file.id, file]]),
  activeFileId: file.id,
  setActiveFileId: (activeFileId: ID) => set(() => ({ activeFileId })),
  setOpenedFiles: (files: Map<ID, ModelFile>) => set(() => ({ openedFiles: new Map(files) })),
  selectFile: (file: ModelFile) => {
    if (file == null) return
    
    const s = get()

    s.openedFiles.set(file.id, file)

    s.setActiveFileId(file.id)
  } 
}))