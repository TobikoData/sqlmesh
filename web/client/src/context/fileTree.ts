import { create } from 'zustand'
import { ModelDirectory, type ModelFile } from '../models'
import { type Directory } from '~/api/client'

interface FileTreeStore {
  project?: ModelDirectory
  files: Map<ID, ModelFile>
  selectedFile?: ModelFile
  selectFile: (selectedFile: ModelFile) => void
  setFiles: (files: ModelFile[]) => void
  refreshProject: () => void
  setProject: (project?: Directory) => void
}

export const useStoreFileTree = create<FileTreeStore>((set, get) => ({
  project: undefined,
  files: new Map(),
  selectedFile: undefined,
  setProject(project) {
    set(() => ({
      project: new ModelDirectory(project),
    }))
  },
  setFiles(files) {
    set(() => ({
      files: files.reduce((acc, file) => acc.set(file.id, file), new Map()),
    }))
  },
  selectFile(selectedFile) {
    set(() => ({
      selectedFile,
    }))
  },
  refreshProject() {
    get().setProject(get().project)
  },
}))
