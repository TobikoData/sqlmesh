import { create } from 'zustand'
import { ModelDirectory, type ModelFile } from '../models'

interface ProjectStore {
  project: ModelDirectory
  setProject: (project?: ModelDirectory) => void
  files: Map<ID, ModelFile>
  setFiles: (files: ModelFile[]) => void
  selectedFile?: ModelFile
  setSelectedFile: (selectedFile?: ModelFile) => void
}

export const useStoreProject = create<ProjectStore>((set, get) => ({
  selectedFile: undefined,
  project: new ModelDirectory(),
  files: new Map(),
  setProject(project) {
    set(() => ({
      project,
    }))
  },
  setFiles(files) {
    set(() => ({
      files: files.reduce((acc, file) => acc.set(file.id, file), new Map()),
    }))
  },
  setSelectedFile(selectedFile) {
    set(() => ({
      selectedFile,
    }))
  },
}))
