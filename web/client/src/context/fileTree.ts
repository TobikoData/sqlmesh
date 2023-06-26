import { create } from 'zustand'
import { ModelDirectory, type ModelFile } from '../models'
import { type Directory } from '~/api/client'
import { type ModelArtifact } from '@models/artifact'

interface FileTreeStore {
  project?: ModelDirectory
  files: Map<ID, ModelFile>
  selected?: ModelArtifact
  activeRange: Set<ModelArtifact>
  selectFile: (selected?: ModelArtifact) => void
  setFiles: (files: ModelFile[]) => void
  setProject: (project?: Directory) => void
  setActiveRange: (activeRange: Set<ModelArtifact>) => void
  refreshProject: () => void
}

export const useStoreFileExplorer = create<FileTreeStore>((set, get) => ({
  project: undefined,
  files: new Map(),
  selected: undefined,
  activeRange: new Set(),
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
  selectFile(selected) {
    set(() => ({
      selected,
      activeRange: selected == null ? new Set() : new Set([selected]),
    }))
  },
  setActiveRange(activeRange) {
    set(() => ({
      activeRange: new Set(activeRange),
    }))
  },
  refreshProject() {
    get().setProject(get().project)
  },
}))
