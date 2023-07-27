import { create } from 'zustand'
import { ModelDirectory, type ModelFile } from '../models'
import { type ModelArtifact } from '@models/artifact'

interface ProjectStore {
  activeRange: ModelArtifact[]
  setActiveRange: (activeRange?: ModelArtifact[]) => void
  project: ModelDirectory
  setProject: (project?: ModelDirectory) => void
  files: Map<ID, ModelFile>
  setFiles: (files: ModelFile[]) => void
  selectedFile?: ModelFile
  setSelectedFile: (selectedFile?: ModelFile) => void
  findArtifactByPath: (path: string) => ModelArtifact | undefined
  findParentByPath: (path: string) => ModelDirectory | undefined
  refreshFiles: () => void
  inActiveRange: (artifact: ModelArtifact) => boolean
}

export const useStoreProject = create<ProjectStore>((set, get) => ({
  project: new ModelDirectory(),
  selectedFile: undefined,
  files: new Map(),
  activeRange: [],
  setActiveRange(activeRange) {
    const s = get()

    set(() => ({
      activeRange: s.project.allVisibleArtifacts.filter(artifact =>
        inActiveRange(artifact, activeRange ?? s.activeRange),
      ),
    }))
  },
  inActiveRange(artifact) {
    const s = get()

    return inActiveRange(artifact, s.activeRange)
  },
  setProject(project) {
    set(() => ({
      project,
    }))
  },
  setFiles(files) {
    const s = get()

    s.files.clear()

    set(() => ({
      files: new Map(
        files.reduce((acc, file) => acc.set(file.id, file), s.files),
      ),
    }))
  },
  setSelectedFile(selectedFile) {
    set(() => ({
      selectedFile,
    }))
  },
  findArtifactByPath(path) {
    const s = get()

    return ModelDirectory.findArtifactByPath(s.project, path)
  },
  findParentByPath(path) {
    const s = get()

    return ModelDirectory.findParentByPath(s.project, path)
  },
  refreshFiles() {
    const s = get()

    s.setFiles(s.project.allFiles)
  },
}))

function inActiveRange(
  artifact: ModelArtifact,
  activeRange: ModelArtifact[],
): boolean {
  return activeRange.includes(artifact)
}
