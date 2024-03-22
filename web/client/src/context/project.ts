import { create } from 'zustand'
import { ModelDirectory, type ModelFile } from '../models'
import { ModelArtifact } from '@models/artifact'

export interface Tests {
  ok: boolean
  time: number
  title: string
  total: string
  successful: number
  failures: number
  errors: number
  dialect: string
  traceback: string
  details: Array<{ message: string; details: string }>
  message?: string
}

interface ProjectStore {
  activeRange: ModelArtifact[]
  setActiveRange: (activeRange?: ModelArtifact[]) => void
  project: ModelDirectory
  setProject: (project?: ModelDirectory) => void
  tests?: Tests
  setTests: (tests?: Tests) => void
  files: Map<ID, ModelFile>
  setFiles: (files: ModelFile[]) => void
  selectedFile?: ModelFile
  setSelectedFile: (selectedFile?: ModelFile) => void
  findArtifactByPath: (path: string) => ModelArtifact | undefined
  refreshFiles: () => void
  inActiveRange: (artifact: ModelArtifact) => boolean
}

export const useStoreProject = create<ProjectStore>((set, get) => ({
  tests: undefined,
  project: new ModelDirectory(),
  selectedFile: undefined,
  files: new Map(),
  activeRange: [],
  setTests(tests) {
    set(() => ({
      tests,
    }))
  },
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

    return ModelArtifact.findArtifactByPath(s.project, path)
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
