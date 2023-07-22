import { create } from 'zustand'
import { ModelDirectory, type ModelFile } from '../models'
import { type ModelArtifact } from '@models/artifact'
import { isFalse, isNil, isNotNil } from '@utils/index'

interface ProjectStore {
  activeRange: ModelArtifact[]
  setActiveRange: (activeRange?: ModelArtifact[]) => void
  project: ModelDirectory
  setProject: (project?: ModelDirectory) => void
  files: Map<ID, ModelFile>
  setFiles: (files: ModelFile[]) => void
  selectedFile?: ModelFile
  setSelectedFile: (selectedFile?: ModelFile) => void
  findAtrifactByPath: (path: string) => ModelArtifact | undefined
  findParentByPath: (path: string) => ModelDirectory | undefined
  refreshFiles: () => void
  inActiveRange: (artifact: ModelArtifact) => boolean
  isTopGroupInActiveRange: (artifact: ModelArtifact) => boolean
  isBottomGroupInActiveRange: (artifact: ModelArtifact) => boolean
  isMiddleGroupInActiveRange: (artifact: ModelArtifact) => boolean
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
  isTopGroupInActiveRange(artifact) {
    const s = get()

    const index = s.project.allVisibleArtifacts.indexOf(artifact)
    const prev = s.project.allVisibleArtifacts[index - 1]
    const next = s.project.allVisibleArtifacts[index + 1]

    return (
      s.inActiveRange(artifact) &&
      (isNil(prev) || isFalse(s.inActiveRange(prev))) &&
      isNotNil(next) &&
      s.inActiveRange(next)
    )
  },
  isBottomGroupInActiveRange(artifact) {
    const s = get()

    const index = s.project.allVisibleArtifacts.indexOf(artifact)
    const prev = s.project.allVisibleArtifacts[index - 1]
    const next = s.project.allVisibleArtifacts[index + 1]

    return (
      s.inActiveRange(artifact) &&
      (isNil(next) || isFalse(s.inActiveRange(next))) &&
      isNotNil(prev) &&
      s.inActiveRange(prev)
    )
  },
  isMiddleGroupInActiveRange(artifact) {
    const s = get()

    const index = s.project.allVisibleArtifacts.indexOf(artifact)
    const prev = s.project.allVisibleArtifacts[index - 1]
    const next = s.project.allVisibleArtifacts[index + 1]

    return (
      s.inActiveRange(artifact) &&
      isNotNil(next) &&
      s.inActiveRange(next) &&
      isNotNil(prev) &&
      s.inActiveRange(prev)
    )
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
  findAtrifactByPath(path) {
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

// function inActiveRangeParent(artifact: ModelArtifact, activeRange: ModelArtifact[]): boolean {
//   let parent = artifact.parent

//   while (isNotNil(parent)) {
//     if (inActiveRange(parent, activeRange)) {
//       return true
//     }

//     parent = parent.parent
//   }

//   return false
// }
