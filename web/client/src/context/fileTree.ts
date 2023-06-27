import { create } from 'zustand'
import { ModelDirectory, type ModelFile } from '../models'
import { type Directory } from '~/api/client'
import { type ModelArtifact } from '@models/artifact'

interface FileTreeStore {
  project?: ModelDirectory
  files: Map<ID, ModelFile>
  selected?: ModelArtifact
  activeRange: Set<ModelArtifact>
  setSelected: (selected?: ModelArtifact) => void
  setFiles: (files: ModelFile[]) => void
  setProject: (project?: Directory) => void
  setActiveRange: (activeRange: Set<ModelArtifact>) => void
  selectArtifactsInRange: (to: ModelArtifact) => void
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
  setSelected(selected) {
    set(() => ({
      selected,
      activeRange: selected == null ? new Set() : new Set([selected]),
    }))
  },
  setActiveRange(activeRange) {
    const s = get()

    set(() => ({
      activeRange: new Set(
        (s.project?.allArtifacts ?? []).filter(artifact =>
          activeRange.has(artifact),
        ),
      ),
    }))
  },
  selectArtifactsInRange(to) {
    const s = get()
    const acitveAtrifacts = Array.from(s.activeRange)
    const first = acitveAtrifacts[0]
    const last = acitveAtrifacts[s.activeRange.size - 1]
    const artifacts = s.project?.allArtifacts ?? []
    const indexTo = artifacts.indexOf(to)
    const indexFirst = first == null ? 0 : artifacts.indexOf(first)
    const indexLast =
      last == null ? s.activeRange.size - 1 : artifacts.indexOf(last)

    let indexStart = indexTo > indexFirst ? indexFirst : indexTo
    let indexEnd =
      indexTo > indexLast || (indexTo > indexFirst && indexTo < indexLast)
        ? indexTo
        : indexLast

    s.setActiveRange(new Set(artifacts.slice(indexStart, indexEnd + 1)))
  },
}))
