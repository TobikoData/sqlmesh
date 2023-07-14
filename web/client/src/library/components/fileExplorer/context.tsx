import { createContext, useState, useContext, useEffect } from 'react'
import {
  type Directory,
  deleteDirectoryApiDirectoriesPathDelete,
  deleteFileApiFilesPathDelete,
  writeDirectoryApiDirectoriesPathPost,
  writeFileApiFilesPathPost,
} from '@api/client'
import { useStoreProject } from '@context/project'
import { ModelArtifact } from '@models/artifact'
import { ModelDirectory } from '@models/directory'
import { getAllFilesInDirectory } from './help'
import {
  isArrayNotEmpty,
  isFalse,
  isNotNil,
  isStringEmptyOrNil,
  toUniqueName,
} from '@utils/index'
import { ModelFile } from '@models/file'
import { useStoreEditor } from '@context/editor'
import { type ResponseWithDetail } from '@api/instance'
import { useStoreContext } from '@context/context'

interface FileExplorer {
  activeRange: Set<ModelArtifact>
  setActiveRange: (activeRange: Set<ModelArtifact>) => void
  selectArtifactsInRange: (to: ModelArtifact) => void
  createDirectory: (parent: ModelDirectory) => void
  createFile: (parent: ModelDirectory) => void
  renameArtifact: (artifact: ModelArtifact, newName?: string) => void
  removeArtifacts: (artifacts: Set<ModelArtifact>) => void
  removeArtifactWithConfirmation: (artifact: ModelArtifact) => void
  moveArtifacts: (artifacts: Set<ModelArtifact>, target: ModelDirectory) => void
}

export const FileExplorerContext = createContext<FileExplorer>({
  activeRange: new Set(),
  setActiveRange: () => {},
  selectArtifactsInRange: () => {},
  createDirectory: () => {},
  createFile: () => {},
  renameArtifact: () => {},
  removeArtifacts: () => {},
  removeArtifactWithConfirmation: () => {},
  moveArtifacts: () => {},
})

export default function FileExplorerProvider({
  children,
}: {
  children: React.ReactNode
}): JSX.Element {
  const project = useStoreProject(s => s.project)
  const files = useStoreProject(s => s.files)
  const setSelectedFile = useStoreProject(s => s.setSelectedFile)
  const setFiles = useStoreProject(s => s.setFiles)

  const addConfirmation = useStoreContext(s => s.addConfirmation)

  const tab = useStoreEditor(s => s.tab)
  const closeTab = useStoreEditor(s => s.closeTab)

  const [isLoading, setIsLoading] = useState(false)
  const [activeRange, setActiveRange] = useState(new Set<ModelArtifact>())

  useEffect(() => {
    setSelectedFile(tab?.file)
  }, [tab])

  function selectArtifactsInRange(to: ModelArtifact): void {
    setActiveRange(activeRange => {
      const IDX_FIRST = 0
      const IDX_LAST = activeRange.size - 1
      const artifacts = project.allArtifacts
      const acitveAtrifacts = Array.from(activeRange)
      const first = acitveAtrifacts[IDX_FIRST]
      const last = acitveAtrifacts[IDX_LAST]
      const indexTo = artifacts.indexOf(to)
      const indexFirst = first == null ? IDX_FIRST : artifacts.indexOf(first)
      const indexLast = last == null ? IDX_LAST : artifacts.indexOf(last)

      const indexStart = indexTo > indexFirst ? indexFirst : indexTo
      const indexEnd =
        indexTo > indexLast || (indexTo > indexFirst && indexTo < indexLast)
          ? indexTo
          : indexLast

      return new Set(artifacts.slice(indexStart, indexEnd + 1))
    })
  }

  function createDirectory(parent: ModelDirectory): void {
    if (isLoading) return

    setIsLoading(true)

    const name = toUniqueName('new_directory')

    writeDirectoryApiDirectoriesPathPost(`${parent.path}/${name}`, {})
      .then(created => {
        if (isFalse((created as any).ok)) {
          console.warn([`Directory: ${parent.path}`, (created as any).detail])

          return
        }

        parent.addDirectory(new ModelDirectory(created, parent))
        parent.open()
      })
      .catch(error => {
        // TODO: Show error notification
        console.log(error)
      })
      .finally(() => {
        setIsLoading(false)
      })
  }

  function createFile(parent: ModelDirectory, extension = '.py'): void {
    if (isLoading) return

    setIsLoading(true)

    const name = toUniqueName('new_file', extension)

    writeFileApiFilesPathPost(`${parent.path}/${name}`, { content: '' })
      .then(created => {
        if (isFalse((created as any).ok)) {
          console.warn([`File: ${parent.path}`, (created as any).detail])

          return
        }

        const file = new ModelFile(created, parent)

        parent.addFile(file)
        parent.open()

        setSelectedFile(file)
        setFiles(project?.allFiles ?? [])
      })
      .catch(error => {
        // TODO: Show error notification
        console.log(error)
      })
      .finally(() => {
        setIsLoading(false)
      })
  }

  function renameArtifact(artifact: ModelArtifact, newName?: string): void {
    if (isLoading || isStringEmptyOrNil(newName)) return

    setIsLoading(true)

    const currentName = artifact.name
    const currentPath = artifact.path

    artifact.rename(newName!.trim())

    if (artifact instanceof ModelDirectory) {
      writeDirectoryApiDirectoriesPathPost(currentPath, {
        new_path: artifact.path,
      })
        .catch(error => {
          // TODO: Show error notification
          console.log(error)

          artifact.rename(currentName)
        })
        .finally(() => {
          setIsLoading(false)

          if (tab != null && artifact.hasFile(tab.file)) {
            setSelectedFile(tab.file)
          }

          setFiles(project?.allFiles ?? [])
        })
    }

    if (artifact instanceof ModelFile) {
      writeFileApiFilesPathPost(currentPath, {
        new_path: artifact.path,
      })
        .then(response => {
          artifact.update(response)

          files.set(artifact.path, artifact)
          files.delete(currentPath)
        })
        .catch(error => {
          // TODO: Show error notification
          console.log(error)

          artifact.rename(currentName)
        })
        .finally(() => {
          setIsLoading(false)
          setFiles(project?.allFiles ?? [])
        })
    }
  }

  function removeArtifacts(artifacts: Set<ModelArtifact>): void {
    if (isLoading) return

    setIsLoading(true)

    const list = Array.from(artifacts)
    const promises = list.map(artifact => {
      if (artifact instanceof ModelFile) {
        return deleteFileApiFilesPathDelete(artifact.path)
      }

      return deleteDirectoryApiDirectoriesPathDelete(artifact.path)
    })

    Promise.all(promises)
      .then(resolvedList => {
        resolvedList.forEach((_, index) => {
          const artifact = list[index]

          if (artifact instanceof ModelFile) {
            closeTab(artifact)

            artifact.parent?.removeFile(artifact)
          }

          if (artifact instanceof ModelDirectory) {
            if (artifact.isNotEmpty) {
              const files = getAllFilesInDirectory(artifact)

              files.forEach(file => {
                closeTab(file)
              })
            }

            artifact.parent?.removeDirectory(artifact)
          }
        })

        setActiveRange(new Set())
        setFiles(project?.allFiles ?? [])
      })
      .catch(error => {
        // TODO: Show error notification
        console.log(error)
      })
      .finally(() => {
        setIsLoading(false)
      })
  }

  function removeArtifactWithConfirmation(artifact: ModelArtifact): void {
    if (activeRange.has(artifact)) {
      // User selected multiple including current directory
      // so here we should prompt to delete all selected
      addConfirmation({
        headline: 'Removing Selected Files/Directories',
        description: `Are you sure you want to remove ${activeRange.size} items?`,
        yesText: 'Yes, Remove',
        noText: 'No, Cancel',
        details: Array.from(activeRange).map(artifact => artifact.path),
        action: () => {
          removeArtifacts(activeRange)
        },
      })
    } else {
      addConfirmation({
        headline: `Removing ${
          artifact instanceof ModelDirectory ? 'Directory' : 'File'
        }`,
        description: `Are you sure you want to remove the ${
          artifact instanceof ModelDirectory ? 'directory' : 'file'
        } "${artifact.name}"?`,
        yesText: 'Yes, Remove',
        noText: 'No, Cancel',
        action: () => {
          if (isNotNil(artifact.parent)) {
            removeArtifacts(new Set([artifact]))
          }
        },
      })
    }
  }

  function moveArtifacts(
    artifacts: Set<ModelArtifact>,
    target: ModelDirectory,
    shouldRenameDuplicates = false,
  ): void {
    if (isLoading) return

    setIsLoading(true)

    const moveArtifactCallbacks: Array<() => void> = []
    const promises: Array<
      Promise<
        | (Directory & ResponseWithDetail)
        | typeof writeDirectoryApiDirectoriesPathPost
      >
    > = []
    const duplicates: ModelArtifact[] = []

    artifacts.forEach(artifact => {
      const isDuplicate = target.containsName(artifact.name)
      const artifactPath = artifact.path

      if (isDuplicate && isFalse(shouldRenameDuplicates)) {
        return duplicates.push(artifact)
      }

      if (shouldRenameDuplicates) {
        artifact.rename(artifact.copyName())
      }

      const new_path = ModelArtifact.toPath(target.path, artifact.name)

      if (artifact instanceof ModelDirectory) {
        moveArtifactCallbacks.push(() => {
          artifact.parent?.removeDirectory(artifact)
          target.addDirectory(artifact)
        })
        promises.push(
          writeDirectoryApiDirectoriesPathPost(artifactPath, { new_path }),
        )

        artifact.allArtifacts.forEach(a => artifacts.delete(a))
      }

      if (artifact instanceof ModelFile) {
        moveArtifactCallbacks.push(() => {
          artifact.parent?.removeFile(artifact)
          target.addFile(artifact)
        })
        promises.push(writeFileApiFilesPathPost(artifactPath, { new_path }))
      }
    })

    if (isArrayNotEmpty(duplicates)) {
      addConfirmation({
        headline: 'Moving Duplicates',
        description: `All duplicated names will be renamed!`,
        yesText: 'Yes, Rename',
        noText: 'No, Cancel',
        details: duplicates.map(artifact => artifact.path),
        action: () => {
          moveArtifacts(artifacts, target, true)
        },
      })

      return
    }

    Promise.all(promises)
      .then(resolvedList => {
        resolvedList.forEach((_, index) => moveArtifactCallbacks[index]?.())
      })
      .catch(error => {
        // TODO: Show error notification
        console.log(error)
      })
      .finally(() => {
        setIsLoading(false)
        setActiveRange(new Set())
        setFiles(project?.allFiles ?? [])
      })
  }

  return (
    <FileExplorerContext.Provider
      value={{
        activeRange,
        createDirectory,
        createFile,
        renameArtifact,
        removeArtifacts,
        removeArtifactWithConfirmation,
        moveArtifacts,
        selectArtifactsInRange,
        setActiveRange(activeRange) {
          setActiveRange(
            () =>
              new Set(
                project.allArtifacts.filter(artifact =>
                  activeRange.has(artifact),
                ),
              ),
          )
        },
      }}
    >
      {children}
    </FileExplorerContext.Provider>
  )
}

export function useFileExplorer(): FileExplorer {
  return useContext(FileExplorerContext)
}
