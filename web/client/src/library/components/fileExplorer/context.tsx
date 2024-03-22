import { createContext, useState, useContext, useEffect } from 'react'
import {
  type Directory,
  deleteDirectoryApiDirectoriesPathDelete,
  deleteFileApiFilesPathDelete,
  writeDirectoryApiDirectoriesPathPost,
  writeFileApiFilesPathPost,
} from '@api/client'
import { useStoreProject } from '@context/project'
import { type ModelArtifact } from '@models/artifact'
import { ModelDirectory } from '@models/directory'
import {
  isArrayNotEmpty,
  isFalse,
  isNil,
  isNotNil,
  isStringEmptyOrNil,
  toUniqueName,
} from '@utils/index'
import { ModelFile } from '@models/file'
import { useStoreEditor } from '@context/editor'
import { type ResponseWithDetail } from '@api/instance'
import { useStoreContext } from '@context/context'
import {
  EnumErrorKey,
  useNotificationCenter,
} from '~/library/pages/root/context/notificationCenter'

export const EnumFileExplorerChange = {
  Added: 1,
  Modified: 2,
  Deleted: 3,
} as const

export type FileExplorerChange = KeyOf<typeof EnumFileExplorerChange>

interface FileExplorer {
  artifactRename?: ModelArtifact
  setArtifactRename: (artifact?: ModelArtifact) => void
  selectArtifactsInRange: (to: ModelArtifact) => void
  createDirectory: (parent: ModelDirectory) => void
  createFile: (parent: ModelDirectory, extension?: string) => void
  renameArtifact: (artifact: ModelArtifact, newName?: string) => void
  removeArtifacts: (artifacts: ModelArtifact[]) => void
  removeArtifactWithConfirmation: (artifact: ModelArtifact) => void
  moveArtifacts: (artifacts: ModelArtifact[], target: ModelDirectory) => void
  isTopGroupInActiveRange: (artifact: ModelArtifact) => boolean
  isBottomGroupInActiveRange: (artifact: ModelArtifact) => boolean
  isMiddleGroupInActiveRange: (artifact: ModelArtifact) => boolean
}

export const FileExplorerContext = createContext<FileExplorer>({
  artifactRename: undefined,
  setArtifactRename: () => {},
  selectArtifactsInRange: () => new Set(),
  createDirectory: () => {},
  createFile: () => {},
  renameArtifact: () => {},
  removeArtifacts: () => {},
  removeArtifactWithConfirmation: () => {},
  moveArtifacts: () => {},
  isTopGroupInActiveRange: () => false,
  isBottomGroupInActiveRange: () => false,
  isMiddleGroupInActiveRange: () => false,
})

export default function FileExplorerProvider({
  children,
}: {
  children: React.ReactNode
}): JSX.Element {
  const { addError } = useNotificationCenter()

  const activeRange = useStoreProject(s => s.activeRange)
  const project = useStoreProject(s => s.project)
  const files = useStoreProject(s => s.files)
  const setActiveRange = useStoreProject(s => s.setActiveRange)
  const setSelectedFile = useStoreProject(s => s.setSelectedFile)
  const setFiles = useStoreProject(s => s.setFiles)
  const inActiveRange = useStoreProject(s => s.inActiveRange)

  const addConfirmation = useStoreContext(s => s.addConfirmation)

  const tab = useStoreEditor(s => s.tab)

  const [isLoading, setIsLoading] = useState(false)
  const [artifactRename, setArtifactRename] = useState<ModelArtifact>()

  useEffect(() => {
    setSelectedFile(tab?.file)
  }, [tab?.id])

  function selectArtifactsInRange(to: ModelArtifact): void {
    const IDX_FIRST = 0
    const IDX_LAST = activeRange.length - 1
    const artifacts = project.allVisibleArtifacts
    const first = activeRange[IDX_FIRST]
    const last = activeRange[IDX_LAST]
    const indexTo = artifacts.indexOf(to)
    const indexFirst = first == null ? IDX_FIRST : artifacts.indexOf(first)
    const indexLast = last == null ? IDX_LAST : artifacts.indexOf(last)

    const indexStart = indexTo > indexFirst ? indexFirst : indexTo
    const indexEnd =
      indexTo > indexLast || (indexTo > indexFirst && indexTo < indexLast)
        ? indexTo
        : indexLast

    setActiveRange(artifacts.slice(indexStart, indexEnd + 1))
  }

  function createDirectory(parent: ModelDirectory): void {
    if (isLoading) return

    setIsLoading(true)

    const name = toUniqueName('new_directory')

    writeDirectoryApiDirectoriesPathPost([parent.path, name].join('/'), {})
      .then(created => {
        if (isNil(created)) return

        parent.open()
      })
      .catch(error => addError(EnumErrorKey.FileExplorer, error))
      .finally(() => {
        setIsLoading(false)
      })
  }

  function createFile(parent: ModelDirectory, extension?: string): void {
    if (isLoading) return

    if (isNil(extension)) {
      if (parent.isModels) {
        extension = '.sql'
      } else if (parent.isTests) {
        extension = '.yaml'
      } else {
        extension = '.py'
      }
    }

    setIsLoading(true)

    const name = toUniqueName('new_file', extension)

    writeFileApiFilesPathPost(`${parent.path}/${name}`, { content: '' })
      .then(created => {
        if (isNil(created)) return

        parent.open()
      })
      .catch(error => addError(EnumErrorKey.FileExplorer, error))
      .finally(() => {
        setIsLoading(false)
      })
  }

  function renameArtifact(artifact: ModelArtifact, newName?: string): void {
    newName = newName?.trim()

    const parentArtifacts = artifact.parent?.artifacts ?? []
    const isDuplicate = parentArtifacts.some(a => a.name === newName)

    if (isLoading || isStringEmptyOrNil(newName) || isDuplicate) return

    setIsLoading(true)

    const currentName = artifact.name
    const currentPath = artifact.path

    artifact.rename(newName)

    if (artifact instanceof ModelDirectory) {
      writeDirectoryApiDirectoriesPathPost(currentPath, {
        new_path: artifact.path,
      })
        .catch(error => {
          addError(EnumErrorKey.FileExplorer, error)

          artifact.rename(currentName)
        })
        .finally(() => {
          setIsLoading(false)

          if (isNotNil(tab) && artifact.hasFile(tab.file)) {
            setSelectedFile(tab.file)
          }

          setFiles(project.allFiles)
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
          addError(EnumErrorKey.FileExplorer, error)

          artifact.rename(currentName)
        })
        .finally(() => {
          setIsLoading(false)
          setFiles(project.allFiles)
        })
    }
  }

  function removeArtifacts(artifacts: ModelArtifact[]): void {
    if (isLoading) return

    setIsLoading(true)

    const promises = artifacts.map(artifact => {
      if (artifact instanceof ModelFile) {
        return deleteFileApiFilesPathDelete(artifact.path)
      }

      return deleteDirectoryApiDirectoriesPathDelete(artifact.path)
    })

    Promise.all(promises)
      .then(() => {
        setActiveRange([])
      })
      .catch(error => addError(EnumErrorKey.FileExplorer, error))
      .finally(() => {
        setIsLoading(false)
      })
  }

  function removeArtifactWithConfirmation(artifact: ModelArtifact): void {
    if (inActiveRange(artifact)) {
      // User selected multiple including current directory
      // so here we should prompt to delete all selected
      addConfirmation({
        headline: 'Removing Selected Files/Directories',
        description: `Are you sure you want to remove ${activeRange.length} items?`,
        yesText: 'Yes, Remove',
        noText: 'No, Cancel',
        details: activeRange.map(artifact => artifact.path),
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
            removeArtifacts([artifact])
          }
        },
      })
    }
  }

  function moveArtifacts(
    artifacts: ModelArtifact[],
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

      const new_path = [target.path, artifact.name].join('/')

      if (artifact instanceof ModelDirectory) {
        moveArtifactCallbacks.push(() => {
          artifact.parent?.removeDirectory(artifact)
          target.addDirectory(artifact)
        })
        promises.push(
          writeDirectoryApiDirectoriesPathPost(artifactPath, { new_path }),
        )

        artifact.allVisibleArtifacts.forEach(a =>
          artifacts.splice(artifacts.indexOf(a), 1),
        )
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
        setActiveRange([])
        setFiles(project.allFiles)
      })
  }

  function isTopGroupInActiveRange(artifact: ModelArtifact): boolean {
    const index = project.allVisibleArtifacts.indexOf(artifact)
    const prev = project.allVisibleArtifacts[index - 1]
    const next = project.allVisibleArtifacts[index + 1]

    return (
      inActiveRange(artifact) &&
      (isNil(prev) || isFalse(inActiveRange(prev))) &&
      isNotNil(next) &&
      inActiveRange(next)
    )
  }

  function isBottomGroupInActiveRange(artifact: ModelArtifact): boolean {
    const index = project.allVisibleArtifacts.indexOf(artifact)
    const prev = project.allVisibleArtifacts[index - 1]
    const next = project.allVisibleArtifacts[index + 1]

    return (
      inActiveRange(artifact) &&
      (isNil(next) || isFalse(inActiveRange(next))) &&
      isNotNil(prev) &&
      inActiveRange(prev)
    )
  }

  function isMiddleGroupInActiveRange(artifact: ModelArtifact): boolean {
    const index = project.allVisibleArtifacts.indexOf(artifact)
    const prev = project.allVisibleArtifacts[index - 1]
    const next = project.allVisibleArtifacts[index + 1]

    return (
      inActiveRange(artifact) &&
      isNotNil(next) &&
      inActiveRange(next) &&
      isNotNil(prev) &&
      inActiveRange(prev)
    )
  }

  return (
    <FileExplorerContext.Provider
      value={{
        artifactRename,
        setArtifactRename,
        createDirectory,
        createFile,
        renameArtifact,
        removeArtifacts,
        removeArtifactWithConfirmation,
        moveArtifacts,
        selectArtifactsInRange,
        isTopGroupInActiveRange,
        isBottomGroupInActiveRange,
        isMiddleGroupInActiveRange,
      }}
    >
      {children}
    </FileExplorerContext.Provider>
  )
}

export function useFileExplorer(): FileExplorer {
  return useContext(FileExplorerContext)
}
