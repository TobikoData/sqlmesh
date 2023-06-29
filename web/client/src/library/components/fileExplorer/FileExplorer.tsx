import React, { type MouseEvent, useEffect, useState } from 'react'
import clsx from 'clsx'
import ModalConfirmation from '../modal/ModalConfirmation'
import type { Confirmation } from '../modal/ModalConfirmation'
import { Button } from '../button/Button'
import { isFalse, isNotNil, isStringEmptyOrNil } from '~/utils'
import Directory from './Directory'
import { useStoreFileExplorer } from '@context/fileTree'
import { ModelFile } from '@models/file'
import {
  deleteDirectoryApiDirectoriesPathDelete,
  deleteFileApiFilesPathDelete,
  writeDirectoryApiDirectoriesPathPost,
  writeFileApiFilesPathPost,
} from '@api/client'
import { useStoreEditor } from '@context/editor'
import { ModelArtifact } from '@models/artifact'
import { ModelDirectory } from '@models/directory'
import * as ContextMenu from '@radix-ui/react-context-menu'
import { getAllFilesInDirectory, toUniqueName } from './help'
import { DndProvider } from 'react-dnd'
import { HTML5Backend } from 'react-dnd-html5-backend'
import DragLayer from './DragLayer'

/* TODO:
  - add move files/directories
  - add search
  - add drag and drop files/directories from desktop
  - add copy and paste
  - add accessability support
*/

export interface PropsArtifact {
  removeArtifactWithConfirmation: (artifact: ModelArtifact) => void
  renameAtrifact: (artifact: ModelArtifact, newName?: string) => void
  className?: string
  style?: React.CSSProperties
}

export default function FileExplorer({
  className,
}: {
  className?: string
}): JSX.Element {
  const project = useStoreFileExplorer(s => s.project)
  const files = useStoreFileExplorer(s => s.files)
  const selected = useStoreFileExplorer(s => s.selected)
  const activeRange = useStoreFileExplorer(s => s.activeRange)
  const setActiveRange = useStoreFileExplorer(s => s.setActiveRange)
  const setSelected = useStoreFileExplorer(s => s.setSelected)
  const setFiles = useStoreFileExplorer(s => s.setFiles)

  const tab = useStoreEditor(s => s.tab)
  const closeTab = useStoreEditor(s => s.closeTab)

  const [isLoading, setIsLoading] = useState(false)
  const [confirmation, setConfirmation] = useState<Confirmation>()
  const [showConfirmation, setShowConfirmation] = useState(false)

  useEffect(() => {
    setShowConfirmation(isNotNil(confirmation))
  }, [confirmation])

  useEffect(() => {
    setActiveRange(new Set())
    setSelected(tab?.file)
  }, [tab])

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

        setSelected(file)
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

  function renameAtrifact(artifact: ModelArtifact, newName?: string): void {
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
            setSelected(tab.file)
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
    let confirmation: Confirmation = {
      headline: `Removing ${
        artifact instanceof ModelDirectory ? 'Directory' : 'File'
      }`,
      description: `Are you sure you want to remove the ${
        artifact instanceof ModelDirectory ? 'directory' : 'file'
      } "${artifact.name}"?`,
      yesText: 'Yes, Remove',
      noText: 'No, Cancel',
      action: () => {
        if (artifact instanceof ModelDirectory) {
          if (artifact.parent != null) {
            removeArtifacts(new Set([artifact]))
          }
        }

        if (artifact instanceof ModelFile) {
          if (artifact.parent != null) {
            removeArtifacts(new Set([artifact]))
          }
        }
      },
    }

    if (activeRange.has(artifact)) {
      // User selected multiple including current directory
      // so here we should prompt to delete all selected
      confirmation = {
        headline: 'Removing Selected Files/Directories',
        description: `Are you sure you want to remove ${activeRange.size} items?`,
        yesText: 'Yes, Remove',
        noText: 'No, Cancel',
        details: Array.from(activeRange).map(artifact => artifact.path),
        action: () => {
          removeArtifacts(activeRange)
        },
      }
    }

    setConfirmation(confirmation)
  }

  function moveArtifacts(
    artifacts: Set<ModelArtifact>,
    target: ModelDirectory,
  ): void {
    if (isLoading) return

    setIsLoading(true)

    const list = Array.from(artifacts)
    const promises = list.map(artifact => {
      const new_path = ModelArtifact.toPath(target.path, artifact.name)

      console.log({ new_path })

      if (artifact instanceof ModelFile) {
        return writeFileApiFilesPathPost(artifact.path, {
          new_path,
        })
      }

      return writeDirectoryApiDirectoriesPathPost(artifact.path, {
        new_path,
      })
    })

    Promise.all(promises)
      .then(resolvedList => {
        resolvedList.forEach((_, index) => {
          const artifact = list[index]

          if (artifact instanceof ModelFile) {
            target.addFile(artifact)
            artifact.parent?.removeFile(artifact)
          }

          if (artifact instanceof ModelDirectory) {
            artifact.parent?.removeDirectory(artifact)
            target.addDirectory(artifact)
          }
        })

        setActiveRange(new Set())
      })
      .catch(error => {
        // TODO: Show error notification
        console.log(error)
      })
      .finally(() => {
        setIsLoading(false)
      })

    setIsLoading(false)
    setActiveRange(new Set())
  }

  function handleKeyDown(e: React.KeyboardEvent): void {
    if (e.key === 'Escape' && selected != null && activeRange.size > 1) {
      setActiveRange(new Set([selected]))
    }

    if (e.metaKey && e.key === 'Backspace' && activeRange.size > 0) {
      setConfirmation({
        headline: 'Removing Selected Files/Directories',
        description: `Are you sure you want to remove ${activeRange.size} items?`,
        yesText: 'Yes, Remove',
        noText: 'No, Cancel',
        details: Array.from(activeRange).map(artifact => artifact.path),
        action: () => {
          removeArtifacts(activeRange)
        },
      })
    }
  }

  return (
    <div
      className={clsx(
        'h-full py-2 overflow-hidden  text-sm text-neutral-500 dark:text-neutral-400 font-regular select-none',
        className,
      )}
    >
      {project != null && (
        <ContextMenuProject
          createFile={() => createFile(project)}
          createDirectory={() => createDirectory(project)}
        >
          <DndProvider backend={HTML5Backend}>
            <div
              className="relative h-full px-2 overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical"
              tabIndex={1}
              onKeyDown={handleKeyDown}
            >
              <DragLayer />
              <Directory
                className="z-20 relative"
                directory={project}
                createFile={createFile}
                createDirectory={createDirectory}
                renameAtrifact={renameAtrifact}
                removeArtifactWithConfirmation={removeArtifactWithConfirmation}
                moveArtifacts={moveArtifacts}
              />
            </div>
          </DndProvider>
        </ContextMenuProject>
      )}
      <ModalConfirmation
        show={showConfirmation}
        onClose={() => undefined}
      >
        <ModalConfirmation.Main>
          {confirmation?.headline != null && (
            <ModalConfirmation.Headline>
              {confirmation?.headline}
            </ModalConfirmation.Headline>
          )}
          {confirmation?.description != null && (
            <ModalConfirmation.Description>
              {confirmation?.description}
            </ModalConfirmation.Description>
          )}
          {confirmation?.details != null && (
            <ModalConfirmation.Details details={confirmation?.details} />
          )}
        </ModalConfirmation.Main>
        <ModalConfirmation.Actions>
          <Button
            className="font-bold"
            size="md"
            variant="danger"
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              confirmation?.action?.()

              setShowConfirmation(false)
            }}
          >
            {confirmation?.yesText ?? 'Confirm'}
          </Button>
          <Button
            size="md"
            variant="alternative"
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              confirmation?.cancel?.()

              setShowConfirmation(false)
            }}
          >
            {confirmation?.noText ?? 'Cancel'}
          </Button>
        </ModalConfirmation.Actions>
      </ModalConfirmation>
    </div>
  )
}

function ContextMenuProject({
  children,
  createFile,
  createDirectory,
}: {
  children: React.ReactNode
  createFile: () => void
  createDirectory: () => void
}): JSX.Element {
  return (
    <ContextMenu.Root>
      <ContextMenu.Trigger
        onContextMenu={(e: MouseEvent) => {
          e.stopPropagation()
        }}
      >
        {children}
      </ContextMenu.Trigger>
      <ContextMenu.Portal>
        <ContextMenu.Content
          className="bg-light rounded-md overflow-hiddin shadow-lg py-2 px-1"
          onClick={(e: MouseEvent) => {
            e.stopPropagation()
          }}
        >
          <ContextMenu.Item
            className="py-1.5 group leading-none rounded-md flex items-center relative pl-6 pr-2 select-none outline-none font-medium text-xs text-neutral-500 hover:bg-accent-500 hover:text-light"
            onSelect={(e: Event) => {
              e.stopPropagation()

              createFile()
            }}
          >
            New File
            <div className="ml-auto pl-5"></div>
          </ContextMenu.Item>
          <ContextMenu.Item
            className="py-1.5 group leading-none rounded-md flex items-center relative pl-6 pr-2 select-none outline-none font-medium text-xs text-neutral-500 hover:bg-accent-500 hover:text-light"
            onSelect={(e: Event) => {
              e.stopPropagation()

              createDirectory()
            }}
          >
            New Folder
            <div className="ml-auto pl-5"></div>
          </ContextMenu.Item>
        </ContextMenu.Content>
      </ContextMenu.Portal>
    </ContextMenu.Root>
  )
}
