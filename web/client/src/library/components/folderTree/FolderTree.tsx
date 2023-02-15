import {
  FolderOpenIcon,
  DocumentIcon,
  FolderPlusIcon,
  DocumentPlusIcon,
  XCircleIcon,
} from '@heroicons/react/24/solid'
import {
  FolderIcon,
  DocumentIcon as DocumentIconOutline,
} from '@heroicons/react/24/outline'
import {
  ChevronRightIcon,
  ChevronDownIcon,
  CheckCircleIcon,
} from '@heroicons/react/20/solid'
import { FormEvent, MouseEvent, useEffect, useMemo, useState } from 'react'
import clsx from 'clsx'
import { ModelFile, ModelDirectory } from '../../../models'
import {
  createDirectoryApiDirectoriesPathPost,
  deleteDirectoryApiDirectoriesPathDelete,
  deleteFileApiFilesPathDelete,
  Directory as DirectoryApi,
  writeFileApiFilesPathPost,
} from '../../../api/client'
import { useStoreFileTree } from '../../../context/fileTree'
import { getAllFilesInDirectory, toUniqueName } from './help'
import ModalConfirmation from '../modal/ModalConfirmation'
import type { Confirmation, WithConfirmation } from '../modal/ModalConfirmation'
import { Button } from '../button/Button'
import { isFalse, isNotNil, isTrue } from '~/utils'

/* TODO:
  - add ability to create file or directory on top level
  - add context menu
  - add rename
  - add drag and drop
  - add copy and paste
  - add move
  - add search
*/

interface PropsDirectory extends WithConfirmation {
  directory: ModelDirectory
}

interface PropsFile extends WithConfirmation {
  file: ModelFile
}

const CSS_ICON_SIZE = 'w-4 h-4'

export function FolderTree({
  project,
}: {
  project?: DirectoryApi
}): JSX.Element {
  const directory = useMemo(() => new ModelDirectory(project), [project])
  const [confirmation, setConfirmation] = useState<Confirmation | undefined>()
  const [showConfirmation, setShowConfirmation] = useState(false)

  useEffect(() => {
    setShowConfirmation(isNotNil(confirmation))
  }, [confirmation])

  return (
    <div className="py-2 px-1 overflow-hidden">
      <ModalConfirmation
        show={showConfirmation}
        headline={confirmation?.headline}
        description={confirmation?.description}
        onClose={() => {
          confirmation?.cancel?.()
        }}
      >
        <Button
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

            setShowConfirmation(false)
          }}
        >
          {confirmation?.noText ?? 'Cancel'}
        </Button>
      </ModalConfirmation>
      <Directory
        directory={directory}
        setConfirmation={setConfirmation}
      />
    </div>
  )
}

function Directory({
  directory,
  setConfirmation,
}: PropsDirectory): JSX.Element {
  const openedFiles = useStoreFileTree(s => s.openedFiles)
  const setOpenedFiles = useStoreFileTree(s => s.setOpenedFiles)

  const [isLoading, setIsLoading] = useState(false)
  const [isOpen, setOpen] = useState(false)
  const [renamingDirectory, setRenamingDirectory] = useState<ModelDirectory>()
  const [newName, setNewName] = useState<string>('')

  const IconChevron = isOpen ? ChevronDownIcon : ChevronRightIcon
  const IconFolder = isOpen ? FolderOpenIcon : FolderIcon

  function createDirectory(e: MouseEvent): void {
    e.stopPropagation()

    if (isLoading) return

    setIsLoading(true)

    const name = toUniqueName('new_directory')

    createDirectoryApiDirectoriesPathPost(`${directory.path}/${name}`)
      .then(created => {
        if (isFalse((created as any).ok)) {
          console.warn([
            `Directory: ${directory.path}`,
            (created as any).detail,
          ])

          return
        }

        directory.addDirectory(new ModelDirectory(created, directory))

        setOpen(true)
      })
      .catch(error => {
        // TODO: Show error notification
        console.log(error)
      })
      .finally(() => {
        setIsLoading(false)
      })
  }

  function createFile(e: MouseEvent): void {
    e.stopPropagation()

    if (isLoading) return

    setIsLoading(true)

    const extension = '.py'
    const name = toUniqueName('new_file', extension)

    writeFileApiFilesPathPost(`${directory.path}/${name}`, { content: '' })
      .then(created => {
        if (isFalse((created as any).ok)) {
          console.warn([`File: ${directory.path}`, (created as any).detail])

          return
        }

        directory.addFile(new ModelFile(created, directory))

        setOpen(true)
      })
      .catch(error => {
        // TODO: Show error notification
        console.log(error)
      })
      .finally(() => {
        setIsLoading(false)
      })
  }

  function remove(): void {
    if (isLoading) return

    setIsLoading(true)

    deleteDirectoryApiDirectoriesPathDelete(directory.path)
      .then(response => {
        if (isFalse((response as any).ok)) {
          console.warn([
            `Directory: ${directory.path}`,
            (response as any).detail,
          ])

          return
        }

        if (directory.isNotEmpty) {
          const files = getAllFilesInDirectory(directory)

          files.forEach(file => {
            openedFiles.delete(file.id)
          })
        }

        directory.parent?.removeDirectory(directory)

        setOpenedFiles(openedFiles)
      })
      .catch(error => {
        // TODO: Show error notification
        console.log({ error })
      })
      .finally(() => {
        setIsLoading(false)
      })
  }

  function removeWithConfirmation(): void {
    setConfirmation({
      headline: 'Removing Directory',
      description: `Are you sure you want to remove the directory "${directory.name}"?`,
      yesText: 'Yes, Remove',
      noText: 'No, Cancel',
      action: remove,
      cancel: () => {
        setConfirmation(undefined)
      },
    })
  }

  function rename(): void {
    if (isLoading || renamingDirectory == null) return

    setIsLoading(true)

    setTimeout(() => {
      renamingDirectory.rename(newName.trim())

      setNewName('')
      setRenamingDirectory(undefined)
      setOpen(true)
      setIsLoading(false)
    })
  }

  function renameWithConfirmation(): void {
    setConfirmation({
      headline: 'Renaming Directory',
      description: `Are you sure you want to rename the directory "${directory.name}"?`,
      yesText: 'Yes, Rename',
      noText: 'No, Cancel',
      action: rename,
      cancel: () => {
        setConfirmation(undefined)
      },
    })
  }

  return (
    <>
      {directory.withParent && (
        <span className="w-full overflow-hidden hover:bg-secondary-100 group flex justify-between items-center rounded-md">
          <div
            className={clsx(
              'mr-1 flex items-center cursor-pointer',
              directory.withDirectories || directory.withFiles
                ? 'ml-0'
                : 'ml-[0.5rem]',
            )}
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              setOpen(!isOpen)
            }}
          >
            {(directory.withDirectories || directory.withFiles) && (
              <IconChevron
                className={clsx(
                  `inline-block ${CSS_ICON_SIZE} ml-1 mr-1 text-secondary-500`,
                )}
              />
            )}
            <IconFolder
              className={`inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-500`}
            />
          </div>
          <span className="w-full h-[1.5rem]  overflow-hidden flex items-center cursor-pointer justify-between pr-1">
            {renamingDirectory?.id === directory.id ? (
              <div className="flex  overflow-hidden items-center">
                <input
                  type="text"
                  className="w-full text-sm overflow-hidden overflow-ellipsis group-hover:text-secondary-500"
                  value={newName ?? directory.name}
                  onInput={(e: FormEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    const elInput = e.target as HTMLInputElement

                    setNewName(elInput.value)
                  }}
                />
                <div className="flex">
                  <CheckCircleIcon
                    className={`inline-block ${CSS_ICON_SIZE} ml-2 text-gray-300 hover:text-gray-500 cursor-pointer`}
                    onClick={(e: MouseEvent) => {
                      e.stopPropagation()

                      renameWithConfirmation()
                    }}
                  />
                </div>
              </div>
            ) : (
              <span className="w-full flex text-base overflow-hidden items-center">
                <span
                  className="w-full text-sm overflow-hidden overflow-ellipsis justify-between group-hover:text-secondary-500"
                  onClick={(e: MouseEvent) => {
                    e.stopPropagation()

                    setOpen(!isOpen)
                  }}
                  onDoubleClick={(e: MouseEvent) => {
                    e.stopPropagation()

                    setNewName(directory.name)
                    setRenamingDirectory(directory)
                  }}
                >
                  {directory.name}
                </span>
                <span className="hidden w-full group-hover:flex items-center justify-end">
                  <DocumentPlusIcon
                    onClick={createFile}
                    className={`cursor-pointer inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-300 hover:text-secondary-500`}
                  />
                  <FolderPlusIcon
                    onClick={createDirectory}
                    className={`cursor-pointer inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-300 hover:text-secondary-500`}
                  />
                  <XCircleIcon
                    onClick={(e: MouseEvent) => {
                      e.stopPropagation()

                      removeWithConfirmation()
                    }}
                    className={`cursor-pointer inline-block ${CSS_ICON_SIZE} ml-2 text-danger-300 hover:text-danger-500`}
                  />
                </span>
              </span>
            )}
          </span>
        </span>
      )}
      {(isOpen || !directory.withParent) && directory.withDirectories && (
        <ul
          className={clsx(
            'mr-1 overflow-hidden',
            directory.withParent ? 'pl-3' : 'mt-1',
          )}
        >
          {directory.directories.map(dir => (
            <li
              key={dir.id}
              title={dir.name}
              className={clsx(isTrue(dir.parent?.withParent) && 'border-l')}
            >
              <Directory
                directory={dir}
                setConfirmation={setConfirmation}
              />
            </li>
          ))}
        </ul>
      )}
      {(isOpen || !directory.withParent) && directory.withFiles && (
        <ul
          className={clsx(
            'mr-1 overflow-hidden block ',
            directory.withParent ? 'pl-3' : 'mt-1',
          )}
        >
          {directory.files.map(file => (
            <li
              key={file.id}
              title={file.name}
              className={clsx(
                isTrue(file.parent?.withParent) && 'border-l px-0',
              )}
            >
              <File
                file={file}
                setConfirmation={setConfirmation}
              />
            </li>
          ))}
        </ul>
      )}
    </>
  )
}

function File({ file, setConfirmation }: PropsFile): JSX.Element {
  const activeFileId = useStoreFileTree(s => s.activeFileId)
  const openedFiles = useStoreFileTree(s => s.openedFiles)
  const setOpenedFiles = useStoreFileTree(s => s.setOpenedFiles)
  const selectFile = useStoreFileTree(s => s.selectFile)

  const [isLoading, setIsLoading] = useState(false)
  const [renamingFile, setRenamingFile] = useState<ModelFile>()
  const [newName, setNewName] = useState<string>('')

  function remove(): void {
    if (isLoading) return

    setIsLoading(true)

    deleteFileApiFilesPathDelete(file.path)
      .then(response => {
        if ((response as unknown as { ok: boolean }).ok) {
          openedFiles.delete(file.id)

          file.parent?.removeFile(file)

          setOpenedFiles(openedFiles)
        } else {
          // TODO: Show error notification
        }
      })
      .catch(error => {
        // TODO: Show error notification
        console.log(error)
      })
      .finally(() => {
        setIsLoading(false)
      })
  }

  function removeWithConfirmation(): void {
    setConfirmation({
      headline: 'Deleting File',
      description: `Are you sure you want to remove the file "${file.name}"?`,
      yesText: 'Yes, Delete',
      noText: 'No, Cancel',
      action: remove,
      cancel: () => {
        setConfirmation(undefined)
      },
    })
  }

  function rename(): void {
    if (isLoading || renamingFile == null) return

    setIsLoading(true)

    const shouldSelectAfterRename = activeFileId === renamingFile.id

    openedFiles.delete(renamingFile.id)

    renamingFile.rename(
      newName.trim().replace(`.${String(renamingFile.extension)}`, ''),
    )

    if (shouldSelectAfterRename) {
      selectFile(renamingFile)
    } else {
      openedFiles.set(renamingFile.id, renamingFile)

      setOpenedFiles(openedFiles)
    }

    setTimeout(() => {
      setNewName('')
      setRenamingFile(undefined)
      setIsLoading(false)
    })
  }

  return (
    <span
      className={clsx(
        'text-base whitespace-nowrap group/file px-[5px] flex justify-between rounded-md',
        file.id === activeFileId ? 'text-secondary-500' : 'text-gray-800',
        file.is_supported && 'group cursor-pointer hover:bg-secondary-100',
      )}
    >
      <span
        className={clsx(
          'flex w-full items-center overflow-hidden overflow-ellipsis',
          !file.is_supported && 'opacity-50 cursor-not-allowed text-gray-800',
        )}
      >
        <div className="flex items-center">
          {openedFiles?.has(file.id) ? (
            <DocumentIcon
              className={`inline-block ${CSS_ICON_SIZE} mr-2 text-secondary-500`}
            />
          ) : (
            <DocumentIconOutline
              className={`inline-block ${CSS_ICON_SIZE} mr-2 text-secondary-500`}
            />
          )}
        </div>
        {renamingFile?.id === file.id ? (
          <div className="flex items-center">
            <input
              type="text"
              className="w-full text-sm overflow-hidden overflow-ellipsis group-hover:text-secondary-500"
              value={newName === '' ? file.name : newName}
              onInput={(e: any) => {
                e.stopPropagation()

                setNewName(e.target.value)
              }}
            />
            <div className="flex">
              <CheckCircleIcon
                className={`inline-block ${CSS_ICON_SIZE} ml-2 text-gray-300 hover:text-gray-500 cursor-pointer`}
                onClick={(e: MouseEvent) => {
                  e.stopPropagation()

                  rename()
                }}
              />
            </div>
          </div>
        ) : (
          <>
            <span
              onClick={(e: MouseEvent) => {
                e.stopPropagation()

                file.is_supported &&
                  file.id !== activeFileId &&
                  selectFile(file)
              }}
              onDoubleClick={(e: MouseEvent) => {
                e.stopPropagation()

                setNewName(file.name)
                setRenamingFile(file)
              }}
              className="w-full text-sm overflow-hidden overflow-ellipsis group-hover:text-secondary-500"
            >
              {file.name}
            </span>
            <span
              className="flex items-center invisible group-hover/file:visible min-w-8"
              onClick={(e: MouseEvent) => {
                e.stopPropagation()

                removeWithConfirmation()
              }}
            >
              <XCircleIcon
                className={`inline-block ${CSS_ICON_SIZE} ml-2 text-danger-300 hover:text-danger-500 cursor-pointer`}
              />
            </span>
          </>
        )}
      </span>
    </span>
  )
}
