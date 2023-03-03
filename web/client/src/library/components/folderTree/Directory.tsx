import { useState, FormEvent, MouseEvent } from 'react'
import {
  ChevronDownIcon,
  ChevronRightIcon,
  FolderOpenIcon,
  FolderIcon,
  CheckCircleIcon,
  DocumentPlusIcon,
  FolderPlusIcon,
  XCircleIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import {
  writeDirectoryApiDirectoriesPathPost,
  writeFileApiFilesPathPost,
  deleteDirectoryApiDirectoriesPathDelete,
} from '~/api/client'
import { useStoreFileTree } from '~/context/fileTree'
import { ModelDirectory, ModelFile } from '~/models'
import { isFalse, isStringEmptyOrNil } from '~/utils'
import { WithConfirmation } from '../modal/ModalConfirmation'
import { toUniqueName, getAllFilesInDirectory } from './help'
import File from './File'

interface PropsDirectory extends WithConfirmation {
  directory: ModelDirectory
}

const CSS_ICON_SIZE = 'w-4 h-4'

export default function Directory({
  directory,
  setConfirmation,
}: PropsDirectory): JSX.Element {
  const activeFileId = useStoreFileTree(s => s.activeFileId)
  const openedFiles = useStoreFileTree(s => s.openedFiles)
  const setOpenedFiles = useStoreFileTree(s => s.setOpenedFiles)
  const selectFile = useStoreFileTree(s => s.selectFile)

  const [isLoading, setIsLoading] = useState(false)
  const [isOpen, setOpen] = useState(false)
  const [newName, setNewName] = useState<string>()

  const IconChevron = isOpen ? ChevronDownIcon : ChevronRightIcon
  const IconFolder = isOpen ? FolderOpenIcon : FolderIcon

  function createDirectory(e: MouseEvent): void {
    e.stopPropagation()

    if (isLoading) return

    setIsLoading(true)

    const name = toUniqueName('new_directory')

    writeDirectoryApiDirectoriesPathPost(`${directory.path}/${name}`, {})
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

    writeFileApiFilesPathPost(`${directory.path}/${name}`, {})
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
    })
  }

  function rename(): void {
    if (
      isLoading ||
      directory == null ||
      isStringEmptyOrNil(newName) ||
      newName == null
    )
      return

    setIsLoading(true)

    const currentName = directory.name
    const currentPath = directory.path
    const files = getAllFilesInDirectory(directory)
    const openedFilesFromDirectory = new Map<ID, ModelFile>()
    let fileThatShouldBeSelectedAfterDirectoryRename: ModelFile | undefined

    files.forEach(file => {
      if (openedFiles.has(file.id)) {
        openedFilesFromDirectory.set(file.id, file)
      }

      if (activeFileId === file.id) {
        fileThatShouldBeSelectedAfterDirectoryRename = file
      }
    })

    directory.rename(newName.trim())

    void writeDirectoryApiDirectoriesPathPost(currentPath, {
      new_path: directory.path,
    })
      .then(() => {
        openedFilesFromDirectory.forEach((file, id) => {
          openedFiles.delete(id)
          openedFiles.set(file.id, file)
        })
      })
      .catch(error => {
        console.log(error)

        directory.rename(currentName)
      })
      .finally(() => {
        setNewName(undefined)
        setIsLoading(false)

        setOpenedFiles(openedFiles)

        if (fileThatShouldBeSelectedAfterDirectoryRename != null) {
          selectFile(fileThatShouldBeSelectedAfterDirectoryRename)
        }
      })
  }

  function renameWithConfirmation(): void {
    setConfirmation({
      headline: 'Renaming Directory',
      description: `Are you sure you want to rename the directory "${directory.name}"?`,
      yesText: 'Yes, Rename',
      noText: 'No, Cancel',
      action: rename,
    })
  }

  return (
    <>
      {directory.withParent && (
        <span className="w-full overflow-hidden hover:bg-secondary-100 group flex justify-between items-center rounded-md">
          <div
            className={clsx(
              'mr-1 flex items-center',
              directory.withDirectories || directory.withFiles
                ? 'ml-0'
                : 'ml-3',
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
          <span className="w-full h-[1.5rem] overflow-hidden flex items-center justify-between pr-1">
            {isStringEmptyOrNil(newName) ? (
              <span className="w-full flex text-base overflow-hidden items-center cursor-default">
                <span
                  className="w-full text-sm overflow-hidden overflow-ellipsis justify-between"
                  onClick={(e: MouseEvent) => {
                    e.stopPropagation()

                    setOpen(!isOpen)
                  }}
                  onDoubleClick={(e: MouseEvent) => {
                    e.stopPropagation()

                    setNewName(directory.name)
                  }}
                >
                  {directory.name}
                </span>
                <span className="hidden w-full group-hover:flex items-center justify-end">
                  <DocumentPlusIcon
                    onClick={createFile}
                    className={`cursor-pointer inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-500`}
                  />
                  <FolderPlusIcon
                    onClick={createDirectory}
                    className={`cursor-pointer inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-500`}
                  />
                  <XCircleIcon
                    onClick={(e: MouseEvent) => {
                      e.stopPropagation()

                      removeWithConfirmation()
                    }}
                    className={`cursor-pointer inline-block ${CSS_ICON_SIZE} ml-2 text-danger-500`}
                  />
                </span>
              </span>
            ) : (
              <div className="flex overflow-hidden items-center">
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
            'mr-1 block ',
            directory.withParent ? 'pl-3' : 'mt-1',
          )}
        >
          {directory.files.map(file => (
            <li
              key={file.id}
              title={file.name}
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
