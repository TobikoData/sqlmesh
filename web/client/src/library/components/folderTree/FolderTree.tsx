import {
  FolderOpenIcon,
  DocumentIcon,
  FolderPlusIcon,
  DocumentPlusIcon,
  XCircleIcon,
} from '@heroicons/react/24/solid'
import { FolderIcon, DocumentIcon as DocumentIconOutline } from '@heroicons/react/24/outline'
import { ChevronRightIcon, ChevronDownIcon, CheckCircleIcon } from '@heroicons/react/20/solid'
import { FormEvent, MouseEvent, useMemo, useState } from 'react'
import clsx from 'clsx'
import { singular } from 'pluralize'
import { ModelFile, ModelDirectory } from '../../../models'
import type { Directory as DirectoryFromAPI } from '../../../api/client'
import { useStoreFileTree } from '../../../context/fileTree'
import { getAllFilesInDirectory, counter } from './help'

/* TODO:
  - connect to API
  - add ability to create file or directory on top level
  - add context menu
  - add confirmation before delete
  - add rename
  - add drag and drop
  - add copy and paste
  - add move
  - add search
*/

interface PropsDirectory {
  directory: ModelDirectory
}

interface PropsFile {
  file: ModelFile
}

const CSS_ICON_SIZE = 'w-4 h-4'

export function FolderTree({ project }: { project?: DirectoryFromAPI }): JSX.Element {
  const directory = useMemo(() => new ModelDirectory(project), [project])

  return (
    <div className="py-2 overflow-hidden">
      <Directory directory={directory} />
    </div>
  )
}

function Directory({ directory }: PropsDirectory): JSX.Element {
  const openedFiles = useStoreFileTree(s => s.openedFiles)
  const setOpenedFiles = useStoreFileTree(s => s.setOpenedFiles)
  const isRoot = !directory.withParent

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

    setTimeout(() => {
      const count = counter.countByKey(directory)
      const name = `new_directory_${count}`.toLowerCase()

      directory.addDirectory(
        new ModelDirectory(
          {
            name,
            path: `${directory.path}/${name}`,
          },
          directory
        )
      )

      setOpen(true)
      setIsLoading(false)
    })
  }

  function createFile(e: MouseEvent): void {
    e.stopPropagation()

    if (isLoading) return

    setIsLoading(true)

    setTimeout(() => {
      const extension = '.py'
      const count = counter.countByKey(directory)

      const name = directory.name.startsWith('new_')
        ? `new_file_${count}${extension}`
        : `new_${singular(directory.name)}_${count}${extension}`.toLowerCase()

      directory.addFile(
        new ModelFile(
          {
            name,
            extension,
            path: `${directory.path}/${name}`,
            content: '',
            is_supported: true,
          },
          directory
        )
      )

      setOpen(true)
      setIsLoading(false)
    })
  }

  function remove(e: MouseEvent): void {
    e.stopPropagation()

    if (isLoading) return

    setIsLoading(true)

    setTimeout(() => {
      if (directory.isNotEmpty) {
        const files = getAllFilesInDirectory(directory)

        files.forEach(file => {
          openedFiles.delete(file.id)
        })
      }

      directory.parent?.removeDirectory(directory)

      setOpenedFiles(openedFiles)
      setIsLoading(false)
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

  return (
    <>
      {!isRoot && (
        <span className="w-full text-base whitespace-nowrap px-2 hover:bg-secondary-100 group flex justify-between rounded-md">
          <span className="w-full flex items-center">
            <div
              className="mr-2 flex items-center cursor-pointer"
              onClick={(e: MouseEvent) => {
                e.stopPropagation()

                setOpen(!isOpen)
              }}
            >
              <IconChevron
                className={clsx(`inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-500`, {
                  'invisible pointer-events-none cursor-default':
                    !directory.withDirectories && !directory.withFiles,
                })}
              />
              <IconFolder className={`inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-500`} />
            </div>
            <span className="w-full h-[1.5rem] flex items-center cursor-pointer justify-between">
              {renamingDirectory?.id === directory.id ? (
                <div className="flex items-center">
                  <input
                    type="text"
                    className="w-full text-sm overflow-hidden overflow-ellipsis group-hover:text-secondary-500"
                    value={newName === '' ? directory.name : newName}
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

                        rename()
                      }}
                    />
                  </div>
                </div>
              ) : (
                <span className="w-full flex justify-between items-center">
                  <span
                    onClick={(e: MouseEvent) => {
                      e.stopPropagation()

                      setOpen(!isOpen)
                    }}
                    onDoubleClick={(e: MouseEvent) => {
                      e.stopPropagation()

                      setRenamingDirectory(directory)
                    }}
                    className="w-full text-sm overflow-hidden overflow-ellipsis group-hover:text-secondary-500"
                  >
                    {directory.name}
                  </span>
                  <span className="hidden group-hover:block">
                    <DocumentPlusIcon
                      onClick={createFile}
                      className={`cursor-pointer inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-300 hover:text-secondary-500`}
                    />
                    <FolderPlusIcon
                      onClick={createDirectory}
                      className={`cursor-pointer inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-300 hover:text-secondary-500`}
                    />
                    <XCircleIcon
                      onClick={remove}
                      className={`cursor-pointer inline-block ${CSS_ICON_SIZE} ml-2 text-danger-300 hover:text-danger-500`}
                    />
                  </span>
                </span>
              )}
            </span>
          </span>
        </span>
      )}
      {(isOpen || isRoot) && directory.withDirectories && (
        <ul className="overflow-hidden">
          {directory.directories.map(dir => (
            <li
              key={dir.id}
              className="border-l px-1"
              title={dir.name}
            >
              <Directory directory={dir} />
            </li>
          ))}
        </ul>
      )}
      {(isOpen || isRoot) && directory.withFiles && (
        <ul
          className={clsx('mr-1 overflow-hidden', directory.withParent ? 'ml-4' : 'ml-[2px] mt-1')}
        >
          {directory.files.map(file => (
            <li
              key={file.id}
              title={file.name}
              className={clsx('pl-1', file.parent?.withParent != null && 'border-l')}
            >
              <File file={file} />
            </li>
          ))}
        </ul>
      )}
    </>
  )
}

function File({ file }: PropsFile): JSX.Element {
  const activeFileId = useStoreFileTree(s => s.activeFileId)
  const openedFiles = useStoreFileTree(s => s.openedFiles)
  const setOpenedFiles = useStoreFileTree(s => s.setOpenedFiles)
  const selectFile = useStoreFileTree(s => s.selectFile)

  const [isLoading, setIsLoading] = useState(false)
  const [renamingFile, setRenamingFile] = useState<ModelFile>()
  const [newName, setNewName] = useState<string>('')

  function remove(file: ModelFile): void {
    if (isLoading) return

    setIsLoading(true)

    setTimeout(() => {
      openedFiles.delete(file.id)

      file.parent?.removeFile(file)

      setOpenedFiles(openedFiles)
      setIsLoading(false)
    })
  }

  function rename(): void {
    if (isLoading || renamingFile == null) return

    setIsLoading(true)

    const shouldSelectAfterRename = activeFileId === renamingFile.id

    openedFiles.delete(renamingFile.id)

    renamingFile.rename(newName.trim().replace(`.${renamingFile.extension}`, ''))

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
        'text-base whitespace-nowrap group/file px-2 flex justify-between rounded-md',
        file.id === activeFileId ? 'text-secondary-500' : 'text-gray-800',
        file.is_supported && 'group cursor-pointer hover:bg-secondary-100'
      )}
    >
      <span
        className={clsx(
          'flex w-full items-center overflow-hidden overflow-ellipsis',
          !file.is_supported && 'opacity-50 cursor-not-allowed text-gray-800'
        )}
      >
        <div className="flex items-center">
          {openedFiles?.has(file.id) ? (
            <DocumentIcon className={`inline-block ${CSS_ICON_SIZE} mr-3 text-secondary-500`} />
          ) : (
            <DocumentIconOutline
              className={`inline-block ${CSS_ICON_SIZE} mr-3 text-secondary-500`}
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

                file.is_supported && file.id !== activeFileId && selectFile(file)
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

                remove(file)
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
