import { useState, type MouseEvent, useEffect } from 'react'
import {
  FolderOpenIcon,
  FolderIcon,
  CheckCircleIcon,
  DocumentPlusIcon,
  FolderPlusIcon,
  XCircleIcon,
  ArrowsUpDownIcon,
} from '@heroicons/react/24/solid'
import { ChevronDownIcon, ChevronRightIcon } from '@heroicons/react/20/solid'
import clsx from 'clsx'
import {
  writeDirectoryApiDirectoriesPathPost,
  writeFileApiFilesPathPost,
  deleteDirectoryApiDirectoriesPathDelete,
} from '~/api/client'
import { ModelDirectory, ModelFile } from '~/models'
import { isFalse, isStringEmptyOrNil } from '~/utils'
import { type WithConfirmation } from '../modal/ModalConfirmation'
import { toUniqueName, getAllFilesInDirectory } from './help'
import { useStoreEditor } from '~/context/editor'
import { useStoreFileExplorer } from '~/context/fileTree'
import File from './File'

interface PropsDirectory extends WithConfirmation {
  directory: ModelDirectory
  className?: string
  style?: React.CSSProperties
}

const CSS_ICON_SIZE = 'w-4 h-4'

export default function Directory({
  directory,
  setConfirmation,
  className,
  style,
}: PropsDirectory): JSX.Element {
  const tab = useStoreEditor(s => s.tab)
  const closeTab = useStoreEditor(s => s.closeTab)

  const activeRange = useStoreFileExplorer(s => s.activeRange)
  const selectFile = useStoreFileExplorer(s => s.selectFile)
  const refreshProject = useStoreFileExplorer(s => s.refreshProject)

  const [isLoading, setIsLoading] = useState(false)
  const [newName, setNewName] = useState<string>()
  const [isOpen, setIsOpen] = useState<boolean>(directory.isOpen)

  const IconChevron = isOpen ? ChevronDownIcon : ChevronRightIcon
  const IconFolder = isOpen ? FolderOpenIcon : FolderIcon

  useEffect(() => {
    // Update component every time ModelDirectory's state "isOpen" is changing
    directory.syncStateOpen = setIsOpen
  }, [])

  useEffect(() => {
    if (isFalse(isOpen) && tab != null && directory.hasFile(tab.file)) {
      directory.open()
    }
  }, [tab])

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
        directory.open()

        refreshProject()
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

    const extension = directory.isModels ? '.sql' : '.py'
    const name = toUniqueName('new_file', extension)

    writeFileApiFilesPathPost(`${directory.path}/${name}`, { content: '' })
      .then(created => {
        if (isFalse((created as any).ok)) {
          console.warn([`File: ${directory.path}`, (created as any).detail])

          return
        }

        const file = new ModelFile(created, directory)

        directory.addFile(file)
        directory.open()

        refreshProject()
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
            closeTab(file)
          })
        }

        directory.parent?.removeDirectory(directory)

        refreshProject()
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

    directory.rename(newName.trim())

    void writeDirectoryApiDirectoriesPathPost(currentPath, {
      new_path: directory.path,
    })
      .catch(error => {
        console.log(error)

        directory.rename(currentName)
      })
      .finally(() => {
        setNewName(undefined)
        setIsLoading(false)

        if (tab != null && directory.hasFile(tab.file)) {
          selectFile(tab.file)
        }
      })
  }

  function renameWithConfirmation(): void {
    if (directory.name === newName) {
      setNewName(undefined)
    } else {
      setConfirmation({
        headline: 'Renaming Directory',
        description: `Are you sure you want to rename the directory "${directory.name}"?`,
        yesText: 'Yes, Rename',
        noText: 'No, Cancel',
        action: rename,
        cancel: () => {
          setNewName(undefined)
        },
      })
    }
  }

  return (
    <>
      {directory.withParent && (
        <span
          className={clsx(
            'w-full overflow-hidden group flex justify-between items-center rounded-md py-[0.125rem] pr-2',
            isFalse(isStringEmptyOrNil(newName)) && 'bg-primary-800',
            activeRange.has(directory) &&
              'text-brand-100 bg-brand-500 dark:bg-brand-700',
            className,
          )}
          style={style}
          onClick={(e: MouseEvent) => {
            e.stopPropagation()

            selectFile(directory)

            directory.toggle()
          }}
        >
          <div className="flex items-center">
            <IconChevron className="inline-block w-5" />
            <IconFolder className="inline-block w-4 fill-primary-500" />
          </div>
          <span className="w-full overflow-hidden flex items-center justify-between ml-1">
            {isStringEmptyOrNil(newName) ? (
              <span className="w-full flex overflow-hidden items-center cursor-default">
                <span
                  className="w-full overflow-hidden overflow-ellipsis justify-between"
                  // onClick={(e: MouseEvent) => {
                  //   e.stopPropagation()

                  //   directory.toggle()
                  // }}
                  // onDoubleClick={(e: MouseEvent) => {
                  //   e.stopPropagation()

                  //   setNewName(directory.name)
                  // }}
                >
                  {directory.name}
                </span>
                <span className="hidden w-full group-hover:flex items-center justify-end">
                  <ArrowsUpDownIcon
                    onClick={(e: MouseEvent) => {
                      e.stopPropagation()

                      if (directory.isCollapsed) {
                        directory.expand()
                      } else {
                        directory.collapse()
                      }
                    }}
                    className={clsx(
                      `cursor-pointer inline-block ${CSS_ICON_SIZE} mr-1`,
                      directory.isCollapsed &&
                        'text-neutral-500 dark:text-neutral-100',
                      directory.isExpanded &&
                        'text-secondary-500 dark:text-primary-500',
                    )}
                  />
                  <DocumentPlusIcon
                    onClick={createFile}
                    className={`cursor-pointer inline-block ${CSS_ICON_SIZE} mr-1 text-neutral-500 dark:text-neutral-100`}
                  />
                  <FolderPlusIcon
                    onClick={createDirectory}
                    className={`cursor-pointer inline-block ${CSS_ICON_SIZE} mr-1 text-neutral-500 dark:text-neutral-100`}
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
              <div className="flex w-full items-center">
                <input
                  type="text"
                  className="w-full overflow-hidden overflow-ellipsis bg-primary-900 text-primary-100"
                  value={newName ?? directory.name}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setNewName(e.target.value)
                  }}
                />
                <div className="flex">
                  <CheckCircleIcon
                    className={`inline-block w-5 ml-2 text-success-500 cursor-pointer`}
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
        <ul className={clsx(activeRange.has(directory) && 'bg-brand-10')}>
          {directory.directories.map(dir => (
            <li
              key={dir.id}
              title={dir.name}
            >
              <Directory
                directory={dir}
                setConfirmation={setConfirmation}
                style={{
                  paddingLeft: directory.withParent
                    ? `${directory.level / 2}rem`
                    : 0,
                }}
              />
            </li>
          ))}
        </ul>
      )}
      {(isOpen || !directory.withParent) && directory.withFiles && (
        <ul className={clsx(activeRange.has(directory) && 'bg-brand-10')}>
          {directory.files.map(file => (
            <li
              key={file.id}
              title={file.name}
            >
              <File
                file={file}
                setConfirmation={setConfirmation}
                style={{
                  paddingLeft: directory.withParent
                    ? `${directory.level / 2}rem`
                    : 0,
                }}
              />
            </li>
          ))}
        </ul>
      )}
    </>
  )
}
