import { useState, type MouseEvent, useEffect } from 'react'
import {
  FolderOpenIcon,
  FolderIcon,
  CheckCircleIcon,
} from '@heroicons/react/24/solid'
import { ChevronDownIcon, ChevronRightIcon } from '@heroicons/react/20/solid'
import clsx from 'clsx'
import { writeDirectoryApiDirectoriesPathPost } from '~/api/client'
import { ModelDirectory } from '~/models'
import { isFalse, isNotNil, isStringEmptyOrNil } from '~/utils'
import { type WithConfirmation } from '../modal/ModalConfirmation'
import { useStoreEditor } from '~/context/editor'
import { useStoreFileExplorer } from '~/context/fileTree'
import File from './File'
import * as ContextMenu from '@radix-ui/react-context-menu'
import { ModelArtifact } from '@models/artifact'

interface PropsDirectory extends WithConfirmation {
  directory: ModelDirectory
  className?: string
  style?: React.CSSProperties
  createFile: (parent: ModelDirectory) => void
  createDirectory: (parent: ModelDirectory) => void
  removeArtifacts: (artifacts: Set<ModelArtifact>) => void
}

export default function Directory({
  directory,
  setConfirmation,
  createDirectory,
  createFile,
  removeArtifacts,
  className,
  style,
}: PropsDirectory): JSX.Element {
  const tab = useStoreEditor(s => s.tab)

  const activeRange = useStoreFileExplorer(s => s.activeRange)
  const selected = useStoreFileExplorer(s => s.selected)
  const selectFile = useStoreFileExplorer(s => s.selectFile)
  const setActiveRange = useStoreFileExplorer(s => s.setActiveRange)

  const [isLoading, setIsLoading] = useState(false)
  const [newName, setNewName] = useState<string>()
  const [isOpen, setIsOpen] = useState<boolean>(directory.isOpen)
  const [isOpenContextMenu, setIsOpenContextMenu] = useState(false)

  useEffect(() => {
    // Update component every time ModelDirectory's state "isOpen" is changing
    directory.syncStateOpen = setIsOpen
  }, [])

  useEffect(() => {
    if (isFalse(isOpen) && tab != null && directory.hasFile(tab.file)) {
      directory.open()
    }
  }, [tab])

  function removeWithConfirmation(): void {
    setConfirmation({
      headline: 'Removing Directory',
      description: `Are you sure you want to remove the directory "${directory.name}"?`,
      yesText: 'Yes, Remove',
      noText: 'No, Cancel',
      action: () => {
        if (directory.parent != null) {
          removeArtifacts(new Set([directory]))
        }
      },
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

  const IconChevron = isOpen ? ChevronDownIcon : ChevronRightIcon
  const IconFolder = isOpen ? FolderOpenIcon : FolderIcon

  return (
    <div className="h-full">
      {directory.withParent && (
        <span
          className={clsx(
            'w-full overflow-hidden group flex rounded-md',
            isFalse(isStringEmptyOrNil(newName)) && 'bg-primary-800',
            isOpenContextMenu && 'bg-primary-10',
            activeRange.has(directory) &&
              'text-brand-100 bg-brand-500 dark:bg-brand-700',
            className,
          )}
          style={style}
          onContextMenu={(e: MouseEvent) => {
            e.stopPropagation()
          }}
          onClick={(e: MouseEvent) => {
            e.stopPropagation()

            if (e.shiftKey) {
              e.preventDefault()
            } else {
              if (isNotNil(newName)) return

              directory.toggle()
            }

            if (e.shiftKey && activeRange.size > 0) {
              activeRange.add(directory)
              setActiveRange(activeRange)
            } else if (directory !== selected) {
              selectFile(directory)
            }
          }}
        >
          <div className="flex items-center mr-2">
            <IconChevron className="inline-block w-5" />
            <IconFolder className="inline-block w-4 fill-primary-500" />
          </div>
          {isStringEmptyOrNil(newName) ? (
            <ContextMenuDirectory
              onOpenChange={setIsOpenContextMenu}
              directory={directory}
              createDirectory={() => createDirectory(directory)}
              createFile={() => createFile(directory)}
              removeWithConfirmation={removeWithConfirmation}
              setNewName={setNewName}
            />
          ) : (
            <DirectoryRename
              directory={directory}
              newName={newName}
              setNewName={setNewName}
              renameWithConfirmation={renameWithConfirmation}
            />
          )}
        </span>
      )}
      {(isOpen || !directory.withParent) && directory.withDirectories && (
        <ul className={clsx(activeRange.has(directory) && 'bg-brand-5')}>
          {directory.directories.map(dir => (
            <li
              key={dir.id}
              title={dir.name}
            >
              <Directory
                directory={dir}
                setConfirmation={setConfirmation}
                createFile={createFile}
                createDirectory={createDirectory}
                removeArtifacts={removeArtifacts}
                style={{
                  paddingLeft: directory.withParent
                    ? `${directory.level / 2 + 0.25}rem`
                    : 0,
                }}
              />
            </li>
          ))}
        </ul>
      )}
      {(isOpen || !directory.withParent) && directory.withFiles && (
        <ul className={clsx(activeRange.has(directory) && 'bg-brand-5')}>
          {directory.files.map(file => (
            <li
              key={file.id}
              title={file.name}
            >
              <File
                file={file}
                setConfirmation={setConfirmation}
                removeArtifacts={removeArtifacts}
                style={{
                  paddingLeft: directory.withParent
                    ? `${directory.level / 2 + 0.25}rem`
                    : 0,
                }}
              />
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

function ContextMenuDirectory({
  directory,
  createFile,
  createDirectory,
  setNewName,
  removeWithConfirmation,
  onOpenChange,
}: {
  directory: ModelDirectory
  createFile: () => void
  createDirectory: () => void
  removeWithConfirmation: () => void
  setNewName: (newName: string) => void
  onOpenChange: (isOpen: boolean) => void
}): JSX.Element {
  return (
    <ContextMenu.Root onOpenChange={onOpenChange}>
      <ContextMenu.Trigger className="w-full overflow-hidden overflow-ellipsis py-[0.125rem] pr-2">
        {directory.name}
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

              if (directory.isCollapsed) {
                directory.expand()
              } else {
                directory.collapse()
              }
            }}
          >
            {directory.isCollapsed ? 'Expand' : 'Collapse'}
            <div className="ml-auto pl-5"></div>
          </ContextMenu.Item>
          <ContextMenu.Separator className="h-[1px] bg-accent-200 m-2" />
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
          <ContextMenu.Separator className="h-[1px] bg-accent-200 m-2" />
          <ContextMenu.Item
            className="py-1.5 group leading-none rounded-md flex items-center relative pl-6 pr-2 select-none outline-none font-medium text-xs text-neutral-500 hover:bg-accent-500 hover:text-light"
            onClick={(e: MouseEvent) => {
              e.stopPropagation()
            }}
            onSelect={(e: Event) => {
              e.stopPropagation()

              setNewName(directory.name)
            }}
          >
            Rename
            <div className="ml-auto pl-5"></div>
          </ContextMenu.Item>
          <ContextMenu.Item
            className="py-1.5 group leading-none rounded-md flex items-center relative pl-6 pr-2 select-none outline-none font-medium text-xs text-danger-500 hover:bg-danger-500 hover:text-light"
            onSelect={(e: Event) => {
              e.stopPropagation()

              removeWithConfirmation()
            }}
          >
            Remove
            <div className="ml-auto pl-5"></div>
          </ContextMenu.Item>
        </ContextMenu.Content>
      </ContextMenu.Portal>
    </ContextMenu.Root>
  )
}

function DirectoryRename({
  directory,
  newName,
  setNewName,
  renameWithConfirmation,
}: {
  directory: ModelDirectory
  setNewName: (newName: string) => void
  renameWithConfirmation: () => void
  newName?: string
}): JSX.Element {
  return (
    <div className="flex w-full items-center py-[0.125rem] pr-2">
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
  )
}
