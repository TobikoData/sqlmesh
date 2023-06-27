import { useState, type MouseEvent, useEffect, useMemo } from 'react'
import {
  FolderOpenIcon,
  FolderIcon,
  CheckCircleIcon,
} from '@heroicons/react/24/solid'
import { ChevronDownIcon, ChevronRightIcon } from '@heroicons/react/20/solid'
import clsx from 'clsx'
import { ModelDirectory } from '~/models'
import { isFalse, isNotNil, isStringEmptyOrNil } from '~/utils'
import { useStoreEditor } from '~/context/editor'
import { useStoreFileExplorer } from '~/context/fileTree'
import File from './File'
import * as ContextMenu from '@radix-ui/react-context-menu'
import { ModelArtifact } from '@models/artifact'

interface PropsDirectory {
  directory: ModelDirectory
  createFile: (parent: ModelDirectory) => void
  createDirectory: (parent: ModelDirectory) => void
  removeArtifactWithConfirmation: (artifact: ModelArtifact) => void
  renameAtrifact: (artifact: ModelArtifact, newName?: string) => void
  className?: string
  style?: React.CSSProperties
}

export default function Directory({
  directory,
  createDirectory,
  createFile,
  removeArtifactWithConfirmation,
  renameAtrifact,
  className,
  style,
}: PropsDirectory): JSX.Element {
  const tab = useStoreEditor(s => s.tab)

  const activeRange = useStoreFileExplorer(s => s.activeRange)
  const setSelected = useStoreFileExplorer(s => s.setSelected)
  const setActiveRange = useStoreFileExplorer(s => s.setActiveRange)
  const selectArtifactsInRange = useStoreFileExplorer(
    s => s.selectArtifactsInRange,
  )

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

  function handleSelect(e: MouseEvent) {
    e.stopPropagation()

    if (e.shiftKey || e.metaKey) {
      e.preventDefault()
    } else {
      if (isNotNil(newName)) return

      directory.toggle()
    }

    if (e.metaKey) {
      if (activeRange.has(directory)) {
        activeRange.delete(directory)
      } else {
        activeRange.add(directory)
      }
      setActiveRange(activeRange)
    } else if (e.shiftKey) {
      selectArtifactsInRange(directory)
    } else {
      if (activeRange.size > 0) {
        setActiveRange(new Set([directory]))
      } else {
        setSelected(directory)
      }
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
          onClick={handleSelect}
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
              removeWithConfirmation={() =>
                removeArtifactWithConfirmation(directory)
              }
              setNewName={setNewName}
            />
          ) : (
            <DirectoryRename
              directory={directory}
              newName={newName}
              setNewName={setNewName}
              rename={() => renameAtrifact(directory, newName)}
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
                createFile={createFile}
                createDirectory={createDirectory}
                removeArtifactWithConfirmation={removeArtifactWithConfirmation}
                renameAtrifact={renameAtrifact}
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
                removeArtifactWithConfirmation={removeArtifactWithConfirmation}
                renameAtrifact={renameAtrifact}
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
  setNewName: (newName?: string) => void
  onOpenChange: (isOpen: boolean) => void
}): JSX.Element {
  const activeRange = useStoreFileExplorer(s => s.activeRange)
  const setActiveRange = useStoreFileExplorer(s => s.setActiveRange)

  const disabled = activeRange.size > 1 && activeRange.has(directory)
  const [isAllDirectories, shouldCollapse, shouldExpand, shouldToggle] =
    useMemo(() => {
      if (activeRange.size < 2) return [false, false, false, false]

      let isAllDirectories = true
      let isAllCollapsed = true
      let isAllsExpanded = true

      for (const atrifact of Array.from(activeRange)) {
        if (atrifact instanceof ModelDirectory && isAllDirectories) {
          if (isAllCollapsed) {
            isAllCollapsed = atrifact.isCollapsed
          }

          if (isAllsExpanded) {
            isAllsExpanded = atrifact.isExpanded
          }
        } else {
          isAllDirectories = false
        }
      }

      return [
        isAllDirectories,
        isAllsExpanded,
        isAllCollapsed,
        isFalse(isAllCollapsed) && isFalse(isAllsExpanded),
      ]
    }, [activeRange])

  return (
    <ContextMenu.Root onOpenChange={onOpenChange}>
      <ContextMenu.Trigger className="w-full flex justify-between items-center overflow-hidden overflow-ellipsis py-[0.125rem] pr-2">
        <span className="overflow-hidden overflow-ellipsis">
          {directory.name}
        </span>
        <span className="inline-block text-xs rounded-full px-2 bg-primary-10 ml-2">
          {directory.directories.length + directory.files.length}
        </span>
      </ContextMenu.Trigger>
      <ContextMenu.Portal>
        <ContextMenu.Content
          className="bg-light rounded-md overflow-hiddin shadow-lg py-2 px-1"
          onClick={(e: MouseEvent) => {
            e.stopPropagation()
          }}
        >
          <ContextMenu.Item
            className={clsx(
              'py-1.5 group leading-none rounded-md flex items-center relative pl-6 pr-2 select-none outline-none font-medium text-xs text-neutral-500 ',
              disabled && isFalse(isAllDirectories)
                ? 'opacity-50 cursor-not-allowed'
                : 'hover:bg-accent-500 hover:text-light',
            )}
            disabled={disabled && isFalse(isAllDirectories)}
            onSelect={(e: Event) => {
              e.stopPropagation()

              if (activeRange.size > 1 && activeRange.has(directory)) {
                ;(activeRange as Set<ModelDirectory>).forEach(artifact => {
                  if (shouldCollapse) {
                    artifact.collapse()
                  } else if (shouldExpand) {
                    artifact.expand()
                  } else if (shouldToggle) {
                    if (artifact.isCollapsed) {
                      artifact.expand()
                    } else {
                      artifact.collapse()
                    }
                  }
                })
              } else {
                if (directory.isCollapsed) {
                  directory.expand()
                } else {
                  directory.collapse()
                }
              }

              setActiveRange(new Set())
            }}
          >
            {isAllDirectories && shouldCollapse && 'Collapse All'}
            {isAllDirectories && shouldExpand && 'Expand All'}
            {isAllDirectories && shouldToggle && 'Toggle All'}
            {isFalse(isAllDirectories) && directory.isCollapsed && 'Expand'}
            {isFalse(isAllDirectories) && directory.isExpanded && 'Collapse'}
            <div className="ml-auto pl-5"></div>
          </ContextMenu.Item>
          <ContextMenu.Separator className="h-[1px] bg-accent-200 m-2" />
          <ContextMenu.Item
            className={clsx(
              'py-1.5 group leading-none rounded-md flex items-center relative pl-6 pr-2 select-none outline-none font-medium text-xs text-neutral-500 ',
              disabled
                ? 'opacity-50 cursor-not-allowed'
                : 'hover:bg-accent-500 hover:text-light',
            )}
            disabled={disabled}
            onSelect={(e: Event) => {
              e.stopPropagation()

              createFile()
            }}
          >
            New File
            <div className="ml-auto pl-5"></div>
          </ContextMenu.Item>
          <ContextMenu.Item
            className={clsx(
              'py-1.5 group leading-none rounded-md flex items-center relative pl-6 pr-2 select-none outline-none font-medium text-xs text-neutral-500 ',
              disabled
                ? 'opacity-50 cursor-not-allowed'
                : 'hover:bg-accent-500 hover:text-light',
            )}
            disabled={disabled}
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
            className={clsx(
              'py-1.5 group leading-none rounded-md flex items-center relative pl-6 pr-2 select-none outline-none font-medium text-xs text-neutral-500 ',
              disabled
                ? 'opacity-50 cursor-not-allowed'
                : 'hover:bg-accent-500 hover:text-light',
            )}
            disabled={disabled}
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
            Remove {activeRange.has(directory) ? activeRange.size : ''}
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
  rename,
}: {
  directory: ModelDirectory
  setNewName: (newName?: string) => void
  rename: () => void
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

            rename()
            setNewName(undefined)
          }}
        />
      </div>
    </div>
  )
}
