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
import { useDrop, useDrag } from 'react-dnd'
import { type PropsArtifact } from './FileExplorer'
import { type ModelArtifact } from '@models/artifact'
import { getEmptyImage } from 'react-dnd-html5-backend'

interface PropsDirectory extends PropsArtifact {
  directory: ModelDirectory
  createFile: (parent: ModelDirectory) => void
  createDirectory: (parent: ModelDirectory) => void
  moveArtifacts: (artifacts: Set<ModelArtifact>, target: ModelDirectory) => void
}

const Directory = function Directory({
  directory,
  createDirectory,
  createFile,
  removeArtifactWithConfirmation,
  renameAtrifact,
  moveArtifacts,
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
  const [isOpen, setIsOpen] = useState<boolean>(directory.isOpened)
  const [isOpenContextMenu, setIsOpenContextMenu] = useState(false)

  const [{ isOver }, drop] = useDrop(
    () => ({
      accept: 'artifact',
      drop(artifact: ModelArtifact, monitor) {
        if (isFalse(monitor.canDrop())) return

        const artifacts = activeRange.has(artifact)
          ? activeRange
          : new Set([artifact])

        moveArtifacts(artifacts, directory)

        directory.open()
      },
      canDrop(artifact) {
        const artifacts = Array.from(
          activeRange.has(artifact) ? activeRange : [artifact],
        )

        return artifacts.every(item => {
          if (item.parent === directory || item === directory) return false
          if (item instanceof ModelDirectory && item.hasDirectory(directory))
            return false

          return true
        })
      },
      collect(monitor) {
        return {
          isOver: monitor.isOver({ shallow: true }) && monitor.canDrop(),
        }
      },
    }),
    [activeRange],
  )

  const [{ isDragging }, drag, preview] = useDrag(
    () => ({
      type: 'artifact',
      item: directory,
      canDrag(monitor) {
        return isStringEmptyOrNil(newName)
      },
      collect(monitor) {
        return {
          isDragging: monitor.isDragging(),
        }
      },
    }),
    [directory, newName],
  )

  useEffect(() => {
    // Update component every time ModelDirectory's state "isOpen" is changing
    directory.syncStateOpen = setIsOpen
  }, [])

  useEffect(() => {
    preview(getEmptyImage(), { captureDraggingState: true })
  }, [preview])

  useEffect(() => {
    if (isFalse(isOpen) && tab != null && directory.hasFile(tab.file)) {
      directory.open()
    }
  }, [tab])

  function handleSelect(e: MouseEvent): void {
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

  return (
    <div className={clsx('h-full', isDragging && 'opacity-50')}>
      <div
        ref={drop}
        className={clsx(isOver && isFalse(isDragging) && 'bg-brand-5')}
      >
        {directory.withParent && (
          <div ref={directory.withParent ? drag : undefined}>
            <DirectoryContainer
              directory={directory}
              className={clsx(
                'w-full overflow-hidden group flex rounded-md',
                isFalse(isStringEmptyOrNil(newName)) && 'bg-primary-800',
                isOpenContextMenu && 'bg-primary-10',
                className,
              )}
              style={style}
              handleSelect={handleSelect}
            >
              <DirectoryIcons
                isOpen={isOpen || (isOver && isFalse(isDragging))}
              />
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
            </DirectoryContainer>
          </div>
        )}
        {((isOver && isFalse(isDragging)) || isOpen || !directory.withParent) &&
          directory.withDirectories && (
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
                    removeArtifactWithConfirmation={
                      removeArtifactWithConfirmation
                    }
                    renameAtrifact={renameAtrifact}
                    moveArtifacts={moveArtifacts}
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
        {((isOver && isFalse(isDragging)) || isOpen || !directory.withParent) &&
          directory.withFiles && (
            <ul className={clsx(activeRange.has(directory) && 'bg-brand-5')}>
              {directory.files.map(file => (
                <li
                  key={file.id}
                  title={file.name}
                >
                  <File
                    file={file}
                    removeArtifactWithConfirmation={
                      removeArtifactWithConfirmation
                    }
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
  const [isAllDirectories, shouldClose, shouldOpen, shouldToggle] =
    useMemo(() => {
      if (activeRange.size < 2) return [false, false, false, false]

      let isAllDirectories = true
      let isAllOpened = true
      let isAllsClosed = true

      for (const atrifact of Array.from(activeRange)) {
        if (atrifact instanceof ModelDirectory && isAllDirectories) {
          if (isAllOpened) {
            isAllOpened = atrifact.isOpened
          }

          if (isAllsClosed) {
            isAllsClosed = atrifact.isClosed
          }
        } else {
          isAllDirectories = false
        }
      }

      return [
        isAllDirectories,
        isAllOpened,
        isAllsClosed,
        isFalse(isAllOpened) && isFalse(isAllsClosed),
      ]
    }, [activeRange])

  return (
    <ContextMenu.Root onOpenChange={onOpenChange}>
      <ContextMenu.Trigger className="w-full">
        <DirectoryDisplay directory={directory} />
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
                  if (shouldClose) {
                    artifact.collapse()
                  } else if (shouldOpen) {
                    artifact.expand()
                  } else if (shouldToggle) {
                    if (artifact.isOpened) {
                      artifact.collapse()
                    } else {
                      artifact.expand()
                    }
                  }
                })
              } else {
                if (directory.isOpened) {
                  directory.collapse()
                } else {
                  directory.expand()
                }
              }

              setActiveRange(new Set())
            }}
          >
            {isAllDirectories && shouldClose && 'Collapse All'}
            {isAllDirectories && shouldOpen && 'Expand All'}
            {isAllDirectories && shouldToggle && 'Toggle All'}
            {isFalse(isAllDirectories) && directory.isOpened && 'Collapse'}
            {isFalse(isAllDirectories) && directory.isClosed && 'Expand'}
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

function DirectoryDisplay({
  directory,
}: {
  directory: ModelDirectory
}): JSX.Element {
  return (
    <div className="w-full flex justify-between items-center overflow-hidden overflow-ellipsis py-[0.125rem]">
      <span className="overflow-hidden overflow-ellipsis">
        {directory.name}
      </span>
      <span className="inline-block text-xs rounded-full px-2 bg-primary-10 ml-2">
        {directory.directories.length + directory.files.length}
      </span>
    </div>
  )
}

function DirectoryContainer({
  directory,
  className,
  style,
  children,
  handleSelect,
}: {
  directory: ModelDirectory
  children: React.ReactNode
  className?: string
  style?: React.CSSProperties
  handleSelect?: (e: MouseEvent) => void
}): JSX.Element {
  const activeRange = useStoreFileExplorer(s => s.activeRange)

  return (
    <span
      className={clsx(
        'w-full overflow-hidden group flex rounded-md px-2',
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
      {children}
    </span>
  )
}

function DirectoryIcons({
  hasChevron = true,
  hasFolder = true,
  isOpen = false,
  className,
}: {
  isOpen?: boolean
  hasChevron?: boolean
  hasFolder?: boolean
  className?: string
}): JSX.Element {
  const IconChevron = isOpen ? ChevronDownIcon : ChevronRightIcon
  const IconFolder = isOpen ? FolderOpenIcon : FolderIcon

  return (
    <div className={clsx('flex items-center mr-2', className)}>
      {hasChevron && <IconChevron className="inline-block w-5" />}
      {hasFolder && (
        <IconFolder className="inline-block w-4 fill-primary-500" />
      )}
    </div>
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

Directory.Container = DirectoryContainer
Directory.Icons = DirectoryIcons
Directory.Display = DirectoryDisplay

export default Directory
