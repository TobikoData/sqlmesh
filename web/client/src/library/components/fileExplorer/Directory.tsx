import { useState, type MouseEvent, useEffect, useMemo } from 'react'
import { FolderOpenIcon, FolderIcon } from '@heroicons/react/24/solid'
import { ChevronDownIcon, ChevronRightIcon } from '@heroicons/react/20/solid'
import clsx from 'clsx'
import { ModelDirectory } from '~/models'
import { isArrayNotEmpty, isFalse, isNotNil, isStringEmptyOrNil } from '~/utils'
import { useStoreProject } from '@context/project'
import File from './File'
import * as ContextMenu from '@radix-ui/react-context-menu'
import { useDrop, useDrag } from 'react-dnd'
import { type ModelArtifact } from '@models/artifact'
import { getEmptyImage } from 'react-dnd-html5-backend'
import { useFileExplorer } from './context'
import FileExplorer from './FileExplorer'
import { useLongPress } from '@uidotdev/usehooks'
const Directory = function Directory({
  directory,
  className,
  style,
}: {
  directory: ModelDirectory
  className?: string
  style?: React.CSSProperties
}): JSX.Element {
  const selectedFile = useStoreProject(s => s.selectedFile)

  const [newName, setNewName] = useState<string>()
  const [isOpen, setIsOpen] = useState<boolean>(directory.isOpened)
  const [isOpenContextMenu, setIsOpenContextMenu] = useState(false)
  const [isDraggable, setIsDraggable] = useState(false)

  const attrs = useLongPress(() => setIsDraggable(true), {
    threshold: 500,
    onFinish() {
      setIsDraggable(false)
    },
    onCancel() {
      setIsDraggable(false)
    },
  })

  const {
    activeRange,
    setActiveRange,
    createDirectory,
    createFile,
    moveArtifacts,
    removeArtifactWithConfirmation,
    selectArtifactsInRange,
  } = useFileExplorer()

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
      canDrop(artifact, monitor) {
        const artifacts = Array.from(
          activeRange.has(artifact) ? activeRange : [artifact],
        )

        return (
          monitor.isOver({ shallow: true }) &&
          isArrayNotEmpty(artifacts) &&
          artifacts.reduce((acc, item) => {
            if (isFalse(acc)) return false

            if (item.parent === directory || item === directory) return false
            if (item instanceof ModelDirectory && item.hasDirectory(directory))
              return false

            return true
          }, true)
        )
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
      end() {
        setIsDraggable(false)
      },
      canDrag() {
        return isStringEmptyOrNil(newName) && isDraggable
      },
      collect(monitor) {
        return {
          isDragging: monitor.isDragging(),
        }
      },
    }),
    [directory, newName, isDraggable],
  )

  const [isAllDirectories, shouldClose, shouldOpen, shouldToggle] =
    useMemo(() => {
      if (activeRange.size < 2) return [false, false, false, false]

      let isAllDirectories = true
      let isAllOpened = true
      let isAllClosed = true

      for (const artifact of Array.from(activeRange)) {
        if (artifact instanceof ModelDirectory && isAllDirectories) {
          if (isAllOpened) {
            isAllOpened = artifact.isOpened
          }

          if (isAllClosed) {
            isAllClosed = artifact.isClosed
          }
        } else {
          isAllDirectories = false
        }
      }

      return [
        isAllDirectories,
        isAllOpened,
        isAllClosed,
        isFalse(isAllOpened) && isFalse(isAllClosed),
      ]
    }, [activeRange])

  useEffect(() => {
    // Setting syncStateOpen in order to have a mechanism
    // to trigger re-render every time ModelDirectory's "isOpen" state is changes
    directory.syncStateOpen = setIsOpen
  }, [])

  useEffect(() => {
    preview(getEmptyImage(), { captureDraggingState: true })
  }, [preview])

  useEffect(() => {
    if (selectedFile == null || isOpen) return

    if (directory.hasFile(selectedFile)) {
      directory.open()
    }
  }, [selectedFile])

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
        activeRange.clear()
      }

      activeRange.add(directory)

      setActiveRange(activeRange)
    }
  }

  const disabled = activeRange.size > 1 && activeRange.has(directory)

  return (
    <div className={clsx('h-full', isDragging && 'opacity-50')}>
      <div
        ref={drop}
        className={clsx(isOver && isFalse(isDragging) && 'bg-brand-5')}
      >
        {directory.withParent && (
          <div
            {...attrs}
            ref={directory.withParent ? drag : undefined}
          >
            <FileExplorer.Container
              artifact={directory}
              className={clsx(
                isFalse(isStringEmptyOrNil(newName)) && 'bg-primary-800',
                isDraggable && 'bg-primary-10 !cursor-grabbing',
                isOpenContextMenu && 'bg-primary-10',
                className,
              )}
              style={style}
              handleSelect={handleSelect}
            >
              <Directory.Icons
                isOpen={isOpen || (isOver && isFalse(isDragging))}
              />
              {isStringEmptyOrNil(newName) ? (
                <FileExplorer.ContextMenu
                  trigger={
                    <FileExplorer.ContextMenuTrigger>
                      <Directory.Display directory={directory} />
                    </FileExplorer.ContextMenuTrigger>
                  }
                  onOpenChange={setIsOpenContextMenu}
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
                        ;(activeRange as Set<ModelDirectory>).forEach(
                          artifact => {
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
                          },
                        )
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
                    {isFalse(isAllDirectories) &&
                      directory.isOpened &&
                      'Collapse'}
                    {isFalse(isAllDirectories) &&
                      directory.isClosed &&
                      'Expand'}
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

                      createFile(directory)
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

                      createDirectory(directory)
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

                      removeArtifactWithConfirmation(directory)
                    }}
                  >
                    Remove {activeRange.has(directory) ? activeRange.size : ''}
                    <div className="ml-auto pl-5"></div>
                  </ContextMenu.Item>
                </FileExplorer.ContextMenu>
              ) : (
                <FileExplorer.Rename
                  artifact={directory}
                  newName={newName}
                  setNewName={setNewName}
                />
              )}
            </FileExplorer.Container>
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

function DirectoryDisplay({
  directory,
}: {
  directory: ModelDirectory
}): JSX.Element {
  return (
    <div className="w-full flex justify-between items-center py-[0.125rem]">
      <span className="overflow-hidden overflow-ellipsis">
        {directory.name}
      </span>
      <span className="inline-block text-xs rounded-full px-2 bg-primary-10 ml-2">
        {directory.directories.length + directory.files.length}
      </span>
    </div>
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

Directory.Icons = DirectoryIcons
Directory.Display = DirectoryDisplay

export default Directory
