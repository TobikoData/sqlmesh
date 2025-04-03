import React, { type MouseEvent, useEffect, useState } from 'react'
import clsx from 'clsx'
import Directory from './Directory'
import { useStoreProject } from '@context/project'
import * as ContextMenu from '@radix-ui/react-context-menu'
import { DndProvider } from 'react-dnd'
import { HTML5Backend } from 'react-dnd-html5-backend'
import DragLayer from './DragLayer'
import { useFileExplorer } from './context'
import { type ModelArtifact } from '@models/artifact'
import SearchList from '@components/search/SearchList'
import { type ModelFile } from '@models/file'
import { EnumSize } from '~/types/enum'
import { useStoreContext } from '@context/context'
import { useApiFiles } from '@api/index'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'

/* TODO:
  - add drag and drop files/directories from desktop
  - add copy and paste
  - add accessability support
*/

const FileExplorer = function FileExplorer({
  className,
}: {
  className?: string
}): JSX.Element {
  const project = useStoreProject(s => s.project)
  const setSelectedFile = useStoreProject(s => s.setSelectedFile)
  const activeRange = useStoreProject(s => s.activeRange)
  const setActiveRange = useStoreProject(s => s.setActiveRange)

  const addConfirmation = useStoreContext(s => s.addConfirmation)

  const { isFetching: isFetchingFiles } = useApiFiles()

  const { removeArtifacts, createDirectory, createFile } = useFileExplorer()

  function handleKeyDown(e: React.KeyboardEvent): void {
    if (e.key === 'Escape') {
      setActiveRange([])
    }

    if (e.ctrlKey && e.key === 'Backspace' && activeRange.length > 0) {
      addConfirmation({
        headline: 'Removing Selected Files/Directories',
        description: `Are you sure you want to remove ${activeRange.length} items?`,
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
      tabIndex={0}
      className={clsx(
        'flex flex-col w-full h-full overflow-hidden text-sm text-neutral-500 dark:text-neutral-400 font-regular select-none',
        className,
      )}
      onKeyDown={handleKeyDown}
    >
      {isFetchingFiles ? (
        <div className="flex justify-center items-center w-full h-full">
          <Loading className="inline-block">
            <Spinner className="w-3 h-3 border border-neutral-10 mr-2" />
            <h3 className="text-md">Getting Project Files...</h3>
          </Loading>
        </div>
      ) : (
        <>
          <SearchList<ModelFile>
            list={project.allFiles}
            searchBy="path"
            displayBy="name"
            size={EnumSize.sm}
            onSelect={setSelectedFile}
            direction="top"
            isFullWidth
          />
          <FileExplorer.ContextMenu
            key={project.id}
            trigger={
              <FileExplorer.ContextMenuTrigger className="h-full pb-1">
                <DndProvider backend={HTML5Backend}>
                  <div className="w-full relative h-full px-1 overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical">
                    <DragLayer />
                    <Directory
                      key={project.id}
                      className="z-20 relative"
                      directory={project}
                    />
                  </div>
                </DndProvider>
              </FileExplorer.ContextMenuTrigger>
            }
          >
            <ContextMenu.Item
              className="py-1.5 group leading-none rounded-md flex items-center relative pl-6 pr-2 select-none outline-none font-medium text-xs text-neutral-500 hover:bg-accent-500 hover:text-light"
              onSelect={(e: Event) => {
                e.stopPropagation()

                createFile(project)
              }}
            >
              New File
              <div className="ml-auto pl-5"></div>
            </ContextMenu.Item>
            <ContextMenu.Item
              className="py-1.5 group leading-none rounded-md flex items-center relative pl-6 pr-2 select-none outline-none font-medium text-xs text-neutral-500 hover:bg-accent-500 hover:text-light"
              onSelect={(e: Event) => {
                e.stopPropagation()

                createDirectory(project)
              }}
            >
              New Folder
              <div className="ml-auto pl-5"></div>
            </ContextMenu.Item>
          </FileExplorer.ContextMenu>
        </>
      )}
    </div>
  )
}

function FileExplorerContextMenu({
  trigger,
  children,
  onOpenChange,
}: {
  trigger: React.ReactNode
  children: React.ReactNode
  onOpenChange?: (isOpen: boolean) => void
}): JSX.Element {
  return (
    <ContextMenu.Root onOpenChange={onOpenChange}>
      {trigger}
      <ContextMenu.Portal>
        <ContextMenu.Content
          className="bg-light rounded-md overflow-hidden shadow-lg py-2 px-1 z-50"
          onClick={(e: MouseEvent) => {
            e.stopPropagation()
          }}
        >
          {children}
        </ContextMenu.Content>
      </ContextMenu.Portal>
    </ContextMenu.Root>
  )
}

function FileExplorerContextMenuTrigger({
  children,
  className,
}: {
  children: React.ReactNode
  className?: string
}): JSX.Element {
  return (
    <ContextMenu.Trigger
      className={clsx('flex w-full overflow-hidden', className)}
      onContextMenu={(e: MouseEvent) => {
        e.stopPropagation()
      }}
    >
      {children}
    </ContextMenu.Trigger>
  )
}

function FileExplorerArtifactRename({
  artifact,
  rename,
  close,
}: {
  artifact: ModelArtifact
  rename: (artifact: ModelArtifact, newName: string) => void
  close: () => void
}): JSX.Element {
  const elInput = React.useRef<HTMLInputElement>(null)
  const [newName, setNewName] = useState<string>('')

  useEffect(() => {
    setNewName(artifact.name)

    setTimeout(() => {
      elInput.current?.focus()
    }, 100)
  }, [artifact])

  return (
    <div className="w-full flex items-center py-[0.125rem] pr-2">
      <input
        ref={elInput}
        type="text"
        className="w-full overflow-hidden overflow-ellipsis bg-primary-900 text-primary-100"
        value={newName}
        onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
          e.stopPropagation()

          setNewName(e.target.value)
        }}
        onBlur={(e: React.ChangeEvent<HTMLInputElement>) => {
          e.stopPropagation()

          close()
        }}
        onKeyDown={(e: React.KeyboardEvent) => {
          if (e.key === 'Enter') {
            e.stopPropagation()

            if (newName.trim() !== '' && newName !== artifact.name) {
              rename(artifact, newName)
            }

            close()
          }

          if (e.key === 'Escape') {
            e.stopPropagation()

            close()
          }
        }}
      />
    </div>
  )
}

function FileExplorerArtifactContainer({
  artifact,
  children,
  isSelected = false,
  className,
  style,
  handleSelect,
  onDoubleClick,
}: {
  artifact: ModelArtifact
  children: React.ReactNode
  isSelected?: boolean
  className?: string
  style?: React.CSSProperties
  handleSelect?: (e: React.MouseEvent | React.KeyboardEvent) => void
  onDoubleClick?: (e: React.MouseEvent) => void
}): JSX.Element {
  const {
    setArtifactRename,
    isBottomGroupInActiveRange,
    isTopGroupInActiveRange,
    isMiddleGroupInActiveRange,
  } = useFileExplorer()

  const activeRange = useStoreProject(s => s.activeRange)
  const inActiveRange = useStoreProject(s => s.inActiveRange)

  return (
    <span
      tabIndex={0}
      className={clsx(
        'w-full flex items-center group/file px-2',
        className,
        isTopGroupInActiveRange(artifact)
          ? 'rounded-t-md'
          : isBottomGroupInActiveRange(artifact)
            ? 'rounded-b-md'
            : isMiddleGroupInActiveRange(artifact)
              ? ''
              : inActiveRange(artifact) && 'rounded-md',
        inActiveRange(artifact) &&
          activeRange.length > 1 &&
          activeRange.indexOf(artifact) < activeRange.length - 1
          ? 'border-b border-primary-400'
          : 'border-b border-transparent',
        inActiveRange(artifact)
          ? 'bg-primary-10 dark:bg-primary-20'
          : isSelected &&
              'rounded-md bg-neutral-10 text-neutral-600 dark:bg-dark-lighter dark:text-primary-500',
      )}
      style={style}
      onClick={handleSelect}
      onDoubleClick={onDoubleClick}
      onKeyDown={(e: React.KeyboardEvent) => {
        if (e.key === 'Enter') {
          e.stopPropagation()

          setArtifactRename(artifact)

          handleSelect?.(e)
        }

        if (e.key === ' ') {
          e.stopPropagation()

          handleSelect?.(e)
        }
      }}
    >
      {children}
    </span>
  )
}

FileExplorer.ContextMenu = FileExplorerContextMenu
FileExplorer.ContextMenuTrigger = FileExplorerContextMenuTrigger
FileExplorer.Rename = FileExplorerArtifactRename
FileExplorer.Container = FileExplorerArtifactContainer

export default FileExplorer
