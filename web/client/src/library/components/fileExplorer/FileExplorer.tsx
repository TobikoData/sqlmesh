import React, { type MouseEvent } from 'react'
import clsx from 'clsx'
import Directory from './Directory'
import { useStoreProject } from '@context/project'
import * as ContextMenu from '@radix-ui/react-context-menu'
import { DndProvider } from 'react-dnd'
import { HTML5Backend } from 'react-dnd-html5-backend'
import DragLayer from './DragLayer'
import { useFileExplorer } from './context'
import { type ModelArtifact } from '@models/artifact'
import { CheckCircleIcon, XCircleIcon } from '@heroicons/react/24/solid'
import SearchList from '@components/search/SearchList'
import { type ModelFile } from '@models/file'
import { EnumSize } from '~/types/enum'
import { useStoreContext } from '@context/context'

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

  const addConfirmation = useStoreContext(s => s.addConfirmation)

  const {
    activeRange,
    removeArtifacts,
    setActiveRange,
    createDirectory,
    createFile,
  } = useFileExplorer()

  function handleKeyDown(e: React.KeyboardEvent): void {
    if (e.key === 'Escape') {
      setActiveRange(new Set([]))
    }

    if (e.metaKey && e.key === 'Backspace' && activeRange.size > 0) {
      addConfirmation({
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
        'flex flex-col h-full overflow-hidden text-sm text-neutral-500 dark:text-neutral-400 font-regular select-none',
        className,
      )}
    >
      <SearchList<ModelFile>
        list={project.allFiles}
        searchBy="path"
        displayBy="name"
        size={EnumSize.sm}
        onSelect={setSelectedFile}
      />
      <FileExplorer.ContextMenu
        key={project.id}
        trigger={
          <FileExplorer.ContextMenuTrigger className="h-full pb-2">
            <DndProvider backend={HTML5Backend}>
              <div
                className="w-full relative h-full p-2 overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical"
                tabIndex={1}
                onKeyDown={handleKeyDown}
              >
                <DragLayer />
                <Directory
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
          className="bg-light rounded-md overflow-hidden shadow-lg py-2 px-1"
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
  newName,
  setNewName,
}: {
  artifact: ModelArtifact
  newName?: string
  setNewName: (name?: string) => void
}): JSX.Element {
  const { renameArtifact } = useFileExplorer()

  return (
    <div className="w-full flex items-center py-[0.125rem] pr-2">
      <input
        type="text"
        className="w-full overflow-hidden overflow-ellipsis bg-primary-900 text-primary-100"
        value={newName}
        onInput={(e: any) => {
          e.stopPropagation()

          setNewName(e.target.value)
        }}
      />
      <div className="flex">
        {artifact.name === newName?.trim() || newName === '' ? (
          <XCircleIcon
            className="inline-block w-4 h-4 ml-2 text-neutral-100 cursor-pointer"
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              setNewName(undefined)
            }}
          />
        ) : (
          <CheckCircleIcon
            className={`inline-block w-4 h-4 ml-2 text-success-500 cursor-pointer`}
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              renameArtifact(artifact, newName)
              setNewName(undefined)
            }}
          />
        )}
      </div>
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
}: {
  artifact: ModelArtifact
  children: React.ReactNode
  isSelected?: boolean
  className?: string
  style?: React.CSSProperties
  handleSelect?: (e: MouseEvent) => void
}): JSX.Element {
  const { activeRange } = useFileExplorer()

  return (
    <span
      className={clsx(
        'w-full flex items-center group/file rounded-md px-2',
        className,
        activeRange.has(artifact)
          ? 'text-brand-100 !bg-brand-500 dark:bg-brand-700 dark:text-brand-100'
          : isSelected &&
              'bg-neutral-200 text-neutral-600 dark:bg-dark-lighter dark:text-primary-500',
      )}
      style={style}
      onClick={handleSelect}
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
