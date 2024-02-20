import React, { useState, type MouseEvent, useEffect } from 'react'
import { DocumentIcon } from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { useStoreProject } from '@context/project'
import * as ContextMenu from '@radix-ui/react-context-menu'
import FileExplorer from './FileExplorer'
import { useFileExplorer } from './context'
import { type ModelFile } from '@models/file'
import { useDrag } from 'react-dnd'
import { getEmptyImage } from 'react-dnd-html5-backend'
import { useLongPress } from '@uidotdev/usehooks'
import { type ModelArtifact } from '@models/artifact'
import { useStoreEditor } from '@context/editor'
import { truncate } from '@utils/index'

function File({
  file,
  className,
  style,
}: {
  file: ModelFile
  className?: string
  style?: React.CSSProperties
}): JSX.Element {
  const selectedFile = useStoreProject(s => s.selectedFile)
  const setSelectedFile = useStoreProject(s => s.setSelectedFile)
  const activeRange = useStoreProject(s => s.activeRange)
  const setActiveRange = useStoreProject(s => s.setActiveRange)
  const inActiveRange = useStoreProject(s => s.inActiveRange)

  const addTab = useStoreEditor(s => s.addTab)
  const selectTab = useStoreEditor(s => s.selectTab)
  const createTab = useStoreEditor(s => s.createTab)

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
    setArtifactRename,
    renameArtifact,
    artifactRename,
    selectArtifactsInRange,
    removeArtifactWithConfirmation,
  } = useFileExplorer()

  const [isOpenContextMenu, setIsOpenContextMenu] = useState(false)

  const [{ isDragging }, drag, preview] = useDrag(
    () => ({
      type: 'artifact',
      item: file,
      end() {
        setIsDraggable(false)
      },
      canDrag() {
        return artifactRename !== file && isDraggable
      },
      collect(monitor) {
        return {
          isDragging: monitor.isDragging(),
        }
      },
    }),
    [file, artifactRename, isDraggable],
  )

  useEffect(() => {
    preview(getEmptyImage(), { captureDraggingState: true })
  }, [preview])

  function handleSelect(e: React.MouseEvent | React.KeyboardEvent): void {
    e.stopPropagation()

    if (e.shiftKey) {
      e.preventDefault()
    }

    let ar: ModelArtifact[] = activeRange

    if (e.metaKey) {
      if (inActiveRange(file)) {
        ar = ar.filter(a => a !== file)
      } else {
        ar.push(file)
      }
      setActiveRange(ar)
    } else if (e.shiftKey && ar.length > 0) {
      selectArtifactsInRange(file)
    } else {
      if (ar.length > 0) {
        ar = []
      }

      ar.push(file)

      setActiveRange(ar)
      setSelectedFile(file)
    }
  }

  function openFileInNewTab(): void {
    const tab = createTab(file)

    addTab(tab)
    selectTab(tab)
  }

  const disabled = activeRange.length > 1 && inActiveRange(file)

  return (
    <div
      {...attrs}
      ref={drag}
    >
      <FileExplorer.Container
        artifact={file}
        isSelected={selectedFile === file}
        className={clsx(
          artifactRename === file && 'bg-primary-10',
          isOpenContextMenu && 'bg-primary-10',
          isDraggable &&
            'bg-primary-10 !cursor-grabbing outline-2 !outline-primary-500',
          isDragging && 'opacity-50',
          className,
        )}
        style={style}
        onDoubleClick={e => {
          e.stopPropagation()

          openFileInNewTab()
        }}
        handleSelect={handleSelect}
      >
        <File.Icons />
        {artifactRename === file ? (
          <FileExplorer.Rename
            artifact={artifactRename}
            rename={renameArtifact}
            close={() => setArtifactRename(undefined)}
          />
        ) : (
          <FileExplorer.ContextMenu
            trigger={
              <FileExplorer.ContextMenuTrigger>
                <File.Display file={file} />
              </FileExplorer.ContextMenuTrigger>
            }
            onOpenChange={setIsOpenContextMenu}
          >
            {selectedFile !== file && (
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

                  openFileInNewTab()
                }}
              >
                Open in New Tab
                <div className="ml-auto pl-5"></div>
              </ContextMenu.Item>
            )}
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

                setArtifactRename(file)
              }}
            >
              Rename
              <div className="ml-auto pl-5"></div>
            </ContextMenu.Item>
            <ContextMenu.Item
              className="py-1.5 group leading-none rounded-md flex items-center relative pl-6 pr-2 select-none outline-none font-medium text-xs text-danger-500 hover:bg-danger-500 hover:text-light"
              onSelect={(e: Event) => {
                e.stopPropagation()

                removeArtifactWithConfirmation(file)
              }}
            >
              Remove {inActiveRange(file) ? activeRange.length : ''}
              <div className="ml-auto pl-5"></div>
            </ContextMenu.Item>
          </FileExplorer.ContextMenu>
        )}
      </FileExplorer.Container>
    </div>
  )
}

function FileDisplay({ file }: { file: ModelFile }): JSX.Element {
  return (
    <span
      title={file.name}
      className={clsx(
        'inline-block overflow-hidden overflow-ellipsis py-[0.125rem]',
      )}
    >
      {truncate(file.name, 50, 20)}
    </span>
  )
}

function FileIcons(): JSX.Element {
  return (
    <div className="flex items-center">
      <DocumentIcon className="inline-block w-3 ml-1 mr-2" />
    </div>
  )
}

File.Display = FileDisplay
File.Icons = FileIcons

export default File
