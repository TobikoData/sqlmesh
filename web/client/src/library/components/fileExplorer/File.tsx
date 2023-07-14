import { useState, type MouseEvent, useEffect } from 'react'
import { DocumentIcon } from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { isFalse, isStringEmptyOrNil } from '~/utils'
import { useStoreProject } from '@context/project'
import * as ContextMenu from '@radix-ui/react-context-menu'
import FileExplorer from './FileExplorer'
import { useFileExplorer } from './context'
import { type ModelFile } from '@models/file'
import { useDrag } from 'react-dnd'
import { getEmptyImage } from 'react-dnd-html5-backend'
import { useLongPress } from '@uidotdev/usehooks'

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
    selectArtifactsInRange,
    removeArtifactWithConfirmation,
  } = useFileExplorer()

  const [newName, setNewName] = useState<string>()
  const [isOpenContextMenu, setIsOpenContextMenu] = useState(false)

  const [{ isDragging }, drag, preview] = useDrag(
    () => ({
      type: 'artifact',
      item: file,
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
    [file, newName, isDraggable],
  )

  useEffect(() => {
    preview(getEmptyImage(), { captureDraggingState: true })
  }, [preview])

  function handleSelect(e: MouseEvent): void {
    e.stopPropagation()

    if (e.shiftKey) {
      e.preventDefault()
    }

    if (e.metaKey) {
      if (activeRange.has(file)) {
        activeRange.delete(file)
      } else {
        activeRange.add(file)
      }

      setActiveRange(activeRange)
    } else if (e.shiftKey && activeRange.size > 0) {
      selectArtifactsInRange(file)
    } else {
      if (activeRange.size > 0) {
        activeRange.clear()
      }

      activeRange.add(file)

      setActiveRange(activeRange)
      setSelectedFile(file)
    }
  }

  const disabled = activeRange.size > 1 && activeRange.has(file)

  return (
    <div
      {...attrs}
      ref={drag}
    >
      <FileExplorer.Container
        artifact={file}
        isSelected={selectedFile === file}
        className={clsx(
          isFalse(isStringEmptyOrNil(newName)) && 'bg-primary-800',
          isOpenContextMenu && 'bg-primary-10',
          isDraggable &&
            'bg-primary-10 !cursor-grabbing outline-2 !outline-primary-500',
          isDragging && 'opacity-50',
          className,
        )}
        style={style}
        handleSelect={handleSelect}
      >
        <File.Icons />
        {isStringEmptyOrNil(newName) ? (
          <FileExplorer.ContextMenu
            trigger={
              <FileExplorer.ContextMenuTrigger>
                <File.Display file={file} />
              </FileExplorer.ContextMenuTrigger>
            }
            onOpenChange={setIsOpenContextMenu}
          >
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

                setNewName(file.name)
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
              Remove {activeRange.has(file) ? activeRange.size : ''}
              <div className="ml-auto pl-5"></div>
            </ContextMenu.Item>
          </FileExplorer.ContextMenu>
        ) : (
          <FileExplorer.Rename
            artifact={file}
            newName={newName}
            setNewName={setNewName}
          />
        )}
      </FileExplorer.Container>
    </div>
  )
}

function FileDisplay({ file }: { file: ModelFile }): JSX.Element {
  return (
    <span
      className={clsx(
        'inline-block overflow-hidden overflow-ellipsis py-[0.125rem]',
        !file.is_supported && 'opacity-50',
      )}
    >
      {file.name}
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
