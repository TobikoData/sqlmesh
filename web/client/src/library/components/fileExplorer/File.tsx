import { useState, type MouseEvent } from 'react'
import {
  DocumentIcon,
  XCircleIcon,
  CheckCircleIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { ModelFile } from '~/models'
import { isFalse, isStringEmptyOrNil } from '~/utils'
import { useStoreEditor } from '~/context/editor'
import { useStoreFileExplorer } from '~/context/fileTree'
import * as ContextMenu from '@radix-ui/react-context-menu'
import { type PropsArtifact } from './FileExplorer'

interface PropsFile extends PropsArtifact {
  file: ModelFile
}

export default function File({
  file,
  removeArtifactWithConfirmation,
  renameAtrifact,
  className,
  style,
}: PropsFile): JSX.Element {
  const tab = useStoreEditor(s => s.tab)

  const activeRange = useStoreFileExplorer(s => s.activeRange)
  const tabs = useStoreEditor(s => s.tabs)
  const replaceTab = useStoreEditor(s => s.replaceTab)

  const selected = useStoreFileExplorer(s => s.selected)
  const setSelected = useStoreFileExplorer(s => s.setSelected)
  const setActiveRange = useStoreFileExplorer(s => s.setActiveRange)
  const selectArtifactsInRange = useStoreFileExplorer(
    s => s.selectArtifactsInRange,
  )

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
    } else if (file !== selected) {
      setSelected(file)

      const shouldReplaceTab =
        selected instanceof ModelFile &&
        isFalse(selected.isChanged) &&
        selected.isRemote &&
        isFalse(tabs.has(file))

      if (shouldReplaceTab) {
        replaceTab(selected, file)
      }
    }
  }

  const [newName, setNewName] = useState<string>()
  const [isOpenContextMenu, setIsOpenContextMenu] = useState(false)

  return (
    <span
      className={clsx(
        'whitespace-nowrap group/file flex rounded-md',
        isFalse(isStringEmptyOrNil(newName)) && 'bg-primary-800',
        isOpenContextMenu && 'bg-primary-10',
        activeRange.has(file)
          ? 'text-brand-100 bg-brand-500 dark:bg-brand-700 dark:text-brand-100'
          : tab?.file === file &&
              'bg-neutral-200 text-neutral-600 dark:bg-dark-lighter dark:text-primary-500',
        className,
      )}
      style={style}
      onContextMenu={(e: MouseEvent) => {
        e.stopPropagation()
      }}
      onClick={handleSelect}
    >
      <span className="flex w-full items-center overflow-hidden overflow-ellipsis">
        <div className="flex items-center">
          <DocumentIcon className="inline-block w-3 ml-1 mr-2" />
        </div>
        {isStringEmptyOrNil(newName) ? (
          <ContextMenuFile
            onOpenChange={setIsOpenContextMenu}
            file={file}
            setNewName={setNewName}
            removeWithConfirmation={() => removeArtifactWithConfirmation(file)}
          />
        ) : (
          <FileRename
            file={file}
            newName={newName}
            setNewName={setNewName}
            rename={() => renameAtrifact(file, newName)}
          />
        )}
      </span>
    </span>
  )
}

function FileName({ file }: { file: ModelFile }): JSX.Element {
  return (
    <span
      title={`${file.name}${file.is_supported ? '' : ' - unsupported format'}`}
      className={clsx(
        'w-full overflow-hidden overflow-ellipsis cursor-default py-[0.125rem] pr-2',
        !file.is_supported && 'opacity-50',
      )}
    >
      {file.name}
    </span>
  )
}

function FileRename({
  file,
  newName,
  setNewName,
  rename,
}: {
  file: ModelFile
  newName?: string
  setNewName: (name?: string) => void
  rename: () => void
}): JSX.Element {
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
        {file.name === newName?.trim() || newName === '' ? (
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

              rename()
              setNewName(undefined)
            }}
          />
        )}
      </div>
    </div>
  )
}

function ContextMenuFile({
  file,
  setNewName,
  removeWithConfirmation,
  onOpenChange,
}: {
  file: ModelFile
  removeWithConfirmation: () => void
  setNewName: (newName: string) => void
  onOpenChange: (isOpen: boolean) => void
  newName?: string
}): JSX.Element {
  const activeRange = useStoreFileExplorer(s => s.activeRange)

  const disabled = activeRange.size > 1 && activeRange.has(file)
  return (
    <ContextMenu.Root onOpenChange={onOpenChange}>
      <ContextMenu.Trigger className="w-full overflow-hidden flex items-center justify-between">
        <FileName file={file} />
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

              removeWithConfirmation()
            }}
          >
            Remove {activeRange.has(file) ? activeRange.size : ''}
            <div className="ml-auto pl-5"></div>
          </ContextMenu.Item>
        </ContextMenu.Content>
      </ContextMenu.Portal>
    </ContextMenu.Root>
  )
}
