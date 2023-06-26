import { useState, type MouseEvent } from 'react'
import {
  DocumentIcon,
  XCircleIcon,
  CheckCircleIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import {
  deleteFileApiFilesPathDelete,
  writeFileApiFilesPathPost,
} from '~/api/client'
import { ModelFile } from '~/models'
import { isFalse, isStringEmptyOrNil } from '~/utils'
import { type WithConfirmation } from '../modal/ModalConfirmation'
import { useStoreEditor } from '~/context/editor'
import { useStoreFileExplorer } from '~/context/fileTree'

interface PropsFile extends WithConfirmation {
  file: ModelFile
  className?: string
  style?: React.CSSProperties
}

export default function File({
  file,
  setConfirmation,
  className,
  style,
}: PropsFile): JSX.Element {
  const tab = useStoreEditor(s => s.tab)
  const closeTab = useStoreEditor(s => s.closeTab)

  const files = useStoreFileExplorer(s => s.files)
  const activeRange = useStoreFileExplorer(s => s.activeRange)

  const refreshProject = useStoreFileExplorer(s => s.refreshProject)

  const [isLoading, setIsLoading] = useState(false)
  const [newName, setNewName] = useState<string>()

  function remove(): void {
    if (isLoading) return

    setIsLoading(true)

    deleteFileApiFilesPathDelete(file.path)
      .then(response => {
        if ((response as unknown as { ok: boolean }).ok) {
          closeTab(file)

          file.parent?.removeFile(file)

          refreshProject()
        } else {
          // TODO: Show error notification
        }
      })
      .catch(error => {
        // TODO: Show error notification
        console.log(error)
      })
      .finally(() => {
        setIsLoading(false)
      })
  }

  function removeWithConfirmation(): void {
    setConfirmation({
      headline: 'Remove File',
      description: `Are you sure you want to remove the file "${file.name}"?`,
      yesText: 'Yes, Remove',
      noText: 'No, Cancel',
      action: remove,
    })
  }

  function rename(): void {
    if (
      isLoading ||
      file == null ||
      isStringEmptyOrNil(newName) ||
      newName == null
    ) {
      setNewName(undefined)

      return
    }

    setIsLoading(true)

    const currentName = file.name
    const currentPath = file.path

    file.rename(newName.trim())

    void writeFileApiFilesPathPost(currentPath, {
      new_path: file.path,
    })
      .then(response => {
        file.update(response)

        files.set(file.path, file)
        files.delete(currentPath)
      })
      .catch(error => {
        console.log(error)

        file.rename(currentName)
      })
      .finally(() => {
        setNewName(undefined)
        setIsLoading(false)

        refreshProject()
      })
  }

  return (
    <span
      className={clsx(
        'whitespace-nowrap group/file py-[0.125rem] flex rounded-md pr-2',
        // 'hover:bg-neutral-100 dark:hover:bg-dark-lighter',
        // file.is_supported &&
        //   'group hover:bg-neutral-100 dark:hover:bg-dark-lighter',
        isFalse(isStringEmptyOrNil(newName)) && 'bg-primary-800',
        // tabs.has(file)
        //   ? 'text-brand-500'
        //   : 'text-neutral-500 dark:text-neutral-100',
        // file === selected && 'bg-brand-500 text-brand-100',
        activeRange.has(file) &&
          'text-brand-100 bg-brand-500 dark:bg-brand-700 dark:text-brand-100',
        tab?.file === file &&
          'bg-neutral-200 text-neutral-900 dark:bg-dark-lighter dark:text-primary-500',
        className,
      )}
      style={style}
    >
      <span
        className={clsx(
          'flex w-full items-center overflow-hidden overflow-ellipsis',
        )}
      >
        <div className="flex items-center">
          <DocumentIcon
            className={clsx(
              `inline-block w-3 ml-1`,
              // file === selected
              //   ? 'text-brand-500'
              //   : 'text-neutral-500 dark:text-neutral-100',
            )}
          />
        </div>
        {newName == null ? (
          <>
            <FileName
              file={file}
              setNewName={setNewName}
            />
            <FileActions removeWithConfirmation={removeWithConfirmation} />
          </>
        ) : (
          <FileRename
            file={file}
            newName={newName}
            setNewName={setNewName}
            rename={rename}
          />
        )}
      </span>
    </span>
  )
}

function FileName({
  file,
  setNewName,
}: {
  file: ModelFile
  setNewName: (name: string) => void
}): JSX.Element {
  const tabs = useStoreEditor(s => s.tabs)
  const replaceTab = useStoreEditor(s => s.replaceTab)

  const selected = useStoreFileExplorer(s => s.selected)
  const selectFile = useStoreFileExplorer(s => s.selectFile)
  const activeRange = useStoreFileExplorer(s => s.activeRange)
  const setActiveRange = useStoreFileExplorer(s => s.setActiveRange)
  return (
    <span
      title={`${file.name}${file.is_supported ? '' : ' - unsupported format'}`}
      onClick={(e: MouseEvent) => {
        e.stopPropagation()

        if (e.shiftKey) {
          e.preventDefault()
        }

        if (e.shiftKey && activeRange.size > 0) {
          activeRange.add(file)
          setActiveRange(activeRange)
        } else if (file !== selected) {
          selectFile(file)

          const shouldReplaceTab =
            selected instanceof ModelFile &&
            isFalse(selected.isChanged) &&
            selected.isRemote &&
            isFalse(tabs.has(file))

          if (shouldReplaceTab) {
            replaceTab(selected, file)
          }
        }
      }}
      onDoubleClick={(e: MouseEvent) => {
        e.stopPropagation()

        setNewName(file.name)
      }}
      className={clsx(
        'w-full overflow-hidden overflow-ellipsis cursor-default ml-1',
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
  setNewName: (name: string | undefined) => void
  rename: () => void
}): JSX.Element {
  return (
    <div className="w-full flex items-center">
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
            className={`inline-block w-4 h-4 ml-2 text-neutral-500 cursor-pointer`}
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
            }}
          />
        )}
      </div>
    </div>
  )
}

function FileActions({
  removeWithConfirmation,
}: {
  removeWithConfirmation: () => void
}): JSX.Element {
  return (
    <span
      className="flex items-center invisible group-hover/file:visible min-w-8"
      onClick={(e: MouseEvent) => {
        e.stopPropagation()

        removeWithConfirmation()
      }}
    >
      <XCircleIcon
        className={`inline-block w-4 h-4 ml-2 text-danger-500 cursor-pointer`}
      />
    </span>
  )
}
