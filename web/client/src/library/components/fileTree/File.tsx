import { useState, type MouseEvent, useEffect } from 'react'
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
import { type ModelFile } from '~/models'
import { isFalse, isStringEmptyOrNil } from '~/utils'
import { type WithConfirmation } from '../modal/ModalConfirmation'
import { useStoreEditor } from '~/context/editor'
import { useStoreFileTree } from '~/context/fileTree'

interface PropsFile extends WithConfirmation {
  file: ModelFile
}

export default function File({
  file,
  setConfirmation,
}: PropsFile): JSX.Element {
  const tab = useStoreEditor(s => s.tab)
  const tabs = useStoreEditor(s => s.tabs)
  const closeTab = useStoreEditor(s => s.closeTab)

  const files = useStoreFileTree(s => s.files)
  const selectedFile = useStoreFileTree(s => s.selectedFile)
  const selectFile = useStoreFileTree(s => s.selectFile)
  const refreshProject = useStoreFileTree(s => s.refreshProject)

  const [isLoading, setIsLoading] = useState(false)
  const [newName, setNewName] = useState<string>()

  useEffect(() => {
    selectFile(tab?.file)
  }, [tab])

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
      headline: 'Deleting File',
      description: `Are you sure you want to remove the file "${file.name}"?`,
      yesText: 'Yes, Delete',
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
        'whitespace-nowrap group/file pl-3 pr-2 py-[0.125rem] flex rounded-md',
        'hover:bg-neutral-100 dark:hover:bg-dark-lighter',
        file.is_supported &&
          'group hover:bg-neutral-100 dark:hover:bg-dark-lighter',
        isFalse(isStringEmptyOrNil(newName)) && 'bg-primary-800',
        tabs.has(file)
          ? 'text-brand-500'
          : 'text-neutral-500 dark:text-neutral-100',
        file === selectedFile && 'bg-neutral-100 dark:bg-dark-lighter',
      )}
    >
      <span
        className={clsx(
          'flex w-full items-center overflow-hidden overflow-ellipsis',
        )}
      >
        <div className="flex items-center">
          <DocumentIcon
            className={clsx(
              `inline-block w-4 h-4 mr-2`,
              file === selectedFile
                ? 'text-brand-500'
                : 'text-neutral-500 dark:text-neutral-100',
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
  const selectedFile = useStoreFileTree(s => s.selectedFile)
  const selectFile = useStoreFileTree(s => s.selectFile)

  return (
    <span
      title={`${file.name}${file.is_supported ? '' : ' - unsupported format'}`}
      onClick={(e: MouseEvent) => {
        e.stopPropagation()

        file !== selectedFile && selectFile(file)
      }}
      onDoubleClick={(e: MouseEvent) => {
        e.stopPropagation()

        setNewName(file.name)
      }}
      className={clsx(
        'w-full overflow-hidden overflow-ellipsis cursor-default',
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
