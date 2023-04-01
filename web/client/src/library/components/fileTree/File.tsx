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
import { type ModelFile } from '~/models'
import { isFalse, isStringEmptyOrNil } from '~/utils'
import { type WithConfirmation } from '../modal/ModalConfirmation'
import { useStoreEditor } from '~/context/editor'
import { useStoreFileTree } from '~/context/fileTree'

interface PropsFile extends WithConfirmation {
  file: ModelFile
}

const CSS_ICON_SIZE = 'w-4 h-4'

export default function File({
  file,
  setConfirmation,
}: PropsFile): JSX.Element {
  const tab = useStoreEditor(s => s.tab)
  const tabs = useStoreEditor(s => s.tabs)
  const closeTab = useStoreEditor(s => s.closeTab)

  const selectFile = useStoreFileTree(s => s.selectFile)
  const refreshProject = useStoreFileTree(s => s.refreshProject)

  const [isLoading, setIsLoading] = useState(false)
  const [newName, setNewName] = useState<string>()

  function remove(): void {
    if (isLoading) return

    setIsLoading(true)

    deleteFileApiFilesPathDelete(file.path)
      .then(response => {
        if ((response as unknown as { ok: boolean }).ok) {
          closeTab(file.id)

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
    )
      return

    setIsLoading(true)

    const currentName = file.name
    const currentPath = file.path

    file.rename(newName.trim().replace(`.${String(file.extension)}`, ''))

    void writeFileApiFilesPathPost(currentPath, {
      new_path: file.path,
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
        'hover:bg-neutral-100 dark:hover:bg-dark-lighter  ',
        file.is_supported &&
          'group hover:bg-neutral-100 dark:hover:bg-dark-lighter',
        isFalse(isStringEmptyOrNil(newName)) && 'bg-primary-800',
        tabs.has(file.id)
          ? 'text-brand-500'
          : 'text-neutral-500 dark:text-neutral-100',
        file === tab.file && 'bg-neutral-100 dark:bg-dark-lighter',
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
              `inline-block ${CSS_ICON_SIZE} mr-2`,
              file === tab.file
                ? 'text-brand-500'
                : 'text-neutral-500 dark:text-neutral-100',
            )}
          />
        </div>
        {isStringEmptyOrNil(newName) ? (
          <>
            <span
              onClick={(e: MouseEvent) => {
                e.stopPropagation()

                file.is_supported && file !== tab.file && selectFile(file)
              }}
              onDoubleClick={(e: MouseEvent) => {
                e.stopPropagation()

                setNewName(file.name)
              }}
              className={clsx(
                'w-full overflow-hidden overflow-ellipsis cursor-default',
                !file.is_supported && 'opacity-50 cursor-not-allowed',
              )}
            >
              {file.name}
            </span>
            <span
              className="flex items-center invisible group-hover/file:visible min-w-8"
              onClick={(e: MouseEvent) => {
                e.stopPropagation()

                removeWithConfirmation()
              }}
            >
              <XCircleIcon
                className={`inline-block ${CSS_ICON_SIZE} ml-2 text-danger-500 cursor-pointer`}
              />
            </span>
          </>
        ) : (
          <div className="w-full flex items-center">
            <input
              type="text"
              className="w-full overflow-hidden overflow-ellipsis bg-primary-900 text-primary-100"
              value={newName === '' ? file.name : newName}
              onInput={(e: any) => {
                e.stopPropagation()

                setNewName(e.target.value)
              }}
            />
            <div className="flex">
              <CheckCircleIcon
                className={`inline-block ${CSS_ICON_SIZE} ml-2 text-success-500 cursor-pointer`}
                onClick={(e: MouseEvent) => {
                  e.stopPropagation()

                  rename()
                }}
              />
            </div>
          </div>
        )}
      </span>
    </span>
  )
}
