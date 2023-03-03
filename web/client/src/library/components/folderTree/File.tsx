import { useState, MouseEvent } from 'react'
import {
  DocumentIcon,
  XCircleIcon,
  CheckCircleIcon,
} from '@heroicons/react/24/solid'
import { DocumentIcon as DocumentIconOutline } from '@heroicons/react/24/outline'
import clsx from 'clsx'
import {
  deleteFileApiFilesPathDelete,
  writeFileApiFilesPathPost,
} from '~/api/client'
import { useStoreFileTree } from '~/context/fileTree'
import { ModelFile } from '~/models'
import { isFalse, isStringEmptyOrNil } from '~/utils'
import { WithConfirmation } from '../modal/ModalConfirmation'

interface PropsFile extends WithConfirmation {
  file: ModelFile
}

const CSS_ICON_SIZE = 'w-4 h-4'

export default function File({
  file,
  setConfirmation,
}: PropsFile): JSX.Element {
  const activeFile = useStoreFileTree(s => s.activeFile)
  const openedFiles = useStoreFileTree(s => s.openedFiles)
  const setOpenedFiles = useStoreFileTree(s => s.setOpenedFiles)
  const selectFile = useStoreFileTree(s => s.selectFile)

  const [isLoading, setIsLoading] = useState(false)
  const [newName, setNewName] = useState<string>()

  function remove(): void {
    if (isLoading) return

    setIsLoading(true)

    deleteFileApiFilesPathDelete(file.path)
      .then(response => {
        if ((response as unknown as { ok: boolean }).ok) {
          openedFiles.delete(file)

          file.parent?.removeFile(file)

          setOpenedFiles(openedFiles)
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
        setOpenedFiles(openedFiles)
      })
  }

  return (
    <span
      className={clsx(
        'text-base whitespace-nowrap group/file pl-3 pr-2 py-[0.125rem] flex justify-between rounded-md',
        file === activeFile ? 'text-secondary-500' : 'text-gray-800',
        file.is_supported && 'group hover:bg-secondary-100',
        isFalse(isStringEmptyOrNil(newName)) && 'bg-warning-100',
      )}
    >
      <span
        className={clsx(
          'flex w-full items-center overflow-hidden overflow-ellipsis',
        )}
      >
        <div className="flex items-center">
          {openedFiles.has(file) ? (
            <DocumentIcon
              className={`inline-block ${CSS_ICON_SIZE} mr-2 text-secondary-500`}
            />
          ) : (
            <DocumentIconOutline
              className={`inline-block ${CSS_ICON_SIZE} mr-2 text-secondary-500`}
            />
          )}
        </div>
        {isStringEmptyOrNil(newName) ? (
          <>
            <span
              onClick={(e: MouseEvent) => {
                e.stopPropagation()

                file.is_supported && file !== activeFile && selectFile(file)
              }}
              onDoubleClick={(e: MouseEvent) => {
                e.stopPropagation()

                setNewName(file.name)
              }}
              className={clsx(
                'w-full text-sm overflow-hidden overflow-ellipsis cursor-default',
                !file.is_supported &&
                  'opacity-50 cursor-not-allowed text-gray-800',
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
              className="w-full text-sm overflow-hidden overflow-ellipsis group-hover:text-secondary-500"
              value={newName === '' ? file.name : newName}
              onInput={(e: any) => {
                e.stopPropagation()

                setNewName(e.target.value)
              }}
            />
            <div className="flex">
              <CheckCircleIcon
                className={`inline-block ${CSS_ICON_SIZE} ml-2 text-gray-500 cursor-pointer`}
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
