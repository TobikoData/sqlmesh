import { type MouseEvent, useEffect, useMemo, useState } from 'react'
import clsx from 'clsx'
import { ModelDirectory } from '../../../models'
import { type Directory as DirectoryApi } from '../../../api/client'
import ModalConfirmation from '../modal/ModalConfirmation'
import type { Confirmation } from '../modal/ModalConfirmation'
import { Button } from '../button/Button'
import { isNotNil } from '~/utils'
import Directory from './Directory'
import { useStoreFileTree } from '~/context/fileTree'

/* TODO:
  - add ability to create file or directory on top level
  - add context menu
  - add drag and drop
  - add copy and paste
  - add move
  - add search
*/

interface PropsFolderTree extends React.HTMLAttributes<HTMLElement> {
  project?: DirectoryApi
}

export default function FolderTree({
  project,
  className,
}: PropsFolderTree): JSX.Element {
  const setFiles = useStoreFileTree(s => s.setFiles)

  const directory = useMemo(() => new ModelDirectory(project), [project])

  const [confirmation, setConfirmation] = useState<Confirmation | undefined>()
  const [showConfirmation, setShowConfirmation] = useState(false)

  useEffect(() => {
    setShowConfirmation(isNotNil(confirmation))
  }, [confirmation])

  useEffect(() => {
    setFiles(directory.allFiles)
  }, [directory])

  return (
    <div
      className={clsx(
        'py-2 px-1 overflow-hidden overflow-y-auto text-sm',
        className,
      )}
    >
      <ModalConfirmation
        show={showConfirmation}
        onClose={() => undefined}
      >
        <ModalConfirmation.Main>
          {confirmation?.headline != null && (
            <ModalConfirmation.Headline>
              {confirmation?.headline}
            </ModalConfirmation.Headline>
          )}
          {confirmation?.description != null && (
            <ModalConfirmation.Description>
              {confirmation?.description}
            </ModalConfirmation.Description>
          )}
        </ModalConfirmation.Main>
        <ModalConfirmation.Actions>
          <Button
            className="font-bold"
            size="md"
            variant="danger"
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              confirmation?.action?.()

              setShowConfirmation(false)
            }}
          >
            {confirmation?.yesText ?? 'Confirm'}
          </Button>
          <Button
            size="md"
            variant="alternative"
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              confirmation?.cancel?.()

              setShowConfirmation(false)
            }}
          >
            {confirmation?.noText ?? 'Cancel'}
          </Button>
        </ModalConfirmation.Actions>
      </ModalConfirmation>
      <Directory
        directory={directory}
        setConfirmation={setConfirmation}
      />
    </div>
  )
}
