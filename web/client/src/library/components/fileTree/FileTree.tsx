import { type MouseEvent, useEffect, useState } from 'react'
import clsx from 'clsx'
import ModalConfirmation from '../modal/ModalConfirmation'
import type { Confirmation } from '../modal/ModalConfirmation'
import { Button } from '../button/Button'
import { isNotNil } from '~/utils'
import Directory from './Directory'
import { useStoreFileTree } from '@context/fileTree'

/* TODO:
  - add ability to create file or directory on top level
  - add context menu
  - add drag and drop
  - add copy and paste
  - add move
  - add search
*/

export default function FileTree({
  className,
}: {
  className?: string
}): JSX.Element {
  const project = useStoreFileTree(s => s.project)

  const [confirmation, setConfirmation] = useState<Confirmation | undefined>()
  const [showConfirmation, setShowConfirmation] = useState(false)

  useEffect(() => {
    setShowConfirmation(isNotNil(confirmation))
  }, [confirmation])

  return (
    <div
      className={clsx(
        'py-2 px-1 overflow-hidden overflow-y-auto text-sm scrollbar scrollbar--vertical',
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
      {project != null && (
        <Directory
          directory={project}
          setConfirmation={setConfirmation}
        />
      )}
    </div>
  )
}
