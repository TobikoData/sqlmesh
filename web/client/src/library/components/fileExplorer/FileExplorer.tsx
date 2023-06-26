import { type MouseEvent, useEffect, useState } from 'react'
import clsx from 'clsx'
import ModalConfirmation from '../modal/ModalConfirmation'
import type { Confirmation } from '../modal/ModalConfirmation'
import { Button } from '../button/Button'
import { isNotNil } from '~/utils'
import Directory from './Directory'
import { useStoreFileExplorer } from '@context/fileTree'
import { ModelFile } from '@models/file'
import { deleteFileApiFilesPathDelete } from '@api/client'
import { useStoreEditor } from '@context/editor'
import { type ModelArtifact } from '@models/artifact'
import { ModelDirectory } from '@models/directory'

/* TODO:
  - add ability to create file or directory on top level
  - add context menu
  - add drag and drop
  - add copy and paste
  - add move
  - add search
*/

export default function FileExplorer({
  className,
}: {
  className?: string
}): JSX.Element {
  const selected = useStoreFileExplorer(s => s.selected)
  const activeRange = useStoreFileExplorer(s => s.activeRange)
  const setActiveRange = useStoreFileExplorer(s => s.setActiveRange)
  const project = useStoreFileExplorer(s => s.project)
  const refreshProject = useStoreFileExplorer(s => s.refreshProject)
  const selectFile = useStoreFileExplorer(s => s.selectFile)

  const tab = useStoreEditor(s => s.tab)
  const closeTab = useStoreEditor(s => s.closeTab)

  const [confirmation, setConfirmation] = useState<Confirmation | undefined>()
  const [showConfirmation, setShowConfirmation] = useState(false)

  useEffect(() => {
    setShowConfirmation(isNotNil(confirmation))
  }, [confirmation])

  useEffect(() => {
    setActiveRange(new Set())
    selectFile(tab?.file)
  }, [tab])

  async function removeFile(file: ModelFile): Promise<void> {
    return deleteFileApiFilesPathDelete(file.path).then(response => {
      if ((response as unknown as { ok: boolean }).ok) {
        closeTab(file)

        file.parent?.removeFile(file)

        refreshProject()
      } else {
        // TODO: Show error notification
        throw new Error('Unable to delete file')
      }
    })
  }

  async function removeArtifacts(artifacts: Set<ModelArtifact>): Promise<void> {
    const promises = Array.from(artifacts).map(artifact => {
      if (artifact instanceof ModelFile) {
        return removeFile(artifact)
      }

      if (artifact instanceof ModelDirectory) {
        return removeFile(artifact)
      }

      return Promise.resolve()
    })

    return Promise.all(promises).then(() => {
      setActiveRange(new Set())
    })
  }

  console.log(project)

  return (
    <div
      className={clsx(
        'h-full py-2 px-2 overflow-hidden overflow-y-auto text-sm text-neutral-500 dark:text-neutral-400  font-regular hover:scrollbar scrollbar--vertical select-none',
        className,
      )}
      tabIndex={1}
      onKeyDown={(e: KeyboardEvent) => {
        if (e.key === 'Escape' && selected != null && activeRange.size > 1) {
          setActiveRange(new Set([selected]))
        }

        if (e.key === 'Backspace' && activeRange.size > 0) {
          void removeArtifacts(activeRange)
        }
      }}
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
          removeFile={removeFile}
        />
      )}
    </div>
  )
}
