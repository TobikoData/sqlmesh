import { Button } from '../button/Button'
import { Divider } from '../divider/Divider'
import { Editor } from '../editor/Editor'
import FolderTree from '../folderTree/FolderTree'
import { useEffect, type MouseEvent, useState, lazy, useCallback } from 'react'
import { EnumSize, EnumVariant } from '../../../types/enum'
import {
  useApiFiles,
  useApiEnvironments,
  apiCancelGetEnvironments,
  apiCancelFiles,
} from '../../../api'
import { EnumPlanAction, useStorePlan } from '../../../context/plan'
import { useChannelEvents } from '../../../api/channels'
import SplitPane from '../splitPane/SplitPane'
import { isArrayEmpty, isFalse, isTrue, debounceAsync } from '~/utils'
import { useStoreContext } from '~/context/context'
import Modal from '../modal/Modal'
import PlanProvider from '../plan/context'
import RunPlan from './RunPlan'
import ActivePlan from './ActivePlan'
import { Dialog } from '@headlessui/react'
import { useQueryClient } from '@tanstack/react-query'

const Plan = lazy(async () => await import('../plan/Plan'))
const Graph = lazy(async () => await import('../graph/Graph'))

export function IDE(): JSX.Element {
  const client = useQueryClient()

  const environment = useStoreContext(s => s.environment)
  const initialStartDate = useStoreContext(s => s.initialStartDate)
  const initialEndDate = useStoreContext(s => s.initialEndDate)
  const addSyncronizedEnvironments = useStoreContext(
    s => s.addSyncronizedEnvironments,
  )

  const activePlan = useStorePlan(s => s.activePlan)
  const setPlanAction = useStorePlan(s => s.setAction)
  const updateTasks = useStorePlan(s => s.updateTasks)

  const [isGraphOpen, setIsGraphOpen] = useState(false)
  const [isPlanOpen, setIsPlanOpen] = useState(false)
  const [isClosingModal, setIsClosingModal] = useState(false)

  const [subscribe] = useChannelEvents()

  const { data: project, refetch: getFiles } = useApiFiles()
  const { data: contextEnvironemnts, refetch: getEnvironments } =
    useApiEnvironments()

  const debouncedGetEnvironemnts = useCallback(
    debounceAsync(getEnvironments, 1000, true),
    [getEnvironments],
  )
  const debouncedGetFiles = useCallback(debounceAsync(getFiles, 1000, true), [
    getFiles,
  ])

  useEffect(() => {
    const unsubscribeTasks = subscribe('tasks', updateTasks)

    void debouncedGetEnvironemnts()
    void debouncedGetFiles()

    return () => {
      debouncedGetEnvironemnts.cancel()
      debouncedGetFiles.cancel()

      apiCancelFiles(client)
      apiCancelGetEnvironments(client)

      unsubscribeTasks?.()
    }
  }, [])

  useEffect(() => {
    if (
      contextEnvironemnts == null ||
      isArrayEmpty(Object.keys(contextEnvironemnts))
    )
      return

    addSyncronizedEnvironments(Object.values(contextEnvironemnts))
  }, [contextEnvironemnts])

  function showGraph(): void {
    setIsGraphOpen(true)
  }

  function showRunPlan(): void {
    setIsPlanOpen(true)
  }

  function closeModal(): void {
    setIsClosingModal(true)
  }

  return (
    <>
      <div className="w-full flex justify-between items-center min-h-[2rem] z-50">
        <div className="px-3 flex items-center whitespace-nowrap">
          <h3 className="font-bold text-primary-500">
            <span className="inline-block">/</span>
            {project?.name}
          </h3>
        </div>
        <div className="px-3 flex items-center min-w-[10rem] justify-end">
          <Button
            className="mr-4"
            variant={EnumVariant.Neutral}
            size={EnumSize.sm}
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              showGraph()
            }}
          >
            Graph
          </Button>
          <RunPlan showRunPlan={showRunPlan} />
          {activePlan != null && <ActivePlan plan={activePlan} />}
        </div>
      </div>
      <Divider />
      {environment != null && (
        <SplitPane
          sizes={[20, 80]}
          minSize={[160]}
          maxSize={[320]}
          snapOffset={0}
          className="flex w-full h-full overflow-hidden"
        >
          <FolderTree project={project} />
          <Editor environment={environment} />
        </SplitPane>
      )}
      <Divider />
      <div className="px-2 py-1 text-xs">Version: 0.0.1</div>
      <Modal
        show={(isGraphOpen || isPlanOpen) && isFalse(isClosingModal)}
        afterLeave={() => {
          setPlanAction(EnumPlanAction.None)
          setIsClosingModal(false)
          setIsGraphOpen(false)
          setIsPlanOpen(false)
        }}
      >
        <Dialog.Panel className="w-full transform overflow-hidden rounded-2xl bg-theme text-left align-middle shadow-xl transition-all">
          {environment != null && isPlanOpen && (
            <PlanProvider>
              <Plan
                environment={environment}
                isInitialPlanRun={
                  environment?.isDefault == null ||
                  isTrue(environment?.isDefault)
                }
                disabled={isClosingModal}
                initialStartDate={initialStartDate}
                initialEndDate={initialEndDate}
                onClose={closeModal}
              />
            </PlanProvider>
          )}

          {isGraphOpen && <Graph closeGraph={closeModal} />}
        </Dialog.Panel>
      </Modal>
    </>
  )
}
