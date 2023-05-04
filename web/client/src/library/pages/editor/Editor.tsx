import { lazy, useState } from 'react'
import FileTree from '@components/fileTree/FileTree'
import PlanProvider from '@components/plan/context'
import SplitPane from '@components/splitPane/SplitPane'
import { useStoreContext } from '@context/context'
import { EnumPlanAction, useStorePlan } from '@context/plan'
import { Dialog } from '@headlessui/react'
import { isFalse, isTrue } from '@utils/index'
import ModalSidebar from '@components/modal/ModalDrawer'
import Editor from '@components/editor/Editor'
import { useIDE } from '../ide/context'
import { useStoreFileTree } from '@context/fileTree'

const Plan = lazy(async () => await import('@components/plan/Plan'))

export default function PageEditor(): JSX.Element {
  const { isPlanOpen, setIsPlanOpen } = useIDE()

  const project = useStoreFileTree(s => s.project)
  const environment = useStoreContext(s => s.environment)
  const initialStartDate = useStoreContext(s => s.initialStartDate)
  const initialEndDate = useStoreContext(s => s.initialEndDate)

  const setPlanAction = useStorePlan(s => s.setAction)

  const [isClosingModal, setIsClosingModal] = useState(false)

  function closeModal(): void {
    setIsClosingModal(true)
  }

  return (
    <>
      {environment != null && (
        <SplitPane
          sizes={[20, 80]}
          minSize={[160]}
          snapOffset={0}
          className="flex w-full h-full overflow-hidden"
        >
          <FileTree project={project} />
          <Editor />
        </SplitPane>
      )}
      <ModalSidebar
        show={isTrue(isPlanOpen) && isFalse(isClosingModal)}
        afterLeave={() => {
          setPlanAction(EnumPlanAction.None)
          setIsClosingModal(false)
          setIsPlanOpen(false)
        }}
      >
        <Dialog.Panel className="bg-theme border-8 border-r-0 border-secondary-10 dark:border-primary-10 absolute w-[90%] md:w-[75%] xl:w-[60%] h-full right-0">
          <PlanProvider>
            <Plan
              environment={environment}
              isInitialPlanRun={
                environment?.isDefault == null || isTrue(environment?.isDefault)
              }
              disabled={isClosingModal}
              initialStartDate={initialStartDate}
              initialEndDate={initialEndDate}
              onClose={closeModal}
            />
          </PlanProvider>
        </Dialog.Panel>
      </ModalSidebar>
    </>
  )
}
