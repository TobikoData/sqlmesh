import ModalSidebar from '@components/modal/ModalDrawer'
import Plan from '@components/plan/Plan'
import PlanProvider from '@components/plan/context'
import { useStoreContext } from '@context/context'
import { useStorePlan, EnumPlanAction } from '@context/plan'
import { Dialog } from '@headlessui/react'
import { isTrue, isFalse } from '@utils/index'
import { memo, useState } from 'react'
import { useIDE } from './context'

const PlanSidebar = memo(function PlanSidebar(): JSX.Element {
  const { isPlanOpen, setIsPlanOpen } = useIDE()

  const environment = useStoreContext(s => s.environment)
  const initialStartDate = useStoreContext(s => s.initialStartDate)
  const initialEndDate = useStoreContext(s => s.initialEndDate)

  const setPlanAction = useStorePlan(s => s.setAction)

  const [isClosingModal, setIsClosingModal] = useState(false)

  function closeModal(): void {
    setIsClosingModal(true)
  }

  const shouldShow = isTrue(isPlanOpen) && isFalse(isClosingModal)

  return (
    <ModalSidebar
      show={shouldShow}
      afterLeave={() => {
        setPlanAction(EnumPlanAction.None)
        setIsClosingModal(false)
        setIsPlanOpen(false)
      }}
    >
      <Dialog.Panel className="bg-theme border-8 border-r-0 border-secondary-10 dark:border-primary-10 absolute w-[90%] md:w-[75%] xl:w-[60%] h-full right-0 flex flex-col">
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
  )
})

export default PlanSidebar
