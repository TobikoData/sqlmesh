import { isFalse, isNil, isTrue } from '~/utils'
import { useStorePlan } from '~/context/plan'
import { useApiPlanRun, useApiPlanApply, useApiCancelPlan } from '~/api'
import PlanHeader from './PlanHeader'
import PlanActions from './PlanActions'
import PlanOptions from './PlanOptions'
import { EnumPlanActions, usePlanDispatch } from './context'
import { useApplyPayload, usePlanPayload } from './hooks'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { EnumVariant } from '~/types/enum'
import PlanApplyStageTracker from './PlanApplyStageTracker'
import { EnumPlanAction, ModelPlanAction } from '@models/plan-action'
import { useStoreProject } from '@context/project'
import { useIDE } from '~/library/pages/ide/context'
import { useStoreContext } from '@context/context'
import { Transition } from '@headlessui/react'
import PlanActionsDescription from './PlanActionsDescription'
import { Modules } from '@api/client'

function Plan(): JSX.Element {
  const dispatch = usePlanDispatch()
  const { clearErrors } = useIDE()

  const environment = useStoreContext(s => s.environment)
  const modules = useStoreContext(s => s.modules)

  const planOverviewTracker = useStorePlan(s => s.planOverview)
  const planApplyTracker = useStorePlan(s => s.planApply)
  const planAction = useStorePlan(s => s.planAction)
  const setPlanAction = useStorePlan(s => s.setPlanAction)

  const setTests = useStoreProject(s => s.setTests)

  const isInitialPlanRun =
    isNil(environment?.isDefault) || isTrue(environment?.isDefault)

  const planPayload = usePlanPayload({ environment, isInitialPlanRun })
  const applyPayload = useApplyPayload({ isInitialPlanRun })

  const { refetch: planRun, cancel: cancelRequestPlanRun } = useApiPlanRun(
    environment.name,
    planPayload,
  )
  const { refetch: planApply, cancel: cancelRequestPlanApply } =
    useApiPlanApply(environment.name, applyPayload)
  const { refetch: cancelPlan } = useApiCancelPlan()

  function cleanUp(): void {
    dispatch([
      { type: EnumPlanActions.ResetPlanDates },
      { type: EnumPlanActions.ResetPlanOptions },
      { type: EnumPlanActions.ResetTestsReport },
    ])
  }

  function reset(): void {
    planApplyTracker.reset()

    cleanUp()
    clearErrors()

    setPlanAction(new ModelPlanAction({ value: EnumPlanAction.Run }))
  }

  function cancel(): void {
    dispatch([{ type: EnumPlanActions.ResetTestsReport }])
    setPlanAction(new ModelPlanAction({ value: EnumPlanAction.Cancelling }))

    let cancelAction

    if (planAction.isApplying) {
      cancelAction = cancelRequestPlanRun
    } else {
      cancelAction = cancelRequestPlanApply
    }

    cancelAction()
    cancelPlan()
      .then(() => {
        setPlanAction(new ModelPlanAction({ value: EnumPlanAction.Run }))
      })
      .catch(() => {
        reset()
      })
  }

  function apply(): void {
    dispatch([{ type: EnumPlanActions.ResetTestsReport }])

    planApply().catch(console.log)
  }

  function run(): void {
    setTests(undefined)

    planOverviewTracker.reset()
    planApplyTracker.reset()

    dispatch([{ type: EnumPlanActions.ResetTestsReport }])

    void planRun()
  }

  return (
    <div className="flex flex-col w-full h-full overflow-hidden">
      <PlanHeader />
      <div className="w-full h-full px-4 overflow-y-scroll hover:scrollbar scrollbar--vertical">
        <Transition
          appear
          show
          enter="transition ease duration-700 transform"
          enterFrom="opacity-0 -translate-y-full"
          enterTo="opacity-100 translate-y-0"
          leave="transition ease duration-1000 transform"
          leaveFrom="opacity-100 translate-y-0"
          leaveTo="opacity-0 -translate-y-full"
        >
          <PlanOptions />
        </Transition>
        <Transition
          appear
          show
          enter="transition ease duration-700 transform"
          enterFrom="opacity-0 scale-95"
          enterTo="opacity-100 scale-100"
          leave="transition ease duration-1000 transform"
          leaveFrom="opacity-100 scale-100"
          leaveTo="opacity-0 scale-95"
        >
          {planAction.isCancelling ? (
            <CancellingPlanApply />
          ) : (
            <PlanApplyStageTracker />
          )}
        </Transition>
      </div>
      {modules.includes(Modules.plans) && (
        <>
          <Transition
            appear
            show={isFalse(planAction.isDone)}
            enter="transition ease duration-1000 transform"
            enterFrom="opacity-0 translate-y-full"
            enterTo="opacity-100 translate-y-0"
            leave="transition ease duration-500 transform"
            leaveFrom="opacity-100 translate-y-0"
            leaveTo="opacity-0 translate-y-full"
          >
            <PlanActionsDescription />
          </Transition>
          <PlanActions
            apply={apply}
            run={run}
            cancel={cancel}
            reset={reset}
          />
        </>
      )}
    </div>
  )
}

export default Plan

function CancellingPlanApply(): JSX.Element {
  return (
    <div className="w-full h-full p-4">
      <div className="w-full h-full flex justify-center items-center p-4 bg-warning-10 rounded-lg overflow-hidden">
        <Loading className="inline-block">
          <Spinner
            variant={EnumVariant.Warning}
            className="w-3 h-3 border border-neutral-10 mr-4"
          />
          <h3 className="text-2xl text-warning-500 font-bold">
            Cancelling Plan...
          </h3>
        </Loading>
      </div>
    </div>
  )
}
