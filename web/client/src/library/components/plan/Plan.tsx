import { isNil, isTrue } from '~/utils'
import { useStorePlan } from '~/context/plan'
import { useApiPlanRun, useApiPlanApply, useApiCancelPlan } from '~/api'
import PlanHeader from './PlanHeader'
import PlanActions from './PlanActions'
import PlanOptions from './PlanOptions'
import { EnumPlanActions, usePlanDispatch } from './context'
import { useApplyPayload, usePlanPayload } from './hooks'
import PlanApplyStageTracker from './PlanApplyStageTracker'
import { EnumPlanAction, ModelPlanAction } from '@models/plan-action'
import { useStoreProject } from '@context/project'
import { useIDE } from '~/library/pages/ide/context'
import { useStoreContext } from '@context/context'
import { Modules } from '@api/client'

function Plan(): JSX.Element {
  const dispatch = usePlanDispatch()
  const { clearErrors } = useIDE()

  const environment = useStoreContext(s => s.environment)
  const modules = useStoreContext(s => s.modules)

  const planOverviewTracker = useStorePlan(s => s.planOverview)
  const planApplyTracker = useStorePlan(s => s.planApply)
  const planCancelTracker = useStorePlan(s => s.planCancel)
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
    resetPlanTrackers()

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
    resetPlanTrackers()

    dispatch([{ type: EnumPlanActions.ResetTestsReport }])

    planApply().catch(console.log)
  }

  function run(): void {
    resetPlanTrackers()

    setTests(undefined)

    dispatch([{ type: EnumPlanActions.ResetTestsReport }])

    void planRun()
  }

  function resetPlanTrackers(): void {
    planOverviewTracker.reset()
    planApplyTracker.reset()
    planCancelTracker.reset()
  }

  return (
    <div className="flex flex-col w-full h-full">
      {environment.isProd && <PlanHeader />}
      <div className="w-full h-full flex flex-col pt-2 pl-4 pr-2 overflow-y-scroll hover:scrollbar scrollbar--vertical">
        <PlanOptions />
        <PlanApplyStageTracker />
      </div>
      {modules.includes(Modules.plans) && (
        <PlanActions
          apply={apply}
          run={run}
          cancel={cancel}
          reset={reset}
        />
      )}
    </div>
  )
}

export default Plan
