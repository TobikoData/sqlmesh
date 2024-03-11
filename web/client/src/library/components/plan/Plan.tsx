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
import { useNotificationCenter } from '~/library/pages/root/context/notificationCenter'
import { useStoreContext } from '@context/context'

function Plan(): JSX.Element {
  const dispatch = usePlanDispatch()
  const { clearErrors } = useNotificationCenter()

  const environment = useStoreContext(s => s.environment)

  const planAction = useStorePlan(s => s.planAction)
  const setPlanAction = useStorePlan(s => s.setPlanAction)
  const resetPlanTrackers = useStorePlan(s => s.resetPlanTrackers)
  const resetPlanCancel = useStorePlan(s => s.resetPlanCancel)
  const clearPlanApply = useStorePlan(s => s.clearPlanApply)

  const setTests = useStoreProject(s => s.setTests)

  const planPayload = usePlanPayload()
  const applyPayload = useApplyPayload()

  const { refetch: planRun, cancel: cancelRequestPlanRun } = useApiPlanRun(
    environment.name,
    planPayload,
  )
  const { refetch: planApply, cancel: cancelRequestPlanApply } =
    useApiPlanApply(environment.name, applyPayload)
  const { refetch: cancelPlan } = useApiCancelPlan()

  function cleanUp(): void {
    clearErrors()

    dispatch([
      { type: EnumPlanActions.ResetPlanDates },
      { type: EnumPlanActions.ResetPlanOptions },
      { type: EnumPlanActions.ResetTestsReport },
      { type: EnumPlanActions.ResetCategories },
    ])
  }

  function reset(): void {
    setPlanAction(new ModelPlanAction({ value: EnumPlanAction.Run }))

    clearPlanApply()
    cleanUp()

    setTimeout(() => resetPlanTrackers(), 500)
  }

  function cancel(): void {
    setPlanAction(new ModelPlanAction({ value: EnumPlanAction.Cancelling }))

    resetPlanCancel()

    dispatch([{ type: EnumPlanActions.ResetTestsReport }])

    let cancelAction

    if (planAction.isApplying) {
      cancelAction = cancelRequestPlanRun
    } else {
      cancelAction = cancelRequestPlanApply
    }

    void cancelAction()
    void cancelPlan()
  }

  function apply(): void {
    setPlanAction(new ModelPlanAction({ value: EnumPlanAction.Applying }))

    clearPlanApply()

    dispatch([{ type: EnumPlanActions.ResetTestsReport }])

    planApply().catch(console.log)
  }

  function run(): void {
    setPlanAction(new ModelPlanAction({ value: EnumPlanAction.Running }))

    resetPlanTrackers()

    setTests(undefined)

    dispatch([{ type: EnumPlanActions.ResetTestsReport }])

    planRun().catch(console.log)
  }

  return (
    <div className="flex flex-col w-full h-full max-w-[80rem]">
      {environment.isProd && <PlanHeader />}
      <div className="relative w-full h-full flex flex-col pt-2 pl-4 pr-2 overflow-y-scroll hover:scrollbar scrollbar--vertical">
        <PlanOptions />
        <PlanApplyStageTracker />
      </div>
      <PlanActions
        apply={apply}
        run={run}
        cancel={cancel}
        reset={reset}
      />
    </div>
  )
}

export default Plan
