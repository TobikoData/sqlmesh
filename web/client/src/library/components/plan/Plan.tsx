import { isFalse, isNil, isTrue } from '~/utils'
import { EnumPlanAction, useStorePlan, type PlanAction } from '~/context/plan'
import { Divider } from '~/library/components/divider/Divider'
import { useApiPlanRun, useApiPlanApply, useApiCancelPlan } from '~/api'
import PlanHeader from './PlanHeader'
import PlanActions from './PlanActions'
import PlanOptions from './PlanOptions'
import { EnumPlanActions, usePlan, usePlanDispatch } from './context'
import { useApplyPayload, usePlanPayload } from './hooks'
import { EnumErrorKey, useIDE } from '~/library/pages/ide/context'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { EnumVariant } from '~/types/enum'
import PlanApplyStageTracker from './PlanApplyStageTracker'
import { useStoreContext } from '@context/context'
import { useEffect, useState } from 'react'

function Plan({
  disabled,
  onClose,
}: {
  disabled: boolean
  onClose: () => void
}): JSX.Element {
  const dispatch = usePlanDispatch()
  const { removeError } = useIDE()

  const { auto_apply } = usePlan()

  const isRunningPlan = useStoreContext(s => s.isRunningPlan)
  const environment = useStoreContext(s => s.environment)

  const planOverviewTracker = useStorePlan(s => s.planOverview)
  const planApplyTracker = useStorePlan(s => s.planApply)
  const planCancelTracker = useStorePlan(s => s.planCancel)

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

  const [planAction, setPlanAction] = useState<PlanAction>(EnumPlanAction.Run)

  useEffect(() => {
    if (
      planOverviewTracker.isFinished &&
      (planApplyTracker.isFinished ||
        (isFalse(planOverviewTracker.isVirtualUpdate) &&
          isFalse(planOverviewTracker.isBackfillUpdate)))
    ) {
      setPlanAction(EnumPlanAction.Done)
    } else if (isRunningPlan && planApplyTracker.isRunning) {
      setPlanAction(EnumPlanAction.Applying)
    } else if (isRunningPlan && planOverviewTracker.isRunning) {
      setPlanAction(EnumPlanAction.Running)
    } else if (
      planOverviewTracker.isFinished &&
      planOverviewTracker.isVirtualUpdate
    ) {
      setPlanAction(EnumPlanAction.ApplyVirtual)
    } else if (
      planOverviewTracker.isFinished &&
      planOverviewTracker.isBackfillUpdate
    ) {
      setPlanAction(EnumPlanAction.ApplyBackfill)
    } else if (planCancelTracker.isCancelling) {
      setPlanAction(EnumPlanAction.Cancelling)
    } else {
      setPlanAction(EnumPlanAction.Run)
    }
  }, [planOverviewTracker, planApplyTracker, isRunningPlan])

  function cleanUp(): void {
    dispatch([
      {
        type: EnumPlanActions.ResetPlanOptions,
      },
    ])
  }

  function reset(): void {
    planOverviewTracker.reset()
    planApplyTracker.reset()

    cleanUp()

    setPlanAction(EnumPlanAction.Run)
  }

  function close(): void {
    removeError(EnumErrorKey.RunPlan)
    removeError(EnumErrorKey.ApplyPlan)
    cleanUp()
    onClose()
  }

  function cancel(): void {
    dispatch([
      {
        type: EnumPlanActions.ResetTestsReport,
      },
    ])
    setPlanAction(EnumPlanAction.Cancelling)

    let cancelAction

    if (planAction === EnumPlanAction.Applying) {
      cancelAction = cancelRequestPlanRun
    } else {
      cancelAction = cancelRequestPlanApply
    }

    cancelAction()
    cancelPlan()
      .then(() => {
        setPlanAction(EnumPlanAction.Run)
      })
      .catch(() => {
        reset()
      })
  }

  function apply(): void {
    dispatch([
      {
        type: EnumPlanActions.ResetTestsReport,
      },
    ])

    planApply().catch(console.log)
  }

  function run(): void {
    planApplyTracker.reset()

    dispatch([
      {
        type: EnumPlanActions.ResetTestsReport,
      },
    ])

    void planRun().then(({ data }) => {
      dispatch([
        {
          type: EnumPlanActions.Dates,
          start: data?.start,
          end: data?.end,
        },
      ])

      if (auto_apply) {
        apply()
      }
    })
  }

  return (
    <div className="flex flex-col w-full h-full overflow-hidden pt-6">
      <PlanHeader />
      <Divider />
      <div className="w-full h-full px-4 overflow-y-scroll hover:scrollbar scrollbar--vertical">
        {planAction === EnumPlanAction.Cancelling ? (
          <CancellingPlanApply />
        ) : planApplyTracker.isRunning || planOverviewTracker.isFinished ? (
          <PlanApplyStageTracker />
        ) : (
          <PlanOptions className="w-full" />
        )}
      </div>
      <Divider />
      <PlanActions
        planAction={planAction}
        disabled={disabled}
        apply={apply}
        run={run}
        cancel={cancel}
        close={close}
        reset={reset}
      />
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
