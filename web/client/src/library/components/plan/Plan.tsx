import { isNil, isTrue } from '~/utils'
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
import { type ModelEnvironment } from '@models/environment'

function Plan({
  environment,
  disabled,
  onClose,
}: {
  environment: ModelEnvironment
  disabled: boolean
  onClose: () => void
}): JSX.Element {
  const dispatch = usePlanDispatch()
  const { removeError } = useIDE()

  const { auto_apply } = usePlan()

  const isRunningPlan = useStoreContext(s => s.isRunningPlan)

  const planOverviewTracker = useStorePlan(s => s.planOverview)
  const planApplyTracker = useStorePlan(s => s.planApply)
  const planCancelTracker = useStorePlan(s => s.planCancel)

  const isInitialPlanRun =
    isNil(environment?.isDefault) || isTrue(environment?.isDefault)

  const planPayload = usePlanPayload({ environment, isInitialPlanRun })
  const applyPayload = useApplyPayload({ isInitialPlanRun })

  const {
    refetch: planRun,
    cancel: cancelRequestPlanRun,
    isFetching: isFetchingPlanRun,
  } = useApiPlanRun(environment.name, planPayload)
  const {
    refetch: planApply,
    cancel: cancelRequestPlanApply,
    isFetching: isFetchingPlanApply,
  } = useApiPlanApply(environment.name, applyPayload)
  const { refetch: cancelPlan, isFetching: isFetchingPlanCancel } =
    useApiCancelPlan()

  const [planAction, setPlanAction] = useState<PlanAction>(EnumPlanAction.Run)

  useEffect(() => {
    if (environment.name !== planOverviewTracker.environment) {
      setPlanAction(EnumPlanAction.Run)
    } else if (
      ((planOverviewTracker.isFinished || planApplyTracker.isFinished) &&
        isNil(planOverviewTracker.applyType)) ||
      planOverviewTracker.isFailed ||
      planApplyTracker.isFailed
    ) {
      setPlanAction(EnumPlanAction.Done)
    } else if (isFetchingPlanRun) {
      setPlanAction(EnumPlanAction.Running)
    } else if (isFetchingPlanApply) {
      setPlanAction(EnumPlanAction.Applying)
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
  }, [
    planOverviewTracker,
    planApplyTracker,
    isRunningPlan,
    isFetchingPlanRun,
    isFetchingPlanApply,
    environment,
  ])

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

  const isFetching =
    isFetchingPlanRun || isFetchingPlanApply || isFetchingPlanCancel
  const showPlanApplyTracker =
    (planApplyTracker.isFinished || planApplyTracker.isRunning) &&
    planApplyTracker.environment === environment.name
  const showPlanOverviewTracker =
    (planOverviewTracker.isFinished || planOverviewTracker.isRunning) &&
    planOverviewTracker.environment === environment.name
  const showPlanTracker = showPlanApplyTracker || showPlanOverviewTracker
  const showPlanCancel = planAction === EnumPlanAction.Cancelling

  return (
    <div className="flex flex-col w-full h-full overflow-hidden">
      <PlanHeader />
      <div className="w-full h-full px-4 overflow-y-scroll hover:scrollbar scrollbar--vertical">
        {showPlanCancel ? (
          <CancellingPlanApply />
        ) : showPlanTracker ? (
          <PlanApplyStageTracker />
        ) : (
          <PlanOptions className="w-full" />
        )}
      </div>
      <Divider />
      <PlanActions
        planAction={planAction}
        apply={apply}
        run={run}
        cancel={cancel}
        close={close}
        reset={reset}
        disabled={isFetching ?? disabled}
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
