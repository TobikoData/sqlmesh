import { useEffect, useState } from 'react'
import { includes, isFalse, isNil } from '~/utils'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
  EnumPlanApplyType,
} from '~/context/plan'
import { Divider } from '~/library/components/divider/Divider'
import { useApiPlanRun, useApiPlanApply, useApiCancelPlan } from '~/api'
import {
  type ContextEnvironmentEnd,
  type ContextEnvironmentStart,
} from '~/api/client'
import PlanWizard from './PlanWizard'
import PlanHeader from './PlanHeader'
import PlanActions from './PlanActions'
import PlanWizardStepOptions from './PlanWizardStepOptions'
import { EnumPlanActions, usePlan, usePlanDispatch } from './context'
import PlanBackfillDates from './PlanBackfillDates'
import { type ModelEnvironment } from '~/models/environment'
import { useApplyPayload, usePlanPayload } from './hooks'
import { useChannelEvents } from '@api/channels'
import { EnumErrorKey, useIDE } from '~/library/pages/ide/context'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { EnumVariant } from '~/types/enum'

function Plan({
  environment,
  isInitialPlanRun,
  initialStartDate,
  initialEndDate,
  disabled,
  onClose,
}: {
  environment: ModelEnvironment
  isInitialPlanRun: boolean
  initialStartDate?: ContextEnvironmentStart
  initialEndDate?: ContextEnvironmentEnd
  disabled: boolean
  onClose: () => void
}): JSX.Element {
  const dispatch = usePlanDispatch()
  const { errors, removeError } = useIDE()

  const { auto_apply, hasChanges, hasBackfills, hasVirtualUpdate } = usePlan()

  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const activePlan = useStorePlan(s => s.activePlan)
  const setActivePlan = useStorePlan(s => s.setActivePlan)
  const setPlanAction = useStorePlan(s => s.setAction)
  const setPlanState = useStorePlan(s => s.setState)

  const [isPlanRan, setIsPlanRan] = useState(false)

  const channel = useChannelEvents()

  const planPayload = usePlanPayload({ environment, isInitialPlanRun })
  const applyPayload = useApplyPayload({ isInitialPlanRun })

  const { refetch: planRun, cancel: cancelRequestPlanRun } = useApiPlanRun(
    environment.name,
    planPayload,
  )
  const { refetch: planApply, cancel: cancelRequestPlanApply } =
    useApiPlanApply(environment.name, applyPayload)
  const { refetch: cancelPlan } = useApiCancelPlan()

  useEffect(() => {
    const channelTests = channel('tests', updateTestsReport)
    const channelPlanChangesReport = channel(
      'plan-changes',
      updatePlanChangesReport,
    )
    const channelPlanValidateReport = channel(
      'plan-validate',
      updatePlanValidateReport,
    )
    const channelApplyReport = channel('apply', updateApplyReport)

    dispatch([
      {
        type: EnumPlanActions.Dates,
        start: initialStartDate,
        end: initialEndDate,
      },
    ])

    channelTests?.subscribe()
    channelPlanChangesReport?.subscribe()
    channelPlanValidateReport?.subscribe()
    channelApplyReport?.subscribe()

    return () => {
      void cancelRequestPlanRun()

      channelTests?.unsubscribe()
      channelPlanChangesReport?.unsubscribe()
      channelPlanValidateReport?.unsubscribe()
      channelApplyReport?.unsubscribe()
    }
  }, [])

  useEffect(() => {
    dispatch([
      {
        type: EnumPlanActions.External,
        isInitialPlanRun,
      },
    ])

    if (isInitialPlanRun) {
      dispatch([
        {
          type: EnumPlanActions.PlanOptions,
          skip_backfill: false,
          forward_only: false,
          no_auto_categorization: false,
          no_gaps: false,
          include_unmodified: true,
        },
      ])
    }
  }, [isInitialPlanRun])

  useEffect(() => {
    if (
      (isFalse(isPlanRan) && environment.isInitial) ||
      includes(
        [
          EnumPlanState.Running,
          EnumPlanState.Applying,
          EnumPlanState.Cancelling,
        ],
        planState,
      )
    )
      return

    if (isFalse(isPlanRan)) {
      setPlanAction(EnumPlanAction.Run)
    } else if (
      (isFalse(hasChanges || hasBackfills) && isFalse(hasVirtualUpdate)) ||
      planState === EnumPlanState.Finished
    ) {
      setPlanAction(EnumPlanAction.Done)
    } else if (planState === EnumPlanState.Failed) {
      setPlanAction(EnumPlanAction.None)
    } else {
      setPlanAction(EnumPlanAction.Apply)
    }
  }, [planState, isPlanRan, hasChanges, hasBackfills, hasVirtualUpdate])

  useEffect(() => {
    if (activePlan == null) return

    dispatch({
      type: EnumPlanActions.BackfillProgress,
      activeBackfill: activePlan,
    })
  }, [activePlan])

  useEffect(() => {
    if (errors.size === 0) return

    setActivePlan(undefined)
    setPlanState(EnumPlanState.Failed)
  }, [errors])

  function updateTestsReport(data: any): void {
    dispatch([
      isNil(data.message)
        ? {
            type: EnumPlanActions.TestsReportErrors,
            testsReportErrors: data,
          }
        : {
            type: EnumPlanActions.TestsReportMessages,
            testsReportMessages: data,
          },
    ])
  }

  function updatePlanChangesReport(data: any): void {
    dispatch([
      {
        type: EnumPlanActions.PlanChangesReport,
        planChangesReport: data,
      },
    ])
  }

  function updatePlanValidateReport(data: any): void {
    dispatch([
      {
        type: EnumPlanActions.PlanValidateReport,
        planValidateReport: data,
      },
    ])
  }

  function updateApplyReport(data: any): void {
    dispatch([
      {
        type: EnumPlanActions.ApplyReport,
        applyReport: data,
      },
    ])
  }

  function cleanUp(): void {
    setIsPlanRan(false)

    dispatch([
      {
        type: EnumPlanActions.ResetBackfills,
      },
      {
        type: EnumPlanActions.ResetChanges,
      },
      {
        type: EnumPlanActions.Dates,
        start: initialStartDate,
        end: initialEndDate,
      },
      {
        type: EnumPlanActions.ResetPlanOptions,
      },
      {
        type: EnumPlanActions.PlanChangesReport,
        planChangesReport: undefined,
      },
      {
        type: EnumPlanActions.ApplyReport,
        applyReport: undefined,
      },
    ])
  }

  function reset(): void {
    setPlanAction(EnumPlanAction.Resetting)

    cleanUp()
    setPlanState(EnumPlanState.Init)

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
    setPlanState(EnumPlanState.Cancelling)
    setPlanAction(EnumPlanAction.Cancelling)

    let cancelAction

    if (planAction === EnumPlanAction.Applying) {
      cancelAction = cancelRequestPlanRun

      channel('tests')?.unsubscribe()
      channel('tasks')?.unsubscribe()
      channel('apply')?.unsubscribe()
      channel('plan-changes')?.unsubscribe()
      channel('plan-validate')?.unsubscribe()

      setActivePlan(undefined)
    } else {
      cancelAction = cancelRequestPlanApply
    }

    cancelAction()
    cancelPlan()
      .then(() => {
        setPlanAction(EnumPlanAction.Run)
        setPlanState(EnumPlanState.Cancelled)
      })
      .catch(() => {
        reset()
      })
  }

  function apply(): void {
    setPlanAction(EnumPlanAction.Applying)
    setPlanState(EnumPlanState.Applying)

    dispatch([
      {
        type: EnumPlanActions.ResetTestsReport,
      },
    ])

    planApply()
      .then(({ data }) => {
        if (data?.type === EnumPlanApplyType.Virtual) {
          setPlanState(EnumPlanState.Finished)
        }
      })
      .catch(console.log)
  }

  function run(): void {
    dispatch([
      {
        type: EnumPlanActions.ResetTestsReport,
      },
    ])
    setPlanAction(EnumPlanAction.Running)
    setPlanState(EnumPlanState.Running)

    void planRun().then(({ data }) => {
      dispatch([
        {
          type: EnumPlanActions.Backfills,
          backfills: data?.backfills,
        },
        {
          type: EnumPlanActions.Changes,
          ...data?.changes,
        },
        {
          type: EnumPlanActions.Dates,
          start: data?.start,
          end: data?.end,
        },
      ])

      setIsPlanRan(true)
      setPlanState(EnumPlanState.Init)

      if (auto_apply) {
        apply()
      } else {
        setPlanAction(EnumPlanAction.Apply)
      }
    })
  }

  return (
    <div className="flex flex-col w-full h-full overflow-hidden pt-6">
      <PlanBlock hasDivider={true} />
      <Divider />
      <Plan.Actions
        disabled={disabled}
        planAction={planAction}
        apply={apply}
        run={run}
        cancel={cancel}
        close={close}
        reset={reset}
      />
    </div>
  )
}

Plan.Actions = PlanActions
Plan.Header = PlanHeader
Plan.Wizard = PlanWizard
Plan.StepOptions = PlanWizardStepOptions
Plan.BackfillDates = PlanBackfillDates

export default Plan

function PlanBlock({
  hasDivider = false,
}: {
  hasDivider?: boolean
}): JSX.Element {
  const planAction = useStorePlan(s => s.action)

  return (
    <>
      <Plan.Header />
      {hasDivider && <Divider />}
      {planAction === EnumPlanAction.Cancelling ? (
        <CancellingPlanOrApply />
      ) : (
        <Plan.Wizard />
      )}
    </>
  )
}

function CancellingPlanOrApply(): JSX.Element {
  return (
    <div className="w-full h-full p-4">
      <div className="w-full h-full flex justify-center items-center p-4 bg-warning-10 rounded-lg overflow-hidden">
        <Loading className="inline-block">
          <Spinner
            variant={EnumVariant.Warning}
            className="w-3 h-3 border border-neutral-10 mr-4"
          />
          <h3 className="text-2xl text-warning-500 font-bold">
            Canceling Plan...
          </h3>
        </Loading>
      </div>
    </div>
  )
}
