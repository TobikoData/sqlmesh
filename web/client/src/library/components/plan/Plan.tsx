import { useCallback, useEffect, useRef, useState } from 'react'
import {
  debounceAsync,
  includes,
  isFalse,
  isObjectNotEmpty,
  isTrue,
} from '~/utils'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
  EnumPlanApplyType,
} from '~/context/plan'
import { Divider } from '~/library/components/divider/Divider'
import {
  useApiPlanRun,
  useApiPlanApply,
  apiCancelPlanApply,
  apiCancelPlanRun,
} from '~/api'
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
import { isCancelledError, useQueryClient } from '@tanstack/react-query'
import { type ModelEnvironment } from '~/models/environment'
import { useApplyPayload, usePlanPayload } from './hooks'
import { useChannelEvents } from '@api/channels'
import SplitPane from '../splitPane/SplitPane'
import { EnumErrorKey, useIDE } from '~/library/pages/ide/context'

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
  const client = useQueryClient()

  const dispatch = usePlanDispatch()
  const { errors, removeError, addError } = useIDE()

  const {
    auto_apply,
    hasChanges,
    hasBackfills,
    hasVirtualUpdate,
    testsReportErrors,
  } = usePlan()

  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const activePlan = useStorePlan(s => s.activePlan)
  const setActivePlan = useStorePlan(s => s.setActivePlan)
  const setPlanAction = useStorePlan(s => s.setAction)
  const setPlanState = useStorePlan(s => s.setState)

  const elTaskProgress = useRef<HTMLDivElement>(null)

  const [isPlanRan, seIsPlanRan] = useState(false)

  const subscribe = useChannelEvents()

  const planPayload = usePlanPayload({ environment, isInitialPlanRun })
  const applyPayload = useApplyPayload({ isInitialPlanRun })

  const { refetch: planRun } = useApiPlanRun(environment.name, planPayload)
  const { refetch: planApply } = useApiPlanApply(environment.name, applyPayload)

  const debouncedPlanRun = useCallback(debounceAsync(planRun, 1000, true), [
    planRun,
  ])

  useEffect(() => {
    const unsubscribeTests = subscribe('tests', testsReport)

    if (environment.isInitial && environment.isDefault) {
      run()
    }

    dispatch([
      {
        type: EnumPlanActions.Dates,
        start: initialStartDate,
        end: initialEndDate,
      },
    ])

    return () => {
      debouncedPlanRun.cancel()

      unsubscribeTests?.()
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

  function testsReport(data: { ok: boolean } & any): void {
    dispatch([
      isTrue(data.ok)
        ? {
            type: EnumPlanActions.TestsReportMessages,
            testsReportMessages: data,
          }
        : {
            type: EnumPlanActions.TestsReportErrors,
            testsReportErrors: data,
          },
    ])
  }

  function cleanUp(): void {
    seIsPlanRan(false)

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
    ])
  }

  function reset(): void {
    setPlanAction(EnumPlanAction.Resetting)

    removeError(EnumErrorKey.General)
    cleanUp()
    setPlanState(EnumPlanState.Init)

    setPlanAction(EnumPlanAction.Run)
  }

  function close(): void {
    removeError(EnumErrorKey.General)
    removeError(EnumErrorKey.RunPlan)
    removeError(EnumErrorKey.ApplyPlan)
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

    const cancelAction =
      planAction === EnumPlanAction.Applying
        ? apiCancelPlanApply
        : apiCancelPlanRun

    cancelAction(client)
      .then(() => {
        setPlanAction(EnumPlanAction.Run)
        setPlanState(EnumPlanState.Cancelled)
      })
      .catch(error => {
        if (isCancelledError(error)) {
          console.log('apiCancelPlanApply', 'Request aborted by React Query')
        } else {
          console.log('apiCancelPlanApply', error)
          reset()
        }
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

    planApply({
      throwOnError: true,
    })
      .then(({ data }) => {
        if (data?.type === EnumPlanApplyType.Virtual) {
          setPlanState(EnumPlanState.Finished)
        }
      })
      .catch(error => {
        if (isCancelledError(error)) {
          console.log('planApply', 'Request aborted by React Query')
        } else {
          addError(EnumErrorKey.ApplyPlan, error)
        }
      })
      .finally(() => {
        elTaskProgress?.current?.scrollIntoView({
          behavior: 'smooth',
          block: 'start',
        })
      })
  }

  function run(): void {
    dispatch([
      {
        type: EnumPlanActions.ResetTestsReport,
      },
    ])
    setPlanAction(EnumPlanAction.Running)
    setPlanState(EnumPlanState.Running)

    debouncedPlanRun({
      throwOnError: true,
    })
      .then(({ data }) => {
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

        seIsPlanRan(true)
        setPlanState(EnumPlanState.Init)

        if (auto_apply) {
          apply()
        } else {
          setPlanAction(EnumPlanAction.Run)
        }
      })
      .catch(error => {
        if (isCancelledError(error)) {
          console.log('planRun', 'Request aborted by React Query')
        } else {
          addError(EnumErrorKey.RunPlan, error)
        }
      })
  }

  const shouldSplitPane = isObjectNotEmpty(testsReportErrors)

  return (
    <div className="flex flex-col w-full h-full overflow-hidden pt-6">
      {shouldSplitPane ? (
        <SplitPane
          sizes={isObjectNotEmpty(testsReportErrors) ? [50, 50] : [30, 70]}
          direction="vertical"
          snapOffset={0}
          className="flex flex-col w-full h-full overflow-hidden"
        >
          <Plan.Header />
          <Plan.Wizard setRefTasksOverview={elTaskProgress} />
        </SplitPane>
      ) : (
        <>
          <Plan.Header />
          <Divider />
          <Plan.Wizard setRefTasksOverview={elTaskProgress} />
        </>
      )}
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
