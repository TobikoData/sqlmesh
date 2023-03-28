import { useCallback, useEffect, useRef, useState } from 'react'
import { debounceAsync, includes, isFalse } from '~/utils'
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
  apiCancelPlanRun,
  apiCancelPlanApply,
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
  const { auto_apply, hasChanges, hasBackfills, hasVirtualUpdate } = usePlan()

  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const activePlan = useStorePlan(s => s.activePlan)
  const setPlanAction = useStorePlan(s => s.setAction)
  const setPlanState = useStorePlan(s => s.setState)

  const elTaskProgress = useRef<HTMLDivElement>(null)

  const [isPlanRan, seIsPlanRan] = useState(false)
  const [error, setError] = useState<Error>()

  const planPayload = usePlanPayload({ environment, isInitialPlanRun })
  const applyPayload = useApplyPayload({ isInitialPlanRun })

  const { refetch: planRun } = useApiPlanRun(environment.name, planPayload)
  const { refetch: planApply } = useApiPlanApply(environment.name, applyPayload)

  const debouncedPlanRun = useCallback(debounceAsync(planRun, 1000, true), [
    planRun,
  ])

  useEffect(() => {
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

      apiCancelPlanRun(client)
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
      includes([EnumPlanState.Finished, EnumPlanState.Failed], planState)
    ) {
      setPlanAction(EnumPlanAction.Done)
    } else {
      setPlanAction(EnumPlanAction.Apply)
    }
  }, [planState, isPlanRan, hasChanges, hasBackfills])

  useEffect(() => {
    if (activePlan == null) return

    dispatch({
      type: EnumPlanActions.BackfillProgress,
      activeBackfill: activePlan,
    })
  }, [activePlan])

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

    cleanUp()
    setPlanState(EnumPlanState.Init)

    setPlanAction(EnumPlanAction.Run)
  }

  function close(): void {
    onClose()
  }

  function cancel(): void {
    setError(undefined)
    setPlanState(EnumPlanState.Cancelling)
    setPlanAction(EnumPlanAction.Cancelling)

    apiCancelPlanApply(client)
      .then(() => {
        setPlanAction(EnumPlanAction.Run)
        setPlanState(EnumPlanState.Cancelled)
      })
      .catch(error => {
        if (isCancelledError(error)) {
          console.log('apiCancelPlanApply', 'Request aborted by React Query')
        } else {
          console.log('apiCancelPlanApply', error)
          setError(error)
          reset()
        }
      })
  }

  function apply(): void {
    setError(undefined)
    setPlanAction(EnumPlanAction.Applying)
    setPlanState(EnumPlanState.Applying)

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
          console.log('planApply', error)
          setError(error)
          reset()
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
    setError(undefined)
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
          console.log('planRun', error)
          setError(error)
          reset()
        }
      })
  }

  return (
    <div className="flex flex-col w-full h-full overflow-hidden pt-6">
      <Plan.Header error={error} />
      <Divider />
      <div className="flex flex-col w-full h-full overflow-hidden overflow-y-auto p-4 scrollbar scrollbar--vertical">
        <Plan.Wizard setRefTaskProgress={elTaskProgress} />
      </div>
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
