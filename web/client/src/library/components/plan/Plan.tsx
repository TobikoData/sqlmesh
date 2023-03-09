import { useEffect, useMemo, useState } from 'react'
import { includes, isFalse, toDate, toDateFormat } from '~/utils'
import { EnumPlanState, EnumPlanAction, useStorePlan } from '~/context/plan'
import { Divider } from '~/library/components/divider/Divider'
import { EnvironmentName } from '~/context/context'
import { useApiPlan } from '~/api'
import {
  Apply,
  applyApiCommandsApplyPost,
  ApplyApiCommandsApplyPostParams,
  ApplyType,
  ContextEnvironmentEnd,
  ContextEnvironmentStart,
} from '~/api/client'
import PlanWizard from './PlanWizard'
import PlanHeader from './PlanHeader'
import PlanActions from './PlanActions'
import PlanWizardStepOptions from './PlanWizardStepOptions'
import { EnumPlanActions, usePlan, usePlanDispatch } from './context'
import PlanBackfillDates from './PlanBackfillDates'

function Plan({
  environment,
  isInitialPlanRun,
  initialStartDate,
  initialEndDate,
  onCancel,
  onClose,
  disabled,
}: {
  environment: EnvironmentName
  isInitialPlanRun: boolean
  initialStartDate?: ContextEnvironmentStart
  initialEndDate?: ContextEnvironmentEnd
  onCancel: () => void
  onClose: () => void
  disabled: boolean
}): JSX.Element {
  const dispatch = usePlanDispatch()
  const {
    start,
    end,
    skip_tests,
    no_gaps,
    skip_backfill,
    forward_only,
    auto_apply,
    from,
    no_auto_categorization,
    restate_models,
    hasChanges,
    hasBackfills,
    change_category,
  } = usePlan()

  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const activePlan = useStorePlan(s => s.activePlan)
  const setPlanAction = useStorePlan(s => s.setAction)
  const setPlanState = useStorePlan(s => s.setState)

  const [isPlanRan, seIsPlanRan] = useState(false)

  const { refetch } = useApiPlan(
    environment,
    start,
    end,
    no_gaps,
    skip_backfill,
    forward_only,
    from,
    no_auto_categorization,
    skip_tests,
    restate_models,
  )

  const [startDate, endDate] = useMemo(() => {
    const ONE_DAY = 1000 * 60 * 60 * 24
    const end = isInitialPlanRun
      ? initialEndDate
      : toDateFormat(new Date(), 'mm/dd/yyyy')
    const dateinitialEndDate = toDate(end)
    const oneDayOffinitialEndDate =
      dateinitialEndDate == null
        ? undefined
        : new Date(dateinitialEndDate.getTime() - ONE_DAY)
    const start = isInitialPlanRun
      ? initialStartDate
      : toDateFormat(oneDayOffinitialEndDate, 'mm/dd/yyyy')

    return [start, end]
  }, [isInitialPlanRun, initialStartDate, initialEndDate])

  useEffect(() => {
    return () => {
      cleanUp()
    }
  }, [])

  useEffect(() => {
    dispatch([
      {
        type: EnumPlanActions.External,
        isInitialPlanRun,
      },
      {
        type: EnumPlanActions.Dates,
        start: startDate,
        end: endDate,
      },
    ])

    if (isInitialPlanRun) {
      dispatch([
        {
          type: EnumPlanActions.AdditionalOptions,
          skip_backfill: false,
          forward_only: false,
          no_auto_categorization: false,
          no_gaps: false,
        },
      ])
    }
  }, [isInitialPlanRun, initialStartDate, initialEndDate])

  useEffect(() => {
    if (
      includes(
        [
          EnumPlanState.Running,
          EnumPlanState.Applying,
          EnumPlanAction.Cancelling,
        ],
        planState,
      )
    )
      return

    if (isFalse(isPlanRan)) {
      setPlanAction(EnumPlanAction.Run)
    } else if (
      isFalse(hasChanges || hasBackfills) ||
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
        start: startDate,
        end: endDate,
      },
      {
        type: EnumPlanActions.ResetAdditionalOptions,
      },
    ])
  }

  function reset(): void {
    setPlanAction(EnumPlanAction.Resetting)

    cleanUp()
    setPlanState(EnumPlanState.Init)

    setPlanAction(EnumPlanAction.Run)
  }

  function cancel(): void {
    setPlanAction(EnumPlanAction.Cancelling)

    onCancel()

    reset()
  }

  function apply(): void {
    setPlanAction(EnumPlanAction.Applying)
    setPlanState(EnumPlanState.Applying)

    const payload: ApplyApiCommandsApplyPostParams = {
      environment,
      skip_backfill,
      skip_tests,
      no_gaps,
      forward_only,
      no_auto_categorization,
      restate_models,
      from_: from,
    }

    if (hasBackfills && isFalse(isInitialPlanRun)) {
      payload.start = start
      payload.end = end
    }

    if (hasChanges && isFalse(isInitialPlanRun) && isFalse(forward_only)) {
      payload.change_category = change_category.value
    }

    applyApiCommandsApplyPost(payload)
      .then((data: Apply) => {
        if (data.type === ApplyType.logical) {
          setPlanState(EnumPlanState.Finished)
        }
      })
      .catch(error => {
        console.error(error)

        reset()
      })
  }

  function close(): void {
    onClose()
  }

  function run(): void {
    setPlanAction(EnumPlanAction.Running)
    setPlanState(EnumPlanState.Running)

    void refetch()
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
      })
      .catch(console.error)
      .finally(() => {
        seIsPlanRan(true)
        setPlanState(EnumPlanState.Init)

        if (auto_apply) {
          setPlanAction(EnumPlanAction.Apply)

          apply()
        } else {
          setPlanAction(EnumPlanAction.Run)
        }
      })
  }

  return (
    <div className="flex flex-col w-full max-h-[90vh] overflow-hidden">
      <Plan.Header environment={environment} />
      <Divider />
      <div className="flex flex-col w-full h-full overflow-hidden overflow-y-auto p-4 scrollbar scrollbar--vertical">
        <Plan.Wizard environment={environment} />
      </div>
      <Divider />
      <Plan.Actions
        disabled={disabled}
        environment={environment}
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
