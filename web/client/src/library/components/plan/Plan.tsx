import { useEffect, useMemo, useState } from 'react'
import {
  includes,
  isFalse,
  isStringEmptyOrNil,
  toDate,
  toDateFormat,
} from '~/utils'
import { EnumPlanState, EnumPlanAction, useStorePlan } from '~/context/plan'
import { Divider } from '~/library/components/divider/Divider'
import { EnvironmentName } from '~/context/context'
import { useApiPlanRun, useApiPlanApply, apiCancelPlanApplyAndRun } from '~/api'
import {
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
import { useQueryClient } from '@tanstack/react-query'

function Plan({
  environment,
  isInitialPlanRun,
  initialStartDate,
  initialEndDate,
  disabled,
  onClose,
}: {
  environment: EnvironmentName
  isInitialPlanRun: boolean
  initialStartDate?: ContextEnvironmentStart
  initialEndDate?: ContextEnvironmentEnd
  disabled: boolean
  onClose: () => void
}): JSX.Element {
  const client = useQueryClient()

  const dispatch = usePlanDispatch()
  const {
    start,
    end,
    skip_tests,
    no_gaps,
    skip_backfill,
    forward_only,
    auto_apply,
    create_from,
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

  const { refetch: planRun } = useApiPlanRun(environment, {
    planDates: {
      start,
      end:
        isInitialPlanRun && isStringEmptyOrNil(restate_models)
          ? undefined
          : end,
    },
    additionalOptions: {
      no_gaps,
      skip_backfill,
      forward_only,
      create_from,
      no_auto_categorization,
      skip_tests,
      restate_models,
    },
  })

  const { refetch: planApply } = useApiPlanApply(environment, {
    change_category:
      hasChanges && isFalse(isInitialPlanRun) && isFalse(forward_only)
        ? change_category.value
        : undefined,
    planDates:
      hasBackfills && isFalse(isInitialPlanRun)
        ? {
            start,
            end,
          }
        : undefined,
    additionalOptions: {
      no_gaps,
      skip_backfill,
      forward_only,
      create_from,
      no_auto_categorization,
      skip_tests,
      restate_models,
    },
  })

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
          EnumPlanState.Cancelling,
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

  function close(): void {
    onClose()
  }

  function cancel(): void {
    setPlanState(EnumPlanState.Cancelling)
    setPlanAction(EnumPlanAction.Cancelling)

    void apiCancelPlanApplyAndRun(client, environment)
      .catch(console.error)
      .finally(() => {
        setPlanAction(EnumPlanAction.Run)
        setPlanState(EnumPlanState.Cancelled)
      })
  }

  function apply(): void {
    setPlanAction(EnumPlanAction.Applying)
    setPlanState(EnumPlanState.Applying)

    void planApply()
      .then(({ data }) => {
        if (data?.type === ApplyType.logical) {
          setPlanState(EnumPlanState.Finished)
        }
      })
      .catch(console.error)
  }

  function run(): void {
    setPlanAction(EnumPlanAction.Running)
    setPlanState(EnumPlanState.Running)

    void planRun()
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
