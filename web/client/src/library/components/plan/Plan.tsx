import { useEffect } from 'react'
import { includes, isFalse } from '~/utils'
import { EnumPlanState, EnumPlanAction, useStorePlan } from '~/context/plan'
import { Divider } from '~/library/components/divider/Divider'
import { EnvironmentName } from '~/context/context'
import { useApiPlan } from '~/api'
import {
  Apply,
  applyApiCommandsApplyPost,
  ApplyApiCommandsApplyPostParams,
  ApplyType,
} from '~/api/client'
import PlanWizard from './PlanWizard'
import PlanHeader from './PlanHeader'
import PlanActions from './PlanActions'
import PlanWizardStepOptions from './PlanWizardStepOptions'
import { EnumPlanActions, usePlan, usePlanDispatch } from './context'
import PlanBackfillDates from './PlanBackfillDates'

function Plan({
  environment,
  isInitial,
  onCancel,
  onClose,
  disabled,
}: {
  environment: EnvironmentName
  isInitial: boolean
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
    category,
    is_initial,
  } = usePlan()

  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const activePlan = useStorePlan(s => s.activePlan)
  const setPlanAction = useStorePlan(s => s.setAction)
  const setPlanState = useStorePlan(s => s.setState)

  const { refetch } = useApiPlan(
    environment,
    start,
    end,
    no_gaps,
    skip_backfill,
    forward_only,
    auto_apply,
    from,
    no_auto_categorization,
    skip_tests,
    restate_models,
  )

  useEffect(() => {
    return () => {
      cleanUp()
    }
  }, [])

  useEffect(() => {
    dispatch({
      type: EnumPlanActions.External,
      is_initial: isInitial,
    })
  }, [isInitial])

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

    if (start == null && end == null) {
      setPlanAction(EnumPlanAction.Run)
    } else if (
      isFalse(hasChanges || hasBackfills) ||
      includes([EnumPlanState.Finished, EnumPlanState.Failed], planState)
    ) {
      setPlanAction(EnumPlanAction.Done)
    } else {
      setPlanAction(EnumPlanAction.Apply)
    }
  }, [planState, start, end, hasChanges, hasBackfills])

  useEffect(() => {
    if (activePlan == null) return

    dispatch({
      type: EnumPlanActions.BackfillProgress,
      activeBackfill: activePlan,
    })
  }, [activePlan])

  function cleanUp(): void {
    dispatch([
      {
        type: EnumPlanActions.ResetBackfills,
      },
      {
        type: EnumPlanActions.ResetChanges,
      },
      {
        type: EnumPlanActions.ResetDates,
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
    }

    if (hasBackfills && isFalse(is_initial)) {
      payload.start = start
    }

    if (hasChanges && isFalse(is_initial)) {
      payload.category = category.value
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
        setPlanAction(EnumPlanAction.Run)
        setPlanState(EnumPlanState.Init)
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
