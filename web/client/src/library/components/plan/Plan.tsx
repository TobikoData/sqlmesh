import { useApiPlan, useApiContextCancel } from '../../../api'
import { useEffect, useMemo, useState } from 'react'
import PlanWizard from './PlanWizard'
import { useQueryClient } from '@tanstack/react-query'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
  EnumCategoryType,
  PlanProgress,
} from '../../../context/plan'
import {
  includes,
  isArrayNotEmpty,
  isFalse,
  toDate,
  toDateFormat,
} from '../../../utils'
import { isModified } from './help'
import { Divider } from '../divider/Divider'
import { EnvironmentName } from '~/context/context'
import {
  applyApiCommandsApplyPost,
  ContextEnvironment,
  ContextEnvironmentBackfill,
  ContextEnvironmentChanges,
} from '~/api/client'
import PlanHeader from './PlanHeader'
import PlanActions from './PlanActions'

export default function Plan({
  environment,
  onCancel,
  onClose,
  disabled,
}: {
  environment: EnvironmentName
  onCancel: () => void
  onClose: () => void
  disabled: boolean
}): JSX.Element {
  const client = useQueryClient()

  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const category = useStorePlan(s => s.category)
  const activePlan = useStorePlan(s => s.activePlan)
  const setPlanAction = useStorePlan(s => s.setAction)
  const setPlanState = useStorePlan(s => s.setState)
  const setCategory = useStorePlan(s => s.setCategory)
  const setBackfillDate = useStorePlan(s => s.setBackfillDate)
  const resetPlanOptions = useStorePlan(s => s.resetPlanOptions)

  const [plan, setPlan] = useState<ContextEnvironment>()
  const [mostRecentPlan, setMostRecentPlan] = useState<PlanProgress>()

  const { refetch } = useApiPlan(environment)

  const [changes, hasChanges, backfills, hasBackfill] = useMemo((): [
    ContextEnvironmentChanges | undefined,
    boolean,
    ContextEnvironmentBackfill[] | undefined,
    boolean,
  ] => {
    const hasChanges = [
      isModified(plan?.changes?.modified),
      isArrayNotEmpty(plan?.changes?.added),
      isArrayNotEmpty(plan?.changes?.removed),
    ].some(Boolean)

    return [
      plan?.changes,
      hasChanges,
      plan?.backfills,
      isArrayNotEmpty(plan?.backfills),
    ]
  }, [plan])

  const [isDone, hasChangesOrBackfill] = useMemo(() => {
    const hasPlan = plan != null
    const hasChangesOrBackfill = hasChanges || hasBackfill
    const isPlanFinishedOrFailed = includes(
      [EnumPlanState.Finished, EnumPlanState.Failed],
      planState,
    )

    return [
      (hasPlan && isFalse(hasChangesOrBackfill)) || isPlanFinishedOrFailed,
      hasChangesOrBackfill,
    ]
  }, [plan, hasChanges, hasBackfill, planState])

  useEffect(() => {
    return () => {
      cleanUp()
    }
  }, [])

  useEffect(() => {
    if (hasChangesOrBackfill) {
      setPlanAction(EnumPlanAction.Apply)
    }

    if (isDone) {
      setPlanAction(EnumPlanAction.Done)
    }
  }, [plan, hasChangesOrBackfill, isDone])

  useEffect(() => {
    if (activePlan != null) {
      setMostRecentPlan(activePlan)
    }
  }, [activePlan])

  function cleanUp(): void {
    void useApiContextCancel(client)

    setPlan(undefined)
    setMostRecentPlan(undefined)
    setCategory(undefined)
    resetPlanOptions()
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

    applyApiCommandsApplyPost({
      environment,
    })
      .then((data: any) => {
        if (data.type === 'logical') {
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
    setPlan(undefined)

    void refetch()
      .then(({ data }) => {
        setPlan(data)

        if (data == null) return

        setBackfillDate('start', toDateFormat(toDate(data.start), 'mm/dd/yyyy'))
        setBackfillDate('end', toDateFormat(toDate(data.end), 'mm/dd/yyyy'))
      })
      .catch(console.error)
      .finally(() => {
        setPlanAction(EnumPlanAction.Run)
        setPlanState(EnumPlanState.Init)
      })
  }

  return (
    <div className="flex flex-col w-full max-h-[90vh] overflow-hidden">
      <PlanHeader environment={environment} />
      <Divider />
      <div className="flex flex-col w-full h-full overflow-hidden overflow-y-auto p-4 scrollbar scrollbar--vertical">
        <PlanWizard
          environment={environment}
          changes={changes}
          hasChanges={hasChanges}
          backfills={backfills}
          hasBackfill={hasBackfill}
          mostRecentPlan={mostRecentPlan}
        />
      </div>
      <Divider />
      <PlanActions
        environment={environment}
        planAction={planAction}
        shouldApplyWithBackfill={
          hasBackfill && category?.id !== EnumCategoryType.NoChange
        }
        apply={apply}
        run={run}
        cancel={cancel}
        close={close}
        reset={reset}
        disabled={disabled}
      />
    </div>
  )
}
