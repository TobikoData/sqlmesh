import { useApiPlan, useApiContextCancel } from '../../../api'
import { useEffect, useMemo, useState } from 'react'
import PlanWizard from './PlanWizard'
import { useQueryClient } from '@tanstack/react-query'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
} from '../../../context/plan'
import {
  includes,
  isArrayNotEmpty,
  isFalse,
  toDate,
  toDateFormat,
} from '../../../utils'
import { useChannel } from '../../../api/channels'
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
}: {
  environment: EnvironmentName
  onCancel: () => void
}): JSX.Element {
  const client = useQueryClient()

  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const category = useStorePlan(s => s.category)
  const setPlanAction = useStorePlan(s => s.setAction)
  const setPlanState = useStorePlan(s => s.setState)
  const setCategory = useStorePlan(s => s.setCategory)
  const setBackfillDate = useStorePlan(s => s.setBackfillDate)
  const updateTasks = useStorePlan(s => s.updateTasks)
  const resetPlanOptions = useStorePlan(s => s.resetPlanOptions)

  const [plan, setPlan] = useState<ContextEnvironment>()

  const [subscribe] = useChannel('/api/tasks', updateTasks)

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

  useEffect(() => {
    if (includes([EnumPlanState.Finished, EnumPlanState.Failed], planState)) {
      setPlan(undefined)
    }
  }, [planState])

  useEffect(() => {
    if (
      planAction === EnumPlanAction.Run ||
      planAction === EnumPlanAction.Closing
    )
      return

    if (hasChanges || hasBackfill) {
      setPlanAction(EnumPlanAction.Apply)
    }

    if (isFalse(hasChanges) && isFalse(hasBackfill)) {
      setPlanAction(EnumPlanAction.Done)
    }
  }, [hasChanges, hasBackfill, plan])

  function cleanUp(): void {
    void useApiContextCancel(client)

    setPlan(undefined)
    setCategory(undefined)
    resetPlanOptions()
  }

  function reset(): void {
    setPlanAction(EnumPlanAction.Resetting)

    cleanUp()

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
      .then(data => {
        if (data.ok) {
          subscribe()
        }
      })
      .catch(error => {
        console.error(error)

        reset()
      })
  }

  function close(): void {
    setPlanAction(EnumPlanAction.Closing)

    cleanUp()
  }

  function run(): void {
    setPlanAction(EnumPlanAction.Running)
    setPlan(undefined)

    refetch()
      .then(({ data }) => {
        setPlan(data)

        if (data != null) {
          setBackfillDate('start', toDateFormat(toDate(data.start)))
          setBackfillDate('end', toDateFormat(toDate(data.end)))
        }
      })
      .catch(console.error)
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
        />
      </div>
      <Divider />
      <PlanActions
        environment={environment}
        planAction={planAction}
        shouldApplyWithBackfill={hasBackfill && category?.id !== 'no-change'}
        apply={apply}
        run={run}
        cancel={cancel}
        close={close}
        reset={reset}
      />
    </div>
  )
}
