import {
  EnumPlanAction,
  EnumPlanState,
  PlanAction,
  PlanProgress,
  PlanState,
} from '../../../context/plan'
import { isArrayNotEmpty, isFalse } from '../../../utils'

export function getActionName(
  action: PlanAction,
  options: string[] = [],
  fallback: string = 'Start',
): string {
  if (!options.includes(action)) return fallback

  let name: string

  switch (action) {
    case EnumPlanAction.Done:
      name = 'Done'
      break
    case EnumPlanAction.Running:
      name = 'Running...'
      break
    case EnumPlanAction.Applying:
      name = 'Applying...'
      break
    case EnumPlanAction.Cancelling:
      name = 'Cancelling...'
      break
    case EnumPlanAction.Resetting:
      name = 'Resetting...'
      break
    case EnumPlanAction.Run:
      name = 'Run'
      break
    case EnumPlanAction.Apply:
      name = 'Apply'
      break
    default:
      name = fallback
      break
  }

  return name
}

export function isModified<T extends object>(modified?: T): boolean {
  return Object.values(modified ?? {}).some(isArrayNotEmpty)
}

export function getBackfillStepHealine({
  planAction,
  planState,
  hasBackfill,
  hasChanges,
  hasNoChange,
  mostRecentPlan,
}: {
  planAction: PlanAction
  planState: PlanState
  hasBackfill: boolean
  hasChanges: boolean
  hasNoChange: boolean
  mostRecentPlan?: PlanProgress
}): string {
  const isMostRecentPlanLogical =
    mostRecentPlan != null && mostRecentPlan.type === 'logical'
  const isMostRecentPlanBackfill =
    mostRecentPlan != null && mostRecentPlan.type === 'backfill'

  if (hasNoChange) return 'No Changes'
  if (hasBackfill) return 'Needs Backfill'
  if (hasChanges && isFalse(hasBackfill))
    return 'Logical Update Will Be Applied'
  if (planAction === EnumPlanAction.Running) return 'Collecting Backfill...'
  if (planState === EnumPlanState.Applying && isFalse(hasBackfill))
    return 'Applying...'
  if (planState === EnumPlanState.Failed) return 'Failed'
  if (planState === EnumPlanState.Cancelled) return 'Cancelled'
  if (planState === EnumPlanState.Finished && isMostRecentPlanBackfill)
    return 'Completed Backfill'
  if (planState === EnumPlanState.Finished && isMostRecentPlanLogical)
    return 'Completed Logical Update'
  if (isMostRecentPlanBackfill) return 'Most Recent Backfill'
  if (isMostRecentPlanLogical) return 'Most Recent Logical Update'

  return 'Updating...'
}
