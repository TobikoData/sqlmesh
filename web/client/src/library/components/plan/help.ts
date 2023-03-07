import {
  EnumPlanAction,
  EnumPlanState,
  PlanAction,
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
  hasLogicalUpdate,
  hasNoChange,
  isBackfilled,
  isLogicalUpdated,
}: {
  planAction: PlanAction
  planState: PlanState
  hasBackfill: boolean
  hasLogicalUpdate: boolean
  hasNoChange: boolean
  isBackfilled: boolean
  isLogicalUpdated: boolean
}): string {
  if (planAction === EnumPlanAction.Running) return 'Collecting Backfill...'
  if (planState === EnumPlanState.Applying && isFalse(hasBackfill))
    return 'Applying...'
  if (planState === EnumPlanState.Failed) return 'Failed'
  if (planState === EnumPlanState.Cancelled) return 'Cancelled'
  if (planState === EnumPlanState.Finished && isBackfilled)
    return 'Completed Backfill'
  if (planState === EnumPlanState.Finished && isLogicalUpdated)
    return 'Completed Logical Update'
  if (hasBackfill) return 'Needs Backfill'
  if (hasLogicalUpdate) return 'Logical Update Will Be Applied'
  if (hasNoChange) return 'No Changes'

  return 'Updating...'
}
