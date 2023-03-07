import {
  EnumPlanAction,
  EnumPlanState,
  PlanAction,
  PlanState,
} from '../../../context/plan'
import { isArrayNotEmpty } from '../../../utils'

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
}: {
  planAction: PlanAction
  planState: PlanState
  hasBackfill: boolean
  hasLogicalUpdate: boolean
  hasNoChange: boolean
}): string {
  if (planAction === EnumPlanAction.Running) return 'Collecting Backfill...'
  if (planState === EnumPlanState.Applying && hasBackfill)
    return 'Backfilling...'
  if (planState === EnumPlanState.Applying && hasLogicalUpdate)
    return 'Applying...'
  if (planState === EnumPlanState.Failed) return 'Failed'
  if (planState === EnumPlanState.Cancelled) return 'Cancelled'
  if (planState === EnumPlanState.Finished && hasBackfill)
    return 'Completed Backfill'
  if (planState === EnumPlanState.Finished && hasLogicalUpdate)
    return 'Completed Logical Update'
  if (hasBackfill) return 'Needs Backfill'
  if (hasLogicalUpdate) return 'Logical Update Will Be Applied'
  if (hasNoChange) return 'No Changes'

  return 'Updating...'
}
