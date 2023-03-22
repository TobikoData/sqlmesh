import {
  EnumPlanAction,
  EnumPlanState,
  type PlanAction,
  type PlanState,
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

export function getBackfillStepHeadline({
  planAction,
  planState,
  hasBackfills,
  hasVirtualUpdate,
  hasNoChanges,
  skip_backfill,
}: {
  planAction: PlanAction
  planState: PlanState
  hasBackfills: boolean
  hasVirtualUpdate: boolean
  hasNoChanges: boolean
  skip_backfill: boolean
}): string {
  if (skip_backfill) return 'Skipping Backfill'
  if (planAction === EnumPlanAction.Running) return 'Collecting Backfill...'
  if (planState === EnumPlanState.Applying && hasBackfills)
    return 'Backfilling...'
  if (planState === EnumPlanState.Applying && hasVirtualUpdate)
    return 'Applying...'
  if (planState === EnumPlanState.Failed) return 'Failed'
  if (planState === EnumPlanState.Cancelled) return 'Cancelled'
  if (planState === EnumPlanState.Finished && hasBackfills)
    return 'Completed Backfill'
  if (planState === EnumPlanState.Finished && hasVirtualUpdate)
    return 'Completed Virtual Update'
  if (hasBackfills) return 'Needs Backfill'
  if (hasVirtualUpdate) return 'Virtual Update Will Be Applied'
  if (hasNoChanges) return 'No Changes'

  return 'Updating...'
}
