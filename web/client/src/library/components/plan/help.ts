import { type ModelPlanOverviewTracker } from '@models/tracker-plan-overview'
import { EnumPlanAction, type PlanAction } from '../../../context/plan'
import { isArrayNotEmpty, isNil, isNotNil } from '../../../utils'
import { type ModelPlanApplyTracker } from '@models/tracker-plan-apply'

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
    case EnumPlanAction.Run:
      name = 'Run'
      break
    case EnumPlanAction.ApplyVirtual:
      name = 'Apply Virtual'
      break
    case EnumPlanAction.ApplyBackfill:
      name = 'Apply Backfill'
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

type PlanOverviewDetails = Pick<
  ModelPlanOverviewTracker,
  | 'start'
  | 'end'
  | 'hasChanges'
  | 'hasBackfills'
  | 'changes'
  | 'backfills'
  | 'validation'
  | 'plan_options'
  | 'isVirtualUpdate'
  | 'isRunning'
>

export function getPlanOverviewDetails(
  planApply: ModelPlanApplyTracker,
  planOverview: ModelPlanOverviewTracker,
): PlanOverviewDetails {
  const isLatest =
    planApply.isFinished &&
    isNil(planOverview.applyType) &&
    isNotNil(planApply.overview)
  const start = isLatest ? planApply.start : planOverview.start
  const end = isLatest ? planApply.end : planOverview.end
  const hasChanges = isLatest
    ? planApply.overview.hasChanges
    : planOverview.hasChanges
  const hasBackfills = isLatest
    ? planApply.overview.hasBackfills
    : planOverview.hasBackfills
  const changes = isLatest ? planApply.changes : planOverview.changes
  const backfills = isLatest ? planApply.backfills : planOverview.backfills
  const validation = isLatest ? planApply.validation : planOverview.validation
  const plan_options = isLatest
    ? planApply.plan_options
    : planOverview.plan_options
  const isVirtualUpdate = isLatest
    ? planApply.overview.isVirtualUpdate
    : planOverview.isVirtualUpdate
  const isRunning = isLatest
    ? planApply.overview.isRunning
    : planOverview.isRunning

  return {
    start,
    end,
    hasChanges,
    hasBackfills,
    changes,
    backfills,
    validation,
    plan_options,
    isVirtualUpdate,
    isRunning,
  }
}
