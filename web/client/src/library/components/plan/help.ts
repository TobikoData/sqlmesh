import { type ModelPlanOverviewTracker } from '@models/tracker-plan-overview'
import { isArrayNotEmpty, isNotNil } from '../../../utils'
import { type ModelPlanApplyTracker } from '@models/tracker-plan-apply'

export function isModified<T extends object>(modified?: T): boolean {
  return Object.values(modified ?? {}).some(isArrayNotEmpty)
}

type PlanOverviewDetails = Pick<
  ModelPlanOverviewTracker,
  | 'meta'
  | 'start'
  | 'end'
  | 'hasChanges'
  | 'hasBackfills'
  | 'changes'
  | 'backfills'
  | 'validation'
  | 'plan_options'
>

export function getPlanOverviewDetails(
  planApply: ModelPlanApplyTracker,
  planOverview: ModelPlanOverviewTracker,
): PlanOverviewDetails {
  const isLatest =
    planApply.isFinished &&
    (planOverview.isLatest || planOverview.isRunning) &&
    isNotNil(planApply.overview)
  const overview = isLatest ? planApply.overview : planOverview
  const plan = isLatest ? planApply : planOverview

  return {
    meta: plan.meta,
    start: plan.start,
    end: plan.end,
    hasChanges: overview.hasChanges,
    hasBackfills: overview.hasBackfills,
    changes: plan.changes,
    backfills: plan.backfills,
    validation: plan.validation,
    plan_options: plan.plan_options,
  }
}
