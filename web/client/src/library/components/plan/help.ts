import { type ModelPlanOverviewTracker } from '@models/tracker-plan-overview'
import { isArrayNotEmpty, isNotNil } from '../../../utils'
import { type ModelPlanApplyTracker } from '@models/tracker-plan-apply'
import { type ModelPlanCancelTracker } from '@models/tracker-plan-cancel'

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
  | 'added'
  | 'removed'
  | 'direct'
  | 'indirect'
  | 'metadata'
  | 'backfills'
  | 'plan_options'
  | 'stageValidation'
  | 'stageChanges'
  | 'stageBackfills'
>

export function getPlanOverviewDetails(
  planApply: ModelPlanApplyTracker,
  planOverview: ModelPlanOverviewTracker,
  planCancel: ModelPlanCancelTracker,
): PlanOverviewDetails {
  const isLatest =
    ((planApply.isFinished &&
      (planOverview.isLatest || planOverview.isRunning)) ||
      planCancel.isFinished) &&
    isNotNil(planApply.overview)
  const overview = isLatest ? planApply.overview : planOverview
  const plan = isLatest ? planApply : planOverview

  return {
    meta: plan.meta,
    start: plan.start,
    end: plan.end,
    hasChanges: overview.hasChanges,
    hasBackfills: overview.hasBackfills,
    backfills: plan.backfills ?? [],
    added: plan.added ?? [],
    removed: plan.removed ?? [],
    direct: plan.direct ?? [],
    indirect: plan.indirect ?? [],
    metadata: plan.metadata ?? [],
    plan_options: plan.plan_options,
    stageValidation: plan.stageValidation,
    stageBackfills: plan.stageBackfills,
    stageChanges: plan.stageChanges,
  }
}
