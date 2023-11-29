import {
  type PlanStageValidation,
  type PlanStageBackfill,
  type PlanStageCreation,
  type PlanStagePromote,
  type PlanStageRestate,
  type PlanStageChanges,
  type PlanStageBackfills,
  type TrackableMeta,
  type PlanOverviewStageTrackerEnd,
  type PlanOptions,
  type PlanOverviewStageTrackerStart,
} from '@api/client'
import { ModelPlanTracker, type PlanTracker } from './tracker-plan'
import { isFalseOrNil, isNil, isNotNil, isTrue } from '@utils/index'
import { ModelPlanOverviewTracker } from './tracker-plan-overview'

export interface PlanApplyTracker extends PlanTracker {
  creation?: PlanStageCreation
  restate?: PlanStageRestate
  backfill?: PlanStageBackfill
  promote?: PlanStagePromote
}

export interface InitialModelPlanApplyTracker {
  update: (
    tracker: PlanApplyTracker,
    planOverview: ModelPlanOverviewTracker,
  ) => void
}

export class ModelPlanApplyTracker
  extends ModelPlanTracker<PlanApplyTracker>
  implements InitialModelPlanApplyTracker
{
  _last: Optional<PlanApplyTracker>
  _planOverview: Optional<ModelPlanOverviewTracker>
  _lastPlanOverview: Optional<ModelPlanOverviewTracker>

  constructor(model?: ModelPlanApplyTracker) {
    super(model?.initial)

    if (model instanceof ModelPlanApplyTracker) {
      this._current = structuredClone(model.current)
      this._last = structuredClone(model._last)
      this._planOverview = new ModelPlanOverviewTracker(model._planOverview)
      this._lastPlanOverview = new ModelPlanOverviewTracker(
        model._lastPlanOverview,
      )
    }
  }

  get current(): Optional<PlanApplyTracker> {
    return this._current ?? this._last
  }

  get environment(): Optional<string> {
    return this._last?.environment ?? this._current?.environment
  }

  get meta(): Optional<TrackableMeta> {
    return this._last?.meta ?? this._current?.meta
  }

  get plan_options(): Optional<PlanOptions> {
    return this._last?.plan_options ?? this._current?.plan_options
  }

  get start(): Optional<PlanOverviewStageTrackerStart> {
    return this._last?.start ?? this._current?.start
  }

  get end(): Optional<PlanOverviewStageTrackerEnd> {
    return this._last?.end ?? this._current?.end
  }

  get overview(): Optional<ModelPlanOverviewTracker> {
    return this._lastPlanOverview ?? this._planOverview
  }

  get validation(): Optional<PlanStageValidation> {
    return this.overview?.validation
  }

  get changes(): Optional<PlanStageChanges> {
    return this.overview?.changes
  }

  get backfills(): Optional<PlanStageBackfills> {
    return this.overview?.backfills
  }

  get creation(): Optional<PlanStageCreation> {
    return this._last?.creation ?? this._current?.creation
  }

  get restate(): Optional<PlanStageRestate> {
    return this._last?.restate ?? this._current?.restate
  }

  get backfill(): Optional<PlanStageBackfill> {
    return this._last?.backfill ?? this._current?.backfill
  }

  get promote(): Optional<PlanStagePromote> {
    return this._last?.promote ?? this._current?.promote
  }

  get shouldShowEvaluation(): boolean {
    return (
      this.isFailed ||
      isNotNil(this.creation) ||
      isNotNil(this.backfill) ||
      isNotNil(this.promote)
    )
  }

  get evaluationStart(): Optional<PlanOverviewStageTrackerStart> {
    return (
      this.creation?.meta?.start ??
      this.backfill?.meta?.start ??
      this.promote?.meta?.start
    )
  }

  get evaluationEnd(): Optional<PlanOverviewStageTrackerEnd> {
    return (
      this.promote?.meta?.end ??
      this.backfill?.meta?.end ??
      this.creation?.meta?.end
    )
  }

  update(
    tracker: PlanApplyTracker,
    planOverview: ModelPlanOverviewTracker,
  ): void {
    if (
      isTrue(planOverview.meta?.done) &&
      (isNil(this.overview) || isNil(this.overview.current))
    ) {
      this._planOverview = new ModelPlanOverviewTracker(planOverview)
    }

    if (isNil(this._current)) {
      this._current = tracker
      this._last = undefined
      this._lastPlanOverview = undefined
    }

    this._current.start = tracker.start
    this._current.end = tracker.end
    this._current.meta = tracker.meta
    this._current.environment = tracker.environment
    this._current.plan_options = tracker.plan_options

    const { creation, restate, backfill, promote } = tracker

    if (
      isNotNil(creation) &&
      (isNil(this._current.creation) ||
        isFalseOrNil(this._current.creation.meta?.done))
    ) {
      this._current.creation = creation
    }

    if (
      isNotNil(restate) &&
      (isNil(this._current.restate) ||
        isFalseOrNil(this._current.restate.meta?.done))
    ) {
      this._current.restate = restate
    }

    if (
      isNotNil(backfill) &&
      (isNil(this._current.backfill) ||
        isFalseOrNil(this._current.backfill.meta?.done))
    ) {
      this._current.backfill = backfill
    }

    if (
      isNotNil(promote) &&
      (isNil(this._current.promote) ||
        isFalseOrNil(this._current.promote.meta?.done))
    ) {
      this._current.promote = promote
    }

    if (isTrue(this.current?.meta?.done)) {
      this._last = this._current
      this._lastPlanOverview = this._planOverview
      this._current = undefined
      this._planOverview = undefined
    }
  }

  reset(): void {
    this._current = undefined
    this._last = undefined
    this._planOverview = undefined
    this._lastPlanOverview = undefined
  }
}
