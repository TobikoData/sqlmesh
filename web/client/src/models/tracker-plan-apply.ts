import {
  type PlanStageValidation,
  type PlanStageBackfill,
  type PlanStageCreation,
  type PlanStagePromote,
  type PlanStageRestate,
  type TrackableMeta,
  type PlanOverviewStageTrackerEnd,
  type PlanOptions,
  type PlanOverviewStageTrackerStart,
  type PlanStageChanges,
  type PlanStageBackfills,
} from '@api/client'
import { ModelPlanTracker, type PlanTracker } from './tracker-plan'
import { isFalse, isFalseOrNil, isNil, isNotNil, isTrue } from '@utils/index'
import { type ModelPlanOverviewTracker } from './tracker-plan-overview'
import {
  type InitialChangeDisplay,
  ModelSQLMeshChangeDisplay,
} from './sqlmesh-change-display'

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
    super(model)

    if (isNotNil(model) && model.isModel) {
      this._current = model._current
      this._last = model._last
      this._planOverview = model._planOverview
      this._lastPlanOverview = model._lastPlanOverview
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

  get backfills(): ModelSQLMeshChangeDisplay[] {
    return this.overview?.backfills ?? []
  }

  get added(): ModelSQLMeshChangeDisplay[] {
    return this.overview?.added ?? []
  }

  get removed(): ModelSQLMeshChangeDisplay[] {
    return this.overview?.removed ?? []
  }

  get direct(): ModelSQLMeshChangeDisplay[] {
    return this.overview?.direct ?? []
  }

  get indirect(): ModelSQLMeshChangeDisplay[] {
    return this.overview?.indirect ?? []
  }

  get metadata(): ModelSQLMeshChangeDisplay[] {
    return this.overview?.metadata ?? []
  }

  get tasks(): Record<string, ModelSQLMeshChangeDisplay> {
    const tasks =
      this._last?.backfill?.tasks ?? this._current?.backfill?.tasks ?? {}

    return Object.entries(tasks ?? {}).reduce(
      (acc: Record<string, ModelSQLMeshChangeDisplay>, [key, value]) => {
        acc[encodeURI(key)] = new ModelSQLMeshChangeDisplay(
          value as InitialChangeDisplay,
        )

        return acc
      },
      {},
    )
  }

  get queue(): ModelSQLMeshChangeDisplay[] {
    return (
      (this.stageBackfill?.queue
        ?.map(key => this.tasks[encodeURI(key)])
        .filter(Boolean) as ModelSQLMeshChangeDisplay[]) ?? []
    )
  }

  get stageValidation(): Optional<PlanStageValidation> {
    return this.overview?.stageValidation
  }

  get stageChanges(): Optional<PlanStageChanges> {
    return this.overview?.stageChanges
  }

  get stageBackfills(): Optional<PlanStageBackfills> {
    return this.overview?.stageBackfills
  }

  get stageCreation(): Optional<PlanStageCreation> {
    return this._last?.creation ?? this._current?.creation
  }

  get stageRestate(): Optional<PlanStageRestate> {
    return this._last?.restate ?? this._current?.restate
  }

  get stageBackfill(): Optional<PlanStageBackfill> {
    return this._last?.backfill ?? this._current?.backfill
  }

  get stagePromote(): Optional<PlanStagePromote> {
    return this._last?.promote ?? this._current?.promote
  }

  get shouldShowEvaluation(): boolean {
    return (
      this.isFailed ||
      isNotNil(this.stageCreation) ||
      isNotNil(this.stageBackfill) ||
      isNotNil(this.stagePromote)
    )
  }

  get evaluationStart(): Optional<PlanOverviewStageTrackerStart> {
    return (
      this.stageCreation?.meta?.start ??
      this.stageBackfill?.meta?.start ??
      this.stagePromote?.meta?.start
    )
  }

  get evaluationEnd(): Optional<PlanOverviewStageTrackerEnd> {
    return (
      this.stagePromote?.meta?.end ??
      this.stageBackfill?.meta?.end ??
      this.stageCreation?.meta?.end
    )
  }

  update(
    tracker: PlanApplyTracker,
    planOverview?: ModelPlanOverviewTracker,
  ): void {
    const { creation, restate, backfill, promote, meta, environment } = tracker
    const isEmpty = isNil(meta) || isNil(environment)
    const newCurrent = isEmpty ? undefined : tracker

    if (isNil(newCurrent)) {
      this._current = undefined

      return
    }

    if (isNil(this._current)) {
      this._current = tracker
    }

    this._current.start = tracker.start
    this._current.end = tracker.end
    this._current.meta = tracker.meta
    this._current.environment = tracker.environment
    this._current.plan_options = tracker.plan_options

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

    if (isTrue(this.current?.meta?.done) && isNil(this._last)) {
      this._last = this._current
      this._lastPlanOverview = this._planOverview
      this._current = undefined
      this._planOverview = undefined
    } else {
      this._planOverview =
        isNotNil(meta) && isNotNil(environment)
          ? planOverview?.clone()
          : undefined
    }

    this.isFetching = isFalse(tracker.meta?.done)
  }

  reset(): void {
    this._current = undefined
    this._planOverview = undefined
  }

  clear(): void {
    this._current = undefined
    this._last = undefined
    this._planOverview = undefined
    this._lastPlanOverview = undefined
  }

  clone(): ModelPlanApplyTracker {
    const tracker = new ModelPlanApplyTracker()

    if (isNotNil(this.current)) {
      tracker.update(structuredClone(this.current), this.overview?.clone())
    }

    return tracker
  }
}
