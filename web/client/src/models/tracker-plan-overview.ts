import {
  type PlanStageValidation,
  type PlanStageBackfills,
  type PlanStageChanges,
  type ChangeIndirect,
  type ChangeDirect,
  type PlanStageBackfillsModels,
  type PlanStageChangesAdded,
  type PlanStageChangesModified,
  type PlanStageChangesRemoved,
} from '@api/client'
import { ModelPlanTracker, type PlanTracker } from './tracker-plan'
import { isArrayNotEmpty, isNil } from '@utils/index'

export interface PlanOverviewTracker extends PlanTracker {
  validation?: PlanStageValidation
  changes?: PlanStageChanges
  backfills?: PlanStageBackfills
}

export interface InitialModelPlanOverviewTracker {
  update: (tracker: PlanTracker) => void
}

export class ModelPlanOverviewTracker
  extends ModelPlanTracker<PlanOverviewTracker>
  implements InitialModelPlanOverviewTracker
{
  hasChanges: Optional<boolean> = undefined
  hasBackfills: Optional<boolean> = undefined

  constructor(model?: ModelPlanOverviewTracker) {
    super(model)

    if (model instanceof ModelPlanOverviewTracker) {
      this.hasChanges = model.hasChanges
      this.hasBackfills = model.hasBackfills
    }
  }

  get validation(): Optional<PlanStageValidation> {
    return this._current?.validation
  }

  get changes(): Optional<PlanStageChanges> {
    return this._current?.changes
  }

  get backfills(): Optional<PlanStageBackfills> {
    return this._current?.backfills
  }

  get models(): Optional<PlanStageBackfillsModels> {
    return this._current?.backfills?.models
  }

  get added(): Optional<PlanStageChangesAdded> {
    return this._current?.changes?.added
  }

  get removed(): Optional<PlanStageChangesRemoved> {
    return this._current?.changes?.removed
  }

  get modified(): Optional<PlanStageChangesModified> {
    return this._current?.changes?.modified
  }

  get direct(): Optional<ChangeDirect[]> {
    return this._current?.changes?.modified?.direct
  }

  get indirect(): Optional<ChangeIndirect[]> {
    return this._current?.changes?.modified?.indirect
  }

  get metadata(): Optional<string[]> {
    return this._current?.changes?.modified?.metadata
  }

  get isLatest(): boolean {
    return this.isFinished && isNil(this.hasBackfills) && isNil(this.hasChanges)
  }

  get skipTests(): boolean {
    return this._current?.plan_options?.skip_tests ?? false
  }

  get skipBackfill(): boolean {
    return this._current?.plan_options?.skip_backfill ?? false
  }

  update(tracker: PlanOverviewTracker): void {
    this._current = tracker

    if (isNil(this._current.backfills?.models)) {
      this.hasBackfills = undefined
    } else {
      this.hasBackfills = isArrayNotEmpty(this._current.backfills?.models)
    }

    if (isNil(this._current.changes)) {
      this.hasChanges = undefined
    } else {
      const { added, removed, modified } = this._current.changes ?? {}

      if (isNil(added) && isNil(removed) && isNil(modified)) {
        this.hasChanges = undefined
      } else {
        const { direct, indirect, metadata } = modified ?? {}

        this.hasChanges =
          isArrayNotEmpty(added) ||
          isArrayNotEmpty(removed) ||
          isArrayNotEmpty(direct) ||
          isArrayNotEmpty(indirect) ||
          isArrayNotEmpty(metadata)
      }
    }
  }

  reset(): void {
    this._current = undefined
    this.hasChanges = undefined
    this.hasBackfills = undefined
  }
}
