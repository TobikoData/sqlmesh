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
  type SnapshotId,
} from '@api/client'
import { ModelPlanTracker, type PlanTracker } from './tracker-plan'
import { isArrayNotEmpty, isFalse, isNil, isNotNil } from '@utils/index'

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
  get validation(): Optional<PlanStageValidation> {
    return this._current?.validation
  }

  get hasChanges(): Optional<boolean> {
    if (isNil(this._current?.changes)) return undefined

    const { added, removed, modified } = this._current.changes ?? {}

    if ([added, removed, modified].every(isNil)) return undefined

    const { direct, indirect, metadata } = modified ?? {}

    return [added, removed, direct, indirect, metadata].some(isArrayNotEmpty)
  }

  get changes(): Optional<PlanStageChanges> {
    return this._current?.changes
  }

  get hasBackfills(): Optional<boolean> {
    return isNil(this._current?.backfills?.models)
      ? undefined
      : isArrayNotEmpty(this._current.backfills?.models)
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

  get metadata(): Optional<SnapshotId[]> {
    return this._current?.changes?.modified?.metadata
  }

  get isVirtualUpdate(): boolean {
    return (
      (isNotNil(this.hasChanges) && isNil(this.hasBackfills)) ||
      (Boolean(this.hasChanges) && this.skipBackfill)
    )
  }

  get isMetadataUpdate(): boolean {
    return Boolean(this.metadata?.length) && isNil(this.hasBackfills)
  }

  get isBackfillUpdate(): boolean {
    return (
      isNil(this.hasChanges) &&
      Boolean(this.hasBackfills) &&
      isFalse(this.skipBackfill)
    )
  }

  get isChangesAndBackfillUpdate(): boolean {
    return (
      Boolean(this.hasChanges) &&
      Boolean(this.hasBackfills) &&
      isFalse(this.skipBackfill)
    )
  }

  get isLatest(): boolean {
    return (
      this.isFinished &&
      isNil(this.hasChanges) &&
      (isNil(this.hasBackfills) || this.skipBackfill)
    )
  }

  get skipTests(): boolean {
    return this._current?.plan_options?.skip_tests ?? false
  }

  get skipBackfill(): boolean {
    return this._current?.plan_options?.skip_backfill ?? false
  }

  update(tracker: PlanOverviewTracker): void {
    this._current = tracker
    this.isFetching = isFalse(tracker.meta?.done)
  }

  reset(): void {
    this._current = undefined
  }
}
