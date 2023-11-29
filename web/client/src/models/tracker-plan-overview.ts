import {
  type PlanStageValidation,
  type PlanStageBackfills,
  type PlanStageChanges,
  type BackfillDetails,
  type ChangeIndirect,
  type ChangeDirect,
  type ModelsDiff,
} from '@api/client'
import { ModelPlanTracker, type PlanTracker } from './tracker-plan'
import { isArrayNotEmpty, isNil, isNotNil, isTrue } from '@utils/index'
import { EnumPlanApplyType, type PlanApplyType } from '@context/plan'

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
    super(model?.initial)

    if (model instanceof ModelPlanOverviewTracker) {
      this._current = structuredClone(model.current)
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

  get models(): Optional<BackfillDetails[]> {
    return this._current?.backfills?.models
  }

  get added(): Optional<string[]> {
    return this._current?.changes?.added
  }

  get removed(): Optional<string[]> {
    return this._current?.changes?.removed
  }

  get modified(): Optional<ModelsDiff> {
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

  get applyType(): Optional<PlanApplyType> {
    if (isTrue(this.hasBackfills)) return EnumPlanApplyType.Backfill
    if (isNotNil(this.hasChanges)) return EnumPlanApplyType.Virtual

    return undefined
  }

  get isLatest(): boolean {
    return this.isFinished && isNil(this.hasChanges) && isNil(this.hasBackfills)
  }

  get isVirtualUpdate(): boolean {
    return this.isFinished && this.applyType === EnumPlanApplyType.Virtual
  }

  get isBackfillUpdate(): boolean {
    return this.isFinished && this.applyType === EnumPlanApplyType.Backfill
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
