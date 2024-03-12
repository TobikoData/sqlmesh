import {
  type PlanStageValidation,
  type PlanStageBackfills,
  type PlanStageChanges,
} from '@api/client'
import { ModelPlanTracker, type PlanTracker } from './tracker-plan'
import {
  isArrayNotEmpty,
  isFalse,
  isFalseOrNil,
  isNil,
  isNotNil,
  isTrue,
} from '@utils/index'
import {
  type InitialChangeDisplay,
  ModelSQLMeshChangeDisplay,
} from './sqlmesh-change-display'

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
  _added: ModelSQLMeshChangeDisplay[] = []
  _removed: ModelSQLMeshChangeDisplay[] = []
  _direct: ModelSQLMeshChangeDisplay[] = []
  _indirect: ModelSQLMeshChangeDisplay[] = []
  _metadata: ModelSQLMeshChangeDisplay[] = []
  _backfills: ModelSQLMeshChangeDisplay[] = []

  constructor(model?: ModelPlanOverviewTracker) {
    super(model)

    if (isNotNil(model) && model.isModel) {
      this._added = model.added
      this._removed = model.removed
      this._direct = model.direct
      this._indirect = model.indirect
      this._metadata = model.metadata
      this._backfills = model.backfills
    }
  }

  get stageValidation(): Optional<PlanStageValidation> {
    return this._current?.validation
  }

  get stageChanges(): Optional<PlanStageChanges> {
    return this._current?.changes
  }

  get stageBackfills(): Optional<PlanStageBackfills> {
    return this._current?.backfills
  }

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

  get hasUpdates(): boolean {
    return isTrue(this.hasChanges) || isTrue(this.hasBackfills)
  }

  get backfills(): ModelSQLMeshChangeDisplay[] {
    return this._backfills
  }

  get added(): ModelSQLMeshChangeDisplay[] {
    return this._added
  }

  get removed(): ModelSQLMeshChangeDisplay[] {
    return this._removed
  }

  get direct(): ModelSQLMeshChangeDisplay[] {
    return this._direct
  }

  get indirect(): ModelSQLMeshChangeDisplay[] {
    return this._indirect
  }

  get metadata(): ModelSQLMeshChangeDisplay[] {
    return this._metadata
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
      isFalseOrNil(this.hasChanges) &&
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

    const changes = this._current?.changes ?? {}
    const modified = changes?.modified ?? {}
    const backfills = this._current?.backfills ?? {}

    this._added =
      changes.added?.map(
        c => new ModelSQLMeshChangeDisplay(c as InitialChangeDisplay),
      ) ?? []
    this._removed =
      changes.removed?.map(
        c => new ModelSQLMeshChangeDisplay(c as InitialChangeDisplay),
      ) ?? []
    this._direct =
      modified.direct?.map(
        c => new ModelSQLMeshChangeDisplay(c as InitialChangeDisplay),
      ) ?? []
    this._indirect =
      modified.indirect?.map(
        c => new ModelSQLMeshChangeDisplay(c as InitialChangeDisplay),
      ) ?? []
    this._metadata =
      modified.metadata?.map(
        c => new ModelSQLMeshChangeDisplay(c as InitialChangeDisplay),
      ) ?? []
    this._backfills =
      backfills.models?.map(
        c => new ModelSQLMeshChangeDisplay(c as InitialChangeDisplay),
      ) ?? []
  }

  clone(): ModelPlanOverviewTracker {
    const tracker = new ModelPlanOverviewTracker()

    if (isNotNil(this.current)) {
      tracker.update(structuredClone(this.current))
    }

    return tracker
  }
}
