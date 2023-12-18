import { includes, isFalse, isNil, isNotNil } from '@utils/index'
import { ModelInitial } from './initial'
import { type ModelPlanOverviewTracker } from './tracker-plan-overview'
import { type ModelPlanApplyTracker } from './tracker-plan-apply'
import { type ModelPlanCancelTracker } from './tracker-plan-cancel'

export const EnumPlanAction = {
  Done: 'done',
  Run: 'run',
  Running: 'running',
  RunningTask: 'running-task',
  ApplyVirtual: 'apply-virtual',
  ApplyBackfill: 'apply-backfill',
  ApplyChanges: 'apply-changes',
  ApplyChangesAndBackfill: 'apply-changes-and-backfill',
  ApplyMetadata: 'apply-metadata',
  Applying: 'applying',
  Cancelling: 'cancelling',
} as const

export type PlanAction = KeyOf<typeof EnumPlanAction>

export interface InitialPlanAction {
  value: PlanAction
}

export class ModelPlanAction<
  T extends InitialPlanAction = InitialPlanAction,
> extends ModelInitial<T> {
  private readonly _value: PlanAction

  constructor(initial?: T | ModelPlanAction<T>) {
    super(
      (initial as ModelPlanAction<T>)?.isModel
        ? (initial as ModelPlanAction<T>).initial
        : {
            ...(initial as T),
            value: initial?.value ?? EnumPlanAction.Run,
          },
    )

    this._value = initial?.value ?? this.initial.value
  }

  get value(): PlanAction {
    return this._value
  }

  get isRun(): boolean {
    return this.value === EnumPlanAction.Run
  }

  get isDone(): boolean {
    return this.value === EnumPlanAction.Done
  }

  get isApplyVirtual(): boolean {
    return this.value === EnumPlanAction.ApplyVirtual
  }

  get isApplyBackfill(): boolean {
    return this.value === EnumPlanAction.ApplyBackfill
  }

  get isApplyChanges(): boolean {
    return this.value === EnumPlanAction.ApplyChanges
  }

  get isApplyChangesAndBackfill(): boolean {
    return this.value === EnumPlanAction.ApplyChangesAndBackfill
  }

  get isApplyMetadata(): boolean {
    return this.value === EnumPlanAction.ApplyMetadata
  }

  get isApply(): boolean {
    return (
      this.isApplyVirtual ||
      this.isApplyBackfill ||
      this.isApplyChanges ||
      this.isApplyChangesAndBackfill ||
      this.isApplyMetadata
    )
  }

  get isCancelling(): boolean {
    return this.value === EnumPlanAction.Cancelling
  }

  get isApplying(): boolean {
    return this.value === EnumPlanAction.Applying
  }

  get isRunning(): boolean {
    return includes(
      [EnumPlanAction.Running, EnumPlanAction.RunningTask],
      this.value,
    )
  }

  get isRunningTask(): boolean {
    return this.value === EnumPlanAction.RunningTask
  }

  get isProcessing(): boolean {
    return this.isRunning || this.isApplying || this.isCancelling
  }

  get isIdle(): boolean {
    return isFalse(this.isProcessing)
  }

  displayStatus(planOverview: ModelPlanOverviewTracker): string {
    if (this.isRunningTask) return 'Running Task...'
    if (this.isApplying) return 'Applying Plan...'
    if (this.isRunning && isNil(planOverview.hasChanges))
      return 'Getting Changes...'
    if (this.isRunning && isNil(planOverview.hasBackfills))
      return 'Getting Backfills...'
    if (this.isRunning) return 'Checking Plan...'

    return 'Plan'
  }

  static getActionDisplayName(
    action: ModelPlanAction,
    options: PlanAction[] = [],
    fallback: string = 'Run',
  ): string {
    if (!options.includes(action.value)) return fallback

    let name: string

    switch (action.value) {
      case EnumPlanAction.Done:
        name = 'Done'
        break
      case EnumPlanAction.Running:
        name = 'Running...'
        break
      case EnumPlanAction.RunningTask:
        name = 'Running Task...'
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
      case EnumPlanAction.ApplyChangesAndBackfill:
        name = 'Apply Changes And Backfill'
        break
      case EnumPlanAction.ApplyChanges:
        name = 'Apply Changes And Skip Backfill'
        break
      case EnumPlanAction.ApplyVirtual:
        name = 'Apply Virtual Update'
        break
      case EnumPlanAction.ApplyBackfill:
        name = 'Apply And Backfill'
        break
      case EnumPlanAction.ApplyMetadata:
        name = 'Apply Metadata'
        break
      default:
        name = fallback
        break
    }

    return name
  }

  static getPlanAction({
    planOverview,
    planApply,
    planCancel,
  }: {
    planOverview: ModelPlanOverviewTracker
    planApply: ModelPlanApplyTracker
    planCancel: ModelPlanCancelTracker
  }): Optional<PlanAction> {
    const {
      isLatest,
      hasBackfills,
      hasChanges,
      isRunning,
      metadata,
      skipBackfill,
    } = planOverview
    const isRunningApply = planApply.isRunning
    const isRunningCancel = planCancel.isRunning
    const isFinished =
      planApply.isFinished ||
      isLatest ||
      (isNil(hasChanges) && Boolean(hasBackfills) && skipBackfill)
    const isMetadataUpdate = Boolean(metadata?.length) && isNil(hasBackfills)
    const isChangesUpdate =
      Boolean(hasChanges) && Boolean(hasBackfills) && skipBackfill
    const isVirtualUpdate = isNotNil(hasChanges) && isNil(hasBackfills)
    const isBackfillUpdate =
      isNil(hasChanges) && Boolean(hasBackfills) && isFalse(skipBackfill)
    const isChangesAndBackfillUpdate =
      Boolean(hasChanges) && Boolean(hasBackfills) && isFalse(skipBackfill)

    if (isRunning) return EnumPlanAction.Running
    if (isRunningCancel) return EnumPlanAction.Cancelling
    if (isRunningApply) return EnumPlanAction.Applying
    if (isFinished) return EnumPlanAction.Done
    if (isMetadataUpdate) return EnumPlanAction.ApplyMetadata
    if (isVirtualUpdate) return EnumPlanAction.ApplyVirtual
    if (isChangesUpdate) return EnumPlanAction.ApplyChanges
    if (isBackfillUpdate) return EnumPlanAction.ApplyBackfill
    if (isChangesAndBackfillUpdate)
      return EnumPlanAction.ApplyChangesAndBackfill
  }
}
