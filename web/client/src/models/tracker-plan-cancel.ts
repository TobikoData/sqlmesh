import { type PlanStageCancel, Status } from '@api/client'
import { ModelPlanTracker, type PlanTracker } from './tracker-plan'
import { isFalseOrNil } from '@utils/index'

export interface PlanCancelTracker extends PlanTracker {
  cancel?: PlanStageCancel
}

export class ModelPlanCancelTracker extends ModelPlanTracker<PlanCancelTracker> {
  constructor(model?: ModelPlanCancelTracker) {
    super(model?.initial)

    if (model instanceof ModelPlanCancelTracker) {
      this._current = structuredClone(model.current)
    }
  }

  get cancel(): Optional<PlanStageCancel> {
    return this._current?.cancel
  }

  get isCancelling(): boolean {
    return (
      isFalseOrNil(this._current?.cancel?.meta?.done) &&
      this._current?.cancel?.meta?.status === Status.init
    )
  }

  update(tracker: PlanCancelTracker): void {
    this._current = tracker
  }
}
