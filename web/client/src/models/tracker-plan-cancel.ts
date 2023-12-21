import { type PlanStageCancel, Status } from '@api/client'
import { ModelPlanTracker, type PlanTracker } from './tracker-plan'
import { isFalse, isFalseOrNil } from '@utils/index'

export interface PlanCancelTracker extends PlanTracker {
  cancel?: PlanStageCancel
}

export class ModelPlanCancelTracker extends ModelPlanTracker<PlanCancelTracker> {
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
    this.isFetching = isFalse(tracker.meta?.done)
  }
}
