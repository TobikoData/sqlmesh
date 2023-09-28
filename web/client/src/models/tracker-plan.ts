import {
  type TrackableMeta,
  type PlanOptions,
  type PlanOverviewStageTrackerStart,
  type PlanOverviewStageTrackerEnd,
  Status,
} from '@api/client'
import { ModelInitial } from './initial'
import { isFalseOrNil, isNil, isTrue } from '@utils/index'

export interface PlanTrackerMeta extends TrackableMeta {
  duration?: number
}

export interface PlanTracker {
  environment: string
  meta: PlanTrackerMeta
  plan_options: PlanOptions
  start?: PlanOverviewStageTrackerStart
  end?: PlanOverviewStageTrackerEnd
}

export class ModelPlanTracker<
  TData extends PlanTracker = PlanTracker,
> extends ModelInitial<TData> {
  _current: Optional<TData>

  get current(): Optional<TData> {
    return this._current
  }

  get environment(): Optional<string> {
    return this._current?.environment
  }

  get meta(): Optional<PlanTrackerMeta> {
    return this._current?.meta
  }

  get plan_options(): Optional<PlanOptions> {
    return this._current?.plan_options
  }

  get start(): Optional<PlanOverviewStageTrackerStart> {
    return this._current?.start
  }

  get end(): Optional<PlanOverviewStageTrackerEnd> {
    return this._current?.end
  }

  get duration(): number {
    return this?.meta?.duration ?? -1
  }

  get isFinished(): boolean {
    return (
      isTrue(this._current?.meta?.done) &&
      this._current?.meta.status !== Status.init
    )
  }

  get isRunning(): boolean {
    return (
      isFalseOrNil(this._current?.meta?.done) &&
      this._current?.meta.status === Status.init
    )
  }

  get isSuccessed(): boolean {
    return this.isFinished && this._current?.meta.status === Status.success
  }

  get isFailed(): boolean {
    return this.isFinished && this._current?.meta.status === Status.success
  }

  get isEmpty(): boolean {
    return isNil(this._current)
  }

  reset(): void {
    this._current = undefined
  }
}
