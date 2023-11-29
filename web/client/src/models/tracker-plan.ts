import {
  type TrackableMeta,
  type PlanOptions,
  type PlanOverviewStageTrackerStart,
  type PlanOverviewStageTrackerEnd,
  Status,
} from '@api/client'
import { ModelInitial } from './initial'
import { isFalseOrNil, isNil, isTrue } from '@utils/index'
import { type ModelEnvironment } from './environment'

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
    return this.current?.environment
  }

  get meta(): Optional<PlanTrackerMeta> {
    return this.current?.meta
  }

  get plan_options(): Optional<PlanOptions> {
    return this.current?.plan_options
  }

  get start(): Optional<PlanOverviewStageTrackerStart> {
    return this.current?.start
  }

  get end(): Optional<PlanOverviewStageTrackerEnd> {
    return this.current?.end
  }

  get duration(): number {
    return this?.meta?.duration ?? -1
  }

  get isFinished(): boolean {
    return (
      isTrue(this.current?.meta?.done) &&
      this.current?.meta?.status !== Status.init
    )
  }

  get isRunning(): boolean {
    return (
      isFalseOrNil(this.current?.meta?.done) &&
      this.current?.meta?.status === Status.init
    )
  }

  get isSuccessed(): boolean {
    return this.isFinished && this.current?.meta?.status === Status.success
  }

  get isFailed(): boolean {
    return (
      this.isFinished &&
      (isNil(this.current?.meta) || this.current?.meta?.status === Status.fail)
    )
  }

  get isEmpty(): boolean {
    return isNil(this.current)
  }

  reset(): void {
    this._current = undefined
  }

  static shouldDisplay(
    tracker: ModelPlanTracker,
    environment: ModelEnvironment,
  ): boolean {
    return (
      (tracker.isFinished || tracker.isRunning) &&
      tracker.environment === environment.name
    )
  }
}
