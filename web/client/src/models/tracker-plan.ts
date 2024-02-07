import {
  type TrackableMeta,
  type PlanOptions,
  type PlanOverviewStageTrackerStart,
  type PlanOverviewStageTrackerEnd,
  Status,
} from '@api/client'
import { ModelInitial } from './initial'
import { isFalse, isFalseOrNil, isNil, isTrue } from '@utils/index'

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
  _isFetching: boolean = false

  constructor(model?: ModelPlanTracker<TData>) {
    super(model?.initial)

    this._current = model?._current
    this._isFetching = model?._isFetching ?? false
  }

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
      this.current?.meta?.status !== Status.init &&
      isFalse(this.isFetching)
    )
  }

  get isRunning(): boolean {
    return (
      (isFalseOrNil(this.current?.meta?.done) &&
        this.current?.meta?.status === Status.init) ||
      this.isFetching
    )
  }

  get isSuccessful(): boolean {
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

  get isFetching(): boolean {
    return this._isFetching
  }

  set isFetching(value: boolean) {
    this._isFetching = value
  }

  reset(): void {
    this._current = undefined
  }
}
