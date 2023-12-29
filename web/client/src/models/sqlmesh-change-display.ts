import {
  type BackfillDetails,
  type ChangeIndirect,
  type ChangeDisplay,
  type NodeType,
  type ChangeDirect,
  type ChangeDirectChangeCategory,
  type BackfillTask,
  type BackfillTaskEnd,
  type BackfillTaskInterval,
} from '@api/client'
import { ModelInitial } from './initial'

export type InitialChangeDisplay = ChangeDisplay &
  ChangeDirect &
  ChangeIndirect &
  BackfillDetails &
  BackfillTask

export class ModelSQLMeshChangeDisplay extends ModelInitial<InitialChangeDisplay> {
  name: string
  view_name: string
  node_type?: NodeType

  change_category?: ChangeDirectChangeCategory
  diff?: string
  indirect?: ModelSQLMeshChangeDisplay[]

  batches?: number

  direct?: ModelSQLMeshChangeDisplay[]

  completed: number
  end?: BackfillTaskEnd
  interval: BackfillTaskInterval
  start: number
  total: number

  constructor(change?: InitialChangeDisplay) {
    super(change)

    this.name = encodeURI(this.initial.name)
    this.view_name = encodeURI(this.initial.view_name)
    this.node_type = this.initial.node_type
    this.change_category = this.initial.change_category
    this.diff = this.initial.diff
    this.batches = this.initial.batches
    this.interval = this.initial.interval
    this.start = this.initial.start
    this.total = this.initial.total
    this.completed = this.initial.completed
    this.end = this.initial.end

    this.indirect = this.initial.indirect?.map(
      c => new ModelSQLMeshChangeDisplay(c as InitialChangeDisplay),
    )
    this.direct = this.initial.direct?.map(
      c => new ModelSQLMeshChangeDisplay(c as InitialChangeDisplay),
    )
  }

  get nodeType(): Optional<NodeType> {
    return this.node_type
  }

  get viewName(): string {
    return this.view_name
  }

  get displayName(): string {
    return decodeURI(this.name)
  }

  get displayViewName(): string {
    return decodeURI(this.view_name)
  }

  get changeCategory(): Optional<ChangeDirectChangeCategory> {
    return this.change_category
  }
}
