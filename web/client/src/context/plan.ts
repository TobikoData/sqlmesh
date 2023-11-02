import { ModelPlanApplyTracker } from '@models/tracker-plan-apply'
import { ModelPlanCancelTracker } from '@models/tracker-plan-cancel'
import { ModelPlanOverviewTracker } from '@models/tracker-plan-overview'
import { create } from 'zustand'

export const EnumPlanAction = {
  Done: 'done',
  Run: 'run',
  Running: 'running',
  ApplyVirtual: 'apply-virtual',
  ApplyBackfill: 'apply-backfill',
  Applying: 'applying',
  Cancelling: 'cancelling',
} as const

export const EnumPlanApplyType = {
  Virtual: 'virtual',
  Backfill: 'backfill',
} as const

export type PlanApplyType = KeyOf<typeof EnumPlanApplyType>
export type PlanAction = KeyOf<typeof EnumPlanAction>

export interface PlanTaskStatus {
  total: number
  completed: number
  view_name: string
  start?: number
  end?: number
  interval?: [string, string]
}
export type PlanTasks = Record<string, PlanTaskStatus>

export interface PlanProgress {
  ok: boolean
  tasks: PlanTasks
  updated_at: string
  start?: number
  end?: number
  total?: number
  completed?: number
  is_completed?: boolean
  type?: PlanApplyType
}

interface PlanStore {
  planOverview: ModelPlanOverviewTracker
  planApply: ModelPlanApplyTracker
  planCancel: ModelPlanCancelTracker
  setPlanOverview: (planOverview?: ModelPlanOverviewTracker) => void
  setPlanApply: (planApply?: ModelPlanApplyTracker) => void
  setPlanCancel: (planCancel?: ModelPlanCancelTracker) => void
}

export const useStorePlan = create<PlanStore>((set, get) => ({
  planOverview: new ModelPlanOverviewTracker(),
  planApply: new ModelPlanApplyTracker(),
  planCancel: new ModelPlanCancelTracker(),
  setPlanApply: (planApply?: ModelPlanApplyTracker) => {
    set(() => ({ planApply: new ModelPlanApplyTracker(planApply) }))
  },
  setPlanOverview: (planOverview?: ModelPlanOverviewTracker) => {
    set(() => ({ planOverview: new ModelPlanOverviewTracker(planOverview) }))
  },
  setPlanCancel: (planCancel?: ModelPlanCancelTracker) => {
    set(() => ({ planCancel: new ModelPlanCancelTracker(planCancel) }))
  },
}))
