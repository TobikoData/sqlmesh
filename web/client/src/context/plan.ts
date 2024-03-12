import { ModelPlanAction } from '@models/plan-action'
import { ModelPlanApplyTracker } from '@models/tracker-plan-apply'
import { ModelPlanCancelTracker } from '@models/tracker-plan-cancel'
import { ModelPlanOverviewTracker } from '@models/tracker-plan-overview'
import { create } from 'zustand'

interface PlanStore {
  planAction: ModelPlanAction
  planOverview: ModelPlanOverviewTracker
  planApply: ModelPlanApplyTracker
  planCancel: ModelPlanCancelTracker
  setPlanOverview: (planOverview?: ModelPlanOverviewTracker) => void
  setPlanApply: (planApply?: ModelPlanApplyTracker) => void
  setPlanCancel: (planCancel?: ModelPlanCancelTracker) => void
  setPlanAction: (planAction?: ModelPlanAction) => void
  resetPlanTrackers: () => void
  resetPlanCancel: () => void
  clearPlanApply: () => void
}

export const useStorePlan = create<PlanStore>((set, get) => ({
  planAction: new ModelPlanAction(),
  planOverview: new ModelPlanOverviewTracker(),
  planApply: new ModelPlanApplyTracker(),
  planCancel: new ModelPlanCancelTracker(),
  setPlanAction: (planAction?: ModelPlanAction) => {
    set(() => ({ planAction: new ModelPlanAction(planAction) }))
  },
  setPlanApply: (planApply?: ModelPlanApplyTracker) => {
    set(() => ({ planApply: new ModelPlanApplyTracker(planApply) }))
  },
  setPlanOverview: (planOverview?: ModelPlanOverviewTracker) => {
    set(() => ({ planOverview: new ModelPlanOverviewTracker(planOverview) }))
  },
  setPlanCancel: (planCancel?: ModelPlanCancelTracker) => {
    set(() => ({ planCancel: new ModelPlanCancelTracker(planCancel) }))
  },
  resetPlanTrackers: () => {
    const s = get()

    s.planApply.reset()
    s.planCancel.reset()
    s.planOverview.reset()

    set(() => ({
      planApply: new ModelPlanApplyTracker(s.planApply),
      planCancel: new ModelPlanCancelTracker(s.planCancel),
      planOverview: new ModelPlanOverviewTracker(s.planOverview),
    }))
  },
  resetPlanCancel: () => {
    const s = get()

    s.planCancel.reset()

    set(() => ({ planCancel: new ModelPlanCancelTracker(s.planCancel) }))
  },
  clearPlanApply: () => {
    const s = get()

    s.planApply.clear()

    set(() => ({ planApply: new ModelPlanApplyTracker(s.planApply) }))
  },
}))
