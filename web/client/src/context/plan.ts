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
}

export const useStorePlan = create<PlanStore>(set => ({
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
}))
