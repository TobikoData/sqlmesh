import { create } from 'zustand'

export const EnumPlanState = {
  Init: 'init',
  Run: 'run',
  Running: 'running',
  Apply: 'apply',
  Applying: 'applying',
  Done: 'done',
  Finished: 'finished',
  Failed: 'failed',
  Resetting: 'resetting',
  Canceling: 'canceling',
  Closing: 'closing',
  Stopping: 'closing',
  None: 'none',
  Setting: 'setting',
} as const;

export type PlanState = typeof EnumPlanState[keyof typeof EnumPlanState]

export const useStorePlan = create((set) => ({
  state: EnumPlanState.Init,
  action: EnumPlanState.None,
  activePlan: getPlan(),
  setNewPlan: (newPlan: any) => set(() => ({ newPlan })),
  setPlan: (plan: any) => set(() => ({ plan })),
  setState: (state: string) => set(() => ({ state })),
  setAction: (action: string) => set(() => ({ action })),
  setBackfillStart: (backfill_start: string) => set(() => ({ backfill_start })),
  setBackfillEnd: (backfill_end: string) => set(() => ({ backfill_end })),
  setEnvironment: (environment: string) => set(() => ({ environment })),
  setCategory: (category: string) => set(() => ({ category })),
  backfill_start: null,
  backfill_end: null,
  environment: null,
  category: null,
  categories: [
    {
      id: 'breaking-change',
      name: 'Breaking Change',
      description: 'This is a breaking change',
    },
    {
      id: 'non-breaking-change',
      name: 'Non-Breaking Change',
      description: 'This is a non-breaking change',
    },
    {
      id: 'no-change',
      name: 'No Change',
      description: 'This is a no change',
    },
  ],
  withBackfill: true,
  setWithBackfill: (withBackfill: boolean) => set(() => ({ withBackfill })),
  backfills: [],
  setBackfills: (backfills: any) => set(() => ({ backfills })),
}))

function getPlan() {
  return {
    environment: "prod",
    tasks: {
      "sushi.customer_revenue_by_day": {
        "completed": 7,
        "total": 40
      },
      "sushi.customers": {
        "completed": 0,
        "total": 1
      },
      "sushi.top_waiters": {
        "completed": 1,
        "total": 1
      },
      "sushi.waiter_as_customer_by_day": {
        "completed": 1,
        "total": 1
      },
      "sushi.waiter_names": {
        "completed": 1,
        "total": 1
      },
      "sushi.waiter_revenue_by_day": {
        "completed": 7,
        "total": 40
      },
      "sushi.items": {
        "completed": 7,
        "total": 14
      },
      "sushi.order_items": {
        "completed": 7,
        "total": 14
      },
      "sushi.orders": {
        "completed": 7,
        "total": 14
      }
    }
  }
}