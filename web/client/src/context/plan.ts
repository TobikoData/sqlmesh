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
  activePlan: null,
  setActivePlan: (activePlan: any) => set(() => ({ activePlan })),
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