import { create } from 'zustand'
import { isObject, isObjectEmpty } from '../utils';

export const EnumPlanState = {
  Init: 'init',
  None: 'none',
  Done: 'done',
  Run: 'run',
  Running: 'running',
  Apply: 'apply',
  Applying: 'applying',
  Finished: 'finished',
  Failed: 'failed',
  Resetting: 'resetting',
  Canceling: 'canceling',
  Closing: 'closing',
  Openning: 'openning',
} as const;

export type PlanState = typeof EnumPlanState[keyof typeof EnumPlanState]

export const useStorePlan = create((set, get) => ({
  state: EnumPlanState.Init,
  action: EnumPlanState.None,
  activePlan: null,
  lastPlan: null,
  setActivePlan: (activePlan: any) => set(() => ({ activePlan })),
  setLastPlan: (lastPlan: any) => set(() => ({ lastPlan })),
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
  updateTasks: (data: any, channel: EventSource, unsubscribe: () => void) => {
    const s: any = get()

    if (channel == null) return

    if (data.environment == null || isObject(data.tasks) === false) {
      s.setState(null)
  
      channel?.close()
      unsubscribe()
  
      return
    }

    const plan = {
      environment: data.environment,
      tasks: data.tasks,
      intervals: data.intervals,
      updated_at: data.updated_at || Date.now(),
    }

    s.setActivePlan(plan)
  
    if (data.ok === false) {
      s.setState(EnumPlanState.Failed)

      s.setLastPlan(plan)
  
      channel?.close()
      unsubscribe()
  
      return
    }
  
    const isAllCompleted = isObjectEmpty(data.tasks) || isAllTasksCompleted(data.tasks)
  
    if (isAllCompleted) {
      s.setState(EnumPlanState.Finished)

      if (isAllTasksCompleted(data.tasks)) {
        s.setLastPlan(plan)
      }

      if (isObjectEmpty(data.tasks)) {
        s.setActivePlan(null)
      }
  
      channel?.close()
      unsubscribe()
    } else {
      s.setState(EnumPlanState.Applying)
    }
  }
}))

function isAllTasksCompleted(tasks: any = {}): boolean {
  return  Object.values(tasks).every((t: any) => t.completed === t.total)
}