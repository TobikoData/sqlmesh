import { create } from 'zustand'
import { ContextEnvironmentBackfill } from '../api/client';
import { isArrayNotEmpty, isObject, isObjectEmpty } from '../utils';

export const EnumPlanAction = {
  None: 'none',
  Done: 'done',
  Run: 'run',
  Running: 'running',
  Apply: 'apply',
  Applying: 'applying',
  Canceling: 'canceling',
  Resetting: 'resetting',
  Closing: 'closing',
  Opening: 'opening',
} as const;

export const EnumPlanState = {
  Init: 'init',
  Applying: 'applying',
  Canceling: 'canceling',
  Finished: 'finished',
  Failed: 'failed',
  Cancelled: 'cancelled',
} as const;

export type PlanState = typeof EnumPlanState[keyof typeof EnumPlanState]
export type PlanAction = typeof EnumPlanAction[keyof typeof EnumPlanAction]

type Category = {
  id: string;
  name: string;
  description: string;
}

type PlanTaskStatus = {
  completed: number;
  total: number;
}

type PlanTasks = { [key: string]: PlanTaskStatus }

type PlanProgress = {
  ok: boolean;
  environment: string;
  tasks: PlanTasks;
  updated_at: string;
}

interface PlanStore {
  state: PlanState;
  action: PlanAction;
  setActivePlan: (activePlan: PlanProgress | null) => void;
  setLastPlan: (lastPlan: PlanProgress | null) => void;
  setState: (state: PlanState) => void;
  setAction: (action: PlanAction) => void;
  setEnvironment: (environment: string) => void;
  setCategory: (category: Category) => void;
  activePlan?: PlanProgress | null;
  lastPlan?: PlanProgress | null;
  backfill_start: string | null;
  backfill_end: string | null;
  environment: string | null;
  category: Category | null;
  categories: Category[];
  withBackfill: boolean;
  setWithBackfill: (withBackfill: boolean) => void;
  backfills: ContextEnvironmentBackfill[];
  setBackfills: (backfills: ContextEnvironmentBackfill[]) => void;
  updateTasks: (data: PlanProgress, channel: EventSource, unsubscribe: () => void) => void;
}

export const useStorePlan = create<PlanStore>((set, get) => ({
  state: EnumPlanState.Init,
  action: EnumPlanAction.None,
  activePlan: null,
  lastPlan: null,
  setActivePlan: (activePlan: PlanProgress | null) => set(() => ({ activePlan })),
  setLastPlan: (lastPlan: PlanProgress | null) => set(() => ({ lastPlan })),
  setState: (state: PlanState) => set(() => ({ state })),
  setAction: (action: PlanAction) => set(() => ({ action })),
  setEnvironment: (environment: string) => set(() => ({ environment })),
  setCategory: (category: Category) => set(() => ({ category })),
  backfill_start: null,
  backfill_end: null,
  setBackfillDate: (type: 'start' | 'end' , date: string) => set(() => ({
    [`backfill_${type}`]: date,
  })),
  environment: null,
  category: null,
  categories: getCategories(),
  withBackfill: true,
  setWithBackfill: (withBackfill: boolean) => set(() => ({ withBackfill })),
  backfills: [],
  setBackfills: (backfills: ContextEnvironmentBackfill[]) => set(() => ({ backfills })),
  updateTasks: (data: PlanProgress, channel: EventSource, unsubscribe: () => void) => {
    const s = get()

    if (channel == null) return

    if (data.environment == null || isObject(data.tasks) === false) {
      s.setState(EnumPlanState.Init)
  
      channel.close()
      unsubscribe()
  
      return
    }

    const plan: PlanProgress = {
      ok: data.ok,
      environment: data.environment,
      tasks: data.tasks,
      updated_at: data.updated_at || new Date().toISOString(),
    }

    s.setActivePlan(plan)
  
    if (data.ok === false) {
      s.setState(EnumPlanState.Failed)

      s.setLastPlan(plan)
  
      channel.close()
      unsubscribe()
  
      return
    }
  
    const isAllCompleted = isObjectEmpty(data.tasks) || isAllTasksCompleted(data.tasks)
  
    if (isAllCompleted) {
      s.setState(EnumPlanState.Finished)

      if (isObjectEmpty(s.activePlan?.tasks)) {
        s.setLastPlan(null)
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

function isAllTasksCompleted(tasks: PlanTasks = {}): boolean {
  return Object.values(tasks).every(t => t.completed === t.total)
}

function getCategories(): Category[] {
  return [
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
  ]
}