import { create } from 'zustand'
import { ContextEnvironmentBackfill } from '../api/client'
import { isObject, isObjectEmpty } from '../utils'

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
} as const

export const EnumPlanState = {
  Init: 'init',
  Applying: 'applying',
  Canceling: 'canceling',
  Finished: 'finished',
  Failed: 'failed',
  Cancelled: 'cancelled',
} as const

export type PlanState = typeof EnumPlanState[keyof typeof EnumPlanState]
export type PlanAction = typeof EnumPlanAction[keyof typeof EnumPlanAction]

interface Category {
  id: string
  name: string
  description: string
}

interface PlanTaskStatus {
  completed: number
  total: number
}

type PlanTasks = Record<string, PlanTaskStatus>

interface PlanProgress {
  ok: boolean
  environment: string
  tasks: PlanTasks
  updated_at: string
}

interface PlanStore {
  state: PlanState
  action: PlanAction
  setActivePlan: (activePlan?: PlanProgress) => void
  setLastPlan: (lastPlan?: PlanProgress) => void
  setState: (state: PlanState) => void
  setAction: (action: PlanAction) => void
  setEnvironment: (environment?: string) => void
  setCategory: (category?: Category) => void
  activePlan?: PlanProgress
  lastPlan?: PlanProgress
  backfill_start?: string
  backfill_end?: string
  setBackfillDate: (type: 'start' | 'end', date: string) => void
  environment?: string
  category?: Category
  categories: Category[]
  withBackfill: boolean
  setWithBackfill: (withBackfill: boolean) => void
  backfills: ContextEnvironmentBackfill[]
  setBackfills: (backfills?: ContextEnvironmentBackfill[]) => void
  updateTasks: (
    data: PlanProgress,
    channel: EventSource,
    unsubscribe: () => void,
  ) => void
}

export const useStorePlan = create<PlanStore>((set, get) => ({
  state: EnumPlanState.Init,
  action: EnumPlanAction.None,
  activePlan: undefined,
  lastPlan: undefined,
  setActivePlan: (activePlan?: PlanProgress) => {
    set(() => ({ activePlan }))
  },
  setLastPlan: (lastPlan?: PlanProgress) => {
    set(() => ({ lastPlan }))
  },
  setState: (state: PlanState) => {
    set(() => ({ state }))
  },
  setAction: (action: PlanAction) => {
    set(() => ({ action }))
  },
  setEnvironment: (environment?: string) => {
    set(() => ({ environment }))
  },
  setCategory: (category?: Category) => {
    set(() => ({ category }))
  },
  backfill_start: undefined,
  backfill_end: undefined,
  setBackfillDate: (type: 'start' | 'end', date: string) => {
    set(() => ({
      [`backfill_${type}`]: date,
    }))
  },
  environment: undefined,
  category: undefined,
  categories: getCategories(),
  withBackfill: true,
  setWithBackfill: (withBackfill: boolean) => {
    set(() => ({ withBackfill }))
  },
  backfills: [],
  setBackfills: (backfills?: ContextEnvironmentBackfill[]) => {
    set(() => (backfills == null ? { backfills: [] } : { backfills }))
  },
  updateTasks: (
    data: PlanProgress,
    channel: EventSource,
    unsubscribe: () => void,
  ) => {
    const s = get()

    if (channel == null) return

    if (data.environment == null || !isObject(data.tasks)) {
      s.setState(EnumPlanState.Init)

      channel.close()
      unsubscribe()

      return
    }

    const plan: PlanProgress = {
      ok: data.ok,
      environment: data.environment,
      tasks: data.tasks,
      updated_at: data.updated_at ?? new Date().toISOString(),
    }

    s.setActivePlan(plan)

    if (!data.ok) {
      s.setState(EnumPlanState.Failed)
      s.setLastPlan(plan)

      channel.close()
      unsubscribe()

      return
    }

    const isAllCompleted =
      isObjectEmpty(data.tasks) || isAllTasksCompleted(data.tasks)

    if (isAllCompleted) {
      s.setState(EnumPlanState.Finished)

      if (isObjectEmpty(s.activePlan?.tasks)) {
        s.setLastPlan(undefined)
      } else {
        s.setLastPlan(plan)
      }

      if (isObjectEmpty(data.tasks)) {
        s.setActivePlan(undefined)
      }

      channel?.close()
      unsubscribe()
    } else {
      s.setState(EnumPlanState.Applying)
    }
  },
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
