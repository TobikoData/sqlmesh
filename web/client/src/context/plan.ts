import { create } from 'zustand'
import { ContextEnvironmentBackfill } from '../api/client'
import { isFalse, isNil, isObject } from '../utils'

export const EnumPlanAction = {
  None: 'none',
  Done: 'done',
  Run: 'run',
  Running: 'running',
  Apply: 'apply',
  Applying: 'applying',
  Cancelling: 'cancelling',
  Resetting: 'resetting',
} as const

export const EnumPlanState = {
  Init: 'init',
  Applying: 'applying',
  Cancelling: 'cancelling',
  Finished: 'finished',
  Failed: 'failed',
  Cancelled: 'cancelled',
} as const

export const EnumCategoryType = {
  BreakingChange: 'breaking-change',
  NonBreakingChange: 'non-breaking-change',
  NoChange: 'no-change',
} as const

export type PlanState = KeyOf<typeof EnumPlanState>
export type PlanAction = KeyOf<typeof EnumPlanAction>
export type CategoryType = KeyOf<typeof EnumCategoryType>

interface Category {
  id: CategoryType
  name: string
  description: string
}

export interface PlanTaskStatus {
  total: number
  completed: number
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
  type?: 'logical' | 'backfill'
}

interface PlanOptions {
  skipTests: boolean
  noGaps: boolean
  skipBackfill: boolean
  forwardOnly: boolean
  autoApply: boolean
  start: string
  end: string
  from: string
  restateModel: string
}

interface PlanStore {
  state: PlanState
  action: PlanAction
  setActivePlan: (activePlan?: PlanProgress) => void
  setMostRecentPlan: (mostRecentPlan?: PlanProgress) => void
  setState: (state: PlanState) => void
  setAction: (action: PlanAction) => void
  setCategory: (category?: Category) => void
  activePlan?: PlanProgress
  mostRecentPlan?: PlanProgress
  backfill_start: string
  backfill_end: string
  setBackfillDate: (type: 'start' | 'end', date: string) => void
  category?: Category
  categories: Category[]
  withBackfill: boolean
  setWithBackfill: (withBackfill: boolean) => void
  backfills: ContextEnvironmentBackfill[]
  setBackfills: (backfills?: ContextEnvironmentBackfill[]) => void
  updateTasks: (data: PlanProgress) => void
  planOptions: PlanOptions
  setPlanOptions: (planOptions: Partial<PlanOptions>) => void
  resetPlanOptions: () => void
}

const planDefaultOptions: PlanOptions = {
  skipTests: true,
  noGaps: false,
  skipBackfill: false,
  forwardOnly: false,
  autoApply: false,
  start: '',
  end: '',
  from: '',
  restateModel: '',
}

const categories = getCategories()

export const useStorePlan = create<PlanStore>((set, get) => ({
  planOptions: planDefaultOptions,
  state: EnumPlanState.Init,
  action: EnumPlanAction.None,
  activePlan: undefined,
  mostRecentPlan: undefined,
  setPlanOptions: (planOptions: Partial<PlanOptions>) => {
    set(s => ({ planOptions: { ...s.planOptions, ...planOptions } }))
  },
  resetPlanOptions: () => {
    set(() => ({ planOptions: planDefaultOptions }))
  },
  setActivePlan: (activePlan?: PlanProgress) => {
    set(() => ({ activePlan }))
  },
  setMostRecentPlan: (mostRecentPlan?: PlanProgress) => {
    set(() => ({ mostRecentPlan }))
  },
  setState: (state: PlanState) => {
    set(() => ({ state }))
  },
  setAction: (action: PlanAction) => {
    set(() => ({ action }))
  },
  setCategory: (category?: Category) => {
    set(() => ({ category: category ?? categories[0] }))
  },
  backfill_start: '',
  backfill_end: '',
  setBackfillDate: (type: 'start' | 'end', date: string) => {
    set(() => ({
      [`backfill_${type}`]: date ?? '',
    }))
  },
  category: categories[0],
  categories,
  withBackfill: true,
  setWithBackfill: (withBackfill: boolean) => {
    set(() => ({ withBackfill }))
  },
  backfills: [],
  setBackfills: (backfills?: ContextEnvironmentBackfill[]) => {
    set(() => (backfills == null ? { backfills: [] } : { backfills }))
  },
  updateTasks: (data: PlanProgress) => {
    const s = get()

    if (isNil(data)) return
    if (isFalse(isObject(data.tasks))) {
      s.setState(EnumPlanState.Init)

      return
    }

    const plan: PlanProgress = {
      ok: data.ok,
      tasks: data.tasks,
      updated_at: data.updated_at ?? new Date().toISOString(),
    }

    s.setActivePlan(plan)

    if (isFalse(data.ok)) {
      s.setState(EnumPlanState.Failed)
      s.setActivePlan(undefined)
      s.setMostRecentPlan(plan)
    } else if (isAllTasksCompleted(data.tasks)) {
      s.setState(EnumPlanState.Finished)
      s.setActivePlan(undefined)
      s.setMostRecentPlan(plan)
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
      id: EnumCategoryType.BreakingChange,
      name: 'Breaking Change',
      description: 'This is a breaking change',
    },
    {
      id: EnumCategoryType.NonBreakingChange,
      name: 'Non-Breaking Change',
      description: 'This is a non-breaking change',
    },
    {
      id: EnumCategoryType.NoChange,
      name: 'No Change',
      description: 'This is a no change',
    },
  ]
}
