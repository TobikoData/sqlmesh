import { create } from 'zustand'

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
  Running: 'running',
  Applying: 'applying',
  Cancelling: 'cancelling',
  Finished: 'finished',
  Failed: 'failed',
  Cancelled: 'cancelled',
} as const

export const EnumPlanApplyType = {
  Virtual: 'virtual',
  Backfill: 'backfill',
} as const

export type PlanApplyType = KeyOf<typeof EnumPlanApplyType>
export type PlanState = KeyOf<typeof EnumPlanState>
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
  state: PlanState
  action: PlanAction
  activePlan?: PlanProgress
  setActivePlan: (activePlan?: PlanProgress) => void
  setState: (state: PlanState) => void
  setAction: (action: PlanAction) => void
}

export const useStorePlan = create<PlanStore>((set, get) => ({
  state: EnumPlanState.Init,
  action: EnumPlanAction.None,
  activePlan: undefined,
  setActivePlan: (activePlan?: PlanProgress) => {
    set(() => ({ activePlan }))
  },
  setState: (state: PlanState) => {
    set(() => ({ state }))
  },
  setAction: (action: PlanAction) => {
    set(() => ({ action }))
  },
}))
