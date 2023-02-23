import { create } from 'zustand'

export interface Environment {
  name: string
  type: 'local' | 'remote'
}

interface ContextStore {
  prod: Environment
  environment?: string
  environments: Environment[]
  setEnvironment: (environment?: string) => void
  setEnvironments: (environments: Environment[]) => void
}

export const PROD: Environment = {
  name: 'prod',
  type: 'local',
}

export const useStoreContext = create<ContextStore>((set, get) => ({
  prod: PROD,
  environment: undefined,
  environments: getDefaultEnvironments(),
  setEnvironment: (environment?: string) => {
    set(() => ({ environment }))
  },
  setEnvironments: (environments: Environment[]) => {
    set(() => ({ environments }))
  },
}))

export function getDefaultEnvironments(): Environment[] {
  return [
    PROD,
    {
      name: 'dev',
      type: 'local',
    },
    {
      name: 'stage',
      type: 'local',
    },
  ]
}
