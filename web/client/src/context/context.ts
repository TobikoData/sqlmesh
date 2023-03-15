import { create } from 'zustand'
import {
  ContextEnvironmentEnd,
  ContextEnvironmentStart,
  Environment,
} from '~/api/client'
import {
  EnumRelativeLocation,
  EnvironmentName,
  ModelEnvironment,
} from '~/models/environment'
import { isStringEmptyOrNil } from '~/utils'

interface ContextStore {
  environment: ModelEnvironment
  environments: Set<ModelEnvironment>
  initialStartDate?: ContextEnvironmentStart
  initialEndDate?: ContextEnvironmentEnd
  isExistingEnvironment: (
    environment: ModelEnvironment | EnvironmentName,
  ) => boolean
  getNextEnvironment: () => ModelEnvironment
  setEnvironment: (environment: ModelEnvironment) => void
  addLocalEnvironments: (
    environments: EnvironmentName,
    created_from: EnvironmentName,
  ) => void
  removeLocalEnvironments: (environments: ModelEnvironment) => void
  addSyncronizedEnvironments: (environments: Environment[]) => void
  setInitialDates: (
    initialStartDate?: ContextEnvironmentStart,
    initialEndDate?: ContextEnvironmentEnd,
  ) => void
}

const environments = new Set(ModelEnvironment.getDefaultEnvironments())
const environment = environments.values().next().value

export const useStoreContext = create<ContextStore>((set, get) => ({
  environment,
  environments,
  initialStartDate: undefined,
  initialEndDate: undefined,
  getNextEnvironment(): ModelEnvironment {
    return get().environments.values().next().value
  },
  setInitialDates(
    initialStartDate?: ContextEnvironmentStart,
    initialEndDate?: ContextEnvironmentEnd,
  ): void {
    set({
      initialStartDate,
      initialEndDate,
    })
  },
  isExistingEnvironment(
    environment: ModelEnvironment | EnvironmentName,
  ): boolean {
    const s = get()

    if ((environment as ModelEnvironment).isModel)
      return s.environments.has(environment as ModelEnvironment)

    let hasEnvironment = false

    s.environments.forEach(env => {
      if (env.name === (environment as EnvironmentName)) {
        hasEnvironment = true
      }
    })

    return hasEnvironment
  },
  setEnvironment(environment: ModelEnvironment): void {
    set(() => {
      ModelEnvironment.save({
        environment,
      })

      return {
        environment,
      }
    })
  },
  addLocalEnvironments(
    localEnvironments: EnvironmentName,
    created_from?: EnvironmentName,
  ): void {
    set(s => {
      if (isStringEmptyOrNil(localEnvironments)) return s

      const environment = new ModelEnvironment(
        {
          name: localEnvironments,
        },
        EnumRelativeLocation.Local,
        created_from,
      )

      s.environments.add(environment)

      s.setEnvironment(environment)

      ModelEnvironment.save({
        environments: Array.from(s.environments),
      })

      return {
        environments: new Set(s.environments),
      }
    })
  },
  removeLocalEnvironments(localEnvironments: ModelEnvironment) {
    set(s => {
      s.environments.delete(localEnvironments)

      ModelEnvironment.save({
        environments: Array.from(s.environments),
      })

      return {
        environments: new Set(s.environments),
      }
    })
  },
  addSyncronizedEnvironments(envs: Environment[] = []) {
    set(s => {
      const environments = Array.from(s.environments)

      envs.forEach(env => {
        let environment = environments.find(
          ({ name: envNameLocal }) => env.name === envNameLocal,
        )

        if (environment == null) {
          environment = new ModelEnvironment(
            env,
            EnumRelativeLocation.Syncronized,
          )

          environments.push(environment)
        } else {
          environment.update(env)
          environment.setType(EnumRelativeLocation.Syncronized)
        }

        if (environment.isInitial && environment.isDefault) {
          s.setEnvironment(environment)
        }
      })

      ModelEnvironment.save({
        environments,
      })

      ModelEnvironment.sort(environments)

      return {
        environments: new Set(environments),
      }
    })
  },
}))
