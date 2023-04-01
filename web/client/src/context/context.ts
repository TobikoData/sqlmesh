import { create } from 'zustand'
import {
  type Model,
  type ModelsModels,
  type ContextEnvironmentEnd,
  type ContextEnvironmentStart,
  type Environment,
} from '~/api/client'
import {
  EnumRelativeLocation,
  type EnvironmentName,
  ModelEnvironment,
} from '~/models/environment'
import { isStringEmptyOrNil } from '~/utils'

interface ContextStore {
  environment: ModelEnvironment
  environments: Set<ModelEnvironment>
  initialStartDate?: ContextEnvironmentStart
  initialEndDate?: ContextEnvironmentEnd
  models: Map<string, Model>
  setModels: (models?: ModelsModels) => void
  isExistingEnvironment: (
    environment: ModelEnvironment | EnvironmentName,
  ) => boolean
  getNextEnvironment: () => ModelEnvironment
  setEnvironment: (environment: ModelEnvironment) => void
  addLocalEnvironment: (
    environments: EnvironmentName,
    created_from: EnvironmentName,
  ) => void
  removeLocalEnvironment: (environments: ModelEnvironment) => void
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
  models: new Map(),
  setModels(models = {}) {
    set(() => {
      return {
        models: Object.values(models).reduce((acc, model) => {
          acc.set(model.name, model)
          acc.set(model.path, model)

          return acc
        }, new Map()),
      }
    })
  },
  getNextEnvironment() {
    return get().environments.values().next().value
  },
  setInitialDates(initialStartDate, initialEndDate) {
    set({
      initialStartDate,
      initialEndDate,
    })
  },
  isExistingEnvironment(environment) {
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
  setEnvironment(environment) {
    set(() => {
      ModelEnvironment.save({
        environment,
      })

      return {
        environment,
      }
    })
  },
  addLocalEnvironment(localEnvironment, created_from) {
    set(s => {
      if (isStringEmptyOrNil(localEnvironment)) return s

      const environment = new ModelEnvironment(
        {
          name: localEnvironment,
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
  removeLocalEnvironment(localEnvironment) {
    set(s => {
      s.environments.delete(localEnvironment)

      ModelEnvironment.save({
        environments: Array.from(s.environments),
      })

      return {
        environments: new Set(s.environments),
      }
    })
  },
  addSyncronizedEnvironments(envs = []) {
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
