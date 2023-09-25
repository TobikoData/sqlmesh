import { type Confirmation } from '@components/modal/ModalConfirmation'
import { ModelSQLMeshModel } from '@models/sqlmesh-model'
import { create } from 'zustand'
import {
  type Model,
  type ContextEnvironmentEnd,
  type ContextEnvironmentStart,
  type Environment,
} from '~/api/client'
import {
  EnumRelativeLocation,
  type EnvironmentName,
  ModelEnvironment,
} from '~/models/environment'
import { isNil, isStringEmptyOrNil } from '~/utils'

interface ContextStore {
  version?: string
  showConfirmation: boolean
  confirmations: Confirmation[]
  environment: ModelEnvironment
  environments: Set<ModelEnvironment>
  defaultEnvironment?: ModelEnvironment
  pinnedEnvironments: ModelEnvironment[]
  initialStartDate?: ContextEnvironmentStart
  initialEndDate?: ContextEnvironmentEnd
  models: Map<string, ModelSQLMeshModel>
  setVersion: (version?: string) => void
  setShowConfirmation: (showConfirmation: boolean) => void
  addConfirmation: (confirmation: Confirmation) => void
  removeConfirmation: () => void
  setModels: (models?: Model[]) => void
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
  addSynchronizedEnvironments: (
    environments: Environment[],
    defaultEnvironment?: string,
    pinnedEnvironments?: string[],
  ) => void
  hasSynchronizedEnvironments: () => boolean
  setInitialDates: (
    initialStartDate?: ContextEnvironmentStart,
    initialEndDate?: ContextEnvironmentEnd,
  ) => void
}

const environments = new Set(ModelEnvironment.getEnvironments())
const environment = environments.values().next().value

export const useStoreContext = create<ContextStore>((set, get) => ({
  version: undefined,
  showConfirmation: false,
  confirmations: [],
  environment,
  environments,
  defaultEnvironment: undefined,
  pinnedEnvironments: [],
  initialStartDate: undefined,
  initialEndDate: undefined,
  models: new Map(),
  setVersion(version) {
    set(() => ({
      version,
    }))
  },
  setShowConfirmation(showConfirmation) {
    set(() => ({
      showConfirmation,
    }))
  },
  addConfirmation(confirmation) {
    set(s => {
      s.confirmations.push(confirmation)

      return {
        confirmations: Array.from(s.confirmations),
      }
    })
  },
  removeConfirmation() {
    const s = get()

    s.confirmations.shift()

    set(() => ({
      confirmations: Array.from(s.confirmations),
    }))
  },
  setModels(models = []) {
    const s = get()

    set(() => ({
      models: models.reduce((acc: Map<string, ModelSQLMeshModel>, model) => {
        let tempModel = s.models.get(model.path) ?? s.models.get(model.name)

        if (tempModel == null) {
          tempModel = new ModelSQLMeshModel(model)
        } else {
          tempModel.update(model)
        }

        acc.set(model.name, tempModel)
        acc.set(model.path, tempModel)

        return acc
      }, new Map()),
    }))
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
  addSynchronizedEnvironments(envs = [], defaultEnvironment, pinnedEnvs = []) {
    set(s => {
      const environments = Array.from(s.environments)

      envs.forEach(env => {
        let environment = environments.find(
          ({ name: envNameLocal }) => env.name === envNameLocal,
        )

        if (isNil(environment)) {
          environment = new ModelEnvironment(
            env,
            EnumRelativeLocation.Synchronized,
          )

          environments.push(environment)
        } else {
          environment.update(env)
          environment.setType(EnumRelativeLocation.Synchronized)
        }

        if (environment.isInitial && environment.isDefault) {
          s.setEnvironment(environment)
        }
      })

      pinnedEnvs.forEach(envName => {
        const environment = environments.find(
          ({ name: envNameLocal }) => envName === envNameLocal,
        )

        if (isNil(environment)) {
          environments.push(
            new ModelEnvironment(
              {
                name: envName,
              },
              EnumRelativeLocation.Local,
              undefined,
              true,
            ),
          )
        } else {
          environment.isPinned = true
        }
      })

      ModelEnvironment.save({ environments })
      ModelEnvironment.sort(environments)

      const profileEnv = ModelEnvironment.getEnvironment()
      let prodEnv = environments.find(({ name }) => name === 'prod')
      let storedEnv = environments.find(({ name }) => name === profileEnv?.name)
      let defaultEnv = environments.find(
        ({ name }) => name === defaultEnvironment,
      )

      environments.forEach(env => {
        switch (env.name) {
          case 'prod':
            prodEnv = env
            break
          case profileEnv?.name:
            storedEnv = env
            break
          case defaultEnvironment:
            defaultEnv = env
        }
      })

      const currentEnv = storedEnv ?? defaultEnv ?? s.environment

      return {
        environment: isStringEmptyOrNil(prodEnv?.id) ? prodEnv : currentEnv,
        defaultEnvironment: defaultEnv,
        pinnedEnvironments: environments.filter(env =>
          pinnedEnvs.includes(env.name),
        ),
        environments: new Set(environments),
      }
    })
  },
  hasSynchronizedEnvironments() {
    const s = get()

    return Array.from(s.environments).some(
      ({ type }) => type === EnumRelativeLocation.Synchronized,
    )
  },
}))
