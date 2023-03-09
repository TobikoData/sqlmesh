import { create } from 'zustand'
import {
  ContextEnvironmentEnd,
  ContextEnvironmentStart,
  Environment,
} from '~/api/client'
import useLocalStorage from '~/hooks/useLocalStorage'
import { isFalse, isStringEmptyOrNil } from '~/utils'

export const EnumRelativeLocation = {
  Local: 'local',
  Remote: 'remote',
} as const

export const EnumDefaultEnvironment = {
  Prod: 'prod',
  Dev: 'dev',
  Stage: 'stage',
} as const

export interface Profile {
  environment: string
  environments: EnvironmentShort[]
}

export type RelativeLocation = KeyOf<typeof EnumRelativeLocation>
export type DefaultEnvironment = KeyOf<typeof EnumDefaultEnvironment>
export type EnvironmentName = DefaultEnvironment | string

export interface EnvironmentShort {
  name: EnvironmentName
  type: RelativeLocation
  isInitial: boolean
}

interface ContextStore {
  environment?: EnvironmentName
  environments: EnvironmentShort[]
  initialStartDate?: ContextEnvironmentStart
  initialEndDate?: ContextEnvironmentEnd
  setEnvironment: (environment: EnvironmentName) => void
  addLocalEnvironments: (environments: EnvironmentName[]) => void
  removeLocalEnvironments: (environments: EnvironmentName[]) => void
  addRemoteEnvironments: (environments: Environment[]) => void
  isExistingEnvironment: (environment: EnvironmentName) => boolean
  setInitialDates: (
    initialStartDate?: ContextEnvironmentStart,
    initialEndDate?: ContextEnvironmentEnd,
  ) => void
}

const [getProfile, setProfile] = useLocalStorage<Profile>('profile')

export const useStoreContext = create<ContextStore>((set, get) => ({
  environment: getProfile()?.environment,
  environments: getDefaultEnvironments(),
  initialStartDate: undefined,
  initialEndDate: undefined,
  setInitialDates(
    initialStartDate?: ContextEnvironmentStart,
    initialEndDate?: ContextEnvironmentEnd,
  ) {
    set({
      initialStartDate,
      initialEndDate,
    })
  },
  isExistingEnvironment(environment: EnvironmentName) {
    const { environments } = get()

    return environments.some(({ name }) => name === environment)
  },
  setEnvironment(environment: EnvironmentName) {
    set(s => {
      s.addLocalEnvironments([environment])

      setProfile({
        environment,
      })

      return {
        environment,
      }
    })
  },
  addLocalEnvironments(localEnvironments: EnvironmentName[] = []) {
    set(s => {
      const environments = structuredClone(s.environments)

      localEnvironments.forEach(name => {
        const environment = environments.find(
          ({ name: envNameLocal }) => name === envNameLocal,
        )

        if (environment == null) {
          environments.push({
            name,
            type: EnumRelativeLocation.Local,
            isInitial: true,
          })
        }
      })

      setProfile({
        environments: getOnlyLocalEnvironments(environments),
      })

      return { environments }
    })
  },
  removeLocalEnvironments(localEnvironments: EnvironmentName[] = []) {
    set(s => {
      const environments = s.environments.filter(({ name }) =>
        isFalse(localEnvironments.includes(name)),
      )

      setProfile({
        environments: getOnlyLocalEnvironments(environments),
      })

      return { environments }
    })
  },
  addRemoteEnvironments(remoteEnvironments: Environment[] = []) {
    set(s => {
      const environments = structuredClone(s.environments)

      remoteEnvironments.forEach(env => {
        const environment = environments.find(
          ({ name: envNameLocal }) => env.name === envNameLocal,
        )

        if (environment == null) {
          environments.push({
            name: env.name,
            type: EnumRelativeLocation.Remote,
            isInitial: isStringEmptyOrNil(env.plan_id),
          })
        } else {
          environment.type = EnumRelativeLocation.Remote
          environment.isInitial = isStringEmptyOrNil(env.plan_id)
        }
      })

      setProfile({
        environments: getOnlyLocalEnvironments(environments),
      })

      sortEnvoirnments(environments)

      return {
        environments,
      }
    })
  },
}))

function sortEnvoirnments(environments: EnvironmentShort[]): void {
  environments.sort(env => (env.type === EnumRelativeLocation.Remote ? -1 : 1))
}

function getOnlyLocalEnvironments(
  environments: EnvironmentShort[] = [],
): EnvironmentShort[] {
  return environments.filter(({ type }) => type === EnumRelativeLocation.Local)
}

function getDefaultEnvironments(): EnvironmentShort[] {
  const profile = getProfile()
  const environments = new Set<string>()

  if (profile?.environment != null) {
    environments.add(profile.environment)
  }

  if (profile?.environments != null) {
    profile.environments.forEach(({ name }) => environments.add(name))
  }

  ;[
    EnumDefaultEnvironment.Prod,
    EnumDefaultEnvironment.Dev,
    EnumDefaultEnvironment.Stage,
  ].forEach(name => environments.add(name))

  const output: EnvironmentShort[] = []

  environments.forEach(name => {
    output.push({
      name,
      type: EnumRelativeLocation.Local,
      isInitial: true,
    })
  })

  return output
}
