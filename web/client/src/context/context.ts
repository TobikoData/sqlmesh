import { create } from 'zustand'
import useLocalStorage from '~/hooks/useLocalStorage'
import { Profile } from '~/library/components/ide/IDE'
import { isFalse } from '~/utils'

export const EnumRelativeLocation = {
  Local: 'local',
  Remote: 'remote',
} as const

export const EnumDefaultEnvironment = {
  Prod: 'prod',
  Dev: 'dev',
  Stage: 'stage',
} as const

export type RelativeLocation = KeyOf<typeof EnumRelativeLocation>
export type DefaultEnvironment = KeyOf<typeof EnumDefaultEnvironment>
export type EnvironmentName = DefaultEnvironment | string

export interface Environment {
  name: EnvironmentName
  type: RelativeLocation
}

interface ContextStore {
  environment?: EnvironmentName
  environments: Environment[]
  setEnvironment: (environment: EnvironmentName) => void
  addLocalEnvironments: (environments: EnvironmentName[]) => void
  removeLocalEnvironments: (environments: EnvironmentName[]) => void
  addRemoteEnvironments: (environments: EnvironmentName[]) => void
  isExistingEnvironment: (environment: EnvironmentName) => boolean
}

const [getProfile, setProfile] = useLocalStorage<Profile>('profile')

export const useStoreContext = create<ContextStore>((set, get) => ({
  environment: getProfile()?.environment,
  environments: getDefaultEnvironments(),
  isExistingEnvironment: (environment: EnvironmentName) => {
    const { environments } = get()

    return environments.some(({ name }) => name === environment)
  },
  setEnvironment: (environment: EnvironmentName) => {
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
  addLocalEnvironments: (localEnvironments: EnvironmentName[] = []) => {
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
          })
        }
      })

      setProfile({
        environments: getOnlyLocalEnvironments(environments),
      })

      return { environments }
    })
  },
  removeLocalEnvironments: (localEnvironments: EnvironmentName[] = []) => {
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
  addRemoteEnvironments: (remoteEnvironments: EnvironmentName[] = []) => {
    set(s => {
      const environments = structuredClone(s.environments)

      remoteEnvironments.forEach(name => {
        const environment = environments.find(
          ({ name: envNameLocal }) => name === envNameLocal,
        )

        if (environment == null) {
          environments.push({
            name,
            type: EnumRelativeLocation.Remote,
          })
        } else {
          environment.type = EnumRelativeLocation.Remote
        }
      })

      setProfile({
        environments: getOnlyLocalEnvironments(environments),
      })

      sortEnvoirnments(environments)

      return { environments }
    })
  },
}))

function sortEnvoirnments(environments: Environment[]): void {
  environments.sort(env => (env.type === EnumRelativeLocation.Remote ? -1 : 1))
}

function getOnlyLocalEnvironments(
  environments: Environment[] = [],
): Environment[] {
  return environments.filter(({ type }) => type === EnumRelativeLocation.Local)
}

function getDefaultEnvironments(): Environment[] {
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

  const output: Environment[] = []

  environments.forEach(name => {
    output.push({
      name,
      type: EnumRelativeLocation.Local,
    })
  })

  return output
}
