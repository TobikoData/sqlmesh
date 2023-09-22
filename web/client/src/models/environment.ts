import { type Environment } from '~/api/client'
import useLocalStorage from '~/hooks/useLocalStorage'
import { isArrayEmpty, isFalse, isNotNil, isStringEmptyOrNil } from '~/utils'

export const EnumDefaultEnvironment = {
  Empty: '',
  Prod: 'prod',
} as const

export const EnumRelativeLocation = {
  Local: 'local',
  Synchronized: 'synchronized',
} as const

export type EnvironmentName = DefaultEnvironment | string
export type DefaultEnvironment = KeyOf<typeof EnumDefaultEnvironment>
export type RelativeLocation = KeyOf<typeof EnumRelativeLocation>

interface InitialEnvironmemt extends Partial<Environment> {
  name?: EnvironmentName
}

interface ProfileEnvironment {
  name: EnvironmentName
  createFrom: EnvironmentName
  isPinned: boolean
}

interface Profile {
  environment: ProfileEnvironment
  environments: ProfileEnvironment[]
}

const [getProfile, setProfile] = useLocalStorage<Profile>('profile')

export class ModelEnvironment {
  private _initial: InitialEnvironmemt
  private _type: RelativeLocation
  private _createFrom: EnvironmentName

  isPinned = false
  isModel = true

  constructor(
    initial: InitialEnvironmemt,
    type: RelativeLocation,
    createFrom: EnvironmentName = EnumDefaultEnvironment.Prod,
    isPinned = false,
  ) {
    this._initial = initial
    this._type = type ?? EnumRelativeLocation.Local
    this._createFrom = this.isDefault
      ? EnumDefaultEnvironment.Empty
      : createFrom
    this.isPinned = isPinned
  }

  get id(): string {
    return this._initial.plan_id ?? ''
  }

  get name(): string {
    return this._initial.name ?? EnumDefaultEnvironment.Empty
  }

  get type(): string {
    return this._type
  }

  get createFrom(): string {
    return this._createFrom
  }

  get isDefault(): boolean {
    return this.name === EnumDefaultEnvironment.Prod
  }

  get isInitial(): boolean {
    return isStringEmptyOrNil(this._initial.plan_id)
  }

  get isLocal(): boolean {
    return this._type === EnumRelativeLocation.Local
  }

  get isSynchronized(): boolean {
    return this._type === EnumRelativeLocation.Synchronized
  }

  setType(type: RelativeLocation): void {
    this._type = type
  }

  setCreatedFrom(createFrom: EnvironmentName): void {
    this._createFrom = createFrom
  }

  update(initial: InitialEnvironmemt): void {
    this._initial = initial
  }

  static save({
    environment,
    environments,
  }: {
    environment?: ModelEnvironment
    environments?: ModelEnvironment[]
  }): void {
    const output: Partial<Profile> = {}

    if (isNotNil(environment)) {
      output.environment = {
        name: environment.name,
        createFrom: environment.createFrom,
        isPinned: environment.isPinned,
      }
    }

    if (isNotNil(environments)) {
      output.environments = ModelEnvironment.getOnlyLocal(environments).map(
        env => ({
          name: env.name,
          createFrom: env.createFrom,
          isPinned: env.isPinned,
        }),
      )
    }

    setProfile(output)
  }

  static getOnlyLocal(envs: ModelEnvironment[] = []): ModelEnvironment[] {
    return envs.filter(
      env => isFalse(isStringEmptyOrNil(env.name)) && env.isLocal,
    )
  }

  static getOnlySynchronized(
    envs: ModelEnvironment[] = [],
  ): ModelEnvironment[] {
    return envs.filter(
      env => isFalse(isStringEmptyOrNil(env.name)) && env.isSynchronized,
    )
  }

  static getEnvironment(): Optional<ProfileEnvironment> {
    const profile = getProfile()

    return profile?.environment
  }

  static getEnvironments(): ModelEnvironment[] {
    const profile = getProfile()
    const environments = new Map<EnvironmentName, ProfileEnvironment>()

    if (isNotNil(profile) && isNotNil(profile.environment)) {
      environments.set(profile.environment.name, profile.environment)
    }

    if (isNotNil(profile) && isNotNil(profile.environments)) {
      profile.environments.forEach(environment =>
        environments.set(environment.name, environment),
      )
    }

    const output: ModelEnvironment[] = []

    Array.from(environments.entries()).forEach(([name, environment]) => {
      output.push(
        new ModelEnvironment(
          { name },
          EnumRelativeLocation.Local,
          environment.createFrom,
          environment.isPinned,
        ),
      )
    })

    if (isArrayEmpty(output)) {
      output.push(
        new ModelEnvironment(
          { name: EnumDefaultEnvironment.Prod },
          EnumRelativeLocation.Local,
        ),
      )
    }

    return output
  }

  static sort(environments: ModelEnvironment[]): ModelEnvironment[] {
    environments.sort(env => (env.isSynchronized ? -1 : 1))

    return environments
  }
}
