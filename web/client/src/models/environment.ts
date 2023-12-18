import { type Environment } from '~/api/client'
import useLocalStorage from '~/hooks/useLocalStorage'
import {
  isArrayEmpty,
  isFalse,
  isNil,
  isNotNil,
  isStringEmptyOrNil,
} from '~/utils'

export const EnumDefaultEnvironment = {
  Empty: '',
  Prod: 'prod',
} as const

export const EnumRelativeLocation = {
  Local: 'local',
  Remote: 'remote',
} as const

export type EnvironmentName = DefaultEnvironment | string
export type DefaultEnvironment = KeyOf<typeof EnumDefaultEnvironment>
export type RelativeLocation = KeyOf<typeof EnumRelativeLocation>

interface InitialEnvironmemt extends Partial<Environment> {
  name?: EnvironmentName
}

interface ProfileEnvironment {
  name: EnvironmentName
  plan_id: string
  type: RelativeLocation
  createFrom: EnvironmentName
  isPinned: boolean
  isDefault: boolean
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

  _isPinned = false
  _isDefault = false
  isModel = true

  constructor(
    initial: InitialEnvironmemt,
    type: RelativeLocation,
    createFrom: EnvironmentName = EnumDefaultEnvironment.Prod,
    isPinned = false,
    isDefault = false,
  ) {
    this._initial = initial
    this._type = type ?? EnumRelativeLocation.Local
    this._createFrom = this._isDefault
      ? EnumDefaultEnvironment.Empty
      : createFrom
    this._isPinned = isPinned
    this._isDefault = isDefault
  }

  get id(): string {
    return this._initial.plan_id ?? ''
  }

  get name(): string {
    return this._initial.name ?? EnumDefaultEnvironment.Empty
  }

  get type(): RelativeLocation {
    return this._type
  }

  get createFrom(): string {
    return this._createFrom
  }

  get isProd(): boolean {
    return this.name === EnumDefaultEnvironment.Prod
  }

  get isDefault(): boolean {
    return this._isDefault
  }

  set isDefault(isDefault: boolean) {
    this._isDefault = isDefault
  }

  get isPinned(): boolean {
    return this._isPinned || this.isDefault || this.isProd
  }

  set isPinned(isPinned: boolean) {
    this._isPinned = isPinned
  }

  get isInitial(): boolean {
    return isStringEmptyOrNil(this._initial?.plan_id)
  }

  get isLocal(): boolean {
    return this._type === EnumRelativeLocation.Local
  }

  get isRemote(): boolean {
    return this._type === EnumRelativeLocation.Remote
  }

  get isSyncronized(): boolean {
    return this.isRemote && isFalse(this.isInitial)
  }

  get isInitialProd(): boolean {
    return this.isInitial && this.isProd
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
        plan_id: environment.id,
        type: environment.type,
        createFrom: environment.createFrom,
        isPinned: environment.isPinned,
        isDefault: environment.isDefault,
      }
    }

    if (isNotNil(environments)) {
      output.environments = ModelEnvironment.getOnlyLocal(environments).map(
        env => ({
          name: env.name,
          plan_id: env.id,
          type: env.type,
          createFrom: env.createFrom,
          isPinned: env.isPinned,
          isDefault: env.isDefault,
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

  static getOnlyRemote(envs: ModelEnvironment[] = []): ModelEnvironment[] {
    return envs.filter(
      env => isFalse(isStringEmptyOrNil(env.name)) && env.isRemote,
    )
  }

  static getEnvironment(): Optional<ModelEnvironment> {
    const { environment } = getProfile() ?? {}

    if (isNil(environment)) return

    const { name, plan_id, type, createFrom, isPinned, isDefault } = environment

    return new ModelEnvironment(
      { name, plan_id },
      type,
      createFrom,
      isPinned,
      isDefault,
    )
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
          { name, plan_id: environment.plan_id },
          environment.type,
          environment.createFrom,
          environment.isPinned,
          environment.isDefault,
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
    environments.sort(env => (env.isRemote ? -1 : 1))

    return environments
  }
}
