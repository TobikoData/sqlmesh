import { isStringEmptyOrNil, toUniqueName } from '@utils/index'
import { type ModelDirectory } from './directory'
import { ModelInitial } from './initial'

export interface InitialArtifact {
  name: string
  path: string
}

export class ModelArtifact<
  T extends InitialArtifact = InitialArtifact,
> extends ModelInitial<T> {
  private _path: string
  private _name: string

  parent: ModelDirectory | undefined
  remove: boolean = false

  constructor(initial?: T | ModelArtifact, parent?: ModelDirectory) {
    super(
      (initial as ModelArtifact<T>)?.isModel
        ? (initial as ModelArtifact<T>).initial
        : {
            ...(initial as T),
            name: initial?.name ?? '',
            path: initial?.path ?? '',
          },
    )

    this._path = initial?.path ?? this.initial.path
    this._name = initial?.name ?? this.initial.name

    this.parent = parent
  }

  get id(): ID {
    return isStringEmptyOrNil(this.path) ? this.initial.id : this.path
  }

  get name(): string {
    return this._name
  }

  get path(): string {
    return this.toPath(this.name, this._path)
  }

  get isUntitled(): boolean {
    return this.name === ''
  }

  get isLocal(): boolean {
    return this.path === ''
  }

  get isRemote(): boolean {
    return this.path !== ''
  }

  get withParent(): boolean {
    return Boolean(this.parent?.isModel)
  }

  copyName(): string {
    return `Copy of ${this.name}__${toUniqueName()}`
  }

  rename(newName: string): void {
    if (this.isRemote) {
      this._path = this.toPath(newName, this._path.replace(this.name, newName))
    }

    this._name = newName
  }

  private toPath(name: string, fallback: string = ''): string {
    return ModelArtifact.toPath(
      this.withParent ? `${this.parent?.path ?? ''}/${name}` : fallback,
    )
  }

  static toPath(...paths: string[]): string {
    return paths.flatMap(path => path.split('/').filter(Boolean)).join('/')
  }
}
