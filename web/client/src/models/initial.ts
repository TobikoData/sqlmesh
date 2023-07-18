import { isNotNil, uid } from '@utils/index'

type Initial<T extends object> = T & { id?: ID }
type InitialWithId<T extends object> = T & { id: ID }

export class ModelInitial<T extends object = any> {
  private readonly _initial: InitialWithId<T>

  isModel = true

  constructor(initial: Initial<T> | InitialWithId<T>) {
    this._initial = isNotNil(initial?.id)
      ? (initial as InitialWithId<T>)
      : new Proxy<InitialWithId<T>>(
          Object.assign(initial ?? {}, {
            id: uid(),
          }),
          {
            set() {
              throw new Error('Cannot change initial file')
            },
          },
        )
  }

  get initial(): InitialWithId<T> {
    return this._initial
  }

  get id(): ID {
    return this.initial.id
  }
}
