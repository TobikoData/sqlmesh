let counter = 0;

type Initial<T extends object> = T & { id?: ID };
type InitialWithId<T extends object> = T & { id: ID };

export class ModelInitial<T extends object = any> {
  private readonly _initial: InitialWithId<T>;

  constructor(initial: Initial<T> | InitialWithId<T>) {
    this._initial = (initial as InitialWithId<T>).id
      ? (initial as InitialWithId<T>)
      : new Proxy<InitialWithId<T>>(Object.assign((initial || {}), {
        id: ++counter
      }), {
        set() {
          throw new Error('Cannot change initial file');
        },
      });
  }

  get initial(): InitialWithId<T> {
    return this._initial;
  }

  get isModel(): boolean {
    return true;
  }

  get id(): ID {
    return this.initial.id;
  }
} 
