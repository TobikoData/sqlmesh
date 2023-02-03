import type { File } from '../api/client';
import { ModelDirectory } from './directory';
import { ModelInitial } from './initial';
import { Artifact } from './types';

interface InitialFile extends File {
  content: string;
  extension: string;
  is_supported: boolean;
}

export class ModelFile extends ModelInitial<InitialFile> implements Artifact {
  private _path: string;

  name: string;
  content: string;
  extension: string;
  is_supported: boolean;
  parent: ModelDirectory | undefined;

  constructor(initial?: File | ModelFile, parent?: ModelDirectory) {
    super((initial as ModelFile)?.isModel ? (initial as ModelFile).initial :{
      extension: initial?.extension || '.sql',
      name: initial?.name || '',
      is_supported: initial?.is_supported || true,
      path: initial?.path || '',
      content: initial?.content || '',
    })

    this._path = this.initial.path;
    this.name = this.initial.name;
    this.extension = this.initial.extension;
    this.is_supported = this.initial.is_supported;
    this.content = this.initial.content;
    this.parent = parent;
  }

  get id(): string | number {
    return this.path || this.initial.id;
  }

  get path(): string {
    return this._path;
  }

  get isUntitled(): boolean {
    return this.name === '';
  }

  get isLocal(): boolean {
    return this.path === '';
  }

  get isEmpty(): boolean {
    return this.content === '';
  }

  get isSupported(): boolean {
    return this.is_supported;
  }

  get isChanged(): boolean {
    return this.content !== this.initial.content;
  }

  get withParent(): boolean {
    return Boolean(this.parent?.isModel);
  }

  rename(newName: string): void {
    this.name = newName;

    if (!this.isLocal && this.parent) {
      this._path = this.parent.path + `${this.name}`
    }
  }
}