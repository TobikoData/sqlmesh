import type { Directory, File } from '../api/client';
import { ModelFile } from './file';
import { ModelInitial } from './initial';
import { Artifact } from './types';

interface InitialDirectory extends Directory {
  files: File[];
  directories: Directory[];
}

export class ModelDirectory extends ModelInitial<InitialDirectory> implements Artifact {
  private _path: string;

  name: string;
  directories: ModelDirectory[];
  files: ModelFile[];
  parent: ModelDirectory | undefined;

  constructor(initial?: Directory | ModelDirectory, parent?: ModelDirectory) {
    super((initial as ModelDirectory)?.isModel ? (initial as ModelDirectory).initial : {
      name: initial?.name || '',
      path: initial?.path || '',
      directories: initial?.directories || [],
      files: initial?.files || [],
    })

    this._path = this.initial.path;
    this.parent = parent;
    this.name = this.initial.name;

    if ((initial as ModelDirectory)?.isModel) {
      this.directories = (initial as ModelDirectory).directories
      this.files = (initial as ModelDirectory).files
    } else {
      this.directories = this.initial.directories?.map(d => new ModelDirectory(d, this)) || [];
      this.files = this.initial.files?.map(f => new ModelFile(f, this)) || [];
    }
  }

  get id(): string | number {
    return this.path || this.initial.id;
  }

  get path(): string {
    return this._path;
  }

  get isChanged(): boolean {
    return this.name !== this.initial.name || this.files.some(f => f.isChanged) || this.directories.some(d => d.isChanged);
  }

  get isLocal(): boolean {
    return this.path === '';
  }

  get isEmpty(): boolean {
    return this.directories.length === 0 && this.files.length === 0;
  }

  get withParent(): boolean {
    return Boolean(this.parent?.isModel);
  }

  addFile(file: ModelFile): void {
    this.files.push(file);
  }
  
  addDirectory(directory: ModelDirectory): void {
    this.directories.push(directory);
  }

  removeFile(file: ModelFile): void {
    this.files = this.files.filter(f => f !== file);
  }

  remiveDirectory(directory: ModelDirectory): void {
    this.directories = this.directories.filter(d => d !== directory);
  }

  rename(newName: string): void {
    this.name = newName;

    if (!this.isLocal && this.parent) {
      this._path = this.parent.path + `${this.name}`
    }
  }
}