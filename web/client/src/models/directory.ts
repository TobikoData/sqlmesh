import type { Directory, File } from '../api/client'
import { InitialArtifact, ModelArtifact } from './artifact'
import { ModelFile } from './file'

interface InitialDirectory extends InitialArtifact, Directory {
  files: File[]
  directories: Directory[]
}

export class ModelDirectory extends ModelArtifact<InitialDirectory> {
  directories: ModelDirectory[]
  files: ModelFile[]

  constructor(initial?: Directory | ModelDirectory, parent?: ModelDirectory) {
    super(
      (initial as ModelDirectory)?.isModel
        ? (initial as ModelDirectory).initial
        : {
            ...(initial as Directory),
            directories: initial?.directories ?? [],
            files: initial?.files ?? [],
          },
      parent,
    )

    if ((initial as ModelDirectory)?.isModel) {
      this.directories = (initial as ModelDirectory).directories
      this.files = (initial as ModelDirectory).files
    } else {
      this.directories = this.initial.directories?.map(
        d => new ModelDirectory(d, this),
      )
      this.files = this.initial.files?.map(f => new ModelFile(f, this))
    }
  }

  get isChanged(): boolean {
    return (
      this.name !== this.initial.name ||
      this.files.some(f => f.isChanged) ||
      this.directories.some(d => d.isChanged)
    )
  }

  get isEmpty(): boolean {
    return this.directories.length === 0 && this.files.length === 0
  }

  get withFiles(): boolean {
    return this.files.length > 0
  }

  get withDirectories(): boolean {
    return this.directories.length > 0
  }

  get isNotEmpty(): boolean {
    return this.withFiles || this.withDirectories
  }

  addFile(file: File): void {
    this.files.push(new ModelFile(file, this))
  }

  addDirectory(directory: Directory): void {
    this.directories.push(new ModelDirectory(directory, this))
  }

  removeFile(file: ModelFile): void {
    this.files = this.files.filter(f => f !== file)
  }

  removeDirectory(directory: ModelDirectory): void {
    this.directories = this.directories.filter(d => d !== directory)
  }
}
