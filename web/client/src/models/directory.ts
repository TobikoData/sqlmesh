import type { Directory, File } from '../api/client'
import { type InitialArtifact, ModelArtifact } from './artifact'
import { ModelFile } from './file'

interface InitialDirectory extends InitialArtifact, Directory {
  files: File[]
  directories: Directory[]
  syncStateOpen?: <T>(state: T) => void
}

export class ModelDirectory extends ModelArtifact<InitialDirectory> {
  private _isOpen = false

  directories: ModelDirectory[]
  files: ModelFile[]

  syncStateOpen?: (state: boolean) => void

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

  get allDirectories(): ModelDirectory[] {
    return this.directories.concat(
      this.directories.map(d => d.allDirectories).flat(),
    )
  }

  get allFiles(): ModelFile[] {
    return this.files.concat(
      this.allDirectories.map(directory => directory.files).flat(),
    )
  }

  get allArtifacts(): ModelArtifact[] {
    return ([] as ModelArtifact[])
      .concat(this.allFiles)
      .concat(this.allDirectories)
  }

  get isOpen(): boolean {
    return this._isOpen
  }

  get isExpanded(): boolean {
    return this.isOpen && this.allDirectories.every(d => d.isExpanded)
  }

  get isCollapsed(): boolean {
    return !this.isOpen && this.allDirectories.every(d => d.isCollapsed)
  }

  get isModels(): boolean {
    return this.path.startsWith('models')
  }

  open(): void {
    this._isOpen = true

    this.syncStateOpen?.(this.isOpen)
  }

  close(): void {
    this._isOpen = false

    this.syncStateOpen?.(this.isOpen)
  }

  toggle(): void {
    this.isOpen ? this.close() : this.open()
  }

  expand(): void {
    this.open()
    this.directories.forEach(directory => {
      directory.expand()
    })
  }

  collapse(): void {
    this.close()
    this.directories.forEach(directory => {
      directory.collapse()
    })
  }

  hasFile(file: ModelFile): boolean {
    return this.allFiles.some(f => f.id === file.id)
  }

  addFile(file: ModelFile): void {
    this.files.push(file)
  }

  addDirectory(directory: ModelDirectory): void {
    this.directories.push(directory)
  }

  removeFile(file: ModelFile): void {
    this.files = this.files.filter(f => f !== file)
  }

  removeDirectory(directory: ModelDirectory): void {
    this.directories = this.directories.filter(d => d !== directory)
  }
}
