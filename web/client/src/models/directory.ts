import { isFalse } from '@utils/index'
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

  directories: ModelDirectory[] = []
  files: ModelFile[] = []

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
      this.update({
        files: this.initial.files,
        directories: this.initial.directories,
      })
    }
  }

  get level(): number {
    return (this.parent?.level ?? -1) + 1
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

  get artifacts(): ModelArtifact[] {
    return (this.directories as ModelArtifact[]).concat(this.files)
  }

  get allDirectories(): ModelDirectory[] {
    return this.directories.concat(
      this.directories.map(d => d.allDirectories).flat(100),
    )
  }

  get allFiles(): ModelFile[] {
    return this.files.concat(
      this.allDirectories.map(directory => directory.files).flat(100),
    )
  }

  get allVisibleArtifacts(): ModelArtifact[] {
    return this.directories
      .map(d => (d.isOpened ? [d, d.allVisibleArtifacts] : [d]))
      .flat(100)
      .concat(this.isOpened ? this.files : [])
  }

  get allArtifacts(): ModelArtifact[] {
    return this.directories
      .map(d => [d, d.allArtifacts])
      .flat(100)
      .concat(this.files)
  }

  get isOpened(): boolean {
    return this._isOpen
  }

  get isClosed(): boolean {
    return isFalse(this._isOpen)
  }

  get isModels(): boolean {
    return this.path.startsWith('models')
  }

  get isTests(): boolean {
    return this.path.startsWith('tests')
  }

  open(): void {
    this._isOpen = true

    this.syncStateOpen?.(this.isOpened)
  }

  close(): void {
    this._isOpen = false

    this.syncStateOpen?.(this.isOpened)
  }

  toggle(): void {
    this.isOpened ? this.close() : this.open()
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

  containsName(name: string): boolean {
    return this.artifacts.some(artifact => artifact.name === name)
  }

  hasFile(file: ModelFile): boolean {
    return this.allFiles.includes(file)
  }

  hasDirectory(directory: ModelDirectory): boolean {
    return this.allDirectories.includes(directory)
  }

  addFile(file: ModelFile): void {
    this.files.push(file)

    file.parent = this
  }

  addDirectory(directory: ModelDirectory): void {
    this.directories.push(directory)

    directory.parent = this
  }

  removeFile(file: ModelFile): void {
    this.files = this.files.filter(f => f !== file)
  }

  removeDirectory(directory: ModelDirectory): void {
    this.directories = this.directories.filter(d => d !== directory)
  }

  update({
    files = [],
    directories = [],
  }: {
    files: File[] | ModelFile[]
    directories: Directory[] | ModelDirectory[]
  }): void {
    this.files = []
    this.directories = []

    directories.forEach(d => {
      this.addDirectory(new ModelDirectory(d, this))
    })
    files.forEach(f => {
      this.addFile(new ModelFile(f, this))
    })
  }
}
