import type { File, FileType } from '../api/client'
import { type ModelDirectory } from './directory'
import { type InitialArtifact, ModelArtifact } from './artifact'

export interface InitialFile extends InitialArtifact, File {
  content: string
  extension: string
  is_supported: boolean
}

export class ModelFile extends ModelArtifact<InitialFile> {
  private _content: string = ''

  content: string
  extension: string
  is_supported: boolean
  type?: FileType

  constructor(initial?: File | ModelFile, parent?: ModelDirectory) {
    super(
      (initial as ModelFile)?.isModel
        ? (initial as ModelFile).initial
        : {
            ...(initial as File),
            extension: initial?.extension ?? '.sql',
            is_supported: initial?.is_supported ?? true,
            content: initial?.content ?? '',
          },
      parent,
    )

    this.extension = initial?.extension ?? this.initial.extension
    this.is_supported = initial?.is_supported ?? this.initial.is_supported
    this._content = this.content = initial?.content ?? this.initial.content
    this.type = initial?.type
  }

  get isEmpty(): boolean {
    return this.content === ''
  }

  get isSupported(): boolean {
    return this.is_supported
  }

  get isChanged(): boolean {
    return this.content !== this._content
  }

  get isSQLMeshModel(): boolean {
    return this.type === 'model'
  }

  updateContent(newContent: string = ''): void {
    this._content = newContent
    this.content = newContent
  }
}
