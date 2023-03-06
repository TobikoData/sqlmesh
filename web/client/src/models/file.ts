import type { File } from '../api/client'
import { ModelDirectory } from './directory'
import { InitialArtifact, ModelArtifact } from './artifact'

interface InitialFile extends InitialArtifact, File {
  content: string
  extension: string
  is_supported: boolean
}

export class ModelFile extends ModelArtifact<InitialFile> {
  content: string
  extension: string
  is_supported: boolean

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
    this.content = initial?.content ?? this.initial.content
  }

  get isEmpty(): boolean {
    return this.content === ''
  }

  get isSupported(): boolean {
    return this.is_supported
  }

  get isChanged(): boolean {
    return this.content !== this.initial.content
  }

  get isSQLMeshModel(): boolean {
    return (
      ['.sql', '.py'].includes(this.extension) &&
      this.path.startsWith('models/')
    )
  }

  get isSQLMeshSeed(): boolean {
    return ['.csv'].includes(this.extension) && this.path.startsWith('seeds/')
  }
}
