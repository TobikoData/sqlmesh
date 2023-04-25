import { type File, FileType } from '../api/client'
import { type ModelDirectory } from './directory'
import { type InitialArtifact, ModelArtifact } from './artifact'
import { isStringEmptyOrNil } from '@utils/index'

export const EnumFileExtensions = {
  SQL: '.sql',
  Python: '.py',
  CSV: '.csv',
  YAML: '.yaml',
} as const

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
            extension: initial?.extension ?? EnumFileExtensions.SQL,
            is_supported: initial?.is_supported ?? true,
            content: initial?.content ?? '',
          },
      parent,
    )

    this.extension = initial?.extension ?? this.initial.extension
    this.is_supported = initial?.is_supported ?? this.initial.is_supported
    this._content = this.content = initial?.content ?? this.initial.content
    this.type = initial?.type ?? getFileType(initial?.path)
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

  get fingerprint(): string {
    return this._content
  }

  get isSQLMeshModelPython(): boolean {
    return this.isSQLMeshModel && this.extension === EnumFileExtensions.Python
  }

  get isSQLMeshModelSQL(): boolean {
    return this.isSQLMeshModel && this.extension === EnumFileExtensions.SQL
  }

  updateContent(newContent: string = ''): void {
    this._content = newContent
    this.content = newContent
  }
}

function getFileType(path?: string): FileType | undefined {
  if (path == null || isStringEmptyOrNil(path)) return

  if (path.startsWith('models')) return FileType.model
}
