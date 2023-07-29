import { type File, FileType } from '../api/client'
import { type ModelDirectory } from './directory'
import { type InitialArtifact, ModelArtifact } from './artifact'
import { isFalse, isStringEmptyOrNil, toUniqueName } from '@utils/index'

export const EnumFileExtensions = {
  SQL: '.sql',
  PY: '.py',
  CSV: '.csv',
  YAML: '.yaml',
  YML: '.yml',
  None: '',
} as const
export type FileExtensions =
  (typeof EnumFileExtensions)[keyof typeof EnumFileExtensions]
export interface InitialFile extends InitialArtifact, File {
  content: string
  extension: string
  is_supported: boolean
}

export class ModelFile extends ModelArtifact<InitialFile> {
  private _content: string = ''
  private _type?: FileType

  content: string
  is_supported: boolean
  extension: FileExtensions

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

    this.extension =
      ((initial?.extension ?? this.initial.extension) as FileExtensions) ?? ''
    this.is_supported =
      initial?.is_supported ?? this.initial.is_supported ?? false
    this._content = this.content =
      initial?.content ?? this.initial.content ?? ''
    this.type = initial?.type
  }

  get type(): FileType | undefined {
    return this._type
  }

  set type(newType: FileType | undefined) {
    this._type = newType ?? getFileType(this.path)
  }

  get isSynced(): boolean {
    return isFalse(isStringEmptyOrNil(this._content))
  }

  get isEmpty(): boolean {
    return isStringEmptyOrNil(this.content)
  }

  get isSupported(): boolean {
    return this.is_supported
  }

  get isChanged(): boolean {
    return this.content !== this._content
  }

  get isSQLMeshModel(): boolean {
    return this.is_supported && this.type === 'model'
  }

  get fingerprint(): string {
    return this._content + this.name + this.path
  }

  get isSQLMeshModelPython(): boolean {
    return this.isSQLMeshModel && this.extension === EnumFileExtensions.PY
  }

  get isSQLMeshModelSQL(): boolean {
    return this.isSQLMeshModel && this.extension === EnumFileExtensions.SQL
  }

  copyName(): string {
    return `Copy of ${
      this.name.split(this.extension)[0] ?? ''
    }__${toUniqueName()}${this.extension}`
  }

  updateContent(newContent: string = ''): void {
    // When modifying a file locally, we only modify the content.
    // Therefore, if we have content but the variable "_content" is empty,
    // it is likely because we restored the file content from localStorage.
    // After updating "_content", we still want to retain the content
    // because it is unsaved changes.
    if (this.isSynced || isFalse(this.isChanged)) {
      this.content = newContent
    }

    this._content = newContent
  }

  update(newFile?: File): void {
    if (newFile == null) {
      this.updateContent('')
    } else {
      this.is_supported = newFile.is_supported ?? false
      this.extension =
        (newFile.extension as FileExtensions) ?? EnumFileExtensions.None
      this.type = newFile.type as FileType

      this.updateContent(newFile.content)
    }
  }
}

function getFileType(path?: string): FileType | undefined {
  if (path == null || isStringEmptyOrNil(path)) return

  if (path.startsWith('models')) return FileType.model
}
