import { type Status, type File } from '../api/client'
import { type ModelDirectory } from './directory'
import { type InitialArtifact, ModelArtifact } from './artifact'
import { isFalse, isNil, isStringEmptyOrNil, toUniqueName } from '@utils/index'

export interface FormatFileStatus {
  path: string
  status: Status
}

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
  isFormatted?: boolean
  content: string
  extension: string
}

export class ModelFile extends ModelArtifact<InitialFile> {
  private _content: string = ''

  content: string
  extension: FileExtensions
  // Undefined means we don't know if it's formatted or not.
  isFormatted?: boolean

  constructor(initial?: File | ModelFile, parent?: ModelDirectory) {
    super(
      (initial as ModelFile)?.isModel
        ? (initial as ModelFile).initial
        : {
            ...(initial as File),
            extension: initial?.extension ?? EnumFileExtensions.SQL,
            content: initial?.content ?? '',
          },
      parent,
    )

    this.extension =
      ((initial?.extension ?? this.initial.extension) as FileExtensions) ?? ''
    this._content = this.content =
      initial?.content ?? this.initial.content ?? ''
    this.isFormatted = (initial as ModelFile)?.isFormatted
  }

  get basename(): string {
    return this.name.replace(this.extension, '')
  }

  get isSynced(): boolean {
    return isFalse(isStringEmptyOrNil(this._content))
  }

  get isEmpty(): boolean {
    return isStringEmptyOrNil(this.content)
  }

  get isChanged(): boolean {
    return this.content !== this._content
  }

  get isSQL(): boolean {
    return this.extension === EnumFileExtensions.SQL
  }

  get fingerprint(): string {
    return this._content + this.name + this.path
  }

  removeChanges(): void {
    this.content = this._content
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
    if (isNil(newFile)) {
      this.updateContent('')
    } else {
      this.extension =
        (newFile.extension as FileExtensions) ?? EnumFileExtensions.None

      this.updateContent(newFile.content ?? '')
    }
  }
}
