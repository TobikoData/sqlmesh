import { type Status, type File } from '../api/client'
import { type ModelDirectory } from './directory'
import { type InitialArtifact, ModelArtifact } from './artifact'
import {
  ensureString,
  isFalse,
  isStringEmpty,
  isStringNotEmpty,
  toUniqueName,
} from '@utils/index'

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
    super(ModelFile.getInitialArtifact(initial), parent)

    this.content = this._content = ensureString(
      initial?.content ?? this.initial.content,
    )
    this.extension = ensureString(
      initial?.extension ?? this.initial.extension,
    ) as FileExtensions
    this.isFormatted = (initial as ModelFile)?.isFormatted
  }

  get basename(): string {
    return this.name.replace(this.extension, '')
  }

  get isEmpty(): boolean {
    return isStringEmpty(this.content)
  }

  get isChanged(): boolean {
    return isStringNotEmpty(this._content) && this.content !== this._content
  }

  get isSQL(): boolean {
    return this.extension === EnumFileExtensions.SQL
  }

  get fingerprint(): string {
    return `${this._content}${this.name}${this.path}`
  }

  removeChanges(): void {
    this.content = this._content
  }

  copyName(): string {
    return `Copy of ${ensureString(
      this.name.split(this.extension)[0],
    )}__${toUniqueName()}${this.extension}`
  }

  update(newFile?: File): void {
    const content = ensureString(newFile?.content)
    // When modifying a file locally, we only modify the content.
    // Therefore, if we have content but the variable "_content" is empty,
    // it is likely because we restored the file content from localStorage.
    // After updating "_content", we still want to retain the content
    // because it is unsaved changes.
    if (isStringEmpty(this._content) || isFalse(this.isChanged)) {
      this.content = content
    }

    this._content = content
    this.extension = ensureString(newFile?.extension) as FileExtensions
  }

  private static getInitialArtifact(initial?: File | ModelFile): InitialFile {
    return ModelFile.isModelFile(initial)
      ? initial.initial
      : {
          name: ensureString(initial?.name),
          path: ensureString(initial?.path),
          extension: initial?.extension ?? EnumFileExtensions.SQL,
          content: ensureString(initial?.content),
        }
  }

  static isModelFile(file: any): file is ModelFile {
    return file instanceof ModelFile
  }
}
