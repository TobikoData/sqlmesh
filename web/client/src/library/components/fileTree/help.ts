import { type ModelDirectory, type ModelFile } from '../../../models'

export function getAllFilesInDirectory(directory: ModelDirectory): ModelFile[] {
  const files = directory.files ?? []
  const directories = directory.directories ?? []

  return [...files, ...directories.map(getAllFilesInDirectory).flat()]
}

export function toUniqueName(prefix?: string, suffix?: string): string {
  // Should be enough for now
  const hex = (Date.now() % 100000).toString(16)

  return `${prefix == null ? '' : `${prefix}_`}${hex}${
    suffix ?? ''
  }`.toLowerCase()
}
