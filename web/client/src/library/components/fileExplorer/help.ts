import { type ModelDirectory, type ModelFile } from '../../../models'

export function getAllFilesInDirectory(directory: ModelDirectory): ModelFile[] {
  const files = directory.files ?? []
  const directories = directory.directories ?? []

  return [...files, ...directories.map(getAllFilesInDirectory).flat()]
}
