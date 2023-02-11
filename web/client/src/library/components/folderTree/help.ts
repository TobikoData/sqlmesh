import { ModelDirectory, ModelFile } from '../../../models'

export function getAllFilesInDirectory(directory: ModelDirectory): ModelFile[] {
  const files = directory.files ?? []
  const directories = directory.directories ?? []

  return [...files, ...directories.map(getAllFilesInDirectory).flat()]
}

class Counter {
  private readonly store = new Map<ModelDirectory, number>()

  countByKey(key: ModelDirectory): number {
    const count = (this.store.get(key) ?? 0) + 1

    this.store.set(key, count)

    return count
  }
}

export const counter = new Counter()
