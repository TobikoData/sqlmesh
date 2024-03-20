import { describe, test, expect } from 'vitest'
import { ModelDirectory } from './directory'

describe('Model Directory', () => {
  test('should create directory with nested atrifacts', () => {
    const directory = new ModelDirectory(getFakePayloadDirectoryWithChildren())

    expect(directory.id).toBeTruthy()
    expect(directory.level).toBe(0)
    expect(directory.name).toBe('project')
    expect(directory.path).toBe('')
    expect(directory.directories.length).toBe(1)
    expect(directory.allDirectories.length).toBe(4)
    expect(directory.allArtifacts.length).toBe(4)

    const folder1 = ModelDirectory.findArtifactByPath(
      directory,
      'test/folder_1',
    ) as ModelDirectory

    expect(folder1).toBeTruthy()
    expect(folder1?.level).toBe(2)

    const folder3 = ModelDirectory.findArtifactByPath(
      directory,
      'test/folder_1/folder_2/folder_3',
    ) as ModelDirectory

    expect(folder3).toBeTruthy()
    expect(folder3?.level).toBe(4)
  })
})

function getFakePayloadDirectoryWithChildren(): any {
  return {
    path: '',
    name: 'project',
    directories: [
      {
        name: 'test',
        path: 'test',
        directories: [
          {
            name: 'folder_1',
            path: 'test/folder_1',
            directories: [
              {
                name: 'folder_2',
                path: 'test/folder_1/folder_2',
                directories: [
                  {
                    name: 'folder_3',
                    path: 'test/folder_1/folder_2/folder_3',
                  },
                ],
              },
            ],
          },
        ],
      },
    ],
  }
}
