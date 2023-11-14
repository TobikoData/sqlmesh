import { describe, test, expect, vi } from 'vitest'
import { ModelDirectory } from './directory'

vi.mock('../utils/index', async () => {
  const actual: any = await vi.importActual('../utils/index')

  return {
    ...actual,
    PATH_SEPARATOR: '/',
  }
})

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

  test('should find parent by path', () => {
    const directory = new ModelDirectory(getFakePayloadDirectoryWithChildren())

    let found = ModelDirectory.findParentByPath(directory, '')

    expect(found).toBe(directory)
    expect(found?.path).toBe('')
    expect(found?.level).toBe(0)

    found = ModelDirectory.findParentByPath(directory, 'test/folder_1')

    expect(found).toBe(directory.directories[0]?.directories[0])
    expect(found?.path).toBe('test/folder_1')
    expect(found?.level).toBe(2)
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
