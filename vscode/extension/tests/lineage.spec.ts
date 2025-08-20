import { test, Page } from './fixtures'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import {
  openLineageView,
  openServerPage,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import { writeFileSync } from 'fs'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

/**
 * Helper function to launch VS Code and test lineage with given project path config
 */
async function testLineageWithProjectPath(page: Page): Promise<void> {
  await openLineageView(page)
  await waitForLoadedSQLMesh(page)
}

test('Lineage panel renders correctly - no project path config (default)', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  await createPythonInterpreterSettingsSpecifier(tempDir)

  await openServerPage(page, tempDir, sharedCodeServer)
  await testLineageWithProjectPath(page)
})

test('Lineage panel renders correctly - relative project path', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  const projectDir = path.join(tempDir, 'projects', 'sushi')
  await fs.copy(SUSHI_SOURCE_PATH, projectDir)

  const settings = {
    'sqlmesh.projectPaths': ['./projects/sushi'],
    'python.defaultInterpreterPath': sharedCodeServer.defaultPythonInterpreter,
  }
  await fs.ensureDir(path.join(tempDir, '.vscode'))
  await fs.writeJson(path.join(tempDir, '.vscode', 'settings.json'), settings, {
    spaces: 2,
  })

  try {
    await openServerPage(page, tempDir, sharedCodeServer)
    await testLineageWithProjectPath(page)
  } finally {
    await fs.remove(tempDir)
  }
})

test('Lineage panel renders correctly - absolute project path', async ({
  page,
  tempDir,
  sharedCodeServer,
}) => {
  // Copy the sushi project to temporary directory
  const projectDir = path.join(tempDir, 'projects', 'sushi')
  await fs.ensureDir(path.join(tempDir, '.vscode'))
  await fs.copy(SUSHI_SOURCE_PATH, projectDir)

  const settings = {
    'sqlmesh.projectPaths': [projectDir],
    'python.defaultInterpreterPath': sharedCodeServer.defaultPythonInterpreter,
  }
  await fs.writeJson(path.join(tempDir, '.vscode', 'settings.json'), settings, {
    spaces: 2,
  })

  await openServerPage(page, tempDir, sharedCodeServer)
  await testLineageWithProjectPath(page)
})

test('Lineage panel renders correctly - relative project outside of workspace', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  const projectDir = path.join(tempDir, 'projects', 'sushi')
  await fs.copy(SUSHI_SOURCE_PATH, projectDir)

  const workspaceDir = path.join(tempDir, 'workspace')
  await fs.ensureDir(workspaceDir)

  const settings = {
    'sqlmesh.projectPaths': ['./../projects/sushi'],
    'python.defaultInterpreterPath': sharedCodeServer.defaultPythonInterpreter,
  }
  await fs.ensureDir(path.join(workspaceDir, '.vscode'))
  await fs.writeJson(
    path.join(workspaceDir, '.vscode', 'settings.json'),
    settings,
    { spaces: 2 },
  )
  await openServerPage(page, workspaceDir, sharedCodeServer)
  await testLineageWithProjectPath(page)
})

test('Lineage panel renders correctly - absolute path project outside of workspace', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  const projectDir = path.join(tempDir, 'projects', 'sushi')
  await fs.copy(SUSHI_SOURCE_PATH, projectDir)

  const workspaceDir = path.join(tempDir, 'workspace')
  await fs.ensureDir(workspaceDir)

  const settings = {
    'sqlmesh.projectPaths': [projectDir],
    'python.defaultInterpreterPath': sharedCodeServer.defaultPythonInterpreter,
  }
  await fs.ensureDir(path.join(workspaceDir, '.vscode'))
  await fs.writeJson(
    path.join(workspaceDir, '.vscode', 'settings.json'),
    settings,
    { spaces: 2 },
  )

  await openServerPage(page, workspaceDir, sharedCodeServer)
  await testLineageWithProjectPath(page)
})

// These work on local machine when debuggin but not on CI, so skipping for now
test('Lineage panel renders correctly - multiworkspace setup', async ({
  page,
  sharedCodeServer,
}) => {
  const workspaceDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-workspace-'),
  )
  const projectDir1 = path.join(workspaceDir, 'projects', 'sushi1')
  const projectDir2 = path.join(workspaceDir, 'projects', 'sushi2')
  await fs.copy(SUSHI_SOURCE_PATH, projectDir1)
  await fs.ensureDir(projectDir2)

  // Add a .code-workspace file with multiple projects
  const workspaceFilePath = path.join(
    workspaceDir,
    'multi-workspace.code-workspace',
  )
  writeFileSync(
    workspaceFilePath,
    JSON.stringify({
      folders: [
        {
          name: 'sushi1',
          path: 'projects/sushi1',
        },
        {
          name: 'sushi2',
          path: 'projects/sushi2',
        },
      ],
    }),
  )

  const settings = {
    'python.defaultInterpreterPath': sharedCodeServer.defaultPythonInterpreter,
  }
  await fs.ensureDir(path.join(projectDir1, '.vscode'))
  await fs.writeJson(
    path.join(projectDir1, '.vscode', 'settings.json'),
    settings,
    { spaces: 2 },
  )

  await openServerPage(page, workspaceFilePath, sharedCodeServer)
  await page.reload()
  await testLineageWithProjectPath(page)
})

test('Lineage panel renders correctly - multiworkspace setup reversed', async ({
  page,
  sharedCodeServer,
}) => {
  const workspaceDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-workspace-'),
  )
  const projectDir1 = path.join(workspaceDir, 'projects', 'sushi1')
  const projectDir2 = path.join(workspaceDir, 'projects', 'sushi2')
  await fs.copy(SUSHI_SOURCE_PATH, projectDir2)
  await fs.ensureDir(projectDir1)

  // Add a .code-workspace file with multiple projects
  const workspaceFilePath = path.join(
    workspaceDir,
    'multi-workspace.code-workspace',
  )
  writeFileSync(
    workspaceFilePath,
    JSON.stringify({
      folders: [
        {
          name: 'sushi1',
          path: 'projects/sushi1',
        },
        {
          name: 'sushi2',
          path: 'projects/sushi2',
        },
      ],
    }),
  )

  const settings = {
    'python.defaultInterpreterPath': sharedCodeServer.defaultPythonInterpreter,
  }
  await fs.ensureDir(path.join(projectDir1, '.vscode'))
  await fs.writeJson(
    path.join(projectDir1, '.vscode', 'settings.json'),
    settings,
    { spaces: 2 },
  )
  await fs.ensureDir(path.join(projectDir2, '.vscode'))
  await fs.writeJson(
    path.join(projectDir2, '.vscode', 'settings.json'),
    settings,
    { spaces: 2 },
  )

  await openServerPage(page, workspaceFilePath, sharedCodeServer)
  await page.reload()
  await testLineageWithProjectPath(page)
})
