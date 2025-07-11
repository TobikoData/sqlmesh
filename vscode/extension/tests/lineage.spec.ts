import { test, Page } from './fixtures'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { openLineageView, SUSHI_SOURCE_PATH } from './utils'
import { writeFileSync } from 'fs'
import {
  createPythonInterpreterSettingsSpecifier,
  startCodeServer,
  stopCodeServer,
} from './utils_code_server'

/**
 * Helper function to launch VS Code and test lineage with given project path config
 */
async function testLineageWithProjectPath(page: Page): Promise<void> {
  await page.waitForLoadState('networkidle')
  await page.waitForLoadState('domcontentloaded')
  await openLineageView(page)
  await page.waitForSelector('text=Loaded SQLMesh context')
}

test('Lineage panel renders correctly - no project path config (default)', async ({
  page,
  sharedCodeServer,
}) => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  await createPythonInterpreterSettingsSpecifier(tempDir)

  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )
  await testLineageWithProjectPath(page)
})

test.skip('Lineage panel renders correctly - relative project path', async ({
  page,
}) => {
  const workspaceDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-workspace-'),
  )
  const projectDir = path.join(workspaceDir, 'projects', 'sushi')
  await fs.copy(SUSHI_SOURCE_PATH, projectDir)

  const context = await startCodeServer({
    tempDir: workspaceDir,
  })

  const settings = {
    'sqlmesh.projectPath': './projects/sushi',
    'python.defaultInterpreterPath': context.defaultPythonInterpreter,
  }
  await fs.ensureDir(path.join(workspaceDir, '.vscode'))
  await fs.writeJson(
    path.join(workspaceDir, '.vscode', 'settings.json'),
    settings,
    { spaces: 2 },
  )

  try {
    await page.goto(`http://127.0.0.1:${context.codeServerPort}`)
    await testLineageWithProjectPath(page)
  } finally {
    await fs.remove(workspaceDir)
  }
})

test.skip('Lineage panel renders correctly - absolute project path', async ({
  page,
}) => {
  const workspaceDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-workspace-'),
  )
  const projectDir = path.join(workspaceDir, 'projects', 'sushi')
  await fs.ensureDir(path.join(workspaceDir, '.vscode'))
  await fs.copy(SUSHI_SOURCE_PATH, projectDir)
  await fs.ensureDir(path.join(workspaceDir, '.vscode'))
  const context = await startCodeServer({
    tempDir: workspaceDir,
  })

  const settings = {
    'sqlmesh.projectPath': projectDir,
    'python.defaultInterpreterPath': context.defaultPythonInterpreter,
  }
  await fs.writeJson(
    path.join(workspaceDir, '.vscode', 'settings.json'),
    settings,
    { spaces: 2 },
  )

  try {
    await page.goto(`http://127.0.0.1:${context.codeServerPort}`)
    await testLineageWithProjectPath(page)
  } finally {
    await stopCodeServer(context)
  }
})

test.skip('Lineage panel renders correctly - relative project outside of workspace', async ({
  page,
}) => {
  const tempFolder = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-workspace-'),
  )
  const projectDir = path.join(tempFolder, 'projects', 'sushi')
  await fs.copy(SUSHI_SOURCE_PATH, projectDir)

  const workspaceDir = path.join(tempFolder, 'workspace')
  await fs.ensureDir(workspaceDir)
  const context = await startCodeServer({
    tempDir: workspaceDir,
  })

  const settings = {
    'sqlmesh.projectPath': './../projects/sushi',
    'python.defaultInterpreterPath': context.defaultPythonInterpreter,
  }
  await fs.ensureDir(path.join(workspaceDir, '.vscode'))
  await fs.writeJson(
    path.join(workspaceDir, '.vscode', 'settings.json'),
    settings,
    { spaces: 2 },
  )

  try {
    await page.goto(`http://127.0.0.1:${context.codeServerPort}`)
    await testLineageWithProjectPath(page)
  } finally {
    await stopCodeServer(context)
  }
})

test.skip('Lineage panel renders correctly - absolute path project outside of workspace', async ({
  page,
  sharedCodeServer,
}) => {
  const tempFolder = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-workspace-'),
  )
  const projectDir = path.join(tempFolder, 'projects', 'sushi')
  await fs.copy(SUSHI_SOURCE_PATH, projectDir)

  const workspaceDir = path.join(tempFolder, 'workspace')
  await fs.ensureDir(workspaceDir)

  const settings = {
    'sqlmesh.projectPath': projectDir,
    'python.defaultInterpreterPath': sharedCodeServer.defaultPythonInterpreter,
  }
  await fs.ensureDir(path.join(workspaceDir, '.vscode'))
  await fs.writeJson(
    path.join(workspaceDir, '.vscode', 'settings.json'),
    settings,
    { spaces: 2 },
  )

  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}?folder=${workspaceDir}`,
  )
  await testLineageWithProjectPath(page)
})

// These work on local machine when debuggin but not on CI, so skipping for now
test.skip('Lineage panel renders correctly - multiworkspace setup', async ({
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

  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}?folder=${workspaceDir}`,
  )
  await page.waitForSelector('text=Open workspace')
  await page.click('text=Open workspace')
  await testLineageWithProjectPath(page)
})

test.skip('Lineage panel renders correctly - multiworkspace setup reversed', async ({
  page,
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

  const context = await startCodeServer({
    tempDir: workspaceDir,
  })

  const settings = {
    'python.defaultInterpreterPath': context.defaultPythonInterpreter,
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

  try {
    await page.goto(`http://127.0.0.1:${context.codeServerPort}`)
    await page.waitForSelector('text=Open workspace')
    await page.click('text=Open workspace')
    await testLineageWithProjectPath(page)
  } finally {
    await stopCodeServer(context)
  }
})
