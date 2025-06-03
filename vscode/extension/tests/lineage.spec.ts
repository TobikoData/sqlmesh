import { test, expect, Page } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { startVSCode, SUSHI_SOURCE_PATH } from './utils'
import { writeFileSync } from 'fs'

/**
 * Helper function to launch VS Code and test lineage with given project path config
 */
async function testLineageWithProjectPath(window: Page): Promise<void> {
  // Trigger lineage command
  await window.keyboard.press(
    process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
  )
  await window.keyboard.type('Lineage: Focus On View')
  await window.keyboard.press('Enter')

  // Wait for "Loaded SQLMesh context" text to appear
  const loadedContextText = window.locator('text=Loaded SQLMesh context')
  await expect(loadedContextText.first()).toBeVisible({ timeout: 10_000 })
}

test('Lineage panel renders correctly - no project path config (default)', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  try {
    const { window, close } = await startVSCode(tempDir)
    await testLineageWithProjectPath(window)
    await close()
  } finally {
    await fs.remove(tempDir)
  }
})

test('Lineage panel renders correctly - relative project path', async () => {
  const workspaceDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-workspace-'),
  )
  const projectDir = path.join(workspaceDir, 'projects', 'sushi')
  await fs.copy(SUSHI_SOURCE_PATH, projectDir)

  const settings = {
    'sqlmesh.projectPath': './projects/sushi',
  }
  await fs.ensureDir(path.join(workspaceDir, '.vscode'))
  await fs.writeJson(
    path.join(workspaceDir, '.vscode', 'settings.json'),
    settings,
    { spaces: 2 },
  )

  try {
    const { window, close } = await startVSCode(workspaceDir)
    await testLineageWithProjectPath(window)
    await close()
  } finally {
    await fs.remove(workspaceDir)
  }
})

test('Lineage panel renders correctly - absolute project path', async () => {
  const workspaceDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-workspace-'),
  )
  const projectDir = path.join(workspaceDir, 'projects', 'sushi')
  await fs.ensureDir(path.join(workspaceDir, '.vscode'))
  await fs.copy(SUSHI_SOURCE_PATH, projectDir)
  await fs.ensureDir(path.join(workspaceDir, '.vscode'))
  const settings = {
    'sqlmesh.projectPath': projectDir,
  }
  await fs.writeJson(
    path.join(workspaceDir, '.vscode', 'settings.json'),
    settings,
    { spaces: 2 },
  )

  try {
    const { window, close } = await startVSCode(workspaceDir)
    await testLineageWithProjectPath(window)
    await close()
  } finally {
    await fs.remove(workspaceDir)
  }
})

test('Lineage panel renders correctly - relative project outside of workspace', async () => {
  const tempFolder = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-workspace-'),
  )
  const projectDir = path.join(tempFolder, 'projects', 'sushi')
  await fs.copy(SUSHI_SOURCE_PATH, projectDir)

  const workspaceDir = path.join(tempFolder, 'workspace')
  await fs.ensureDir(workspaceDir)

  const settings = {
    'sqlmesh.projectPath': './../projects/sushi',
  }
  await fs.ensureDir(path.join(workspaceDir, '.vscode'))
  await fs.writeJson(
    path.join(workspaceDir, '.vscode', 'settings.json'),
    settings,
    { spaces: 2 },
  )

  try {
    const { window, close } = await startVSCode(workspaceDir)
    await testLineageWithProjectPath(window)
    await close()
  } finally {
    await fs.remove(tempFolder)
  }
})

test('Lineage panel renders correctly - absolute path project outside of workspace', async () => {
  const tempFolder = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-workspace-'),
  )
  const projectDir = path.join(tempFolder, 'projects', 'sushi')
  await fs.copy(SUSHI_SOURCE_PATH, projectDir)

  const workspaceDir = path.join(tempFolder, 'workspace')
  await fs.ensureDir(workspaceDir)

  const settings = {
    'sqlmesh.projectPath': projectDir,
  }
  await fs.ensureDir(path.join(workspaceDir, '.vscode'))
  await fs.writeJson(
    path.join(workspaceDir, '.vscode', 'settings.json'),
    settings,
    { spaces: 2 },
  )

  try {
    const { window, close } = await startVSCode(workspaceDir)
    await testLineageWithProjectPath(window)
    await close()
  } finally {
    await fs.remove(tempFolder)
  }
})

test('Lineage panel renders correctly - multiworkspace setup', async () => {
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

  try {
    const { window, close } = await startVSCode(workspaceFilePath)
    await testLineageWithProjectPath(window)
    await close()
  } finally {
    await fs.remove(workspaceDir)
  }
})

test('Lineage panel renders correctly - multiworkspace setup reversed', async () => {
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

  try {
    const { window, close } = await startVSCode(workspaceFilePath)
    await testLineageWithProjectPath(window)
    await close()
  } finally {
    await fs.remove(workspaceDir)
  }
})
