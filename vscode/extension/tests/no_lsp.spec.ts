import { expect, test } from '@playwright/test'
import fs from 'fs-extra'
import os from 'os'
import path from 'path'
import {
  createVirtualEnvironment,
  pipInstall,
  REPO_ROOT,
  startVSCode,
  SUSHI_SOURCE_PATH,
} from './utils'

test('missing LSP dependencies shows install prompt', async ({}, testInfo) => {
  testInfo.setTimeout(120_000) // 2 minutes for venv creation and package installation
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  const pythonEnvDir = path.join(tempDir, '.venv')
  const pythonDetails = await createVirtualEnvironment(pythonEnvDir)
  const custom_materializations = path.join(
    REPO_ROOT,
    'examples',
    'custom_materializations',
  )
  const sqlmeshWithExtras = `${REPO_ROOT}[bigquery]`
  await pipInstall(pythonDetails, [sqlmeshWithExtras, custom_materializations])

  try {
    // Copy sushi project
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)

    // Configure VS Code settings to use our Python environment
    const settings = {
      'python.defaultInterpreterPath': pythonDetails.pythonPath,
      'sqlmesh.environmentPath': pythonEnvDir,
    }
    await fs.ensureDir(path.join(tempDir, '.vscode'))
    await fs.writeJson(
      path.join(tempDir, '.vscode', 'settings.json'),
      settings,
      { spaces: 2 },
    )

    // Start VS Code
    const { window, close } = await startVSCode(tempDir)

    // Open a SQL file to trigger SQLMesh activation
    // Wait for the models folder to be visible
    await window.waitForSelector('text=models')

    // Click on the models folder
    await window
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()

    // Open the top_waiters model
    await window
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    // Wait for the message to show that LSP extras need to be installed
    await window.waitForSelector('text=LSP dependencies missing')
    expect(await window.locator('text=Install').count()).toBeGreaterThanOrEqual(
      1,
    )

    await close()
  } finally {
    // Clean up
    await fs.remove(tempDir)
  }
})
