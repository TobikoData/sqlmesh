import { expect, test } from './fixtures'
import fs from 'fs-extra'
import os from 'os'
import path from 'path'
import {
  createVirtualEnvironment,
  openFile,
  openLineageView,
  pipInstall,
  REPO_ROOT,
  SUSHI_SOURCE_PATH,
} from './utils'

test('missing LSP dependencies shows install prompt', async ({
  page,
  sharedCodeServer,
}) => {
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

  // Copy sushi project
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  // Configure VS Code settings to use our Python environment
  const settings = {
    'python.defaultInterpreterPath': pythonDetails.pythonPath,
    'sqlmesh.environmentPath': pythonEnvDir,
  }
  await fs.ensureDir(path.join(tempDir, '.vscode'))
  await fs.writeJson(path.join(tempDir, '.vscode', 'settings.json'), settings, {
    spaces: 2,
  })

  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )

  // Open a SQL file to trigger SQLMesh activation
  // Wait for the models folder to be visible
  await page.waitForSelector('text=models')

  // Click on the models folder
  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()

  // Open the top_waiters model
  await page
    .getByRole('treeitem', { name: 'customers.sql', exact: true })
    .locator('a')
    .click()

  // Wait for the message to show that LSP extras need to be installed
  await page.waitForSelector('text=LSP dependencies missing')
  expect(await page.locator('text=Install').count()).toBeGreaterThanOrEqual(1)
})

test('lineage, no sqlmesh found', async ({
  page,
  sharedCodeServer,
}, testInfo) => {
  testInfo.setTimeout(120_000) // 2 minutes for venv creation and package installation

  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  const pythonEnvDir = path.join(tempDir, '.venv')
  const pythonDetails = await createVirtualEnvironment(pythonEnvDir)

  // Copy sushi project
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  // Configure VS Code settings to use our Python environment
  const settings = {
    'python.defaultInterpreterPath': pythonDetails.pythonPath,
    'sqlmesh.environmentPath': pythonEnvDir,
  }
  await fs.ensureDir(path.join(tempDir, '.vscode'))
  await fs.writeJson(path.join(tempDir, '.vscode', 'settings.json'), settings, {
    spaces: 2,
  })

  // navigate to code-server instance
  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )
  await page.waitForLoadState('networkidle')

  // Open lineage view
  await openLineageView(page)

  // Assert shows that sqlmesh is not installed
  await page.waitForSelector('text=SQLMesh LSP not found')
})

// Checks that if you have another file open like somewhere else, it still checks the workspace first for a successful context
// it's very flaky but runs when debugging
// - the typing in of the file name is very flaky
test.skip('check that the LSP runs correctly by opening lineage when looking at another file before not in workspace', async ({
  page,
  sharedCodeServer,
}) => {
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  const pythonEnvDir = path.join(tempDir, '.venv')
  const pythonDetails = await createVirtualEnvironment(pythonEnvDir)
  const sqlmeshWithExtras = `${REPO_ROOT}[lsp, bigquery]`
  const custom_materializations = path.join(
    REPO_ROOT,
    'examples',
    'custom_materializations',
  )
  await pipInstall(pythonDetails, [sqlmeshWithExtras, custom_materializations])

  // Configure VS Code settings to use our Python environment
  const settings = {
    'python.defaultInterpreterPath': pythonDetails.pythonPath,
    'sqlmesh.environmentPath': tempDir,
  }
  await fs.ensureDir(path.join(tempDir, '.vscode'))
  await fs.writeJson(path.join(tempDir, '.vscode', 'settings.json'), settings, {
    spaces: 2,
  })

  // Write a sql file in another folder
  const tempDir2 = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-2-'),
  )
  const sqlFile = path.join(tempDir2, 'models', 'customers.sql')
  await fs.ensureDir(path.dirname(sqlFile))
  await fs.writeFile(sqlFile, 'SELECT 1')

  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )
  await page.waitForLoadState('networkidle')

  // Open the SQL file from the other directory
  await openFile(page, sqlFile)

  await page.waitForSelector('text=Loaded SQLMesh context')
})
