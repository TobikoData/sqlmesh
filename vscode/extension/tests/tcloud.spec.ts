import { expect, test } from './fixtures'
import path from 'path'
import fs from 'fs-extra'
import {
  createVirtualEnvironment,
  openServerPage,
  pipInstall,
  REPO_ROOT,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import { setTcloudVersion, setupAuthenticatedState } from './tcloud_utils'
import {
  createPythonInterpreterSettingsSpecifier,
  startCodeServer,
  stopCodeServer,
} from './utils_code_server'

/**
 * Helper function to create and set up a Python virtual environment
 */
async function setupPythonEnvironment(envDir: string): Promise<string> {
  // Create virtual environment
  const pythonDetails = await createVirtualEnvironment(envDir)

  // Install the mock tcloud package
  const mockTcloudPath = path.join(__dirname, 'tcloud')
  await pipInstall(pythonDetails, [mockTcloudPath])

  // Install sqlmesh from the local repository with LSP support
  const customMaterializations = path.join(
    REPO_ROOT,
    'examples',
    'custom_materializations',
  )
  const sqlmeshWithExtras = `${REPO_ROOT}[lsp,bigquery]`
  await pipInstall(pythonDetails, [sqlmeshWithExtras, customMaterializations])

  return pythonDetails.pythonPath
}

test('not signed in, shows sign in window', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  const pythonEnvDir = path.join(tempDir, '.venv')

  const context = await startCodeServer({ tempDir })

  try {
    // Copy sushi project
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)

    // Create a tcloud.yaml to mark this as a tcloud project
    const tcloudConfig = {
      url: 'https://mock.tobikodata.com',
      org: 'test-org',
      project: 'test-project',
    }
    await fs.writeFile(
      path.join(tempDir, 'tcloud.yaml'),
      `url: ${tcloudConfig.url}\norg: ${tcloudConfig.org}\nproject: ${tcloudConfig.project}\n`,
    )

    // Set tcloud version to 2.10.0
    await setTcloudVersion(tempDir, '2.10.0')

    // Set up Python environment with mock tcloud and sqlmesh
    const pythonPath = await setupPythonEnvironment(pythonEnvDir)

    // Configure VS Code settings to use our Python environment
    const settings = {
      'python.defaultInterpreterPath': pythonPath,
      'sqlmesh.environmentPath': pythonEnvDir,
    }
    await fs.ensureDir(path.join(tempDir, '.vscode'))
    await fs.writeJson(
      path.join(tempDir, '.vscode', 'settings.json'),
      settings,
      { spaces: 2 },
    )

    await openServerPage(page, tempDir, sharedCodeServer)

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

    // Wait for the file to open
    await page.waitForLoadState('networkidle')

    await page.waitForSelector(
      'text=Please sign in to Tobiko Cloud to use SQLMesh',
    )
  } finally {
    await stopCodeServer(context)
  }
})

test('signed in and not installed shows installation window', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  const pythonEnvDir = path.join(tempDir, '.venv')

  const context = await startCodeServer({ tempDir })

  try {
    // Copy sushi project
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)

    // Create a tcloud.yaml to mark this as a tcloud project
    const tcloudConfig = {
      url: 'https://mock.tobikodata.com',
      org: 'test-org',
      project: 'test-project',
    }
    await fs.writeFile(
      path.join(tempDir, 'tcloud.yaml'),
      `url: ${tcloudConfig.url}\norg: ${tcloudConfig.org}\nproject: ${tcloudConfig.project}\n`,
    )

    // Write mock ".tcloud_auth_state.json" file
    await setupAuthenticatedState(tempDir)

    // Set tcloud version to 2.10.0
    await setTcloudVersion(tempDir, '2.10.0')

    // Set up Python environment with mock tcloud and sqlmesh
    const pythonPath = await setupPythonEnvironment(pythonEnvDir)

    // Configure VS Code settings to use our Python environment
    const settings = {
      'python.defaultInterpreterPath': pythonPath,
      'sqlmesh.environmentPath': pythonEnvDir,
    }
    await fs.ensureDir(path.join(tempDir, '.vscode'))
    await fs.writeJson(
      path.join(tempDir, '.vscode', 'settings.json'),
      settings,
      { spaces: 2 },
    )

    await openServerPage(page, tempDir, sharedCodeServer)

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

    await page.waitForSelector('text=Installing enterprise python package')
    await expect(
      page.locator('text=Installing enterprise python package'),
    ).toHaveCount(2)

    await waitForLoadedSQLMesh(page)
  } finally {
    await stopCodeServer(context)
  }
})

test('tcloud sqlmesh_lsp command starts the sqlmesh_lsp in old version when ready', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  const pythonEnvDir = path.join(tempDir, '.venv')

  try {
    // Copy sushi project
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)

    // Create a tcloud.yaml to mark this as a tcloud project
    const tcloudConfig = {
      url: 'https://mock.tobikodata.com',
      org: 'test-org',
      project: 'test-project',
    }
    await fs.writeFile(
      path.join(tempDir, 'tcloud.yaml'),
      `url: ${tcloudConfig.url}\norg: ${tcloudConfig.org}\nproject: ${tcloudConfig.project}\n`,
    )

    // Write mock ".tcloud_auth_state.json" file
    await setupAuthenticatedState(tempDir)

    // Set tcloud version to 2.10.0
    await setTcloudVersion(tempDir, '2.10.0')

    // Set up Python environment with mock tcloud and sqlmesh
    const pythonPath = await setupPythonEnvironment(pythonEnvDir)

    // Mark sqlmesh as installed
    const binDir = path.dirname(pythonPath)
    const installStateFile = path.join(binDir, '.sqlmesh_installed')
    await fs.writeFile(installStateFile, '')

    // Configure VS Code settings to use our Python environment
    const settings = {
      'python.defaultInterpreterPath': pythonPath,
      'sqlmesh.environmentPath': pythonEnvDir,
    }
    await fs.ensureDir(path.join(tempDir, '.vscode'))
    await fs.writeJson(
      path.join(tempDir, '.vscode', 'settings.json'),
      settings,
      { spaces: 2 },
    )

    // Start VS Code
    await openServerPage(page, tempDir, sharedCodeServer)

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

    // Verify the context loads successfully
    await waitForLoadedSQLMesh(page)
  } finally {
    // Clean up
    await fs.remove(tempDir)
  }
})

test('tcloud sqlmesh_lsp command starts the sqlmesh_lsp in new version when ready', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  const pythonEnvDir = path.join(tempDir, '.venv')

  try {
    // Copy sushi project
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)

    // Create a tcloud.yaml to mark this as a tcloud project
    const tcloudConfig = {
      url: 'https://mock.tobikodata.com',
      org: 'test-org',
      project: 'test-project',
    }
    await fs.writeFile(
      path.join(tempDir, 'tcloud.yaml'),
      `url: ${tcloudConfig.url}\norg: ${tcloudConfig.org}\nproject: ${tcloudConfig.project}\n`,
    )

    // Write mock ".tcloud_auth_state.json" file
    await setupAuthenticatedState(tempDir)

    // Set tcloud version to 2.10.0
    await setTcloudVersion(tempDir, '2.10.1')

    // Set up Python environment with mock tcloud and sqlmesh
    const pythonPath = await setupPythonEnvironment(pythonEnvDir)

    // Mark sqlmesh as installed
    const binDir = path.dirname(pythonPath)
    const installStateFile = path.join(binDir, '.sqlmesh_installed')
    await fs.writeFile(installStateFile, '')

    // Configure VS Code settings to use our Python environment
    const settings = {
      'python.defaultInterpreterPath': pythonPath,
      'sqlmesh.environmentPath': pythonEnvDir,
    }
    await fs.ensureDir(path.join(tempDir, '.vscode'))
    await fs.writeJson(
      path.join(tempDir, '.vscode', 'settings.json'),
      settings,
      { spaces: 2 },
    )

    await openServerPage(page, tempDir, sharedCodeServer)

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

    // Verify the context loads successfully
    await waitForLoadedSQLMesh(page)
  } finally {
    // Clean up
    await fs.remove(tempDir)
  }
})

// This test is skipped becuase of the way the sign in window is shown is not useable by playwright. It's not solvable
// but the test is still useful when running it manually.
test.skip('tcloud not signed in and not installed, shows sign in window and then fact that loaded', async ({
  page,
  tempDir,
}) => {
  const pythonEnvDir = path.join(tempDir, '.venv')

  // Create a tcloud.yaml to mark this as a tcloud project
  const tcloudConfig = {
    url: 'https://mock.tobikodata.com',
    org: 'test-org',
    project: 'test-project',
  }
  await fs.writeFile(
    path.join(tempDir, 'tcloud.yaml'),
    `url: ${tcloudConfig.url}\norg: ${tcloudConfig.org}\nproject: ${tcloudConfig.project}\n`,
  )

  // Set up Python environment with mock tcloud and sqlmesh
  const pythonPath = await setupPythonEnvironment(pythonEnvDir)

  // Configure VS Code settings to use our Python environment
  const settings = {
    'python.defaultInterpreterPath': pythonPath,
    'sqlmesh.environmentPath': pythonEnvDir,
  }
  await fs.ensureDir(path.join(tempDir, '.vscode'))
  await fs.writeJson(path.join(tempDir, '.vscode', 'settings.json'), settings, {
    spaces: 2,
  })

  // Set tcloud version to 2.10.0
  await setTcloudVersion(tempDir, '2.10.1')

  // Start VS Code
  const context = await startCodeServer({
    tempDir,
  })
  await createPythonInterpreterSettingsSpecifier(tempDir)
  await page.goto(`http://127.0.0.1:${context.codeServerPort}`)

  try {
    // Copy sushi project
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)

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

    // Verify the sign in window is shown
    await page.waitForSelector(
      'text=Please sign in to Tobiko Cloud to use SQLMesh',
    )

    // Click on the sign in button
    await page
      .getByRole('button', { name: 'Sign in' })
      .filter({ hasText: 'Sign in' })
      .click()
    await page.waitForSelector('text="Signed in successfully"')

    await page.waitForSelector('text=Installing enterprise python package')

    await waitForLoadedSQLMesh(page)
  } finally {
    await stopCodeServer(context)
  }
})
