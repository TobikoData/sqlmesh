import { expect, test } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import {
  createVirtualEnvironment,
  pipInstall,
  REPO_ROOT,
  startVSCode,
  SUSHI_SOURCE_PATH,
} from './utils'

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

/**
 * Helper function to set up a pre-authenticated tcloud state
 */
async function setupAuthenticatedState(tempDir: string): Promise<void> {
  const authStateFile = path.join(tempDir, '.tcloud_auth_state.json')
  const authState = {
    is_logged_in: true,
    id_token: {
      iss: 'https://mock.tobikodata.com',
      aud: 'mock-audience',
      sub: 'user-123',
      scope: 'openid email profile',
      iat: Math.floor(Date.now() / 1000),
      exp: Math.floor(Date.now() / 1000) + 3600, // Valid for 1 hour
      email: 'test@example.com',
      name: 'Test User',
    },
  }
  await fs.writeJson(authStateFile, authState)
}

/**
 * Helper function to set the tcloud version for testing
 */
async function setTcloudVersion(
  tempDir: string,
  version: string,
): Promise<void> {
  const versionStateFile = path.join(tempDir, '.tcloud_version_state.json')
  await fs.writeJson(versionStateFile, { version })
}

test.describe('Tcloud', () => {
  test('not signed in, shows sign in window', async ({}, testInfo) => {
    testInfo.setTimeout(120_000) // 2 minutes for venv creation and package installation
    const tempDir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'vscode-test-tcloud-'),
    )
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

      // Wait for the file to open
      await window.waitForTimeout(2000)

      await window.waitForSelector(
        'text=Please sign in to Tobiko Cloud to use SQLMesh',
      )

      // Close VS Code
      await close()
    } finally {
      // Clean up
      await fs.remove(tempDir)
    }
  })

  test('signed in and not installed shows installation window', async ({}, testInfo) => {
    testInfo.setTimeout(120_000) // 2 minutes for venv creation and package installation
    const tempDir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'vscode-test-tcloud-'),
    )
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

      await window.waitForSelector('text=Installing enterprise python package')
      expect(
        await window.locator('text=Installing enterprise python package'),
      ).toHaveCount(2)

      await window.waitForSelector('text=Loaded SQLMesh context')

      // Close VS Code
      await close()
    } finally {
      // Clean up
      await fs.remove(tempDir)
    }
  })

  test('tcloud sqlmesh_lsp command starts the sqlmesh_lsp in old version when ready', async ({}, testInfo) => {
    testInfo.setTimeout(120_000) // 2 minutes for venv creation and package installation
    const tempDir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'vscode-test-tcloud-'),
    )
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

      // Verify the context loads successfully
      await window.waitForSelector('text=Loaded SQLMesh context')

      // Close VS Code
      await close()
    } finally {
      // Clean up
      await fs.remove(tempDir)
    }
  })

  test('tcloud sqlmesh_lsp command starts the sqlmesh_lsp in new version when ready', async ({}, testInfo) => {
    testInfo.setTimeout(120_000) // 2 minutes for venv creation and package installation
    const tempDir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'vscode-test-tcloud-'),
    )
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

      // Verify the context loads successfully
      await window.waitForSelector('text=Loaded SQLMesh context')

      // Close VS Code
      await close()
    } finally {
      // Clean up
      await fs.remove(tempDir)
    }
  })
})
