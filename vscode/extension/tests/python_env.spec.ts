import { test, Page } from './fixtures'
import fs from 'fs-extra'
import {
  createVirtualEnvironment,
  openLineageView,
  pipInstall,
  PythonEnvironment,
  REPO_ROOT,
  SUSHI_SOURCE_PATH,
} from './utils'
import os from 'os'
import path from 'path'
import { setTcloudVersion, setupAuthenticatedState } from './tcloud_utils'
import { CodeServerContext } from './utils_code_server'

function writeEnvironmentConfig(sushiPath: string) {
  const configPath = path.join(sushiPath, 'config.py')
  const originalConfig = fs.readFileSync(configPath, 'utf8')

  const newConfig =
    `
import os

test_var = os.getenv("TEST_VAR")
if test_var is None or test_var == "":
    raise Exception("TEST_VAR is not set")
` + originalConfig

  fs.writeFileSync(configPath, newConfig)
}

async function runTest(
  page: Page,
  context: CodeServerContext,
  tempDir: string,
): Promise<void> {
  await page.goto(
    `http://127.0.0.1:${context.codeServerPort}` + `/?folder=${tempDir}`,
  )
  await page.waitForSelector('text=models')
  await openLineageView(page)
}

async function setupEnvironment(): Promise<{
  tempDir: string
  pythonDetails: PythonEnvironment
}> {
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  const pythonEnvDir = path.join(tempDir, '.venv')
  const pythonDetails = await createVirtualEnvironment(pythonEnvDir)
  const custom_materializations = path.join(
    REPO_ROOT,
    'examples',
    'custom_materializations',
  )
  const sqlmeshWithExtras = `${REPO_ROOT}[bigquery,lsp]`
  await pipInstall(pythonDetails, [sqlmeshWithExtras, custom_materializations])

  const settings = {
    'python.defaultInterpreterPath': pythonDetails.pythonPath,
    'sqlmesh.environmentPath': pythonEnvDir,
  }
  await fs.ensureDir(path.join(tempDir, '.vscode'))
  await fs.writeJson(path.join(tempDir, '.vscode', 'settings.json'), settings, {
    spaces: 2,
  })
  return { tempDir, pythonDetails }
}

test.describe('python environment variable injection on sqlmesh_lsp', () => {
  test('normal setup - error ', async ({ page, sharedCodeServer }) => {
    const { tempDir } = await setupEnvironment()
    writeEnvironmentConfig(tempDir)
    await runTest(page, sharedCodeServer, tempDir)
    await page.waitForSelector('text=Error creating context')
  })

  test('normal setup - set', async ({ page, sharedCodeServer }, testInfo) => {
    testInfo.setTimeout(120_000)

    const { tempDir } = await setupEnvironment()
    writeEnvironmentConfig(tempDir)
    const env_file = path.join(tempDir, '.env')
    fs.writeFileSync(env_file, 'TEST_VAR=test_value')
    await runTest(page, sharedCodeServer, tempDir)
    await page.waitForSelector('text=Loaded SQLMesh context')
  })
})

async function setupTcloudProject(
  tempDir: string,
  pythonDetails: PythonEnvironment,
) {
  // Install the mock tcloud package
  const mockTcloudPath = path.join(__dirname, 'tcloud')
  await pipInstall(pythonDetails, [mockTcloudPath])

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
  // Set tcloud version to 2.10.1
  await setTcloudVersion(tempDir, '2.10.1')
}

test.describe('tcloud version', () => {
  test('normal setup - error ', async ({ page, sharedCodeServer }) => {
    const { tempDir, pythonDetails } = await setupEnvironment()
    await setupTcloudProject(tempDir, pythonDetails)
    writeEnvironmentConfig(tempDir)
    await runTest(page, sharedCodeServer, tempDir)
    await page.waitForSelector('text=Error creating context')
  })

  test('normal setup - set', async ({ page, sharedCodeServer }) => {
    const { tempDir, pythonDetails } = await setupEnvironment()
    await setupTcloudProject(tempDir, pythonDetails)
    writeEnvironmentConfig(tempDir)
    const env_file = path.join(tempDir, '.env')
    fs.writeFileSync(env_file, 'TEST_VAR=test_value')
    await runTest(page, sharedCodeServer, tempDir)
    await page.waitForSelector('text=Loaded SQLMesh context')
  })
})
