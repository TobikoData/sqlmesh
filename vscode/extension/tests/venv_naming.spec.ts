import { test } from './fixtures'
import fs from 'fs-extra'
import os from 'os'
import path from 'path'
import {
  createVirtualEnvironment,
  openLineageView,
  pipInstall,
  REPO_ROOT,
  SUSHI_SOURCE_PATH,
} from './utils'

test('venv being named .env', async ({ page, sharedCodeServer }) => {
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )

  const pythonEnvDir = path.join(tempDir, '.env')
  const pythonDetails = await createVirtualEnvironment(pythonEnvDir)
  const custom_materializations = path.join(
    REPO_ROOT,
    'examples',
    'custom_materializations',
  )
  const sqlmeshWithExtras = `${REPO_ROOT}[bigquery,lsp]`
  await pipInstall(pythonDetails, [sqlmeshWithExtras, custom_materializations])
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  const settings = {
    'python.defaultInterpreterPath': pythonDetails.pythonPath,
  }
  await fs.ensureDir(path.join(tempDir, '.vscode'))
  await fs.writeJson(path.join(tempDir, '.vscode', 'settings.json'), settings, {
    spaces: 2,
  })

  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )
  await page.waitForSelector('text=models')
  await openLineageView(page)
  await page.waitForSelector('text=Loaded SQLMesh Context')
})
