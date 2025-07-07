import { test } from '@playwright/test'
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
import { startCodeServer, stopCodeServer } from './utils_code_server'

test('venv being named .env', async ({ page }, testInfo) => {
  testInfo.setTimeout(120_000) // 2 minutes for venv creation and package installation
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

  const context = await startCodeServer({ tempDir })

  try {
    await page.goto(`http://127.0.0.1:${context.codeServerPort}`)
    await page.waitForSelector('text=models')
    await openLineageView(page)
    await page.waitForSelector('text=Loaded SQLMesh Context')
  } finally {
    await stopCodeServer(context)
  }
})
