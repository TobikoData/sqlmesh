import { test } from './fixtures'
import fs from 'fs-extra'
import path from 'path'
import {
  createVirtualEnvironment,
  openLineageView,
  openServerPage,
  pipInstall,
  REPO_ROOT,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'

test('venv being named .env', async ({ page, sharedCodeServer, tempDir }) => {
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

  await openServerPage(page, tempDir, sharedCodeServer)
  await page.waitForSelector('text=models')
  await openLineageView(page)
  await waitForLoadedSQLMesh(page)
})
