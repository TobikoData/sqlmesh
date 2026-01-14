import { test } from './fixtures'
import {
  MULTI_SOURCE_PATH,
  openServerPage,
  waitForLoadedSQLMesh,
} from './utils'
import fs from 'fs-extra'

test('should work with multi-project setups', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  await fs.copy(MULTI_SOURCE_PATH, tempDir)

  // Open the server
  await openServerPage(page, tempDir, sharedCodeServer)

  // Open a model
  await page
    .getByRole('treeitem', { name: 'repo_1', exact: true })
    .locator('a')
    .click()
  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()
  await page
    .getByRole('treeitem', { name: 'a.sql', exact: true })
    .locator('a')
    .click()

  // Wait for for the project to be loaded
  await waitForLoadedSQLMesh(page)
})
