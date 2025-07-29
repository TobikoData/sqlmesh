import { test } from './fixtures'
import fs from 'fs-extra'
import {
  openServerPage,
  runCommand,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

test('Format project works correctly', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  await createPythonInterpreterSettingsSpecifier(tempDir)
  await openServerPage(page, tempDir, sharedCodeServer)

  //   Wait for the models folder to be visible
  await page.waitForSelector('text=models')

  // Click on the models folder, excluding external_models
  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()

  // Open the customer_revenue_lifetime model
  await page
    .getByRole('treeitem', { name: 'customers.sql', exact: true })
    .locator('a')
    .click()

  await page.waitForSelector('text=grain')
  await waitForLoadedSQLMesh(page)

  // Format the project
  await runCommand(page, 'Test: Run All Tests')

  await page.waitForSelector('text=test_order_items')
})
