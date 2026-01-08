import { test, expect } from './fixtures'
import fs from 'fs-extra'
import {
  goToDefinition,
  openServerPage,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

test('Stop server works', async ({ page, sharedCodeServer, tempDir }) => {
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  await createPythonInterpreterSettingsSpecifier(tempDir)
  await openServerPage(page, tempDir, sharedCodeServer)

  // Wait for the models folder to be visible
  await page.waitForSelector('text=models')

  // Click on the models folder
  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()

  // Open the customer_revenue_lifetime model
  await page
    .getByRole('treeitem', { name: 'top_waiters.sql', exact: true })
    .locator('a')
    .click()

  await page.waitForSelector('text=grain')
  await waitForLoadedSQLMesh(page)

  // Render the model
  await page.locator('text=@MULTIPLY').click()
  await goToDefinition(page)

  // Check if the model is rendered by check if "`oi`.`order_id` AS `order_id`," is in the window
  await expect(page.locator('text=def multiply(')).toBeVisible()
})

test('Go to definition for model', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  await createPythonInterpreterSettingsSpecifier(tempDir)

  // Navigate to code-server instance
  await openServerPage(page, tempDir, sharedCodeServer)

  // Wait for the models folder to be visible
  await page.waitForSelector('text=models')

  // Click on the models folder
  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()

  // Open the top_waiters model
  await page
    .getByRole('treeitem', { name: 'top_waiters.sql', exact: true })
    .locator('a')
    .click()

  await page.waitForSelector('text=grain')
  await waitForLoadedSQLMesh(page)

  // Go to definition for the model
  await page.locator('text=sushi.waiter_revenue_by_day').first().click()
  await goToDefinition(page)
  await expect(
    page.locator('text=SUM(oi.quantity * i.price)::DOUBLE AS revenue'),
  ).toBeVisible()
})
