import { test, expect } from './fixtures'
import fs from 'fs-extra'
import {
  openServerPage,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

test('Model type hinting', async ({ page, sharedCodeServer, tempDir }) => {
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

  // Open the customers_revenue_by_day model
  await page
    .getByRole('treeitem', {
      name: 'customer_revenue_by_day.sql',
      exact: true,
    })
    .locator('a')
    .click()

  await page.waitForSelector('text=grain')
  await waitForLoadedSQLMesh(page)

  // Wait a moment for hints to appear
  await page.waitForTimeout(500)

  // Check if the hint is visible
  expect(await page.locator('text="country code"::INT').count()).toBe(1)
})
