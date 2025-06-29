import { test, expect } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { SUSHI_SOURCE_PATH } from './utils'
import { startCodeServer, stopCodeServer } from './utils_code_server'

test('Model type hinting', async ({ page }) => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  const context = await startCodeServer({
    tempDir,
    placeFileWithPythonInterpreter: true,
  })

  try {
    // Navigate to code-server instance
    await page.goto(`http://127.0.0.1:${context.codeServerPort}`)

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
    await page.waitForSelector('text=Loaded SQLMesh Context')

    // Wait a moment for hints to appear
    await page.waitForTimeout(500)

    // Check if the hint is visible
    expect(await page.locator('text="country code"::INT').count()).toBe(1)
  } finally {
    await stopCodeServer(context)
  }
})
