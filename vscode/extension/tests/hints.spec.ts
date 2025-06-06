import { test, expect } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { startVSCode, SUSHI_SOURCE_PATH } from './utils'

test('Model type hinting', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  try {
    const { window, close } = await startVSCode(tempDir)

    // Wait for the models folder to be visible
    await window.waitForSelector('text=models')

    // Click on the models folder
    await window
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()

    // Open the customers_revenue_by_day model
    await window
      .getByRole('treeitem', {
        name: 'customer_revenue_by_day.sql',
        exact: true,
      })
      .locator('a')
      .click()

    await window.waitForSelector('text=grain')
    await window.waitForSelector('text=Loaded SQLMesh Context')

    // Wait a moment for hints to appear
    await window.waitForTimeout(500)

    // Check if the hint is visible
    expect(await window.locator('text="country code"::INT').count()).toBe(1)

    await close()
  } finally {
    await fs.remove(tempDir)
  }
})
