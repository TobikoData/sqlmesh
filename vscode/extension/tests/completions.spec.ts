import { test, expect } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { startVSCode, SUSHI_SOURCE_PATH } from './utils'

test('Autocomplete for model names', async () => {
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

    // Open the top_waiters model
    await window
      .getByRole('treeitem', { name: 'top_waiters.sql', exact: true })
      .locator('a')
      .click()

    await window.waitForSelector('text=grain')
    await window.waitForSelector('text=Loaded SQLMesh Context')

    await window.locator('text=grain').first().click()

    // Move to the end of the file
    await window.keyboard.press('Control+End')

    // Add a new line
    await window.keyboard.press('Enter')

    // Type the beginning of sushi.customers to trigger autocomplete
    await window.keyboard.type('sushi.cus')

    // Wait a moment for autocomplete to appear
    await window.waitForTimeout(500)

    // Check if the autocomplete suggestion for sushi.customers is visible
    await expect(window.locator('text=sushi.customers')).toBeVisible()

    await close()
  } finally {
    await fs.remove(tempDir)
  }
})
