import { test, expect } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { startVSCode, SUSHI_SOURCE_PATH } from './utils'

test('Format project works correctly', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  try {
    const { window, close } = await startVSCode(tempDir)

    //   Wait for the models folder to be visible
    await window.waitForSelector('text=models')

    // Click on the models folder, excluding external_models
    await window
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()

    // Open the customer_revenue_lifetime model
    await window
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    await window.waitForSelector('text=grain')
    await window.waitForSelector('text=Loaded SQLMesh Context')

    // Render the model
    await window.keyboard.press(
      process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
    )
    await window.keyboard.type('Format SQLMesh Project')
    await window.keyboard.press('Enter')

    // Check that the notification appears saying 'Project formatted successfully'
    await expect(
      window.getByText('Project formatted successfully', { exact: true }),
    ).toBeVisible()

    await close()
  } finally {
    await fs.remove(tempDir)
  }
})
