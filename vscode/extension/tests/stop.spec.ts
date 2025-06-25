import path from 'path'
import { startVSCode, SUSHI_SOURCE_PATH } from './utils'
import os from 'os'
import { test } from '@playwright/test'
import fs from 'fs-extra'

test('Stop server works', async () => {
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

    // Stop the server
    await window.keyboard.press(
      process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
    )
    await window.keyboard.type('SQLMesh: Stop Server')
    await window.keyboard.press('Enter')

    // Await LSP server stopped message
    await window.waitForSelector('text=LSP server stopped')

    // Render the model
    await window.keyboard.press(
      process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
    )
    await window.keyboard.type('Render Model')
    await window.keyboard.press('Enter')

    // Await error message
    await window.waitForSelector(
      'text="Failed to render model: LSP client not ready."',
    )
    await close()
  } finally {
    await fs.remove(tempDir)
  }
})
