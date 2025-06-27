import path from 'path'
import { SUSHI_SOURCE_PATH } from './utils'
import os from 'os'
import { test } from '@playwright/test'
import fs from 'fs-extra'
import { startCodeServer, stopCodeServer } from './utils_code_server'

test('Stop server works', async ({ page }) => {
  test.setTimeout(120000) // Increase timeout to 2 minutes

  console.log('Starting test: Stop server works')
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  const context = await startCodeServer(tempDir, true)

  try {
    // Navigate to code-server instance
    await page.goto(`http://127.0.0.1:${context.codeServerPort}`)

    // Wait for code-server to load
    await page.waitForLoadState('networkidle')
    await page.waitForSelector('[role="application"]', { timeout: 10000 })

    // Wait for the models folder to be visible in the file explorer
    await page.waitForSelector('text=models')

    // Click on the models folder, excluding external_models
    await page
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()

    // Open the customers.sql model
    await page
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    await page.waitForSelector('text=grain')
    await page.waitForSelector('text=Loaded SQLMesh Context')

    // Stop the server
    await page.keyboard.press(
      process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
    )
    await page.keyboard.type('SQLMesh: Stop Server')
    await page.keyboard.press('Enter')

    // Await LSP server stopped message
    await page.waitForSelector('text=LSP server stopped')

    // Render the model
    await page.keyboard.press(
      process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
    )
    await page.keyboard.type('Render Model')
    await page.keyboard.press('Enter')

    // Await error message
    await page.waitForSelector(
      'text="Failed to render model: LSP client not ready."',
    )
  } finally {
    await stopCodeServer(context)
  }
})
