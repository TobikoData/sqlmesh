import { test } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { startVSCode, SUSHI_SOURCE_PATH } from './utils'

test('Workspace diagnostics show up in the diagnostics panel', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  const configPath = path.join(tempDir, 'config.py')
  const configContent = await fs.readFile(configPath, 'utf8')
  const updatedContent = configContent.replace('enabled=False', 'enabled=True')
  await fs.writeFile(configPath, updatedContent)

  try {
    const { window, close } = await startVSCode(tempDir)

    // Wait for the models folder to be visible
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

    await // Open problems panel
    await window.keyboard.press(
      process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
    )
    await window.keyboard.type('View: Focus Problems')
    await window.keyboard.press('Enter')

    await window.waitForSelector('text=problems')
    await window.waitForSelector('text=All models should have an owner')

    await close()
  } finally {
    await fs.remove(tempDir)
  }
})
