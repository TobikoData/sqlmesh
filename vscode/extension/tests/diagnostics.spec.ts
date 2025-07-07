import { test } from './fixtures'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { runCommand, SUSHI_SOURCE_PATH } from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

test('Workspace diagnostics show up in the diagnostics panel', async ({
  page,
  sharedCodeServer,
}) => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  await createPythonInterpreterSettingsSpecifier(tempDir)

  const configPath = path.join(tempDir, 'config.py')
  const configContent = await fs.readFile(configPath, 'utf8')
  const updatedContent = configContent.replace('enabled=False', 'enabled=True')
  await fs.writeFile(configPath, updatedContent)

  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )

  // Wait for the models folder to be visible
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

  // Open problems panel
  await runCommand(page, 'View: Focus Problems')

  await page.waitForSelector('text=problems')
  await page.waitForSelector('text=All models should have an owner')
})
