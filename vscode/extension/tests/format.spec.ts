import { test, expect } from './fixtures'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { runCommand, SUSHI_SOURCE_PATH } from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

test('Format project works correctly', async ({ page, sharedCodeServer }) => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  await createPythonInterpreterSettingsSpecifier(tempDir)

  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )

  //   Wait for the models folder to be visible
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

  await page.waitForSelector('text=grain')
  await page.waitForSelector('text=Loaded SQLMesh Context')

  // Format the project
  await runCommand(page, 'SQLMesh: Format Project')

  // Check that the notification appears saying 'Project formatted successfully'
  await expect(
    page.getByText('Project formatted successfully', { exact: true }),
  ).toBeVisible()
})
