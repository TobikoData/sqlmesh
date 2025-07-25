import fs from 'fs-extra'
import path from 'path'
import os from 'os'
import {
  openProblemsView,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import { test, expect } from './fixtures'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

test('noselectstar quickfix', async ({ page, sharedCodeServer }) => {
  // Base test setup
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  await createPythonInterpreterSettingsSpecifier(tempDir)

  // Override the settings for the linter
  const configPath = path.join(tempDir, 'config.py')
  const read = await fs.readFile(configPath, 'utf8')
  // Replace linter to be on
  const target = 'enabled=True'
  const replaced = read.replace('enabled=False', 'enabled=True')
  // Assert replaced correctly
  expect(replaced).toContain(target)

  // Replace the rules to only have noselectstar
  const targetRules = `rules=[
            "noselectstar",
        ],`
  const replacedTheOtherRules = replaced.replace(
    `rules=[
            "ambiguousorinvalidcolumn",
            "invalidselectstarexpansion",
            "noselectstar",
            "nomissingaudits",
            "nomissingowner",
            "nomissingexternalmodels",
        ],`,
    targetRules,
  )
  expect(replacedTheOtherRules).toContain(targetRules)

  await fs.writeFile(configPath, replacedTheOtherRules)
  // Replace the file to cause the error
  const modelPath = path.join(tempDir, 'models', 'latest_order.sql')
  const readModel = await fs.readFile(modelPath, 'utf8')
  // Replace the specific select with the select star
  const modelReplaced = readModel.replace(
    'SELECT id, customer_id, start_ts, end_ts, event_date',
    'SELECT *',
  )
  await fs.writeFile(modelPath, modelReplaced)

  // Open the code server with the specified directory
  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )
  await page.waitForLoadState('networkidle')

  // Open the file with the linter issue
  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()
  await page
    .getByRole('treeitem', { name: 'latest_order.sql', exact: true })
    .locator('a')
    .click()

  await waitForLoadedSQLMesh(page)

  await openProblemsView(page)

  await page.getByRole('button', { name: 'Show fixes' }).click()
  await page
    .getByRole('menuitem', { name: 'Replace SELECT * with' })
    .first()
    .click()

  // Wait for the quick fix to be applied
  await page.waitForTimeout(2_000)

  // Assert that the model no longer contains SELECT * but SELECT id, customer_id, waiter_id, start_ts, end_ts, event_date
  const readUpdatedFile = (await fs.readFile(modelPath)).toString('utf8')
  expect(readUpdatedFile).not.toContain('SELECT *')
  expect(readUpdatedFile).toContain(
    'SELECT id, customer_id, waiter_id, start_ts, end_ts, event_date',
  )
})
