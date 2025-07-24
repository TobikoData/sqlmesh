import { test, expect } from './fixtures'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import {
  openServerPage,
  saveFile,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'
import { DuckDBInstance } from '@duckdb/node-api'

test.describe('Update external models columns', () => {
  test('New external model', async ({ page, sharedCodeServer }) => {
    // Normal setting up
    const tempDir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'vscode-test-sushi-'),
    )
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)
    await createPythonInterpreterSettingsSpecifier(tempDir)

    // Changing the config to set the default gateway to use the fixed one.
    const configPath = path.join(tempDir, 'config.py')
    const configContent = await fs.readFile(configPath, 'utf8')
    const original = `default_gateway="duckdb",`
    expect(configContent).toContain(original)
    const target = `default_gateway="duckdb_persistent",`
    const updatedConfigContent = configContent.replace(original, target)
    expect(updatedConfigContent).toContain(target)
    await fs.writeFile(configPath, updatedConfigContent)

    // Create an additional table in the database
    const table = 'raw.test_table'
    const databasePath = path.join(tempDir, 'data', 'duckdb.db')
    const instance = await DuckDBInstance.create(databasePath)
    const connection = await instance.connect()
    await connection.run(`CREATE SCHEMA IF NOT EXISTS raw`)
    await connection.run(
      `CREATE TABLE IF NOT EXISTS ${table}(
           id INTEGER,
           value VARCHAR
         )`,
    )
    connection.closeSync()
    instance.closeSync()
    expect(fs.existsSync(databasePath)).toBe(true)

    // Update the external_models in the config to include the new table but
    // not the columns by appending '- name: ${table}' to the external_models.yaml file
    const externalModelsPath = path.join(tempDir, 'external_models.yaml')
    const externalModelsContent = await fs.readFile(externalModelsPath, 'utf8')
    const newExternalModel = `- name: ${table}`
    const updatedExternalModelsContent = `${externalModelsContent}\n${newExternalModel}`
    await fs.writeFile(externalModelsPath, updatedExternalModelsContent)

    // Open the server page
    await openServerPage(page, tempDir, sharedCodeServer)

    //   Wait for the models folder to be visible
    await page.waitForSelector('text=models')

    // Click on the models folder, excluding external_models
    await page
      .getByRole('treeitem', { name: 'external_models.yaml', exact: true })
      .locator('a')
      .click()

    await waitForLoadedSQLMesh(page)

    // Click the update columns button
    await page.waitForSelector('text=Update Columns')
    const updateColumnButtons = page.getByRole('button', {
      name: 'Update Columns',
      exact: true,
    })
    // Click each one of them
    for (const button of await updateColumnButtons.all()) {
      await button.click()
      await page.waitForTimeout(1_000) // Wait for the action to complete
    }

    await page.waitForTimeout(1_000)
    await saveFile(page)
    await page.waitForTimeout(1_000)

    // Check the file contains the columns
    const updatedExternalModelsContentAfterUpdate = await fs.readFile(
      externalModelsPath,
      'utf8',
    )
    expect(updatedExternalModelsContentAfterUpdate).toContain(
      `- name: ${table}\n  columns:\n    id: INT\n    value: TEXT`,
    )
  })
})
