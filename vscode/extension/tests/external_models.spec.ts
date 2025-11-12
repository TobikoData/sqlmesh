import {
  openServerPage,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'
import { test, expect } from './fixtures'
import fs from 'fs-extra'
import path from 'path'

test.describe('External model files trigger lsp', () => {
  test('external_models.yaml', async ({ page, sharedCodeServer, tempDir }) => {
    const file = 'external_models.yaml'

    await fs.copy(SUSHI_SOURCE_PATH, tempDir)

    // Assert external_models.yaml exists
    const externalModelsYamlPath = path.join(tempDir, file)
    expect(await fs.pathExists(externalModelsYamlPath)).toBe(true)

    await createPythonInterpreterSettingsSpecifier(tempDir)
    await openServerPage(page, tempDir, sharedCodeServer)

    // Wait for the models folder to be visible
    await page.waitForSelector('text=models')

    // Click on the external_models file (e.g., external_models.yaml or external_models.yml)
    await page
      .getByRole('treeitem', { name: file, exact: true })
      .locator('a')
      .click()

    await page.waitForSelector('text=raw.demographics')
    await waitForLoadedSQLMesh(page)
  })

  test('external_models.yml', async ({ page, sharedCodeServer, tempDir }) => {
    const file = 'external_models.yml'

    await fs.copy(SUSHI_SOURCE_PATH, tempDir)

    // Move external_models.yaml to external_models.yml
    const externalModelsYamlPath = path.join(tempDir, 'external_models.yaml')
    const externalModelsYmlPath = path.join(tempDir, file)
    await fs.rename(externalModelsYamlPath, externalModelsYmlPath)

    // Assert external_models.yml exists
    expect(await fs.pathExists(externalModelsYmlPath)).toBe(true)

    await createPythonInterpreterSettingsSpecifier(tempDir)
    await openServerPage(page, tempDir, sharedCodeServer)

    // Wait for the models folder to be visible
    await page.waitForSelector('text=models')

    // Click on the external_models.yml file
    await page
      .getByRole('treeitem', { name: file, exact: true })
      .locator('a')
      .click()

    await page.waitForSelector('text=raw.demographics')
    await waitForLoadedSQLMesh(page)
  })
})
