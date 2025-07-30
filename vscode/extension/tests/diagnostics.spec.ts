import { expect, test } from './fixtures'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { openProblemsView, openServerPage, SUSHI_SOURCE_PATH } from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'
import { execAsync } from '../src/utilities/exec'
import yaml from 'yaml'

test('Workspace diagnostics show up in the diagnostics panel', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  await createPythonInterpreterSettingsSpecifier(tempDir)

  const configPath = path.join(tempDir, 'config.py')
  const configContent = await fs.readFile(configPath, 'utf8')
  const updatedContent = configContent.replace('enabled=False', 'enabled=True')
  await fs.writeFile(configPath, updatedContent)

  await openServerPage(page, tempDir, sharedCodeServer)
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

  await openProblemsView(page)

  await page.waitForSelector('text=problems')
  await page.waitForSelector('text=All models should have an owner')
})

test.describe('Bad config.py/config.yaml file issues', () => {
  const setup = async (tempDir: string) => {
    // Run the sqlmesh CLI from the root of the repo using the local path
    const sqlmeshCliPath = path.resolve(__dirname, '../../../.venv/bin/sqlmesh')
    const result = await execAsync(sqlmeshCliPath, ['init', 'duckdb'], {
      cwd: tempDir,
    })
    expect(result.exitCode).toBe(0)
  }

  test('sqlmesh init, then corrupted config.yaml, bad yaml', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setup(tempDir)
    await createPythonInterpreterSettingsSpecifier(tempDir)

    const configYamlPath = path.join(tempDir, 'config.yaml')
    // Write an invalid YAML to config.yaml
    await fs.writeFile(configYamlPath, 'invalid_yaml; asdfasudfy')

    await openServerPage(page, tempDir, sharedCodeServer)

    // Open full_model.sql model
    await page
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()
    await page
      .getByRole('treeitem', { name: 'full_model.sql', exact: true })
      .locator('a')
      .click()

    // Wait for the error to appear
    await page.waitForSelector('text=Error creating context')

    await openProblemsView(page)

    // Asser that the error is present in the problems view
    await page
      .getByText('Invalid YAML configuration:')
      .first()
      .isVisible({ timeout: 5_000 })
  })

  test('sqlmesh init, then corrupted config.yaml, bad parameters', async ({
    page,
    sharedCodeServer,
  }) => {
    const tempDir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'vscode-test-tcloud-'),
    )
    await setup(tempDir)
    await createPythonInterpreterSettingsSpecifier(tempDir)

    const configYamlPath = path.join(tempDir, 'config.yaml')
    // Write an invalid YAML to config.yaml
    const config = {
      gateway: 'test',
    }
    // Write config to the yaml file
    await fs.writeFile(configYamlPath, yaml.stringify(config))

    await openServerPage(page, tempDir, sharedCodeServer)

    // Open full_model.sql model
    await page
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()
    await page
      .getByRole('treeitem', { name: 'full_model.sql', exact: true })
      .locator('a')
      .click()

    // Wait for the error to appear
    await page.waitForSelector('text=Error creating context')

    await openProblemsView(page)

    // Asser that the error is present in the problems view
    await page
      .getByText('Invalid project config:', { exact: true })
      .first()
      .isVisible({ timeout: 5_000 })
  })

  test('sushi example, correct python, bad config', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)
    await createPythonInterpreterSettingsSpecifier(tempDir)

    const configPyPath = path.join(tempDir, 'config.py')
    // Write an invalid Python to config.py
    await fs.writeFile(configPyPath, 'config = {}')

    await openServerPage(page, tempDir, sharedCodeServer)

    // Open customers.sql model
    await page
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()
    await page
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    // Expect the error to appear
    await page.waitForSelector('text=Error creating context')

    await openProblemsView(page)

    // Assert that the error is present in the problems view
    const errorElement = page
      .getByText('Config needs to be a valid object of type')
      .first()
    await expect(errorElement).toBeVisible({ timeout: 5000 })
  })

  test('sushi example, bad config.py', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)
    await createPythonInterpreterSettingsSpecifier(tempDir)

    const configPyPath = path.join(tempDir, 'config.py')
    // Write an invalid Python to config.py
    await fs.writeFile(configPyPath, 'invalid_python_code = [1, 2, 3')

    await openServerPage(page, tempDir, sharedCodeServer)

    // Open customers.sql model
    await page
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()
    await page
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    // Expect the error to appear
    await page.waitForSelector('text=Error creating context')

    await openProblemsView(page)

    // Assert that the error is present in the problems view
    const errorElement = page.getByText('Failed to load config file:').first()
    await expect(errorElement).toBeVisible({ timeout: 5000 })
  })
})

test.describe('Diagnostics for bad SQLMesh models', () => {
  test('duplicate model names', async ({ page, sharedCodeServer, tempDir }) => {
    // Copy over the sushi project
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)
    await createPythonInterpreterSettingsSpecifier(tempDir)

    // Duplicate the customers.sql model
    const customersSqlPath = path.join(tempDir, 'models', 'customers.sql')
    const duplicatedCustomersSqlPath = path.join(
      tempDir,
      'models',
      'customers_duplicated.sql',
    )
    await fs.copy(customersSqlPath, duplicatedCustomersSqlPath)

    await openServerPage(page, tempDir, sharedCodeServer)

    // Open full_model.sql model
    await page
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()
    await page
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    // Wait for the error to appear
    await page.waitForSelector('text=Error creating context')

    await openProblemsView(page)

    // Asser that the error is present in the problems view
    await page
      .getByText('Duplicate SQLMesh model name')
      .first()
      .isVisible({ timeout: 5_000 })
  })

  test('bad model block', async ({ page, sharedCodeServer, tempDir }) => {
    // Copy over the sushi project
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)
    await createPythonInterpreterSettingsSpecifier(tempDir)

    // Add a model with a bad model block
    const customersSqlPath = path.join(tempDir, 'models', 'bad_model.sql')
    const contents =
      'MODEL ( name sushi.bad_block, test); SELECT * FROM sushi.customers'
    await fs.writeFile(customersSqlPath, contents)

    await page.goto(
      `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
    )
    await page.waitForLoadState('networkidle')

    // Open the customers.sql model
    await page
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()
    await page
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    // Wait for the error to appear
    await page.waitForSelector('text=Error creating context')

    await openProblemsView(page)

    // Assert error is present in the problems view
    const errorElement = page
      .getByText("Required keyword: 'value' missing for")
      .first()
    await expect(errorElement).toBeVisible({ timeout: 5000 })
  })
})

test.describe('Diagnostics for bad audits', () => {
  test('bad audit block in audit', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    // Copy over the sushi project
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)
    await createPythonInterpreterSettingsSpecifier(tempDir)

    // Make an existing audit file a bad audit
    const auditFilePath = path.join(
      tempDir,
      'audits',
      'assert_item_price_above_zero.sql',
    )
    const readFile = await fs.readFile(auditFilePath, 'utf8')
    const updatedContent = readFile.replace('AUDIT (', 'AUDIT ( rubbish value,')
    await fs.writeFile(auditFilePath, updatedContent)

    // Navigate to the code-server instance
    await openServerPage(page, tempDir, sharedCodeServer)

    // Open a the customers.sql model
    await page
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()
    await page
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    // Wait for the error to appear
    await page.waitForSelector('text=Error creating context')

    await openProblemsView(page)

    // Assert that the error is present in the problems view
    const errorElement = page
      .getByText("Invalid extra fields {'rubbish'} in the audit definition")
      .first()
    await expect(errorElement).toBeVisible({ timeout: 5000 })
  })
})
