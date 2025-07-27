import { test, expect } from './fixtures'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import {
  openServerPage,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

test('Autocomplete for model names', async ({ page, sharedCodeServer }) => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  await createPythonInterpreterSettingsSpecifier(tempDir)
  await openServerPage(page, tempDir, sharedCodeServer)

  // Wait for the models folder to be visible
  await page.waitForSelector('text=models')

  // Click on the models folder
  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()

  // Open the top_waiters model
  await page
    .getByRole('treeitem', { name: 'top_waiters.sql', exact: true })
    .locator('a')
    .click()

  await page.waitForSelector('text=grain')
  await waitForLoadedSQLMesh(page)

  await page.locator('text=grain').first().click()

  // Move to the end of the file
  for (let i = 0; i < 100; i++) {
    await page.keyboard.press('ArrowDown')
  }

  // Add a new line
  await page.keyboard.press('Enter')

  // Type the beginning of sushi.customers to trigger autocomplete
  await page.keyboard.type('sushi.waiter_as_customer')

  // Wait a moment for autocomplete to appear
  await page.waitForTimeout(500)

  // Check if the autocomplete suggestion for sushi.customers is visible
  expect(
    await page.locator('text=sushi.waiter_as_customer_by_day').count(),
  ).toBeGreaterThanOrEqual(1)
  expect(
    await page.locator('text=SQLMesh Model').count(),
  ).toBeGreaterThanOrEqual(1)
})

// Skip the macro completions test as regular checks because they are flaky and
// covered in other non-integration tests.
test.describe('Macro Completions', () => {
  test('Completion for inbuilt macros', async ({ page, sharedCodeServer }) => {
    const tempDir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'vscode-test-sushi-'),
    )
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)

    await createPythonInterpreterSettingsSpecifier(tempDir)
    await openServerPage(page, tempDir, sharedCodeServer)

    // Wait for the models folder to be visible
    await page.waitForSelector('text=models')

    // Click on the models folder
    await page
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()

    // Open the top_waiters model
    await page
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    await page.waitForSelector('text=grain')
    await waitForLoadedSQLMesh(page)

    await page.locator('text=grain').first().click()

    // Move to the end of the file
    for (let i = 0; i < 100; i++) {
      await page.keyboard.press('ArrowDown')
    }

    // Add a new line
    await page.keyboard.press('Enter')

    await page.waitForTimeout(500)

    // Hit the '@' key to trigger autocomplete for inbuilt macros
    await page.keyboard.press('@')
    await page.keyboard.type('eac')

    // Wait a moment for autocomplete to appear
    await page.waitForTimeout(500)

    // Check if the autocomplete suggestion for inbuilt macros is visible
    expect(await page.locator('text=@each').count()).toBeGreaterThanOrEqual(1)
  })

  test('Completion for custom macros', async ({ page, sharedCodeServer }) => {
    const tempDir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'vscode-test-sushi-'),
    )
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)

    await createPythonInterpreterSettingsSpecifier(tempDir)
    await openServerPage(page, tempDir, sharedCodeServer)

    // Wait for the models folder to be visible
    await page.waitForSelector('text=models')

    // Click on the models folder
    await page
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()

    // Open the top_waiters model
    await page
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    await page.waitForSelector('text=grain')
    await waitForLoadedSQLMesh(page)

    await page.locator('text=grain').first().click()

    // Move to the end of the file
    for (let i = 0; i < 100; i++) {
      await page.keyboard.press('ArrowDown')
    }

    // Add a new line
    await page.keyboard.press('Enter')

    // Type the beginning of a macro to trigger autocomplete
    await page.keyboard.press('@')
    await page.keyboard.type('add_o')

    // Wait a moment for autocomplete to appear
    await page.waitForTimeout(500)

    // Check if the autocomplete suggestion for custom macros is visible
    expect(await page.locator('text=@add_one').count()).toBeGreaterThanOrEqual(
      1,
    )
  })
})
