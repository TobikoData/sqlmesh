import { test, expect } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { startVSCode, SUSHI_SOURCE_PATH } from './utils'

test('Render works correctly', async () => {
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

    // Render the model
    await window.keyboard.press(
      process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
    )
    await window.keyboard.type('Render Model')
    await window.keyboard.press('Enter')

    // Check if the model is rendered by check if "`oi`.`order_id` AS `order_id`," is in the window
    await expect(
      window.locator('text="marketing"."customer_id" AS'),
    ).toBeVisible()
    await expect(
      window.locator('text=sushi.customers (rendered)'),
    ).toBeVisible()

    await close()
  } finally {
    await fs.remove(tempDir)
  }
})

test('Render works correctly with model without a description', async () => {
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

    // Open the latest_order model
    await window
      .getByRole('treeitem', { name: 'latest_order.sql', exact: true })
      .locator('a')
      .click()

    await window.waitForSelector('text=custom_full_with_custom_kind')
    await window.waitForSelector('text=Loaded SQLMesh Context')

    // Render the model
    await window.keyboard.press(
      process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
    )
    await window.keyboard.type('Render Model')
    await window.keyboard.press('Enter')

    // Check if the model is rendered correctly
    await expect(window.locator('text="orders"."id" AS "id",')).toBeVisible()
    await expect(
      window.locator('text=sushi.latest_order (rendered)'),
    ).toBeVisible()

    await close()
  } finally {
    await fs.remove(tempDir)
  }
})

test('Render works correctly with every rendered model opening a new tab', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  try {
    const { window, close } = await startVSCode(tempDir)

    // Wait for the models folder to be visible
    await window.waitForSelector('text=models')
    await window
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()
    await window
      .getByRole('treeitem', { name: 'latest_order.sql', exact: true })
      .locator('a')
      .click()
    await window.waitForSelector('text=custom_full_with_custom_kind')
    await window.waitForSelector('text=Loaded SQLMesh Context')

    // Render the model
    await window.keyboard.press(
      process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
    )
    await window.keyboard.type('Render Model')
    await window.keyboard.press('Enter')

    // Check if the model is rendered correctly
    await expect(
      window.locator('text=sushi.latest_order (rendered)'),
    ).toBeVisible()

    // Open the customers model
    await window
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()
    await window.waitForSelector('text=grain')

    // Render the customers model
    await window.keyboard.press(
      process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
    )
    await window.keyboard.type('Render Model')
    await window.keyboard.press('Enter')

    // Assert both tabs exist
    await expect(
      window.locator('text=sushi.latest_order (rendered)'),
    ).toBeVisible()
    await expect(
      window.locator('text=sushi.customers (rendered)'),
    ).toBeVisible()

    await close()
  } finally {
    await fs.remove(tempDir)
  }
})

test('Render shows model picker when no active editor is open', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  try {
    const { window, close } = await startVSCode(tempDir)

    // Load the lineage view to initialize SQLMesh context (like lineage.spec.ts does)
    await window.keyboard.press(
      process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
    )
    await window.keyboard.type('Lineage: Focus On View')
    await window.keyboard.press('Enter')

    // Wait for "Loaded SQLmesh Context" text to appear
    const loadedContextText = window.locator('text=Loaded SQLMesh Context')
    await expect(loadedContextText.first()).toBeVisible({ timeout: 10_000 })

    // Run the render command without any active editor
    await window.keyboard.press(
      process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
    )
    await window.keyboard.type('Render Model')
    await window.keyboard.press('Enter')

    // Type to filter for customers model and select it
    await window.keyboard.type('customers')
    await window.waitForSelector('text=sushi.customers', { timeout: 5000 })
    await window.locator('text=sushi.customers').click()

    // Verify the rendered model is shown
    await expect(window.locator('text=sushi.customers (rendered)')).toBeVisible(
      { timeout: 15000 },
    )

    await close()
  } finally {
    await fs.remove(tempDir)
  }
})
