import { test, expect } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { startVSCode, SUSHI_SOURCE_PATH } from './utils'

test('Go to definition for macro', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  try {
    const { window, close } = await startVSCode(tempDir)

    // Wait for the models folder to be visible
    await window.waitForSelector('text=models')

    // Click on the models folder
    await window
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()

    // Open the customer_revenue_lifetime model
    await window
      .getByRole('treeitem', { name: 'top_waiters.sql', exact: true })
      .locator('a')
      .click()

    await window.waitForSelector('text=grain')
    await window.waitForSelector('text=Loaded SQLMesh Context')

    // Render the model
    await window.locator('text=@MULTIPLY').click({
      modifiers: ['Meta'],
    })

    // Check if the model is rendered by check if "`oi`.`order_id` AS `order_id`," is in the window
    await expect(window.locator('text=def multiply(')).toBeVisible()

    await close()
  } finally {
    await fs.removeSync(tempDir)
  }
})

test('Go to definition for model', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  try {
    const { window, close } = await startVSCode(tempDir)

    // Wait for the models folder to be visible
    await window.waitForSelector('text=models')

    // Click on the models folder
    await window
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()

    // Open the top_waiters model
    await window
      .getByRole('treeitem', { name: 'top_waiters.sql', exact: true })
      .locator('a')
      .click()

    await window.waitForSelector('text=grain')
    await window.waitForSelector('text=Loaded SQLMesh Context')

    // Go to definition for the model
    await window
      .locator('text=sushi.waiter_revenue_by_day')
      .first()
      .click({
        modifiers: ['Meta'],
      })
    await expect(
      window.locator('text=SUM(oi.quantity * i.price)::DOUBLE AS revenue'),
    ).toBeVisible()
    await close()
  } finally {
    await fs.removeSync(tempDir)
  }
})
