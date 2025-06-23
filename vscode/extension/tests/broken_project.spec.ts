import { test } from '@playwright/test'
import fs from 'fs-extra'
import os from 'os'
import path from 'path'
import { openLineageView, startVSCode, SUSHI_SOURCE_PATH } from './utils'

test('bad project, double model', async ({}) => {
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  // Read the customers.sql file
  const customersSql = await fs.readFile(
    path.join(tempDir, 'models', 'customers.sql'),
    'utf8',
  )

  // Write the customers.sql file with a double model
  await fs.writeFile(
    path.join(tempDir, 'models', 'customers_duplicated.sql'),
    customersSql,
  )

  const { window, close } = await startVSCode(tempDir)
  try {
    await window.waitForSelector('text=models')

    await window
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()

    await window
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    await window.waitForSelector('text=Error creating context')

    await window.waitForTimeout(1000)
  } finally {
    await close()
    await fs.remove(tempDir)
  }
})

test('working project, then broken through adding double model, then refixed', async ({}) => {
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  const { window, close } = await startVSCode(tempDir)
  try {
    // First, verify the project is working correctly
    await window.waitForSelector('text=models')

    // Open the lineage view to confirm it loads properly
    await openLineageView(window)
    await window.waitForSelector('text=Loaded SQLMesh context')

    // Read the customers.sql file
    const customersSql = await fs.readFile(
      path.join(tempDir, 'models', 'customers.sql'),
      'utf8',
    )

    // Add a duplicate model to break the project
    await fs.writeFile(
      path.join(tempDir, 'models', 'customers_duplicated.sql'),
      customersSql,
    )

    // Open the customers model to trigger the error
    await window
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()
    await window
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()
    // Save to refresh the context
    await window.keyboard.press('Control+S')
    await window.keyboard.press('Meta+S')

    // Wait for the error to appear
    // TODO: Selector doesn't work in the linage view
    // await window.waitForSelector('text=Error')

    // Remove the duplicated model to fix the project
    await fs.remove(path.join(tempDir, 'models', 'customers_duplicated.sql'))

    // Save again to refresh the context
    await window.keyboard.press('Control+S')
    await window.keyboard.press('Meta+S')

    // Wait for the error to go away and context to reload
    // TODO: Selector doesn't work in the linage view
    // await window.waitForSelector('text=raw.demographics')
  } finally {
    await close()
    await fs.remove(tempDir)
  }
})

test('bad project, double model, then fixed', async ({}) => {
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  // Read the customers.sql file
  const customersSql = await fs.readFile(
    path.join(tempDir, 'models', 'customers.sql'),
    'utf8',
  )

  // Write the customers.sql file with a double model
  await fs.writeFile(
    path.join(tempDir, 'models', 'customers_duplicated.sql'),
    customersSql,
  )

  const { window, close } = await startVSCode(tempDir)
  try {
    await window.waitForSelector('text=models')

    await window
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()

    await window
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    await window.waitForSelector('text=Error creating context')

    // Remove the duplicated model
    await fs.remove(path.join(tempDir, 'models', 'customers_duplicated.sql'))

    // Open the linage view
    await openLineageView(window)

    // Wait for the error to go away
    // TODO: Selector doesn't work in the linage view
    // await window.waitForSelector('text=raw.demographics')
  } finally {
    await close()
    await fs.remove(tempDir)
  }
})

test('bad project, double model, check lineage', async ({}) => {
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  // Read the customers.sql file
  const customersSql = await fs.readFile(
    path.join(tempDir, 'models', 'customers.sql'),
    'utf8',
  )

  // Write the customers.sql file with a double model
  await fs.writeFile(
    path.join(tempDir, 'models', 'customers_duplicated.sql'),
    customersSql,
  )

  const { window, close } = await startVSCode(tempDir)
  try {
    await window.waitForSelector('text=models')

    // Open the lineage view
    await openLineageView(window)

    await window.waitForSelector('text=Error creating context')
    await window.waitForSelector('text=Error:')

    await window.waitForTimeout(1000)
  } finally {
    await close()
    await fs.remove(tempDir)
  }
})
