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
    await window.waitForSelector('text=Loaded SQLMesh context')
  } finally {
    await close()
    await fs.remove(tempDir)
  }
})
